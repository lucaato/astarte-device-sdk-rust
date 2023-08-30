use std::{collections::HashMap, ops::Deref, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{debug, trace, error, info};
use rumqttc::{AsyncClient, EventLoop, Event as MqttEvent, Packet, ConnAck, Publish};
use tokio::sync::Mutex;

use crate::{store::{StoredProp, PropertyStore}, shared::SharedDevice, interface::{Ownership, mapping::path::MappingPath}, retry::DelaiedPoll, payload, EventSender, interfaces::Interfaces, topic::parse_topic, AstarteDeviceDataEvent, Interface, types::AstarteType};

use super::{Register, Connection, ReceivedEvent};

pub struct SharedMqtt {
    realm: String,
    device_id: String,
    eventloop: Mutex<EventLoop>,
}

pub struct Mqtt {
    shared: Arc<SharedMqtt>,
    client: AsyncClient,
}

impl Deref for Mqtt {
    type Target = SharedMqtt;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

impl Clone for Mqtt {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            client: self.client.clone(),
        }
    }
}

struct Introspection {
    interfaces: String,
    server_interfaces: Vec<String>,
    device_properties: Vec<StoredProp>,
}

impl Introspection {
    fn filter_device_properties(properties: Vec<StoredProp>, interfaces: &Interfaces) -> Vec<StoredProp> {
        properties
            .into_iter()
            .filter(|prop| match interfaces.get_property(&prop.interface) {
                Some(interface) =>
                    interface.ownership() == Ownership::Device
                        && interface.version_major() == prop.interface_major,
                None => false,
            })
            .collect()
    }

    fn filter_server_interfaces(interfaces: &Interfaces) -> Vec<String> {
        interfaces.iter_interfaces()
            .filter_map(|interface| match interface.ownership() {
                Ownership::Server => Some(interface.interface_name().to_owned()),
                Ownership::Device => None
            })
            .collect()
    }

    // TODO function to generically filter stuff from Ownership
    //fn filter_server_interfaces(interfaces)

    async fn from_device<S>(device: &SharedDevice<S>) -> Result<Self, crate::store::error::StoreError>
    where
        S: PropertyStore {
        let properties = device.store.load_all_props().await?;
        let interfaces = device.interfaces.read().await;

        let device_properties = Self::filter_device_properties(properties, &interfaces);
        let server_interfaces = Self::filter_server_interfaces(&interfaces);

        Ok(Self {
            interfaces: interfaces.get_introspection_string(),
            server_interfaces,
            device_properties,
        })
    }
}

impl Mqtt {
    pub(crate) fn new(realm: String, device_id: String, eventloop: EventLoop, client: AsyncClient) -> Self {
        Self {
            shared: Arc::new(SharedMqtt {
                realm,
                device_id,
                eventloop: Mutex::new(eventloop),
            }),
            client,
        }
    }

    fn client_id(&self) -> String {
        format!("{}/{}", self.realm, self.device_id)
		// TODO use format args
		// format_args!();
    }

    async fn connack<S>(&self, device: &SharedDevice<S>, connack: rumqttc::ConnAck) -> Result<(), crate::Error>
    where
        S: PropertyStore {
        if connack.session_present {
            return Ok(());
        }

        let Introspection { interfaces, server_interfaces, device_properties } = Introspection::from_device(device).await?;

        self.subscribe_server_interfaces(&server_interfaces).await?; // interfaces
        self.send_introspection(interfaces).await?; // interfaces
        self.send_emptycache().await?;
        self.send_device_properties(&device_properties).await?;
        info!("connack done");

        Ok(())
    }

    async fn subscribe_server_interfaces(&self, server_interfaces: &[String]) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                self.client_id() + "/control/consumer/properties",
                rumqttc::QoS::ExactlyOnce,
            )
            .await?;

        for iface in server_interfaces {
            self.client
                .subscribe(
                    self.client_id() + "/" + iface + "/#",
                    rumqttc::QoS::ExactlyOnce,
                )
                .await?;
        }

        Ok(())
    }

    async fn send_emptycache(&self) -> Result<(), crate::Error> {
        let url = self.client_id() + "/control/emptyCache";
        debug!("sending emptyCache to {}", url);

        self.client
            .publish(url, rumqttc::QoS::ExactlyOnce, false, "1")
            .await?;

        Ok(())
    }

    async fn send_device_properties(&self, device_properties: &[StoredProp]) -> Result<(), crate::Error> {
        for prop in device_properties {
			// TODO this should probably be moved inside the connection the part that sends a property over the connection
            let topic = format!("{}/{}{}", self.client_id(), prop.interface, prop.path);

            debug!(
                "sending device-owned property = {}{}",
                prop.interface, prop.path
            );

            let payload = payload::serialize_individual(&prop.value, None)?;

            self.client
                .publish(topic, rumqttc::QoS::ExactlyOnce, false, payload)
                .await?;
        }

        Ok(())
    }

    // i renamed it to include mqtt to avoid confunsion with the next_event these are different kind of events
    async fn poll_mqtt_event(&self) -> Result<MqttEvent, crate::Error> {
        let mut lock = self.eventloop.lock().await;

        loop {
            match lock.poll().await {
                Ok(event) => return Ok(event),
                Err(err) => {
                    error!("couldn't poll the event loop: {err:#?}");

                    return DelaiedPoll::retry_poll_event(&mut lock).await;
                }
            }
        }
    }

    async fn poll_ack(&self) -> Result<ConnAck, crate::Error> {
        loop {
            match self.poll_mqtt_event().await? {
                MqttEvent::Incoming(Packet::ConnAck(connack)) => return Ok(connack),
                MqttEvent::Incoming(packet) => error!("Got different packet {:?} while waiting for ack", packet),
                MqttEvent::Outgoing(outgoing) => error!("Got {:?} while waiting for ack packet", outgoing),
            }
        }
    }

    async fn poll(&self) -> Result<Packet, crate::Error> {
        loop {
            match self.poll_mqtt_event().await? {
                MqttEvent::Incoming(packet) => return Ok(packet),
                MqttEvent::Outgoing(outgoing) => trace!("MQTT Outgoing = {:?}", outgoing),
            }
        }
    }
}

#[async_trait]
impl<S> Connection<S> for Mqtt
where
    S: PropertyStore {

    type SendPayload = Vec<u8>;
	type Payload = rumqttc::Publish;
	type Err = crate::Error;

	async fn connect(&self, device: &SharedDevice<S>) -> Result<(), Self::Err> {
        debug!("Trying to connect");

        let connack = self.poll_ack().await?;

        self.connack(device, connack).await
    }

    /// Poll updates from mqtt, can be placed in a loop to receive data.
    ///
    /// This is a blocking function. It should be placed on a dedicated thread/task or as the main
    /// thread.
    ///
    /// ```no_run
    /// use astarte_device_sdk::{AstarteDeviceSdk, options::AstarteOptions};
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     let mut sdk_options = AstarteOptions::new("_","_","_","_");
    ///     let (mut device, mut rx_events) = AstarteDeviceSdk::new(sdk_options).await.unwrap();
    ///
    ///     tokio::spawn(async move {
    ///         while let Some(event) = rx_events.recv().await {
    ///             assert!(event.is_ok());
    ///         };
    ///     });
    ///
    ///     device.handle_events().await;
    /// }
    /// ```
	async fn next_event(&self, device: &SharedDevice<S>) -> Result<Self::Payload, crate::Error> {
        loop {
            let packet = self.poll().await?;

            // Keep consuming and processing packets until we have data for the user
            match packet {
                rumqttc::Packet::ConnAck(connack) => self.connack(device, connack).await?,
                rumqttc::Packet::Publish(publish) => return Ok(publish),
                _ => {}
            }
        }
	}

	/// Handles an incoming publish
    async fn handle_payload(
        &self,
        device: &SharedDevice<S>, publish: Publish) -> Result<ReceivedEvent, crate::Error> {
        let (_, _, interface, path) = parse_topic(&publish.topic)?;

        debug!("received publish for interface \"{interface}\" and path \"{path}\"");

        // It can be borrowed as a &[u8]
        let bdata = publish.payload;

        match (interface, path.as_str()) {
            ("control", "/consumer/properties") => {
                debug!("Purging properties");

                Ok(ReceivedEvent::PurgeProperties(bdata))
            }
            _ => {
                debug!("Incoming publish = {} {:x}", publish.topic, bdata);

                if cfg!(debug_assertions) {
                    device.interfaces.read().await.validate_receive(&interface, &path, &bdata);
                }

                let data= payload::deserialize(&bdata)?;

                Ok(ReceivedEvent::Data(AstarteDeviceDataEvent {
                    interface: interface.to_string(),
                    path: path.to_string(),
                    data,
                }))
            }
        }
    }

	async fn send<'a>(&self, device: &SharedDevice<S>, interface_name: &str,
        interface_path: &str, payload: Self::SendPayload, timestamp: Option<DateTime<Utc>>) -> Result<(), Self::Err> {
        
        let ifaces_lock = device.interfaces.read().await;

        if cfg!(debug_assertions) {
            ifaces_lock.validate_send(
                interface_name,
                interface_path,
                &payload,
                &timestamp,
            )?;
        }

        let qos = ifaces_lock.get_mqtt_reliability(interface_name, interface_path);

        self.client.publish(self.client_id() + "/" + interface_name.trim_matches('/') + interface_path.as_str(),
            qos, false, payload).await?;

        Ok(())
    }

	fn serialize_individual(&self, data: &AstarteType, timestamp: Option<DateTime<Utc>>) -> Result<Self::SendPayload, Self::Err> {
        let buf = payload::serialize_individual(&data, timestamp)?;

        Ok(buf)
    }

	fn serialize_object(&self, data: &HashMap<String, AstarteType>, timestamp: Option<DateTime<Utc>>) -> Result<Self::SendPayload, Self::Err> {
        let buf = payload::serialize_object(&data, timestamp)?;

        Ok(buf)
    }
}

#[async_trait]
impl Register for Mqtt {
    async fn subscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                self.client_id() + "/" + interface_name + "/#",
                rumqttc::QoS::ExactlyOnce,
            )
			.await
            .map_err(crate::Error::from)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .unsubscribe(self.client_id() + "/" + interface_name + "/#")
			.await
            .map_err(crate::Error::from)
    }

	async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error> {
        debug!("sending introspection = {}", introspection);

        self.client
            .publish(self.client_id(), rumqttc::QoS::ExactlyOnce, false, introspection)
			.await
            .map_err(crate::Error::from)
	}
}
