use std::{collections::HashMap, ops::Deref, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use log::{debug, error, info, trace};
use rumqttc::{Event as MqttEvent, Packet};
use tokio::sync::Mutex;

#[cfg(test)]
pub(crate) use crate::mock::{MockAsyncClient as AsyncClient, MockEventLoop as EventLoop};
#[cfg(not(test))]
pub(crate) use rumqttc::{AsyncClient, EventLoop};

use crate::{
    interface::{mapping::path::MappingPath, Ownership},
    interfaces::Interfaces,
    payload, properties,
    retry::DelaiedPoll,
    shared::SharedDevice,
    store::{PropertyStore, StoredProp},
    topic::parse_topic,
    types::AstarteType,
    AstarteDeviceDataEvent,
};

use super::{Connection, Registry};

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
    fn filter_device_properties(
        properties: Vec<StoredProp>,
        interfaces: &Interfaces,
    ) -> Vec<StoredProp> {
        properties
            .into_iter()
            .filter(|prop| match interfaces.get_property(&prop.interface) {
                Some(interface) => {
                    interface.ownership() == Ownership::Device
                        && interface.version_major() == prop.interface_major
                }
                None => false,
            })
            .collect()
    }

    fn filter_server_interfaces(interfaces: &Interfaces) -> Vec<String> {
        interfaces
            .iter_interfaces()
            .filter_map(|interface| match interface.ownership() {
                Ownership::Server => Some(interface.interface_name().to_owned()),
                Ownership::Device => None,
            })
            .collect()
    }

    async fn from_device<S>(
        device: &SharedDevice<S>,
    ) -> Result<Self, crate::store::error::StoreError>
    where
        S: PropertyStore,
    {
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
    pub(crate) fn new(
        realm: String,
        device_id: String,
        eventloop: EventLoop,
        client: AsyncClient,
    ) -> Self {
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
    }

    async fn connack<S>(
        &self,
        device: &SharedDevice<S>,
        connack: rumqttc::ConnAck,
    ) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        if connack.session_present {
            return Ok(());
        }

        let Introspection {
            interfaces,
            server_interfaces,
            device_properties,
        } = Introspection::from_device(device).await?;

        self.subscribe_server_interfaces(&server_interfaces).await?; // interfaces
        self.send_introspection(interfaces).await?; // interfaces
        self.send_emptycache().await?;
        self.send_device_properties(&device_properties).await?;
        info!("connack done");

        Ok(())
    }

    async fn subscribe_server_interfaces(
        &self,
        server_interfaces: &[String],
    ) -> Result<(), crate::Error> {
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

    async fn send_device_properties(
        &self,
        device_properties: &[StoredProp],
    ) -> Result<(), crate::Error> {
        for prop in device_properties {
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

    async fn purge_properties<S>(
        &self,
        device: &SharedDevice<S>,
        bdata: &[u8],
    ) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        let stored_props = device.store.load_all_props().await?;

        let paths = properties::extract_set_properties(bdata)?;

        for stored_prop in stored_props {
            if paths.contains(&format!("{}{}", stored_prop.interface, stored_prop.path)) {
                continue;
            }

            device
                .store
                .delete_prop(&stored_prop.interface, &stored_prop.path)
                .await?;
        }

        Ok(())
    }

    // i renamed it to include mqtt to avoid confunsion with the next_event these are different kind of events
    async fn poll_mqtt_event(&self) -> Result<MqttEvent, crate::Error> {
        let mut lock = self.eventloop.lock().await;

        match lock.poll().await {
            Ok(event) => Ok(event),
            Err(err) => {
                error!("couldn't poll the event loop: {err:#?}");

                DelaiedPoll::retry_poll_event(&mut lock).await
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
    S: PropertyStore,
{
    // TODO could we introduce a type even for the "topic" format so that we could use a MappingPath and pass it around currently we can't return a mapping path from a connection since it borrows data from the topic
    type SendPayload = Vec<u8>;
    type Payload = Bytes;
    type Err = crate::Error;

    async fn next_event(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<(String, String, Self::Payload), crate::Error> {
        // Keep consuming packets until we have an actual "data" event
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => self.connack(device, connack).await?,
                rumqttc::Packet::Publish(publish) => {
                    let (_, _, interface, path) = parse_topic(&publish.topic)?;

                    debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

                    match (interface, path.as_str()) {
                        ("control", "/consumer/properties") => {
                            debug!("Purging properties");

                            // TODO currently the purge properties is connection implementation specific but it could be moved back into the device by readding an enum
                            self.purge_properties(device, &publish.payload).await?;
                        }
                        _ => {
                            return Ok((interface.to_string(), path.to_string(), publish.payload));
                        }
                    }
                }
                _ => {}
            }
        }
    }

    /// Handles an incoming publish
    async fn handle_payload(
        &self,
        device: &SharedDevice<S>,
        (interface, path, bdata): (String, &MappingPath<'_>, Self::Payload),
    ) -> Result<AstarteDeviceDataEvent, crate::Error> {
        if cfg!(debug_assertions) {
            device
                .interfaces
                .read()
                .await
                .validate_receive(interface.as_str(), path, &bdata)?;
        }

        let data = payload::deserialize(&bdata)?;

        Ok(AstarteDeviceDataEvent {
            interface: interface.to_string(),
            path: path.to_string(),
            data,
        })
    }

    async fn send<'a>(
        &self,
        device: &SharedDevice<S>,
        interface_name: &str,
        interface_path: &MappingPath<'a>,
        payload: Self::SendPayload,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), Self::Err> {
        let ifaces_lock = device.interfaces.read().await;

        if cfg!(debug_assertions) {
            ifaces_lock.validate_send(interface_name, interface_path, &payload, &timestamp)?;
        }

        let qos = ifaces_lock.get_mqtt_reliability(interface_name, interface_path);

        self.client
            .publish(
                self.client_id() + "/" + interface_name.trim_matches('/') + interface_path.as_str(),
                qos,
                false,
                payload,
            )
            .await?;

        Ok(())
    }

    fn serialize_individual(
        &self,
        data: &AstarteType,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self::SendPayload, Self::Err> {
        let buf = payload::serialize_individual(data, timestamp)?;

        Ok(buf)
    }

    fn serialize_object(
        &self,
        data: &HashMap<String, AstarteType>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self::SendPayload, Self::Err> {
        let buf = payload::serialize_object(data, timestamp)?;

        Ok(buf)
    }
}

#[async_trait]
impl Registry for Mqtt {
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
            .publish(
                self.client_id(),
                rumqttc::QoS::ExactlyOnce,
                false,
                introspection,
            )
            .await
            .map_err(crate::Error::from)
    }
}

#[cfg(test)]
mod test {
    use std::str::FromStr;

    use rumqttc::Packet;
    use tokio::sync::{mpsc, RwLock};

    use crate::{
        interfaces::Interfaces,
        shared::SharedDevice,
        store::{memory::MemoryStore, wrapper::StoreWrapper},
        Interface,
    };

    use super::{AsyncClient, EventLoop, Mqtt, MqttEvent};

    fn mock_mqtt_connection(client: AsyncClient, eventl: EventLoop) -> Mqtt {
        Mqtt::new("realm".to_string(), "device_id".to_string(), eventl, client)
    }

    #[tokio::test]
    async fn test_poll_server_connack() {
        let mut eventl = EventLoop::default();
        let client = AsyncClient::default();

        eventl.expect_poll().once().returning(|| {
            Ok(MqttEvent::Incoming(rumqttc::Packet::ConnAck(
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )))
        });

        let mqtt_connection = mock_mqtt_connection(client, eventl);

        let ack = mqtt_connection
            .poll()
            .await
            .expect("Error while receiving the connack");

        if let Packet::ConnAck(ack) = ack {
            assert_eq!(ack.session_present, false);
            assert_eq!(ack.code, rumqttc::ConnectReturnCode::Success);
        }
    }

    #[tokio::test]
    async fn test_connack_client_response() {
        let eventl = EventLoop::default();
        let mut client = AsyncClient::default();

        client
            .expect_subscribe()
            .once()
            .returning(|_: String, _| Ok(()));

        client
            .expect_publish::<String, String>()
            .returning(|topic, _, _, _| {
                // Client id
                assert_eq!(topic, "realm/device_id");

                Ok(())
            });

        client
            .expect_publish::<String, &str>()
            .returning(|topic, _, _, payload| {
                // empty cache
                assert_eq!(topic, "realm/device_id/control/emptyCache");

                assert_eq!(payload, "1");

                Ok(())
            });

        client.expect_subscribe::<String>().returning(|topic, _qos| {
            assert_eq!(topic, "realm/device_id/org.astarte-platform.rust.examples.individual-datastream.ServerDatastream/#");

            Ok(())
        });

        let interfaces = [
            Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap(),
            Interface::from_str(crate::test::INDIVIDUAL_SERVER_DATASTREAM).unwrap(),
        ];

        let (tx, _rx) = mpsc::channel(2);

        let mqtt_connection = mock_mqtt_connection(client, eventl);
        let shared_device = SharedDevice {
            interfaces: RwLock::new(
                Interfaces::from(interfaces).expect("Error while contructing Interfaces object"),
            ),
            store: StoreWrapper {
                store: MemoryStore::new(),
            },
            tx,
        };

        mqtt_connection
            .connack(
                &shared_device,
                rumqttc::ConnAck {
                    session_present: false,
                    code: rumqttc::ConnectReturnCode::Success,
                },
            )
            .await
            .unwrap();
    }
}
