use std::{collections::HashMap, fmt::Display, ops::Deref, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use log::{debug, error, info, trace};
use once_cell::sync::OnceCell;
use rumqttc::{Event as MqttEvent, Packet, Publish};
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

#[derive(Debug, Clone, Copy)]
struct ClientId<'a> {
    realm: &'a str,
    device_id: &'a str,
}

impl<'a> Display for ClientId<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.realm, self.device_id)
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

    fn client_id(&self) -> ClientId {
        ClientId {
            realm: &self.realm,
            device_id: &self.device_id,
        }
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
                format!("{}/control/consumer/properties", self.client_id()),
                rumqttc::QoS::ExactlyOnce,
            )
            .await?;

        for iface in server_interfaces {
            self.client
                .subscribe(
                    format!("{}/{iface}/#", self.client_id()),
                    rumqttc::QoS::ExactlyOnce,
                )
                .await?;
        }

        Ok(())
    }

    async fn send_emptycache(&self) -> Result<(), crate::Error> {
        let url = format!("{}/control/emptyCache", self.client_id());
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
    type SendPayload = Vec<u8>;
    type Payload = Publish;

    async fn next_event(&self, device: &SharedDevice<S>) -> Result<Self::Payload, crate::Error> {
        static PURGE_PROPERTIES_TOPIC: OnceCell<String> = OnceCell::new();

        // Keep consuming packets until we have an actual "data" event
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => self.connack(device, connack).await?,
                rumqttc::Packet::Publish(publish) => {
                    let purge_topic = PURGE_PROPERTIES_TOPIC.get_or_init(|| {
                        format!("{}/control/consumer/properties", self.client_id())
                    });

                    debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

                    if purge_topic == &publish.topic {
                        debug!("Purging properties");
                        self.purge_properties(device, &publish.payload).await?;
                    } else {
                        return Ok(publish);
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
        publish: Self::Payload,
    ) -> Result<AstarteDeviceDataEvent, crate::Error> {
        let (_, _, interface, path) = parse_topic(&publish.topic)?;
        let bdata = publish.payload;

        if cfg!(debug_assertions) {
            device
                .interfaces
                .read()
                .await
                .validate_receive(interface, &path, &bdata)?;
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
    ) -> Result<(), crate::Error> {
        let ifaces_lock = device.interfaces.read().await;

        if cfg!(debug_assertions) {
            ifaces_lock.validate_send(interface_name, interface_path, &payload, &timestamp)?;
        }

        let qos = ifaces_lock.get_mqtt_reliability(interface_name, interface_path);

        self.client
            .publish(
                format!(
                    "{}/{}{}",
                    self.client_id(),
                    interface_name.trim_matches('/'),
                    interface_path
                ),
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
    ) -> Result<Self::SendPayload, crate::Error> {
        let buf = payload::serialize_individual(data, timestamp)?;

        Ok(buf)
    }

    fn serialize_object(
        &self,
        data: &HashMap<String, AstarteType>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self::SendPayload, crate::Error> {
        let buf = payload::serialize_object(data, timestamp)?;

        Ok(buf)
    }
}

#[async_trait]
impl Registry for Mqtt {
    async fn subscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .subscribe(
                format!("{}/{interface_name}/#", self.client_id()),
                rumqttc::QoS::ExactlyOnce,
            )
            .await
            .map_err(crate::Error::from)
    }

    async fn unsubscribe(&self, interface_name: &str) -> Result<(), crate::Error> {
        self.client
            .unsubscribe(format!("{}/{interface_name}/#", self.client_id()))
            .await
            .map_err(crate::Error::from)
    }

    async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error> {
        debug!("sending introspection = {}", introspection);

        let path = self.client_id().to_string();

        self.client
            .publish(path, rumqttc::QoS::ExactlyOnce, false, introspection)
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
