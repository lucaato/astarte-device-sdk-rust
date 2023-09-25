/*

* This file is part of Astarte.
*
* Copyright 2021 SECO Mind Srl
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* SPDX-License-Identifier: Apache-2.0
*/
use std::{collections::HashMap, fmt::Display, ops::Deref, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;
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
    interface::{mapping::{path::MappingPath, self}, Ownership},
    interfaces::{Interfaces, MappingRef, ObjectRef},
    payload::{self, Payload}, properties,
    retry::DelayedPoll,
    shared::SharedDevice,
    store::{PropertyStore, StoredProp},
    topic::parse_topic,
    types::AstarteType,
    AstarteDeviceDataEvent, Aggregation, Interface,
};

use super::{Connection, Registry, ReceivedEvent};

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

            let payload = Payload::new(&prop.value).to_vec()?;

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

                DelayedPoll::retry_poll_event(&mut lock).await
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

    async fn send<'a>(
        &self,
        interface: &Interface,
        path: &str,
        reliability: rumqttc::QoS,
        payload: Vec<u8>,
    ) -> Result<(), crate::Error> {
        self.client
            .publish(
                format!(
                    "{}/{}{}",
                    self.client_id(),
                    interface.interface_name(),
                    path
                ),
                reliability,
                false,
                payload,
            )
            .await?;

        Ok(())
    }
}

#[async_trait]
impl<S> Connection<S> for Mqtt
where
    S: PropertyStore,
{
    type Payload = Bytes;

    async fn next_event(&self, device: &SharedDevice<S>) -> Result<ReceivedEvent<Self::Payload>, crate::Error> {
        static PURGE_PROPERTIES_TOPIC: OnceCell<String> = OnceCell::new();
        static CLIENT_ID: OnceCell<String> = OnceCell::new();

        // Keep consuming packets until we have an actual "data" event
        loop {
            match self.poll().await? {
                rumqttc::Packet::ConnAck(connack) => self.connack(device, connack).await?,
                rumqttc::Packet::Publish(publish) => {
                    let purge_topic = PURGE_PROPERTIES_TOPIC.get_or_init(|| format!("{}/control/consumer/properties", self.client_id()));

                    debug!("Incoming publish = {} {:x}", publish.topic, publish.payload);

                    if purge_topic == &publish.topic {
                        debug!("Purging properties");

                        self.purge_properties(device, &publish.payload).await?;
                    } else {
                        let client_id = CLIENT_ID.get_or_init(|| format!("{}", self.client_id()));
                        let (interface, path) = parse_topic(&client_id, &publish.topic)?;

                        return Ok(ReceivedEvent {
                            interface: interface.to_string(),
                            path: path.to_string(),
                            payload: publish.payload,
                        });
                    }
                }
                packet => {
                    trace!("packet received {packet:?}");
                }
            }
        }
    }

    fn deserialize_individual(&self, mapping: MappingRef<'_, &Interface>, payload: &Self::Payload) -> Result<(AstarteType, Option<DateTime<Utc>>), crate::Error> {
        payload::deserialize_individual(mapping, payload)
            .map_err(|err| err.into())
    }

    fn deserialize_object(&self, object: ObjectRef, path: &MappingPath, payload: &Self::Payload) -> Result<(HashMap<String, AstarteType>, Option<DateTime<Utc>>), crate::Error> {
        payload::deserialize_object(object, path, payload)
            .map_err(|err| err.into())
    }

    // TODO add MappingPath parameter to have a validted path as input
    async fn send_individual<'a>(&self,
        mapping: MappingRef<'a, &'a Interface>,
        data: &AstarteType,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), crate::Error> {
        let buf = payload::serialize_individual(mapping, data, timestamp)?;

        self.send(mapping.interface(), mapping.mapping().endpoint(), mapping.reliability().into(), buf).await
    }

    async fn send_object(&self,
        object: ObjectRef<'_>,
        path: &MappingPath,
        data: &HashMap<String, AstarteType>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), crate::Error> {
        let buf = payload::serialize_object(object, path, data, timestamp)?;

        self.send(object.interface, path.as_str(), object.reliability().into(), buf).await
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
            assert!(!ack.session_present);
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
