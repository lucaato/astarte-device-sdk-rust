use std::{collections::HashMap, ops::Deref, sync::RwLock, fmt::Debug};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Utc, DateTime};
use log::{debug, trace, error, info};
use rumqttc::{AsyncClient, EventLoop, Event as MqttEvent, Publish};
use tokio::sync::Mutex;

use crate::{interfaces::Interfaces, EventSender, Interface, types::AstarteType, retry::DelaiedPoll, AstarteDeviceSdk, store::{PropertyStore, StoredProp}, interface::{mapping::path::MappingPath, self}, Aggregation, AstarteDeviceDataEvent, topic::parse_topic, payload, shared::SharedDevice};

pub mod mqtt;
//#[cfg(feature)]
//pub mod grpc;

//enum ConnectionError<E> {
//	
//}

pub enum ReceivedEvent {
	PurgeProperties(Bytes),
	Data(AstarteDeviceDataEvent),
}

#[async_trait]
pub(crate) trait Connection<S>: Send + Sync + Clone {
	type SendPayload: Send + Sync;
	type Payload: Send + Sync;
	type Err: std::fmt::Debug + Into<crate::Error>;
//     type Payload

	// this will be moved in connectionbuilder ?
	// clarification this is not the function to actually connect this is the callback connected to do stuff that should be done after the connection is opened
	// fow now i will keep this commented until i decide if it's really useful fn connected(&mut self, interfaces: &Interfaces);
//     connect(&mut self, interfaces: &Interfaces) (BUILD in AstarteOptions)
	async fn connect(&self, device: &SharedDevice<S>) -> Result<(), crate::Error/*Self::Err*/>;

	async fn next_event(&self, device: &SharedDevice<S>) -> Result<Self::Payload, crate::Error/*Self::Err*/>;

	async fn handle_payload(&self, device: &SharedDevice<S>, payload: Self::Payload) -> Result<ReceivedEvent, crate::Error/*Self::Err*/>;

	// can't expose private traits so i need to pass in a str
	async fn send<'a>(&self, device: &SharedDevice<S>, interface_name: &str,
        interface_path: &MappingPath<'a>, payload: Self::SendPayload, timestamp: Option<DateTime<Utc>>) -> Result<(), crate::Error/*Self::Err*/>;

	fn serialize(&self, data: &AstarteType, timestamp: Option<DateTime<Utc>>) -> Result<Self::SendPayload, crate::Error/*Self::Err*/>;

	fn serialize_object(&self, data: &HashMap<String, AstarteType>, timestamp: Option<DateTime<Utc>>) -> Result<Self::SendPayload, crate::Error/*Self::Err*/>;
}

#[async_trait]
pub(crate) trait Register {
	// functions specific to mqtt (this will be implemented just by Mqtt struct)
	async fn subscribe(&self, interface: &str) -> Result<(), crate::Error>;

	async fn unsubscribe(&self, interface: &str) -> Result<(), crate::Error>;

	async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error>;
}
