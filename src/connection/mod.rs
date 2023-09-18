use std::{collections::HashMap, error::Error as StdError};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{Utc, DateTime};

use crate::{types::AstarteType, interface::mapping::path::MappingPath, AstarteDeviceDataEvent, shared::SharedDevice};

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
pub(crate) trait Connection<S>: Send + Sync + Clone + 'static
where
	Self::SendPayload: Send + Sync + 'static,
	Self::Payload: Send + Sync + 'static,
	Self::Err: StdError + Send + Sync + 'static,
{
	type SendPayload;
	type Payload;
	type Err;

    // Since we already handle the connack in the poll function this shouldn't be needed
	//async fn connect(&self, device: &SharedDevice<S>) -> Result<(), crate::Error/*Self::Err*/>;

	async fn next_event(&self, device: &SharedDevice<S>) -> Result<Self::Payload, crate::Error/*Self::Err*/>;

	async fn handle_payload(&self, device: &SharedDevice<S>, payload: Self::Payload) -> Result<ReceivedEvent, crate::Error/*Self::Err*/>;

	async fn send<'a>(&self, device: &SharedDevice<S>, interface_name: &str,
        interface_path: &MappingPath<'a>, payload: Self::SendPayload, timestamp: Option<DateTime<Utc>>) -> Result<(), crate::Error/*Self::Err*/>;

	fn serialize_individual(&self, data: &AstarteType, timestamp: Option<DateTime<Utc>>) -> Result<Self::SendPayload, crate::Error/*Self::Err*/>;

	fn serialize_object(&self, data: &HashMap<String, AstarteType>, timestamp: Option<DateTime<Utc>>) -> Result<Self::SendPayload, crate::Error/*Self::Err*/>;
}

#[async_trait]
pub(crate) trait Register {
	async fn subscribe(&self, interface: &str) -> Result<(), crate::Error>;

	async fn unsubscribe(&self, interface: &str) -> Result<(), crate::Error>;

	async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error>;
}
