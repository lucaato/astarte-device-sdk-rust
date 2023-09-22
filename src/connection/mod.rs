use std::collections::HashMap;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{
    interface::mapping::path::MappingPath, shared::SharedDevice, types::AstarteType,
    AstarteDeviceDataEvent,
};

pub mod mqtt;

#[async_trait]
pub(crate) trait Connection<S>: Send + Sync + Clone + 'static {
    type SendPayload: Send + Sync;
    type Payload: Send + Sync + 'static;

    async fn next_event(&self, device: &SharedDevice<S>) -> Result<Self::Payload, crate::Error>;

    async fn handle_payload(
        &self,
        device: &SharedDevice<S>,
        payload: Self::Payload,
    ) -> Result<AstarteDeviceDataEvent, crate::Error>;

    async fn send<'a>(
        &self,
        device: &SharedDevice<S>,
        interface_name: &str,
        interface_path: &MappingPath<'a>,
        payload: Self::SendPayload,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<(), crate::Error>;

    fn serialize_individual(
        &self,
        data: &AstarteType,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self::SendPayload, crate::Error>;

    fn serialize_object(
        &self,
        data: &HashMap<String, AstarteType>,
        timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self::SendPayload, crate::Error>;
}

#[async_trait]
pub(crate) trait Registry {
    async fn subscribe(&self, interface: &str) -> Result<(), crate::Error>;

    async fn unsubscribe(&self, interface: &str) -> Result<(), crate::Error>;

    async fn send_introspection(&self, introspection: String) -> Result<(), crate::Error>;
}
