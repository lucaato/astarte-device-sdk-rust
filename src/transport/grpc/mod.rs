use std::{collections::HashMap, ops::Deref, sync::Arc};

use astarte_message_hub_proto::{
    astarte_data_type, message_hub_client::MessageHubClient, AstarteMessage, Node,
};
use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::{
    builder::{ConnectionConfig, DeviceBuilder},
    convert::messagehub::map_values_to_astarte_type,
    interface::{
        mapping::path::MappingPath,
        reference::{MappingRef, ObjectRef},
    },
    interfaces::Interfaces,
    shared::SharedDevice,
    store::PropertyStore,
    types::AstarteType,
    validate::{ValidatedIndividual, ValidatedObject},
    Interface, Timestamp,
};

use super::{Publish, Receive, ReceivedEvent, Register};

/// Errors raised while using the [`Grpc`] transport
#[derive(Debug, thiserror::Error)]
pub enum GrpcTransportError {
    #[error("Transport error while working with grpc: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("Status error {0}")]
    Status(#[from] tonic::Status),
    #[error("Error while serializing the interfaces")]
    InterfacesSerialization(#[from] serde_json::Error),
    #[error("Attempting to deserialize individual message but got an object")]
    DeserializationExpectedIndividual,
    #[error("Attempting to deserialize object message but got an individual value")]
    DeserializationExpectedObject,
}

/// Shared data of the grpc connection, this struct is internal to the [`Grpc`] connection
/// where is wrapped in an arc to share an immutable reference across tasks.
pub struct SharedGrpc {
    client: Mutex<MessageHubClient<tonic::transport::Channel>>,
    stream: Mutex<tonic::codec::Streaming<AstarteMessage>>,
    uuid: String,
}

/// This struct represents a GRPC connection handler for an Astarte device. It manages the
/// interaction with the [astarte-message-hub](https://github.com/astarte-platform/astarte-message-hub), sending and receiving [`AstarteMessage`]
/// following the Astarte message hub protocol.
#[derive(Clone)]
pub struct Grpc {
    shared: Arc<SharedGrpc>,
}

impl Grpc {
    pub(crate) fn new(
        client: MessageHubClient<tonic::transport::Channel>,
        stream: tonic::codec::Streaming<AstarteMessage>,
        uuid: String,
    ) -> Self {
        Self {
            shared: Arc::new(SharedGrpc {
                client: Mutex::new(client),
                stream: Mutex::new(stream),
                uuid,
            }),
        }
    }

    /// Polls a message from the tonic stream and tries reattaching if necessary
    ///
    /// An [`Option`] is returned directly from the [`tonic::codec::Streaming::message`] method.
    /// A result of [`Option::None`] signals a disconnectiond and should be handled by the caller
    async fn next_message(&self) -> Result<Option<AstarteMessage>, GrpcTransportError> {
        self.stream
            .lock()
            .await
            .message()
            .await
            .map_err(GrpcTransportError::from)
    }

    async fn try_attach(
        client: &mut MessageHubClient<tonic::transport::Channel>,
        data: NodeData,
    ) -> Result<tonic::codec::Streaming<AstarteMessage>, GrpcTransportError> {
        client
            .attach(tonic::Request::new(data.node))
            .await
            .map(|r| r.into_inner())
            .map_err(GrpcTransportError::from)
    }

    async fn try_detach_attach(&self, data: NodeData) -> Result<(), GrpcTransportError> {
        let mut client = self.client.lock().await;

        // during the detach phase only the uuid is needed we can pass an empty array
        // since the interfaces are alredy known to the message hub
        // this api will change in the future
        client
            .detach(Node::new(self.uuid.clone(), &[] as &[Vec<u8>]))
            .await
            .map_err(GrpcTransportError::from)?;

        *self.stream.lock().await = Grpc::try_attach(&mut client, data).await?;

        Ok(())
    }
}

#[async_trait]
impl Publish for Grpc {
    async fn send_individual(&self, data: ValidatedIndividual<'_>) -> Result<(), crate::Error> {
        self.client
            .lock()
            .await
            .send(tonic::Request::new(data.try_into()?))
            .await
            .map(|_| ())
            .map_err(|e| GrpcTransportError::from(e).into())
    }

    async fn send_object(&self, data: ValidatedObject<'_>) -> Result<(), crate::Error> {
        self.client
            .lock()
            .await
            .send(tonic::Request::new(data.try_into()?))
            .await
            .map(|_| ())
            .map_err(|e| GrpcTransportError::from(e).into())
    }
}

impl Deref for Grpc {
    type Target = SharedGrpc;

    fn deref(&self) -> &Self::Target {
        &self.shared
    }
}

#[async_trait]
impl Receive for Grpc {
    type Payload = GrpcReceivePayload;

    async fn next_event<S>(
        &self,
        device: &SharedDevice<S>,
    ) -> Result<ReceivedEvent<Self::Payload>, crate::Error>
    where
        S: PropertyStore,
    {
        loop {
            match self.next_message().await? {
                Some(message) => {
                    let event: ReceivedEvent<Self::Payload> = message.try_into()?;

                    return Ok(event);
                }
                None => {
                    let data = {
                        let interfaces = device.interfaces.read().await;
                        NodeData::try_from_unlocked(self.uuid.clone(), &interfaces)?
                    };

                    let mut stream = self.stream.lock().await;
                    let mut client = self.client.lock().await;

                    *stream = Grpc::try_attach(&mut client, data).await?;
                }
            }
        }
    }

    fn deserialize_individual(
        &self,
        _mapping: MappingRef<'_, &Interface>,
        payload: Self::Payload,
    ) -> Result<(AstarteType, Option<Timestamp>), crate::Error> {
        let astarte_data_type::Data::AstarteIndividual(individual_data) = payload.data else {
            return Err(crate::Error::from(
                GrpcTransportError::DeserializationExpectedIndividual,
            ));
        };

        Ok((individual_data.try_into()?, payload.timestamp))
    }

    fn deserialize_object(
        &self,
        _object: ObjectRef,
        _path: &MappingPath<'_>,
        payload: Self::Payload,
    ) -> Result<(HashMap<String, AstarteType>, Option<Timestamp>), crate::Error> {
        let astarte_data_type::Data::AstarteObject(object) = payload.data else {
            return Err(crate::Error::from(
                GrpcTransportError::DeserializationExpectedObject,
            ));
        };

        Ok((
            map_values_to_astarte_type(object.object_data)?,
            payload.timestamp,
        ))
    }
}

#[async_trait]
impl Register for Grpc {
    async fn add_interface<S>(
        &self,
        device: &SharedDevice<S>,
        _added_interface: &str,
    ) -> Result<(), crate::Error>
    where
        S: PropertyStore,
    {
        let data = {
            let interfaces = device.interfaces.read().await;
            NodeData::try_from_unlocked(self.uuid.clone(), &interfaces)?
        };

        self.try_detach_attach(data)
            .await
            .map_err(crate::Error::from)
    }

    async fn remove_interface(
        &self,
        interfaces: &Interfaces,
        _removed_interface: Interface,
    ) -> Result<(), crate::Error> {
        let data = NodeData::try_from_unlocked(self.uuid.clone(), interfaces)?;

        self.try_detach_attach(data)
            .await
            .map_err(crate::Error::from)
    }
}

pub(crate) struct GrpcReceivePayload {
    data: astarte_data_type::Data,
    timestamp: Option<Timestamp>,
}

impl GrpcReceivePayload {
    pub(crate) fn new(data: astarte_data_type::Data, timestamp: Option<Timestamp>) -> Self {
        Self { data, timestamp }
    }
}

pub struct GrpcConfig {
    // TODO Should i enforce receiving a uuid struct [`uuid::Uuid`] or any string is fine ?
    uuid: String,
    endpoint: String,
}

impl GrpcConfig {
    pub fn new(uuid: String, endpoint: String) -> Self {
        Self { uuid, endpoint }
    }
}

/// Configuration for the grpc connection
#[async_trait]
impl ConnectionConfig for GrpcConfig {
    type Con = Grpc;
    type Err = GrpcTransportError;

    async fn connect<S, C>(self, builder: &DeviceBuilder<S, C>) -> Result<Self::Con, Self::Err>
    where
        S: PropertyStore,
        C: Send + Sync,
    {
        let mut client = MessageHubClient::connect(self.endpoint).await?;

        let node_data = NodeData::try_from_unlocked(self.uuid.clone(), &builder.interfaces)?;
        let stream = Grpc::try_attach(&mut client, node_data).await?;

        Ok(Grpc::new(client, stream, self.uuid))
    }
}

struct NodeData {
    node: Node,
}

impl NodeData {
    fn try_from_unlocked(
        uuid: String,
        interfaces: &Interfaces,
    ) -> Result<Self, GrpcTransportError> {
        let interfaces_defs: Vec<Vec<u8>> = interfaces
            .iter_interfaces()
            .map(|i| serde_json::to_string(i).map(|s| s.into_bytes()))
            .collect::<Result<_, serde_json::Error>>()?;

        Ok(Self {
            node: Node::new(uuid, &interfaces_defs),
        })
    }
}
