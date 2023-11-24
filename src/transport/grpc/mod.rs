pub mod convert;

use std::{collections::HashMap, ops::Deref, sync::Arc};

use astarte_message_hub_proto::{
    astarte_data_type, message_hub_client::MessageHubClient, AstarteMessage, Node,
};
use async_trait::async_trait;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::{
    builder::{ConnectionConfig, DeviceBuilder},
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

use self::convert::map_values_to_astarte_type;

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
    //#[error("The detach synchronous call during drop failed: {0}")]
    //DetachDuringDrop(#[from] std::io::Error),
}

/// Shared data of the grpc connection, this struct is internal to the [`Grpc`] connection
/// where is wrapped in an arc to share an immutable reference across tasks.
pub struct SharedGrpc {
    client: Mutex<MessageHubClient<tonic::transport::Channel>>,
    stream: Mutex<tonic::codec::Streaming<AstarteMessage>>,
    uuid: Uuid,
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
        uuid: Uuid,
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

    async fn try_detach<S>(
        client: &mut MessageHubClient<tonic::transport::Channel>,
        uuid: S,
    ) -> Result<(), GrpcTransportError>
    where
        S: ToString,
    {
        // During the detach phase only the uuid is needed we can pass an empty array
        // as the interface_json since the interfaces are alredy known to the message hub
        // this api will change in the future
        client
            .detach(Node::new(uuid, &[] as &[Vec<u8>]))
            .await
            .map(|_| ())
            .map_err(GrpcTransportError::from)
    }

    async fn try_detach_attach(&self, data: NodeData) -> Result<(), GrpcTransportError> {
        // the lock on stream is actually intended since we are detaching and re-attaching
        // we want to make sure no one uses the stream while the client is detached
        let mut stream = self.stream.lock().await;
        let mut client = self.client.lock().await;

        Grpc::try_detach(&mut client, &self.uuid).await?;

        *stream = Grpc::try_attach(&mut client, data).await?;

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
                    let data =
                        NodeData::try_from_unlocked(self.uuid, &*device.interfaces.read().await)?;

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
        let data = NodeData::try_from_unlocked(self.uuid, &*device.interfaces.read().await)?;

        self.try_detach_attach(data)
            .await
            .map_err(crate::Error::from)
    }

    async fn remove_interface(
        &self,
        interfaces: &Interfaces,
        _removed_interface: Interface,
    ) -> Result<(), crate::Error> {
        let data = NodeData::try_from_unlocked(self.uuid, interfaces)?;

        self.try_detach_attach(data)
            .await
            .map_err(crate::Error::from)
    }
}

/*
/// Implemented to correctly detach the device from the message hub
///
/// This implementation currently blocks the current thread to wait
/// for the complete disconnection from the message hub.
impl Drop for Grpc {
    fn drop(&mut self) {
    }
}
*/

/// Internal struct holding the received grpc message
#[derive(Debug, PartialEq)]
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
    uuid: Uuid,
    endpoint: String,
}

impl GrpcConfig {
    pub fn new(uuid: Uuid, endpoint: String) -> Self {
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

        let node_data = NodeData::try_from_unlocked(self.uuid, &builder.interfaces)?;
        let stream = Grpc::try_attach(&mut client, node_data).await?;

        Ok(Grpc::new(client, stream, self.uuid))
    }
}

/// Wrapper that contains data needed while connecting the node to the astarte message hub
struct NodeData {
    node: Node,
}

impl NodeData {
    fn try_from_unlocked(uuid: Uuid, interfaces: &Interfaces) -> Result<Self, GrpcTransportError> {
        let interfaces_defs: Vec<Vec<u8>> = interfaces
            .iter_interfaces()
            .map(|i| serde_json::to_string(i).map(|s| s.into_bytes()))
            .collect::<Result<_, serde_json::Error>>()?;

        Ok(Self {
            node: Node::new(uuid, &interfaces_defs),
        })
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, future::Future, str::FromStr, sync::Arc};

    use astarte_message_hub_proto::{
        message_hub_client::MessageHubClient,
        message_hub_server::{MessageHub, MessageHubServer},
        AstarteMessage, Node,
    };
    use async_trait::async_trait;
    use tempfile::{NamedTempFile, TempPath};
    use tokio::{
        net::{UnixListener, UnixStream},
        sync::{mpsc, Mutex},
    };
    use tokio_stream::wrappers::{ReceiverStream, UnixListenerStream};
    use uuid::{uuid, Uuid};

    use crate::{
        interface::mapping::path::MappingPath, interfaces::Interfaces, store::memory::MemoryStore,
        transport::test::mock_validate_object, types::AstarteType, Aggregation, AstarteAggregate,
        AstarteDeviceDataEvent, AstarteDeviceSdk, Client, EventReceiver, Interface,
    };

    use super::super::{test::mock_shared_device, Publish, Receive, ReceivedEvent};

    use super::{Grpc, GrpcReceivePayload, NodeData};

    #[derive(Debug)]
    enum ServerReceivedRequest {
        Attach(Node),
        Send(AstarteMessage),
        Detach(Node),
    }

    struct TestMessageHubServer {
        /// This stream can be used to send test events that will be handled by the astarte device sdk code
        /// and by the Grpc client.
        /// The value is wrapped in an [`Option`] to allow the value to be taken away.
        /// This means that currently a device can be attached only once.
        server_send: Mutex<
            Option<
                mpsc::Receiver<Result<astarte_message_hub_proto::AstarteMessage, tonic::Status>>,
            >,
        >,
        /// This stream contains requests received by the server
        server_received: mpsc::Sender<ServerReceivedRequest>,
    }

    impl TestMessageHubServer {
        fn new(
            server_send: mpsc::Receiver<
                Result<astarte_message_hub_proto::AstarteMessage, tonic::Status>,
            >,
            server_received: mpsc::Sender<ServerReceivedRequest>,
        ) -> Self {
            Self {
                server_send: Mutex::new(Some(server_send)),
                server_received,
            }
        }
    }

    #[async_trait]
    impl MessageHub for TestMessageHubServer {
        type AttachStream = tokio_stream::wrappers::ReceiverStream<
            Result<astarte_message_hub_proto::AstarteMessage, tonic::Status>,
        >;

        async fn attach(
            &self,
            request: tonic::Request<super::Node>,
        ) -> std::result::Result<tonic::Response<Self::AttachStream>, tonic::Status> {
            let inner = request.into_inner();
            println!("Client '{}' attached", inner.uuid.clone());

            self.server_received.send(ServerReceivedRequest::Attach(inner)).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            let receiver = self.server_send.lock().await
                .take().expect("No more streams found, you should provide one Receiver to be able to send data to the attach caller");

            Ok(tonic::Response::new(ReceiverStream::new(receiver)))
        }

        async fn send(
            &self,
            request: tonic::Request<super::AstarteMessage>,
        ) -> std::result::Result<tonic::Response<::pbjson_types::Empty>, tonic::Status> {
            self.server_received.send(ServerReceivedRequest::Send(request.into_inner())).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(pbjson_types::Empty::default()))
        }

        async fn detach(
            &self,
            request: tonic::Request<super::Node>,
        ) -> Result<tonic::Response<::pbjson_types::Empty>, tonic::Status> {
            let inner = request.into_inner();
            println!("Client '{}' detached", inner.uuid.clone());

            self.server_received.send(ServerReceivedRequest::Detach(inner)).await
                .expect("Could not send notification of a server received message, connect a channel to the Receiver");

            Ok(tonic::Response::new(pbjson_types::Empty::default()))
        }
    }

    fn make_unix_socket_path() -> Result<TempPath, std::io::Error> {
        let temp = NamedTempFile::new()?;
        std::fs::remove_file(&temp)?;

        Ok(temp.into_temp_path())
    }

    fn make_server(
        socket: &TempPath,
        server: TestMessageHubServer,
    ) -> Result<impl Future<Output = ()>, Box<dyn std::error::Error>> {
        let uds = UnixListener::bind(socket)?;
        let stream = UnixListenerStream::new(uds);

        Ok(async move {
            tonic::transport::Server::builder()
                .add_service(MessageHubServer::new(server))
                .serve_with_incoming(stream)
                .await
                .unwrap();
        })
    }

    async fn make_client(
        socket: Arc<TempPath>,
    ) -> Result<MessageHubClient<tonic::transport::Channel>, tonic::transport::Error> {
        // since we use a connector we are sure the url does not get read
        let channel = tonic::transport::Endpoint::try_from("http://any.url")
            .unwrap()
            .connect_with_connector(tower::service_fn(move |_: http::Uri| {
                let socket = Arc::clone(&socket);

                async move {
                    let socket = Arc::clone(&socket);

                    UnixStream::connect(&*socket).await
                }
            }))
            .await?;

        Ok(MessageHubClient::new(channel))
    }

    async fn mock_grpc_actors(
        server_impl: TestMessageHubServer,
    ) -> Result<
        (
            impl Future<Output = ()>,
            MessageHubClient<tonic::transport::Channel>,
        ),
        Box<dyn std::error::Error>,
    > {
        let socket = make_unix_socket_path()?;

        let shared_socket = Arc::new(socket);

        let server = make_server(&shared_socket, server_impl)?;

        let client = make_client(Arc::clone(&shared_socket)).await?;

        Ok((server, client))
    }

    const ID: Uuid = uuid!("67e55044-10b1-426f-9247-bb680e5fe0c8");

    async fn mock_astarte_grpc_client(
        mut message_hub_client: MessageHubClient<tonic::transport::Channel>,
        interfaces: &Interfaces,
    ) -> Result<Grpc, Box<dyn std::error::Error>> {
        let node_data = NodeData::try_from_unlocked(ID, interfaces)?;
        let stream = Grpc::try_attach(&mut message_hub_client, node_data).await?;

        Ok(Grpc::new(message_hub_client, stream, ID))
    }

    async fn mock_astarte_device_sdk_grpc<I>(
        client: MessageHubClient<tonic::transport::Channel>,
        interfaces: I,
    ) -> (AstarteDeviceSdk<MemoryStore, Grpc>, EventReceiver)
    where
        I: IntoIterator<Item = Interface>,
    {
        let (tx, rx) = mpsc::channel(50);
        let interfaces = Interfaces::from_iter(interfaces);

        println!("Creating client and server");
        let connection = mock_astarte_grpc_client(client, &interfaces)
            .await
            .expect("Could not construct a test connection");

        let sdk = AstarteDeviceSdk::new(interfaces, MemoryStore::new(), connection, tx);

        (sdk, rx)
    }

    struct TestServerChannels {
        server_response_sender:
            mpsc::Sender<Result<astarte_message_hub_proto::AstarteMessage, tonic::Status>>,
        server_request_receiver: mpsc::Receiver<ServerReceivedRequest>,
    }

    fn build_test_message_hub_server() -> (TestMessageHubServer, TestServerChannels) {
        // Holds the stream of messages that will follow an attach, the server stores the receiver
        // and relays messages to the stream got by the client that called `attach`
        let server_response_channel = mpsc::channel(10);

        // This channel holds requests that arrived to the server and can be used to verify that the
        // requests received are correct, the server will store the trasmitting end of the channel
        // to send events when a new request is received
        let server_request_channel = mpsc::channel(10);

        (
            TestMessageHubServer::new(server_response_channel.1, server_request_channel.0),
            TestServerChannels {
                server_response_sender: server_response_channel.0,
                server_request_receiver: server_request_channel.1,
            },
        )
    }

    macro_rules! expect_messages {
        ($poll_result_fn:expr; $($pattern:pat $($(=> $var:ident = $expr_value:expr;)? if $guard:expr),+),+) => {{
            let mut i = 0usize;

            $(
                // One based indexing
                i += 1;

                match $poll_result_fn {
                    Result::Ok(v) => {
                        match v {
                            $pattern => {
                                $(
                                    $(
                                        let $var = $expr_value;
                                    )?

                                    if !($guard) {
                                        panic!("The message n.{} didn't pass the guard '{}'", i, stringify!($guard));
                                    }
                                )*

                                println!("Matched message n.{}", i);
                            },
                            // Depending on the user declaration this pattern could be unreachable and this is fine
                            #[allow(unreachable_patterns)]
                            actual => panic!("Expected message n.{} to be matching the pattern '{}' but got '{:?}'", i, stringify!($pattern), actual),
                        }
                    }
                    Result::Err(e) => {
                        panic!("Expected message n.{} with pattern '{}' but the `{}` returned an `Err` {:?}", i, stringify!($pattern), stringify!($poll_result_fn), e);
                    }
                }
            )+
        }};
    }

    #[tokio::test]
    async fn test_attach() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let client_operations = async move {
            // When the grpc connection gets created the attach methods is called
            let _connection = mock_astarte_grpc_client(client, &Interfaces::new())
                .await
                .unwrap();
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string()
        );
    }

    #[tokio::test]
    async fn test_send_individual() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        const STRING_VALUE: &str = "value";

        let client_operations = async move {
            let (device, _rx) = mock_astarte_device_sdk_grpc(
                client,
                [Interface::from_str(crate::test::DEVICE_PROPERTIES).unwrap()],
            )
            .await;

            device
                .send(
                    "org.astarte-platform.rust.examples.individual-properties.DeviceProperties",
                    "/1/name",
                    AstarteType::String(STRING_VALUE.to_string()),
                )
                .await
                .unwrap();
        };

        // Poll client and server future
        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Send(m)
            => data_event = AstarteDeviceDataEvent::try_from(m).expect("Malformed message");
                if data_event.interface == "org.astarte-platform.rust.examples.individual-properties.DeviceProperties"
                && data_event.path == "/1/name"
                && matches!(data_event.data, Aggregation::Individual(AstarteType::String(v)) if v == STRING_VALUE)
        );
    }

    struct MockObject {}

    impl AstarteAggregate for MockObject {
        fn astarte_aggregate(self) -> Result<HashMap<String, AstarteType>, crate::error::Error> {
            let mut obj = HashMap::new();
            obj.insert("endpoint1".to_string(), AstarteType::Double(4.2));
            obj.insert(
                "endpoint2".to_string(),
                AstarteType::String("obj".to_string()),
            );
            obj.insert(
                "endpoint3".to_string(),
                AstarteType::BooleanArray(vec![true]),
            );

            Ok(obj)
        }
    }

    #[tokio::test]
    async fn test_send_object_timestamp() {
        let (server_impl, mut channels) = build_test_message_hub_server();
        let (server_future, client) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let client_operations = async move {
            let interface = Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap();
            let path = MappingPath::try_from("/1").unwrap();
            let interfaces = Interfaces::from_iter([interface.clone()]);
            let connection = mock_astarte_grpc_client(client, &interfaces).await.unwrap();

            let validated_object = mock_validate_object(
                &interface,
                &path,
                MockObject {},
                Some(chrono::offset::Utc::now()),
            )
            .unwrap();

            connection.send_object(validated_object).await.unwrap()
        };

        tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            _ = client_operations => println!("Client sent its data"),
        }

        expect_messages!(channels.server_request_receiver.try_recv();
            ServerReceivedRequest::Attach(a) if a.uuid == ID.to_string(),
            ServerReceivedRequest::Send(m)
            => data_event = AstarteDeviceDataEvent::try_from(m).expect("Malformed message");
                if data_event.interface == "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                    && data_event.path == "/1",
            => object_value = {  let Aggregation::Object(v) = data_event.data else { panic!("Expected object") }; v };
                if object_value["endpoint1"] == AstarteType::Double(4.2)
                    && object_value["endpoint2"] == AstarteType::String("obj".to_string())
                    && object_value["endpoint3"] == AstarteType::BooleanArray(vec![true])
        );
    }

    #[tokio::test]
    async fn test_receive_object() {
        let (server_impl, channels) = build_test_message_hub_server();
        let (server_future, client) = mock_grpc_actors(server_impl)
            .await
            .expect("Could not construct test client and server");

        let expected_object = Aggregation::Object((MockObject {}).astarte_aggregate().unwrap());

        let proto_payload: astarte_message_hub_proto::astarte_message::Payload =
            expected_object.try_into().unwrap();

        let astarte_message = astarte_message_hub_proto::AstarteMessage {
            interface_name: "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                .to_string(),
            path: "/1".to_string(),
            timestamp: None,
            payload: Some(proto_payload.clone()),
        };

        // Send object from server
        channels
            .server_response_sender
            .send(Ok(astarte_message))
            .await
            .unwrap();

        let interfaces =
            Interfaces::from_iter([
                Interface::from_str(crate::test::OBJECT_DEVICE_DATASTREAM).unwrap()
            ]);

        let client_connection = mock_astarte_grpc_client(client, &interfaces);

        let connection = tokio::select! {
            _ = server_future => panic!("The server closed before the client could complete sending the data"),
            res = client_connection => {
                println!("Client connected correctly: {}", res.is_ok());

                res.expect("Expected correct connection in test")
            },
        };

        let mock_shared_device = mock_shared_device(interfaces, mpsc::channel(1).0); // the channel won't be used

        let astarte_message_hub_proto::astarte_message::Payload::AstarteData(
            astarte_message_hub_proto::AstarteDataType {
                data: Some(expected_data),
            },
        ) = proto_payload.clone()
        else {
            panic!("Unexpected data format");
        };

        expect_messages!(connection.next_event(&mock_shared_device).await;
            ReceivedEvent {
                ref interface,
                ref path,
                payload: GrpcReceivePayload {
                    data,
                    timestamp: None,
                },
            } if interface == "org.astarte-platform.rust.examples.object-datastream.DeviceDatastream"
                && path == "/1"
                && data == expected_data
        );
    }
}
