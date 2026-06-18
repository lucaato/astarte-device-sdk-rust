use std::{ffi::c_void, str::FromStr, sync::atomic::AtomicPtr, time::Duration};

use astarte_device_sdk::{
    Client, DeviceEvent, EventLoop,
    builder::DeviceBuilder,
    store::memory::MemoryStore,
    transport::mqtt::{Credential, MqttArgs, MqttConfig},
};
use diplomat_runtime::{DiplomatOwnedUTF8StrSlice, DiplomatResult, DiplomatUtf8StrSlice};
use eyre::Context;
use tokio::sync::mpsc;
use tracing::error;
use url::Url;

use crate::ffi::{DeviceConfig, DeviceHandle, ReceiveEvent, SendData};

#[diplomat::bridge]
mod ffi {
    use astarte_device_sdk::{
        AstarteData, Value,
        aggregate::AstarteObject,
        astarte_interfaces,
        chrono::{DateTime, Utc},
    };
    use diplomat_runtime::{DiplomatUtf8StrSlice, DiplomatWrite};
    use eyre::Context;
    use tokio::{runtime::Runtime, sync::mpsc, task::JoinHandle};

    use crate::{connect_device, run_device_tasks};

    use super::DeviceCommand;

    use std::{fmt::Write, sync::atomic::AtomicPtr};

    pub struct DeviceConfig<'a> {
        pub device_id: DiplomatUtf8StrSlice<'a>,
        pub cred_secr: DiplomatUtf8StrSlice<'a>,
        pub realm: DiplomatUtf8StrSlice<'a>,
        pub pairing_url: DiplomatUtf8StrSlice<'a>,
        pub interfaces_dir: DiplomatUtf8StrSlice<'a>,
    }

    #[diplomat::opaque]
    pub enum SendData {
        Individual(AstarteData),
        IndividualWithTimestamp(AstarteData, DateTime<Utc>),
        Aggregate(AstarteObject),
        AggregateWithTimestamp(AstarteObject, DateTime<Utc>),
        Property(Option<AstarteData>),
    }

    #[diplomat::opaque]
    pub struct ReceiveEvent {
        interface: String,
        path: String,
        value: Value,
    }

    #[diplomat::enum_convert(astarte_interfaces::schema::MappingType)]
    pub enum DeviceMappingType {
        Double,
        Integer,
        Boolean,
        LongInteger,
        String,
        BinaryBlob,
        DateTime,
        DoubleArray,
        IntegerArray,
        BooleanArray,
        LongIntegerArray,
        StringArray,
        BinaryBlobArray,
        DateTimeArray,
    }

    #[diplomat::opaque]
    pub struct DeviceIndividualValue(AstarteData, DateTime<Utc>);

    impl DeviceIndividualValue {
        pub fn get_type(&self) -> DeviceMappingType {
            match self.0 {
                AstarteData::Double(_) => DeviceMappingType::Double,
                AstarteData::Integer(_) => DeviceMappingType::Integer,
                AstarteData::Boolean(_) => DeviceMappingType::Boolean,
                AstarteData::LongInteger(_) => DeviceMappingType::LongInteger,
                AstarteData::String(_) => DeviceMappingType::String,
                AstarteData::BinaryBlob(_) => DeviceMappingType::BinaryBlob,
                AstarteData::DateTime(_) => DeviceMappingType::DateTime,
                AstarteData::DoubleArray(_) => DeviceMappingType::DoubleArray,
                AstarteData::IntegerArray(_) => DeviceMappingType::IntegerArray,
                AstarteData::BooleanArray(_) => DeviceMappingType::BooleanArray,
                AstarteData::LongIntegerArray(_) => DeviceMappingType::LongIntegerArray,
                AstarteData::StringArray(_) => DeviceMappingType::StringArray,
                AstarteData::BinaryBlobArray(_) => DeviceMappingType::BinaryBlobArray,
                AstarteData::DateTimeArray(_) => DeviceMappingType::DateTimeArray,
            }
        }

        pub fn as_string<'a>(&'a self) -> Option<&'a str> {
            if let AstarteData::String(s) = &self.0 {
                Some(&s)
            } else {
                None
            }
        }

        // crate::lib::impl_opion_get!(as_double(Double) -> f64);
        // crate::lib::impl_opion_get!(as_integer(Integer) -> i32);
        // crate::lib::impl_opion_get!(as_boolean(Boolean) -> bool);
        // crate::lib::impl_opion_get!(as_long(LongInteger) -> u64);

        pub fn as_binary_blob<'a>(&'a self) -> Option<&'a [u8]> {
            if let AstarteData::BinaryBlob(s) = &self.0 {
                Some(s)
            } else {
                None
            }
        }

        /// Returns the datetime value of this AstarteIndividualValue in *millisecons*
        pub fn as_datetime(&self) -> Option<i64> {
            if let AstarteData::DateTime(d) = &self.0 {
                Some(d.timestamp_millis())
            } else {
                None
            }
        }

        // still missing
        // DoubleArray,
        // IntegerArray,
        // BooleanArray,
        // LongIntegerArray,
        // StringArray,
        // BinaryBlobArray,
        // DateTimeArray,

        pub fn as_double_array<'a>(&'a self) -> Option<&'a [f64]> {
            if let AstarteData::DoubleArray(d) = &self.0 {
                Some(unsafe { std::mem::transmute(d.as_slice()) })
            } else {
                None
            }
        }

        pub fn as_integer_array<'a>(&'a self) -> Option<&'a [i32]> {
            if let AstarteData::IntegerArray(d) = &self.0 {
                Some(d)
            } else {
                None
            }
        }

        pub fn as_boolean_array<'a>(&'a self) -> Option<&'a [bool]> {
            if let AstarteData::BooleanArray(d) = &self.0 {
                Some(d)
            } else {
                None
            }
        }

        /// Returns the timestamp of this value in *millisecons*
        pub fn get_timestamp(&self) -> i64 {
            self.1.timestamp_millis()
        }
    }

    pub enum DeviceValueType {
        Individual,
        Object,
        Property,
    }

    impl ReceiveEvent {
        pub(crate) fn new(interface: String, path: String, value: Value) -> Self {
            Self {
                interface,
                path,
                value,
            }
        }

        pub fn interface(&self, write: &mut DiplomatWrite) -> Result<(), ()> {
            write!(write, "{}", self.interface).map_err(|_| ())
        }

        pub fn path(&self, write: &mut DiplomatWrite) -> Result<(), ()> {
            write!(write, "{}", self.path).map_err(|_| ())
        }

        pub fn value_type(&self) -> DeviceValueType {
            if self.value.is_individual() {
                if self.value.is_property() {
                    DeviceValueType::Property
                } else {
                    DeviceValueType::Individual
                }
            } else {
                DeviceValueType::Object
            }
        }

        pub fn as_individual(&self) -> Option<Box<DeviceIndividualValue>> {
            self.value
                .as_individual()
                .map(|(data, timestamp)| Box::new(DeviceIndividualValue(data.clone(), *timestamp)))
        }

        // pub fn as_object(&self) -> Option

        // pub fn as_object(&self) -> DiplomatOption<> {

        // }

        // pub fn as_property(&self) ->  {

        // }
    }

    // zst to be used as a user_data pointer anythig can be passed
    // as a pointer to parameters of this type
    #[diplomat::opaque]
    pub struct UserDataPtr;

    #[diplomat::opaque]
    pub struct DeviceHandle {
        tx: mpsc::Sender<DeviceCommand>,
        rt: Runtime,
        device_handle: JoinHandle<()>,
    }

    impl DeviceHandle {
        pub(crate) fn new(
            tx: mpsc::Sender<DeviceCommand>,
            rt: Runtime,
            device_handle: JoinHandle<()>,
        ) -> Self {
            Self {
                tx,
                rt,
                device_handle,
            }
        }

        pub extern "C" fn connect<'a, C, L>(
            config: DeviceConfig<'a>,
            connect_callback: C,
            connect_user_data: &UserDataPtr,
            loop_error_callback: L,
            loop_user_data: &UserDataPtr,
        ) where
            C: Fn(Result<Box<DeviceHandle>, ()>, &UserDataPtr) + Send,
            L: Fn(Result<(), ()>, &UserDataPtr) + Send,
        {
            // let connect_user_data = connect_user_data_ptr as *const UserDataPtr as usize;

            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .wrap_err("could not start runtime");

            let rt = match rt {
                Ok(rt) => rt,
                Err(e) => {
                    connect_callback(Err(()), connect_user_data);
                    return;
                }
            };

            let result = rt.block_on(async move { connect_device(config).await });

            let (client, connection) = match result {
                Ok(device) => device,
                Err(e) => {
                    rt.spawn_blocking(move || {
                        connect_callback(Err(()), connect_user_data);
                    });

                    return;
                }
            };

            let (tx, rx) = mpsc::channel(100);

            let device_handle = rt.spawn(async move {
                let result = run_device_tasks(client, connection, rx).await;

                if let Err(e) = result {
                    loop_error_callback(Err(()), loop_user_data);
                }
            });

            // no spawn_blocking here since we are outside the runtime and we need to wrap it in the Box
            connect_callback(
                Ok(Box::new(Self::new(tx, rt, device_handle))),
                connect_user_data,
            );
        }
    }
}

// macro_rules! impl_opion_get {
//     ($method:ident($variant:ident) -> $ret_type:ident) => {
//         pub fn $method(&self) -> Option<$ret_type> {
//             if let AstarteData::$variant(d) = self.0 {
//                 Some($ret_type::from(d.clone()))
//             } else {
//                 None
//             }
//         }
//     };
// }

pub type DeviceHandleLoopCallback =
    extern "C" fn(result: DiplomatResult<(), DiplomatOwnedUTF8StrSlice>, user_data: *mut c_void);

// pub type DeviceHandleConnectCallback = extern "C" fn(
//     result: DiplomatResult<Box<DeviceHandle>, DiplomatOwnedUTF8StrSlice>,
//     user_data: *mut c_void,
// );

pub type DeviceHandleReceiveCallback = extern "C" fn(
    event: DiplomatResult<ReceiveEvent, DiplomatOwnedUTF8StrSlice>,
    user_data: *mut c_void,
);

pub type DeviceHandleSendCallback =
    extern "C" fn(result: DiplomatResult<(), DiplomatOwnedUTF8StrSlice>, user_data: *mut c_void);

pub type DeviceHandleDisconnectCallback =
    extern "C" fn(result: DiplomatResult<(), DiplomatOwnedUTF8StrSlice>, user_data: *mut c_void);

impl DeviceHandle {
    #[unsafe(no_mangle)]
    pub extern "C" fn device_handle_connect<'a>(
        config: DeviceConfig<'a>,
        connect_callback: DeviceHandleConnectCallback,
        connect_user_data_ptr: *mut c_void,
        loop_error_callback: DeviceHandleLoopCallback,
        loop_user_data: *mut c_void,
    ) {
        let connect_user_data = AtomicPtr::new(connect_user_data_ptr);

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .wrap_err("could not start runtime");

        let rt = match rt {
            Ok(rt) => rt,
            Err(e) => {
                connect_callback(
                    Err(DiplomatOwnedUTF8StrSlice::from(
                        e.to_string().into_boxed_str(),
                    ))
                    .into(),
                    connect_user_data.load(std::sync::atomic::Ordering::Relaxed),
                );
                return;
            }
        };

        let result = rt.block_on(async move { connect_device(config).await });

        let (client, connection) = match result {
            Ok(device) => device,
            Err(e) => {
                let connect_user_data = AtomicPtr::new(connect_user_data_ptr);

                rt.spawn_blocking(move || {
                    connect_callback(
                        Err(DiplomatOwnedUTF8StrSlice::from(
                            e.to_string().into_boxed_str(),
                        ))
                        .into(),
                        connect_user_data.load(std::sync::atomic::Ordering::Relaxed),
                    );
                });

                return;
            }
        };

        let (tx, rx) = mpsc::channel(100);

        let loop_user_data = AtomicPtr::new(loop_user_data);

        let device_handle = rt.spawn(async move {
            let result = run_device_tasks(client, connection, rx).await;

            if let Err(e) = result {
                loop_error_callback(
                    Err(DiplomatOwnedUTF8StrSlice::from(
                        e.to_string().into_boxed_str(),
                    ))
                    .into(),
                    loop_user_data.load(std::sync::atomic::Ordering::Relaxed),
                );
            }
        });

        // no spawn_blocking here since we are outside the runtime and we need to wrap it in the Box
        connect_callback(
            Ok(Box::new(Self::new(tx, rt, device_handle))).into(),
            connect_user_data.load(std::sync::atomic::Ordering::Relaxed),
        );
    }
}

struct SendValueCommand {
    interface: String,
    path: String,
    value: SendData,
    callback: DeviceHandleSendCallback,
    user_data: AtomicPtr<c_void>,
}

struct PollCommand {
    callback: DeviceHandleReceiveCallback,
    user_data: AtomicPtr<c_void>,
}

struct DisconnectCommand {
    callback: DeviceHandleDisconnectCallback,
    user_data: AtomicPtr<c_void>,
}

enum DeviceCommand {
    SendValue(SendValueCommand),
    Poll(PollCommand),
    Disconnect(DisconnectCommand),
}

async fn connect_device(
    config: DeviceConfig<'_>,
) -> eyre::Result<(
    impl Client + Clone + Send + 'static,
    impl EventLoop + Send + 'static,
)> {
    let args = MqttArgs {
        realm: config.realm.to_string(),
        device_id: config.device_id.to_string(),
        credential: Credential::secret(config.cred_secr.to_string()),
        pairing_url: Url::from_str(config.pairing_url.as_ref()).unwrap(),
    };

    let mqtt_config = MqttConfig::new(args).ignore_ssl_errors();

    let mut tmp_dir = std::env::temp_dir();

    tmp_dir.push("astarte-example-bindings");

    let (client, connection) = DeviceBuilder::new()
        .writable_dir(tmp_dir)
        .store(MemoryStore::new())
        .interface_directory(config.interfaces_dir.to_string())
        .unwrap()
        .connection(mqtt_config)
        .build()
        .await?;

    Ok((client, connection))
}

fn handle_poll_command(poll_command: PollCommand, client: impl Client + Clone + Send + 'static) {
    let PollCommand {
        callback,
        user_data,
    } = poll_command;

    tokio::spawn(async move {
        let result = client.recv().await;

        let event = match result {
            Ok(e) => e,
            Err(e) => {
                error!(%e, "error while receiving");

                callback(
                    Err(DiplomatOwnedUTF8StrSlice::from(
                        e.to_string().into_boxed_str(),
                    ))
                    .into(),
                    user_data.load(std::sync::atomic::Ordering::Relaxed),
                );

                return;
            }
        };

        let DeviceEvent {
            interface,
            path,
            data,
        } = event;

        // NOTE this stuff needs to be freed
        let event = ReceiveEvent::new(interface, path, data);

        callback(
            Ok(event).into(),
            user_data.load(std::sync::atomic::Ordering::Relaxed),
        );
    });
}

async fn run_device_tasks(
    client: impl Client + Clone + Send + 'static,
    connection: impl EventLoop + Send + 'static,
    mut rx: mpsc::Receiver<DeviceCommand>,
) -> eyre::Result<()> {
    let handle_events = tokio::task::spawn(async move { connection.handle_events().await });

    while let Some(command) = rx.recv().await {
        match command {
            DeviceCommand::Poll(poll_command) => handle_poll_command(poll_command, client.clone()),
            DeviceCommand::SendValue(send_value_command) => todo!(),
            DeviceCommand::Disconnect(disconnect_command) => todo!(),
            // _ => tokio::time::sleep(Duration::from_secs(10)).await,
        }
    }

    handle_events.await??;

    Ok(())
}
