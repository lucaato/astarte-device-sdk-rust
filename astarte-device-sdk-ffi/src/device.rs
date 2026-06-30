use std::{mem, ptr::NonNull, str::FromStr, thread};

use astarte_device_sdk::{
    EventLoop,
    builder::DeviceBuilder,
    client::ClientConnection,
    pairing::api::PairingApi,
    store::memory::MemoryStore,
    transport::mqtt::{Credential, Mqtt, MqttArgs, MqttConfig},
};
use eyre::Context;
use ffi_convert::{AsRust, CDrop, CReprOf};
use tokio::{
    runtime::Runtime,
    task::{self, JoinHandle},
};
use url::Url;

use crate::{
    BorrowAsRust, BorrowCRepr, CCompatibleType, DeviceHandleConnectCallback,
    DeviceHandleLoopCallback, NativeResult, StringResult, UserData,
    config::{DeviceConfig, NativeDeviceConfig},
};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OpaqueDeviceHadle {
    _data: (),
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct NativeDeviceHandle(*mut OpaqueDeviceHadle);

impl CCompatibleType<NativeDeviceHandle> for NativeDeviceHandle {
    fn as_c_compat(input: NativeDeviceHandle) -> Result<Self, std::ffi::NulError> {
        Ok(input)
    }
}

impl CDrop for NativeDeviceHandle {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        let ptr: *mut DeviceHandle = unsafe { mem::transmute(self.0) };
        let _box = unsafe { Box::from_raw(ptr) };

        Ok(())
    }
}

// to allow converting a boxed device handle to a native device handle
impl CReprOf<Box<DeviceHandle>> for NativeDeviceHandle {
    fn c_repr_of(input: Box<DeviceHandle>) -> Result<Self, ffi_convert::CReprOfError> {
        let raw = Box::into_raw(input);

        Ok(Self(unsafe { mem::transmute(raw) }))
    }
}

// to allow getting a owned DeviceHandle from a pointer
// FIXME this is unsafe since we don't know who is currently accessing the handle
// impl AsRust<Box<DeviceHandle>> for NativeDeviceHandle {
//     fn as_rust(&self) -> Result<Box<DeviceHandle>, ffi_convert::AsRustError> {
//         let concrete: *mut DeviceHandle = unsafe { mem::transmute(self.0) };
//         let owned = unsafe { Box::from_raw(concrete) };

//         Ok(owned)
//     }
// }

impl BorrowAsRust<DeviceHandle> for NativeDeviceHandle {
    fn unsafe_borrow_as_rust<'a>(input: NonNull<Self>) -> &'a DeviceHandle {
        let this: &NativeDeviceHandle = unsafe { input.as_ref() };
        let concrete: &'a DeviceHandle = unsafe { mem::transmute(this.0) };

        concrete
    }
}

impl BorrowCRepr<NativeDeviceHandle> for NativeDeviceHandle {
    fn borrow_raw(input: &NativeDeviceHandle) -> Self {
        *input
    }
}

pub struct DeviceHandle {
    rt: Runtime,
    client: astarte_device_sdk::client::DeviceClient<Mqtt<MemoryStore, PairingApi>>,
    loop_handle: task::JoinHandle<()>,
}

impl DeviceHandle {
    // blocking function call to create device handle
    pub fn connect(
        // config: NativeDeviceConfig,
        // connect_cbk: DeviceHandleConnectCallback,
        // connect_user_data: UserData,
        // loop_cbk: DeviceHandleLoopCallback,
        // loop_user_data: UserData,
        config: DeviceConfig,
    ) -> eyre::Result<Box<DeviceHandle>> {
        // let config = config.as_rust()?;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let mk_client_handle = rt.spawn(async move {
            let (client, connection) = Self::mk_device(config).await?;

            let loop_handle = tokio::spawn(async move {
                let result = connection
                    .handle_events()
                    .await
                    // NOTE dummy type to not have a zst
                    .map(|_| true)
                    .wrap_err("error in device handle_events");

                let native_res = StringResult::<bool>::as_c_compat(result).unwrap();

                tokio::task::spawn_blocking(move || {
                    loop_cbk(&NativeResult::borrow_raw(&native_res), loop_user_data);
                });
            });

            Ok((loop_handle, client))
        });

        let (loop_handle, client) = rt.block_on(mk_client_handle)??;
        let device_handle = Box::new(DeviceHandle {
            rt,
            client,
            loop_handle,
        });

        // {
        //     Ok() => Box::new(DeviceHandle {
        //         rt,
        //         client,
        //         loop_handle,
        //     }),
        //     Err(e) => {
        //         let result = StringResult::<NativeDeviceHandle>::as_c_compat(Err(e)).unwrap();
        //         connect_cbk(&NativeResult::borrow_raw(&result), connect_user_data);
        //         return;
        //     }
        // };

        // let native_device = NativeDeviceHandle::c_repr_of(device_handle).unwrap();

        // let result = StringResult::<NativeDeviceHandle>::as_c_compat(Ok(native_device)).unwrap();

        // connect_cbk(&NativeResult::borrow_raw(&result), connect_user_data);

        Ok(device_handle)
    }

    async fn mk_device(
        config: DeviceConfig,
    ) -> eyre::Result<(
        astarte_device_sdk::client::DeviceClient<Mqtt<MemoryStore, PairingApi>>,
        astarte_device_sdk::connection::DeviceConnection<Mqtt<MemoryStore, PairingApi>>,
    )> {
        let args = MqttArgs {
            realm: config.realm,
            device_id: config.device_id,
            credential: Credential::secret(config.cred_secr),
            pairing_url: Url::from_str(config.pairing_url.as_ref())?,
        };

        let mqtt_config = MqttConfig::new(args).ignore_ssl_errors();

        let mut tmp_dir = std::env::temp_dir();

        tmp_dir.push("astarte-example-bindings");

        let (client, connection) = DeviceBuilder::new()
            .writable_dir(tmp_dir)
            .store(MemoryStore::new())
            .interface_directory(config.interfaces_dir.to_string())?
            .connection(mqtt_config)
            .build()
            .await?;

        Ok((client, connection))
    }

    // fn handle_poll_command(
    //     poll_command: PollCommand,
    //     client: impl Client + Clone + Send + 'static,
    // ) {
    //     let PollCommand {
    //         callback,
    //         user_data,
    //     } = poll_command;

    //     tokio::spawn(async move {
    //         let result = client.recv().await;

    //         let event = match result {
    //             Ok(e) => e,
    //             Err(e) => {
    //                 error!(%e, "error while receiving");

    //                 callback(
    //                     Err(DiplomatOwnedUTF8StrSlice::from(
    //                         e.to_string().into_boxed_str(),
    //                     ))
    //                     .into(),
    //                     user_data.load(std::sync::atomic::Ordering::Relaxed),
    //                 );

    //                 return;
    //             }
    //         };

    //         let DeviceEvent {
    //             interface,
    //             path,
    //             data,
    //         } = event;

    //         // NOTE this stuff needs to be freed
    //         let event = ReceiveEvent::new(interface, path, data);

    //         callback(
    //             Ok(event).into(),
    //             user_data.load(std::sync::atomic::Ordering::Relaxed),
    //         );
    //     });
    // }
    //
    async fn stop(
        // NOTE since we have no check that allows this function to be called only once we should
        mut client: impl ClientConnection + Clone,
        device_handle: JoinHandle<()>,
    ) -> eyre::Result<()> {
        client.disconnect().await?;

        device_handle.await?;

        Ok(())
    }

    // pub fn disconnect(self: Box<Self>) -> eyre::Result<()> {
    //     let Self {
    //         rt,
    //         client,
    //         device_handle,
    //     } = *self;

    //     rt.block_on(async move { Self::stop(client, device_handle).await })?;

    //     rt.shutdown_background();

    //     Ok(())
    // }
}

pub type DeviceHandleReceiveCallback =
    extern "C" fn(event: StringResult<ReceiveEvent>, user_data: UserData);

pub type DeviceHandleSendCallback = extern "C" fn(result: StringResult<()>, user_data: UserData);

pub type DeviceHandleDisconnectCallback =
    extern "C" fn(result: StringResult<()>, user_data: UserData);

pub struct SendValueCommand {
    interface: String,
    path: String,
    value: SendData,
    callback: DeviceHandleSendCallback,
    user_data: UserData,
}

pub struct PollCommand {
    callback: DeviceHandleReceiveCallback,
    user_data: UserData,
}

pub struct DisconnectCommand {
    callback: DeviceHandleDisconnectCallback,
    user_data: UserData,
}

impl DisconnectCommand {
    pub fn new(callback: DeviceHandleDisconnectCallback, user_data: UserData) -> Self {
        Self {
            callback,
            user_data,
        }
    }
}

pub enum DeviceCommand {
    SendValue(SendValueCommand),
    Poll(PollCommand),
    Disconnect(DisconnectCommand),
}

pub struct ReceiveEvent {
    data: (),
}
pub struct SendData {
    data: (),
}
