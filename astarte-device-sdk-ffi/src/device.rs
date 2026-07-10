use std::{
    ffi::{CString, NulError},
    mem,
    ptr::NonNull,
    str::FromStr,
    sync::{RwLock, RwLockReadGuard},
    thread,
};

use astarte_device_sdk::{
    Client, DeviceEvent, EventLoop,
    builder::DeviceBuilder,
    client::ClientConnection,
    pairing::api::PairingApi,
    store::memory::MemoryStore,
    transport::{
        Connection,
        mqtt::{Credential, Mqtt, MqttArgs, MqttConfig},
    },
    types::Double,
};
use eyre::{Context, OptionExt, bail, eyre};
use ffi_convert::{AsRust, CDrop, CReprOf, UnexpectedNullPointerError};
use tokio::{
    runtime::Runtime,
    task::{self, JoinHandle},
};
use tracing::{debug, error, info};
use url::Url;

use crate::{
    BorrowAsRust,
    config::{DeviceConfig, NativeDeviceConfig},
    data::{IndividualSend, NativeIndividualSend},
};

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct OpaqueDeviceHadle {
    _data: (),
}

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct NativeDeviceHandle(*mut OpaqueDeviceHadle);
unsafe impl Send for NativeDeviceHandle {}

impl NativeDeviceHandle {
    pub fn new(opaque: *mut OpaqueDeviceHadle) -> Self {
        Self(opaque)
    }
}

// FIXME this drop impl does nothing this is by design
// so that when we pass this inside NativeStringResult<NativeDeviceHandle>
// we know that this won't get dropped
// this is because the error that contains the cstring still needs to be freed
// to avoid doing this workaround we could
// - have a new type that does not require cdrop for the ok type but still owns and drops the error
// - provide a free function that does not drop the native device handle (which is only dropped when passed to disconnect)
//   and pass the result as value so that the drop does not get called by rust
impl CDrop for NativeDeviceHandle {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        // let ptr: *mut DeviceHandle = unsafe { mem::transmute(self.0) };
        // let _box = unsafe { Box::from_raw(ptr) };

        Ok(())
    }
}

// to allow converting a boxed device handle to a native device handle
impl CReprOf<Box<DeviceHandle>> for NativeDeviceHandle {
    fn c_repr_of(input: Box<DeviceHandle>) -> Result<Self, ffi_convert::CReprOfError> {
        let raw = Box::into_raw(input);
        // SAFETY we transmute it back to a DeviceHandle pointer
        let opaque = unsafe { mem::transmute(raw) };

        Ok(Self(opaque))
    }
}

// to allow getting a owned DeviceHandle from a pointer
// NOTE this is unsafe since we don't know who is currently accessing the handle
impl AsRust<Box<DeviceHandle>> for NativeDeviceHandle {
    fn as_rust(&self) -> Result<Box<DeviceHandle>, ffi_convert::AsRustError> {
        // SAFETY we transmute it back to a DeviceHandle pointer
        let concrete: *mut DeviceHandle = unsafe { mem::transmute(self.0) };
        let owned = unsafe { Box::from_raw(concrete) };

        Ok(owned)
    }
}

impl BorrowAsRust<DeviceHandle> for NativeDeviceHandle {
    fn borrow_as_rust<'a>(self) -> Result<&'a DeviceHandle, UnexpectedNullPointerError> {
        // SAFETY we transmute it back to a DeviceHandle pointer
        let ptr = NonNull::new(self.0).ok_or(UnexpectedNullPointerError)?;
        let ptr: NonNull<DeviceHandle> = unsafe { mem::transmute(ptr) };

        Ok(unsafe { ptr.as_ref() })
    }
}

struct InnerDeviceData {
    rt: Runtime,
    client: astarte_device_sdk::client::DeviceClient<Mqtt<MemoryStore, PairingApi>>,
    loop_handle: task::JoinHandle<()>,
}

pub struct DeviceHandle {
    inner: RwLock<Option<InnerDeviceData>>,
}

impl DeviceHandle {
    fn new(
        rt: Runtime,
        client: astarte_device_sdk::client::DeviceClient<Mqtt<MemoryStore, PairingApi>>,
        loop_handle: task::JoinHandle<()>,
    ) -> Self {
        let inner = InnerDeviceData {
            rt,
            client,
            loop_handle,
        };

        Self {
            inner: RwLock::new(Some(inner)),
        }
    }

    // blocking function call to create device handle
    pub fn connect<F>(config: NativeDeviceConfig, loop_end: F) -> eyre::Result<Box<DeviceHandle>>
    where
        F: FnOnce(Result<(), astarte_device_sdk::Error>) + Send + 'static,
    {
        let config = config.as_rust()?;

        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let result: eyre::Result<_> = rt.block_on(async move {
            let (client, connection) = Self::mk_device(config).await?;

            let loop_handle = Self::spawn_eventloop(connection, loop_end);

            Ok((loop_handle, client))
        });

        let (loop_handle, client) = result?;

        let device_handle = Box::new(Self::new(rt, client, loop_handle));

        Ok(device_handle)
    }

    fn spawn_eventloop<C, F>(connection: C, loop_end: F) -> JoinHandle<()>
    where
        C: EventLoop + Send + 'static,
        F: FnOnce(Result<(), astarte_device_sdk::Error>) + Send + 'static,
    {
        // tokio::task::spawn_blocking(move || {
        //     let rt = tokio::runtime::Handle::current();

        //     let result = rt
        //         .block_on(connection.handle_events())
        //         .inspect_err(|error| error!(%error, "help plz"));

        //     loop_end(result);
        // })
        tokio::spawn(async move {
            let result = connection.handle_events().await;

            tokio::task::block_in_place(move || {
                loop_end(result);
            });
        })
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

    fn inner_ref(&self) -> eyre::Result<RwLockReadGuard<'_, Option<InnerDeviceData>>> {
        let Ok(read) = self.inner.try_read() else {
            bail!("can't take write lock, already locked somewhere, retry");
        };

        Ok(read)
    }

    // pub fn send<F>(handle: NativeDeviceHandle, sent: F, data: ())
    // where
    //     F: FnOnce(Result<(), astarte_device_sdk::Error>) + Send + 'static,
    // {
    //     let handle = handle.borrow_as_rust().wrap_err("can't borrow device")?;

    //     let inner = handle.inner_ref()?;
    //     let inner = inner.as_ref().ok_or(eyre!("already disconnected"))?;
    // }
    //

    // fn receive_check() ->  {

    // }

    pub fn receive<F>(handle: NativeDeviceHandle, received: F)
    where
        F: FnOnce(eyre::Result<DeviceEvent>) + Send + 'static,
    {
        let guard = handle
            .borrow_as_rust()
            .wrap_err("can't borrow device")
            .and_then(|h| h.inner_ref());

        let guard = match guard {
            Ok(g) => g,
            Err(e) => {
                received(Err(e));
                return;
            }
        };

        let inner = match guard.as_ref().ok_or(eyre!("already disconnected")) {
            Ok(i) => i,
            Err(e) => {
                received(Err(e));
                return;
            }
        };

        let InnerDeviceData { rt, client, .. } = inner;

        let client = client.clone();

        rt.spawn(async move {
            let event = client
                .recv()
                .await
                .wrap_err("error while receiving message");
            // .and_then(|e| {
            //     NativeDeviceEvent::c_repr_of(e)
            //         .wrap_err("can't make device event ffi compatible")
            // });

            received(event);
        });
    }

    pub fn send_individual<F>(
        handle: NativeDeviceHandle,
        send_data: *const NativeIndividualSend,
        sent: F,
    ) where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let guard = handle
            .borrow_as_rust()
            .wrap_err("can't borrow device")
            .and_then(|h| h.inner_ref());

        let guard = match guard {
            Ok(g) => g,
            Err(e) => {
                sent(Err(e));
                return;
            }
        };

        let inner = match guard.as_ref().ok_or(eyre!("already disconnected")) {
            Ok(i) => i,
            Err(e) => {
                sent(Err(e));
                return;
            }
        };

        let InnerDeviceData { rt, client, .. } = inner;

        let send_data = unsafe {
            send_data
                .as_ref()
                .ok_or_eyre("send data is required to be non null and valid")
        };
        let send_data = match send_data {
            Ok(s) => s,
            Err(e) => {
                error!(%e, "data reference error");
                sent(Err(e));
                return;
            }
        };

        // NOTE this clones the data
        let send_data = match send_data
            .as_rust()
            .wrap_err("error in c to rust conversion")
        {
            Ok(i) => i,
            Err(error) => {
                error!(%error, "conversion errror");
                sent(Err(error));
                return;
            }
        };

        debug!(?send_data, "individual data received");

        let mut client = client.clone();

        rt.spawn(async move {
            let IndividualSend {
                interface,
                path,
                data,
                timestamp,
            } = send_data;

            let result = if let Some(timestamp) = timestamp {
                client
                    .send_individual_with_timestamp(&interface, &path, data, timestamp)
                    .await
            } else {
                client.send_individual(&interface, &path, data).await
            };

            info!(?result, "individual data sent");

            sent(result.wrap_err("error while sending individual"));
        });
    }

    // fn send_data(handle: NativeDeviceHandle) {
    //     let handle = handle.borrow_as_rust().wrap_err("can't borrow device")?;
    // }

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

    fn into_inner(handle: NativeDeviceHandle) -> eyre::Result<InnerDeviceData> {
        let handle = handle.borrow_as_rust().wrap_err("can't borrow device")?;

        let Ok(mut write) = handle.inner.try_write() else {
            bail!("can't take write lock, already locked somewhere, retry");
        };

        write.take().ok_or_eyre("already disconnected")
    }

    // this does not free the handle that has to be freed after using [`free`]
    pub fn disconnect(handle: NativeDeviceHandle) -> eyre::Result<()> {
        let inner = Self::into_inner(handle)?;

        let InnerDeviceData {
            rt,
            mut client,
            loop_handle,
        } = inner;

        // info!("spawning disconnect task");
        // rt.spawn_blocking(move || {
        //     let rt = tokio::runtime::Handle::current();

        //     let result = rt.block_on(async move {
        //         info!("disconnecting");

        //         client
        //             .disconnect()
        //             .await
        //             .wrap_err("cannot disconnect client")?;

        //         info!("disconnected");

        //         loop_handle.await.wrap_err("error while joining task")?;

        //         info!("joined loop");

        //         Ok(())
        //     });

        //     disconnect_cbk(result);
        // });

        let result = rt.block_on(async move {
            info!("disconnecting");

            client
                .disconnect()
                .await
                .wrap_err("cannot disconnect client")?;

            info!("disconnected");

            loop_handle.await.wrap_err("error while joining task")?;

            info!("joined loop");

            Ok(())
        });

        info!("shutting down runtime");
        rt.shutdown_background();

        result
    }

    pub fn free(handle: NativeDeviceHandle) -> eyre::Result<()> {
        handle
            .as_rust()
            .map(drop)
            .wrap_err("error while dropping handle")
    }
}

// pub type DeviceHandleReceiveCallback =
//     extern "C" fn(event: StringResult<ReceiveEvent>, user_data: UserData);

// pub type DeviceHandleSendCallback = extern "C" fn(result: StringResult<()>, user_data: UserData);

// pub type DeviceHandleDisconnectCallback =
//     extern "C" fn(result: StringResult<()>, user_data: UserData);

// pub struct SendValueCommand {
//     interface: String,
//     path: String,
//     value: SendData,
//     callback: DeviceHandleSendCallback,
//     user_data: UserData,
// }

// pub struct PollCommand {
//     callback: DeviceHandleReceiveCallback,
//     user_data: UserData,
// }

// pub struct DisconnectCommand {
//     callback: DeviceHandleDisconnectCallback,
//     user_data: UserData,
// }

// impl DisconnectCommand {
//     pub fn new(callback: DeviceHandleDisconnectCallback, user_data: UserData) -> Self {
//         Self {
//             callback,
//             user_data,
//         }
//     }
// }

// pub enum DeviceCommand {
//     SendValue(SendValueCommand),
//     Poll(PollCommand),
//     Disconnect(DisconnectCommand),
// }

// pub struct ReceiveEvent {
//     data: (),
// }
// pub struct SendData {
//     data: (),
// }
