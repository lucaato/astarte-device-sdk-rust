use std::{
    ffi::{CString, NulError},
    mem,
    ptr::NonNull,
    str::FromStr,
    sync::Arc,
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
    sync::RwLock,
    task::{self, JoinHandle},
};
use tracing::{debug, error, info};
use url::Url;

use crate::{
    BorrowAsRust,
    config::{DeviceConfig, NativeDeviceConfig},
    data::{IndividualSend, NativeIndividualSend, NativeObjectSend, ObjectSend},
};

macro_rules! ok_or_call {
    ($expr:expr, $callback:ident) => {
        match $expr {
            Ok(h) => h,
            Err(e) => {
                $callback(Err(e));
                return;
            }
        }
    };
}

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
    pub fn new() -> eyre::Result<Self> {
        let handle = Box::new(DeviceRuntimeHandle::new()?);

        Self::c_repr_of(handle).wrap_err("can't construct native handle")
    }

    pub fn connect<CF, EF>(self, config: NativeDeviceConfig, connected: CF, exited: EF)
    where
        CF: FnOnce(eyre::Result<()>) + Send + 'static,
        EF: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let handle = ok_or_call!(
            self.borrow_as_rust().wrap_err("can't borrow native handle"),
            connected
        );

        let config = ok_or_call!(config.as_rust().wrap_err("can't convert config"), connected);

        handle.connect(config, connected, exited);
    }

    pub fn receive<F>(self, received: F)
    where
        F: FnOnce(eyre::Result<DeviceEvent>) + Send + 'static,
    {
        let handle = ok_or_call!(
            self.borrow_as_rust().wrap_err("can't borrow native handle"),
            received
        );

        handle.receive(received);
    }

    pub fn send_individual<F>(self, individual: *const NativeIndividualSend, sent: F)
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let handle = ok_or_call!(
            self.borrow_as_rust().wrap_err("can't borrow native handle"),
            sent
        );

        // SAFETY: the pointer has to be valid for the complete duration of the function
        let individual = unsafe { individual.as_ref() }
            .ok_or_eyre("send data is required to be non null and valid")
            .and_then(|i| i.as_rust().wrap_err("can't convert individual"));

        let individual = ok_or_call!(individual, sent);

        handle.send_individual(individual, sent);
    }

    pub fn send_object<F>(self, object: *const NativeObjectSend, sent: F)
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let handle = ok_or_call!(
            self.borrow_as_rust().wrap_err("can't borrow native handle"),
            sent
        );

        // SAFETY: the pointer has to be valid for the complete duration of the function
        let object = unsafe { object.as_ref() }
            .ok_or_eyre("send data is required to be non null and valid")
            .and_then(|o| o.as_rust().wrap_err("can't convert object"));

        let object = ok_or_call!(object, sent);

        handle.send_object(object, sent);
    }

    pub fn disconnect<F>(self, disconnected: F)
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let handle = ok_or_call!(
            self.borrow_as_rust().wrap_err("can't borrow native handle"),
            disconnected
        );

        handle.disconnect(disconnected);
    }

    pub fn free(self) -> eyre::Result<()> {
        self.as_rust()
            .map(drop)
            .wrap_err("error while dropping handle")
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
impl CReprOf<Box<DeviceRuntimeHandle>> for NativeDeviceHandle {
    fn c_repr_of(input: Box<DeviceRuntimeHandle>) -> Result<Self, ffi_convert::CReprOfError> {
        let raw = Box::into_raw(input);
        // SAFETY we transmute it back to a DeviceHandle pointer
        let opaque = unsafe { mem::transmute(raw) };

        Ok(Self(opaque))
    }
}

// to allow getting a owned DeviceHandle from a pointer
// NOTE this is unsafe since we don't know who is currently accessing the handle
impl AsRust<Box<DeviceRuntimeHandle>> for NativeDeviceHandle {
    fn as_rust(&self) -> Result<Box<DeviceRuntimeHandle>, ffi_convert::AsRustError> {
        // SAFETY we transmute it back to a DeviceHandle pointer
        let concrete: *mut DeviceRuntimeHandle = unsafe { mem::transmute(self.0) };
        let owned = unsafe { Box::from_raw(concrete) };

        Ok(owned)
    }
}

impl BorrowAsRust<DeviceRuntimeHandle> for NativeDeviceHandle {
    fn borrow_as_rust<'a>(self) -> Result<&'a DeviceRuntimeHandle, UnexpectedNullPointerError> {
        // SAFETY we transmute it back to a DeviceHandle pointer
        let ptr = NonNull::new(self.0).ok_or(UnexpectedNullPointerError)?;
        let ptr: NonNull<DeviceRuntimeHandle> = unsafe { mem::transmute(ptr) };

        Ok(unsafe { ptr.as_ref() })
    }
}

struct DeviceClientData {
    client: astarte_device_sdk::client::DeviceClient<Mqtt<MemoryStore, PairingApi>>,
    loop_handle: task::JoinHandle<()>,
}

impl DeviceClientData {
    pub(crate) async fn connect<F>(config: DeviceConfig, exited: F) -> eyre::Result<Self>
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let (client, connection) = Self::mk_device(config).await?;

        let loop_handle = tokio::spawn(async move {
            let result = connection
                .handle_events()
                .await
                .wrap_err("handle events error");

            tokio::task::block_in_place(move || {
                exited(result);
            });
        });

        Ok(Self {
            client,
            loop_handle,
        })
    }

    pub(crate) async fn receive(&self) -> eyre::Result<DeviceEvent> {
        self.client.recv().await.wrap_err("can't receive event")
    }

    pub(crate) async fn send_individual(&self, individual: IndividualSend) -> eyre::Result<()> {
        let IndividualSend {
            interface,
            path,
            data,
            timestamp,
        } = individual;

        let mut client = self.client.clone();

        let result = if let Some(timestamp) = timestamp {
            client
                .send_individual_with_timestamp(&interface, &path, data, timestamp)
                .await
        } else {
            client.send_individual(&interface, &path, data).await
        };

        result.wrap_err("can't send individual")
    }

    pub(crate) async fn send_object(&self, object: ObjectSend) -> eyre::Result<()> {
        let ObjectSend {
            interface,
            path,
            data,
            timestamp,
        } = object;

        let mut client = self.client.clone();

        let result = if let Some(timestamp) = timestamp {
            client
                .send_object_with_timestamp(&interface, &path, data, timestamp)
                .await
        } else {
            client.send_object(&interface, &path, data).await
        };

        result.wrap_err("can't send object")
    }

    pub(crate) async fn disconnect(mut self) -> eyre::Result<()> {
        self.client.disconnect().await?;

        self.loop_handle.await.wrap_err("can't join handle_events")
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
}

pub struct DeviceRuntimeHandle {
    rt: Runtime,
    inner: Arc<RwLock<Option<DeviceClientData>>>,
}

impl DeviceRuntimeHandle {
    pub fn new() -> eyre::Result<DeviceRuntimeHandle> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let inner = Arc::new(RwLock::new(None));

        Ok(Self { rt, inner })
    }

    // blocking function call to create device handle
    pub fn connect<CF, EF>(&self, config: DeviceConfig, connected: CF, exited: EF)
    where
        CF: FnOnce(eyre::Result<()>) + Send + 'static,
        EF: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        // NOTE if we want to avoid this arc we could spawn a thread and make this function blocking ?
        let inner = Arc::clone(&self.inner);

        self.rt.spawn(async move {
            let mut inner = inner.write().await;

            if inner.is_some() {
                connected(Err(eyre::eyre!("device already configured")));
                return;
            }

            let client = match DeviceClientData::connect(config, exited).await {
                Ok(d) => d,
                Err(e) => {
                    connected(Err(e));
                    return;
                }
            };

            *inner = Some(client);

            connected(Ok(()));
        });
    }

    pub fn receive<F>(&self, received: F)
    where
        F: FnOnce(eyre::Result<DeviceEvent>) + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);

        self.rt.spawn(async move {
            let client = inner.read().await;
            let client = ok_or_call!(client.as_ref().ok_or_eyre("client not connected"), received);

            let result = client.receive().await;

            received(result);
        });
    }

    pub fn send_individual<F>(&self, individual: IndividualSend, sent: F)
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);

        self.rt.spawn(async move {
            let client = inner.read().await;
            let client = ok_or_call!(client.as_ref().ok_or_eyre("client not connected"), sent);

            let result = client.send_individual(individual).await;

            sent(result);
        });
    }

    pub fn send_object<F>(&self, object: ObjectSend, sent: F)
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);

        self.rt.spawn(async move {
            let client = inner.read().await;
            let client = ok_or_call!(client.as_ref().ok_or_eyre("client not connected"), sent);

            let result = client.send_object(object).await;

            sent(result);
        });
    }

    fn into_inner(handle: NativeDeviceHandle) -> eyre::Result<DeviceClientData> {
        let handle = handle.borrow_as_rust().wrap_err("can't borrow device")?;

        let Ok(mut write) = handle.inner.try_write() else {
            bail!("can't take write lock, already locked somewhere, retry");
        };

        write.take().ok_or_eyre("already disconnected")
    }

    // this does not free the handle that has to be freed after using [`free`]
    pub fn disconnect<F>(&self, disconnected: F)
    where
        F: FnOnce(eyre::Result<()>) + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);

        self.rt.spawn(async move {
            let mut client = inner.write().await;
            let client = ok_or_call!(
                client.take().ok_or_eyre("client not connected"),
                disconnected
            );

            let result = client.disconnect().await;
            disconnected(result);
        });
    }
}
