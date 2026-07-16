use std::{
    ffi::{CString, NulError, c_char, c_void},
    fmt::Pointer,
    mem::ManuallyDrop,
    ptr::NonNull,
    thread,
};

use eyre::Context;
use ffi_convert::{AsRust, CDrop, CReprOf, CReprOfError, UnexpectedNullPointerError};
use tracing::{error, info, level_filters::LevelFilter};

use crate::{
    config::NativeDeviceConfig,
    data::{NativeDeviceData, NativeDeviceEvent, NativeIndividualSend, NativeObjectSend},
    device::{DeviceRuntimeHandle, NativeDeviceHandle, OpaqueDeviceHadle},
};

pub mod config;
pub mod data;
pub mod device;

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct UserData(*mut c_void);
unsafe impl Send for UserData {}

#[repr(transparent)]
#[derive(Debug, Clone)]
pub struct StaticString(*const c_char);
unsafe impl Send for StaticString {}

impl CDrop for StaticString {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        use ffi_convert::RawPointerConverter;

        unsafe { std::ffi::CString::drop_raw_pointer(self.0)? };

        Ok(())
    }
}

impl CReprOf<String> for StaticString {
    fn c_repr_of(input: String) -> Result<Self, CReprOfError> {
        let leaked = CString::new(input)?.into_raw();

        Ok(Self(leaked as *const c_char))
    }
}

impl Drop for StaticString {
    fn drop(&mut self) {
        let _ = self.do_drop();
    }
}

#[repr(C)]
#[derive(Debug, Clone)]
pub enum NativeOption<T> {
    Some(T),
    None,
}

impl<T> CDrop for NativeOption<T> {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        Ok(())
    }
}

impl<T, U: CReprOf<T>> CReprOf<Option<T>> for NativeOption<U> {
    fn c_repr_of(input: Option<T>) -> Result<Self, CReprOfError> {
        let nat = match input {
            Some(u) => Self::Some(U::c_repr_of(u)?),
            None => Self::None,
        };

        Ok(nat)
    }
}

impl<T: AsRust<U>, U> AsRust<Option<U>> for NativeOption<T> {
    fn as_rust(&self) -> Result<Option<U>, ffi_convert::AsRustError> {
        let opt = match self {
            NativeOption::Some(t) => Some(t.as_rust()?),
            NativeOption::None => None,
        };

        Ok(opt)
    }
}

#[repr(transparent)]
pub struct NativeManuallyDrop<T>(ManuallyDrop<T>);

impl<T> NativeManuallyDrop<T> {
    pub fn new(inner: T) -> Self {
        Self(ManuallyDrop::new(inner))
    }
}

impl<T> CDrop for NativeManuallyDrop<T> {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        // do not drop anything
        Ok(())
    }
}

impl<T, U: CDrop + CReprOf<T>> CReprOf<T> for NativeManuallyDrop<U> {
    fn c_repr_of(input: T) -> Result<Self, CReprOfError> {
        let native = Self::new(U::c_repr_of(input)?);

        Ok(native)
    }
}

#[repr(C)]
#[derive(Debug)]
pub enum NativeStringResult<T> {
    Ok(T),
    Err(StaticString),
}

impl<T> NativeStringResult<T> {
    fn err_const_str(e: eyre::Report) -> Result<StaticString, CReprOfError> {
        let report_string = format!("{e:?}");

        StaticString::c_repr_of(report_string)
    }

    pub(crate) fn from_report(result: eyre::Result<T>) -> Result<Self, CReprOfError> {
        let native = match result {
            Ok(o) => Self::Ok(o),
            Err(e) => Self::Err(Self::err_const_str(e)?),
        };

        Ok(native)
    }
}

impl<T> CDrop for NativeStringResult<T> {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        // fields are automatically dropped by rust drop glue
        Ok(())
    }
}

impl<T, U: CDrop + CReprOf<T>> CReprOf<eyre::Result<T>> for NativeStringResult<U> {
    fn c_repr_of(input: eyre::Result<T>) -> Result<Self, CReprOfError> {
        let native = match input {
            Ok(o) => Self::Ok(U::c_repr_of(o)?),
            Err(e) => Self::Err(Self::err_const_str(e)?),
        };

        Ok(native)
    }
}

pub trait BorrowAsRust<T> {
    fn borrow_as_rust<'a>(self) -> Result<&'a T, UnexpectedNullPointerError>;
}

fn init_tracing() {
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    tracing_subscriber::registry()
        // .with(console_subscriber::spawn())
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive("astarte_device_sdk=debug".parse().unwrap())
                .from_env_lossy()
                // .add_directive("tokio=trace".parse().unwrap())
                // .add_directive("runtime=trace".parse().unwrap())
                .add_directive(LevelFilter::INFO.into()),
        )
        .try_init()
        .unwrap();
}

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_init() -> NativeDeviceHandle {
    // NOTE if we want the result here we need to free in the caller
    let handle_result = NativeDeviceHandle::new().unwrap();
    // let native_result = NativeStringResult::from_report(handle_result).unwrap();
    // native_result
    handle_result
}

pub type DeviceHandleBuildCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

pub type DeviceHandleLoopCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

// NOTE this function is called connect but after the callback is called we are still not connected
// this could result in surprising behaviour in case of non stored datastream send which would get dropped
#[unsafe(no_mangle)]
pub extern "C" fn device_handle_connect(
    handle: NativeDeviceHandle,
    config: NativeDeviceConfig,
    build_cbk: DeviceHandleBuildCallback,
    build_user_data: UserData,
    loop_cbk: DeviceHandleLoopCallback,
    loop_user_data: UserData,
) {
    // FIXME remove this
    // console_subscriber::init();
    color_eyre::install().unwrap();
    init_tracing();
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| eyre::eyre!("couldn't install default crypto provider"))
        .unwrap();
    // FIXME remove this end

    let connected = move |result: eyre::Result<()>| {
        let result: eyre::Result<bool> =
            result.wrap_err("error while building client").map(|_| true);

        let c_res = NativeStringResult::c_repr_of(result).unwrap();

        build_cbk(&c_res, build_user_data);
    };

    let exited = move |result: eyre::Result<()>| {
        let result: eyre::Result<bool> = result.wrap_err("error in handle_events").map(|_| true);

        let c_res = NativeStringResult::c_repr_of(result).unwrap();

        loop_cbk(&c_res, loop_user_data);
    };

    handle.connect(config, connected, exited);
}

pub type DeviceHandleReceiveCallback = extern "C" fn(
    result: *const NativeStringResult<NativeManuallyDrop<NativeDeviceEvent>>,
    user_data: UserData,
);

#[unsafe(no_mangle)]
pub extern "C" fn device_client_receive(
    device_handle: NativeDeviceHandle,
    callback: DeviceHandleReceiveCallback,
    user_data: UserData,
) {
    let received = move |res| {
        let c_res = NativeStringResult::c_repr_of(res).unwrap();

        callback(&c_res, user_data);
    };

    device_handle.receive(received);
}

// NOTE since device event could contain a lot of data we avoid copying it when over to foreign functions
// this has to be called by calling code
#[unsafe(no_mangle)]
pub extern "C" fn device_handle_free_device_event(mut event: NativeDeviceEvent) {
    if let Err(e) = event.do_drop() {
        error!("{e:#}");
    }
}

pub type DeviceHandleSendCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_send_individual(
    device_handle: NativeDeviceHandle,
    data: *const NativeIndividualSend,
    callback: DeviceHandleSendCallback,
    user_data: UserData,
) {
    let sent = move |res: eyre::Result<()>| {
        let c_res = NativeStringResult::c_repr_of(res.map(|_| true)).unwrap();

        callback(&c_res, user_data);
    };

    device_handle.send_individual(data, sent);
}

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_send_object(
    device_handle: NativeDeviceHandle,
    data: *const NativeObjectSend,
    callback: DeviceHandleSendCallback,
    user_data: UserData,
) {
    let sent = move |res: eyre::Result<()>| {
        let c_res = NativeStringResult::c_repr_of(res.map(|_| true)).unwrap();

        callback(&c_res, user_data);
    };

    device_handle.send_object(data, sent);
}

pub type DeviceHandleDisconnectCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_disconnect(
    handle: NativeDeviceHandle,
    disconnect_cbk: DeviceHandleDisconnectCallback,
    user_data: UserData,
) {
    let disconnected = move |res: eyre::Result<()>| {
        let c_res = NativeStringResult::c_repr_of(res.map(|_| true)).unwrap();

        disconnect_cbk(&c_res, user_data);
    };

    handle.disconnect(disconnected);
}

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_free(handle: NativeDeviceHandle) {
    if let Err(e) = handle.free() {
        error!("{e:#}");
    }
}
