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
    device::{DeviceHandle, NativeDeviceHandle, OpaqueDeviceHadle},
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
            Err(e) => {
                let report_string = format!("{e:?}");

                Self::Err(StaticString::c_repr_of(report_string)?)
            }
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

pub type DeviceHandleConnectCallback =
    extern "C" fn(result: *const NativeStringResult<NativeDeviceHandle>, user_data: UserData);

pub type DeviceHandleLoopCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

// #[unsafe(no_mangle)]
// pub extern "C" fn test_free() {
//     let err = eyre::eyre!("this is a test error");

//     let result: eyre::Result<bool> = Err(err);

//     let string_res = NativeStringResult::<bool>::c_repr_of(result).unwrap();

//     println!("{:?}", string_res);
// }

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_init() ->  {
    
}

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_connect(
    config: NativeDeviceConfig,
    connect_cbk: DeviceHandleConnectCallback,
    connect_user_data: UserData,
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

    let loop_end = move |result: Result<(), astarte_device_sdk::Error>| {
        let result: eyre::Result<bool> = result.wrap_err("error in handle_events").map(|_| true);

        let c_res = NativeStringResult::c_repr_of(result).unwrap();

        loop_cbk(&c_res, loop_user_data);
    };

    thread::spawn(move || {
        let result = DeviceHandle::connect(config, loop_end);

        let c_res = NativeStringResult::c_repr_of(result).unwrap();

        connect_cbk(&c_res, connect_user_data);
    });
}

// this function just frees the box associated with the handle but does not disconnect directly the device
#[unsafe(no_mangle)]
pub extern "C" fn device_handle_free(handle: NativeDeviceHandle) {
    if let Err(e) = DeviceHandle::free(handle) {
        error!("{e:#}");
    }
}

pub type DeviceHandleDisconnectCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_disconnect(
    handle: NativeDeviceHandle,
    disconnect_cbk: DeviceHandleDisconnectCallback,
    user_data: UserData,
) {
    thread::spawn(move || {
        let result = DeviceHandle::disconnect(handle).map(|_| true);

        let c_res = NativeStringResult::c_repr_of(result).unwrap();

        disconnect_cbk(&c_res, user_data);
    });
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
    let cbk = move |res| {
        let c_res = NativeStringResult::c_repr_of(res).unwrap();

        callback(&c_res, user_data);
    };

    DeviceHandle::receive(device_handle, cbk);
}

// NOTE since device event could contain a lot of data we avoid copying it when over to foreign functions
#[unsafe(no_mangle)]
pub extern "C" fn device_client_free_device_event(mut event: NativeDeviceEvent) {
    event.do_drop().unwrap()
}

pub type DeviceHandleSendCallback =
    extern "C" fn(result: *const NativeStringResult<bool>, user_data: UserData);

#[unsafe(no_mangle)]
pub extern "C" fn device_client_send_individual(
    device_handle: NativeDeviceHandle,
    data: *const NativeIndividualSend,
    callback: DeviceHandleSendCallback,
    user_data: UserData,
) {
    let cbk = move |res: eyre::Result<()>| {
        let c_res = NativeStringResult::c_repr_of(res.map(|_| true)).unwrap();

        callback(&c_res, user_data);
    };

    DeviceHandle::send_individual(device_handle, data, cbk);
}

#[unsafe(no_mangle)]
pub extern "C" fn device_client_send_object(
    device_handle: NativeDeviceHandle,
    data: *const NativeObjectSend,
    callback: DeviceHandleSendCallback,
    user_data: UserData,
) {
    let cbk = move |res: eyre::Result<()>| {
        let c_res = NativeStringResult::c_repr_of(res.map(|_| true)).unwrap();

        callback(&c_res, user_data);
    };

    DeviceHandle::send_object(device_handle, data, cbk);
}
