use std::{
    ffi::{CString, NulError, c_char, c_void},
    ptr::NonNull,
};

use ffi_convert::{CDrop, CReprOf};

use crate::{
    config::NativeDeviceConfig,
    device::{DeviceHandle, NativeDeviceHandle},
};

pub mod config;
pub mod device;

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct UserData(*mut c_void);
unsafe impl Send for UserData {}

#[repr(transparent)]
#[derive(Debug, Clone, Copy)]
pub struct ErrorString(*const c_char);
unsafe impl Send for ErrorString {}

#[derive(Debug, Clone)]
pub enum StringResult<T> {
    Ok(T),
    Err(CString),
}

#[repr(C)]
pub enum NativeResult<T> {
    Ok(T),
    Err(ErrorString),
}

/// Borrows an ffi compatible struct from the input, the borrowed data does not have to be a reference
/// This is useful for example when calling callbacks,
/// in this case we know that the data needs to be cloned inside the callback>
/// We want to borrow the data in a C compatible way and then continue
/// in the rust side with the full ownership of the data.
pub trait BorrowCRepr<T> {
    fn borrow_raw(input: &T) -> Self;
}

pub trait BorrowAsRust<T> {
    fn unsafe_borrow_as_rust<'a>(input: NonNull<Self>) -> &'a T;
}

// impl<T, U> BorrowCRepr<Result<T, CString>> for NativeResult<U>
// where
//     U: BorrowCRepr<T>,
// {
//     fn borrow_raw(input: &Result<T, CString>) -> Self {
//         input
//     }
// }

impl<T> CDrop for NativeResult<T>
where
    T: CDrop,
{
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        match self {
            NativeResult::Ok(o) => o.do_drop(),
            NativeResult::Err(_e) => {
                // NOTE the error string is still owned by rust we don't need to drop it
                Ok(())
            }
        }
    }
}

impl<T, U> CReprOf<StringResult<T>> for NativeResult<U>
where
    U: CReprOf<T>,
{
    fn c_repr_of(input: StringResult<T>) -> Result<Self, ffi_convert::CReprOfError> {
        let res = match input {
            StringResult::Ok(o) => NativeResult::Ok(U::c_repr_of(o)?),
            StringResult::Err(e) => {
                let strptr = e.as_c_str().as_ptr();

                NativeResult::Err(ErrorString(strptr))
            }
        };

        Ok(res)
    }
}

// returns a owned rust struct that can be borrowed with BorrowCRepr
// this allows the caller to still have atuomatic drop of the resource
// but at the same time it can use BorrowCRepr::borrow to get a reference
// this is useful when calling c callbacks
trait CCompatibleType<T>: Sized {
    fn as_c_compat(input: T) -> Result<Self, NulError>;
}

impl CCompatibleType<bool> for bool {
    fn as_c_compat(input: bool) -> Result<Self, NulError> {
        Ok(input)
    }
}

impl CCompatibleType<String> for CString {
    fn as_c_compat(input: String) -> Result<Self, NulError> {
        CString::new(input)
    }
}

impl<T, U> CCompatibleType<eyre::Result<T>> for StringResult<U>
where
    U: CCompatibleType<T>,
{
    fn as_c_compat(input: eyre::Result<T>) -> Result<Self, NulError> {
        let ok = match input {
            Ok(o) => Self::Ok(U::as_c_compat(o)?),
            Err(e) => {
                let owned = CString::new(e.to_string())?;

                Self::Err(owned)
            }
        };

        Ok(ok)
    }
}

impl BorrowCRepr<bool> for bool {
    fn borrow_raw(input: &bool) -> Self {
        *input
    }
}

impl<T, U> BorrowCRepr<StringResult<T>> for NativeResult<U>
where
    U: BorrowCRepr<T>,
{
    fn borrow_raw(input: &StringResult<T>) -> Self {
        match input {
            StringResult::Ok(t) => Self::Ok(U::borrow_raw(t)),
            StringResult::Err(cstring) => {
                let str = cstring.as_c_str();
                let ptr = str.as_ptr();

                Self::Err(ErrorString(ptr))
            }
        }
    }
}

// impl From<Result<T, E>> for NativeResult<T>
// where
//     E: std::error::Error,
// {
//     fn from(value: Result<T, E>) -> Self {
//         match value {
//             Ok(v) => Self::Ok(v),
//             Err(e) => Self::Err(e.to_string()),
//         }
//     }
// }

pub type DeviceHandleConnectCallback =
    extern "C" fn(result: *const NativeResult<NativeDeviceHandle>, user_data: UserData);

#[unsafe(no_mangle)]
pub extern "C" fn device_handle_connect(
    config: NativeDeviceConfig,
    connect_cbk: DeviceHandleConnectCallback,
    connect_user_data: UserData,
    loop_cbk: DeviceHandleLoopCallback,
    loop_user_data: UserData,
) {
    let result = DeviceHandle::connect(
        config,
        connect_cbk,
        connect_user_data,
        loop_cbk,
        loop_user_data,
    );

    result.unwrap();
}

pub type DeviceHandleLoopCallback =
    extern "C" fn(result: *const NativeResult<bool>, user_data: UserData);

// blocking function that waits for the device and runtime to stop
// WARN this invalidates the device_handle pointer, it's only safe to use when we know the wrapper object won't be accessed no more
// #[unsafe(no_mangle)]
// pub extern "C" fn device_handle_stop(handle: NativeDeviceHandle) {
//     let result = DeviceHandle::disconnect(handle.as_rust().unwrap());

//     if let Err(error) = result {
//         error!(%error, "error when disconnecting device");
//     }
// }

// WARN this consumes the device so the pointer won't be usable afterward
// call this only when the target language won't ever use this again like
// when the garbage collector is cleaning up the object
// #[unsafe(no_mangle)]
// pub extern "C" fn device_client_stop(
//     device_handle: NativeDeviceHandle,
//     callback: DeviceHandleDisconnectCallback,
//     user_data: UserData,
// ) {
//     let boxed_device = device_handle.as_rust().unwrap();

//     boxed_device
//         .tx
//         .blocking_send(DeviceCommand::Disconnect(DisconnectCommand::new(callback, user_data))
//         .unwrap();

//     // change this to a callback
//     let res = rx.blocking_recv().unwrap();

//     println!("rust: {res:?}");

//     boxed_device.runtime_thread.join().unwrap();

//     println!("rust: thread joined");
// }

// Define callback types for future use
// pub type AstarteDeviceConnectionCallback = extern "C" fn(user_data: *mut c_void);
// pub type AstarteDeviceDisconnectionCallback = extern "C" fn(user_data: *mut c_void);

// #[derive(Debug)]
// pub struct NativeDeviceHandle {
//     tx: mpsc::Sender<InternalDeviceCommand>,
//     runtime_thread: JoinHandle<()>,
// }

/*
#[repr(C)]
#[derive(Debug, Clone)]
pub enum AstarteDataFfi {
    Double(f64),
    Integer(i32),
    Boolean(bool),
    LongInteger(i64),
    String(*const c_char),
    BinaryBlob(u8 *),
    DateTime(i64),
    DoubleArray(Vec<f64>),
    IntegerArray(Vec<i32>),
    BooleanArray(Vec<bool>),
    LongIntegerArray(Vec<i64>),
    StringArray(* *const c_char),
    BinaryBlobArray(Vec<Vec<u8>>),
    DateTimeArray(Vec<i64>),
}

#[unsafe(no_mangle)]
pub extern "C" fn device_test_data_ffi(value: AstarteDataFfi) -> u32 {
    0
}

#[repr(C)]
#[typeshare]
pub enum CMappingType {
    /// Double mapping.
    Double,
    /// Integer mapping.
    Integer,
    /// Boolean mapping.
    Boolean,
    /// Long integers mapping.
    LongInteger,
    /// String mapping.
    String,
    /// Binary mapping.
    BinaryBlob,
    /// Date time mapping.
    DateTime,
    /// Double array mapping.
    DoubleArray,
    /// Integer array mapping.
    IntegerArray,
    /// Boolean array mapping.
    BooleanArray,
    /// Long integer array mapping.
    LongIntegerArray,
    /// String array mapping.
    StringArray,
    /// Binary array mapping.
    BinaryBlobArray,
    /// Date time array mapping.
    DateTimeArray,
}

impl From<&AstarteData> for CMappingType {
    fn from(value: &AstarteData) -> Self {
        match value {
            AstarteData::Double(_) => CMappingType::Double,
            AstarteData::Integer(_) => CMappingType::Integer,
            AstarteData::Boolean(_) => CMappingType::Boolean,
            AstarteData::LongInteger(_) => CMappingType::LongInteger,
            AstarteData::String(_) => CMappingType::String,
            AstarteData::BinaryBlob(_) => CMappingType::BinaryBlob,
            AstarteData::DateTime(_) => CMappingType::DateTime,
            AstarteData::DoubleArray(_) => CMappingType::DoubleArray,
            AstarteData::IntegerArray(_) => CMappingType::IntegerArray,
            AstarteData::BooleanArray(_) => CMappingType::BooleanArray,
            AstarteData::LongIntegerArray(_) => CMappingType::LongIntegerArray,
            AstarteData::StringArray(_) => CMappingType::StringArray,
            AstarteData::BinaryBlobArray(_) => CMappingType::BinaryBlobArray,
            AstarteData::DateTimeArray(_) => CMappingType::DateTimeArray,
        }
    }
}

// no repr c since we only pass around opaque pointers
pub struct CAstarteData(AstarteData);

#[unsafe(no_mangle)]
pub extern "C" fn device_data_int(value: i32) -> *mut CAstarteData {
    Box::into_raw(Box::new(CAstarteData(AstarteData::Integer(value))))
}

#[unsafe(no_mangle)]
pub extern "C" fn device_data_longint(value: i64) -> *mut CAstarteData {
    Box::into_raw(Box::new(CAstarteData(AstarteData::LongInteger(value))))
}

#[repr(C)]
#[typeshare]
pub struct CValue;

#[unsafe(no_mangle)]
pub extern "C" fn device_client_start(config: *const CAstarteDeviceConfig) -> *mut CDeviceHandle {
    std::panic::set_hook(Box::new(|info| {
        println!("rust: panicing :'( => {info}");
    }));

    let (tx, mut rx) = mpsc::channel::<InternalDeviceCommand>(100);

    let config = unsafe { config.as_ref().unwrap() };
    // let connection_cbk = config.connection_cbk;
    // let disconnection_cbk = config.disconnection_cbk;
    // let datastream_individual_cbk = config.datastream_individual_cbk;
    // let datastream_object_cbk = config.datastream_object_cbk;
    // let property_set_cbk = config.property_set_cbk;
    // let property_unset_cbk = config.property_unset_cbk;
    // let cbk_user_data = config.cbk_user_data;

    let device_id = unsafe { CStr::from_ptr(config.device_id) }
        .to_string_lossy()
        .to_string();
    let credential = unsafe { CStr::from_ptr(config.cred_secr) }
        .to_string_lossy()
        .to_string();
    let realm = unsafe { CStr::from_ptr(config.realm) }
        .to_string_lossy()
        .to_string();
    let pairing_url = unsafe { CStr::from_ptr(config.pairing_url) }
        .to_string_lossy()
        .to_string();
    let interfaces_dir = unsafe { CStr::from_ptr(config.interfaces_dir) }
        .to_string_lossy()
        .to_string();

    println!("rust: Received parameters {device_id} {credential} {realm} {pairing_url}");

    let runtime_thread = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // device init
            let args = MqttArgs {
                realm: realm.clone(),
                device_id: device_id.clone(),
                credential: Credential::secret(credential),
                pairing_url: Url::from_str(&pairing_url).unwrap(),
            };

            let mqtt_config = MqttConfig::new(args).ignore_ssl_errors();

            let mut tmp_dir = std::env::temp_dir();

            tmp_dir.push("astarte-example-bindings");

            println!("rust: using {tmp_dir:?}");

            let (client, connection) = DeviceBuilder::new()
                .writable_dir(tmp_dir)
                .store(MemoryStore::new())
                .interface_directory(interfaces_dir)
                .unwrap()
                .connection(mqtt_config)
                .build()
                .await
                .unwrap();

            println!("rust: created client");

            let handle_events = tokio::task::spawn(async move {
                println!("rust: spawning handle events");

                connection.handle_events().await.unwrap();
            });

            while let Some(cmd) = rx.recv().await {
                match cmd {
                    InternalDeviceCommand::SendValue {
                        interface,
                        path,
                        value,
                        callback,
                        user_data,
                    } => {
                        println!("rust: spawning task to take care of the send");
                        let mut client = client.clone();

                        tokio::spawn(async move {
                            match value {
                                Value::Individual { data, timestamp } => {
                                    let _r = client
                                        .send_individual_with_timestamp(
                                            &interface, &path, data, timestamp,
                                        )
                                        .await;

                                    println!("rust: calling c callback");

                                    callback(unsafe { std::mem::transmute(user_data) })
                                }
                                Value::Object { data, timestamp } => {
                                    todo!()
                                }
                                Value::Property(astarte_data) => {
                                    todo!()
                                }
                            }
                        });
                    }
                    InternalDeviceCommand::Disconnect { tx } => {
                        let mut client = client.clone();

                        println!("rust: disconnecting device");

                        if let Err(e) = tx.send(client.disconnect().await) {
                            warn!(?e, "can't answer on one shot channel");
                        }

                        break;
                    }
                    InternalDeviceCommand::PollElement {
                        callback,
                        user_data,
                    } => {
                        let client = client.clone();
                        println!("rust: spawning task to take care of the poll");

                        tokio::spawn(async move {
                            let DeviceEvent {
                                interface,
                                path,
                                data,
                            } = client.recv().await.unwrap();

                            // NOTE this stuff needs to be freed
                            let cinterface = CString::new(interface).unwrap().into_raw();
                            let cpath = CString::new(path).unwrap().into_raw();
                            let cdata = Box::into_raw(Box::new(data));

                            callback(
                                cinterface,
                                cpath,
                                unsafe { std::mem::transmute(cdata) },
                                unsafe { std::mem::transmute(user_data) },
                            );
                        });
                    }
                }
            }

            handle_events.await.unwrap();
        });

        rt.shutdown_timeout(Duration::from_secs(5));
    });

    Box::into_raw(Box::new(CDeviceHandle { tx, runtime_thread }))
}

#[unsafe(no_mangle)]
pub extern "C" fn device_client_receive(
    device_handle: *mut CDeviceHandle,
    callback: AstarteDeviceReceiveCallback,
    user_data: *mut c_void,
) {
    let device = unsafe { device_handle.as_ref().unwrap() };

    println!("rust: sending poll element");
    device
        .tx
        .blocking_send(InternalDeviceCommand::PollElement {
            callback,
            user_data: UserData(user_data),
        })
        .unwrap();
    println!("rust: poll element sent");
}

#[unsafe(no_mangle)]
pub extern "C" fn device_client_send_individual(
    device_handle: *mut CDeviceHandle,
    interface_name: *const c_char,
    path: *const c_char,
    data: *mut CAstarteData,
    callback: AstarteDeviceSendCallback,
    user_data: *mut c_void,
) {
    let device = unsafe { device_handle.as_ref().unwrap() };

    let interface_name = unsafe { CStr::from_ptr(interface_name) }
        .to_string_lossy()
        .to_string();
    let path = unsafe { CStr::from_ptr(path) }
        .to_string_lossy()
        .to_string();

    let data = *unsafe { Box::from_raw(data) };

    println!("rust: sending element through channel");

    device
        .tx
        .blocking_send(InternalDeviceCommand::SendValue {
            interface: interface_name,
            path,
            value: Value::Individual {
                data: data.0,
                timestamp: Utc::now(),
            },
            callback,
            user_data: UserData(user_data),
        })
        .unwrap();

    println!("rust: element sent through channel");
}

#[unsafe(no_mangle)]
pub extern "C" fn device_client_stop(device_handle: *mut CDeviceHandle) {
    // WARN this consumes the device so the pointer won't be usable afterward
    let boxed_device = unsafe { Box::from_raw(device_handle) };
    let (tx, rx) = tokio::sync::oneshot::channel();

    boxed_device
        .tx
        .blocking_send(InternalDeviceCommand::Disconnect { tx })
        .unwrap();

    // change this to a callback
    let res = rx.blocking_recv().unwrap();

    println!("rust: {res:?}");

    boxed_device.runtime_thread.join().unwrap();

    println!("rust: thread joined");
}

// utilities

// ==========================================
// Event Information Getters
// ==========================================

/// Frees a string allocated by the Rust FFI.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn device_event_free_string(s: *mut std::ffi::c_char) {
    if !s.is_null() {
        let _ = unsafe { CString::from_raw(s) };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn device_event_free_value(s: *mut CValue) {
    if !s.is_null() {
        let _ = unsafe { Box::from_raw(std::mem::transmute::<_, *mut Value>(s)) };
    }
}

#[repr(C)]
pub enum CValueType {
    Individual,
    Object,
    PropertySet,
    PropertyUnset,
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn device_event_value_get_value_type(
    event: *const CValue,
    out_type: *mut CValueType,
) -> bool {
    if event.is_null() || out_type.is_null() {
        return false;
    }

    let ev = unsafe { &*std::mem::transmute::<_, *mut Value>(event) };

    unsafe {
        *out_type = match ev {
            Value::Individual { .. } => CValueType::Individual,
            Value::Object { .. } => CValueType::Object,
            Value::Property(Some(_)) => CValueType::PropertySet,
            Value::Property(None) => CValueType::PropertyUnset,
        }
    };

    true
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn device_event_value_get_data_type(
    event: *const CValue,
    out_type: *mut CMappingType,
) -> bool {
    if event.is_null() || out_type.is_null() {
        return false;
    }

    let ev = unsafe { &*std::mem::transmute::<_, *mut Value>(event) };

    let astarte_data = match ev {
        Value::Individual { data, .. } => data,
        Value::Property(Some(data)) => data,
        Value::Property(None) => return false, // No data to type-check
        Value::Object { .. } => todo!("Data type checking for Object is not yet implemented"),
    };

    // Assuming a conversion exists or you are working directly with `CAstarteData`
    unsafe { *out_type = CMappingType::from(astarte_data) };

    true
}

/// Example getter: Retrieves an integer value if the underlying data is an Integer.
/// Returns `true` if successful, `false` if the data was missing or of a different type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn device_event_value_get_integer(
    event: *const CValue,
    out_val: *mut i32,
) -> bool {
    if event.is_null() || out_val.is_null() {
        return false;
    }

    let ev = unsafe { &*std::mem::transmute::<_, *mut Value>(event) };

    let astarte_data = match ev {
        Value::Individual { data, .. } => data,
        Value::Property(Some(data)) => data,
        Value::Property(None) => return false,
        Value::Object { .. } => todo!("Integer retrieval for Object is not yet implemented"),
    };

    if let AstarteData::Integer(val) = astarte_data {
        unsafe {
            *out_val = *val;
        }
        true
    } else {
        false
    }
}
*/
