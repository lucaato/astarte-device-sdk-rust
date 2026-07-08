use std::ffi::{CString, c_char};

use astarte_device_sdk::{AstarteData, DeviceEvent, Value};
use ffi_convert::{CArray, CDrop, CReprOf, CStringArray};

/*
 * NOTE by using ffi_convert you should copy data when converting to rust and to ffi compatible structs
 * since we don't want that for events data (it could be large) we'll also have borrowed struct that won't be dropped by us but will be
 * handled by the calling language
 */

// ~~FIXME~~ this is actually the correct behaviour drop is called by rust for every sub object cool the CDrop derive does not call c_drop of underlying structs it seems so this need to be manual
#[repr(C)]
#[derive(CReprOf, CDrop, Debug)]
#[target_type(DeviceEvent)]
pub struct NativeDeviceEvent {
    pub interface: *const c_char,
    pub path: *const c_char,
    pub data: NativeValue,
}

#[repr(C)]
#[derive(CDrop, Debug)]
pub enum NativeValue {
    Individual {
        data: NativeDeviceData,
        timestamp: i64,
    },
    Object {
        data: CArray<NativeObjectEntry>,
        timestamp: i64,
    },
    PropertySet(NativeDeviceData),
    PropertyUnset,
}

impl CReprOf<Value> for NativeValue {
    fn c_repr_of(input: Value) -> Result<Self, ffi_convert::CReprOfError> {
        let native = match input {
            Value::Individual { data, timestamp } => Self::Individual {
                data: NativeDeviceData::c_repr_of(data)?,
                timestamp: timestamp.timestamp_millis(),
            },
            Value::Object { data, timestamp } => Self::Object {
                data: CArray::c_repr_of(data.into_vec())?,
                timestamp: timestamp.timestamp_millis(),
            },
            Value::Property(Some(data)) => Self::PropertySet(NativeDeviceData::c_repr_of(data)?),
            Value::Property(None) => Self::PropertyUnset,
        };

        Ok(native)
    }
}

#[repr(C)]
#[derive(CDrop, Debug)]
pub struct NativeObjectEntry {
    path: *const c_char,
    value: NativeDeviceData,
}

impl CReprOf<(String, AstarteData)> for NativeObjectEntry {
    fn c_repr_of((path, data): (String, AstarteData)) -> Result<Self, ffi_convert::CReprOfError> {
        let path = CString::c_repr_of(path)?.into_raw();
        let value = NativeDeviceData::c_repr_of(data)?;

        Ok(Self { path, value })
    }
}

// FIXME the CDrop implementation does not work for enum with fields
// so implement manually
#[repr(C)]
#[derive(CDrop, Debug)]
pub enum NativeDeviceData {
    Double(f64),
    Integer(i32),
    Boolean(bool),
    LongInteger(i64),
    String(*const c_char),
    BinaryBlob(CArray<u8>),
    DateTime(i64),
    DoubleArray(CArray<f64>),
    IntegerArray(CArray<i32>),
    BooleanArray(CArray<bool>),
    LongIntegerArray(CArray<i64>),
    StringArray(CStringArray),
    BinaryBlobArray(CArray<CArray<u8>>),
    DateTimeArray(CArray<i64>),
}

// impl CDrop for NativeDeviceData {
//     fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
//         match self {
//             Self::Double(_) => Ok(()),
//             Self::Integer(_) => Ok(()),
//             Self::Boolean(_) => Ok(()),
//             Self::LongInteger(_) => Ok(()),
//             Self::String(s) => s.do_drop(),
//             Self::BinaryBlob(b) => b.do_drop(),
//             Self::DateTime(_) => Ok(()),
//             Self::DoubleArray(a) => a.do_drop(),
//             Self::IntegerArray(a) => a.do_drop(),
//             Self::BooleanArray(a) => a.do_drop(),
//             Self::LongIntegerArray(a) => a.do_drop(),
//             Self::StringArray(a) => a.do_drop(),
//             Self::BinaryBlobArray(a) => a.do_drop(),
//             Self::DateTimeArray(a) => a.do_drop(),
//         }
//     }
// }

impl CReprOf<AstarteData> for NativeDeviceData {
    fn c_repr_of(input: AstarteData) -> Result<Self, ffi_convert::CReprOfError> {
        let native = match input {
            AstarteData::Double(double) => Self::Double(f64::from(double)),
            AstarteData::Integer(integer) => Self::Integer(integer),
            AstarteData::Boolean(boolean) => Self::Boolean(boolean),
            AstarteData::LongInteger(long_integer) => Self::LongInteger(long_integer),
            AstarteData::String(string) => Self::String(CString::c_repr_of(string)?.into_raw()),
            AstarteData::BinaryBlob(items) => Self::BinaryBlob(CArray::c_repr_of(items)?),
            AstarteData::DateTime(date_time) => Self::DateTime(date_time.timestamp_millis()),
            AstarteData::DoubleArray(doubles) => {
                // NOTE if allowed by the orphan rule create from impl in sdk main crate
                // SAFETY: [`Double`] is repr(transparent) and contains an f64
                let doubles: Vec<f64> = unsafe { std::mem::transmute(doubles) };

                Self::DoubleArray(CArray::c_repr_of(doubles)?)
            }
            AstarteData::IntegerArray(items) => Self::IntegerArray(CArray::c_repr_of(items)?),
            AstarteData::BooleanArray(items) => Self::BooleanArray(CArray::c_repr_of(items)?),
            AstarteData::LongIntegerArray(items) => {
                Self::LongIntegerArray(CArray::c_repr_of(items)?)
            }
            AstarteData::StringArray(items) => Self::StringArray(CStringArray::c_repr_of(items)?),
            AstarteData::BinaryBlobArray(items) => Self::BinaryBlobArray(CArray::c_repr_of(items)?),
            AstarteData::DateTimeArray(date_times) => Self::DateTimeArray(CArray::c_repr_of(
                date_times
                    .iter()
                    .map(|dt| dt.timestamp_millis())
                    .collect::<Vec<_>>(),
            )?),
        };

        Ok(native)
    }
}
