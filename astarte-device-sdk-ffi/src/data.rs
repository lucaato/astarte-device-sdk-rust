use std::ffi::{CStr, CString, c_char};

use astarte_device_sdk::{
    AstarteData, DeviceEvent, Value,
    aggregate::AstarteObject,
    chrono::{DateTime, Utc},
    types::Double,
};
use ffi_convert::{AsRust, AsRustError, CArray, CDrop, CReprOf, CStringArray};

use crate::{BorrowAsRust, NativeOption};

#[repr(transparent)]
#[derive(Clone, Debug)]
pub struct NativeTimestamp(i64);

impl AsRust<DateTime<Utc>> for NativeTimestamp {
    fn as_rust(&self) -> Result<DateTime<Utc>, ffi_convert::AsRustError> {
        DateTime::from_timestamp_millis(self.0)
            .ok_or(AsRustError::Other("can't convert timestamp".into()))
    }
}

impl CDrop for NativeTimestamp {
    fn do_drop(&mut self) -> Result<(), ffi_convert::CDropError> {
        Ok(())
    }
}

impl CReprOf<DateTime<Utc>> for NativeTimestamp {
    fn c_repr_of(input: DateTime<Utc>) -> Result<Self, ffi_convert::CReprOfError> {
        Ok(Self(input.timestamp_millis()))
    }
}

#[repr(C)]
#[derive(AsRust, Debug)]
#[target_type(IndividualSend)]
pub struct NativeIndividualSend {
    pub interface: *const c_char,
    pub path: *const c_char,
    pub data: NativeDeviceData,
    pub timestamp: NativeOption<NativeTimestamp>,
}

#[derive(Clone, Debug)]
pub struct IndividualSend {
    pub interface: String,
    pub path: String,
    pub data: AstarteData,
    pub timestamp: Option<DateTime<Utc>>,
}

#[repr(C)]
#[derive(Debug)]
pub struct NativeObjectSend {
    pub interface: *const c_char,
    pub path: *const c_char,
    pub data: CArray<NativeObjectEntry>,
    pub timestamp: NativeOption<NativeTimestamp>,
}

impl AsRust<ObjectSend> for NativeObjectSend {
    fn as_rust(&self) -> Result<ObjectSend, AsRustError> {
        let interface = unsafe { CStr::from_ptr(self.interface) }.as_rust()?;
        let path = unsafe { CStr::from_ptr(self.path) }.as_rust()?;
        let data = AstarteObject::from_iter(self.data.as_rust()?.into_iter());
        let timestamp = self.timestamp.as_rust()?;

        Ok(ObjectSend {
            interface,
            path,
            data,
            timestamp,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ObjectSend {
    pub interface: String,
    pub path: String,
    pub data: AstarteObject,
    pub timestamp: Option<DateTime<Utc>>,
}

/*
 * NOTE by using ffi_convert you should copy data when converting to rust and to ffi compatible structs
 * since we don't want that for events data (it could be large) we'll also have borrowed struct that won't be dropped by us but will be
 * handled by the calling language
 */

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

impl AsRust<(String, AstarteData)> for NativeObjectEntry {
    fn as_rust(&self) -> Result<(String, AstarteData), AsRustError> {
        let path = unsafe { CStr::from_ptr(self.path) }.as_rust()?;
        let value = self.value.as_rust()?;

        Ok((path, value))
    }
}

#[repr(C)]
#[derive(CDrop, Debug)]
pub enum NativeDeviceData {
    Double(f64),
    Integer(i32),
    Boolean(bool),
    LongInteger(i64),
    String(*const c_char),
    BinaryBlob(CArray<u8>),
    DateTime(NativeTimestamp),
    DoubleArray(CArray<f64>),
    IntegerArray(CArray<i32>),
    BooleanArray(CArray<bool>),
    LongIntegerArray(CArray<i64>),
    StringArray(CStringArray),
    BinaryBlobArray(CArray<CArray<u8>>),
    DateTimeArray(CArray<NativeTimestamp>),
}

impl AsRust<AstarteData> for NativeDeviceData {
    fn as_rust(&self) -> Result<AstarteData, AsRustError> {
        let native = match self {
            NativeDeviceData::Double(double) => AstarteData::Double(
                Double::try_from(*double)
                    .map_err(|_| AsRustError::Other("invalid double value".into()))?,
            ),
            NativeDeviceData::Integer(integer) => AstarteData::Integer(*integer),
            NativeDeviceData::Boolean(boolean) => AstarteData::Boolean(*boolean),
            NativeDeviceData::LongInteger(long_integer) => AstarteData::LongInteger(*long_integer),
            NativeDeviceData::String(string) => {
                AstarteData::String(unsafe { CStr::from_ptr(*string) }.as_rust()?)
            }
            NativeDeviceData::BinaryBlob(blob) => AstarteData::BinaryBlob(blob.as_rust()?),
            NativeDeviceData::DateTime(date_time) => AstarteData::DateTime(date_time.as_rust()?),
            NativeDeviceData::DoubleArray(doubles) => {
                let vec: Vec<f64> = doubles.as_rust()?;
                let doubles = vec
                    .into_iter()
                    .map(|d| Double::try_from(d))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| AsRustError::Other("invalid double value".into()))?;

                AstarteData::DoubleArray(doubles)
            }
            NativeDeviceData::IntegerArray(items) => AstarteData::IntegerArray(items.as_rust()?),
            NativeDeviceData::BooleanArray(items) => AstarteData::BooleanArray(items.as_rust()?),
            NativeDeviceData::LongIntegerArray(items) => {
                AstarteData::LongIntegerArray(items.as_rust()?)
            }
            NativeDeviceData::StringArray(items) => AstarteData::StringArray(items.as_rust()?),
            NativeDeviceData::BinaryBlobArray(items) => {
                AstarteData::BinaryBlobArray(items.as_rust()?)
            }
            NativeDeviceData::DateTimeArray(items) => AstarteData::DateTimeArray(items.as_rust()?),
        };

        Ok(native)
    }
}

impl CReprOf<AstarteData> for NativeDeviceData {
    fn c_repr_of(input: AstarteData) -> Result<Self, ffi_convert::CReprOfError> {
        let native = match input {
            AstarteData::Double(double) => Self::Double(f64::from(double)),
            AstarteData::Integer(integer) => Self::Integer(integer),
            AstarteData::Boolean(boolean) => Self::Boolean(boolean),
            AstarteData::LongInteger(long_integer) => Self::LongInteger(long_integer),
            AstarteData::String(string) => Self::String(CString::c_repr_of(string)?.into_raw()),
            AstarteData::BinaryBlob(items) => Self::BinaryBlob(CArray::c_repr_of(items)?),
            AstarteData::DateTime(date_time) => {
                Self::DateTime(NativeTimestamp::c_repr_of(date_time)?)
            }
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
            AstarteData::DateTimeArray(date_times) => {
                Self::DateTimeArray(CArray::c_repr_of(date_times)?)
            }
        };

        Ok(native)
    }
}
