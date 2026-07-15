"""Python FFI bindings for astarte-device-sdk using cffi (ABI mode)."""

from __future__ import annotations

import struct
import array
import re
import asyncio
import datetime
from cffi import FFI
from pathlib import Path
from typing import Union
from collections.abc import Sequence, Iterable, Iterator
from types import MappingProxyType
from abc import ABC, abstractmethod
from dataclasses import dataclass

import cffi

ffi = FFI()

here = Path(__file__).resolve().parent

with open(here / "header.h", "r") as file:
    with_includes = file.read()
    definitions = re.sub("#include.+", "", with_includes)

    ffi.cdef(definitions)

def _find_library() -> str:
    """Find the astarte device SDK shared library."""
    lib_name = "libastarte_device_sdk_ffi.so"

    target = here.parents[0] / "target" / "debug" / lib_name

    print(target)

    if target.exists():
        return str(target)
    else:
        raise Exception(f"{target} does not exist")


_lib = None

def _get_lib():
    global _lib
    if _lib is None:
        _lib = ffi.dlopen(_find_library())
    return _lib

class DeviceBinaryBlob:
    # TODO implement buffer protocol too
    
    def __init__(self, view: memoryview, event: DeviceEvent | None = None):
        # Store reference to parent event to prevent use-after-free
        self._event = event
        self._view = view

    @staticmethod
    def from_bytes(buf: bytes) -> DeviceBinaryBlob:
        view = memoryview(buf).cast("B").toreadonly()
        return DeviceBinaryBlob(view)

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceBinaryBlob:
        data = value.data_ptr
        len: int = value.size

        buffer = ffi.buffer(data, len)
        view = memoryview(buffer).cast("B").toreadonly()

        return DeviceBinaryBlob(view, event)

    def to_cdata(self) -> FFI.CData:
        # this keeps a reference to event so no data is freed until this buffer goes out of scope
        buffer = ffi.from_buffer("uint8_t[]", self)

        native = ffi.new("CArray_u8 *")
        native.data_ptr = buffer
        native.size = len(buffer)
        return native

    def __bytes__(self):
        return self._view.tobytes()

    def __len__(self):
        return len(self._view)

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self._view[j] for j in range(*i.indices(len(self)))]
        if i < 0 or i >= len(self):
            raise IndexError("Array index out of range")
        return self._view[i]

    def __iter__(self):
        for i in range(len(self)):
            yield self._view[i]

    def __buffer__(self, flags):
        return self._view.__buffer__(flags)

DeviceDataScalarType = Union[
    float,
    int,
    bool,
    str,
    DeviceBinaryBlob,
    datetime.datetime,
]

DeviceDataVectorType = (
    Sequence[float] | Sequence[int] | Sequence[bool] | 
    Sequence[str] | Sequence[DeviceBinaryBlob] | Sequence[datetime.datetime]
)

class DeviceData(ABC):
    """Abstract base class for DeviceData variants."""

    def as_value(self) -> DeviceDataScalarType | None:
        """
        Returns the underlying immutable data if this value is a scalar.
        """
        return None

    def as_vector(self) -> DeviceDataVectorType | None:
        """
        Returns the underlying immutable data if this value is a vector.
        """
        return None

    @abstractmethod
    def to_cdata(self) -> FFI.CData:
        """
        Returns the cdata enum representing this class data **without coping**.
        """
        pass

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceData:
        tag = value.tag
        lib = _get_lib()
        
        if tag == lib.Double:
            return DeviceDataDouble.from_cdata(value)
        elif tag == lib.Integer:
            return DeviceDataInteger.from_cdata(value)
        elif tag == lib.Boolean:
            return DeviceDataBoolean.from_cdata(value)
        elif tag == lib.LongInteger:
            return DeviceDataLongInteger.from_cdata(value)
        elif tag == lib.String:
            return DeviceDataString.from_cdata(value)
        elif tag == lib.BinaryBlob:
            return DeviceDataBinaryBlob.from_cdata(value, event)
        elif tag == lib.DateTime:
            return DeviceDataDateTime.from_cdata(value)
        elif tag == lib.DoubleArray:
            return DeviceDataDoubleArray.from_cdata(value, event)
        elif tag == lib.IntegerArray:
            return DeviceDataIntegerArray.from_cdata(value, event)
        elif tag == lib.BooleanArray:
            return DeviceDataBooleanArray.from_cdata(value, event)
        elif tag == lib.LongIntegerArray:
            return DeviceDataLongIntegerArray.from_cdata(value, event)
        elif tag == lib.StringArray:
            return DeviceDataStringArray.from_cdata(value)
        elif tag == lib.BinaryBlobArray:
            return DeviceDataBinaryBlobArray.from_cdata(value, event)
        elif tag == lib.DateTimeArray:
            return DeviceDataDateTimeArray.from_cdata(value)
        else:
            raise InvalidNativeValueError(f"Unknown DeviceData tag: {tag}")

class DeviceDataDouble(DeviceData):
    def __init__(self, double_val: float):
        self._value = double_val

    def as_value(self) -> float:
        return self._value

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataDouble:
        return DeviceDataDouble(value.double_)

    def to_cdata(self) -> FFI.CData:
        native = ffi.new("NativeDeviceData *")

        native.tag = _get_lib().Double
        native.double_ = self._value

        return native

class DeviceDataInteger(DeviceData):
    def __init__(self, int_val: int):
        self._value = int_val

    def as_value(self) -> int:
        return self._value

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataInteger:
        return DeviceDataInteger(value.integer)

    def to_cdata(self) -> FFI.CData:
        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().Integer
        native.integer = self._value
        return native

class DeviceDataBoolean(DeviceData):
    def __init__(self, bool_val: bool):
        self._value = bool_val

    def as_value(self) -> bool:
        return self._value

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataBoolean:
        return DeviceDataBoolean(value.boolean)

    def to_cdata(self) -> FFI.CData:
        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().Boolean
        native.boolean = self._value
        return native

class DeviceDataLongInteger(DeviceData):
    def __init__(self, long_int_val: int):
        self._value = long_int_val

    def as_value(self) -> int:
        return self._value

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataLongInteger:
        return DeviceDataLongInteger(value.long_integer)

    def to_cdata(self) -> FFI.CData:
        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().LongInteger
        native.long_integer = self._value
        return native

class DeviceDataString(DeviceData):
    def __init__(self, value: str):
        self._value = value

    def as_value(self) -> str:
        return self._value

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataString:
        string = ffi.string(value.string).decode()

        return DeviceDataString(string)

    def to_cdata(self) -> FFI.CData:
        string = ffi.new("char[]", self._value.encode());

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().String
        native.string = string
        return native

class DeviceDataBinaryBlob(DeviceData):
    def __init__(self, blob: DeviceBinaryBlob):
        self._value = blob

    @staticmethod
    def from_bytes(buf: bytes) -> DeviceDataBinaryBlob:
        blob = DeviceBinaryBlob.from_bytes(buf)
        return DeviceDataBinaryBlob(blob)

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataBinaryBlob:
        blob = DeviceBinaryBlob.from_cdata(value.binary_blob, event)
        return DeviceDataBinaryBlob(blob)

    def to_cdata(self) -> FFI.CData:
        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().BinaryBlob
        native.binary_blob = self._value.to_cdata()[0]
        return native

    def as_value(self) -> DeviceBinaryBlob:
        return self._value


class DeviceDataDateTime(DeviceData):
    def __init__(self, timestamp: datetime.datetime):
        self._value = timestamp

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataDateTime:
        timestamp_ms: int = value.date_time
        ts = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0)
        return DeviceDataDateTime(ts)

    def to_cdata(self) -> FFI.CData:
        timestamp_ms = int(self._value.timestamp() * 1000.0)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().DateTime
        native.date_time = timestamp_ms 
        return native

    def as_value(self) -> datetime.datetime:
        return self._value


# --- Array Variants ---
# NOTE for array variants data is getting mapped when accessed
# for example accessing a string element of an array will copy the data at that time
# evaluate if this is desirable or shuld be changed
# array of native types are just getting read out of native data
# so no copy but data needs to be kept alive

class DeviceDataDoubleArray(DeviceData):
    def __init__(self, view: memoryview[float], event: DeviceEvent | None = None):
        if view.format != "d":
            raise ValueError("expected a view of type d")

        # NOTE Store reference to parent event to prevent use-after-free this type does not copy
        self._event = event
        self._view: memoryview[float] = view.toreadonly() # type: ignore

    @staticmethod
    def from_array(arr: array.array) -> DeviceDataDoubleArray:
        if arr.typecode != "d":
            raise ValueError("expected a view of type d")

        view: memoryview[float] = memoryview(arr)
        return DeviceDataDoubleArray(view)

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataDoubleArray:
        data_ptr = value.double_array.data_ptr
        len = value.double_array.size

        buffer = ffi.buffer(data_ptr, len * struct.calcsize("d"))
        view = memoryview(buffer).cast("d")

        return DeviceDataDoubleArray(view, event)

    def to_cdata(self) -> FFI.CData:
        # wrap self to keep alive the data
        buffer = ffi.from_buffer("double[]", self)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().DoubleArray
        native.double_array.data_ptr = buffer
        native.double_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._view)

    # def __getitem__(self, i):
    #     if isinstance(i, slice):
    #         return [self._ptr[j] for j in range(*i.indices(self._len))]
    #     if i < 0 or i >= self._len:
    #         raise IndexError("Array index out of range")
    #     return self._ptr[i]

    # def __iter__(self):
    #     for i in range(self._len):
    #         yield self._ptr[i]

    def __buffer__(self, flags):
        return memoryview(self._view).__buffer__(flags)

class DeviceDataIntegerArray(DeviceData):
    def __init__(self, view: memoryview[int], event: DeviceEvent | None = None):
        if view.format != "i":
            raise ValueError("expected a view of type i")

        # NOTE Store reference to parent event to prevent use-after-free this type does not copy
        self._event = event
        self._view = view.toreadonly()

    @staticmethod
    def from_array(arr: array.array) -> DeviceDataIntegerArray:
        if arr.typecode != "i":
            raise ValueError("expected an array of type i")

        view: memoryview[int] = memoryview(arr)
        return DeviceDataIntegerArray(view)

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataIntegerArray:
        data_ptr = value.integer_array.data_ptr
        len = value.integer_array.size

        buffer = ffi.buffer(data_ptr, len * struct.calcsize("i"))
        view = memoryview(buffer).cast("i")

        return DeviceDataIntegerArray(view, event)

    def to_cdata(self) -> FFI.CData:
        # wrap self to keep alive the data
        buffer = ffi.from_buffer("int32_t[]", self)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().IntegerArray
        native.integer_array.data_ptr = buffer
        native.integer_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._view)

    def __buffer__(self, flags):
        return memoryview(self._view).__buffer__(flags)


class DeviceDataBooleanArray(DeviceData):
    def __init__(self, view: memoryview[bool], event: DeviceEvent | None = None):
        if view.format != "?":
            raise ValueError("expected a view of type ?")

        # NOTE Store reference to parent event to prevent use-after-free this type does not copy
        self._event = event
        self._view = view.toreadonly()

    @staticmethod
    def from_list(bools: list[bool]) -> DeviceDataBooleanArray:
        native_bools = ffi.new("bool[]", bools)
        buffer = ffi.buffer(native_bools)
        view = memoryview(buffer).cast("?")

        return DeviceDataBooleanArray(view)

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataBooleanArray:
        data_ptr = value.boolean_array.data_ptr
        len = value.boolean_array.size

        buffer = ffi.buffer(data_ptr, len * struct.calcsize("?"))
        view = memoryview(buffer).cast("?")

        return DeviceDataBooleanArray(view, event)

    def to_cdata(self) -> FFI.CData:
        # wrap self to keep alive the data
        buffer = ffi.from_buffer("bool[]", self)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().BooleanArray
        native.boolean_array.data_ptr = buffer
        native.boolean_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._view)

    def __buffer__(self, flags):
        return memoryview(self._view).__buffer__(flags)


class DeviceDataLongIntegerArray(DeviceData):
    def __init__(self, view: memoryview[int], event: DeviceEvent | None = None):
        if view.format != "q":
            raise ValueError("expected a view of type q")

        # NOTE Store reference to parent event to prevent use-after-free this type does not copy
        self._event = event
        self._view = view.toreadonly()

    @staticmethod
    def from_array(arr: array.array) -> DeviceDataLongIntegerArray:
        if arr.typecode != "q":
            raise ValueError("expected an array of type q")

        view: memoryview[int] = memoryview(arr)
        return DeviceDataLongIntegerArray(view)

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataLongIntegerArray:
        data_ptr = value.long_integer_array.data_ptr
        len = value.long_integer_array.size

        buffer = ffi.buffer(data_ptr, len * struct.calcsize("q"))
        view = memoryview(buffer).cast("q")

        return DeviceDataLongIntegerArray(view, event)

    def to_cdata(self) -> FFI.CData:
        # wrap self to keep alive the data
        buffer = ffi.from_buffer("int64_t[]", self)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().LongIntegerArray
        native.long_integer_array.data_ptr = buffer
        native.long_integer_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._view)

    def __buffer__(self, flags):
        return memoryview(self._view).__buffer__(flags)


class DeviceDataStringArray(DeviceData):
    def __init__(self, strings: list[str]):
        self._strings = strings

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataStringArray:
        ptr = value.string_array.data
        len = value.string_array.size

        strings = [ffi.string(ptr[i]).decode() for i in range(len)]

        return DeviceDataStringArray(strings)

    def to_cdata(self) -> FFI.CData:
        cdata_list = [ffi.new("char[]", s.encode()) for s in self._strings]
        native_strings = ffi.new("char*[]", cdata_list)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().StringArray
        native.string_array.data = native_strings
        native.string_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._strings)

    def __getitem__(self, i):
        return self._strings[i]

    def __iter__(self):
        self._strings.__iter__()


class DeviceDataBinaryBlobArray(DeviceData, Iterable[DeviceBinaryBlob]):
    def __init__(self, blobs: list[DeviceBinaryBlob]):
        self._blobs = blobs

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataBinaryBlobArray:
        ptr = value.binary_blob_array.data_ptr
        len: int = value.binary_blob_array.size

        blobs = [DeviceBinaryBlob.from_cdata(ptr[i], event) for i in range(len)]

        return DeviceDataBinaryBlobArray(blobs)

    def to_cdata(self) -> FFI.CData:
        cdata_list = [b.to_cdata()[0] for b in self._blobs]
        native_blobs = ffi.new("struct CArray_u8[]", cdata_list)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().BinaryBlobArray
        native.binary_blob_array.data_ptr = native_blobs
        native.binary_blob_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._blobs)

    def __getitem__(self, i):
        return self._blobs[i]

    def __iter__(self) -> Iterator[DeviceBinaryBlob]:
        return self._blobs.__iter__()


class DeviceDataDateTimeArray(DeviceData):
    def __init__(self, datetimes: list[datetime.datetime]):
        # NOTE Store reference to parent event to prevent use-after-free this type does not copy (it copies when an item is requested)
        self._datetimes = datetimes

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceDataDateTimeArray:
        data = value.date_time_array.data_ptr
        len: int = value.date_time_array.size

        datetimes = [datetime.datetime.fromtimestamp(data[i] / 1000.0) for i in range(len)]

        return DeviceDataDateTimeArray(datetimes)

    def to_cdata(self) -> FFI.CData:
        cdata_list = [int(d.timestamp() * 1000.0) for d in self._datetimes]
        native_datetimes = ffi.new("NativeTimestamp[]", cdata_list)

        native = ffi.new("NativeDeviceData *")
        native.tag = _get_lib().DateTimeArray
        native.date_time_array.data_ptr = native_datetimes
        native.date_time_array.size = len(self)
        return native

    def __len__(self) -> int:
        return len(self._datetimes)

    def __getitem__(self, i):
        return self._datetimes[i]

    def __iter__(self):
        self._datetimes.__iter__()


class DeviceObject:
    def __init__(self, data: dict[str, DeviceData]):
        self.data = MappingProxyType(data)

    @staticmethod
    def from_cdata(entries: FFI.CData, event: DeviceEvent | None = None) -> DeviceObject:
        map = {}
        len = entries.size

        for i in range(0, len):
            c_entry = entries.data_ptr[i]

            path = ffi.string(c_entry.path).decode()
            value = DeviceData.from_cdata(c_entry.value, event)

            map[path] = value

        return DeviceObject(map)

    def to_cdata(self) -> FFI.CData:
        entries = []

        for path, data in self.data.items():
            print(f"item ({path} and {data})")

            c_path = ffi.new("char[]", path.encode())
            c_data = data.to_cdata()

            entry = ffi.new("NativeObjectEntry *")
            entry.path = c_path
            entry.value = c_data[0]

            entries.append(entry[0])

        object = ffi.new("CArray_NativeObjectEntry *")
        object.data_ptr = ffi.new("NativeObjectEntry[]", entries)
        object.size = len(entries)

        return object

class DeviceValue(ABC):
    """Abstract base class for DeviceValue variants."""
    
    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceValue:
        tag = value.tag
        lib = _get_lib()
        
        if tag == lib.Individual:
            return DeviceValueIndividual.from_cdata(value, event)
        elif tag == lib.Object:
            return DeviceValueObject.from_cdata(value, event)
        elif tag == lib.PropertySet:
            return DeviceValuePropertySet.from_cdata(value, event)
        elif tag == lib.PropertyUnset:
            return DeviceValuePropertyUnset.from_cdata(value)
        else:
            raise InvalidNativeValueError(f"Unknown DeviceValue tag: {tag}")


class DeviceValueIndividual(DeviceValue):
    def __init__(self, data: DeviceData, timestamp: datetime.datetime):
        self._data = data
        self._timestamp = timestamp

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceValueIndividual:
        data = DeviceData.from_cdata(value.individual.data, event)
        timestamp = datetime.datetime.fromtimestamp(value.individual.timestamp / 1000.0)
        return DeviceValueIndividual(data, timestamp)

    def to_cdata(self) -> FFI.CData:
        value = ffi.new("NativeValue *")
        data = self._data.to_cdata()
        timestamp = int(self._timestamp.timestamp() * 1000)

        individual_body = ffi.new("Individual_Body")
        individual_body.data = data[0]
        individual_body.timestamp = timestamp

        value.tag = _get_lib().Individual
        value.individual = individual_body

        return value

    @property
    def data(self) -> DeviceData:
        return self._data
        
    @property
    def timestamp(self) -> datetime.datetime:
        return self._timestamp


class DeviceValueObject(DeviceValue):
    def __init__(self, data: DeviceObject, timestamp: datetime.datetime):
        self._data = data
        self._timestamp = timestamp

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceValueObject:
        object = DeviceObject.from_cdata(value.object.data, event)
        timestamp = datetime.datetime.fromtimestamp(value.object.timestamp / 1000.0)

        return DeviceValueObject(object, timestamp)

    def to_cdata(self) -> FFI.CData:
        value = ffi.new("NativeValue *")

        data = self._data.to_cdata()
        timestamp = int(self._timestamp.timestamp() * 1000)

        object_body = ffi.new("Object_Body")
        object_body.data = data
        object_body.timestamp = timestamp

        value.tag = _get_lib().Object
        value.object = object_body

        return value

    @property
    def data(self) -> MappingProxyType[str, DeviceData]:
        return self._data
        
    @property
    def timestamp(self) -> datetime.datetime:
        return self._timestamp


class DeviceValuePropertySet(DeviceValue):
    def __init__(self, property: DeviceData):
        self._property = property

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceValuePropertySet:
        property = DeviceData.from_cdata(value.property_set, event)
        return DeviceValuePropertySet(property)

    def to_cdata(self) -> FFI.CData:
        value = ffi.new("NativeValue *")
        data = self._property.to_cdata()

        value.tag = _get_lib().PropertySet
        value.property_set = data[0]

        return value

    @property
    def property(self) -> DeviceData:
        return self._property


class DeviceValuePropertyUnset(DeviceValue):
    def __init__(self):
        pass

    @staticmethod
    def from_cdata(value: FFI.CData, event: DeviceEvent | None = None) -> DeviceValuePropertyUnset:
        return DeviceValuePropertyUnset()

    def to_cdata(self) -> FFI.CData:
        value = ffi.new("NativeValue *")

        value.tag = _get_lib().PropertyUnset

        return value

# ==========================================
# DeviceEvent
# ==========================================

class DeviceEvent:
    def __init__(self, ok_data: FFI.CData):
        # Tie the C-allocation lifecycle to this object
        self._ptr = ffi.gc(ok_data, _get_lib().device_client_free_device_event)

        self._interface = ffi.string(self._ptr.interface).decode()
        self._path = ffi.string(self._ptr.path).decode()
        self._data = DeviceValue.from_cdata(self._ptr.data, self)

    @property
    def interface(self) -> str:
        return self._interface
        
    @property
    def path(self) -> str:
        return self._path
        
    @property
    def data(self) -> DeviceValue:
        return self._data

class DeviceConfig:
    def __init__(
        self,
        device_id: str,
        cred_secr: str,
        realm: str,
        pairing_url: str,
        interfaces_dir: str,
    ):
        self._device_id = device_id
        self._cred_secr = cred_secr
        self._realm = realm
        self._pairing_url = pairing_url
        self._interfaces_dir = interfaces_dir

class Device:
    """Python wrapper for an Astarte device client."""

    def __init__(
        self,
        config: DeviceConfig,
    ):
        self._config = config
        self._ptr = None
        # list of handles to keep alive created by cffi.new_handle function
        self._handles = set()

    def __del__(self):
        print("freeing handle")

        if len(self._handles) != 0:
            print("error: some handles are still present while the object is being gced")

        _get_lib().device_handle_free(self._ptr)

    def ffi_handle(self, handle_data: any) -> FFI.CData:
        c_handle = ffi.new_handle(handle_data)
        self._handles.add(c_handle)

        return c_handle

    @staticmethod
    def from_ffi_handle(c_handle: FFI.CData) -> any:
        # TODO here we should return a superclass of device data that always contains a reference to the Device
        # this way we can use it to retrieve the handles set and remove the handle we converted
        handle_data = ffi.from_handle(c_handle)
        # NOTE this will raise an error if the handle was not stored before (from_ffi_handle can be called only once!)
        handle_data.device._handles.remove(c_handle)

        return handle_data

    @staticmethod
    def connect(config: DeviceConfig) -> tuple[asyncio.Future[Device], asyncio.Future[None]]:
        device = Device(config)

        device_id = ffi.new("char[]", device._config._device_id.encode())
        cred_secr = ffi.new("char[]", device._config._cred_secr.encode())
        realm = ffi.new("char[]", device._config._realm.encode())
        pairing_url = ffi.new("char[]", device._config._pairing_url.encode())
        interfaces_dir = ffi.new("char[]", device._config._interfaces_dir.encode())

        device_handle_config = ffi.new("NativeDeviceConfig *")
        device_handle_config.device_id = device_id
        device_handle_config.cred_secr = cred_secr
        device_handle_config.realm = realm
        device_handle_config.pairing_url = pairing_url
        device_handle_config.interfaces_dir = interfaces_dir

        loop = asyncio.get_running_loop()
        # handle events future
        loop_future = loop.create_future()
        loop_data = HandleEventsFutureData(loop_future, device)
        loop_data_handle = device.ffi_handle(loop_data)
        # connect future
        connect_future = loop.create_future()
        connect_data_handle = device.ffi_handle(ConnectFutureData(connect_future, device, loop_data))

        _get_lib().device_handle_connect(device_handle_config[0], connect_cbk, connect_data_handle, loop_cbk, loop_data_handle)

        return (connect_future, loop_future)

    def disconnect(self) -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        user_data = self.ffi_handle(DisconnectFutureData(future, self))

        # FIXME the disconnect could now fail if the rwlock is locked we should loop
        # a few times until it succeeds
        _get_lib().device_handle_disconnect(self._ptr, disconnect_cbk, user_data)

        return future
        
        
    def receive_data(self) -> asyncio.Future[DeviceEvent]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        handle = self.ffi_handle(ReceiveFutureData(future, self))

        _get_lib().device_client_receive(self._ptr, receive_cbk, handle)

        return future

    def send_individual(self, interface: str, path: str, data: DeviceData, timestamp: datetime.datetime | None = None) -> asyncio.Future[None]:
        interface_cstr = ffi.new("char[]", interface.encode())
        path_cstr = ffi.new("char[]", path.encode())
        data_c = data.to_cdata()[0]

        individual_data = ffi.new("NativeIndividualSend *")
        individual_data.interface = interface_cstr
        individual_data.path = path_cstr
        individual_data.data = data_c
        if timestamp is None:
            individual_data.timestamp.tag = _get_lib().None_NativeTimestamp
        else:
            individual_data.timestamp.tag = _get_lib().Some_NativeTimestamp
            individual_data.timestamp.some = int(timestamp.timestamp() * 1000.0)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        handle = self.ffi_handle(SendFutureData(future, self))

        _get_lib().device_client_send_individual(self._ptr, individual_data, send_cbk, handle)

        return future


    def send_object(self, interface: str, path: str, object: DeviceObject, timestamp: datetime.datetime | None = None) -> asyncio.Future[None]:
        interface_cstr = ffi.new("char[]", interface.encode())
        path_cstr = ffi.new("char[]", path.encode())
        object_c = object.to_cdata()[0]

        object_data = ffi.new("NativeObjectSend *")
        object_data.interface = interface_cstr
        object_data.path = path_cstr
        object_data.data = object_c
        if timestamp is None:
            object_data.timestamp.tag = _get_lib().None_NativeTimestamp
        else:
            object_data.timestamp.tag = _get_lib().Some_NativeTimestamp
            object_data.timestamp.some = int(timestamp.timestamp() * 1000.0)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        handle = self.ffi_handle(SendFutureData(future, self))

        _get_lib().device_client_send_object(self._ptr, object_data, send_cbk, handle)

        return future


class InvalidNativeValueError(Exception):
    pass

class SendFutureData:
    def __init__(self, future: asyncio.Future[None], device: Device):
        self.future = future
        self.device = device

class SendError(Exception):
    pass

@ffi.callback("void(const struct NativeStringResult_bool *, UserData)")
def send_cbk(native_res, user_data):
    data: SendFutureData = Device.from_ffi_handle(user_data)

    # FIXME maybe here we have to check if the future got cancelled
    
    if native_res.tag == _get_lib().Ok_bool:
        data.future.get_loop().call_soon_threadsafe(data.future.set_result, None)
    elif native_res.tag == _get_lib().Err_bool:
        error_str = ffi.string(native_res.err, 1024).decode()
        error = SendError(error_str)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()
        

class ReceiveFutureData:
    def __init__(self, future: asyncio.Future[DeviceEvent], device: Device):
        self.future = future
        self.device = device

class ReceiveError(Exception):
    pass

@ffi.callback("void(const struct NativeStringResult_NativeManuallyDrop_NativeDeviceEvent *, UserData)")
def receive_cbk(native_res, user_data):
    data: ReceiveFutureData = Device.from_ffi_handle(user_data)

    # FIXME maybe here we have to check if the future got cancelled
    
    if native_res.tag == _get_lib().Ok_NativeManuallyDrop_NativeDeviceEvent:
        event = DeviceEvent(native_res.ok)

        data.future.get_loop().call_soon_threadsafe(data.future.set_result, event)
    elif native_res.tag == _get_lib().Err_NativeManuallyDrop_NativeDeviceEvent:
        error_str = ffi.string(native_res.err, 1024).decode()
        error = ReceiveError(error_str)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()
        

class DisconnectFutureData:
    def __init__(self, future: asyncio.Future[None], device: Device):
        self.future = future
        self.device = device

class DisconnectError(Exception):
    pass

@ffi.callback("void(const struct NativeStringResult_bool *, UserData)")
def disconnect_cbk(native_res, user_data):
    data: DisconnectFutureData = Device.from_ffi_handle(user_data)
    
    if native_res.tag == _get_lib().Ok_bool:
        data.future.get_loop().call_soon_threadsafe(data.future.set_result, None)
    elif native_res.tag == _get_lib().Err_bool:
        error_str = ffi.string(native_res.err, 1024).decode()
        loop_error = DisconnectError(error_str)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, loop_error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()
        

class ConnectFutureData:
    def __init__(self, future: asyncio.Future[Device], device: Device, loop_data: HandleEventsFutureData):
        self.future = future
        self.device = device
        self.loop_data = loop_data

    def fail_loop_future(self, error: Exception):
        future = self.loop_data.future

        future.get_loop().call_soon_threadsafe(future.set_exception, error)

class ConnectError(Exception):
    pass

@ffi.callback("void(const struct NativeStringResult_NativeDeviceHandle *, UserData)")
def connect_cbk(native_result, user_data):
    data: ConnectFutureData = Device.from_ffi_handle(user_data)

    if native_result.tag == _get_lib().Ok_NativeDeviceHandle:
        device_ptr = native_result.ok
        data.device._ptr = device_ptr

        data.future.get_loop().call_soon_threadsafe(data.future.set_result, data.device)
    elif native_result.tag == _get_lib().Err_NativeDeviceHandle:
        error_str = ffi.string(native_result.err, 1024).decode()
        connect_error = ConnectError(error_str)

        # NOTE if the connect fail we force a fail in the loop callback too
        data.fail_loop_future(connect_error)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, connect_error)
    else:
        err = InvalidNativeValueError()

        # NOTE if the connect fail we force a fail in the loop callback too
        data.fail_loop_future(err)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, err)

        # FIXME don't know if raising is a good idea in a callback
        raise err


class HandleEventsFutureData:
    def __init__(self, future: asyncio.Future[None], device: Device):
        self.future = future
        self.device = device

class HandleEventsError(Exception):
    pass

@ffi.callback("void(const struct NativeStringResult_bool *, UserData )")
def loop_cbk(native_result, user_data):
    data: HandleEventsFutureData = Device.from_ffi_handle(user_data)

    if native_result.tag == _get_lib().Ok_bool:
        data.future.get_loop().call_soon_threadsafe(data.future.set_result, None)
    elif native_result.tag == _get_lib().Err_bool:
        error_str = ffi.string(native_result.err, 1024).decode()
        loop_error = HandleEventsError(error_str)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, loop_error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()


# _________________________________________________________
# library use
# 

async def wait_handle_events(handle_events_future):
    try:
        await handle_events_future

        print("handle events joined :D")
    except Exception as e:
        print("got error in handle events future")


async def main():
    config = DeviceConfig(
        device_id="DayugqhpTPi2RgkELFPj9Q",
        cred_secr="SOnybEIx906lTxT7uaHh9K6zQ7dWBWb0mXwmwvO6Q1k=",
        realm="test",
        pairing_url="http://api.astarte.localhost/pairing",
        interfaces_dir="test_binding/interfaces",
    )

    (device_future, handle_events_future) = Device.connect(config)

    device = await device_future

    asyncio.create_task(wait_handle_events(handle_events_future))

    print(device)

    # TODO add function to await for complete connection
    # it is counter intuitive that after the connect we are disconnected 
    # maybe change method name to build
    await asyncio.sleep(2)

    await device.send_individual("org.astarte-platform.rust.e2etest.DeviceDatastream",
                                 "/doublearray_endpoint",
                                 DeviceDataDoubleArray.from_array(array.array("d", [1.0, 1.1, 1.2, 1.3, 1.4])),
                                 datetime.datetime.now()
                             )

    object = DeviceObject({
      "/double_endpoint": DeviceDataDouble(3.14),
      "/integer_endpoint": DeviceDataInteger(1),
      "/boolean_endpoint": DeviceDataBoolean(True),
      "/longinteger_endpoint": DeviceDataLongInteger(1 << 32),
      "/string_endpoint": DeviceDataString("hey"),
      "/binaryblob_endpoint": DeviceDataBinaryBlob(DeviceBinaryBlob.from_bytes(b'tests')),
      "/datetime_endpoint": DeviceDataDateTime(datetime.datetime.now()),
      "/doublearray_endpoint": DeviceDataDoubleArray.from_array(array.array("d", [1.1, 1.2, 1.3])),
      "/integerarray_endpoint": DeviceDataIntegerArray.from_array(array.array("i", [1, 2, 3])),
      "/booleanarray_endpoint": DeviceDataBooleanArray.from_list([True, False, True]),
      "/longintegerarray_endpoint": DeviceDataLongIntegerArray.from_array(array.array("q", [1 << 33, 1 << 34])),
      "/stringarray_endpoint": DeviceDataStringArray(["a", "b", "c"]),
      "/binaryblobarray_endpoint": DeviceDataBinaryBlobArray([DeviceBinaryBlob.from_bytes(b'blob1'), DeviceBinaryBlob.from_bytes(b'blob2')]),
      "/datetimearray_endpoint": DeviceDataDateTimeArray([datetime.datetime.now(), datetime.datetime.now()]),
   })

    await device.send_object("org.astarte-platform.rust.e2etest.DeviceAggregate", "/test", object)

    event = await device.receive_data()
    print(event.interface)
    print(event.path)

    if isinstance(event.data, DeviceValueObject):
        obj: DeviceValueObject = event.data
        print(obj.data)

        for v in obj.data.values():
            print(type(v))

            val = v.as_value()
            if isinstance(v, DeviceDataBinaryBlob):
                b: DeviceDataBinaryBlob = v

                print("bytes", bytes(b.as_value()))
            elif val is not None:
                print("scalar", val)

            vec = v.as_vector()
            if isinstance(v, DeviceDataBinaryBlobArray):
                b_vec: DeviceDataBinaryBlobArray = v

                print("bytes array", [bytes(b) for b in b_vec])
            elif vec is not None:
                print("vector", [d for d in vec])

    print("==========> received eventtttttt", event)

    print("stopping device...", flush=True)

    await device.disconnect()

    print("stopped device", flush=True)
    

if __name__ == "__main__":
    asyncio.run(main())
