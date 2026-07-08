"""Python FFI bindings for astarte-device-sdk using cffi (ABI mode)."""

from __future__ import annotations

import re
import asyncio
import datetime
from cffi import FFI
from pathlib import Path
from typing import Union
from collections.abc import Sequence
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

class DeviceBinayBlob:
    def __init__(self, c_array: FFI.CData):
        self._view = memoryview(ffi.buffer(c_array.data_ptr, c_array.size)).cast("B").toreadonly()

    def __bytes__(self):
        return self._view.tobytes()

    def __len__(self):
        return len(self._view)

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self._view[j] for j in range(*i.indices(self.__len__()))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return self._view[i]

    def __iter__(self):
        for i in range(self.__len__()):
            yield self._view[i]

DeviceDataScalarType = Union[
    float,
    int,
    bool,
    str,
    DeviceBinayBlob,
    datetime.datetime,
]

DeviceDataVectorType = (
    Sequence[float] | Sequence[int] | Sequence[bool] | 
    Sequence[str] | Sequence[DeviceBinayBlob] | Sequence[datetime.datetime]
)

class DeviceData(ABC):
    """Abstract base class for DeviceData variants."""

    def as_vector(self) -> DeviceDataVectorType | None:
        """
        Returns the underlying sequence if this value is a vector.
        """
        return None

    def as_value(self) -> DeviceDataScalarType | None:
        """
        Returns the underlying immutable data if this value is a scalar.
        """
        return None

    @staticmethod
    def from_cdata(value: FFI.CData) -> DeviceData:
        tag = value.tag
        lib = _get_lib()
        
        if tag == lib.Double:
            return DeviceDataDouble(value.double_)
        elif tag == lib.Integer:
            return DeviceDataInteger(value.integer)
        elif tag == lib.Boolean:
            return DeviceDataBoolean(value.boolean)
        elif tag == lib.LongInteger:
            return DeviceDataLongInteger(value.long_integer)
        elif tag == lib.String:
            return DeviceDataString(value.string)
        elif tag == lib.BinaryBlob:
            return DeviceDataBinaryBlob(value.binary_blob)
        elif tag == lib.DateTime:
            return DeviceDataDateTime(value.date_time)
        elif tag == lib.DoubleArray:
            return DeviceDataDoubleArray(value.double_array)
        elif tag == lib.IntegerArray:
            return DeviceDataIntegerArray(value.integer_array)
        elif tag == lib.BooleanArray:
            return DeviceDataBooleanArray(value.boolean_array)
        elif tag == lib.LongIntegerArray:
            return DeviceDataLongIntegerArray(value.long_integer_array)
        elif tag == lib.StringArray:
            return DeviceDataStringArray(value.string_array)
        elif tag == lib.BinaryBlobArray:
            return DeviceDataBinaryBlobArray(value.binary_blob_array)
        elif tag == lib.DateTimeArray:
            return DeviceDataDateTimeArray(value.date_time_array)
        else:
            raise InvalidNativeValueError(f"Unknown DeviceData tag: {tag}")


class DeviceDataDouble(DeviceData):
    def __init__(self, double_val: float):
        self._value = double_val

    def as_value(self) -> float:
        return self._value


class DeviceDataInteger(DeviceData):
    def __init__(self, int_val: int):
        self._value = int_val

    def as_value(self) -> int:
        return self._value


class DeviceDataBoolean(DeviceData):
    def __init__(self, bool_val: bool):
        self._value = bool_val

    def as_value(self) -> bool:
        return self._value


class DeviceDataLongInteger(DeviceData):
    def __init__(self, long_int_val: int):
        self._value = long_int_val

    def as_value(self) -> int:
        return self._value


class DeviceDataString(DeviceData):
    def __init__(self, c_string: FFI.CData):
        self._value = ffi.string(c_string).decode("utf-8")

    def as_value(self) -> str:
        return self._value


class DeviceDataBinaryBlob(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._value = DeviceBinayBlob(c_array)

    def as_value(self) -> DeviceBinayBlob:
        return self._value


class DeviceDataDateTime(DeviceData):
    def __init__(self, timestamp_ms: int):
        self._value = datetime.datetime.fromtimestamp(timestamp_ms / 1000.0)

    def as_value(self) -> datetime.datetime:
        return self._value


# --- Array Variants ---
# NOTE for array variants data that needs to be mapped is getting mapped when accessed
# for example accessing a string element of an array will copy the data at that time
# evaluate if this is desirable or shuld be changed
# array of native types are just getting read out of native data

class DeviceDataDoubleArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data_ptr
        self._len = c_array.size

    def as_vector(self) -> Sequence[float]:
        return self

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self._ptr[j] for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return self._ptr[i]

    def __iter__(self):
        for i in range(self._len):
            yield self._ptr[i]


class DeviceDataIntegerArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data_ptr
        self._len = c_array.size

    def as_vector(self) -> Sequence[int]:
            return self

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self._ptr[j] for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return self._ptr[i]

    def __iter__(self):
        for i in range(self._len):
            yield self._ptr[i]


class DeviceDataBooleanArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data_ptr
        self._len = c_array.size

    def as_vector(self) -> Sequence[bool]:
            return self

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self._ptr[j] for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return self._ptr[i]

    def __iter__(self):
        for i in range(self._len):
            yield self._ptr[i]


class DeviceDataLongIntegerArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data_ptr
        self._len = c_array.size

    def as_vector(self) -> Sequence[int]:
            return self
        
    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [self._ptr[j] for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return self._ptr[i]

    def __iter__(self):
        for i in range(self._len):
            yield self._ptr[i]


class DeviceDataStringArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data
        self._len = c_array.size

    def as_vector(self) -> Sequence[str]:
            return self

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [ffi.string(self._ptr[j]).decode("utf-8") for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return ffi.string(self._ptr[i]).decode("utf-8")

    def __iter__(self):
        for i in range(self._len):
            yield ffi.string(self._ptr[i]).decode("utf-8")


class DeviceDataBinaryBlobArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data_ptr
        self._len = c_array.size

    def as_vector(self) -> Sequence[DeviceBinayBlob]:
            return self

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [DeviceBinayBlob(self._ptr[j]) for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return DeviceBinayBlob(self._ptr[i])

    def __iter__(self):
        for i in range(self._len):
            yield DeviceBinayBlob(self._ptr[i])



class DeviceDataDateTimeArray(DeviceData):
    def __init__(self, c_array: FFI.CData):
        self._ptr = c_array.data_ptr
        self._len = c_array.size

    def as_vector(self) -> Sequence[datetime.datetime]:
            return self

    def __len__(self) -> int:
        return self._len

    def __getitem__(self, i):
        if isinstance(i, slice):
            return [datetime.datetime.fromtimestamp(self._ptr[j] / 1000.0) for j in range(*i.indices(self._len))]
        if i < 0 or i >= self._len:
            raise IndexError("Array index out of range")
        return datetime.datetime.fromtimestamp(self._ptr[i] / 1000.0)

    def __iter__(self):
        for i in range(self._len):
            yield datetime.datetime.fromtimestamp(self._ptr[i] / 1000.0)


class DeviceValue(ABC):
    """Abstract base class for DeviceValue variants."""
    @staticmethod
    def from_cdata(value: FFI.CData) -> DeviceValue:
        tag = value.tag
        lib = _get_lib()
        
        if tag == lib.Individual:
            return DeviceValueIndividual(value.individual)
        elif tag == lib.Object:
            return DeviceValueObject(value.object)
        elif tag == lib.PropertySet:
            return DeviceValuePropertySet(value.property_set)
        elif tag == lib.PropertyUnset:
            return DeviceValuePropertyUnset()
        else:
            raise InvalidNativeValueError(f"Unknown DeviceValue tag: {tag}")


class DeviceValueIndividual(DeviceValue):
    def __init__(self, value: FFI.CData):
        self._data = DeviceData.from_cdata(value.data)
        self._timestamp = datetime.datetime.fromtimestamp(value.timestamp / 1000.0)

    @property
    def data(self) -> DeviceData:
        return self._data
        
    @property
    def timestamp(self) -> datetime.datetime:
        return self._timestamp


class DeviceValueObject(DeviceValue):
    def __init__(self, value: FFI.CData):
        self._data = self.__data_to_map(value.data)
        self._timestamp = datetime.datetime.fromtimestamp(value.timestamp / 1000.0)

    def __entry_tuple(self, entry: FFI.CData) -> tuple[str, DeviceData]:
        path = ffi.string(entry.path).decode("utf-8")
        value = DeviceData.from_cdata(entry.value)

        return (path, value)

    def __data_to_map(self, entries: FFI.CData) -> MappingProxyType[str, DeviceData]:
        map = {}

        for i in range(0, entries.size):
            (path, value) = self.__entry_tuple(entries.data_ptr[i])
            map[path] = value

        return MappingProxyType(map)

    @property
    def data(self) -> MappingProxyType[str, DeviceData]:
        return self._data
        
    @property
    def timestamp(self) -> datetime.datetime:
        return self._timestamp


class DeviceValuePropertySet(DeviceValue):
    def __init__(self, value: FFI.CData):
        self._property = DeviceData.from_cdata(value)

    @property
    def property(self) -> DeviceData:
        return self._property


class DeviceValuePropertyUnset(DeviceValue):
    def __init__(self):
        pass


# ==========================================
# DeviceEvent
# ==========================================

class DeviceEvent:
    def __init__(self, ok_data: FFI.CData):
        # Tie the C-allocation lifecycle to this object
        self._ptr = ffi.gc(ok_data, _get_lib().device_client_free_device_event)

        self._interface = ffi.string(self._ptr.interface).decode("utf-8")
        self._path = ffi.string(self._ptr.path).decode("utf-8")
        self._data = DeviceValue.from_cdata(self._ptr.data)

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

    def ffi_handle(self, handle_data: any) -> FFI.CData:
        c_handle = ffi.new_handle(handle_data)
        self._handles.add(c_handle)

        return c_handle

    @staticmethod
    def from_ffi_handle(c_handle: FFI.CData) -> any:
        # TODO here we should return a subclass of some device data that always contains a reference to the Device
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

class InvalidNativeValueError(Exception):
    pass

class ReceiveFutureData:
    def __init__(self, future: asyncio.Future[DeviceEvent], device: DeviceHandle):
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
        error_str = ffi.string(native_res.err, 1024).decode("utf-8")
        error = ReceiveError(error_str)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()
        

class DisconnectFutureData:
    def __init__(self, future: asyncio.Future[None], device: DeviceHandle):
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
        error_str = ffi.string(native_res.err, 1024).decode("utf-8")
        loop_error = DisconnectError(error_str)
        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, loop_error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()
        

class ConnectFutureData:
    def __init__(self, future: asyncio.Future[Device], device: DeviceHandle, loop_data: HandleEventsFutureData):
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
        error_str = ffi.string(native_result.err, 1024).decode("utf-8")
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
    def __init__(self, future: asyncio.Future[None], device: DeviceHandle):
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
        error_str = ffi.string(native_result.err, 1024).decode("utf-8")
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
        cred_secr="HBUDjQja7Ap0GmgIoZjGYqltCy89B+VmlgvSmoFOte0=",
        realm="test",
        pairing_url="http://api.astarte.localhost/pairing",
        interfaces_dir="test_binding/interfaces",
    )

    (device_future, handle_events_future) = Device.connect(config)

    device = await device_future

    asyncio.create_task(wait_handle_events(handle_events_future))

    print(device)

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
