"""Python FFI bindings for astarte-device-sdk using cffi (ABI mode)."""

from __future__ import annotations

import re
import asyncio
import os
import platform
import time
from cffi import FFI
from pathlib import Path
from typing import Callable, Optional, Tuple, Any

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

    def ffi_handle(self, handle_data: any) -> ffi.CData:
        c_handle = ffi.new_handle(handle_data)
        self._handles.add(c_handle)

        return c_handle

    @staticmethod
    def from_ffi_handle(c_handle: ffi.CData) -> any:
        # TODO here we should return a subclass of some device data that always contains a reference to the Device
        # this way we can use it to retrieve the handles set and remove the handle we converted
        handle_data = ffi.from_handle(c_handle)
        # NOTE this will raise an error if the handle was not stored before
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
        # connect future
        connect_future = loop.create_future()
        connect_cbk_data = device.ffi_handle(ConnectFutureData(connect_future, device))
        # handle events future
        loop_future = loop.create_future()
        loop_cbk_data = device.ffi_handle(HandleEventsFutureData(loop_future, device))

        _get_lib().device_handle_connect(device_handle_config[0], connect_cbk, connect_cbk_data, loop_cbk, loop_cbk_data)

        return (connect_future, loop_future)
        
    # def receive_data(self) -> asyncio.Future[EventData]:
    #     loop = asyncio.get_running_loop()
    #     future = loop.create_future()
    #     receiver = ReceiveData(loop, future, self)
    #     handle = ffi.new_handle(receiver)

    #     # NOTE removed in data_received_callback
    #     self.future_handles.add(handle)

    #     lib = _get_lib()
    #     lib.device_client_receive(self._handle, data_received_callback, handle)

    #     return future

    def __del__(self):
        print("destructorrr")

class ConnectFutureData:
    def __init__(self, future: asyncio.Future[Device], device: DeviceHandle):
        self.future = future
        self.device = device

class ConnectError(Exception):
    pass

class InvalidNativeValueError(Exception):
    pass

@ffi.callback("void(const struct NativeResult_NativeDeviceHandle *, UserData)")
def connect_cbk(native_result, user_data):
    data: ConnectFutureData = Device.from_ffi_handle(user_data)

    if native_result.tag == _get_lib().Ok_NativeDeviceHandle:
        device_ptr = native_result.ok
        data.device._ptr = device_ptr

        data.future.get_loop().call_soon_threadsafe(data.future.set_result, data.device)
    elif native_result.tag == _get_lib().Err_NativeDeviceHandle:
        error_str = ffi.string(native_result.err, 1024)
        connect_error = ConnectError(error_str)

        data.future.get_loop().call_soon_threadsafe(data.future.set_exception, connect_error)
    else:
        # FIXME don't know if raising is a good idea in a callback
        raise InvalidNativeValueError()


class HandleEventsFutureData:
    def __init__(self, future: asyncio.Future[None], device: DeviceHandle):
        self.future = future
        self.device = device

class HandleEventsError(Exception):
    pass

@ffi.callback("void(const struct NativeResult_bool *, UserData )")
def loop_cbk(native_result, user_data):
    data: HandleEventsFutureData = Device.from_ffi_handle(user_data)

    if native_result.tag == _get_lib().Ok_bool:
        data.future.get_loop().call_soon_threadsafe(data.future.set_result, None)
    elif native_result.tag == _get_lib().Err_bool:
        error_str = ffi.string(native_result.err, 1024)
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
    except Exception as e:
        print("got error in handle events future")


async def main():
    config = DeviceConfig(
        device_id="DayugqhpTPi2RgkELFPj9Q",
        cred_secr="Dey3KvNGsecLiiy5dZIUR8Ziv7Nfgq+vKuShoq4XzEM=",
        realm="test",
        pairing_url="http://api.astarte.localhost/pairing",
        interfaces_dir="examples/individual_datastream/interfaces",
    )

    (device_future, handle_events_future) = Device.connect(config)

    device = await device_future

    asyncio.create_task(wait_handle_events(handle_events_future))

    await asyncio.sleep(10)

    print("stopping device...", flush=True)
    # device.stop()
    

if __name__ == "__main__":
    asyncio.run(main())
