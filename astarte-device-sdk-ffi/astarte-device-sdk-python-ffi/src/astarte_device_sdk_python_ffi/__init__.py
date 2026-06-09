"""Python FFI bindings for astarte-device-sdk using cffi (ABI mode)."""

from __future__ import annotations

import asyncio
import os
import platform
import time
from cffi import FFI
from pathlib import Path
from typing import Callable, Optional, Tuple, Any

import cffi
# from package._foo import ffi

ffi = FFI()

ffi.cdef("""
typedef enum CValueType {
  Individual,
  Object,
  PropertySet,
  PropertyUnset,
} CValueType;

typedef enum CMappingType {
  /**
   * Double mapping.
   */
  Double,
  /**
   * Integer mapping.
   */
  Integer,
  /**
   * Boolean mapping.
   */
  Boolean,
  /**
   * Long integers mapping.
   */
  LongInteger,
  /**
   * String mapping.
   */
  String,
  /**
   * Binary mapping.
   */
  BinaryBlob,
  /**
   * Date time mapping.
   */
  DateTime,
  /**
   * Double array mapping.
   */
  DoubleArray,
  /**
   * Integer array mapping.
   */
  IntegerArray,
  /**
   * Boolean array mapping.
   */
  BooleanArray,
  /**
   * Long integer array mapping.
   */
  LongIntegerArray,
  /**
   * String array mapping.
   */
  StringArray,
  /**
   * Binary array mapping.
   */
  BinaryBlobArray,
  /**
   * Date time array mapping.
   */
  DateTimeArray,
} CMappingType;

typedef struct CAstarteData CAstarteData;

typedef struct CDeviceHandle CDeviceHandle;

typedef struct CAstarteDeviceConfig {
  const char *device_id;
  const char *cred_secr;
  const char *realm;
  const char *pairing_url;
  const char *interfaces_dir;
} CAstarteDeviceConfig;

typedef struct CValue {

} CValue;

typedef void (*AstarteDeviceReceiveCallback)(const char *interface,
                                             const char *path,
                                             const struct CValue *value,
                                             void *user_data);

typedef void (*AstarteDeviceSendCallback)(void *user_data);

struct CAstarteData *device_data_int(int32_t value);

struct CAstarteData *device_data_longint(int64_t value);

struct CDeviceHandle *device_client_start(const struct CAstarteDeviceConfig *config);

void device_client_receive(struct CDeviceHandle *device_handle,
                           AstarteDeviceReceiveCallback callback,
                           void *user_data);

void device_client_send_individual(struct CDeviceHandle *device_handle,
                                   const char *interface_name,
                                   const char *path,
                                   struct CAstarteData *data,
                                   AstarteDeviceSendCallback callback,
                                   void *user_data);

void device_client_stop(struct CDeviceHandle *device_handle);

/**
 * Frees a string allocated by the Rust FFI.
 */
void device_event_free_string(char *s);

void device_event_free_value(struct CValue *s);

bool device_event_value_get_value_type(const struct CValue *event, enum CValueType *out_type);

bool device_event_value_get_data_type(const struct CValue *event, enum CMappingType *out_type);

/**
 * Example getter: Retrieves an integer value if the underlying data is an Integer.
 * Returns `true` if successful, `false` if the data was missing or of a different type.
 */
bool device_event_value_get_integer(const struct CValue *event, int32_t *out_val);
""")

# TODO use extern python callbacks with separate module
ffi.cdef("""
extern "Python" void connection_cb();
""")

from enum import Enum

class CMappingType(Enum):
    """Python representation of the CMappingType C enum."""
    Double = 0
    Integer = 1
    Boolean = 2
    LongInteger = 3
    String = 4
    BinaryBlob = 5
    DateTime = 6
    DoubleArray = 7
    IntegerArray = 8
    BooleanArray = 9
    LongIntegerArray = 10
    StringArray = 11
    BinaryBlobArray = 12
    DateTimeArray = 13

def int_to_mapping_type(value: int):
    try:
        mapping_enum = CMappingType(value)
        
        return mapping_enum
    except ValueError:
        raise ValueError(f"Invalid integer '{value}' for CMappingType.")

def _find_library() -> str:
    """Find the astarte device SDK shared library."""
    lib_name = "libastarte_device_sdk_ffi.so"
    system = platform.system()
    if system == "Darwin":
        lib_name = "libastarte_device_sdk_ffi.dylib"
    elif system == "Windows":
        lib_name = "astarte_device_sdk_ffi.dll"

    here = Path(__file__).resolve().parent

    # Path hierarchy: .../astarte-device-sdk-ffi/astarte-device-sdk-python-ffi/src/astarte_device_sdk_python_ffi/
    # parents[2] = .../astarte-device-sdk-ffi/astarte-device-sdk-python-ffi
    ffi_root = here.parents[1]
    print(ffi_root)

    search_paths = [
        ffi_root / lib_name
    ]

    env_lib = os.environ.get("ASTARTE_SDK_LIB_PATH")
    if env_lib:
        search_paths.insert(0, Path(env_lib) / lib_name)

    for path in search_paths:
        if path.exists():
            return str(path)

    return lib_name


_lib = None


def _get_lib():
    global _lib
    if _lib is None:
        _lib = ffi.dlopen(_find_library())
    return _lib


class AstarteDevice:
    """Python wrapper for an Astarte device client."""

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
        self._handle = None
        self._cb_keepalive = None
        self.future_handles = set()

    def start(self):
        """Start the device client. Returns the raw CDeviceHandle pointer."""
        lib = _get_lib()

        config = ffi.new("CAstarteDeviceConfig *")

        device_id = ffi.new("char[]", self._device_id.encode())
        cred_secr = ffi.new("char[]", self._cred_secr.encode())
        realm = ffi.new("char[]", self._realm.encode())
        pairing_url = ffi.new("char[]", self._pairing_url.encode())
        interfaces_dir = ffi.new("char[]", self._interfaces_dir.encode()) 

        config.device_id = device_id
        config.cred_secr = cred_secr
        config.realm = realm
        config.pairing_url = pairing_url
        config.interfaces_dir = interfaces_dir

        # self._cb_keepalive = connection_cb

        self._handle = lib.device_client_start(config)
        print(f"device started {self._handle}", flush=True)

    def receive_data(self) -> asyncio.Future[EventData]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        receiver = ReceiveData(loop, future, self)
        handle = ffi.new_handle(receiver)

        # NOTE removed in data_received_callback
        self.future_handles.add(handle)

        lib = _get_lib()
        lib.device_client_receive(self._handle, data_received_callback, handle)

        return future

    def send_data(self, interface_name: str, path: str, data: int) -> asyncio.Future[None]:
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        receiver = SendData(loop, future, self)
        handle = ffi.new_handle(receiver)

        # NOTE removed in data_received_callback
        self.future_handles.add(handle)

        # TODO FIXME this could be unneeded, string can be passed directly in theory
        c_interface_name = ffi.new("char[]", interface_name.encode())
        c_path = ffi.new("char[]", path.encode())

        lib = _get_lib()

        c_data = lib.device_data_longint(data)

        lib.device_client_send_individual(self._handle, c_interface_name, c_path, c_data, data_send_callback, handle)

        return future

    def stop(self):
        lib = _get_lib()
        lib.device_client_stop(self._handle)


class SendData:
    loop: asyncio.AbstractEventLoop
    device: AstarteDevice
    future: asyncio.Future[None]

    def __init__(self, loop: asyncio.AbstractEventLoop, future: asyncio.Future[None], device: AstarteDevice):
        self.used_loop = loop
        self.device = device
        self.future = future
        print("initialized senddata")

@ffi.callback("void(void *)")
def data_send_callback(handle):
    receiver: SendData = ffi.from_handle(handle)

    receiver.device.future_handles.discard(handle)

    print(f"data sent python c callback")

    receiver.used_loop.call_soon_threadsafe(receiver.future.set_result, None)

class ReceiveData:
    loop: asyncio.AbstractEventLoop
    device: AstarteDevice
    future: asyncio.Future[EventData]

    def __init__(self, loop: asyncio.AbstractEventLoop, future: asyncio.Future[EventData], device: AstarteDevice):
        self.used_loop = loop
        self.device = device
        self.future = future
        print("initialized receive data")

@ffi.callback("void(char *, char *, CValue *, void *)")
def data_received_callback(interface_name, path, value, handle):
    receiver: ReceiveData = ffi.from_handle(handle)

    receiver.device.future_handles.discard(handle)
    
    event = EventData(interface_name, path, value)

    receiver.used_loop.call_soon_threadsafe(receiver.future.set_result, event)
    
class EventData:
    interface_name: str
    path: str
    c_data: FFI.CData

    def __init__(self, c_interface_name: FFI.CData, c_path: FFI.CData, c_data: FFI.CData):
        lib = _get_lib()

        interface_name = str(ffi.string(ffi.gc(c_interface_name, lib.device_event_free_string)))
        path = str(ffi.string(ffi.gc(c_path, lib.device_event_free_string)))

        self.interface_name = interface_name
        self.path = path
        self.c_data = ffi.gc(c_data, lib.device_event_free_value)

    def get_type(self) -> CMappingType:
        lib = _get_lib()

        type_enum = ffi.new("CMappingType *")

        valid = lib.device_event_value_get_data_type(self.c_data, type_enum)

        if valid:
            return int_to_mapping_type(int(type_enum[0]))
        else:
            raise Exception("not valid :(")

        
    # def __del__(self):
        
# actual use of the device

async def send_data_loop(device):
    i = 0
    while True:
        i += 1

        # await device.send_data("org.astarte-platform.rust.examples.individual-datastream.DeviceDatastream", "/endpoint1", i)

        await asyncio.sleep(20.0)

async def receive_data_loop(device):
    while True:
        print("receiving data")

        event = await device.receive_data()

        print("received event of type", event.interface_name, event.path, event.get_type())

async def main(device):
    asyncio.create_task(receive_data_loop(device))

    await send_data_loop(device)

if __name__ == "__main__":
    device = AstarteDevice(
        device_id="DayugqhpTPi2RgkELFPj9Q",
        cred_secr="hV96foZQApU+J086iHN1F/Q/siVvBD1znIQW7UrOosU=",
        realm="test",
        pairing_url="http://api.astarte.localhost/pairing",
        interfaces_dir="../../examples/individual_datastream/interfaces",
    )

    device.start()

    asyncio.run(main(device))

    print("stopping device...", flush=True)
    device.stop()

