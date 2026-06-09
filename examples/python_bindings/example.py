#!/usr/bin/env python3
# example.py: Demonstrates how to use the Astarte Device SDK from Python via ctypes.

import ctypes
import os
import sys

# Load the shared library
lib_path = os.path.join(os.path.dirname(__file__), "../../target/release/libastarte_device_sdk.so")
lib = ctypes.CDLL(lib_path)

# Define the CAstarteDeviceConfig struct
class CAstarteDeviceConfig(ctypes.Structure):
    _fields_ = [
        ("device_id", ctypes.c_char_p),
        ("cred_secr", ctypes.c_char_p),
        ("realm", ctypes.c_char_p),
        ("pairing_url", ctypes.c_char_p),
        ("interfaces_dir", ctypes.c_char_p),
        ("connection_cbk", ctypes.c_void_p),
        ("disconnection_cbk", ctypes.c_void_p),
        ("datastream_individual_cbk", ctypes.c_void_p),
        ("datastream_object_cbk", ctypes.c_void_p),
        ("property_set_cbk", ctypes.c_void_p),
        ("property_unset_cbk", ctypes.c_void_p),
        ("cbk_user_data", ctypes.c_void_p),
    ]

# Define callback types
ConnectionCallback = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
DisconnectionCallback = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
DatastreamIndividualCallback = ctypes.CFUNCTYPE(
    None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p
)
DatastreamObjectCallback = ctypes.CFUNCTYPE(
    None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p
)
PropertySetCallback = ctypes.CFUNCTYPE(
    None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p
)
PropertyUnsetCallback = ctypes.CFUNCTYPE(None, ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p)

# Define the device_client_start function
lib.device_client_start.argtypes = [CAstarteDeviceConfig]
lib.device_client_start.restype = ctypes.c_void_p

def on_connection(user_data):
    print(f"Connected! User data: {user_data}")

def on_disconnection(user_data):
    print(f"Disconnected! User data: {user_data}")

def on_datastream_individual(interface, path, value, user_data):
    print(
        f"Datastream Individual Received! Interface: {interface.decode()}, Path: {path.decode()}, Value: {value.decode()}, User Data: {user_data}"
    )

def on_datastream_object(interface, path, value, user_data):
    print(
        f"Datastream Object Received! Interface: {interface.decode()}, Path: {path.decode()}, Value: {value.decode()}, User Data: {user_data}"
    )

def on_property_set(interface, path, value, user_data):
    print(
        f"Property Set! Interface: {interface.decode()}, Path: {path.decode()}, Value: {value.decode()}, User Data: {user_data}"
    )

def on_property_unset(interface, path, user_data):
    print(f"Property Unset! Interface: {interface.decode()}, Path: {path.decode()}, User Data: {user_data}")

def main():
    # Register callbacks
    connection_cbk = ConnectionCallback(on_connection)
    disconnection_cbk = DisconnectionCallback(on_disconnection)
    datastream_individual_cbk = DatastreamIndividualCallback(on_datastream_individual)
    datastream_object_cbk = DatastreamObjectCallback(on_datastream_object)
    property_set_cbk = PropertySetCallback(on_property_set)
    property_unset_cbk = PropertyUnsetCallback(on_property_unset)

    # Create the config
    config = CAstarteDeviceConfig(
        device_id=b"my_device",
        cred_secr=b"secret",
        realm=b"my_realm",
        pairing_url=b"https://example.com/pairing",
        interfaces_dir=b"/path/to/interfaces",
        connection_cbk=ctypes.cast(connection_cbk, ctypes.c_void_p),
        disconnection_cbk=ctypes.cast(disconnection_cbk, ctypes.c_void_p),
        datastream_individual_cbk=ctypes.cast(datastream_individual_cbk, ctypes.c_void_p),
        datastream_object_cbk=ctypes.cast(datastream_object_cbk, ctypes.c_void_p),
        property_set_cbk=ctypes.cast(property_set_cbk, ctypes.c_void_p),
        property_unset_cbk=ctypes.cast(property_unset_cbk, ctypes.c_void_p),
        cbk_user_data=ctypes.c_void_p(None),
    )

    # Start the device client
    handle = lib.device_client_start(config)
    print(f"Device client started! Handle: {handle}")

    # Keep the program running
    try:
        while True:
            pass
    except KeyboardInterrupt:
        print("Exiting...")


if __name__ == "__main__":
    main()