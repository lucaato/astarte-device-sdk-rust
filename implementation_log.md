# Implementation Log

This log tracks the progress of the `astarte-device-sdk-ffi` crate based on `implementation_plan.md`.

## Completed
- [x] Initial `astarte-device-sdk-ffi` crate creation
- [x] `boltffi.toml` configuration
- [x] `Cargo.toml` dependencies and workspace configuration
- [x] `src/types.rs` - Basic BoltFFI data types (`DeviceConfig`, `MappingType`, `ValueType`, `StoredProperty`, `ObjectEntry`)
- [x] `src/error.rs` - Error enums (`ValueError`, `DeviceError`)

## In Progress
- [ ] `src/data.rs` - `AstarteDataValue`
- [ ] `src/lib.rs` - Module re-exports

## Pending
- [ ] `src/event.rs` - `DeviceEvent`, `IndividualEvent`, `ObjectEvent`
- [ ] `src/device.rs` - `DeviceHandle`

