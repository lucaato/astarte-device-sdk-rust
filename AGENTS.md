# FFI Binding Improvements - Parent Event References

## Scope of Operation

This operation focuses on improving the FFI bindings for the astarte-device-sdk to prevent use-after-free issues by adding parent event references to all child objects in the `test_binding/send.py` file.

## Problem Statement

The current FFI binding implementation in `test_binding/send.py` creates child objects (like `DeviceValue`, `DeviceData`, etc.) that don't maintain references to their parent `DeviceEvent` object. This can lead to use-after-free scenarios where:

1. A `DeviceEvent` instance is garbage collected
2. Child objects (values, data) still exist and are accessed
3. Accessing the child objects tries to use freed memory, causing crashes or undefined behavior

Note: The event lifetime is independent of the device lifetime - an event can be alive when no device is valid, so child objects should reference the event, not the device.

## Solution Implemented

### 1. Core Changes in `test_binding/send.py`

#### DeviceEvent Class
- No changes to constructor - does not require device parameter
- Passes `self` (the event) to child `DeviceValue.from_cdata()` method

#### DeviceValue Classes
- Added `_event: DeviceEvent | None` field to base `DeviceValue` class
- Modified all subclasses (`DeviceValueIndividual`, `DeviceValueObject`, `DeviceValuePropertySet`, `DeviceValuePropertyUnset`) to:
  - Accept event parameter in constructor
  - Call `super().__init__(event)` to store parent reference
  - Pass event reference to child `DeviceData` objects

#### DeviceData Classes  
- Added `_event: DeviceEvent | None` field to base `DeviceData` class
- Modified `from_cdata()` static method to accept and pass event reference
- Updated all subclasses to accept event parameter and pass to parent constructor
- This includes scalar types (`Double`, `Integer`, `Boolean`, etc.) and array types

#### DeviceBinayBlob Class
- Added `_event: DeviceEvent | None` field to store parent event reference
- Modified constructor to accept event parameter

### 2. Reference Chain

The implementation creates a strong reference chain:

```
DeviceEvent (top-level)
  └── DeviceValue (holds reference to DeviceEvent)
      └── DeviceData (holds reference to DeviceEvent)
          └── DeviceBinayBlob (holds reference to DeviceEvent)
```

This ensures that as long as any child object exists, the parent `DeviceEvent` object will be kept alive, preventing use-after-free scenarios.

## Benefits

1. **Memory Safety**: Prevents use-after-free crashes by maintaining proper object lifetimes
2. **Strong References**: Uses strong references (not weak) to ensure parent stays alive as long as children exist
3. **Backward Compatibility**: All event parameters are optional with `None` defaults, so existing code continues to work
4. **Comprehensive Coverage**: All data classes in the hierarchy now maintain parent references
5. **Correct Lifetime Management**: Child objects reference the event (which has independent lifetime from device) rather than the device

## Files Modified

- `test_binding/send.py`: Main implementation with parent event references added throughout the class hierarchy

## Testing Considerations

- Existing code should continue to work due to optional event parameters
- New code should pass event references to all child objects
- Memory usage may increase slightly due to additional references, but this prevents crashes
- Garbage collection will work correctly as reference cycles are managed by Python's GC