#ifndef DeviceIndividualValue_H
#define DeviceIndividualValue_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "diplomat_runtime.h"

#include "DeviceMappingType.d.h"

#include "DeviceIndividualValue.d.h"






DeviceMappingType DeviceIndividualValue_get_type(const DeviceIndividualValue* self);

typedef struct DeviceIndividualValue_as_string_result {union {DiplomatStringView ok; }; bool is_ok;} DeviceIndividualValue_as_string_result;
DeviceIndividualValue_as_string_result DeviceIndividualValue_as_string(const DeviceIndividualValue* self);

typedef struct DeviceIndividualValue_as_binary_blob_result {union {DiplomatU8View ok; }; bool is_ok;} DeviceIndividualValue_as_binary_blob_result;
DeviceIndividualValue_as_binary_blob_result DeviceIndividualValue_as_binary_blob(const DeviceIndividualValue* self);

typedef struct DeviceIndividualValue_as_datetime_result {union {int64_t ok; }; bool is_ok;} DeviceIndividualValue_as_datetime_result;
DeviceIndividualValue_as_datetime_result DeviceIndividualValue_as_datetime(const DeviceIndividualValue* self);

typedef struct DeviceIndividualValue_as_double_array_result {union {DiplomatF64View ok; }; bool is_ok;} DeviceIndividualValue_as_double_array_result;
DeviceIndividualValue_as_double_array_result DeviceIndividualValue_as_double_array(const DeviceIndividualValue* self);

typedef struct DeviceIndividualValue_as_integer_array_result {union {DiplomatI32View ok; }; bool is_ok;} DeviceIndividualValue_as_integer_array_result;
DeviceIndividualValue_as_integer_array_result DeviceIndividualValue_as_integer_array(const DeviceIndividualValue* self);

typedef struct DeviceIndividualValue_as_boolean_array_result {union {DiplomatBoolView ok; }; bool is_ok;} DeviceIndividualValue_as_boolean_array_result;
DeviceIndividualValue_as_boolean_array_result DeviceIndividualValue_as_boolean_array(const DeviceIndividualValue* self);

int64_t DeviceIndividualValue_get_timestamp(const DeviceIndividualValue* self);

void DeviceIndividualValue_destroy(DeviceIndividualValue* self);





#endif // DeviceIndividualValue_H
