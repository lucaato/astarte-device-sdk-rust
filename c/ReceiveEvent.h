#ifndef ReceiveEvent_H
#define ReceiveEvent_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "diplomat_runtime.h"

#include "DeviceIndividualValue.d.h"
#include "DeviceValueType.d.h"

#include "ReceiveEvent.d.h"






typedef struct ReceiveEvent_interface_result { bool is_ok;} ReceiveEvent_interface_result;
ReceiveEvent_interface_result ReceiveEvent_interface(const ReceiveEvent* self, DiplomatWrite* write);

typedef struct ReceiveEvent_path_result { bool is_ok;} ReceiveEvent_path_result;
ReceiveEvent_path_result ReceiveEvent_path(const ReceiveEvent* self, DiplomatWrite* write);

DeviceValueType ReceiveEvent_value_type(const ReceiveEvent* self);

DeviceIndividualValue* ReceiveEvent_as_individual(const ReceiveEvent* self);

void ReceiveEvent_destroy(ReceiveEvent* self);





#endif // ReceiveEvent_H
