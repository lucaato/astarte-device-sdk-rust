#ifndef DeviceValueType_D_H
#define DeviceValueType_D_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "diplomat_runtime.h"





typedef enum DeviceValueType {
  DeviceValueType_Individual = 0,
  DeviceValueType_Object = 1,
  DeviceValueType_Property = 2,
} DeviceValueType;

typedef struct DeviceValueType_option {union { DeviceValueType ok; }; bool is_ok; } DeviceValueType_option;



#endif // DeviceValueType_D_H
