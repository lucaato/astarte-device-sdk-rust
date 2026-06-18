#ifndef DeviceMappingType_D_H
#define DeviceMappingType_D_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "diplomat_runtime.h"





typedef enum DeviceMappingType {
  DeviceMappingType_Double = 0,
  DeviceMappingType_Integer = 1,
  DeviceMappingType_Boolean = 2,
  DeviceMappingType_LongInteger = 3,
  DeviceMappingType_String = 4,
  DeviceMappingType_BinaryBlob = 5,
  DeviceMappingType_DateTime = 6,
  DeviceMappingType_DoubleArray = 7,
  DeviceMappingType_IntegerArray = 8,
  DeviceMappingType_BooleanArray = 9,
  DeviceMappingType_LongIntegerArray = 10,
  DeviceMappingType_StringArray = 11,
  DeviceMappingType_BinaryBlobArray = 12,
  DeviceMappingType_DateTimeArray = 13,
} DeviceMappingType;

typedef struct DeviceMappingType_option {union { DeviceMappingType ok; }; bool is_ok; } DeviceMappingType_option;



#endif // DeviceMappingType_D_H
