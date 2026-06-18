#ifndef DeviceConfig_D_H
#define DeviceConfig_D_H

#include <stdio.h>
#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "diplomat_runtime.h"





typedef struct DeviceConfig {
  DiplomatStringView device_id;
  DiplomatStringView cred_secr;
  DiplomatStringView realm;
  DiplomatStringView pairing_url;
  DiplomatStringView interfaces_dir;
} DeviceConfig;

typedef struct DeviceConfig_option {union { DeviceConfig ok; }; bool is_ok; } DeviceConfig_option;



#endif // DeviceConfig_D_H
