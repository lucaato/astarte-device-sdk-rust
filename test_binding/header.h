#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct NativeDeviceConfig {
  const char *device_id;
  const char *cred_secr;
  const char *realm;
  const char *pairing_url;
  const char *interfaces_dir;
} NativeDeviceConfig;

typedef struct OpaqueDeviceHadle {

} OpaqueDeviceHadle;

typedef struct OpaqueDeviceHadle *NativeDeviceHandle;

typedef const char *StaticString;

typedef enum NativeResult_NativeDeviceHandle_Tag {
  Ok_NativeDeviceHandle,
  Err_NativeDeviceHandle,
} NativeResult_NativeDeviceHandle_Tag;

typedef struct NativeResult_NativeDeviceHandle {
  NativeResult_NativeDeviceHandle_Tag tag;
  union {
    struct {
      NativeDeviceHandle ok;
    };
    struct {
      StaticString err;
    };
  };
} NativeResult_NativeDeviceHandle;

typedef void *UserData;

typedef void (*DeviceHandleConnectCallback)(const struct NativeResult_NativeDeviceHandle *result,
                                            UserData user_data);

typedef enum NativeResult_bool_Tag {
  Ok_bool,
  Err_bool,
} NativeResult_bool_Tag;

typedef struct NativeResult_bool {
  NativeResult_bool_Tag tag;
  union {
    struct {
      bool ok;
    };
    struct {
      StaticString err;
    };
  };
} NativeResult_bool;

typedef void (*DeviceHandleLoopCallback)(const struct NativeResult_bool *result, UserData user_data);

typedef void (*DeviceHandleDisconnectCallback)(const struct NativeResult_bool *result,
                                               UserData user_data);

void device_handle_connect(struct NativeDeviceConfig config,
                           DeviceHandleConnectCallback connect_cbk,
                           UserData connect_user_data,
                           DeviceHandleLoopCallback loop_cbk,
                           UserData loop_user_data);

void device_handle_disconnect(NativeDeviceHandle handle,
                              DeviceHandleDisconnectCallback disconnect_cbk,
                              UserData user_data);
