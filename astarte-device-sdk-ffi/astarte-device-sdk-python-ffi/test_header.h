#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef struct CDeviceHandle CDeviceHandle;

typedef void (*AstarteDeviceConnectionCallback)(void *user_data);

typedef struct CAstarteDeviceConfig {
  const char *device_id;
  const char *cred_secr;
  const char *realm;
  const char *pairing_url;
  const char *interfaces_dir;
  /**
   * Optional callback for a connection event.
   */
  AstarteDeviceConnectionCallback connection_cbk;
} CAstarteDeviceConfig;

struct CDeviceHandle *device_client_start(struct CAstarteDeviceConfig config);

void device_client_stop(struct CDeviceHandle *device_handle);
