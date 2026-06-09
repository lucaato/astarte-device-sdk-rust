import cffi

ffibuilder = cffi.FFI()

# 1. Define the C API (read directly from your header file)
ffibuilder.cdef(r"""
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
""")

ffibuilder.set_source("package._foo", None)   # <=

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
