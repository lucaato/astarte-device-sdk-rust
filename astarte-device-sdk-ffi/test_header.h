#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

typedef enum CValueType {
  Individual,
  Object,
  PropertySet,
  PropertyUnset,
} CValueType;

typedef enum CMappingType {
  /**
   * Double mapping.
   */
  Double,
  /**
   * Integer mapping.
   */
  Integer,
  /**
   * Boolean mapping.
   */
  Boolean,
  /**
   * Long integers mapping.
   */
  LongInteger,
  /**
   * String mapping.
   */
  String,
  /**
   * Binary mapping.
   */
  BinaryBlob,
  /**
   * Date time mapping.
   */
  DateTime,
  /**
   * Double array mapping.
   */
  DoubleArray,
  /**
   * Integer array mapping.
   */
  IntegerArray,
  /**
   * Boolean array mapping.
   */
  BooleanArray,
  /**
   * Long integer array mapping.
   */
  LongIntegerArray,
  /**
   * String array mapping.
   */
  StringArray,
  /**
   * Binary array mapping.
   */
  BinaryBlobArray,
  /**
   * Date time array mapping.
   */
  DateTimeArray,
} CMappingType;

typedef struct CAstarteData CAstarteData;

typedef struct CDeviceHandle CDeviceHandle;

typedef struct CAstarteDeviceConfig {
  const char *device_id;
  const char *cred_secr;
  const char *realm;
  const char *pairing_url;
  const char *interfaces_dir;
} CAstarteDeviceConfig;

typedef struct CValue {

} CValue;

typedef void (*AstarteDeviceReceiveCallback)(const char *interface,
                                             const char *path,
                                             const struct CValue *value,
                                             void *user_data);

typedef void (*AstarteDeviceSendCallback)(void *user_data);

struct CAstarteData *device_data_int(int32_t value);

struct CAstarteData *device_data_longint(int64_t value);

struct CDeviceHandle *device_client_start(const struct CAstarteDeviceConfig *config);

void device_client_receive(struct CDeviceHandle *device_handle,
                           AstarteDeviceReceiveCallback callback,
                           void *user_data);

void device_client_send_individual(struct CDeviceHandle *device_handle,
                                   const char *interface_name,
                                   const char *path,
                                   struct CAstarteData *data,
                                   AstarteDeviceSendCallback callback,
                                   void *user_data);

void device_client_stop(struct CDeviceHandle *device_handle);

/**
 * Frees a string allocated by the Rust FFI.
 */
void device_event_free_string(char *s);

void device_event_free_value(struct CValue *s);

bool device_event_value_get_value_type(const struct CValue *event, enum CValueType *out_type);

bool device_event_value_get_data_type(const struct CValue *event, enum CMappingType *out_type);

/**
 * Example getter: Retrieves an integer value if the underlying data is an Integer.
 * Returns `true` if successful, `false` if the data was missing or of a different type.
 */
bool device_event_value_get_integer(const struct CValue *event, int32_t *out_val);
