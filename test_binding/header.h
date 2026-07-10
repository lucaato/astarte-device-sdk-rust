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

typedef enum NativeStringResult_NativeDeviceHandle_Tag {
  Ok_NativeDeviceHandle,
  Err_NativeDeviceHandle,
} NativeStringResult_NativeDeviceHandle_Tag;

typedef struct NativeStringResult_NativeDeviceHandle {
  NativeStringResult_NativeDeviceHandle_Tag tag;
  union {
    struct {
      NativeDeviceHandle ok;
    };
    struct {
      StaticString err;
    };
  };
} NativeStringResult_NativeDeviceHandle;

typedef void *UserData;

typedef void (*DeviceHandleConnectCallback)(const struct NativeStringResult_NativeDeviceHandle *result,
                                            UserData user_data);

typedef enum NativeStringResult_bool_Tag {
  Ok_bool,
  Err_bool,
} NativeStringResult_bool_Tag;

typedef struct NativeStringResult_bool {
  NativeStringResult_bool_Tag tag;
  union {
    struct {
      bool ok;
    };
    struct {
      StaticString err;
    };
  };
} NativeStringResult_bool;

typedef void (*DeviceHandleLoopCallback)(const struct NativeStringResult_bool *result,
                                         UserData user_data);

typedef void (*DeviceHandleDisconnectCallback)(const struct NativeStringResult_bool *result,
                                               UserData user_data);

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_u8 {
  /**
   * Pointer to the first element of the array
   */
  const uint8_t *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_u8;

typedef int64_t NativeTimestamp;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_f64 {
  /**
   * Pointer to the first element of the array
   */
  const double *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_f64;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_i32 {
  /**
   * Pointer to the first element of the array
   */
  const int32_t *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_i32;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_bool {
  /**
   * Pointer to the first element of the array
   */
  const bool *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_bool;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_i64 {
  /**
   * Pointer to the first element of the array
   */
  const int64_t *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_i64;

/**
 * A utility type to represent arrays of string
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, CStringArray};
 * let pizza_names = vec!["Diavola".to_string(), "Margarita".to_string(), "Regina".to_string()];
 * let c_pizza_names = CStringArray::c_repr_of(pizza_names).expect("could not convert !");
 *
 * ```
 */
typedef struct CStringArray {
  /**
   * Pointer to the first element of the array
   */
  const char *const *data;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CStringArray;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_CArray_u8 {
  /**
   * Pointer to the first element of the array
   */
  const struct CArray_u8 *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_CArray_u8;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_NativeTimestamp {
  /**
   * Pointer to the first element of the array
   */
  const NativeTimestamp *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_NativeTimestamp;

typedef enum NativeDeviceData_Tag {
  Double,
  Integer,
  Boolean,
  LongInteger,
  String,
  BinaryBlob,
  DateTime,
  DoubleArray,
  IntegerArray,
  BooleanArray,
  LongIntegerArray,
  StringArray,
  BinaryBlobArray,
  DateTimeArray,
} NativeDeviceData_Tag;

typedef struct NativeDeviceData {
  NativeDeviceData_Tag tag;
  union {
    struct {
      double double_;
    };
    struct {
      int32_t integer;
    };
    struct {
      bool boolean;
    };
    struct {
      int64_t long_integer;
    };
    struct {
      const char *string;
    };
    struct {
      struct CArray_u8 binary_blob;
    };
    struct {
      NativeTimestamp date_time;
    };
    struct {
      struct CArray_f64 double_array;
    };
    struct {
      struct CArray_i32 integer_array;
    };
    struct {
      struct CArray_bool boolean_array;
    };
    struct {
      struct CArray_i64 long_integer_array;
    };
    struct {
      struct CStringArray string_array;
    };
    struct {
      struct CArray_CArray_u8 binary_blob_array;
    };
    struct {
      struct CArray_NativeTimestamp date_time_array;
    };
  };
} NativeDeviceData;

typedef struct NativeObjectEntry {
  const char *path;
  struct NativeDeviceData value;
} NativeObjectEntry;

/**
 * A utility type to represent arrays of the parametrized type.
 * Note that the parametrized type should have a C-compatible representation.
 *
 * # Example
 *
 * ```
 * use ffi_convert::{CReprOf, AsRust, CDrop, CArray};
 * use libc::c_char;
 *
 * pub struct PizzaTopping {
 *     pub ingredient: String,
 * }
 *
 * #[derive(CDrop, CReprOf, AsRust)]
 * #[target_type(PizzaTopping)]
 * pub struct CPizzaTopping {
 *     pub ingredient: *const c_char
 * }
 *
 * let toppings = vec![
 *         PizzaTopping { ingredient: "Cheese".to_string() },
 *         PizzaTopping { ingredient: "Ham".to_string() } ];
 *
 * let ctoppings = CArray::<CPizzaTopping>::c_repr_of(toppings);
 *
 * ```
 */
typedef struct CArray_NativeObjectEntry {
  /**
   * Pointer to the first element of the array
   */
  const struct NativeObjectEntry *data_ptr;
  /**
   * Number of elements in the array
   */
  uintptr_t size;
} CArray_NativeObjectEntry;

typedef enum NativeValue_Tag {
  Individual,
  Object,
  PropertySet,
  PropertyUnset,
} NativeValue_Tag;

typedef struct Individual_Body {
  struct NativeDeviceData data;
  int64_t timestamp;
} Individual_Body;

typedef struct Object_Body {
  struct CArray_NativeObjectEntry data;
  int64_t timestamp;
} Object_Body;

typedef struct NativeValue {
  NativeValue_Tag tag;
  union {
    Individual_Body individual;
    Object_Body object;
    struct {
      struct NativeDeviceData property_set;
    };
  };
} NativeValue;

typedef struct NativeDeviceEvent {
  const char *interface;
  const char *path;
  struct NativeValue data;
} NativeDeviceEvent;

typedef struct NativeDeviceEvent NativeManuallyDrop_NativeDeviceEvent;

typedef enum NativeStringResult_NativeManuallyDrop_NativeDeviceEvent_Tag {
  Ok_NativeManuallyDrop_NativeDeviceEvent,
  Err_NativeManuallyDrop_NativeDeviceEvent,
} NativeStringResult_NativeManuallyDrop_NativeDeviceEvent_Tag;

typedef struct NativeStringResult_NativeManuallyDrop_NativeDeviceEvent {
  NativeStringResult_NativeManuallyDrop_NativeDeviceEvent_Tag tag;
  union {
    struct {
      NativeManuallyDrop_NativeDeviceEvent ok;
    };
    struct {
      StaticString err;
    };
  };
} NativeStringResult_NativeManuallyDrop_NativeDeviceEvent;

typedef void (*DeviceHandleReceiveCallback)(const struct NativeStringResult_NativeManuallyDrop_NativeDeviceEvent *result,
                                            UserData user_data);

typedef enum NativeOption_NativeTimestamp_Tag {
  Some_NativeTimestamp,
  None_NativeTimestamp,
} NativeOption_NativeTimestamp_Tag;

typedef struct NativeOption_NativeTimestamp {
  NativeOption_NativeTimestamp_Tag tag;
  union {
    struct {
      NativeTimestamp some;
    };
  };
} NativeOption_NativeTimestamp;

typedef struct NativeIndividualSend {
  const char *interface;
  const char *path;
  struct NativeDeviceData data;
  struct NativeOption_NativeTimestamp timestamp;
} NativeIndividualSend;

typedef void (*DeviceHandleSendCallback)(const struct NativeStringResult_bool *result,
                                         UserData user_data);

void device_handle_connect(struct NativeDeviceConfig config,
                           DeviceHandleConnectCallback connect_cbk,
                           UserData connect_user_data,
                           DeviceHandleLoopCallback loop_cbk,
                           UserData loop_user_data);

void device_handle_free(NativeDeviceHandle handle);

void device_handle_disconnect(NativeDeviceHandle handle,
                              DeviceHandleDisconnectCallback disconnect_cbk,
                              UserData user_data);

void device_client_receive(NativeDeviceHandle device_handle,
                           DeviceHandleReceiveCallback callback,
                           UserData user_data);

void device_client_free_device_event(struct NativeDeviceEvent event);

void device_client_send_individual(NativeDeviceHandle device_handle,
                                   const struct NativeIndividualSend *data,
                                   DeviceHandleSendCallback callback,
                                   UserData user_data);
