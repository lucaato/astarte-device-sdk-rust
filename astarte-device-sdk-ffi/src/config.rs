use std::ffi::c_char;

use ffi_convert::AsRust;

pub struct DeviceConfig {
    pub device_id: String,
    pub cred_secr: String,
    pub realm: String,
    pub pairing_url: String,
    pub interfaces_dir: String,
}

#[repr(C)]
#[derive(AsRust)]
#[target_type(DeviceConfig)]
pub struct NativeDeviceConfig {
    pub device_id: *const c_char,
    pub cred_secr: *const c_char,
    pub realm: *const c_char,
    pub pairing_url: *const c_char,
    pub interfaces_dir: *const c_char,
}
unsafe impl Send for NativeDeviceConfig {}
