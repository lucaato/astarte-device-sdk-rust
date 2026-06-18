project(
  'astarte_device_lib',
  'cpp',
  version: '0.0.1',
  meson_version: '>=1.0.0',
  default_options: ['cpp_std=c++20', 'b_ndebug=if-release'],
)

python = import('python').find_installation()
nanobind_dep = dependency('nanobind')

diplomat_inc = include_directories('target/python/include')
nanobind_inc_cmd = run_command(python, '-m', 'nanobind', '--include_dir', check: true)
nanobind_inc = include_directories(nanobind_inc_cmd.stdout().strip())

package = 'astarte-device-sdk-ffi'
library = 'libastarte_device.a'

rust_lib = custom_target(
  'build_rust_lib',
  output: library, 
  command: [
    'sh', '-c',
    'cargo build --release -p astarte-device-sdk-ffi --manifest-path "$1" && cp "$2" "$3"',
    '_', 
    meson.project_source_root() / 'Cargo.toml',                             
    meson.project_source_root() / 'target/release/' / library,
    '@OUTPUT@'
  ],
  build_by_default: true,
  build_always_stale: true, 
)
sqlite3_dep = dependency('sqlite3', required: true)
rust_dep = declare_dependency(
  link_with: rust_lib,  
  dependencies: [sqlite3_dep]
)

diplomat_output = ['target/python/AstarteDeviceLib_ext.cpp',
  'target/python/sub_modules/AstarteDeviceLib/DeviceValueType_binding.cpp',
  'target/python/sub_modules/AstarteDeviceLib/DeviceMappingType_binding.cpp',
  'target/python/sub_modules/AstarteDeviceLib/DeviceIndividualValue_binding.cpp',
  'target/python/sub_modules/AstarteDeviceLib/ReceiveEvent_binding.cpp',
  'target/python/sub_modules/AstarteDeviceLib/SendData_binding.cpp',
  'target/python/sub_modules/AstarteDeviceLib/DeviceConfig_binding.cpp',
  'target/python/sub_modules/AstarteDeviceLib/DeviceHandle_binding.cpp']

diplomat_exe = find_program('diplomat-tool', required: true)
run_command(
  diplomat_exe,
  '-e', meson.project_source_root() / package / 'src/lib.rs',
  '-c', meson.project_source_root() / package / 'diplomat.toml',
  'nanobind',
  meson.project_source_root() / 'target/python',
  check: true
)

module_name = 'AstarteDeviceLib'

mod = python.extension_module(
  module_name,
  sources: files(diplomat_output),
  include_directories: [diplomat_inc, nanobind_inc],
  dependencies: [nanobind_dep, rust_dep],
  install: true,
)

stubgen = nanobind_dep.get_variable('stubgen')

custom_target(
  'astarte_device_module_stubs',
  output: 'astarte_device_module.pyi',
  depends: mod,
  command: [python, stubgen, '-m', module_name, '-M', 'py.typed'],
  env: {'PYTHONPATH': meson.current_build_dir()},
  build_by_default: true
)

