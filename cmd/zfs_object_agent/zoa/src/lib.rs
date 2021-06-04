use std::ffi::CStr;
use std::os::raw::c_char;
use zoa_common::init;

#[no_mangle]
pub extern "C" fn libzoa_init(socket_dir_ptr: *const c_char, log_file_ptr: *const c_char) {
    let socket_dir = unsafe {
        CStr::from_ptr(socket_dir_ptr)
            .to_string_lossy()
            .into_owned()
    };
    let log_file = unsafe { CStr::from_ptr(log_file_ptr).to_string_lossy().into_owned() };

    let verbosity = 2;
    let file_name = Some(log_file.as_str());

    init::setup_logging(verbosity, file_name);
    init::start(socket_dir.as_str(), None);
}
