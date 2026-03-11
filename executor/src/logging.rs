#[allow(unused_macros)]
macro_rules! log_trace {
    ($target:expr, $($arg:tt)*) => {
        if tracing::enabled!(target: $target, tracing::Level::TRACE) {
            tracing::trace!(target: $target, $($arg)*);
        }
    };
}

#[allow(unused_macros)]
macro_rules! log_debug {
    ($target:expr, $($arg:tt)*) => {
        if tracing::enabled!(target: $target, tracing::Level::DEBUG) {
            tracing::debug!(target: $target, $($arg)*);
        }
    };
}

#[allow(unused_macros)]
macro_rules! log_info {
    ($target:expr, $($arg:tt)*) => {
        if tracing::enabled!(target: $target, tracing::Level::INFO) {
            tracing::info!(target: $target, $($arg)*);
        }
    };
}

#[allow(unused_macros)]
macro_rules! log_warn {
    ($target:expr, $($arg:tt)*) => {
        if tracing::enabled!(target: $target, tracing::Level::WARN) {
            tracing::warn!(target: $target, $($arg)*);
        }
    };
}

#[allow(unused_macros)]
macro_rules! log_error {
    ($target:expr, $($arg:tt)*) => {
        if tracing::enabled!(target: $target, tracing::Level::ERROR) {
            tracing::error!(target: $target, $($arg)*);
        }
    };
}

#[allow(unused_imports)]
pub(crate) use log_debug;
#[allow(unused_imports)]
pub(crate) use log_error;
#[allow(unused_imports)]
pub(crate) use log_info;
#[allow(unused_imports)]
pub(crate) use log_trace;
#[allow(unused_imports)]
pub(crate) use log_warn;
