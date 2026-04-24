#[cfg(not(test))]
use std::cell::RefCell;
use std::fs;
#[cfg(not(test))]
use std::fs::File;
#[cfg(not(test))]
use std::fs::OpenOptions;
#[cfg(not(test))]
use std::io::Write;
use std::path::Path;
use std::sync::OnceLock;

use backend_service::DiagnosticLogLevel;

static WORKER_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

#[cfg(not(test))]
struct CachedLogFile {
    path: String,
    file: File,
}

#[cfg(not(test))]
thread_local! {
    static BACKEND_LOG_FILE: RefCell<Option<CachedLogFile>> = const { RefCell::new(None) };
}

pub(crate) fn init_tracing_file_logger(path: &str, filter: &str) {
    if WORKER_GUARD.get().is_some() {
        return;
    }

    let path = Path::new(path);
    if let Some(parent) = path.parent() {
        let _ = fs::create_dir_all(parent);
    }

    let (dir, file) = match (path.parent(), path.file_name()) {
        (Some(dir), Some(file)) => (dir.to_path_buf(), file.to_string_lossy().to_string()),
        _ => (Path::new("/tmp").to_path_buf(), "pg_fusion.log".to_string()),
    };

    let file_appender = tracing_appender::rolling::never(dir, file);
    let (writer, guard) = tracing_appender::non_blocking(file_appender);

    let filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn"));

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_writer(writer)
        .finish();

    let _ = tracing::subscriber::set_global_default(subscriber);
    let _ = WORKER_GUARD.set(guard);
}

pub(crate) fn backend_log_enabled(required: DiagnosticLogLevel) -> bool {
    let configured = crate::guc::backend_log_level();
    configured != DiagnosticLogLevel::Off && configured >= required
}

pub(crate) fn write_backend_log(
    required: DiagnosticLogLevel,
    component: &str,
    target: &str,
    message: impl FnOnce() -> String,
) {
    if !backend_log_enabled(required) {
        return;
    }

    #[cfg(test)]
    {
        let _ = (component, target, message);
    }
    #[cfg(not(test))]
    {
        let path = crate::guc::extension_log_path();
        write_backend_log_line(&path, required, component, target, &message());
    }
}

#[cfg(not(test))]
fn write_backend_log_line(
    path: &str,
    level: DiagnosticLogLevel,
    component: &str,
    target: &str,
    message: &str,
) {
    BACKEND_LOG_FILE.with(|slot| {
        let mut cached = slot.borrow_mut();
        if cached.as_ref().is_none_or(|cached| cached.path != path) {
            *cached = open_log_file(path);
        }

        let Some(cached) = cached.as_mut() else {
            return;
        };
        let _ = writeln!(
            cached.file,
            "pid={} component={} level={:?} target={} {}",
            std::process::id(),
            component,
            level,
            target,
            message
        );
    });
}

#[cfg(not(test))]
fn open_log_file(path: &str) -> Option<CachedLogFile> {
    let path_ref = Path::new(path);
    if let Some(parent) = path_ref.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path_ref)
        .ok()?;
    Some(CachedLogFile {
        path: path.to_string(),
        file,
    })
}
