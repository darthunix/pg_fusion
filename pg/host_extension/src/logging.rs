use std::fs;
use std::path::Path;
use std::sync::OnceLock;

static WORKER_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

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
        _ => (
            Path::new("/tmp").to_path_buf(),
            "pg_fusion_worker.log".to_string(),
        ),
    };

    let file_appender = tracing_appender::rolling::never(dir, file);
    let (writer, guard) = tracing_appender::non_blocking(file_appender);

    let filter = tracing_subscriber::EnvFilter::try_new(filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

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
