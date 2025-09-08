use executor::server::{Connection, Storage};
use executor::stack::TreiberStack;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
// Use thread-safe OnceLock for globals instead of OnceCell on a mutable static.
use std::fs;
use std::path::Path;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::runtime::Builder;
use tokio::signal::unix::{signal as unix_signal, SignalKind};

// FIXME: This should be configurable.
const TOKIO_THREAD_NUMBER: usize = 1;
pub(crate) const RECV_CAP: usize = 8192;
pub(crate) const SEND_CAP: usize = 8192;

#[pg_guard]
pub(crate) unsafe extern "C-unwind" fn init_datafusion_worker() {
    info!("Registering DataFusion background worker");
    BackgroundWorkerBuilder::new("datafusion")
        .set_function("worker_main")
        .set_library("pg_fusion")
        .enable_shmem_access(Some(init_shmem))
        .load();
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C-unwind" fn init_shmem() {
    let num = crate::max_backends() as usize;

    // Allocate flags region: one `AtomicBool` per backend connection.
    {
        use executor::layout::shared_state_layout;
        let shared = shared_state_layout(num).expect("shared_state_layout");
        let ptr = pgrx::pg_sys::ShmemAlloc(shared.layout.size());
        FLAGS_PTR.store(ptr as *mut u8, Ordering::Release);
        // Zero-initialize flags
        std::ptr::write_bytes(ptr as *mut u8, 0, shared.layout.size());
        info!(
            "init_shmem: allocated flags region: bytes={} count={}",
            shared.layout.size(),
            num
        );
    }

    // Allocate connection regions: one `ConnectionLayout` per backend connection.
    {
        use executor::layout::connection_layout;
        const RECV_CAP: usize = 8192;
        const SEND_CAP: usize = 8192;
        let layout = connection_layout(RECV_CAP, SEND_CAP).expect("connection_layout");
        let total = layout.layout.size() * num;
        let base = pgrx::pg_sys::ShmemAlloc(total);
        CONNS_PTR.store(base as *mut u8, Ordering::Release);
        // Zero-initialize and set client pid to i32::MAX
        let base_u8 = base as *mut u8;
        std::ptr::write_bytes(base_u8, 0, total);
        for i in 0..num {
            let conn_base = base_u8.add(i * layout.layout.size());
            let (_, _, client_ptr) = executor::layout::connection_ptrs(conn_base, layout);
            (*client_ptr).store(i32::MAX, Ordering::Relaxed);
        }
        info!(
            "init_shmem: allocated connections region: bytes_per_conn={} total_bytes={} recv_cap={} send_cap={} count={}",
            layout.layout.size(), total, RECV_CAP, SEND_CAP, num
        );
    }

    // Allocate Treiber stack region.
    {
        use executor::layout::{treiber_stack_layout, treiber_stack_ptrs};
        let layout = treiber_stack_layout(num).expect("treiber_stack_layout");
        let base = pgrx::pg_sys::ShmemAlloc(layout.layout.size());
        STACK_PTR.store(base as *mut u8, Ordering::Release);
        // Initialize memory and construct stack header in place
        let base_u8 = base as *mut u8;
        std::ptr::write_bytes(base_u8, 0, layout.layout.size());
        let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base_u8, layout);
        let stack = unsafe { TreiberStack::new(next_ptr, num) };
        unsafe { ptr::write(hdr_ptr, stack) };
        info!(
            "init_shmem: allocated treiber stack: bytes={} nodes={}",
            layout.layout.size(),
            num
        );
    }

    // Allocate shared server PID cell
    {
        let layout = executor::layout::server_pid_layout().expect("server pid layout");
        let base = pgrx::pg_sys::ShmemAlloc(layout.layout.size());
        SERVER_PID_PTR.store(base as *mut u8, Ordering::Release);
        // zero-init, value will be set in worker_main
        std::ptr::write_bytes(base as *mut u8, 0, layout.layout.size());
        info!(
            "init_shmem: allocated server pid cell: bytes={}",
            layout.layout.size()
        );
    }
}

fn signal_client(conn: &Connection, id: usize) {
    // Notify client process; ignore errors (e.g., invalid pid)
    if let Err(e) = conn.signal_client() {
        tracing::trace!(connection_id = id, error = %e, "signal_client failed (ignored)");
    } else {
        tracing::trace!(connection_id = id, "client signaled");
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let num = crate::max_backends() as usize;
    init_tracing_file_logger();
    let pid = unsafe { libc::getpid() };
    info!(
        "worker_main: starting DataFusion worker pid={} max_backends={}",
        pid, num
    );

    // Build SharedState from shared flags
    let flags_slice = unsafe {
        let flags_base = FLAGS_PTR.load(Ordering::Acquire);
        assert!(!flags_base.is_null(), "FLAGS_PTR not set");
        let layout = executor::layout::shared_state_layout(num).expect("flags layout");
        let flags_ptr = executor::layout::shared_state_flags_ptr(flags_base, layout);
        slice::from_raw_parts(flags_ptr, num)
    };
    let state = Arc::new(executor::ipc::SharedState::new(flags_slice));
    info!(
        "worker_main: SharedState ready (flags={})",
        flags_slice.len()
    );

    // Set server PID in shared memory (OS pid)
    unsafe {
        let base = SERVER_PID_PTR.load(Ordering::Acquire);
        assert!(!base.is_null(), "SERVER_PID_PTR not set");
        let layout = executor::layout::server_pid_layout().expect("server pid layout");
        let pid_ptr = executor::layout::server_pid_ptr(base, layout);
        let pid = libc::getpid();
        (*pid_ptr).store(pid as i32, Ordering::Relaxed);
        info!("worker_main: published server pid={}", pid);
    }

    // Build runtime
    let rt = Builder::new_multi_thread()
        .worker_threads(TOKIO_THREAD_NUMBER)
        .enable_all()
        .build()
        .unwrap();
    info!(
        "worker_main: tokio runtime built, threads={}",
        TOKIO_THREAD_NUMBER
    );

    // Build connections from shared memory and run tasks
    rt.block_on(async move {
        // Spawn shared signal listener (wakes sockets on SIGUSR1)
        tokio::spawn(executor::ipc::signal_listener(Arc::clone(&state)));

        let base = CONNS_PTR.load(Ordering::Acquire);
        assert!(!base.is_null(), "CONNS_PTR not set");
        let layout = executor::layout::connection_layout(RECV_CAP, SEND_CAP).expect("conn layout");
        let srv_pid_ptr = unsafe {
            let base = SERVER_PID_PTR.load(Ordering::Acquire);
            assert!(!base.is_null(), "SERVER_PID_PTR not set");
            let l = executor::layout::server_pid_layout().expect("server pid layout");
            executor::layout::server_pid_ptr(base, l)
        };
        for id in 0..num {
            let conn_base = unsafe { base.add(id * layout.layout.size()) };
            let (recv_base, send_base, client_ptr) =
                unsafe { executor::layout::connection_ptrs(conn_base, layout) };
            let socket = unsafe {
                executor::ipc::Socket::from_layout_with_state(
                    id,
                    Arc::clone(&state),
                    recv_base,
                    layout.recv_socket_layout,
                )
            };
            let send_buffer = unsafe {
                executor::buffer::LockFreeBuffer::from_layout(send_base, layout.send_buffer_layout)
            };
            let mut conn = Connection::new(socket, send_buffer, unsafe { &*srv_pid_ptr }, unsafe {
                &*client_ptr
            });

            tokio::spawn(async move {
                tracing::trace!(connection_id = id, "connection task started");
                let mut storage = Storage::default();
                loop {
                    let res = match conn.poll().await {
                        Ok(_) => {
                            tracing::trace!(connection_id = id, "processing message");
                            conn.process_message(&mut storage)
                        }
                        Err(e) => Err(e),
                    };
                    match res {
                        Ok(()) => tracing::trace!(connection_id = id, "message processed"),
                        Err(err) => {
                            let ferr = match err.downcast::<common::FusionError>() {
                                Ok(f) => f,
                                Err(e) => common::FusionError::Other(e),
                            };
                            tracing::error!(connection_id = id, error = %ferr, "processing error");
                            storage.flush();
                            conn.handle_error(ferr);
                            signal_client(&conn, id);
                        }
                    }
                    signal_client(&conn, id);
                }
            });
        }

        // Keep worker alive until a termination signal arrives
        let mut sigterm = unix_signal(SignalKind::terminate()).expect("sigterm is not supported");
        let mut sigint = unix_signal(SignalKind::interrupt()).ok();
        let mut sigquit = unix_signal(SignalKind::quit()).ok();
        let mut sighup = unix_signal(SignalKind::hangup()).ok();

        loop {
            tokio::select! {
                _ = sigterm.recv() => break,
                // Some systems may deliver INT/QUIT; treat as shutdown as well.
                _ = async { if let Some(s) = &mut sigint { s.recv().await; } } => break,
                _ = async { if let Some(s) = &mut sigquit { s.recv().await; } } => break,
                // SIGHUP typically for reload; ignore and continue waiting.
                _ = async { if let Some(s) = &mut sighup { s.recv().await; } } => {},
            }
        }
    });
}

// Initialize a global tracing subscriber that writes to a file. Safe to call multiple times.
fn init_tracing_file_logger() {
    static GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();
    if GUARD.get().is_some() {
        return;
    }
    let path =
        std::env::var("PG_FUSION_LOG").unwrap_or_else(|_| "/tmp/pg_fusion_worker.log".to_string());
    let p = Path::new(&path);
    if let Some(parent) = p.parent() {
        let _ = fs::create_dir_all(parent);
    }
    let (dir, file) = match (p.parent(), p.file_name()) {
        (Some(d), Some(f)) => (d.to_path_buf(), f.to_string_lossy().to_string()),
        _ => (
            Path::new("/tmp").to_path_buf(),
            "pg_fusion_worker.log".to_string(),
        ),
    };

    let file_appender = tracing_appender::rolling::never(dir, file);
    let (nb, guard) = tracing_appender::non_blocking(file_appender);

    let filter = tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        // Default: verbose for our crates; info elsewhere
        tracing_subscriber::EnvFilter::new("info,pg_fusion=trace,executor=trace")
    });

    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(false)
        .with_target(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_writer(nb)
        .finish();

    // Ignore if already set; we only attempt once due to OnceLock check above.
    let _ = tracing::subscriber::set_global_default(subscriber);
    let _ = GUARD.set(guard);
}

// Shared pointers to newly allocated regions (atomic raw pointers)
static FLAGS_PTR: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
static CONNS_PTR: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
static STACK_PTR: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());
static SERVER_PID_PTR: AtomicPtr<u8> = AtomicPtr::new(core::ptr::null_mut());

// Accessor for Treiber stack stored in shared memory
pub(crate) fn treiber_stack() -> &'static TreiberStack {
    unsafe {
        let base = STACK_PTR.load(Ordering::Acquire);
        assert!(!base.is_null(), "STACK_PTR not set");
        let layout = executor::layout::treiber_stack_layout(crate::max_backends() as usize)
            .expect("treiber_stack_layout");
        let (hdr_ptr, _next_ptr) = executor::layout::treiber_stack_ptrs(base, layout);
        &*hdr_ptr
    }
}

pub(crate) fn shared_flags_slice() -> &'static [AtomicBool] {
    let num = crate::max_backends() as usize;
    unsafe {
        let flags_base = FLAGS_PTR.load(Ordering::Acquire);
        assert!(!flags_base.is_null(), "FLAGS_PTR not set");
        let layout = executor::layout::shared_state_layout(num).expect("flags layout");
        let flags_ptr = executor::layout::shared_state_flags_ptr(flags_base, layout);
        std::slice::from_raw_parts(flags_ptr, num)
    }
}

pub(crate) fn connections_layout() -> executor::layout::ConnectionLayout {
    executor::layout::connection_layout(RECV_CAP, SEND_CAP).expect("conn layout")
}

pub(crate) fn connections_base() -> *mut u8 {
    CONNS_PTR.load(Ordering::Acquire)
}

pub(crate) fn server_pid_atomic() -> &'static AtomicI32 {
    unsafe {
        let base = SERVER_PID_PTR.load(Ordering::Acquire);
        assert!(!base.is_null(), "SERVER_PID_PTR not set");
        let layout = executor::layout::server_pid_layout().expect("server pid layout");
        let pid_ptr = executor::layout::server_pid_ptr(base, layout);
        &*pid_ptr
    }
}
