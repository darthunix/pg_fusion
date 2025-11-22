use executor::server::{Connection, Storage};
use executor::pgscan::ScanRegistry;
use executor::stack::TreiberStack;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::pg_sys::AsPgCStr;
use std::fs;
use std::path::Path;
use std::ptr;
use std::slice;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::runtime::Builder;
use tokio::signal::unix::{signal as unix_signal, SignalKind};

// FIXME: This should be configurable.
const TOKIO_THREAD_NUMBER: usize = 1;
pub(crate) const RECV_CAP: usize = 8192;
pub(crate) const SEND_CAP: usize = 8192;
// Heap block buffer configuration per connection
pub(crate) const SLOTS_PER_CONN: usize = 2; // two logical slots per connection
pub(crate) const BLOCKS_PER_SLOT: usize = 2; // double buffering inside each slot
pub(crate) const RESULT_RING_CAP: usize = 64 * 1024; // bytes per-connection for result rows

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

    // Flags region: one AtomicBool per backend connection.
    {
        use executor::layout::shared_state_layout;
        let shared = shared_state_layout(num).expect("shared_state_layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:flags".as_pg_cstr(),
            shared.layout.size(),
            &mut found,
        ) as *mut u8;
        if !found {
            std::ptr::write_bytes(base, 0, shared.layout.size());
        }
        info!(
            "init_shmem: flags region ready: bytes={} count={} found={}",
            shared.layout.size(),
            num,
            found
        );
    }

    // Connection regions: one ConnectionLayout per backend connection.
    {
        use executor::layout::connection_layout;
        const RECV_CAP: usize = 8192;
        const SEND_CAP: usize = 8192;
        let layout = connection_layout(RECV_CAP, SEND_CAP).expect("connection_layout");
        let total = layout.layout.size() * num;
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:conns".as_pg_cstr(),
            total,
            &mut found,
        ) as *mut u8;
        if !found {
            std::ptr::write_bytes(base, 0, total);
            for i in 0..num {
                let conn_base = unsafe { base.add(i * layout.layout.size()) };
                let (_, _, client_ptr) = executor::layout::connection_ptrs(conn_base, layout);
                unsafe { (*client_ptr).store(i32::MAX, Ordering::Relaxed) };
            }
        }
        info!(
            "init_shmem: connections region ready: bytes_per_conn={} total_bytes={} recv_cap={} send_cap={} count={} found={}",
            layout.layout.size(), total, RECV_CAP, SEND_CAP, num, found
        );
    }

    // Per-connection heap block buffers (SlotBlocksLayout).
    {
        use executor::layout::slot_blocks_layout;
        let blksz = pgrx::pg_sys::BLCKSZ as usize;
        let layout =
            slot_blocks_layout(SLOTS_PER_CONN, blksz, BLOCKS_PER_SLOT).expect("slot_blocks_layout");
        let total = layout.layout.size() * num;
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:slot_blocks".as_pg_cstr(),
            total,
            &mut found,
        ) as *mut u8;
        if !found {
            std::ptr::write_bytes(base, 0, total);
        }
        // Publish base and layout to executor module for in-process access
        executor::shm::set_slot_blocks(base, layout);
        info!(
            "init_shmem: slot blocks ready: bytes_per_conn={} total_bytes={} slots_per_conn={} blocks_per_slot={} blksz={} count={} found={}",
            layout.layout.size(), total, SLOTS_PER_CONN, BLOCKS_PER_SLOT, blksz, num, found
        );
    }

    // Treiber stack region.
    {
        use executor::layout::{treiber_stack_layout, treiber_stack_ptrs};
        let layout = treiber_stack_layout(num).expect("treiber_stack_layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:stack".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
        let base_u8 = base as *mut u8;
        if !found {
            std::ptr::write_bytes(base_u8, 0, layout.layout.size());
            let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base_u8, layout);
            let stack = TreiberStack::new(next_ptr, num);
            ptr::write(hdr_ptr, stack);
        }
        info!(
            "init_shmem: treiber stack ready: bytes={} nodes={} found={}",
            layout.layout.size(),
            num,
            found
        );
    }

    // Shared server PID cell
    {
        let layout = executor::layout::server_pid_layout().expect("server pid layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:server_pid".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
        if !found {
            std::ptr::write_bytes(base as *mut u8, 0, layout.layout.size());
        }
        info!(
            "init_shmem: server pid cell ready: bytes={} found={}",
            layout.layout.size(),
            found
        );
    }

    // Per-connection result ring buffers.
    {
        let layout =
            executor::layout::result_ring_layout(RESULT_RING_CAP).expect("result_ring_layout");
        let total = layout.layout.size() * num;
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:result_ring".as_pg_cstr(),
            total,
            &mut found,
        ) as *mut u8;
        if !found {
            std::ptr::write_bytes(base, 0, total);
        }
        // Publish base and layout to executor module
        executor::shm::set_result_ring(base, layout);
        info!(
            "init_shmem: result ring ready: bytes_per_conn={} total_bytes={} cap={} count={} found={}",
            layout.layout.size(),
            total,
            RESULT_RING_CAP,
            num,
            found
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
        let layout = executor::layout::shared_state_layout(num).expect("flags layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:flags".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
        let flags_ptr = executor::layout::shared_state_flags_ptr(base, layout);
        slice::from_raw_parts(flags_ptr, num)
    };
    let state = Arc::new(executor::ipc::SharedState::new(flags_slice));
    info!(
        "worker_main: SharedState ready (flags={})",
        flags_slice.len()
    );

    // Set server PID in shared memory (OS pid)
    unsafe {
        let layout = executor::layout::server_pid_layout().expect("server pid layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:server_pid".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
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

        let layout = executor::layout::connection_layout(RECV_CAP, SEND_CAP).expect("conn layout");
        let mut conns_found = false;
        let base = unsafe {
            pgrx::pg_sys::ShmemInitStruct(
                "pg_fusion:conns".as_pg_cstr(),
                layout.layout.size() * num,
                &mut conns_found,
            ) as *mut u8
        };
        let srv_pid_ptr = unsafe {
            let l = executor::layout::server_pid_layout().expect("server pid layout");
            let mut found = false;
            let base = pgrx::pg_sys::ShmemInitStruct(
                "pg_fusion:server_pid".as_pg_cstr(),
                l.layout.size(),
                &mut found,
            ) as *mut u8;
            executor::layout::server_pid_ptr(base, l)
        };
        for id in 0..num {
            let conn_base = unsafe { base.add(id * layout.layout.size()) };
            let (recv_base, send_base, client_ptr) =
                unsafe { executor::layout::connection_ptrs(conn_base, layout) };
            tracing::trace!(
                connection_id = id,
                recv_base = ?recv_base,
                send_base = ?send_base,
                "worker_main: connection bases"
            );
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
            let mut conn =
                Connection::new(id, socket, send_buffer, unsafe { &*srv_pid_ptr }, unsafe {
                    &*client_ptr
                });
            // Attach result ring buffer for this connection
            let res_layout = result_ring_layout();
            let mut rr_found = false;
            let rr_base = unsafe {
                pgrx::pg_sys::ShmemInitStruct(
                    "pg_fusion:result_ring".as_pg_cstr(),
                    res_layout.layout.size() * num,
                    &mut rr_found,
                ) as *mut u8
            };
            let res_base = unsafe { rr_base.add(id * res_layout.layout.size()) };
            let res_buf =
                unsafe { executor::buffer::LockFreeBuffer::from_layout(res_base, res_layout) };
            conn.set_result_buffer(res_buf);

            tokio::spawn(async move {
                tracing::trace!(connection_id = id, "connection task started");
                let mut storage = Storage::default();
                // Ensure registry knows this connection id for SHM addressing
                storage.set_registry_conn(id);
                loop {
                    let res = match conn.poll().await {
                        Ok(_) => {
                            tracing::trace!(connection_id = id, "processing message");
                            conn.process_message(&mut storage).await
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

// Accessor for Treiber stack stored in shared memory
pub(crate) fn treiber_stack() -> &'static TreiberStack {
    unsafe {
        let num = crate::max_backends() as usize;
        let layout = executor::layout::treiber_stack_layout(num).expect("treiber_stack_layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:stack".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
        let (hdr_ptr, _next_ptr) = executor::layout::treiber_stack_ptrs(base, layout);
        &*hdr_ptr
    }
}

pub(crate) fn shared_flags_slice() -> &'static [AtomicBool] {
    let num = crate::max_backends() as usize;
    unsafe {
        let layout = executor::layout::shared_state_layout(num).expect("flags layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:flags".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
        let flags_ptr = executor::layout::shared_state_flags_ptr(base, layout);
        std::slice::from_raw_parts(flags_ptr, num)
    }
}

pub(crate) fn connections_layout() -> executor::layout::ConnectionLayout {
    executor::layout::connection_layout(RECV_CAP, SEND_CAP).expect("conn layout")
}

pub(crate) fn connections_base() -> *mut u8 {
    let num = crate::max_backends() as usize;
    let layout = connections_layout();
    let mut found = false;
    unsafe {
        pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:conns".as_pg_cstr(),
            layout.layout.size() * num,
            &mut found,
        ) as *mut u8
    }
}

pub(crate) fn slot_blocks_layout() -> executor::layout::SlotBlocksLayout {
    let blksz = pgrx::pg_sys::BLCKSZ as usize;
    executor::layout::slot_blocks_layout(SLOTS_PER_CONN, blksz, BLOCKS_PER_SLOT)
        .expect("slot_blocks_layout")
}

pub(crate) fn slot_blocks_base() -> *mut u8 {
    let num = crate::max_backends() as usize;
    let layout = slot_blocks_layout();
    let mut found = false;
    unsafe {
        pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:slot_blocks".as_pg_cstr(),
            layout.layout.size() * num,
            &mut found,
        ) as *mut u8
    }
}

/// Return the base pointer for the slot blocks region of a specific connection id.
pub(crate) fn slot_blocks_base_for(conn_id: usize) -> *mut u8 {
    unsafe {
        let layout = slot_blocks_layout();
        let num = crate::max_backends() as usize;
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:slot_blocks".as_pg_cstr(),
            layout.layout.size() * num,
            &mut found,
        ) as *mut u8;
        base.add(conn_id * layout.layout.size())
    }
}

pub(crate) fn server_pid_atomic() -> &'static AtomicI32 {
    unsafe {
        let layout = executor::layout::server_pid_layout().expect("server pid layout");
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:server_pid".as_pg_cstr(),
            layout.layout.size(),
            &mut found,
        ) as *mut u8;
        let pid_ptr = executor::layout::server_pid_ptr(base, layout);
        &*pid_ptr
    }
}

pub(crate) fn result_ring_layout() -> executor::layout::BufferLayout {
    executor::layout::result_ring_layout(RESULT_RING_CAP).expect("result ring layout")
}

pub(crate) fn result_ring_base_for(conn_id: usize) -> *mut u8 {
    unsafe {
        let layout = result_ring_layout();
        let num = crate::max_backends() as usize;
        let mut found = false;
        let base = pgrx::pg_sys::ShmemInitStruct(
            "pg_fusion:result_ring".as_pg_cstr(),
            layout.layout.size() * num,
            &mut found,
        ) as *mut u8;
        base.add(conn_id * layout.layout.size())
    }
}
