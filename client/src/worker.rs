use libc::c_void;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::prelude::*;
use std::slice;
use std::sync::Arc;
use std::future;
use std::cell::OnceCell;
use std::ptr;
use std::sync::atomic::{AtomicI32, Ordering};
use tokio::runtime::Builder;

// FIXME: This should be configurable.
const TOKIO_THREAD_NUMBER: usize = 1;
const RECV_CAP: usize = 8192;
const SEND_CAP: usize = 8192;

#[pg_guard]
pub(crate) fn init_datafusion_worker() {
    BackgroundWorkerBuilder::new("datafusion")
        .set_function("worker_main")
        .set_library("pg_fusion")
        .enable_shmem_access(Some(init_shmem))
        .load();
}

#[pg_guard]
#[no_mangle]
pub unsafe extern "C" fn init_shmem() {
    let num = crate::max_backends() as usize;

    // Allocate flags region: one `AtomicBool` per backend connection.
    {
        use server::layout::shared_state_layout;
        let shared = shared_state_layout(num).expect("shared_state_layout");
        let ptr = pgrx::pg_sys::ShmemAlloc(shared.layout.size());
        FLAGS_PTR.set(ptr).ok();
        // Zero-initialize flags
        std::ptr::write_bytes(ptr as *mut u8, 0, shared.layout.size());
    }

    // Allocate connection regions: one `ConnectionLayout` per backend connection.
    {
        use server::layout::connection_layout;
        const RECV_CAP: usize = 8192;
        const SEND_CAP: usize = 8192;
        let layout = connection_layout(RECV_CAP, SEND_CAP).expect("connection_layout");
        let total = layout.layout.size() * num;
        let base = pgrx::pg_sys::ShmemAlloc(total);
        CONNS_PTR.set(base).ok();
        // Zero-initialize and set client pid to i32::MAX
        let base_u8 = base as *mut u8;
        std::ptr::write_bytes(base_u8, 0, total);
        for i in 0..num {
            let conn_base = base_u8.add(i * layout.layout.size());
            let (_, _, client_ptr) = server::layout::connection_ptrs(conn_base, layout);
            (*client_ptr).store(i32::MAX, Ordering::Relaxed);
        }
    }

    // Allocate Treiber stack region.
    {
        use server::layout::{treiber_stack_layout, treiber_stack_ptrs};
        use server::stack::TreiberStack;
        let layout = treiber_stack_layout(num).expect("treiber_stack_layout");
        let base = pgrx::pg_sys::ShmemAlloc(layout.layout.size());
        STACK_PTR.set(base).ok();
        // Initialize memory and construct stack header in place
        let base_u8 = base as *mut u8;
        std::ptr::write_bytes(base_u8, 0, layout.layout.size());
        let (hdr_ptr, next_ptr) = treiber_stack_ptrs(base_u8, layout);
        let stack = unsafe { TreiberStack::new(next_ptr, num) };
        unsafe { ptr::write(hdr_ptr, stack) };
    }

    // Allocate shared server PID cell
    {
        let layout = server::layout::server_pid_layout().expect("server pid layout");
        let base = pgrx::pg_sys::ShmemAlloc(layout.layout.size());
        SERVER_PID_PTR.set(base).ok();
        // zero-init, value will be set in worker_main
        std::ptr::write_bytes(base as *mut u8, 0, layout.layout.size());
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let num = crate::max_backends() as usize;

    // Build SharedState from shared flags
    let flags_slice = unsafe {
        let flags_base = *FLAGS_PTR.get().expect("FLAGS_PTR not set");
        let layout = server::layout::shared_state_layout(num).expect("flags layout");
        let flags_ptr = server::layout::shared_state_flags_ptr(flags_base as *mut u8, layout);
        slice::from_raw_parts(flags_ptr, num)
    };
    let state = Arc::new(server::ipc::SharedState::new(flags_slice));

    // Set server PID in shared memory (OS pid)
    unsafe {
        let base = *SERVER_PID_PTR.get().expect("SERVER_PID_PTR not set") as *mut u8;
        let layout = server::layout::server_pid_layout().expect("server pid layout");
        let pid_ptr = server::layout::server_pid_ptr(base, layout);
        let pid = libc::getpid();
        (*pid_ptr).store(pid as i32, Ordering::Relaxed);
    }

    // Build runtime
    let rt = Builder::new_multi_thread()
        .worker_threads(TOKIO_THREAD_NUMBER)
        .enable_all()
        .build()
        .unwrap();

    // Build connections from shared memory and run tasks
    rt.block_on(async move {
        // Spawn shared signal listener (wakes sockets on SIGUSR1)
        tokio::spawn(server::ipc::signal_listener(Arc::clone(&state)));

        let base = unsafe { *CONNS_PTR.get().expect("CONNS_PTR not set") as *mut u8 };
        let layout = server::layout::connection_layout(RECV_CAP, SEND_CAP).expect("conn layout");
        let srv_pid_ptr = unsafe {
            let base = *SERVER_PID_PTR.get().expect("SERVER_PID_PTR not set") as *mut u8;
            let l = server::layout::server_pid_layout().expect("server pid layout");
            server::layout::server_pid_ptr(base, l)
        };
        for id in 0..num {
            let conn_base = unsafe { base.add(id * layout.layout.size()) };
            let (recv_base, send_base, client_ptr) = unsafe {
                server::layout::connection_ptrs(conn_base, layout)
            };
            let socket = unsafe {
                server::ipc::Socket::from_layout_with_state(
                    id,
                    Arc::clone(&state),
                    recv_base,
                    layout.recv_socket_layout,
                )
            };
            let send_buffer =
                unsafe { server::buffer::LockFreeBuffer::from_layout(send_base, layout.send_buffer_layout) };
            let pid = unsafe { (*client_ptr).load(Ordering::Relaxed) };
            let mut conn = server::server::Connection::new(socket, send_buffer, unsafe { &*srv_pid_ptr })
                .with_client(AtomicI32::new(pid));

            tokio::spawn(async move {
                let mut storage = server::server::Storage::default();
                loop {
                    let res = conn
                        .poll()
                        .await
                        .and_then(|_| conn.process_message(&mut storage));
                    if let Err(err) = res {
                        let ferr = match err.downcast::<server::error::FusionError>() {
                            Ok(f) => f,
                            Err(e) => server::error::FusionError::Other(e),
                        };
                        conn.handle_error(ferr);
                    }
                    // Notify client process; ignore errors (e.g., invalid pid)
                    let _ = conn.signal_client();
                }
            });
        }

        // Keep worker alive indefinitely
        future::pending::<()>().await;
    });
}

// Shared pointers to newly allocated regions
static mut FLAGS_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut CONNS_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut STACK_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut SERVER_PID_PTR: OnceCell<*mut c_void> = OnceCell::new();
