use anyhow::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::TableReference;
use libc::c_void;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::MyProcNumber;
use pgrx::prelude::*;
use smol_str::{format_smolstr, SmolStr};
use std::cell::OnceCell;
use std::ptr;
use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;

// FIXME: This should be configurable.
const TOKIO_THREAD_NUMBER: usize = 1;
const WORKER_WAIT_TIMEOUT: Duration = Duration::from_millis(100);
const SLOT_WAIT_TIMEOUT: Duration = Duration::from_millis(1);

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
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    unimplemented!();
}

// Shared pointers to newly allocated regions
static mut FLAGS_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut CONNS_PTR: OnceCell<*mut c_void> = OnceCell::new();
static mut STACK_PTR: OnceCell<*mut c_void> = OnceCell::new();
