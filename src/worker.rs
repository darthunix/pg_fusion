use crate::ipc::{init_shmem, Bus, Slot};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::{MyProcNumber, ProcNumber};
use pgrx::prelude::*;
use std::cell::OnceCell;

static mut WORKER_NUMBER: OnceCell<ProcNumber> = OnceCell::new();

pub(crate) fn worker_id() -> ProcNumber {
    unsafe { *WORKER_NUMBER.get().unwrap() }
}

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
pub extern "C" fn worker_main(_arg: pg_sys::Datum) {
    unsafe {
        WORKER_NUMBER.set(MyProcNumber).unwrap();
    }
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    while BackgroundWorker::wait_latch(None) {
        log!("DataFusion worker is running");
        for slot in Bus::new().into_iter() {
            let Some(slot) = slot else {
                continue;
            };

            slot.unlock();
        }
    }
}
