use crate::fsm::executor::StateMachine;
use crate::ipc::{init_shmem, max_backends, Bus, Slot, SlotStream};
use crate::protocol::{read_header, Direction, Flag, Packet};
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::{MyProcNumber, ProcNumber};
use pgrx::prelude::*;
use std::cell::OnceCell;
use tokio::runtime::Builder;

static mut WORKER_NUMBER: OnceCell<ProcNumber> = OnceCell::new();
// FIXME: This should be configurable.
const TOKIO_THREAD_NUMBER: usize = 1;

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
    let rt = Builder::new_multi_thread()
        .worker_threads(TOKIO_THREAD_NUMBER)
        .enable_all()
        .build()
        .unwrap();
    let mut states: Vec<StateMachine> = Vec::with_capacity(max_backends() as usize);
    for _ in 0..max_backends() {
        states.push(StateMachine::new());
    }

    while BackgroundWorker::wait_latch(None) {
        log!("DataFusion worker is running");
        for (id, locked_slot) in Bus::new().into_iter().enumerate() {
            let Some(slot) = locked_slot else {
                continue;
            };
            let mut stream = SlotStream::from(slot);
            let header = read_header(&mut stream);
            if header.direction == Direction::FromWorker {
                continue;
            }
            let machine = &mut states[id];
            match header.packet {
                Packet::Parse => {
                    // TODO: handle long queries that span multiple packets.
                    assert_eq!(header.flag, Flag::Last);
                    match stream.look_ahead(header.length as usize) {
                        Ok(data) => {
                            let query = std::str::from_utf8(data).unwrap();
                            log!("Received query: {}", query);
                        }
                        Err(e) => {}
                    }
                }
                _ => {
                    log!("Received header: {:?}", header.packet);
                }
            }
        }
    }
}
