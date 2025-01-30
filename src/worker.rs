use crate::fsm::executor::StateMachine;
use crate::fsm::{ExecutorEvent, ExecutorOutput};
use crate::ipc::{init_shmem, max_backends, Bus, Slot, SlotStream};
use crate::protocol::{read_header, Direction, Flag, Header, Packet};
use anyhow::Result;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::{MyProcNumber, ProcNumber};
use pgrx::prelude::*;
use std::cell::OnceCell;
use tokio::runtime::Builder;
use tokio::task::JoinHandle;

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
    let mut tasks = Vec::with_capacity(max_backends() as usize);

    rt.block_on(async {
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
                let event = ExecutorEvent::from(&header.packet);
                let Ok(output) = machine.consume(&event) else {
                    log!("Failed to consume event: {:?}", event);
                    // TODO: reset the state machine and send an error message.
                    continue;
                };
                let handle = match output {
                    Some(ExecutorOutput::Sleep) => continue,
                    Some(ExecutorOutput::BuildAst) => tokio::spawn(build_ast(header, stream)),
                    _ => {
                        log!("Unexpected output: {:?}", output);
                        panic!("Unexpected output");
                    }
                };
                tasks.push(handle);
            }
        }
        for task in tasks {
            let _ = task.await.unwrap();
        }
    });
}

async fn build_ast(header: Header, mut stream: SlotStream) -> Result<()> {
    // TODO: handle long queries that span multiple packets.
    assert_eq!(header.flag, Flag::Last);
    let buffer = stream.look_ahead(header.length as usize)?;
    let query = std::str::from_utf8(buffer)?;
    log!("Received query: {}", query);
    stream.rewind(header.length as usize)?;
    // TODO: compile the query.
    Ok(())
}
