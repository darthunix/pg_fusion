use crate::error::FusionError;
use crate::fsm::executor::StateMachine;
use crate::fsm::{ExecutorEvent, ExecutorOutput};
use crate::ipc::{init_shmem, max_backends, Bus, SlotNumber, SlotStream};
use crate::protocol::{consume_header, read_query, Direction, Flag, Header, Packet};
use anyhow::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::TableReference;
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

enum TaskResult {
    None,
    Parsing((Statement, Vec<TableReference>)),
}

struct WorkerContext {
    statements: Vec<Option<Statement>>,
    states: Vec<StateMachine>,
    tasks: Vec<(SlotNumber, JoinHandle<Result<TaskResult>>)>,
}

impl WorkerContext {
    fn new() -> Self {
        let capacity = max_backends() as usize;
        let tasks = Vec::with_capacity(capacity);
        let mut states = Vec::with_capacity(capacity);
        let mut statements = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            states.push(StateMachine::new());
            statements.push(None);
        }
        Self {
            statements,
            states,
            tasks,
        }
    }

    fn flush(&mut self, slot_id: SlotNumber) {
        let machine = &mut self.states[slot_id as usize];
        let event = ExecutorEvent::Error;
        machine
            .consume(&event)
            .expect("Failed to consume Flush event");
        self.statements[slot_id as usize] = None;
        for (id, task) in &mut self.tasks {
            if *id == slot_id {
                task.abort();
                break;
            }
        }
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn worker_main(_arg: pg_sys::Datum) {
    unsafe {
        WORKER_NUMBER.set(MyProcNumber).unwrap();
    }
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let mut ctx = WorkerContext::new();
    let rt = Builder::new_multi_thread()
        .worker_threads(TOKIO_THREAD_NUMBER)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        while BackgroundWorker::wait_latch(None) {
            log!("DataFusion worker is running");
            for (id, locked_slot) in Bus::new().into_iter().enumerate() {
                let Some(slot) = locked_slot else {
                    continue;
                };
                let mut stream = SlotStream::from(slot);
                let header = consume_header(&mut stream).expect("Failed to consume header");
                if header.direction == Direction::ToBackend {
                    continue;
                }
                let machine = &mut ctx.states[id];
                let event = ExecutorEvent::try_from(&header.packet).expect("Failed to convert packet to event");
                let Ok(output) = machine.consume(&event) else {
                    log!("Failed to consume event: {:?}", event);
                    // FIXME: reset the state machine and send an error message.
                    continue;
                };
                let handle = match output {
                    Some(ExecutorOutput::Sleep) => continue,
                    Some(ExecutorOutput::Parse) => tokio::spawn(parse(header, stream)),
                    _ => {
                        log!("Unexpected output: {:?}", output);
                        panic!("Unexpected output");
                    }
                };
                let task_id = u32::try_from(id).expect("Failed to convert slot id to u32");
                ctx.tasks.push((task_id, handle));
            }
        }
        for (id, task) in ctx.tasks {
            let task_result = task.await.expect("Failed to await task");
            let Ok(result) = task_result else {
                log!("An error occurred while processing the task");
                // FIXME: reset the state machine and send an error message.
                continue;
            };
            match result {
                TaskResult::None => continue,
                TaskResult::Parsing((stmt, tables)) => {
                    ctx.statements[id as usize] = Some(stmt);
                    let machine = &mut ctx.states[id as usize];
                    if tables.is_empty() {
                        // We don't need any table metadata for this query.
                        // So, we can simply compile the statement into a logical plan.
                        let event = ExecutorEvent::Metadata;
                        machine
                            .consume(&event)
                            .expect("Failed to consume Metadata event");
                        // TODO: compile the statement into a logical plan.
                    } else if !request_metadata(id, tables) {
                        log!("Looks like current backend dies and the slot is acquired by another backend");
                        let event = ExecutorEvent::Error;
                        machine
                            .consume(&event)
                            .expect("Failed to consume Error event");
                    }
                }
            }
        }
    });
}

async fn parse(header: Header, mut stream: SlotStream) -> Result<TaskResult> {
    assert_eq!(header.packet, Packet::Parse);
    // TODO: handle long queries that span multiple packets.
    assert_eq!(header.flag, Flag::Last);
    let (query, _) = read_query(&mut stream)?;
    log!("Received query: {}", query);

    let stmts = DFParser::parse_sql(query)?;
    let Some(stmt) = stmts.into_iter().next() else {
        return Err(FusionError::ParseQuery(query.to_string()).into());
    };
    stream.rewind(header.length as usize)?;

    let state = SessionStateBuilder::new().build();
    let tables = state.resolve_table_references(&stmt)?;
    Ok(TaskResult::Parsing((stmt, tables)))
}

fn request_metadata(id: SlotNumber, tables: Vec<TableReference>) -> bool {
    // The slot should be locked before sending the response.
    // Normally this operation can not fail because its backend
    // is waiting for response and should not hold any locks.
    // But if it does it means that the old backend was terminated
    // and some other one acquired the same slot. So, fsm should
    // be reset to initial value.
    let Some(slot) = Bus::new().slot_locked(id) else {
        log!("Failed to lock the slot for table metadata response");
        return false;
    };
    let mut stream = SlotStream::from(slot);
    // TODO: serialize table references with protobuf(?)

    true
}
