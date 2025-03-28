use crate::error::FusionError;
use crate::fsm::executor::StateMachine;
use crate::fsm::ExecutorOutput;
use crate::ipc::{init_shmem, max_backends, Bus, SlotNumber, SlotStream, INVALID_SLOT_NUMBER};
use crate::protocol::{consume_header, read_query, send_error, Direction, Flag, Header, Packet};
use anyhow::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::TableReference;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::{MyProcNumber, ProcNumber};
use pgrx::prelude::*;
use smol_str::{format_smolstr, SmolStr};
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
        machine
            .consume(&Packet::Failure)
            .expect("Failed to consume failure event during flush");
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
    let mut do_retry = false;
    let mut slots_with_error: Vec<(SlotNumber, SmolStr)> =
        vec![(INVALID_SLOT_NUMBER, "".into()); max_backends() as usize];

    rt.block_on(async {
        log!("DataFusion worker is running");
        while do_retry || BackgroundWorker::wait_latch(None) {
            do_retry = false;
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
                let slot_id = u32::try_from(id).expect("Failed to convert slot id to u32");
                let Ok(output) = machine.consume(&header.packet) else {
                    let msg = format_smolstr!("Failed to consume event: {:?}", &header.packet);
                    response_error(slot_id, &mut ctx, stream, &msg);
                    continue;
                };
                let handle = match output {
                    Some(ExecutorOutput::Parse) => tokio::spawn(parse(header, stream)),
                    Some(ExecutorOutput::Flush) => {
                        ctx.flush(slot_id);
                        continue;
                    }
                    Some(ExecutorOutput::Compile) => todo!(),
                    Some(ExecutorOutput::Bind) => todo!(),
                    _ => {
                        log!("Unexpected output: {:?}", output);
                        panic!("Unexpected output");
                    }
                };
                ctx.tasks.push((slot_id, handle));
            }
            let wait_stream = |slot_id: u32| -> SlotStream {
                let stream;
                loop {
                    let Some(slot) = Bus::new().slot_locked(slot_id) else {
                        BackgroundWorker::wait_latch(None);
                        continue;
                    };
                    stream = Some(SlotStream::from(slot));
                    break;
                }
                stream.expect("Failed to acquire a slot stream")
            };
            for (id, task) in &mut ctx.tasks {
                let result = task.await.expect("Failed to await task");
                match result {
                    Ok(TaskResult::Parsing((stmt, tables))) => {
                        if tables.is_empty() {
                            // We don't need any table metadata for this query.
                            // So, we can generate a fake metadata event and proceed to the next
                            // step.
                            do_retry = true;
                            // TODO: write a fake metadata event to the slot to consume on the next
                            // iteration.
                            todo!();
                        } else {
                            todo!();
                        }
                    }
                    Err(err) => {
                        let msg = format_smolstr!("Failed to execute a task: {:?}", err);
                        // We already hold a mutable reference to the worker context,
                        // so this is a hack to avoid borrow checker complaints.
                        slots_with_error[*id as usize] = (*id, msg);
                    }
                }
            }
            for (slot_id, msg) in &slots_with_error {
                if *slot_id != INVALID_SLOT_NUMBER {
                    let stream = wait_stream(*slot_id);
                    response_error(*slot_id, &mut ctx, stream, msg);
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

#[inline]
fn slot_warning() {
    // The slot should be locked before sending the response.
    // Normally this operation can not fail because its backend
    // is waiting for response and should not hold any locks.
    // But if it does it means that the old backend was terminated
    // and some other one acquired the same slot. So, fsm should
    // be reset to initial value.
    warning!(
        "{} {} {} {}",
        "Failed to lock the slot for error response.",
        "Looks like the old backend was terminated",
        "and the slot is acquired by another backend.",
        "The state machine will be reset to the initial state.",
    );
}

fn response_error(id: SlotNumber, ctx: &mut WorkerContext, stream: SlotStream, message: &str) {
    ctx.flush(id);
    if let Err(err) = send_error(id, stream, message) {
        warning!("Failed to send the error message: {}", err);
    }
}
