use crate::error::FusionError;
use crate::fsm::executor::StateMachine;
use crate::fsm::ExecutorOutput;
use crate::ipc::{
    init_shmem, max_backends, set_worker_id, Bus, SlotNumber, SlotStream, INVALID_SLOT_NUMBER,
};
use crate::protocol::{
    consume_header, prepare_metadata, read_params, read_query, request_params, send_error,
    send_table_refs, write_header, Direction, Flag, Header, Packet,
};
use crate::sql::Catalog;
use anyhow::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::TableReference;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::MyProcNumber;
use pgrx::prelude::*;
use smol_str::{format_smolstr, SmolStr};
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

enum TaskResult {
    Parsing((Statement, Vec<TableReference>)),
    Compilation(LogicalPlan),
    Bind(LogicalPlan),
}

struct WorkerContext {
    statements: Vec<Option<Statement>>,
    states: Vec<StateMachine>,
    logical_plans: Vec<Option<LogicalPlan>>,
    tasks: Vec<(SlotNumber, JoinHandle<Result<TaskResult>>)>,
}

impl WorkerContext {
    fn new() -> Self {
        let capacity = max_backends() as usize;
        let tasks = Vec::with_capacity(capacity);
        let mut states = Vec::with_capacity(capacity);
        let mut statements = Vec::with_capacity(capacity);
        let mut logical_plans = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            states.push(StateMachine::new());
            statements.push(None);
            logical_plans.push(None);
        }
        Self {
            statements,
            states,
            logical_plans,
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

fn init_slots() -> Result<()> {
    for locked_slot in Bus::new().into_iter().flatten() {
        let mut stream = SlotStream::from(locked_slot);
        stream.reset();
        let header = Header {
            direction: Direction::ToBackend,
            packet: Packet::None,
            length: 0,
            flag: Flag::Last,
        };
        write_header(&mut stream, &header)?;
    }
    Ok(())
}

#[pg_guard]
#[no_mangle]
pub extern "C" fn worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let mut ctx = WorkerContext::new();
    let rt = Builder::new_multi_thread()
        .worker_threads(TOKIO_THREAD_NUMBER)
        .enable_all()
        .build()
        .unwrap();
    let mut do_retry = false;
    let mut errors: Vec<Option<SmolStr>> = vec![None; max_backends() as usize];
    init_slots().expect("Failed to initialize slots");
    unsafe { set_worker_id(MyProcNumber) };

    log!("DataFusion worker is running");
    while do_retry || BackgroundWorker::wait_latch(Some(WORKER_WAIT_TIMEOUT)) {
        // Do not use any pgrx API in this loop: tokio has a multithreaded runtime,
        // while PostgreSQL functions can work only in single thread processes.
        rt.block_on(async {
            do_retry = false;
            // Process packets from the slots.
            for (id, locked_slot) in Bus::new().into_iter().enumerate() {
                let Some(slot) = locked_slot else {
                    continue;
                };
                let mut stream = SlotStream::from(slot);
                let header = match consume_header(&mut stream) {
                    Ok(header) => header,
                    Err(err) => {
                        errors[id] = Some(format_smolstr!("Failed to consume header: {:?}", err));
                        continue;
                    }
                };
                if header.direction == Direction::ToBackend {
                    continue;
                }
                let machine = &mut ctx.states[id];
                let slot_id = u32::try_from(id).expect("Failed to convert slot id to u32");
                let output = match machine.consume(&header.packet) {
                    Ok(output) => output,
                    Err(err) => {
                        let msg = format_smolstr!("Failed to change machine state: {:?}", err);
                        errors[id] = Some(msg);
                        continue;
                    }
                };
                let handle = match output {
                    Some(ExecutorOutput::Parse) => tokio::spawn(parse(header, stream)),
                    Some(ExecutorOutput::Flush) => {
                        ctx.flush(slot_id);
                        continue;
                    }
                    Some(ExecutorOutput::Compile) => {
                        let Some(stmt) = std::mem::take(&mut ctx.statements[id]) else {
                            errors[id] = Some(format_smolstr!("No statement found for slot: {id}"));
                            continue;
                        };
                        tokio::spawn(compile(header, stream, stmt))
                    }
                    Some(ExecutorOutput::Bind) => {
                        let Some(plan) = std::mem::take(&mut ctx.logical_plans[id]) else {
                            errors[id] =
                                Some(format_smolstr!("No logical plan found for slot: {id}"));
                            continue;
                        };
                        tokio::spawn(bind(header, stream, plan))
                    }
                    None => unreachable!("Empty output in the worker state machine"),
                };
                ctx.tasks.push((slot_id, handle));
            }
            // Wait for the tasks to finish and process their results.
            for (id, task) in &mut ctx.tasks {
                let result = task.await.expect("Failed to await task");
                match result {
                    Ok(TaskResult::Parsing((stmt, tables))) => {
                        let mut stream = wait_stream(*id).await;
                        if tables.is_empty() {
                            // We don't need any table metadata for this query.
                            // So, write a fake metadata packet to the slot and proceed it
                            // in the next iteration.
                            do_retry = true;
                            if let Err(err) = prepare_metadata(&[], &mut stream) {
                                errors[*id as usize] =
                                    Some(format_smolstr!("Failed to prepare metadata: {:?}", err));
                                continue;
                            }
                        } else {
                            send_table_refs(*id, stream, tables.as_slice())
                                .expect("Failed to reqest table references");
                        }
                        ctx.statements[*id as usize] = Some(stmt);
                    }
                    Ok(TaskResult::Compilation(plan)) => {
                        let stream = wait_stream(*id).await;
                        if let Err(err) = request_params(*id, stream) {
                            errors[*id as usize] =
                                Some(format_smolstr!("Failed to request params: {:?}", err));
                            continue;
                        }
                        ctx.logical_plans[*id as usize] = Some(plan);
                    }
                    Ok(TaskResult::Bind(plan)) => {
                        ctx.logical_plans[*id as usize] = Some(plan);
                    }
                    Err(err) => {
                        errors[*id as usize] =
                            Some(format_smolstr!("Failed to execute task: {:?}", err))
                    }
                }
            }
        });
        // Process errors in the main PostgreSQL thread.
        for (slot_id, msg) in errors.iter_mut().enumerate() {
            if let Some(msg) = msg {
                let stream;
                loop {
                    let Some(slot) = Bus::new().slot_locked(slot_id as u32) else {
                        BackgroundWorker::wait_latch(Some(SLOT_WAIT_TIMEOUT));
                        continue;
                    };
                    stream = SlotStream::from(slot);
                    break;
                }
                response_error(slot_id as u32, &mut ctx, stream, msg);
            }
            *msg = None;
        }
    }
}

#[inline(always)]
async fn wait_stream(slot_id: u32) -> SlotStream {
    loop {
        let Some(slot) = Bus::new().slot_locked(slot_id) else {
            tokio::time::sleep(SLOT_WAIT_TIMEOUT).await;
            continue;
        };
        return SlotStream::from(slot);
    }
}

async fn parse(header: Header, mut stream: SlotStream) -> Result<TaskResult> {
    assert_eq!(header.packet, Packet::Parse);
    assert_eq!(header.direction, Direction::ToWorker);
    // TODO: handle long queries that span multiple packets.
    assert_eq!(header.flag, Flag::Last);
    let (query, _) = read_query(&mut stream)?;

    let stmts = DFParser::parse_sql(query)?;
    let Some(stmt) = stmts.into_iter().next() else {
        return Err(FusionError::ParseQuery(query.to_string()).into());
    };
    stream.rewind(header.length as usize)?;

    let state = SessionStateBuilder::new().build();
    let tables = state.resolve_table_references(&stmt)?;
    Ok(TaskResult::Parsing((stmt, tables)))
}

async fn compile(header: Header, mut stream: SlotStream, stmt: Statement) -> Result<TaskResult> {
    assert_eq!(header.packet, Packet::Metadata);
    assert_eq!(header.direction, Direction::ToWorker);
    let catalog = Catalog::from_stream(&mut stream)?;
    let planner = SqlToRel::new(&catalog);
    let base_plan = planner.statement_to_plan(stmt)?;
    Ok(TaskResult::Compilation(base_plan))
}

async fn bind(
    header: Header,
    mut stream: SlotStream,
    base_plan: LogicalPlan,
) -> Result<TaskResult> {
    assert_eq!(header.packet, Packet::Bind);
    assert_eq!(header.direction, Direction::ToWorker);
    let params = read_params(&mut stream)?;
    let plan = base_plan.with_param_values(params)?;
    Ok(TaskResult::Bind(plan))
}

fn response_error(id: SlotNumber, ctx: &mut WorkerContext, stream: SlotStream, message: &str) {
    ctx.flush(id);
    send_error(id, stream, message).expect("Failed to send error response");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::tests::{make_slot, SLOT_SIZE};
    use crate::protocol::prepare_query;

    #[tokio::test]
    async fn test_parse() {
        let mut buffer: [u8; SLOT_SIZE] = [0; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut buffer).into();
        let sql = "SELECT * FROM foo";
        prepare_query(&mut stream, sql).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        let result = parse(header, stream).await.expect("Failed to parse query");
        let TaskResult::Parsing((stmt, tables)) = result else {
            panic!("Expected parsing result");
        };
        let expected_stmt = DFParser::parse_sql(sql)
            .expect("Failed to parse SQL")
            .into_iter()
            .next()
            .expect("Failed to get statement");
        assert_eq!(stmt, expected_stmt);
        assert_eq!(tables.len(), 1);
        let expected_table = TableReference::bare("foo");
        assert_eq!(tables[0], expected_table);
    }
}
