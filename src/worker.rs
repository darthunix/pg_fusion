use crate::error::FusionError;
use crate::fsm::executor::StateMachine;
use crate::fsm::Action;
use crate::ipc::{
    init_shmem, max_backends, set_worker_id, Bus, SlotNumber, SlotStream, INVALID_PROC_NUMBER,
};
use crate::protocol::{
    consume_header, prepare_columns, prepare_empty_metadata, prepare_explain, prepare_table_refs,
    read_params, read_query, request_params, send_error, signal, write_header, Direction, Flag,
    Header, Packet,
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

// POSTGRES WORLD
// Do not use any async functions in this part of the code.

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
    Explain(SmolStr),
}

struct WorkerContext {
    statements: Vec<Option<Statement>>,
    states: Vec<StateMachine>,
    logical_plans: Vec<Option<LogicalPlan>>,
    tasks: Vec<(SlotNumber, JoinHandle<Result<TaskResult>>)>,
}

impl WorkerContext {
    fn with_capacity(capacity: usize) -> Self {
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
        self.states[slot_id as usize] = StateMachine::new();
        self.statements[slot_id as usize] = None;
        for (id, task) in &mut self.tasks {
            if *id == slot_id {
                task.abort();
                break;
            }
        }
    }
}

fn init_slots(holder: i32) -> Result<()> {
    for locked_slot in Bus::new().into_iter(holder).flatten() {
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

fn response_error(id: SlotNumber, ctx: &mut WorkerContext, stream: SlotStream, message: &str) {
    ctx.flush(id);
    send_error(id, stream, message).expect("Failed to send error response");
}

// POSTGRES - ASYNC BRIDGE
// The place where async world meets the postgres world.

#[pg_guard]
#[no_mangle]
pub extern "C" fn worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let capacity = max_backends() as usize;
    let mut ctx = WorkerContext::with_capacity(capacity);
    let rt = Builder::new_multi_thread()
        .worker_threads(TOKIO_THREAD_NUMBER)
        .enable_all()
        .build()
        .unwrap();
    let mut do_retry = false;
    let mut errors: Vec<Option<SmolStr>> = vec![None; capacity];
    let mut signals: Vec<bool> = vec![false; capacity];
    let worker_proc_number = unsafe { MyProcNumber };
    init_slots(worker_proc_number).expect("Failed to initialize slots");
    set_worker_id(worker_proc_number);

    log!("DataFusion worker is running");
    while do_retry || BackgroundWorker::wait_latch(Some(WORKER_WAIT_TIMEOUT)) {
        log!("DataFusion worker is processing slots");
        rt.block_on(async {
            do_retry = false;
            create_tasks(&mut ctx, &mut errors, worker_proc_number).await;
            wait_results(
                &mut ctx,
                &mut errors,
                &mut signals,
                &mut do_retry,
                worker_proc_number,
            )
            .await;
        });
        log!("DataFusion worker is processing errors");
        // Process errors returned by the tasks.
        for (slot_id, msg) in errors.iter_mut().enumerate() {
            if let Some(msg) = msg {
                let stream;
                loop {
                    let Some(slot) = Bus::new().slot_locked(slot_id as u32, worker_proc_number)
                    else {
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
        log!("DataFusion worker is processing signals");
        // Signal backends about new messages.
        for (id, do_signal) in signals.iter_mut().enumerate() {
            let slot_id = id as u32;
            if *do_signal {
                signal(slot_id, Direction::ToBackend);
                *do_signal = false;
            }
        }
    }
    set_worker_id(INVALID_PROC_NUMBER);
    log!("DataFusion worker is stopping");
}

// ASYNC WORLD
// Do not use any pgrx symbols in async functions. Tokio has a multithreaded
// runtime, while postgres functions can work only in single thread.

/// Process packets from the slots and create tasks for them.
async fn create_tasks(ctx: &mut WorkerContext, errors: &mut [Option<SmolStr>], holder: i32) {
    for (id, locked_slot) in Bus::new().into_iter(holder).enumerate() {
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
        let Ok(slot_id) = u32::try_from(id) else {
            errors[id] = Some(format_smolstr!("Failed to convert id to u32: {id}"));
            continue;
        };
        let output = match machine.consume(&header.packet) {
            Ok(output) => output,
            Err(err) => {
                let msg = format_smolstr!("Failed to change machine state: {:?}", err);
                errors[id] = Some(msg);
                continue;
            }
        };
        let handle = match output {
            Some(Action::Parse) => tokio::spawn(parse(header, stream)),
            Some(Action::Flush) => {
                ctx.flush(slot_id);
                continue;
            }
            Some(Action::Compile) => {
                let Some(stmt) = std::mem::take(&mut ctx.statements[id]) else {
                    errors[id] = Some(format_smolstr!("No statement found for slot: {id}"));
                    continue;
                };
                tokio::spawn(compile(header, stream, stmt))
            }
            Some(Action::Bind) => {
                let Some(plan) = std::mem::take(&mut ctx.logical_plans[id]) else {
                    errors[id] = Some(format_smolstr!("No logical plan found for slot: {id}"));
                    continue;
                };
                tokio::spawn(bind(header, stream, plan))
            }
            Some(Action::Explain) => {
                let Some(plan) = std::mem::take(&mut ctx.logical_plans[id]) else {
                    errors[id] = Some(format_smolstr!("No logical plan found for slot: {id}"));
                    continue;
                };
                tokio::spawn(explain(plan))
            }
            None => {
                errors[id] = Some(format_smolstr!("No action found for slot: {id}"));
                continue;
            }
        };
        ctx.tasks.push((slot_id, handle));
    }
}

/// Wait for the tasks to finish and process their results.
async fn wait_results(
    ctx: &mut WorkerContext,
    errors: &mut [Option<SmolStr>],
    signals: &mut [bool],
    do_retry: &mut bool,
    holder: i32,
) {
    while let Some((id, task)) = ctx.tasks.pop() {
        let result = match task.await {
            Ok(result) => result,
            Err(err) => {
                errors[id as usize] = Some(format_smolstr!("Failed to join task: {:?}", err));
                continue;
            }
        };
        match result {
            Ok(TaskResult::Parsing((stmt, tables))) => {
                let mut stream = wait_stream(id, holder).await;
                if tables.is_empty() {
                    // We don't need any table metadata for this query.
                    // So, write a fake metadata packet to the slot and proceed it
                    // in the next iteration.
                    *do_retry = true;
                    match prepare_empty_metadata(&mut stream) {
                        Ok(()) => signals[id as usize] = true,
                        Err(err) => {
                            errors[id as usize] =
                                Some(format_smolstr!("Failed to prepare metadata: {:?}", err));
                            continue;
                        }
                    }
                } else {
                    match prepare_table_refs(&mut stream, tables.as_slice()) {
                        Ok(()) => signals[id as usize] = true,
                        Err(err) => {
                            errors[id as usize] = Some(format_smolstr!(
                                "Failed to prepare table references: {:?}",
                                err
                            ));
                            continue;
                        }
                    }
                }
                ctx.statements[id as usize] = Some(stmt);
            }
            Ok(TaskResult::Compilation(plan)) => {
                let mut stream = wait_stream(id, holder).await;
                match request_params(&mut stream) {
                    Ok(()) => signals[id as usize] = true,
                    Err(err) => {
                        errors[id as usize] =
                            Some(format_smolstr!("Failed to request params: {:?}", err));
                        continue;
                    }
                }
                ctx.logical_plans[id as usize] = Some(plan);
            }
            Ok(TaskResult::Bind(plan)) => {
                let mut stream = wait_stream(id, holder).await;
                match prepare_columns(&mut stream, plan.schema().fields()) {
                    Ok(()) => signals[id as usize] = true,
                    Err(err) => {
                        errors[id as usize] =
                            Some(format_smolstr!("Failed to prepare columns: {:?}", err));
                        continue;
                    }
                }
                ctx.logical_plans[id as usize] = Some(plan);
            }
            Ok(TaskResult::Explain(explain)) => {
                let mut stream = wait_stream(id, holder).await;
                match prepare_explain(&mut stream, &explain) {
                    Ok(()) => signals[id as usize] = true,
                    Err(err) => {
                        errors[id as usize] =
                            Some(format_smolstr!("Failed to prepare explain: {:?}", err));
                        continue;
                    }
                }
            }
            Err(err) => {
                errors[id as usize] = Some(format_smolstr!("Failed to execute task: {:?}", err))
            }
        }
    }
}

#[inline(always)]
async fn wait_stream(slot_id: u32, holder: i32) -> SlotStream {
    loop {
        let Some(slot) = Bus::new().slot_locked(slot_id, holder) else {
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

async fn explain(plan: LogicalPlan) -> Result<TaskResult> {
    let explain = format_smolstr!("{}", plan.display_indent_schema());
    Ok(TaskResult::Explain(explain))
}

#[cfg(test)]
mod tests {
    use datafusion::scalar::ScalarValue;
    use rmp::decode::{read_array_len, read_bin_len, read_u8};
    use rmp::encode::{write_array_len, write_bool, write_str, write_u32, write_u8};

    use super::*;
    use crate::data_type::{write_scalar_value, EncodedType};
    use crate::ipc::tests::{make_slot, SLOT_SIZE};
    use crate::ipc::{Slot, BUS_PTR};
    use crate::protocol::prepare_query;

    // MOCKS

    fn encode_foo_meta(stream: &mut SlotStream) {
        stream.reset();
        write_header(stream, &Header::default()).unwrap();
        let pos_init = stream.position();
        // amount of tables
        write_array_len(stream, 1).unwrap();
        // table foo
        write_array_len(stream, 2).unwrap();
        // oid
        write_u32(stream, 42).unwrap();
        // name
        write_str(stream, "foo").unwrap();
        // amount of columns
        write_array_len(stream, 2).unwrap();
        // column a
        write_array_len(stream, 3).unwrap();
        // type Int32
        write_u8(stream, EncodedType::Int32 as u8).unwrap();
        // nullable
        write_bool(stream, true).unwrap();
        // name
        write_str(stream, "a").unwrap();
        // column b
        write_array_len(stream, 3).unwrap();
        // type Utf8
        write_u8(stream, EncodedType::Utf8 as u8).unwrap();
        // not nullable
        write_bool(stream, false).unwrap();
        // name
        write_str(stream, "b").unwrap();
        let pos_final = stream.position();
        let length = u16::try_from(pos_final - pos_init).unwrap();
        let header = Header {
            direction: Direction::ToWorker,
            packet: Packet::Metadata,
            length,
            flag: Flag::Last,
        };
        stream.reset();
        write_header(stream, &header).unwrap();
        stream.rewind(length as usize).unwrap();
    }

    fn encode_a_param(stream: &mut SlotStream) {
        stream.reset();
        write_header(stream, &Header::default()).unwrap();
        let pos_init = stream.position();
        // amount of params
        write_array_len(stream, 1).unwrap();
        // a
        write_scalar_value(stream, &ScalarValue::Int32(Some(1)))
            .expect("Failed to write scalar value");
        let pos_final = stream.position();
        let length = u16::try_from(pos_final - pos_init).unwrap();
        let header = Header {
            direction: Direction::ToWorker,
            packet: Packet::Bind,
            length,
            flag: Flag::Last,
        };
        stream.reset();
        write_header(stream, &header).unwrap();
        stream.rewind(length as usize).unwrap();
    }

    fn decode_foo_references(stream: &mut SlotStream) {
        stream.reset();
        let header = consume_header(stream).unwrap();
        assert_eq!(header.packet, Packet::Metadata);
        assert_eq!(header.direction, Direction::ToBackend);
        assert_eq!(header.flag, Flag::Last);

        let table_num = read_array_len(stream).unwrap();
        assert_eq!(table_num, 1);
        let elem_num = read_array_len(stream).unwrap();
        assert_eq!(elem_num, 1);
        let foo_len = read_bin_len(stream).unwrap();
        assert_eq!(foo_len as usize, b"foo\0".len());
        let foo = stream.look_ahead(foo_len as usize).unwrap();
        assert_eq!(foo, b"foo\0");
        stream.rewind(foo_len as usize).unwrap();
    }

    fn decode_columns(stream: &mut SlotStream) {
        stream.reset();
        let header = consume_header(stream).unwrap();
        assert_eq!(header.packet, Packet::Columns);
        assert_eq!(header.direction, Direction::ToBackend);
        assert_eq!(header.flag, Flag::Last);

        let column_num = read_array_len(stream).unwrap();
        assert_eq!(column_num, 2);
        let a_etype = read_u8(stream).unwrap();
        assert_eq!(a_etype, EncodedType::Int32 as u8);
        let a_len = read_bin_len(stream).unwrap();
        assert_eq!(a_len as usize, b"a\0".len());
        let a = stream.look_ahead(a_len as usize).unwrap();
        assert_eq!(a, b"a\0");
        stream.rewind(a_len as usize).unwrap();
        let b_etype = read_u8(stream).unwrap();
        assert_eq!(b_etype, EncodedType::Utf8 as u8);
        let b_len = read_bin_len(stream).unwrap();
        assert_eq!(b_len as usize, b"b\0".len());
        let b = stream.look_ahead(b_len as usize).unwrap();
        assert_eq!(b, b"b\0");
        stream.rewind(b_len as usize).unwrap();
    }

    // TESTS

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

    #[tokio::test]
    async fn test_compile_and_bind() {
        let mut buffer: [u8; SLOT_SIZE] = [0; SLOT_SIZE];

        // Test compilation of a query.
        let mut stream: SlotStream = make_slot(&mut buffer).into();
        encode_foo_meta(&mut stream);
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        let sql = "SELECT * FROM foo WHERE a = $1";
        let stmt = DFParser::parse_sql(sql)
            .expect("Failed to parse SQL")
            .into_iter()
            .next()
            .expect("Failed to get statement");
        let result = compile(header, stream, stmt)
            .await
            .expect("Failed to compile query");
        let TaskResult::Compilation(plan) = result else {
            panic!("Expected compilation result");
        };
        let explain = format_smolstr!("{}", plan.display_indent_schema());
        assert_eq!(
            explain,
            r#"Projection: * [a:Int32;N, b:Utf8]
  Filter: foo.a = $1 [a:Int32;N, b:Utf8]
    TableScan: foo [a:Int32;N, b:Utf8]"#,
        );

        // Now we need to bind the parameters.
        let mut stream: SlotStream = make_slot(&mut buffer).into();
        encode_a_param(&mut stream);
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        let result = bind(header, stream, plan)
            .await
            .expect("Failed to bind parameters");
        let TaskResult::Bind(plan) = result else {
            panic!("Expected bind result");
        };
        let explain = format_smolstr!("{}", plan.display_indent_schema());
        assert_eq!(
            explain,
            r#"Projection: * [a:Int32;N, b:Utf8]
  Filter: foo.a = Int32(1) [a:Int32;N, b:Utf8]
    TableScan: foo [a:Int32;N, b:Utf8]"#,
        );
    }

    #[tokio::test]
    async fn test_compile_empty_tables() {
        let mut buffer: [u8; SLOT_SIZE] = [0; SLOT_SIZE];
        let mut stream: SlotStream = make_slot(&mut buffer).into();
        prepare_empty_metadata(&mut stream).unwrap();
        stream.reset();
        let header = consume_header(&mut stream).unwrap();
        let sql = "SELECT 1";
        let stmt = DFParser::parse_sql(sql)
            .expect("Failed to parse SQL")
            .into_iter()
            .next()
            .expect("Failed to get statement");
        let result = compile(header, stream, stmt)
            .await
            .expect("Failed to compile query");
        let TaskResult::Compilation(plan) = result else {
            panic!("Expected compilation result");
        };
        let explain = format_smolstr!("{}", plan.display_indent_schema());
        assert_eq!(
            explain,
            "Projection: Int64(1) [Int64(1):Int64]\n  EmptyRelation []",
        );
    }

    #[tokio::test]
    async fn test_loop() {
        let holder = 42;
        let capacity = 2;
        let mut ctx = WorkerContext::with_capacity(capacity);
        let mut errors: Vec<Option<SmolStr>> = vec![None; capacity];
        let mut signals: Vec<bool> = vec![false; capacity];
        let mut do_retry = false;
        let bus_size = Slot::estimated_size() * capacity;
        let mut buffer = vec![0; bus_size];
        unsafe { BUS_PTR.set(buffer.as_mut_ptr() as _).unwrap() };
        init_slots(holder).expect("Failed to initialize slots");
        // Check processing of the parse message.
        let sql = "SELECT * FROM foo WHERE a = $1";
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            prepare_query(&mut stream, sql).unwrap();
        }
        create_tasks(&mut ctx, &mut errors, holder).await;
        wait_results(&mut ctx, &mut errors, &mut signals, &mut do_retry, holder).await;
        let error = errors[0].take();
        assert!(error.is_none(), "Error: {:?}", error);
        let stmt = ctx.statements.first().unwrap().as_ref().unwrap();
        let expected_stmt = DFParser::parse_sql(sql)
            .expect("Failed to parse SQL")
            .into_iter()
            .next()
            .expect("Failed to get statement");
        assert_eq!(stmt, &expected_stmt);
        // Check request for table metadata and mock response.
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            decode_foo_references(&mut stream);
        }
        // Check compilation of the statement.
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            encode_foo_meta(&mut stream);
        }
        create_tasks(&mut ctx, &mut errors, holder).await;
        wait_results(&mut ctx, &mut errors, &mut signals, &mut do_retry, holder).await;
        let error = errors[0].take();
        assert!(error.is_none(), "Error: {:?}", error);
        let plan = ctx.logical_plans.first().unwrap().as_ref().unwrap();
        let explain = format_smolstr!("{}", plan.display_indent_schema());
        assert_eq!(
            explain,
            r#"Projection: * [a:Int32;N, b:Utf8]
  Filter: foo.a = $1 [a:Int32;N, b:Utf8]
    TableScan: foo [a:Int32;N, b:Utf8]"#,
        );
        // Check request for parameters and mock response.
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            let header = consume_header(&mut stream).unwrap();
            assert_eq!(header.packet, Packet::Bind);
            assert_eq!(header.direction, Direction::ToBackend);
            encode_a_param(&mut stream);
        }
        // Check binding of the parameters.
        create_tasks(&mut ctx, &mut errors, holder).await;
        wait_results(&mut ctx, &mut errors, &mut signals, &mut do_retry, holder).await;
        let error = errors[0].take();
        assert!(error.is_none(), "Error: {:?}", error);
        let plan = ctx.logical_plans.first().unwrap().as_ref().unwrap();
        let explain = format_smolstr!("{}", plan.display_indent_schema());
        assert_eq!(
            explain,
            r#"Projection: * [a:Int32;N, b:Utf8]
  Filter: foo.a = Int32(1) [a:Int32;N, b:Utf8]
    TableScan: foo [a:Int32;N, b:Utf8]"#,
        );
        // Check columns in response.
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            decode_columns(&mut stream);
        }
        // Check explain.
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            let header = Header {
                direction: Direction::ToWorker,
                packet: Packet::Explain,
                length: 0,
                flag: Flag::Last,
            };
            write_header(&mut stream, &header).unwrap();
        }
        create_tasks(&mut ctx, &mut errors, holder).await;
        wait_results(&mut ctx, &mut errors, &mut signals, &mut do_retry, holder).await;
        let error = errors[0].take();
        assert!(error.is_none(), "Error: {:?}", error);
        {
            let slot = Bus::new().slot_locked(0, holder).unwrap();
            let mut stream = SlotStream::from(slot);
            let header = consume_header(&mut stream).unwrap();
            assert_eq!(header.packet, Packet::Explain);
            assert_eq!(header.direction, Direction::ToBackend);
            let len = read_bin_len(&mut stream).unwrap();
            assert_eq!(len as usize, explain.len() + 1);
            let expected = format_smolstr!("{}\0", explain);
            let explain = stream.look_ahead(len as usize).unwrap();
            assert_eq!(explain, expected.as_bytes());
        }
    }
}
