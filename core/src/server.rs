use crate::buffer::LockFreeBuffer;
use crate::error::FusionError;
use crate::fsm::executor::StateMachine;
use crate::fsm::Action;
use crate::ipc::Socket;
use crate::protocol::bind::{read_params, request_params};
use crate::protocol::columns::prepare_columns;
use crate::protocol::explain::prepare_explain;
use crate::protocol::failure::prepare_error;
use crate::protocol::metadata::prepare_table_refs;
use crate::protocol::parse::read_query;
use crate::protocol::{consume_header, Direction, Packet};
use crate::sql::Catalog;
use anyhow::{bail, Result};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::scalar::ScalarValue;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::TableReference;
use smol_str::{format_smolstr, SmolStr};

#[derive(Default)]
struct Storage {
    state: StateMachine,
    statement: Option<Statement>,
    logical_plan: Option<LogicalPlan>,
}

impl Storage {
    fn flush(&mut self) {
        *self = Self::default();
    }
}

pub struct Connection<'bytes> {
    storage: Storage,
    recv_socket: Socket<'bytes>,
    send_buffer: LockFreeBuffer<'bytes>,
}

impl<'bytes> Connection<'bytes> {
    pub fn new(recv_socket: Socket<'bytes>, send_buffer: LockFreeBuffer<'bytes>) -> Self {
        Self {
            storage: Storage::default(),
            recv_socket,
            send_buffer,
        }
    }

    pub async fn poll(&mut self) -> Result<()> {
        (&mut self.recv_socket).await?;
        Ok(())
    }

    pub fn process_message(&mut self) -> Result<()> {
        let header = consume_header(&mut self.recv_socket.buffer)?;
        if header.direction == Direction::ToBackend {
            return Ok(());
        }
        let mut packet = header.packet.clone();
        let mut skip_metadata = false;
        loop {
            let action = self.storage.state.consume(&packet)?;
            let result = match action {
                Some(Action::Bind) => {
                    let Some(plan) = std::mem::take(&mut self.storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing bind message".into(),
                        ));
                    };
                    let params = read_params(&mut self.recv_socket.buffer)?;
                    bind(plan, params)
                }
                Some(Action::Compile) => {
                    let Some(stmt) = std::mem::take(&mut self.storage.statement) else {
                        bail!(FusionError::NotFound(
                            "Statement".into(),
                            "while processing compile message".into(),
                        ));
                    };
                    let catalog = if skip_metadata {
                        Catalog::default()
                    } else {
                        Catalog::from_stream(&mut self.recv_socket.buffer)?
                    };
                    compile(stmt, &catalog)
                }
                Some(Action::Explain) => {
                    let Some(plan) = std::mem::take(&mut self.storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing explain message".into(),
                        ));
                    };
                    explain(&plan)
                }
                Some(Action::Flush) => {
                    self.storage.flush();
                    return Ok(());
                }
                Some(Action::Parse) => {
                    let query = read_query(&mut self.recv_socket.buffer)?;
                    parse(query)
                }
                None => {
                    bail!(FusionError::NotFound(
                        "find an action".into(),
                        format_smolstr!("consumed packet: {packet:?}")
                    ));
                }
            }?;
            match result {
                TaskResult::Bind(plan) => {
                    prepare_columns(&mut self.send_buffer, plan.schema().fields())?;
                    self.storage.logical_plan = Some(plan);
                }
                TaskResult::Compilation(plan) => {
                    self.storage.logical_plan = Some(plan);
                    request_params(&mut self.send_buffer)?;
                }
                TaskResult::Explain(explain) => {
                    prepare_explain(&mut self.send_buffer, explain.as_str())?;
                }
                TaskResult::Parsing((stmt, tables)) => {
                    self.storage.statement = Some(stmt);
                    if tables.is_empty() {
                        // We don't need any table metadata for this query.
                        // Let's move connection to the next state.
                        skip_metadata = true;
                        packet = Packet::Metadata;
                        continue;
                    } else {
                        prepare_table_refs(&mut self.send_buffer, tables.as_slice())?
                    }
                }
            }
            break;
        }
        Ok(())
    }

    fn handle_error(&mut self, error: FusionError) {
        self.recv_socket.buffer.flush_read();
        let error_message = format_smolstr!("{error}");
        if let Err(e) = prepare_error(&mut self.send_buffer, &error_message) {
            // TODO: we should rather keep enough space in the send buffer to handle errors.
            eprintln!("Failed to prepare error response: {e}");
        }
        // TODO: notify client about the error.
    }
}

fn bind(plan: LogicalPlan, params: Vec<ScalarValue>) -> Result<TaskResult> {
    let new_plan = plan.with_param_values(params)?;
    Ok(TaskResult::Bind(new_plan))
}

fn compile(stmt: Statement, catalog: &Catalog) -> Result<TaskResult> {
    let planner = SqlToRel::new(catalog);
    let base_plan = planner.statement_to_plan(stmt)?;
    Ok(TaskResult::Compilation(base_plan))
}

fn explain(plan: &LogicalPlan) -> Result<TaskResult> {
    let explain = format_smolstr!("{}", plan.display_indent_schema());
    Ok(TaskResult::Explain(explain))
}

fn parse(query: SmolStr) -> Result<TaskResult> {
    let stmts = DFParser::parse_sql(query.as_str())?;
    let Some(stmt) = stmts.into_iter().next() else {
        return Err(FusionError::ParseQuery(query).into());
    };

    let state = SessionStateBuilder::new().build();
    let tables = state.resolve_table_references(&stmt)?;
    Ok(TaskResult::Parsing((stmt, tables)))
}

enum TaskResult {
    Parsing((Statement, Vec<TableReference>)),
    Compilation(LogicalPlan),
    Bind(LogicalPlan),
    Explain(SmolStr),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::fsm::ExecutorState;
    use crate::ipc::SharedState;
    use crate::protocol::metadata::process_metadata_with_response;
    use crate::protocol::metadata::tests::{
        mock_schema_table_lookup, mock_table_lookup, mock_table_serialize,
    };
    use crate::protocol::parse::prepare_query;
    use crate::protocol::Tape;
    use std::cell::UnsafeCell;
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    struct ConnMemory {
        rx: UnsafeCell<[u8; 8 + 120]>,
        tx: UnsafeCell<[u8; 8 + 120]>,
        flag: UnsafeCell<[AtomicBool; 1]>,
    }
    impl ConnMemory {
        const fn new() -> Self {
            Self {
                rx: UnsafeCell::new([0; 8 + 120]),
                tx: UnsafeCell::new([0; 8 + 120]),
                flag: UnsafeCell::new([AtomicBool::new(false); 1]),
            }
        }
    }
    unsafe impl Sync for ConnMemory {}

    fn flush(conn: &mut Connection) {
        conn.storage.flush();
        conn.recv_socket.buffer.flush_read();
        conn.send_buffer.flush_read();
    }

    #[tokio::test]
    async fn test_parse() -> Result<()> {
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flag.get() }));
        let recv_buffer = LockFreeBuffer::new(unsafe { &mut *BYTES.rx.get() });
        let send_buffer = LockFreeBuffer::new(unsafe { &mut *BYTES.tx.get() });
        let socket = Socket::new(0, Arc::clone(&state), recv_buffer);
        let mut conn = Connection::new(socket, send_buffer);
        tokio::spawn(async move {
            // Test parsing a query with tables.
            assert_eq!(conn.storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from t";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message()
                .expect("Failed to process parse message");
            let Ok(TaskResult::Parsing((stmt, _))) = parse(sql.into()) else {
                unreachable!();
            };
            assert_eq!(conn.storage.statement, Some(stmt));
            assert_eq!(conn.storage.state.state(), &ExecutorState::Statement);

            // Test parsing a query without tables.
            flush(&mut conn);
            assert_eq!(conn.storage.state.state(), &ExecutorState::Initialized);
            let sql = "select 1";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message()
                .expect("Failed to process parse message");
            let Ok(TaskResult::Parsing((stmt, _))) = parse(sql.into()) else {
                unreachable!();
            };
            let Ok(TaskResult::Compilation(plan)) = compile(stmt, &Catalog::default()) else {
                unreachable!();
            };
            assert_eq!(conn.storage.logical_plan, Some(plan));
            assert_eq!(conn.storage.state.state(), &ExecutorState::LogicalPlan);
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_from_parse_to_compile() -> Result<()> {
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flag.get() }));
        let recv_buffer = LockFreeBuffer::new(unsafe { &mut *BYTES.rx.get() });
        let send_buffer = LockFreeBuffer::new(unsafe { &mut *BYTES.tx.get() });
        let socket = Socket::new(0, Arc::clone(&state), recv_buffer);
        let mut conn = Connection::new(socket, send_buffer);
        tokio::spawn(async move {
            assert_eq!(conn.storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from public.t1";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message()
                .expect("Failed to process parse message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);

            assert_eq!(conn.storage.state.state(), &ExecutorState::Statement);
            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume metadata header");
            assert_eq!(header.direction, Direction::ToBackend);
            assert_eq!(header.packet, Packet::Metadata);
            process_metadata_with_response(
                &mut conn.send_buffer,
                &mut conn.recv_socket.buffer,
                mock_schema_table_lookup,
                mock_table_lookup,
                mock_table_serialize,
            )
            .expect("Failed to process metadata");
            conn.process_message()
                .expect("Failed to process metadata message");
            assert_eq!(conn.storage.state.state(), &ExecutorState::LogicalPlan);
        })
        .await?;
        Ok(())
    }
}
