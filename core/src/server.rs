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
use std::sync::atomic::{AtomicI32, Ordering};

#[derive(Default)]
pub struct Storage {
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
    recv_socket: Socket<'bytes>,
    send_buffer: LockFreeBuffer<'bytes>,
    client_pid: AtomicI32,
}

impl<'bytes> Connection<'bytes> {
    pub fn new(recv_socket: Socket<'bytes>, send_buffer: LockFreeBuffer<'bytes>) -> Self {
        Self {
            recv_socket,
            send_buffer,
            // The upper bound in /proc/sys/kernel/pid_max is 4_194_304.
            // So i32::MAX is always an invalid PID.
            client_pid: AtomicI32::new(i32::MAX),
        }
    }

    pub fn with_client(mut self, pid: AtomicI32) -> Self {
        self.client_pid = pid;
        self
    }

    pub async fn poll(&mut self) -> Result<()> {
        (&mut self.recv_socket).await?;
        Ok(())
    }

    /// Send SIGUSR1 to the client process ID stored in this connection.
    /// On non-Unix platforms, returns an error.
    pub fn signal_client(&self) -> Result<()> {
        #[cfg(unix)]
        {
            let pid = self.client_pid.load(Ordering::Relaxed);
            if pid <= 0 || pid == i32::MAX {
                bail!(FusionError::FailedTo(
                    "send SIGUSR1".into(),
                    format_smolstr!("invalid pid: {pid}")
                ));
            }
            let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
            if rc == -1 {
                let err = std::io::Error::last_os_error();
                bail!(FusionError::FailedTo(
                    "send SIGUSR1".into(),
                    format_smolstr!("{err}")
                ));
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            bail!(FusionError::FailedTo(
                "send SIGUSR1".into(),
                "unsupported platform".into()
            ));
        }
    }

    pub fn process_message(&mut self, storage: &mut Storage) -> Result<()> {
        let header = consume_header(&mut self.recv_socket.buffer)?;
        if header.direction == Direction::ToBackend {
            return Ok(());
        }
        let mut packet = header.packet.clone();
        let mut skip_metadata = false;
        loop {
            let action = storage.state.consume(&packet)?;
            let result = match action {
                Some(Action::Bind) => {
                    let Some(plan) = std::mem::take(&mut storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing bind message".into(),
                        ));
                    };
                    let params = read_params(&mut self.recv_socket.buffer)?;
                    bind(plan, params)
                }
                Some(Action::Compile) => {
                    let Some(stmt) = std::mem::take(&mut storage.statement) else {
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
                    let Some(plan) = std::mem::take(&mut storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing explain message".into(),
                        ));
                    };
                    explain(&plan)
                }
                Some(Action::Flush) => {
                    storage.flush();
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
                    storage.logical_plan = Some(plan);
                }
                TaskResult::Compilation(plan) => {
                    storage.logical_plan = Some(plan);
                    request_params(&mut self.send_buffer)?;
                }
                TaskResult::Explain(explain) => {
                    prepare_explain(&mut self.send_buffer, explain.as_str())?;
                }
                TaskResult::Parsing((stmt, tables)) => {
                    storage.statement = Some(stmt);
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

    pub fn handle_error(&mut self, error: FusionError) {
        self.recv_socket.buffer.flush_read();
        let error_message = format_smolstr!("{error}");
        if let Err(e) = prepare_error(&mut self.send_buffer, &error_message) {
            tracing::error!(target = "pg_fusion::server", error = %e, "Failed to prepare error response");
        }
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
    use crate::protocol::bind::prepare_params;
    use crate::protocol::columns::consume_columns;
    use crate::protocol::explain::request_explain;
    use crate::protocol::metadata::process_metadata_with_response;
    use crate::protocol::metadata::tests::{
        mock_schema_table_lookup, mock_table_lookup, mock_table_serialize,
    };
    use crate::protocol::parse::prepare_query;
    use crate::protocol::Tape;
    use core::mem::size_of;
    use rmp::decode::read_bin_len;
    use std::cell::UnsafeCell;
    use std::ffi::CStr;
    use std::io::Read;
    use std::os::raw::c_void;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32};
    use std::sync::Arc;

    const PAYLOAD_SIZE: usize = 248;

    // Use explicit connection layout: two lock-free buffers and a client PID.
    // Each lock-free buffer occupies 2 * AtomicU32 (head, tail) + PAYLOAD_SIZE bytes of data.
    const BUF_META: usize = size_of::<AtomicU32>() * 2;
    const PID_SIZE: usize = size_of::<AtomicI32>();
    const CONN_BYTES: usize = (BUF_META + PAYLOAD_SIZE) * 2 + PID_SIZE;

    struct ConnMemory {
        conn: UnsafeCell<[u8; CONN_BYTES]>,
        flags: UnsafeCell<[AtomicBool; 1]>,
    }
    impl ConnMemory {
        const fn new() -> Self {
            Self {
                conn: UnsafeCell::new([0; CONN_BYTES]),
                flags: UnsafeCell::new([AtomicBool::new(false); 1]),
            }
        }
    }
    unsafe impl Sync for ConnMemory {}

    fn flush(conn: &mut Connection, storage: &mut Storage) {
        storage.flush();
        conn.recv_socket.buffer.flush_read();
        conn.send_buffer.flush_read();
    }

    #[tokio::test]
    async fn test_parse() -> Result<()> {
        use crate::layout::{connection_layout, connection_ptrs};
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flags.get() }));

        // Derive recv/send buffers from the connection layout.
        let layout = connection_layout(PAYLOAD_SIZE, PAYLOAD_SIZE).expect("layout");
        let base = unsafe { (&mut *BYTES.conn.get()).as_mut_ptr() as *mut u8 };
        let (recv_base, send_base, _pid_ptr) = unsafe { connection_ptrs(base, layout) };

        let socket = unsafe {
            Socket::from_layout_with_state(
                0,
                Arc::clone(&state),
                recv_base,
                layout.recv_socket_layout,
            )
        };
        let send_buffer =
            unsafe { LockFreeBuffer::from_layout(send_base, layout.send_buffer_layout) };
        let mut conn = Connection::new(socket, send_buffer);
        let storage = Storage::default();
        tokio::spawn(async move {
            let mut storage = storage;
            // Test parsing a query with tables.
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from t";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message(&mut storage)
                .expect("Failed to process parse message");
            let Ok(TaskResult::Parsing((stmt, _))) = parse(sql.into()) else {
                unreachable!();
            };
            assert_eq!(storage.statement, Some(stmt));
            assert_eq!(storage.state.state(), &ExecutorState::Statement);

            // Test parsing a query without tables.
            flush(&mut conn, &mut storage);
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let sql = "select 1";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message(&mut storage)
                .expect("Failed to process parse message");
            let Ok(TaskResult::Parsing((stmt, _))) = parse(sql.into()) else {
                unreachable!();
            };
            let Ok(TaskResult::Compilation(plan)) = compile(stmt, &Catalog::default()) else {
                unreachable!();
            };
            assert_eq!(storage.logical_plan, Some(plan));
            assert_eq!(storage.state.state(), &ExecutorState::LogicalPlan);
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_from_parse_to_columns() -> Result<()> {
        use crate::layout::{connection_layout, connection_ptrs};
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flags.get() }));

        let layout = connection_layout(PAYLOAD_SIZE, PAYLOAD_SIZE).expect("layout");
        let base = unsafe { (&mut *BYTES.conn.get()).as_mut_ptr() as *mut u8 };
        let (recv_base, send_base, _pid_ptr) = unsafe { connection_ptrs(base, layout) };

        let socket = unsafe {
            Socket::from_layout_with_state(
                0,
                Arc::clone(&state),
                recv_base,
                layout.recv_socket_layout,
            )
        };
        let send_buffer =
            unsafe { LockFreeBuffer::from_layout(send_base, layout.send_buffer_layout) };
        let mut conn = Connection::new(socket, send_buffer);
        let storage = Storage::default();
        tokio::spawn(async move {
            let mut storage = storage;
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from public.t1 where b = $1";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message(&mut storage)
                .expect("Failed to process parse message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::Statement);

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
            conn.process_message(&mut storage)
                .expect("Failed to process metadata message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::LogicalPlan);

            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume bind header");
            assert_eq!(header.direction, Direction::ToBackend);
            assert_eq!(header.packet, Packet::Bind);
            prepare_params(&mut conn.recv_socket.buffer, || {
                (1, vec![Ok(ScalarValue::Int32(Some(1)))].into_iter())
            })
            .expect("Failed to prepare params");
            conn.process_message(&mut storage)
                .expect("Failed to process bind message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::LogicalPlan);

            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume bind header");
            assert_eq!(header.direction, Direction::ToBackend);
            assert_eq!(header.packet, Packet::Columns);
            type Payload = (i16, u8, Vec<u8>);
            let mut columns: Vec<Payload> = Vec::new();
            let columns_ptr = &mut columns as *mut Vec<Payload> as *mut c_void;
            let repack = |pos: i16, etype: u8, name: &[u8], ptr: *mut c_void| -> Result<()> {
                let columns: &mut Vec<Payload> = unsafe { &mut *(ptr as *mut Vec<Payload>) };
                columns.push((pos, etype, name.to_vec()));
                Ok(())
            };
            consume_columns(&mut conn.send_buffer, columns_ptr, repack)
                .expect("Failed to consume columns");
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], (0, 4, b"a\0".into()));

            request_explain(&mut conn.recv_socket.buffer).expect("Failed to request explain");
            conn.process_message(&mut storage)
                .expect("Failed to process explain message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume explain header");
            assert_eq!(header.direction, Direction::ToBackend);
            assert_eq!(header.packet, Packet::Explain);
            let len =
                read_bin_len(&mut conn.send_buffer).expect("Failed to get explain length") as usize;
            let mut buffer = vec![0u8; len];
            conn.send_buffer
                .read_exact(&mut buffer)
                .expect("Failed to read explain to buffer");
            assert!(!buffer.is_empty());
            let explain = CStr::from_bytes_with_nul(&buffer)
                .expect("Failed to convert explain bytes to C string")
                .to_str()
                .expect("Failed to cast C string to str");
            let expected_explain = "Projection: public.t1.a [a:Int64;N]\n  \
                Filter: public.t1.b = Int32(1) [a:Int64;N, b:Utf8]\n    \
                TableScan: public.t1 [a:Int64;N, b:Utf8]";
            assert_eq!(explain, expected_explain);
        })
        .await?;
        Ok(())
    }
}
