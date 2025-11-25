use crate::buffer::LockFreeBuffer;
use crate::fsm::executor::StateMachine;
use crate::fsm::Action;
use crate::ipc::Socket;
use crate::pgscan::{count_scans, for_each_scan, ScanRegistry};
use crate::sql::Catalog;
use anyhow::Error;
use anyhow::{bail, Result};
use common::FusionError;
use datafusion::arrow::array::{Array, Int32Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion_sql::parser::{DFParser, Statement};
use datafusion_sql::planner::SqlToRel;
use datafusion_sql::TableReference;
use futures::StreamExt;
use protocol::bind::{read_params, request_params};
use protocol::columns::prepare_columns;
use protocol::explain::prepare_explain;
use protocol::failure::prepare_error;
use protocol::heap::read_heap_block_bitmap_meta;
use protocol::heap::request_heap_block;
use protocol::metadata::prepare_table_refs;
use protocol::parse::read_query;
use protocol::tuple::{consume_column_layout, PgAttrWire};
use protocol::Tape;
use protocol::{consume_header, is_data_tag, ControlPacket, DataPacket, Direction};
use rmp::decode::{read_u16 as read_u16_msgpack, read_u64 as read_u64_msgpack};
use smol_str::{format_smolstr, SmolStr};
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{debug, trace};

#[derive(Default)]
pub struct Storage {
    state: StateMachine,
    statement: Option<Statement>,
    logical_plan: Option<LogicalPlan>,
    physical_plan: Option<Arc<dyn ExecutionPlan>>,
    registry: Arc<ScanRegistry>,
    exec_task: Option<JoinHandle<()>>,
    pg_attrs: Option<Vec<PgAttrWire>>,
}

impl Storage {
    pub fn flush(&mut self) {
        // Preserve the registry across flushes
        let registry = Arc::clone(&self.registry);
        // Abort any running execution task to avoid leaks
        if let Some(handle) = self.exec_task.take() {
            handle.abort();
        }
        *self = Self::default();
        self.registry = registry;
    }

    pub fn set_registry_conn(&mut self, conn_id: usize) {
        self.registry = Arc::new(crate::pgscan::ScanRegistry::with_conn(conn_id));
    }
}

pub struct Connection<'bytes> {
    id: usize,
    recv_socket: Socket<'bytes>,
    send_buffer: LockFreeBuffer<'bytes>,
    result_buffer: Option<LockFreeBuffer<'bytes>>,
    client_pid: &'bytes AtomicI32,
    server_pid: &'bytes AtomicI32,
}

impl<'bytes> Connection<'bytes> {
    pub fn new(
        id: usize,
        recv_socket: Socket<'bytes>,
        send_buffer: LockFreeBuffer<'bytes>,
        server_pid: &'bytes AtomicI32,
        client_pid: &'bytes AtomicI32,
    ) -> Self {
        Self {
            id,
            recv_socket,
            send_buffer,
            result_buffer: None,
            client_pid,
            server_pid,
        }
    }

    pub async fn poll(&mut self) -> Result<()> {
        let pending = self.recv_socket.buffer.len();
        if pending > 0 {
            trace!("poll: data already available: {} bytes", pending);
            return Ok(());
        }
        trace!("poll: waiting for socket signal");
        (&mut self.recv_socket).await?;
        trace!(
            "poll: socket signaled (data available: {} bytes)",
            self.recv_socket.buffer.len()
        );
        Ok(())
    }

    /// Send SIGUSR1 to the client process ID stored in this connection.
    /// On non-Unix platforms, returns an error.
    pub fn signal_client(&self) -> Result<()> {
        #[cfg(unix)]
        {
            let pid = self.client_pid.load(Ordering::Relaxed);
            trace!(client_pid = pid, "signal_client: about to send SIGUSR1");
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
            trace!(client_pid = pid, "signal_client: SIGUSR1 sent");
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

    /// Read the server PID from shared memory.
    pub fn server_pid(&self) -> i32 {
        self.server_pid.load(Ordering::Relaxed)
    }

    pub async fn process_message(&mut self, storage: &mut Storage) -> Result<()> {
        // Guard against spurious wakeups where no bytes are available yet.
        let pending = self.recv_socket.buffer.len();
        if pending < 6 {
            // Header is at least 6 bytes (3 fixints + u16)
            trace!("process_message: woke but no header bytes available",);
            return Ok(());
        }
        let header = consume_header(&mut self.recv_socket.buffer)?;
        debug!(
            direction = ?header.direction,
            tag = header.tag,
            flag = ?header.flag,
            length = header.length,
            recv_unread = self.recv_socket.buffer.len(),
            send_unread = self.send_buffer.len(),
            "process_message: header received"
        );
        if header.direction == Direction::ToClient {
            trace!("process_message: header direction ToClient, ignoring");
            return Ok(());
        }
        // Column layout arrives during planning/prepare; store it for later encoding use.
        if header.tag == ControlPacket::ColumnLayout as u8 {
            let attrs = consume_column_layout(&mut self.recv_socket.buffer)?;
            let len = attrs.len();
            tracing::trace!(
                target = "executor::server",
                pg_attrs = len,
                "column layout received"
            );
            storage.pg_attrs = Some(attrs);
            return Ok(());
        }
        // Data packets are handled out-of-band: push heap blocks into registry channels.
        if is_data_tag(header.tag) {
            match DataPacket::try_from(header.tag) {
                Ok(DataPacket::Heap) => {
                    // Read metadata; we expect the visibility bitmap to be stored in shared memory.
                    let meta =
                        read_heap_block_bitmap_meta(&mut self.recv_socket.buffer, header.length)?;
                    trace!(
                        target = "executor::server",
                        scan_id = meta.scan_id,
                        slot_id = meta.slot_id,
                        table_oid = meta.table_oid,
                        blkno = meta.blkno,
                        num_offsets = meta.num_offsets,
                        bitmap_inline_len = meta.bitmap_len,
                        "heap bitmap meta received"
                    );
                    // If payload included inline bitmap bytes (legacy), drain them without allocation.
                    let mut remaining = meta.bitmap_len as usize;
                    while remaining > 0 {
                        let mut buf = [0u8; 256];
                        let chunk = remaining.min(buf.len());
                        std::io::Read::read_exact(&mut self.recv_socket.buffer, &mut buf[..chunk])?;
                        remaining -= chunk;
                    }
                    // Trailing u16 carries the visibility bitmap length in shared memory
                    let vis_len = read_u16_msgpack(&mut self.recv_socket.buffer)?;
                    trace!(
                        target = "executor::server",
                        scan_id = meta.scan_id,
                        slot_id = meta.slot_id,
                        vis_len,
                        "heap bitmap vis_len (shm) received"
                    );
                    if let Some(tx) = storage.registry.sender(meta.scan_id as u64) {
                        let block = crate::pgscan::HeapBlock {
                            scan_id: meta.scan_id as u64,
                            slot_id: meta.slot_id,
                            table_oid: meta.table_oid,
                            blkno: meta.blkno,
                            num_offsets: meta.num_offsets,
                            vis_len,
                        };
                        // Best-effort send; if the channel is full, await until space is available
                        if let Err(e) = tx.send(block).await {
                            tracing::warn!(
                                target = "pg_fusion::server",
                                "failed to enqueue heap block for scan {}: {e}",
                                meta.scan_id
                            );
                        }
                        // Request the next heap block for this scan using the same slot
                        trace!(
                            target = "executor::server",
                            scan_id = meta.scan_id,
                            table_oid = meta.table_oid,
                            slot_id = meta.slot_id,
                            "requesting next heap block"
                        );
                        if let Err(e) = request_heap_block(
                            &mut self.send_buffer,
                            meta.scan_id,
                            meta.table_oid,
                            meta.slot_id,
                        ) {
                            tracing::error!(
                                target = "pg_fusion::server",
                                "failed to request next heap block for scan {}: {e}",
                                meta.scan_id
                            );
                        }
                    } else {
                        trace!(
                            target = "pg_fusion::server",
                            scan_id = meta.scan_id,
                            "received heap block for unknown scan_id; dropping"
                        );
                        // Drop the payload (already consumed above)
                    }
                    return Ok(());
                }
                Ok(DataPacket::Eof) => {
                    // Per-scan EOF notification
                    let scan_id = read_u64_msgpack(&mut self.recv_socket.buffer)?;
                    storage.registry.close(scan_id as u64);
                    return Ok(());
                }
                _ => {
                    trace!("process_message: unrecognized data packet, ignoring");
                    return Ok(());
                }
            }
        }
        let mut packet = ControlPacket::try_from(header.tag)?;
        let mut skip_metadata = false;
        loop {
            let action = storage.state.consume(&packet)?;
            trace!(current_packet = ?packet, action = ?action, "process_message: state consumed");
            let result = match action {
                Some(Action::Bind) => {
                    let Some(plan) = std::mem::take(&mut storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing bind message".into(),
                        ));
                    };
                    let params = read_params(&mut self.recv_socket.buffer)?;
                    trace!("process_message: Action::Bind with {} params", params.len());
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
                        Catalog::with_registry(Arc::clone(&storage.registry))
                    } else {
                        Catalog::from_stream(
                            &mut self.recv_socket.buffer,
                            Arc::clone(&storage.registry),
                        )?
                    };
                    trace!(
                        skip_metadata,
                        "process_message: Action::Compile (catalog {}loaded)",
                        if skip_metadata { "not " } else { "" }
                    );
                    compile(stmt, &catalog)
                }
                Some(Action::Explain) => {
                    trace!("process_message: Action::Explain");
                    if let Some(phys) = storage.physical_plan.as_ref() {
                        explain_physical(phys)
                    } else {
                        bail!(FusionError::NotFound(
                            "Physical plan".into(),
                            "no plan available for explain".into(),
                        ));
                    }
                }
                Some(Action::Optimize) => {
                    let Some(plan) = std::mem::take(&mut storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing optimize message".into(),
                        ));
                    };
                    trace!("process_message: Action::Optimize");
                    optimize(plan)
                }
                Some(Action::Flush) => {
                    trace!("process_message: Action::Flush (reset state)");
                    storage.flush();
                    return Ok(());
                }
                Some(Action::Parse) => {
                    let query = read_query(&mut self.recv_socket.buffer)?;
                    trace!(query = %query, "process_message: Action::Parse");
                    parse(query.into())
                }
                Some(Action::Translate) => {
                    let Some(plan) = std::mem::take(&mut storage.logical_plan) else {
                        bail!(FusionError::NotFound(
                            "Logical plan".into(),
                            "while processing translate message".into(),
                        ));
                    };
                    trace!("process_message: Action::Translate");
                    translate(plan).await
                }
                Some(Action::OpenDataFlow) => {
                    trace!("process_message: Action::OpenDataFlow");
                    open_data_flow(self, storage)
                }
                Some(Action::StartDataFlow) => {
                    trace!("process_message: Action::StartDataFlow");
                    start_data_flow(self, storage)
                }
                Some(Action::EndDataFlow) => {
                    trace!("process_message: Action::EndDataFlow");
                    end_data_flow(storage)
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
                    trace!(
                        columns = plan.schema().fields().len(),
                        "process_message: prepared columns for Bind"
                    );
                    storage.logical_plan = Some(plan);
                    // Immediately schedule an Optimize pass for the next loop iteration.
                    packet = ControlPacket::Optimize;
                    trace!("process_message: scheduling Optimize after Bind");
                    continue;
                }
                TaskResult::Compilation(plan) => {
                    storage.logical_plan = Some(plan);
                    request_params(&mut self.send_buffer)?;
                    trace!("process_message: requested params after Compilation");
                }
                TaskResult::Optimized(plan) => {
                    storage.logical_plan = Some(plan);
                    packet = ControlPacket::Translate;
                    trace!("process_message: scheduling Translate after Optimize");
                    continue;
                }
                TaskResult::Translated(phys) => {
                    storage.physical_plan = phys;
                    trace!("process_message: transitioned to PhysicalPlan (built physical plan)");
                }
                TaskResult::Explain(explain) => {
                    prepare_explain(&mut self.send_buffer, explain.as_str())?;
                    trace!(
                        explain_len = explain.len(),
                        "process_message: prepared Explain"
                    );
                }
                TaskResult::Noop => {
                    // Intentionally no-op (e.g., heap packet consumed elsewhere)
                }
                TaskResult::Parsing((stmt, tables)) => {
                    storage.statement = Some(stmt);
                    if tables.is_empty() {
                        // We don't need any table metadata for this query.
                        // Let's move connection to the next state.
                        skip_metadata = true;
                        packet = ControlPacket::Metadata;
                        trace!("process_message: no tables, skipping metadata, forcing ControlPacket::Metadata");
                        continue;
                    } else {
                        trace!(
                            tables = tables.len(),
                            "process_message: preparing table refs"
                        );
                        prepare_table_refs(&mut self.send_buffer, tables.as_slice())?
                    }
                }
            }
            break;
        }
        debug!(
            send_unread = self.send_buffer.len(),
            client_pid = self.client_pid.load(Ordering::Relaxed),
            "process_message: response buffered, ready to signal client"
        );
        Ok(())
    }

    pub fn handle_error(&mut self, error: FusionError) {
        self.send_buffer.rollback();
        self.recv_socket.buffer.flush_read();
        let error_message = format_smolstr!("{error}");
        if let Err(e) = prepare_error(&mut self.send_buffer, &error_message) {
            tracing::error!(target = "pg_fusion::server", error = %e, "Failed to prepare error response");
        }
    }
}

/// Encode a RecordBatch into wire tuples and write them to the result ring.
/// Returns the number of rows written.
fn encode_and_write_rows(
    batch: &RecordBatch,
    attrs: &[PgAttrWire],
    ring: &mut LockFreeBuffer,
) -> usize {
    use protocol::tuple::{encode_wire_tuple, Field as F};
    // write_frame used via full path
    let rows = batch.num_rows();
    let cols = std::cmp::min(batch.num_columns(), attrs.len());
    if cols == 0 || rows == 0 {
        tracing::trace!(
            target = "executor::server",
            rows,
            cols,
            attrs = attrs.len(),
            "execution: empty batch or no attrs; skipping write"
        );
        return 0;
    }
    // Working buffers reused across rows to minimize allocations
    let mut byref_idx: Vec<Option<usize>> = vec![None; cols];
    let mut owned: Vec<Vec<u8>> = Vec::with_capacity(cols);
    let mut buf: Vec<u8> = Vec::with_capacity(256);
    let mut written = 0usize;
    for row in 0..rows {
        // First pass: collect byref bytes and remember their indices
        owned.clear();
        for (i, idx_ref) in byref_idx.iter_mut().enumerate().take(cols) {
            *idx_ref = None;
            let array = batch.column(i);
            if array.is_null(row) {
                continue;
            }
            if array.data_type() == &datafusion::arrow::datatypes::DataType::Utf8 {
                let a = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("utf8 array");
                let s = a.value(row);
                let idx = owned.len();
                owned.push(s.as_bytes().to_vec());
                *idx_ref = Some(idx);
            }
        }
        // Second pass: build Field view and encode (scope fields to drop borrows before next row)
        {
            let mut fields: Vec<F> = Vec::with_capacity(cols);
            for (i, idx_ref) in byref_idx.iter().enumerate().take(cols) {
                let array = batch.column(i);
                if array.is_null(row) {
                    fields.push(F::Null);
                    continue;
                }
                match array.data_type() {
                    datafusion::arrow::datatypes::DataType::Int32 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<Int32Array>()
                            .expect("int32 array");
                        let v = a.value(row);
                        fields.push(F::ByVal4(v.to_le_bytes()));
                    }
                    datafusion::arrow::datatypes::DataType::Int64 => {
                        let a = array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .expect("int64 array");
                        let v = a.value(row);
                        fields.push(F::ByVal8(v.to_le_bytes()));
                    }
                    datafusion::arrow::datatypes::DataType::Utf8 => {
                        if let Some(idx) = idx_ref {
                            fields.push(F::ByRef(owned[*idx].as_slice()));
                        } else {
                            fields.push(F::Null);
                        }
                    }
                    _ => fields.push(F::Null),
                }
            }
            buf.clear();
            if encode_wire_tuple(&mut buf, &attrs[..cols], &fields).is_ok() {
                let _ = protocol::result::write_frame(ring, &buf);
                written += 1;
            }
        }
    }
    written
}

impl<'bytes> Connection<'bytes> {
    /// For worker setup: set the per-connection result ring buffer writer.
    pub fn set_result_buffer(&mut self, buf: LockFreeBuffer<'bytes>) {
        self.result_buffer = Some(buf);
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

fn explain_physical(plan: &Arc<dyn ExecutionPlan>) -> Result<TaskResult> {
    let s = format!("{}", displayable(plan.as_ref()).indent(false));
    Ok(TaskResult::Explain(SmolStr::from(s)))
}

fn optimize(plan: LogicalPlan) -> Result<TaskResult> {
    // Force single-partition planning to keep all operators in one pipeline
    let mut opts = ConfigOptions::default();
    opts.execution.target_partitions = 1;
    let state = SessionStateBuilder::new().with_config(opts.into()).build();
    let optimized = state.optimize(&plan)?;
    Ok(TaskResult::Optimized(optimized))
}

async fn translate(plan: LogicalPlan) -> Result<TaskResult> {
    // Ensure physical planning also uses single-partition execution
    let mut opts = ConfigOptions::default();
    opts.execution.target_partitions = 1;
    let state = SessionStateBuilder::new().with_config(opts.into()).build();
    let optimized = state.optimize(&plan)?; // ensure optimized prior to physical
    match state.create_physical_plan(&optimized).await {
        Ok(physical) => Ok(TaskResult::Translated(Some(physical))),
        Err(e) => {
            // Propagate the actual physical planning error so the backend can report it
            // instead of failing later during OpenDataFlow.
            tracing::warn!("translate: failed to build physical plan: {e}");
            anyhow::bail!(FusionError::FailedTo(
                "build physical plan".into(),
                format_smolstr!("{e}")
            ))
        }
    }
}

fn parse(query: SmolStr) -> Result<TaskResult> {
    let stmts = DFParser::parse_sql(query.as_str())?;
    let Some(mut stmt) = stmts.into_iter().next() else {
        return Err(FusionError::ParseQuery(query).into());
    };
    // If the incoming SQL is EXPLAIN <stmt>, unwrap to the inner statement so that
    // subsequent compile/translate produce a plan for the actual query rather than
    // DataFusion's ExplainExec wrapper.
    stmt = match stmt {
        Statement::Explain(inner) => {
            // Attempt to unwrap inner statement of EXPLAIN
            #[allow(clippy::redundant_clone)]
            let s = *inner.statement.clone();
            s
        }
        other => other,
    };

    let state = SessionStateBuilder::new().build();
    let tables = state.resolve_table_references(&stmt)?;
    Ok(TaskResult::Parsing((stmt, tables)))
}

fn open_data_flow(_conn: &mut Connection, storage: &mut Storage) -> Result<TaskResult> {
    if storage.physical_plan.is_none() {
        bail!(FusionError::NotFound(
            "Physical plan".into(),
            "open data flow requires a physical plan".into(),
        ));
    }
    let phys = storage.physical_plan.as_ref().expect("checked above");
    // Reserve capacity to avoid HashMap reallocation while registering
    let scan_count = count_scans(phys);
    trace!(
        scan_count,
        "open_data_flow: reserving scan registry capacity"
    );
    storage.registry.reserve(scan_count);
    // Register channels per scan in the connection-local registry; no response payload
    let _ = for_each_scan::<_, Error>(phys, |id, table_oid| {
        trace!(
            scan_id = id,
            table_oid,
            "open_data_flow: registering scan channel"
        );
        let _ = storage.registry.register(id, 16);
        Ok(())
    });
    Ok(TaskResult::Noop)
}

fn start_data_flow(conn: &mut Connection, storage: &mut Storage) -> Result<TaskResult> {
    if storage.physical_plan.is_none() {
        bail!(FusionError::NotFound(
            "Physical plan".into(),
            "start data flow requires a physical plan".into(),
        ));
    }
    // If a task is already running, don't start another
    if storage.exec_task.is_some() {
        trace!("start_data_flow: execution already running, skipping");
        return Ok(TaskResult::Noop);
    }

    let plan = Arc::clone(storage.physical_plan.as_ref().expect("checked above"));
    let pg_attrs = storage.pg_attrs.clone().unwrap_or_default();
    tracing::trace!(
        target = "executor::server",
        pg_attrs = pg_attrs.len(),
        "start_data_flow: attrs snapshot"
    );
    // Build a fresh TaskContext for execution (single partition)
    let mut opts = ConfigOptions::default();
    opts.execution.target_partitions = 1;
    let state = SessionStateBuilder::new().with_config(opts.into()).build();
    let ctx = state.task_ctx();

    let conn_id = conn.id;
    let client_pid_val = conn.client_pid.load(std::sync::atomic::Ordering::Relaxed); // snapshot pid
    trace!(conn_id, "start_data_flow: spawning execution task");
    let handle = tokio::spawn(async move {
        // Execute all output partitions and merge them into the single result ring
        let mut ring = crate::shm::result_ring_writer_for(conn_id);
        let part_count = plan.output_partitioning().partition_count();
        tracing::trace!(
            target = "executor::server",
            partitions = part_count,
            "execution: determined output partitions"
        );
        for part in 0..part_count {
            match plan.execute(part, Arc::clone(&ctx)) {
                Ok(mut stream) => {
                    while let Some(res) = stream.next().await {
                        match res {
                            Err(e) => {
                                tracing::error!(
                                    target = "pg_fusion::server",
                                    "execution stream error (part {part}): {e}"
                                );
                                break;
                            }
                            Ok(batch) => {
                                if pg_attrs.is_empty() {
                                    continue;
                                }
                                let wrote = encode_and_write_rows(&batch, &pg_attrs, &mut ring);
                                tracing::trace!(
                                    target = "executor::server",
                                    rows = wrote,
                                    cols = batch.num_columns(),
                                    partition = part,
                                    "execution: batch encoded and written"
                                );
                                if wrote > 0 {
                                    let pid = client_pid_val;
                                    if pid > 0 && pid != i32::MAX {
                                        unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
                                    }
                                }
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(
                        target = "pg_fusion::server",
                        "failed to start partition {part}: {e}"
                    );
                }
            }
        }
        tracing::trace!(
            target = "pg_fusion::server",
            "execution streams completed for all partitions"
        );
        // Write EOF sentinel row to result ring to unblock backend
        let _ = protocol::result::write_eof(&mut ring);
        // Nudge the backend in case it is waiting on latch
        let pid = client_pid_val;
        if pid > 0 && pid != i32::MAX {
            unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
        }
    });
    storage.exec_task = Some(handle);
    // Send a tiny ack to the backend indicating execution is ready
    trace!("start_data_flow: sending ExecReady to backend");
    protocol::exec::prepare_exec_ready(&mut conn.send_buffer)?;
    // No demo row: result rows will be written by the execution task
    // Kick off initial heap block requests for each scan using per-scan assigned slot
    if let Some(phys) = storage.physical_plan.as_ref() {
        let registry = Arc::clone(&storage.registry);
        let _ = for_each_scan::<_, Error>(phys, |id, table_oid| {
            let slot = registry.slot_for(id).unwrap_or(0);
            trace!(
                scan_id = id,
                table_oid,
                slot_id = slot,
                "start_data_flow: initial heap request"
            );
            request_heap_block(&mut conn.send_buffer, id, table_oid, slot)?;
            Ok(())
        });
    }
    Ok(TaskResult::Noop)
}

fn end_data_flow(storage: &mut Storage) -> Result<TaskResult> {
    // Use unified reset to ensure state machine returns to Initialized and
    // any in-flight execution task is aborted, while preserving registry object.
    storage.flush();
    Ok(TaskResult::Noop)
}

enum TaskResult {
    Parsing((Statement, Vec<TableReference>)),
    Compilation(LogicalPlan),
    Bind(LogicalPlan),
    Optimized(LogicalPlan),
    Translated(Option<Arc<dyn ExecutionPlan>>),
    Explain(SmolStr),
    Noop,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::LockFreeBuffer;
    use crate::fsm::ExecutorState;
    use crate::ipc::SharedState;
    use core::mem::size_of;
    use protocol::bind::prepare_params;
    use protocol::columns::consume_columns;
    use protocol::explain::request_explain;
    use protocol::metadata::process_metadata_with_response;
    use protocol::parse::prepare_query;
    use protocol::ControlPacket;
    use rmp::decode::read_bin_len;
    use std::cell::UnsafeCell;
    use std::ffi::CStr;
    use std::io::{Read, Write};
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

    // Local mocks for metadata processing used by tests
    fn mock_table_lookup(name: &[u8]) -> anyhow::Result<u32> {
        if name == b"t2\0" {
            return Ok(666);
        }
        unreachable!();
    }

    fn mock_schema_table_lookup(schema: &[u8], name: &[u8]) -> anyhow::Result<u32> {
        if schema == b"public\0" && name == b"t1\0" {
            return Ok(42);
        }
        unreachable!();
    }

    fn mock_table_serialize(
        id: u32,
        need_schema: bool,
        output: &mut dyn std::io::Write,
    ) -> anyhow::Result<()> {
        match id {
            42 => {
                assert!(need_schema);
                output
                    .write_all(b"\x93\xce\x00\x00\x00\x2a\xa6public\xa2t1")
                    .expect("write t1");
                // Two columns: a:Int64;N, b:Utf8 (not null)
                output
                    .write_all(b"\x92\x93\xcc\x04\xc3\xa1a\x93\xcc\x01\xc2\xa1b")
                    .expect("write t1 columns");
            }
            666 => {
                assert!(!need_schema);
                output
                    .write_all(b"\x92\xce\x00\x00\x02\x9a\xa2t2")
                    .expect("write t2");
                // One column (example): c:Utf8 nullable
                output
                    .write_all(b"\x91\x93\xcc\x01\xc3\xa1c")
                    .expect("write t2 columns");
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_parse() -> Result<()> {
        use crate::layout::{connection_layout, connection_ptrs};
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flags.get() }));

        // Derive recv/send buffers from the connection layout.
        let layout = connection_layout(PAYLOAD_SIZE, PAYLOAD_SIZE).expect("layout");
        let base = unsafe { (*BYTES.conn.get()).as_mut_ptr() };
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
        static SERVER_PID: AtomicI32 = AtomicI32::new(0);
        static CLIENT_PID: AtomicI32 = AtomicI32::new(0);
        let mut conn = Connection::new(0, socket, send_buffer, &SERVER_PID, &CLIENT_PID);
        let storage = Storage {
            registry: Arc::new(ScanRegistry::new()),
            ..Default::default()
        };
        tokio::spawn(async move {
            let mut storage = storage;
            // Test parsing a query with tables.
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from t";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message(&mut storage)
                .await
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
                .await
                .expect("Failed to process parse message");
            let Ok(TaskResult::Parsing((stmt, _))) = parse(sql.into()) else {
                unreachable!();
            };
            let Ok(TaskResult::Compilation(plan)) =
                compile(stmt, &Catalog::with_registry(Arc::new(ScanRegistry::new())))
            else {
                unreachable!();
            };
            assert_eq!(storage.logical_plan, Some(plan));
            assert_eq!(storage.state.state(), &ExecutorState::LogicalPlan);
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_from_parse_to_physical() -> Result<()> {
        use crate::layout::{connection_layout, connection_ptrs};
        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flags.get() }));

        let layout = connection_layout(PAYLOAD_SIZE, PAYLOAD_SIZE).expect("layout");
        let base = unsafe { (*BYTES.conn.get()).as_mut_ptr() };
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
        static SERVER_PID: AtomicI32 = AtomicI32::new(0);
        static CLIENT_PID: AtomicI32 = AtomicI32::new(0);
        let mut conn = Connection::new(0, socket, send_buffer, &SERVER_PID, &CLIENT_PID);
        let storage = Storage {
            registry: Arc::new(ScanRegistry::new()),
            ..Default::default()
        };
        tokio::spawn(async move {
            let mut storage = storage;
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from public.t1 where b = $1";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("Failed to prepare SQL");
            conn.process_message(&mut storage)
                .await
                .expect("Failed to process parse message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::Statement);

            // 1) Parse -> Statement, server requests Metadata
            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume metadata header");
            assert_eq!(header.direction, Direction::ToClient);
            assert_eq!(header.tag, ControlPacket::Metadata as u8);
            process_metadata_with_response(
                &mut conn.send_buffer,
                &mut conn.recv_socket.buffer,
                mock_schema_table_lookup,
                mock_table_lookup,
                mock_table_serialize,
            )
            .expect("Failed to process metadata");
            // 2) Compile -> LogicalPlan after sending metadata
            conn.process_message(&mut storage)
                .await
                .expect("Failed to process metadata message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::LogicalPlan);

            // 3) Server requests Bind (Columns will be sent back), then we send params
            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume bind header");
            assert_eq!(header.direction, Direction::ToClient);
            assert_eq!(header.tag, ControlPacket::Bind as u8);
            prepare_params(&mut conn.recv_socket.buffer, || {
                (1, vec![Ok(ScalarValue::Int32(Some(1)))].into_iter())
            })
            .expect("Failed to prepare params");
            // 4) Bind -> Optimize -> Translate (to PhysicalPlan) in subsequent iterations
            conn.process_message(&mut storage)
                .await
                .expect("Failed to process bind/optimize/translate messages");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::PhysicalPlan);
            // Optionally, physical plan may or may not be built in this environment
            // assert!(storage.physical_plan.is_some() || storage.physical_plan.is_none());

            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume bind header");
            assert_eq!(header.direction, Direction::ToClient);
            assert_eq!(header.tag, ControlPacket::Columns as u8);
            type Payload = (i16, u8, bool, Vec<u8>);
            let mut columns: Vec<Payload> = Vec::new();
            let columns_ptr = &mut columns as *mut Vec<Payload> as *mut c_void;
            let repack = |pos: i16,
                          etype: u8,
                          nullable: bool,
                          name: &[u8],
                          ptr: *mut c_void|
             -> Result<()> {
                let columns: &mut Vec<Payload> = unsafe { &mut *(ptr as *mut Vec<Payload>) };
                columns.push((pos, etype, nullable, name.to_vec()));
                Ok(())
            };
            consume_columns(&mut conn.send_buffer, columns_ptr, repack)
                .expect("Failed to consume columns");
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0], (0, 4, true, b"a\0".into()));

            request_explain(&mut conn.recv_socket.buffer).expect("Failed to request explain");
            conn.process_message(&mut storage)
                .await
                .expect("Failed to process explain message");
            assert_eq!(conn.recv_socket.buffer.len(), 0);
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let header =
                consume_header(&mut conn.send_buffer).expect("Failed to consume explain header");
            assert_eq!(header.direction, Direction::ToClient);
            assert_eq!(header.tag, ControlPacket::Explain as u8);
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
            // Physical explain should be non-empty and reference a TableScan
            assert!(!explain.is_empty());
            // Physical explain should contain execution nodes (e.g., *Exec)
            assert!(explain.contains("Exec"));
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_begin_scan_registers_scans_on_begin() -> Result<()> {
        use crate::layout::{connection_layout, connection_ptrs};
        use protocol::exec::request_begin_scan;

        static BYTES: ConnMemory = ConnMemory::new();
        let state = Arc::new(SharedState::new(unsafe { &*BYTES.flags.get() }));

        let layout = connection_layout(PAYLOAD_SIZE, PAYLOAD_SIZE).expect("layout");
        let base = unsafe { (*BYTES.conn.get()).as_mut_ptr() };
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
        static SERVER_PID: AtomicI32 = AtomicI32::new(0);
        static CLIENT_PID: AtomicI32 = AtomicI32::new(0);
        let mut conn = Connection::new(0, socket, send_buffer, &SERVER_PID, &CLIENT_PID);
        let storage = Storage {
            registry: Arc::new(ScanRegistry::new()),
            ..Default::default()
        };
        tokio::spawn(async move {
            let mut storage = storage;
            // Drive to PhysicalPlan state for a query that touches one table
            assert_eq!(storage.state.state(), &ExecutorState::Initialized);
            let sql = "select a from public.t1 where b = $1";
            prepare_query(&mut conn.recv_socket.buffer, sql).expect("prepare SQL");
            conn.process_message(&mut storage)
                .await
                .expect("process parse");
            // Metadata request -> supply it
            let header = consume_header(&mut conn.send_buffer).expect("consume metadata hdr");
            assert_eq!(header.direction, Direction::ToClient);
            assert_eq!(header.tag, ControlPacket::Metadata as u8);
            process_metadata_with_response(
                &mut conn.send_buffer,
                &mut conn.recv_socket.buffer,
                mock_schema_table_lookup,
                mock_table_lookup,
                mock_table_serialize,
            )
            .expect("process metadata");
            // Compile -> Bind request
            conn.process_message(&mut storage)
                .await
                .expect("process metadata msg");
            let header = consume_header(&mut conn.send_buffer).expect("consume bind hdr");
            assert_eq!(header.direction, Direction::ToClient);
            assert_eq!(header.tag, ControlPacket::Bind as u8);
            // Provide params then proceed to PhysicalPlan
            prepare_params(&mut conn.recv_socket.buffer, || {
                (1, vec![Ok(ScalarValue::Int32(Some(1)))].into_iter())
            })
            .expect("prepare params");
            conn.process_message(&mut storage)
                .await
                .expect("bind/opt/translate");
            assert_eq!(storage.state.state(), &ExecutorState::PhysicalPlan);

            // Now send BeginScan; executor should register channels without sending payload
            request_begin_scan(&mut conn.recv_socket.buffer).expect("write BeginScan");
            // Ensure header is committed in the ring; defensively flush and assert
            conn.recv_socket
                .buffer
                .flush()
                .expect("flush begin-scan header");
            assert!(
                conn.recv_socket.buffer.len() >= 5,
                "begin-scan header not queued"
            );
            let before = conn.send_buffer.len();
            conn.process_message(&mut storage)
                .await
                .expect("process begin-scan");
            // No additional response expected
            assert_eq!(conn.send_buffer.len(), before);

            // Verify a receiver was registered for the scan with table_oid 42
            let mut scan_id_opt: Option<u64> = None;
            if let Some(phys) = storage.physical_plan.as_ref() {
                let _ = for_each_scan::<_, anyhow::Error>(phys, |id, table_oid| {
                    if table_oid == 42 {
                        scan_id_opt = Some(id);
                    }
                    Ok(())
                });
            }
            let scan_id = scan_id_opt.expect("scan id for table_oid 42 not found");
            let rx = storage.registry.take_receiver(scan_id);
            assert!(rx.is_some());
        })
        .await?;

        Ok(())
    }
}
