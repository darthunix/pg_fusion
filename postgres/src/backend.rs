use crate::ipc::{connection_id, connection_shared, current_connection_id, ConnectionShared};
use crate::worker::{result_ring_base_for, result_ring_layout};
use crate::worker::{slot_blocks_base_for, slot_blocks_layout};
use anyhow::Result as AnyResult;
use common::FusionError;
use datafusion::arrow::datatypes::DataType;
use executor::buffer::LockFreeBuffer;
use executor::layout::{slot_block_ptr, slot_block_vis_ptr};
use libc::c_long;
use pgrx::pg_sys::AsPgCStr;
use pgrx::pg_sys::SysCacheIdentifier::TYPEOID;
use pgrx::pg_sys::{
    self, error, palloc0, CustomExecMethods, CustomScanMethods, CustomScanState, InvalidOid, List,
    MyLatch, Node, Oid, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
};
use pgrx::{check_for_interrupts, pg_guard};
use protocol::bind::prepare_params;
use protocol::columns::consume_columns;
use protocol::consume_header;
use protocol::data_type::EncodedType;
use protocol::exec::prepare_scan_eof;
use protocol::exec::{request_begin_scan, request_end_scan, request_exec_scan};
use protocol::explain::request_explain;
use protocol::failure::{read_error, request_failure};
use protocol::heap::{prepare_heap_block_meta_shm, read_heap_block_request};
use protocol::metadata::process_metadata_with_response;
use protocol::parse::prepare_query;
use protocol::tuple::decode_wire_tuple;
use protocol::tuple::{prepare_column_layout, PgAttrWire};
use protocol::Tape;
use protocol::{ControlPacket, DataPacket, Direction};
use rmp::decode::read_bin_len;
use rmp::encode::{write_array_len, write_bool, write_str, write_u32, write_u8};
use smallvec::SmallVec;
use smol_str::format_smolstr;
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::ffi::c_char;
use std::ffi::CStr;
use std::os::raw::c_void;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;
use std::time::Instant;

fn handle_metadata(send: &mut LockFreeBuffer, recv: &mut LockFreeBuffer) -> AnyResult<()> {
    let schema_table_lookup = |schema: &[u8], table: &[u8]| -> AnyResult<u32> {
        let ns = CStr::from_bytes_with_nul(schema)
            .map_err(|_| anyhow::anyhow!("invalid schema cstr"))?;
        let tbl =
            CStr::from_bytes_with_nul(table).map_err(|_| anyhow::anyhow!("invalid table cstr"))?;
        let ns_oid = unsafe { pg_sys::get_namespace_oid(ns.as_ptr(), false) };
        let rel_oid = unsafe { pg_sys::get_relname_relid(tbl.as_ptr(), ns_oid) };
        if rel_oid == InvalidOid {
            Err(anyhow::anyhow!("table not found"))
        } else {
            Ok(rel_oid.to_u32())
        }
    };
    let table_lookup = |table: &[u8]| -> AnyResult<u32> {
        let tbl =
            CStr::from_bytes_with_nul(table).map_err(|_| anyhow::anyhow!("invalid table cstr"))?;
        let mut search_path: [Oid; 16] = [InvalidOid; 16];
        let path_len = unsafe {
            pg_sys::fetch_search_path_array(search_path.as_mut_ptr(), search_path.len() as i32)
        } as usize;
        let path = &search_path[..path_len];
        for ns_oid in path {
            let rel_oid = unsafe { pg_sys::get_relname_relid(tbl.as_ptr(), *ns_oid) };
            if rel_oid != InvalidOid {
                return Ok(rel_oid.to_u32());
            }
        }
        Err(anyhow::anyhow!("table not found in search_path"))
    };
    let table_serialize = |rel_oid: u32, need_schema: bool, out: &mut dyn std::io::Write| {
        let rel_oid = Oid::from(rel_oid);
        let rel = unsafe { pgrx::PgRelation::with_lock(rel_oid, pg_sys::AccessShareLock as i32) };
        struct Out<'a>(&'a mut dyn std::io::Write);
        impl std::io::Write for Out<'_> {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.write(buf)
            }
            fn flush(&mut self) -> std::io::Result<()> {
                self.0.flush()
            }
        }
        let mut ow = Out(out);
        if need_schema {
            write_array_len(&mut ow, 3)?;
            write_u32(&mut ow, rel_oid.to_u32())?;
            write_str(&mut ow, rel.namespace())?;
            write_str(&mut ow, rel.name())?;
        } else {
            write_array_len(&mut ow, 2)?;
            write_u32(&mut ow, rel_oid.to_u32())?;
            write_str(&mut ow, rel.name())?;
        }
        let tuple_desc = rel.tuple_desc();
        let attr_num = u32::try_from(tuple_desc.iter().filter(|a| !a.is_dropped()).count())?;
        write_array_len(&mut ow, attr_num)?;
        for attr in tuple_desc.iter() {
            if attr.is_dropped() {
                continue;
            }
            write_array_len(&mut ow, 3)?;
            let etype = match oid_to_encoded_type(attr.atttypid) {
                Some(t) => t as u8,
                None => {
                    // Fail fast on unsupported types so the client can fall back
                    return Err(anyhow::anyhow!(FusionError::UnsupportedType(
                        format_smolstr!("pg type oid {:?}", attr.atttypid)
                    )));
                }
            };
            write_u8(&mut ow, etype)?;
            let is_nullable = !attr.attnotnull;
            write_bool(&mut ow, is_nullable)?;
            write_str(&mut ow, attr.name())?;
        }
        AnyResult::Ok(())
    };

    process_metadata_with_response(
        send,
        recv,
        schema_table_lookup,
        table_lookup,
        table_serialize,
    )?;
    Ok(())
}

fn probe_metrics_for_conn(conn_id: usize) -> Option<&'static executor::telemetry::ConnTelemetry> {
    if !executor::telemetry::probe_enabled() {
        return None;
    }
    if let Some(metrics) = executor::telemetry::try_conn_telemetry(conn_id) {
        return Some(metrics);
    }
    let num = crate::max_backends() as usize;
    let layout = executor::telemetry::telemetry_layout(num).ok()?;
    let mut _found = false;
    let base = unsafe {
        pg_sys::ShmemInitStruct(
            "pg_fusion:telemetry".as_pg_cstr(),
            layout.layout.size(),
            &mut _found,
        ) as *mut u8
    };
    executor::telemetry::set_telemetry(base, num);
    executor::telemetry::try_conn_telemetry(conn_id)
}

fn probe_metrics_current() -> Option<&'static executor::telemetry::ConnTelemetry> {
    current_connection_id().and_then(|id| probe_metrics_for_conn(id as usize))
}

fn reset_probe_metrics_for_current_conn() {
    if !executor::telemetry::probe_enabled() {
        return;
    }
    let conn_id = match connection_id() {
        Ok(v) => v as usize,
        Err(_) => return,
    };
    if let Some(metrics) = probe_metrics_for_conn(conn_id) {
        let epoch = metrics.reset_for_new_epoch();
        PROBE_EPOCH.with(|cell| cell.set(epoch));
        LAST_WORKER_REQUEST_SEQ.with(|cell| cell.set(0));
        LAST_RESULT_SIGNAL_SEQ.with(|cell| cell.set(0));
    }
}

fn log_probe_summary(reason: &str) {
    if !executor::telemetry::probe_enabled() {
        return;
    }
    let conn_id = match current_connection_id() {
        Some(v) => v as usize,
        None => return,
    };
    let Some(metrics) = probe_metrics_for_conn(conn_id) else {
        return;
    };
    let s = metrics.snapshot();
    pgrx::warning!(
        "pg_fusion_probe summary conn={} reason={} epoch={} backend_wait_ms={:.3} backend_wait_count={} backend_wait_timeouts={} backend_heap_serve_ms={:.3} backend_heap_serve_count={} backend_result_read_ms={:.3} backend_result_read_count={}",
        conn_id,
        reason,
        s.epoch,
        executor::telemetry::ProbeSnapshot::total_ms(s.backend_wait_ns),
        s.backend_wait_count,
        s.backend_wait_timeout_count,
        executor::telemetry::ProbeSnapshot::total_ms(s.backend_heap_serve_ns),
        s.backend_heap_serve_count,
        executor::telemetry::ProbeSnapshot::total_ms(s.backend_result_read_ns),
        s.backend_result_read_count,
    );
    pgrx::warning!(
        "pg_fusion_probe transport conn={} backend_to_worker_page_ms={:.3} backend_to_worker_page_count={} backend_to_worker_page_avg_us={:.3} worker_to_backend_req_ms={:.3} worker_to_backend_req_count={} worker_to_backend_req_avg_us={:.3} worker_to_backend_result_ms={:.3} worker_to_backend_result_count={} worker_to_backend_result_avg_us={:.3}",
        conn_id,
        executor::telemetry::ProbeSnapshot::total_ms(s.backend_to_worker_page_ns),
        s.backend_to_worker_page_count,
        executor::telemetry::ProbeSnapshot::avg_us(
            s.backend_to_worker_page_ns,
            s.backend_to_worker_page_count,
        ),
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_to_backend_req_ns),
        s.worker_to_backend_req_count,
        executor::telemetry::ProbeSnapshot::avg_us(
            s.worker_to_backend_req_ns,
            s.worker_to_backend_req_count,
        ),
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_to_backend_result_ns),
        s.worker_to_backend_result_count,
        executor::telemetry::ProbeSnapshot::avg_us(
            s.worker_to_backend_result_ns,
            s.worker_to_backend_result_count,
        ),
    );
    pgrx::warning!(
        "pg_fusion_probe worker conn={} worker_poll_wait_ms={:.3} worker_poll_wait_count={} worker_heap_msg_ms={:.3} worker_heap_msg_count={} worker_heap_copy_ms={:.3} worker_heap_tx_send_ms={:.3} worker_request_next_ms={:.3} worker_pgscan_wait_ms={:.3} worker_pgscan_wait_count={} worker_pgscan_decode_ms={:.3} worker_pgscan_decode_count={} worker_result_write_ms={:.3} worker_result_write_count={}",
        conn_id,
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_poll_wait_ns),
        s.worker_poll_wait_count,
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_heap_msg_ns),
        s.worker_heap_msg_count,
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_heap_copy_ns),
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_heap_tx_send_ns),
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_request_next_ns),
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_pgscan_wait_ns),
        s.worker_pgscan_wait_count,
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_pgscan_decode_ns),
        s.worker_pgscan_decode_count,
        executor::telemetry::ProbeSnapshot::total_ms(s.worker_result_write_ns),
        s.worker_result_write_count,
    );
}

thread_local! {
    static SCAN_METHODS: CustomScanMethods = CustomScanMethods {
        CustomName: c"DataFusionScan".as_ptr() as *const c_char,
        CreateCustomScanState: Some(create_df_scan_state),
    };
    static EXEC_METHODS: CustomExecMethods = CustomExecMethods {
        CustomName: c"DataFusionScan".as_ptr() as *const c_char,
        BeginCustomScan: Some(begin_df_scan),
        ExecCustomScan: Some(exec_df_scan),
        EndCustomScan: Some(end_df_scan),
        ReScanCustomScan: None,
        MarkPosCustomScan: None,
        RestrPosCustomScan: None,
        EstimateDSMCustomScan: None,
        InitializeDSMCustomScan: None,
        ReInitializeDSMCustomScan: None,
        InitializeWorkerCustomScan: None,
        ShutdownCustomScan: None,
        ExplainCustomScan: Some(explain_df_scan),
    };
}

#[pg_guard]
#[no_mangle]
pub(crate) extern "C-unwind" fn init_datafusion_methods() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(scan_methods());
    }
}

pub(crate) fn scan_methods() -> *const CustomScanMethods {
    SCAN_METHODS.with(|m| m as *const CustomScanMethods)
}

pub(crate) fn exec_methods() -> *const CustomExecMethods {
    EXEC_METHODS.with(|m| m as *const CustomExecMethods)
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn create_df_scan_state(cscan: *mut pg_sys::CustomScan) -> *mut Node {
    // Acquire connection and shared buffers
    let id = match connection_id() {
        Ok(v) => v,
        Err(e) => error!("Failed to acquire connection id: {}", e),
    };
    let mut shared = match connection_shared(id) {
        Ok(s) => s,
        Err(e) => error!("Failed to map shared connection: {}", e),
    };
    // Ensure server can signal us
    shared.set_client_pid(unsafe { libc::getpid() } as i32);

    // Flush any stale bytes in the incoming ring before starting a new planning handshake
    // to avoid misaligned frames from a previous query.
    let mut flushed = 0usize;
    if shared.send.len() > 0 {
        use std::io::Read;
        loop {
            let avail = shared.send.len();
            if avail == 0 {
                break;
            }
            let mut buf = [0u8; 512];
            let take = avail.min(buf.len());
            match Read::read(&mut shared.send, &mut buf[..take]) {
                Ok(n) if n > 0 => flushed += n,
                _ => break,
            }
        }
        pgrx::debug1!(
            "handshake: flushed stale bytes before Parse (flushed={})",
            flushed
        );
    }

    // Fast-fail when worker is not running (PID not published yet)
    if shared.server_pid() <= 0 {
        error!(
            "DataFusion worker is not running (server PID is 0). Make sure shared_preload_libraries = 'pg_fusion' and restart the cluster."
        );
    }

    // Read the query from CustomScan private list and send it to server
    let list = unsafe { (*cscan).custom_private };
    let pattern = unsafe { (*list_nth(list, 0)).ptr_value as *const c_char };
    let query = unsafe { CStr::from_ptr(pattern) }
        .to_str()
        .expect("Expected a zero-terminated string");
    if let Err(err) = prepare_query(&mut shared.recv, query) {
        let _ = request_failure(&mut shared.recv);
        let _ = shared.signal_server();
        error!("Failed to prepare SQL: {}", err);
    }
    if let Err(err) = shared.signal_server() {
        error!("Failed to signal server: {}", err);
    }

    // Process server responses until Columns received
    let mut drain_remaining: usize = 0;
    loop {
        // If we are in the middle of draining an unexpected DataPacket payload, drain incrementally
        if drain_remaining > 0 {
            let avail = shared.send.len();
            if avail == 0 {
                wait_latch(Some(Duration::from_millis(1)));
            } else {
                use std::io::Read;
                let to_drain = drain_remaining.min(avail);
                let mut left = to_drain;
                while left > 0 {
                    let mut buf = [0u8; 256];
                    let take = left.min(buf.len());
                    match Read::read(&mut shared.send, &mut buf[..take]) {
                        Ok(n) if n > 0 => {
                            left -= n;
                            drain_remaining -= n;
                        }
                        _ => break,
                    }
                }
            }
            // Try another loop iteration; either we finished draining or will continue
            continue;
        }
        // If a full header is already buffered, parse it immediately; otherwise wait briefly.
        let cur_len = shared.send.len();
        if cur_len < 6 {
            wait_latch(Some(Duration::from_millis(1)));
            let cur_len2 = shared.send.len();
            if cur_len2 < 6 {
                continue;
            }
        }
        let header = match consume_header(&mut shared.send) {
            Ok(h) => h,
            Err(_err) => {
                // Partial frame observed; retry next wake without failing the query
                pgrx::debug1!("handshake: partial header (len={})", shared.send.len());
                continue;
            }
        };
        pgrx::debug1!(
            "handshake: parsed header tag={} dir={:?} recv_unread={}",
            header.tag,
            header.direction,
            shared.send.len()
        );
        if header.direction != Direction::ToClient {
            continue;
        }
        match ControlPacket::try_from(header.tag) {
            Ok(ControlPacket::None) => continue,
            Ok(ControlPacket::Failure) => match read_error(&mut shared.send) {
                Ok(msg) => error!("Failed to compile the query: {}", msg),
                Err(err) => error!("Double error: {}", err),
            },
            Err(_) if DataPacket::try_from(header.tag).is_ok() => {
                // Not expected during planning; begin draining payload incrementally to realign the stream.
                drain_remaining = header.length as usize;
                continue;
            }
            Ok(ControlPacket::Metadata) => {
                pgrx::debug1!("handshake: got Metadata");
                if let Err(err) = handle_metadata(&mut shared.send, &mut shared.recv) {
                    let _ = request_failure(&mut shared.recv);
                    let _ = shared.signal_server();
                    error!("Failed to process metadata: {}", err);
                }
                if let Err(err) = shared.signal_server() {
                    error!("Failed to signal server: {}", err);
                }
            }
            Ok(ControlPacket::Bind) => {
                pgrx::debug1!("handshake: got Bind");
                // For now, send empty params (queries without params)
                if let Err(err) = prepare_params(&mut shared.recv, || {
                    (
                        0usize,
                        std::iter::empty::<AnyResult<datafusion::scalar::ScalarValue>>(),
                    )
                }) {
                    let _ = request_failure(&mut shared.recv);
                    let _ = shared.signal_server();
                    error!("Failed to prepare params: {}", err);
                }
                if let Err(err) = shared.signal_server() {
                    error!("Failed to signal server: {}", err);
                }
            }
            Ok(ControlPacket::Columns) => {
                pgrx::debug1!("handshake: got Columns, consuming");
                // We'll build a Postgres TargetEntry list for the scan's output
                // and assign it to custom_scan_tlist after consumption.
                let mut tlist: *mut List = std::ptr::null_mut();
                let list_ptr = (&mut tlist as *mut *mut List) as *mut c_void;
                // Collect per-attribute layout to return back to executor
                let mut attr_layout: Vec<PgAttrWire> = Vec::new();
                let repack = |pos: i16,
                              etype: u8,
                              nullable: bool,
                              name: &[u8],
                              ptr: *mut c_void|
                 -> anyhow::Result<()> {
                    let pos = pos + 1;
                    let oid = type_to_oid(&EncodedType::try_from(etype)?.to_arrow())?;
                    unsafe {
                        // ptr carries a *mut *mut List — update it with the new head after append
                        let list_pp = ptr as *mut *mut List;
                        let mut list = *list_pp;
                        let tuple =
                            pg_sys::SearchSysCache1(TYPEOID as i32, pg_sys::ObjectIdGetDatum(oid));
                        if tuple.is_null() {
                            anyhow::bail!(FusionError::UnsupportedType(format_smolstr!(
                                "{:?}", oid
                            )));
                        }
                        let typtup = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_type;
                        let expr = pg_sys::makeVar(
                            pg_sys::INDEX_VAR,
                            pos,
                            oid,
                            (*typtup).typtypmod,
                            (*typtup).typcollation,
                            0,
                        );
                        // Record wire layout for this attribute
                        let align = match (*typtup).typalign as u8 as char {
                            'c' => 1u8,
                            's' => 2u8,
                            'i' => 4u8,
                            'd' => 8u8,
                            _ => 1u8,
                        };
                        attr_layout.push(PgAttrWire {
                            atttypid: oid.to_u32(),
                            typmod: (*typtup).typtypmod,
                            attlen: (*typtup).typlen,
                            attalign: align,
                            attbyval: (*typtup).typbyval,
                            nullable,
                        });
                        let col_name = palloc0(name.len()) as *mut u8;
                        std::ptr::copy_nonoverlapping(name.as_ptr(), col_name, name.len());
                        let entry = pg_sys::makeTargetEntry(
                            expr as *mut pg_sys::Expr,
                            pos,
                            col_name as *mut i8,
                            false,
                        );
                        // Append and update list head
                        list = pg_sys::list_append_unique_ptr(list, entry as *mut c_void);
                        *list_pp = list;
                        pg_sys::ReleaseSysCache(tuple);
                    }
                    Ok(())
                };
                if let Err(e) = consume_columns(&mut shared.send, list_ptr, repack) {
                    error!("Failed to consume columns: {e}");
                }
                // Assign the constructed target list back to the CustomScan plan node
                unsafe {
                    (*cscan).custom_scan_tlist = tlist;
                    // Also set the plan's targetlist to keep executor/RowDescription in sync
                    (*cscan).scan.plan.targetlist = tlist;
                }
                // Send the collected layout back to the executor
                if let Err(err) = prepare_column_layout(&mut shared.recv, &attr_layout) {
                    let _ = request_failure(&mut shared.recv);
                    let _ = shared.signal_server();
                    error!("Failed to send column layout: {}", err);
                }
                if let Err(err) = shared.signal_server() {
                    error!("Failed to signal server: {}", err);
                }
                pgrx::debug1!("handshake: Columns processed and ColumnLayout sent");
                break;
            }

            Ok(ControlPacket::Parse)
            | Ok(ControlPacket::Explain)
            | Ok(ControlPacket::Optimize)
            | Ok(ControlPacket::Translate) => {
                let _ = request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!(
                    "Unexpected packet while creating a custom plan: {:?}",
                    header.tag
                )
            }
            Ok(ControlPacket::BeginScan)
            | Ok(ControlPacket::ExecScan)
            | Ok(ControlPacket::EndScan)
            | Ok(ControlPacket::ExecReady) => {
                // Ignore execution-time control messages during planning
                pgrx::debug1!(
                    "handshake: ignoring execution-time packet tag={}",
                    header.tag
                );
                continue;
            }
            Ok(ControlPacket::ColumnLayout) => continue,
            Err(_) => continue,
        }
    }

    // Allocate a proper CustomScanState object; Postgres expects the pointer
    // to point directly to a CustomScanState (no extra Node header prefix).
    let state_ptr = palloc0(std::mem::size_of::<CustomScanState>()) as *mut CustomScanState;
    let mut state = CustomScanState {
        methods: exec_methods(),
        ..Default::default()
    };
    // Set the NodeTag for this PlanState
    state.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    // Write the initialized state into the allocated memory
    std::ptr::write(state_ptr, state);
    state_ptr as *mut Node
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn begin_df_scan(
    _node: *mut CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: i32,
) {
    EXEC_SCAN_STARTED.with(|f| f.set(false));
    RESULT_RING_EOF.with(|f| f.set(false));
    HEAP_EOF_SENT.with(|f| f.set(false));
    EMPTY_SPINS.with(|f| f.set(0));
    PROBE_EPOCH.with(|f| f.set(0));
    LAST_WORKER_REQUEST_SEQ.with(|f| f.set(0));
    LAST_RESULT_SIGNAL_SEQ.with(|f| f.set(0));
    // Initialize ScanTupleSlot as a MinimalTuple slot with a TupleDesc
    unsafe {
        // Derive TupleDesc from the CustomScan's target list
        let plan_ptr = (*_node).ss.ps.plan;
        if !plan_ptr.is_null() {
            let cscan = plan_ptr as *mut pg_sys::CustomScan;
            let tlist = (*cscan).custom_scan_tlist;
            if !tlist.is_null() {
                let tupdesc = pg_sys::ExecTypeFromTL(tlist);
                if !tupdesc.is_null() {
                    // Use MinimalTuple slot ops to match ExecStoreMinimalTuple
                    pg_sys::ExecInitScanTupleSlot(
                        _estate,
                        &mut (*_node).ss,
                        tupdesc,
                        &pg_sys::TTSOpsMinimalTuple,
                    );
                    // Initialize a result slot matching the same TupleDesc
                    let resslot =
                        pg_sys::MakeSingleTupleTableSlot(tupdesc, &pg_sys::TTSOpsMinimalTuple);
                    (*_node).ss.ps.ps_ResultTupleSlot = resslot;
                }
            }
        }
    }
    // Notify executor to prepare data channels for upcoming scan(s)
    let id = match connection_id() {
        Ok(v) => v,
        Err(e) => error!("Failed to acquire connection id: {}", e),
    };
    let mut shared = match connection_shared(id) {
        Ok(s) => s,
        Err(e) => error!("Failed to map shared connection: {}", e),
    };
    pgrx::debug1!("begin_df_scan: requesting BeginScan (conn={})", id);
    if let Err(err) = request_begin_scan(&mut shared.recv) {
        let _ = request_failure(&mut shared.recv);
        let _ = shared.signal_server();
        error!("Failed to request begin scan: {}", err);
    }
    if let Err(err) = shared.signal_server() {
        error!("Failed to signal server: {}", err);
    }
    // No explicit handshake payload expected; executor registers channels internally.
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn exec_df_scan(
    _node: *mut CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    // Ensure scan tuple slot is compatible with MinimalTuple before storing
    unsafe {
        let estate = (*_node).ss.ps.state;
        if !estate.is_null() {
            let mut need_minimal = false;
            let mut tupdesc: *mut pg_sys::TupleDescData = std::ptr::null_mut();
            let slot = (*_node).ss.ss_ScanTupleSlot;
            if slot.is_null() {
                need_minimal = true;
            } else if (*slot).tts_ops != &pg_sys::TTSOpsMinimalTuple {
                need_minimal = true;
                tupdesc = (*slot).tts_tupleDescriptor;
            } else {
                tupdesc = (*slot).tts_tupleDescriptor;
            }
            if tupdesc.is_null() {
                let plan_ptr = (*_node).ss.ps.plan as *mut pg_sys::CustomScan;
                if !plan_ptr.is_null() {
                    let tlist = (*plan_ptr).custom_scan_tlist;
                    if !tlist.is_null() {
                        tupdesc = pg_sys::ExecTypeFromTL(tlist);
                    }
                }
            }
            if need_minimal && !tupdesc.is_null() {
                pg_sys::ExecInitScanTupleSlot(
                    estate,
                    &mut (*_node).ss,
                    tupdesc,
                    &pg_sys::TTSOpsMinimalTuple,
                );
            }
        }
    }
    // Notify executor that backend is switching to data flow (begin producing/consuming data)
    let id = match connection_id() {
        Ok(v) => v,
        Err(e) => error!("Failed to acquire connection id: {}", e),
    };
    let mut shared = match connection_shared(id) {
        Ok(s) => s,
        Err(e) => error!("Failed to map shared connection: {}", e),
    };
    EXEC_SCAN_STARTED.with(|started| {
        if !started.get() {
            reset_probe_metrics_for_current_conn();
            pgrx::debug1!("exec_df_scan: requesting ExecScan (conn={})", id);
            if let Err(err) = request_exec_scan(&mut shared.recv) {
                let _ = request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Failed to request exec scan: {}", err);
            }
            if let Err(err) = shared.signal_server() {
                error!("Failed to signal server: {}", err);
            }
            started.set(true);
        }
    });
    // Wait until executor confirms it is ready to consume data (only once per scan)
    let mut need_wait = true;
    EXEC_READY_SEEN.with(|seen| {
        if seen.get() {
            need_wait = false;
        }
    });
    if need_wait {
        let mut drain_remaining: usize = 0;
        loop {
            if drain_remaining > 0 {
                let avail = shared.send.len();
                if avail == 0 {
                    wait_latch(Some(Duration::from_millis(1)));
                } else {
                    use std::io::Read;
                    let to_drain = drain_remaining.min(avail);
                    let mut left = to_drain;
                    while left > 0 {
                        let mut buf = [0u8; 256];
                        let take = left.min(buf.len());
                        match Read::read(&mut shared.send, &mut buf[..take]) {
                            Ok(n) if n > 0 => {
                                left -= n;
                                drain_remaining -= n;
                            }
                            _ => break,
                        }
                    }
                }
                // Continue draining until complete
                continue;
            }
            let cur = shared.send.len();
            if cur < 6 {
                pgrx::debug1!("exec_df_scan: waiting ExecReady (len={})", cur);
                wait_latch(Some(Duration::from_millis(1)));
                if shared.send.len() < 6 {
                    continue;
                }
            }
            let header = match consume_header(&mut shared.send) {
                Ok(h) => h,
                Err(_err) => {
                    pgrx::debug1!(
                        "exec_df_scan: partial header while waiting ExecReady (len={})",
                        shared.send.len()
                    );
                    // Transient decode mismatch (partial frame); retry on next wake
                    continue;
                }
            };
            if header.direction != Direction::ToClient {
                continue;
            }
            match ControlPacket::try_from(header.tag) {
                Ok(ControlPacket::ExecReady) => {
                    pgrx::debug1!("exec_df_scan: ExecReady received (conn={})", id);
                    EXEC_READY_SEEN.with(|seen| seen.set(true));
                    break;
                }
                Ok(ControlPacket::Failure) => match read_error(&mut shared.send) {
                    Ok(msg) => error!("Executor failed to start: {}", msg),
                    Err(err) => error!("Double error: {}", err),
                },
                Err(_) if DataPacket::try_from(header.tag).is_ok() => {
                    // Begin draining unexpected data payload incrementally
                    drain_remaining = header.length as usize;
                    continue;
                }
                _ => continue,
            }
        }
    }
    // Loop until we either produce a row or observe EOF.
    let tupslot = unsafe { (*_node).ss.ss_ScanTupleSlot };
    if tupslot.is_null() {
        return std::ptr::null_mut();
    }
    loop {
        // Make progress on any pending heap block request.
        process_pending_heap_request(&mut shared);
        // Try to fetch a row from result ring
        if let Some(slot) = unsafe { try_store_wire_tuple_from_result(_node) } {
            EMPTY_SPINS.with(|f| f.set(0));
            return slot;
        }
        // Brief spin to catch just-written frames without sleeping
        let mut spun = false;
        for _ in 0..128 {
            let base = result_ring_base_for(match connection_id() {
                Ok(v) => v as usize,
                Err(_) => 0,
            });
            let layout = result_ring_layout();
            let ring = LockFreeBuffer::from_layout(base, layout);
            if ring.len() > 0 {
                spun = true;
                break;
            }
            std::hint::spin_loop();
        }
        if spun {
            continue;
        }
        // If EOF sentinel was seen, terminate scan
        if RESULT_RING_EOF.with(|f| f.get()) {
            log_probe_summary("result_eof");
            RESULT_RING_EOF.with(|f| f.set(false));
            HEAP_EOF_SENT.with(|f| f.set(false));
            EMPTY_SPINS.with(|f| f.set(0));
            return std::ptr::null_mut();
        }
        // Conservative bailout after sending heap EOF
        let mut bail = false;
        HEAP_EOF_SENT.with(|eof_sent| {
            if eof_sent.get() {
                EMPTY_SPINS.with(|spins| {
                    let cur = spins.get().saturating_add(1);
                    spins.set(cur);
                    if cur > 5000 {
                        // ~5s with 1ms waits
                        bail = true;
                    }
                });
            }
        });
        if bail {
            pgrx::warning!("result_ring: bailout after EOF (no frames observed)");
            log_probe_summary("result_bailout");
            RESULT_RING_EOF.with(|f| f.set(false));
            HEAP_EOF_SENT.with(|f| f.set(false));
            EMPTY_SPINS.with(|f| f.set(0));
            return std::ptr::null_mut();
        }
        // Otherwise, wait to be signaled and retry (short 1ms guard)
        wait_latch(Some(Duration::from_millis(1)));
    }
}

/// Try to process at most one pending heap page request from the executor.
/// Non-blocking and best-effort: silently returns if no applicable message is queued.
fn process_pending_heap_request(shared: &mut ConnectionShared) {
    if shared.send.len() == 0 {
        return;
    }
    if let Ok(header) = consume_header(&mut shared.send) {
        if header.direction != Direction::ToClient {
            return;
        }
        if let Ok(dp) = DataPacket::try_from(header.tag) {
            match dp {
                DataPacket::Heap => {
                    if let Ok((scan_id, table_oid, slot_id)) =
                        read_heap_block_request(&mut shared.send)
                    {
                        if let Some(metrics) = probe_metrics_current() {
                            let now_ns = executor::telemetry::monotonic_now_ns();
                            LAST_WORKER_REQUEST_SEQ.with(|last_seen| {
                                let mut seq = last_seen.get();
                                metrics.observe_worker_heap_request(&mut seq, now_ns);
                                last_seen.set(seq);
                            });
                        }
                        let t0 = Instant::now();
                        let serve_t0_ns = executor::telemetry::probe_enabled()
                            .then(executor::telemetry::monotonic_now_ns);
                        // If EOF was already sent for this scan_id, ignore further heap requests
                        if let Ok(done) = COMPLETED.lock() {
                            if done.contains(&scan_id) {
                                pgrx::debug1!(
                                    "process_pending_heap_request: ignoring request for completed scan_id={}",
                                    scan_id
                                );
                                return;
                            }
                        }
                        pgrx::debug1!(
                            "process_pending_heap_request: heap request received scan_id={} table_oid={} slot_id={}",
                            scan_id, table_oid, slot_id
                        );
                        // Determine next block number for this scan
                        static PROGRESS: LazyLock<Mutex<HashMap<u64, u32>>> =
                            LazyLock::new(|| Mutex::new(HashMap::new()));
                        static COMPLETED: LazyLock<Mutex<HashSet<u64>>> =
                            LazyLock::new(|| Mutex::new(HashSet::new()));
                        let blkno = {
                            let mut map = PROGRESS.lock().unwrap();
                            let e = map.entry(scan_id).or_insert(0);
                            let cur = *e;
                            *e = e.saturating_add(1);
                            cur
                        };
                        pgrx::debug1!(
                            "process_pending_heap_request: computed blkno={} for scan_id={}",
                            blkno,
                            scan_id
                        );
                        // Open relation with AccessShareLock and validate block number
                        let rel_oid = Oid::from(table_oid);
                        let rel = unsafe {
                            pg_sys::relation_open(
                                rel_oid,
                                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                            )
                        };
                        if rel.is_null() {
                            // Relation not found; cannot serve this request
                            pgrx::warning!(
                                "process_pending_heap_request: relation {} not found",
                                table_oid
                            );
                            return;
                        }
                        // Use RelationGetNumberOfBlocksInFork to get heap size in blocks
                        let nblocks = unsafe {
                            pg_sys::RelationGetNumberOfBlocksInFork(
                                rel,
                                pg_sys::ForkNumber::MAIN_FORKNUM,
                            )
                        } as u32;
                        if blkno >= nblocks {
                            // End of relation reached: send EOF for this scan
                            pgrx::debug1!(
                                "process_pending_heap_request: EOF scan_id={} slot_id={} (blkno {} >= nblocks {})",
                                scan_id, slot_id, blkno, nblocks
                            );
                            let _ = prepare_scan_eof(&mut shared.recv, scan_id);
                            let _ = shared.signal_server();
                            // Reset scan progress for this scan_id to allow future scans to start from 0
                            {
                                let mut map = PROGRESS.lock().unwrap();
                                map.remove(&scan_id);
                            }
                            {
                                let mut done = COMPLETED.lock().unwrap();
                                done.insert(scan_id);
                            }
                            HEAP_EOF_SENT.with(|f| f.set(true));
                            unsafe {
                                pg_sys::relation_close(
                                    rel,
                                    pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                                )
                            };
                            return;
                        }
                        // Read the requested block
                        let buf = unsafe {
                            pg_sys::ReadBufferExtended(
                                rel,
                                pg_sys::ForkNumber::MAIN_FORKNUM,
                                blkno as pg_sys::BlockNumber,
                                pg_sys::ReadBufferMode::RBM_NORMAL,
                                std::ptr::null_mut(),
                            )
                        };
                        if buf <= 0 {
                            unsafe { pg_sys::RelationClose(rel) };
                            pgrx::warning!(
                                "process_pending_heap_request: ReadBufferExtended failed"
                            );
                            return;
                        }
                        // Lock buffer for shared access while copying
                        unsafe {
                            pg_sys::LockBuffer(buf, pg_sys::BUFFER_LOCK_SHARE as i32);
                        }
                        let blksz = pg_sys::BLCKSZ as usize;
                        let page_ptr = unsafe { pg_sys::BufferGetPage(buf) } as *const u8;
                        let page = unsafe { std::slice::from_raw_parts(page_ptr, blksz) };
                        // Compute number of offsets (max line pointer number)
                        let hdr = unsafe { &*(page_ptr as *const pg_sys::PageHeaderData) };
                        let lower = hdr.pd_lower as usize;
                        let num_offsets = if lower < std::mem::size_of::<pg_sys::PageHeaderData>() {
                            0u16
                        } else {
                            let cnt = (lower - std::mem::size_of::<pg_sys::PageHeaderData>())
                                / std::mem::size_of::<pg_sys::ItemIdData>();
                            u16::try_from(cnt).unwrap_or(u16::MAX)
                        };
                        // Prepare visibility bitmap using MVCC visibility against the active snapshot
                        let bytes = (num_offsets as usize).div_ceil(8);
                        // Obtain SHM visibility buffer and zero it (avoid heap allocation)
                        let conn = connection_id().unwrap_or_default() as usize;
                        let base = slot_blocks_base_for(conn);
                        let layout = slot_blocks_layout();
                        let vis_ptr =
                            unsafe { slot_block_vis_ptr(base, layout, slot_id as usize, 0) };
                        let cap = layout.vis_bytes_per_block;
                        let vis_len = std::cmp::min(bytes, cap);
                        unsafe { std::ptr::write_bytes(vis_ptr, 0u8, vis_len) };
                        // Snapshot to use for visibility checks; if none, treat all as visible
                        let snapshot = unsafe { pg_sys::GetActiveSnapshot() };
                        // Iterate 1-based offsets up to num_offsets
                        for off in 1..=num_offsets as usize {
                            // Fetch line pointer; skip non-normal entries
                            let itemid = unsafe {
                                pg_sys::PageGetItemId(
                                    page_ptr as pg_sys::Page,
                                    off as pg_sys::OffsetNumber,
                                )
                            };
                            if itemid.is_null() {
                                continue;
                            }
                            // Check ItemId flags (LP_NORMAL) by unpacking the 32-bit representation
                            let raw: u32 = unsafe { std::ptr::read(itemid as *const u32) };
                            let flags_u8 = ((raw >> 15) & 0x03) as u8;
                            if (flags_u8 as u32) != pg_sys::LP_NORMAL {
                                continue;
                            }
                            // Build TID and perform HOT-chain visibility search within the buffer
                            let mut htup: pg_sys::HeapTupleData = unsafe { std::mem::zeroed() };
                            htup.t_tableOid = pg_sys::Oid::from(table_oid);
                            unsafe {
                                pg_sys::ItemPointerSetBlockNumber(
                                    &mut htup.t_self,
                                    blkno as pg_sys::BlockNumber,
                                );
                                pg_sys::ItemPointerSetOffsetNumber(
                                    &mut htup.t_self,
                                    off as pg_sys::OffsetNumber,
                                );
                            }
                            let is_visible = if snapshot.is_null() {
                                true
                            } else {
                                let mut all_dead: bool = false;
                                unsafe {
                                    pg_sys::heap_hot_search_buffer(
                                        &mut htup.t_self,                        // in/out TID
                                        rel,                                     // relation
                                        buf,                                     // buffer
                                        snapshot,                                // snapshot
                                        &mut htup as *mut pg_sys::HeapTupleData, // out tuple
                                        &mut all_dead as *mut bool,              // all-dead flag
                                        true,                                    // first_call
                                    )
                                }
                            };
                            if is_visible {
                                // Mark the actual visible member of a HOT chain
                                let vis_off: usize =
                                    unsafe { pg_sys::ItemPointerGetOffsetNumber(&htup.t_self) }
                                        as usize;
                                let idx0 = vis_off.saturating_sub(1);
                                let byte = idx0 / 8;
                                if byte < vis_len {
                                    let bit = 1u8 << ((idx0 % 8) as u8);
                                    unsafe {
                                        let p = vis_ptr.add(byte);
                                        *p |= bit;
                                    }
                                }
                            }
                        }
                        // Publish page bytes and the computed visibility bitmap already stored in SHM, then notify executor
                        let _ = shm_write_heap_block(slot_id, page);
                        // Send lightweight metadata indicating the SHM bitmap length
                        let _ = prepare_heap_block_meta_shm(
                            &mut shared.recv,
                            scan_id,
                            slot_id,
                            table_oid,
                            blkno,
                            num_offsets,
                            vis_len as u16,
                        );
                        if let (true, Some(metrics)) = (
                            executor::telemetry::probe_enabled(),
                            probe_metrics_current(),
                        ) {
                            metrics.publish_backend_page(executor::telemetry::monotonic_now_ns());
                        }
                        let _ = shared.signal_server();
                        if let (Some(serve_t0_ns), Some(metrics)) =
                            (serve_t0_ns, probe_metrics_current())
                        {
                            metrics.record_backend_heap_serve(
                                executor::telemetry::monotonic_now_ns().saturating_sub(serve_t0_ns),
                            );
                        }
                        let us = t0.elapsed().as_micros() as u64;
                        pgrx::debug1!(
                            "process_pending_heap_request: served blkno={} scan_id={} in {} us",
                            blkno,
                            scan_id,
                            us
                        );
                        // Unlock and release buffer; close relation
                        unsafe {
                            pg_sys::UnlockReleaseBuffer(buf);
                            pg_sys::relation_close(
                                rel,
                                pg_sys::AccessShareLock as pg_sys::LOCKMODE,
                            );
                        }
                    }
                }
                DataPacket::Eof => {
                    // Ignore EOF from executor in backend context
                }
            }
        }
    }
}

thread_local! {
    static RESULT_RING_EOF: Cell<bool> = const { Cell::new(false) };
    static EXEC_SCAN_STARTED: Cell<bool> = const { Cell::new(false) };
    static EXEC_READY_SEEN: Cell<bool> = const { Cell::new(false) };
    static HEAP_EOF_SENT: Cell<bool> = const { Cell::new(false) };
    static EMPTY_SPINS: Cell<u32> = const { Cell::new(0) };
    static PROBE_EPOCH: Cell<u64> = const { Cell::new(0) };
    static LAST_WORKER_REQUEST_SEQ: Cell<u64> = const { Cell::new(0) };
    static LAST_RESULT_SIGNAL_SEQ: Cell<u64> = const { Cell::new(0) };
    static HANDSHAKE_T0: std::cell::RefCell<Option<Instant>> = const { std::cell::RefCell::new(None) };
    static EXECREADY_T0: std::cell::RefCell<Option<Instant>> = const { std::cell::RefCell::new(None) };
}

/// Read one row from the result ring and populate `tupslot`.
/// Returns `tupslot` on success, or null on EOF/empty ring/error.
unsafe fn try_store_wire_tuple_from_result(
    node: *mut CustomScanState,
) -> Option<*mut pg_sys::TupleTableSlot> {
    let conn_id = match connection_id() {
        Ok(v) => v as usize,
        Err(_) => 0,
    };
    let probe_metrics = probe_metrics_for_conn(conn_id);
    let base = result_ring_base_for(conn_id);
    let layout = result_ring_layout();
    let mut ring = LockFreeBuffer::from_layout(base, layout);
    let avail = ring.len();
    if avail == 0 {
        pgrx::debug1!("result_ring: empty (conn={})", conn_id);
        return None;
    }
    if let Some(metrics) = probe_metrics {
        let now_ns = executor::telemetry::monotonic_now_ns();
        LAST_RESULT_SIGNAL_SEQ.with(|last_seen| {
            let mut seq = last_seen.get();
            metrics.observe_worker_result_signal(&mut seq, now_ns);
            last_seen.set(seq);
        });
    }
    let read_t0_ns =
        executor::telemetry::probe_enabled().then(executor::telemetry::monotonic_now_ns);
    pgrx::debug1!("result_ring: bytes available={} (conn={})", avail, conn_id);
    use std::io::Read;
    // Read fixed 4-byte little-endian row_len
    let row_len = match protocol::result::read_frame_len(&mut ring) {
        Ok(v) => v as usize,
        Err(e) => {
            pgrx::warning!(
                "result_ring: failed to read frame len: {} (conn={})",
                e,
                conn_id
            );
            if let (Some(read_t0_ns), Some(metrics)) = (read_t0_ns, probe_metrics) {
                metrics.record_backend_result_read(
                    executor::telemetry::monotonic_now_ns().saturating_sub(read_t0_ns),
                );
            }
            return None;
        }
    };
    pgrx::debug1!("result_ring: frame_len={} (conn={})", row_len, conn_id);
    if row_len == 0 {
        // EOF sentinel
        pgrx::debug1!("result_ring: EOF sentinel (conn={})", conn_id);
        RESULT_RING_EOF.with(|f| f.set(true));
        if let (Some(read_t0_ns), Some(metrics)) = (read_t0_ns, probe_metrics) {
            metrics.record_backend_result_read(
                executor::telemetry::monotonic_now_ns().saturating_sub(read_t0_ns),
            );
        }
        return None;
    }
    // Read payload into buffer
    let mut buf = vec![0u8; row_len];
    if Read::read_exact(&mut ring, &mut buf).is_err() {
        pgrx::warning!(
            "result_ring: failed to read frame payload (len={})",
            row_len
        );
        if let (Some(read_t0_ns), Some(metrics)) = (read_t0_ns, probe_metrics) {
            metrics.record_backend_result_read(
                executor::telemetry::monotonic_now_ns().saturating_sub(read_t0_ns),
            );
        }
        return None;
    }
    let (hdr, bitmap, data) = match decode_wire_tuple(&buf) {
        Ok(v) => v,
        Err(e) => {
            pgrx::warning!("result_ring: decode error: {}", e);
            if let (Some(read_t0_ns), Some(metrics)) = (read_t0_ns, probe_metrics) {
                metrics.record_backend_result_read(
                    executor::telemetry::monotonic_now_ns().saturating_sub(read_t0_ns),
                );
            }
            return None;
        }
    };
    // Use the scan slot; core ExecScan will handle projection into result slot
    let scanslot = (*node).ss.ss_ScanTupleSlot;
    let tdesc = (*scanslot).tts_tupleDescriptor;
    let natts_td = (*tdesc).natts as usize;
    let natts_wire = hdr.nattrs as usize;
    let limit = std::cmp::min(natts_td, natts_wire);
    let attrs = (*tdesc).attrs.as_slice((*tdesc).natts as _);
    let mut values: Vec<pg_sys::Datum> = vec![pg_sys::Datum::from(0usize); natts_td];
    let mut nulls: Vec<bool> = vec![true; natts_td];
    let mut off: usize = 0;
    for i in 0..limit {
        // null bitmap: bit set means NULL
        if (hdr.flags & 0x01) != 0 {
            let byte = i / 8;
            let bit = i % 8;
            if byte < bitmap.len() && (bitmap[byte] & (1u8 << bit)) != 0 {
                nulls[i] = true; // remains NULL
                continue;
            }
        }
        let att = &attrs[i];
        let align = match att.attalign as u8 as char {
            'c' => 1usize,
            's' => 2,
            'i' => 4,
            'd' => 8,
            _ => 1,
        };
        if align > 1 {
            let rem = off % align;
            if rem != 0 {
                off += align - rem;
            }
        }
        if att.attlen == -1 {
            // varlena: wire encodes u32 length prefix then bytes
            if off + 4 > data.len() {
                nulls[i] = true;
                continue;
            }
            let len = u32::from_le_bytes([data[off], data[off + 1], data[off + 2], data[off + 3]])
                as usize;
            off += 4;
            if off + len > data.len() {
                nulls[i] = true;
                continue;
            }
            let src = &data[off..off + len];
            // For now, build TEXT varlena; for generic varlena types, need per-oid handling
            let cptr = src.as_ptr() as *const i8;
            let txt = pg_sys::cstring_to_text_with_len(cptr, len as i32);
            values[i] = pg_sys::PointerGetDatum(txt as *mut std::ffi::c_void);
            nulls[i] = false;
            off += len;
        } else {
            let need = att.attlen as usize;
            if off + need > data.len() {
                nulls[i] = true;
                continue;
            }
            // Read little-endian bytes into Datum
            let mut tmp = [0u8; 8];
            tmp[..need].copy_from_slice(&data[off..off + need]);
            let v = u64::from_le_bytes(tmp);
            values[i] = pg_sys::Datum::from(v as usize);
            nulls[i] = false;
            off += need;
        }
    }
    // Form MinimalTuple and store in slot
    let mtup = pg_sys::heap_form_minimal_tuple(tdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    pg_sys::ExecStoreMinimalTuple(mtup, scanslot, false);
    if let (Some(read_t0_ns), Some(metrics)) = (read_t0_ns, probe_metrics) {
        metrics.record_backend_result_read(
            executor::telemetry::monotonic_now_ns().saturating_sub(read_t0_ns),
        );
    }
    Some(scanslot)
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn end_df_scan(_node: *mut CustomScanState) {
    // Only notify executor to end a scan if execution actually started.
    // EXPLAIN (without ANALYZE) should not drive the scan FSM; avoid sending EndScan.
    let mut should_end = false;
    EXEC_SCAN_STARTED.with(|started| {
        if started.get() {
            should_end = true;
        }
    });
    EXEC_READY_SEEN.with(|ready| {
        if ready.get() {
            should_end = true;
        }
    });
    if should_end {
        let id = match connection_id() {
            Ok(v) => v,
            Err(e) => error!("Failed to acquire connection id: {}", e),
        };
        let mut shared = match connection_shared(id) {
            Ok(s) => s,
            Err(e) => error!("Failed to map shared connection: {}", e),
        };
        if let Err(err) = request_end_scan(&mut shared.recv) {
            let _ = request_failure(&mut shared.recv);
            let _ = shared.signal_server();
            error!("Failed to request end scan: {}", err);
        }
        if let Err(err) = shared.signal_server() {
            error!("Failed to signal server: {}", err);
        }
    }
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn explain_df_scan(
    _node: *mut CustomScanState,
    _ancestors: *mut List,
    es: *mut pg_sys::ExplainState,
) {
    // Acquire connection and shared buffers
    let id = match connection_id() {
        Ok(v) => v,
        Err(e) => error!("Failed to acquire connection id: {}", e),
    };
    let mut shared = match connection_shared(id) {
        Ok(s) => s,
        Err(e) => error!("Failed to map shared connection: {}", e),
    };
    shared.set_client_pid(unsafe { libc::getpid() } as i32);

    // Fast-fail when worker is not running (PID not published yet)
    if shared.server_pid() <= 0 {
        error!(
            "DataFusion worker is not running (server PID is 0). Make sure shared_preload_libraries = 'pg_fusion' and restart the cluster."
        );
    }

    // Request explain and notify the server
    if let Err(err) = request_explain(&mut shared.recv) {
        let _ = request_failure(&mut shared.recv);
        let _ = shared.signal_server();
        error!("Failed to request explain: {}", err);
    }
    if let Err(err) = shared.signal_server() {
        error!("Failed to signal server: {}", err);
    }

    // Wait for the explain response
    loop {
        wait_latch(Some(Duration::from_millis(1)));
        if shared.send.len() < 6 {
            continue;
        }
        let header = match consume_header(&mut shared.send) {
            Ok(h) => h,
            Err(_err) => {
                // Partial frame or transient mismatch; retry next wake
                continue;
            }
        };
        if header.direction != Direction::ToClient {
            continue;
        }
        match ControlPacket::try_from(header.tag) {
            Ok(ControlPacket::None) => continue,
            Ok(ControlPacket::Failure) => match read_error(&mut shared.send) {
                Ok(msg) => error!("Failed to compile the query: {}", msg),
                Err(err) => error!("Double error: {}", err),
            },
            Err(_) if DataPacket::try_from(header.tag).is_ok() => {
                // Not expected for EXPLAIN; ignore.
                continue;
            }
            Ok(ControlPacket::Explain) => {
                let len = read_bin_len(&mut shared.send)
                    .expect("Failed to read length in explain message");
                let mut buf = SmallVec::<[u8; 256]>::new();
                buf.resize(len as usize, 0);
                let read = std::io::Read::read(&mut shared.send, buf.as_mut_slice())
                    .expect("Failed to read explain payload");
                debug_assert_eq!(read, len as usize);
                unsafe {
                    pg_sys::ExplainPropertyText(
                        c"DataFusion Plan".as_ptr() as _,
                        buf.as_ptr() as _,
                        es,
                    );
                }
                break;
            }
            _ => {
                let _ = request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Unexpected tag for explain: {:?}", header.tag)
            }
        }
    }
}

fn wait_latch(timeout: Option<Duration>) {
    let wait_t0 = if executor::telemetry::probe_enabled() && current_connection_id().is_some() {
        Some(executor::telemetry::monotonic_now_ns())
    } else {
        None
    };
    // In PostgreSQL, WaitLatch timeout -1 means wait forever, 0 means do not wait.
    // For None we want to block until signaled, not to return immediately.
    let timeout_ms: c_long = timeout
        .map(|t| t.as_millis().try_into().unwrap())
        .unwrap_or(-1);
    // Request WL_TIMEOUT only when we actually have a non-negative timeout.
    let events = if timeout.is_some() {
        WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH
    } else {
        WL_LATCH_SET | WL_POSTMASTER_DEATH
    };
    let rc = unsafe {
        let rc = pg_sys::WaitLatch(
            MyLatch,
            events as i32,
            timeout_ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(MyLatch);
        rc
    };
    check_for_interrupts!();
    if let (Some(wait_t0), Some(metrics)) = (wait_t0, probe_metrics_current()) {
        metrics.record_backend_wait(
            executor::telemetry::monotonic_now_ns().saturating_sub(wait_t0),
            rc & WL_TIMEOUT as i32 != 0,
        );
    }
    if rc & WL_TIMEOUT as i32 != 0 {
        // Timeout is expected in non-blocking polling; do not raise ERROR.
        pgrx::debug1!("wait_latch: timeout (non-fatal)");
    } else if rc & WL_POSTMASTER_DEATH as i32 != 0 {
        panic!("Postmaster is dead");
    }
}

fn list_nth(list: *mut List, n: i32) -> *mut pg_sys::ListCell {
    assert_ne!(list, std::ptr::null_mut());
    unsafe {
        assert!(n >= 0 && n < (*list).length);
        (*list).elements.offset(n as isize)
    }
}

fn type_to_oid(dtype: &DataType) -> AnyResult<pg_sys::Oid> {
    let oid = match dtype {
        DataType::Boolean => pg_sys::BOOLOID,
        DataType::Utf8 => pg_sys::TEXTOID,
        DataType::Int16 => pg_sys::INT2OID,
        DataType::Int32 => pg_sys::INT4OID,
        DataType::Int64 => pg_sys::INT8OID,
        DataType::Float32 => pg_sys::FLOAT4OID,
        DataType::Float64 => pg_sys::FLOAT8OID,
        DataType::Date32 => pg_sys::DATEOID,
        DataType::Time64(_) => pg_sys::TIMEOID,
        DataType::Timestamp(_, _) => pg_sys::TIMESTAMPOID,
        DataType::Interval(_) => pg_sys::INTERVALOID,
        _ => {
            return Err(FusionError::UnsupportedType(format_smolstr!("{:?}", dtype)).into());
        }
    };
    Ok(oid)
}

/// Copy a heap page into the shared memory buffer for the given `slot_id`.
/// Returns the number of bytes copied (clamped to `layout.block_len`).
fn shm_write_heap_block(slot_id: u16, page: &[u8]) -> AnyResult<u32> {
    let conn = connection_id()? as usize;
    let base = slot_blocks_base_for(conn);
    let layout = slot_blocks_layout();
    // For now use block index 0; double-buffering can rotate indices later.
    let dst = unsafe { slot_block_ptr(base, layout, slot_id as usize, 0) };
    let n = std::cmp::min(layout.block_len, page.len());
    unsafe {
        std::ptr::copy_nonoverlapping(page.as_ptr(), dst, n);
    }
    Ok(u32::try_from(n).unwrap_or(layout.block_len as u32))
}

#[inline]
fn oid_to_encoded_type(oid: pg_sys::Oid) -> Option<EncodedType> {
    match oid {
        // Booleans
        o if o == pg_sys::BOOLOID => Some(EncodedType::Boolean),
        // Text-like
        o if o == pg_sys::TEXTOID
            || o == pg_sys::VARCHAROID
            || o == pg_sys::BPCHAROID
            || o == pg_sys::NAMEOID =>
        {
            Some(EncodedType::Utf8)
        }
        // Integers
        o if o == pg_sys::INT2OID => Some(EncodedType::Int16),
        o if o == pg_sys::INT4OID => Some(EncodedType::Int32),
        o if o == pg_sys::INT8OID => Some(EncodedType::Int64),
        // Floats
        o if o == pg_sys::FLOAT4OID => Some(EncodedType::Float32),
        o if o == pg_sys::FLOAT8OID => Some(EncodedType::Float64),
        // Date/Time
        o if o == pg_sys::DATEOID => Some(EncodedType::Date32),
        o if o == pg_sys::TIMEOID || o == pg_sys::TIMETZOID => Some(EncodedType::Time64),
        o if o == pg_sys::TIMESTAMPOID || o == pg_sys::TIMESTAMPTZOID => {
            Some(EncodedType::Timestamp)
        }
        // Interval
        o if o == pg_sys::INTERVALOID => Some(EncodedType::Interval),
        // Not (yet) supported
        _ => None,
    }
}
