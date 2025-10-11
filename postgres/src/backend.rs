use crate::ipc::{connection_id, connection_shared};
use anyhow::Result as AnyResult;
use common::FusionError;
use datafusion::arrow::datatypes::DataType;
use executor::buffer::LockFreeBuffer;
use libc::c_long;
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
use protocol::explain::request_explain;
use protocol::exec::{request_begin_scan, request_end_scan, request_exec_scan};
use protocol::failure::{read_error, request_failure};
use protocol::metadata::process_metadata_with_response;
use protocol::parse::prepare_query;
use protocol::Tape;
use protocol::{ControlPacket, DataPacket, Direction};
use rmp::decode::read_bin_len;
use rmp::encode::{write_array_len, write_bool, write_str, write_u32, write_u8};
use smallvec::SmallVec;
use smol_str::format_smolstr;
use std::ffi::c_char;
use std::ffi::CStr;
use std::os::raw::c_void;
use std::time::Duration;

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
    loop {
        // Wait to be signaled by server
        wait_latch(None);
        if shared.send.len() == 0 {
            continue;
        }
        let header = match consume_header(&mut shared.send) {
            Ok(h) => h,
            Err(err) => {
                let _ = request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Failed to consume header: {}", err)
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
                // Not expected during planning; ignore.
                continue;
            }
            Ok(ControlPacket::Metadata) => {
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
                let list_ptr = (*cscan).custom_scan_tlist as *mut c_void;
                let repack =
                    |pos: i16, etype: u8, name: &[u8], ptr: *mut c_void| -> anyhow::Result<()> {
                        let pos = pos + 1;
                        let oid = type_to_oid(&EncodedType::try_from(etype)?.to_arrow())?;
                        unsafe {
                            let list = ptr as *mut List;
                            let tuple = pg_sys::SearchSysCache1(
                                TYPEOID as i32,
                                pg_sys::ObjectIdGetDatum(oid),
                            );
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
                            let col_name = palloc0(name.len()) as *mut u8;
                            std::ptr::copy_nonoverlapping(name.as_ptr(), col_name, name.len());
                            let entry = pg_sys::makeTargetEntry(
                                expr as *mut pg_sys::Expr,
                                pos,
                                col_name as *mut i8,
                                false,
                            );
                            pg_sys::ReleaseSysCache(tuple);
                            pg_sys::list_append_unique_ptr(list, entry as *mut c_void);
                        }
                        Ok(())
                    };
                if let Err(e) = consume_columns(&mut shared.send, list_ptr, repack) {
                    error!("Failed to consume columns: {e}");
                }
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
            | Ok(ControlPacket::EndScan) => {
                // Ignore execution-time control messages during planning
                continue;
            }
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
    // Notify executor to prepare data channel (Data socket) for upcoming scan
    let id = match connection_id() {
        Ok(v) => v,
        Err(e) => error!("Failed to acquire connection id: {}", e),
    };
    let mut shared = match connection_shared(id) {
        Ok(s) => s,
        Err(e) => error!("Failed to map shared connection: {}", e),
    };
    if let Err(err) = request_begin_scan(&mut shared.recv) {
        let _ = request_failure(&mut shared.recv);
        let _ = shared.signal_server();
        error!("Failed to request begin scan: {}", err);
    }
    if let Err(err) = shared.signal_server() {
        error!("Failed to signal server: {}", err);
    }
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn exec_df_scan(
    _node: *mut CustomScanState,
) -> *mut pg_sys::TupleTableSlot {
    // Notify executor that backend is switching to data flow (begin producing/consuming data)
    let id = match connection_id() {
        Ok(v) => v,
        Err(e) => error!("Failed to acquire connection id: {}", e),
    };
    let mut shared = match connection_shared(id) {
        Ok(s) => s,
        Err(e) => error!("Failed to map shared connection: {}", e),
    };
    if let Err(err) = request_exec_scan(&mut shared.recv) {
        let _ = request_failure(&mut shared.recv);
        let _ = shared.signal_server();
        error!("Failed to request exec scan: {}", err);
    }
    if let Err(err) = shared.signal_server() {
        error!("Failed to signal server: {}", err);
    }
    // Execution not implemented yet; return NULL slot to indicate no tuples
    std::ptr::null_mut()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn end_df_scan(_node: *mut CustomScanState) {
    // Notify executor to close data/control flow associated with the scan
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
        wait_latch(None);
        if shared.send.len() == 0 {
            continue;
        }
        let header = match consume_header(&mut shared.send) {
            Ok(h) => h,
            Err(err) => {
                let _ = request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Failed to consume header: {}", err)
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
    if rc & WL_TIMEOUT as i32 != 0 {
        error!("Waiting latch timeout exceeded");
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
