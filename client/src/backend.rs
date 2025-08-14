use crate::worker::treiber_stack;
use anyhow::Result as AnyResult;
use libc::c_long;
use pgrx::pg_sys::{
    error, fetch_search_path_array, get_namespace_oid, get_relname_relid, palloc0,
    CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, EState, ExplainPropertyText,
    ExplainState, InvalidOid, List, ListCell, MyLatch, MyProcNumber, Node, NodeTag, Oid,
    ParamExternData, ParamListInfo, RegisterCustomScanMethods, ResetLatch, TupleTableSlot,
    WaitLatch, PG_WAIT_EXTENSION, WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
};
use pgrx::{check_for_interrupts, pg_guard, warning};
use rmp::decode::{read_array_len, read_bin_len};
use rmp::encode::{write_array_len, write_bool, write_str, write_u32, write_u8};
use smallvec::SmallVec;
use std::cell::OnceCell;
use std::ffi::c_char;
use std::ffi::CStr;
use std::time::Duration;

use crate::ipc::{connection_id, connection_shared};
use pgrx::pg_sys;
use server::buffer::LockFreeBuffer;
use server::data_type::EncodedType;
use server::protocol::bind::prepare_params as srv_prepare_params;
use server::protocol::consume_header as srv_consume_header;
use server::protocol::explain::request_explain as srv_request_explain;
use server::protocol::failure::{
    read_error as srv_read_error, request_failure as srv_request_failure,
};
use server::protocol::metadata::process_metadata_with_response as srv_process_metadata;
use server::protocol::parse::prepare_query as srv_prepare_query;
use server::protocol::Tape;
use server::protocol::{Direction, Packet};

fn handle_metadata(send: &mut LockFreeBuffer, recv: &mut LockFreeBuffer) -> AnyResult<()> {
    let schema_table_lookup = |schema: &[u8], table: &[u8]| -> AnyResult<u32> {
        let ns = CStr::from_bytes_with_nul(schema)
            .map_err(|_| anyhow::anyhow!("invalid schema cstr"))?;
        let tbl =
            CStr::from_bytes_with_nul(table).map_err(|_| anyhow::anyhow!("invalid table cstr"))?;
        let ns_oid = unsafe { get_namespace_oid(ns.as_ptr(), false) };
        let rel_oid = unsafe { get_relname_relid(tbl.as_ptr(), ns_oid) };
        if rel_oid == InvalidOid {
            Err(anyhow::anyhow!("table not found"))
        } else {
            Ok(rel_oid.as_u32())
        }
    };
    let table_lookup = |table: &[u8]| -> AnyResult<u32> {
        let tbl =
            CStr::from_bytes_with_nul(table).map_err(|_| anyhow::anyhow!("invalid table cstr"))?;
        let mut search_path: [Oid; 16] = [InvalidOid; 16];
        let path_len =
            unsafe { fetch_search_path_array(search_path.as_mut_ptr(), search_path.len() as i32) }
                as usize;
        let path = &search_path[..path_len];
        for ns_oid in path {
            let rel_oid = unsafe { get_relname_relid(tbl.as_ptr(), *ns_oid) };
            if rel_oid != InvalidOid {
                return Ok(rel_oid.as_u32());
            }
        }
        Err(anyhow::anyhow!("table not found in search_path"))
    };
    let table_serialize = |rel_oid: u32, need_schema: bool, out: &mut dyn std::io::Write| {
        // Replicate postgres/protocol::serialize_table but writing to dyn Write
        let rel_oid = Oid::from(rel_oid);
        let rel = unsafe { pgrx::PgRelation::with_lock(rel_oid, pg_sys::AccessShareLock as i32) };
        struct Out<'a>(&'a mut dyn std::io::Write);
        impl<'a> std::io::Write for Out<'a> {
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
            write_u32(&mut ow, rel_oid.as_u32())?;
            write_str(&mut ow, rel.namespace())?;
            write_str(&mut ow, rel.name())?;
        } else {
            write_array_len(&mut ow, 2)?;
            write_u32(&mut ow, rel_oid.as_u32())?;
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
            let etype = EncodedType::Utf8 as u8; // TODO: map OIDs properly
            write_u8(&mut ow, etype)?;
            let is_nullable = !attr.attnotnull;
            write_bool(&mut ow, is_nullable)?;
            write_str(&mut ow, attr.name())?;
        }
        AnyResult::Ok(())
    };

    srv_process_metadata(
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
        CustomName: b"DataFusionScan\0".as_ptr() as *const c_char,
        CreateCustomScanState: Some(create_df_scan_state),
    };
    static EXEC_METHODS: CustomExecMethods = CustomExecMethods {
        CustomName: b"DataFusionScan\0".as_ptr() as *const c_char,
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
pub(crate) extern "C" fn init_datafusion_methods() {
    unsafe {
        RegisterCustomScanMethods(scan_methods());
    }
}

pub(crate) fn scan_methods() -> *const CustomScanMethods {
    SCAN_METHODS.with(|m| &*m as *const CustomScanMethods)
}

pub(crate) fn exec_methods() -> *const CustomExecMethods {
    EXEC_METHODS.with(|m| &*m as *const CustomExecMethods)
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn create_df_scan_state(cscan: *mut CustomScan) -> *mut Node {
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

    // Read the query from CustomScan private list and send it to server
    let list = unsafe { (*cscan).custom_private };
    let pattern = unsafe { (*list_nth(list, 0)).ptr_value as *const c_char };
    let query = unsafe { CStr::from_ptr(pattern) }
        .to_str()
        .expect("Expected a zero-terminated string");
    if let Err(err) = srv_prepare_query(&mut shared.recv, query) {
        let _ = srv_request_failure(&mut shared.recv);
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
        let header = match srv_consume_header(&mut shared.send) {
            Ok(h) => h,
            Err(err) => {
                let _ = srv_request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Failed to consume header: {}", err)
            }
        };
        if header.direction != Direction::ToClient {
            continue;
        }
        match header.packet {
            Packet::None => continue,
            Packet::Failure => match srv_read_error(&mut shared.send) {
                Ok(msg) => error!("Failed to compile the query: {}", msg),
                Err(err) => error!("Double error: {}", err),
            },
            Packet::Metadata => {
                if let Err(err) = handle_metadata(&mut shared.send, &mut shared.recv) {
                    let _ = srv_request_failure(&mut shared.recv);
                    let _ = shared.signal_server();
                    error!("Failed to process metadata: {}", err);
                }
                if let Err(err) = shared.signal_server() {
                    error!("Failed to signal server: {}", err);
                }
            }
            Packet::Bind => {
                // For now, send empty params (queries without params)
                if let Err(err) = srv_prepare_params(&mut shared.recv, || {
                    (
                        0usize,
                        std::iter::empty::<AnyResult<datafusion::scalar::ScalarValue>>(),
                    )
                }) {
                    let _ = srv_request_failure(&mut shared.recv);
                    let _ = shared.signal_server();
                    error!("Failed to prepare params: {}", err);
                }
                if let Err(err) = shared.signal_server() {
                    error!("Failed to signal server: {}", err);
                }
            }
            Packet::Columns => {
                // Columns are ready; planning phase done
                break;
            }
            Packet::Parse | Packet::Explain => {
                let _ = srv_request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!(
                    "Unexpected packet while creating a custom plan: {:?}",
                    header.packet
                )
            }
        }
    }

    let css = CustomScanState {
        methods: exec_methods(),
        ..Default::default()
    };
    let mut node = PgNode::empty(std::mem::size_of::<CustomScanState>());
    node.set_tag(NodeTag::T_CustomScanState);
    node.set_data(unsafe {
        std::slice::from_raw_parts(
            &css as *const _ as *const u8,
            std::mem::size_of::<CustomScanState>(),
        )
    });
    node.mut_node()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn begin_df_scan(node: *mut CustomScanState, estate: *mut EState, eflags: i32) {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn exec_df_scan(node: *mut CustomScanState) -> *mut TupleTableSlot {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn end_df_scan(node: *mut CustomScanState) {
    todo!()
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn explain_df_scan(
    _node: *mut CustomScanState,
    _ancestors: *mut List,
    es: *mut ExplainState,
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

    // Request explain and notify the server
    if let Err(err) = srv_request_explain(&mut shared.recv) {
        let _ = srv_request_failure(&mut shared.recv);
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
        let header = match srv_consume_header(&mut shared.send) {
            Ok(h) => h,
            Err(err) => {
                let _ = srv_request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Failed to consume header: {}", err)
            }
        };
        if header.direction != Direction::ToClient {
            continue;
        }
        match header.packet {
            Packet::None => continue,
            Packet::Failure => match srv_read_error(&mut shared.send) {
                Ok(msg) => error!("Failed to compile the query: {}", msg),
                Err(err) => error!("Double error: {}", err),
            },
            Packet::Explain => {
                let len = read_bin_len(&mut shared.send)
                    .expect("Failed to read length in explain message");
                let mut buf = SmallVec::<[u8; 256]>::new();
                buf.resize(len as usize, 0);
                let read = std::io::Read::read(&mut shared.send, buf.as_mut_slice())
                    .expect("Failed to read explain payload");
                debug_assert_eq!(read, len as usize);
                unsafe {
                    ExplainPropertyText(b"DataFusion Plan\0".as_ptr() as _, buf.as_ptr() as _, es);
                }
                break;
            }
            _ => {
                let _ = srv_request_failure(&mut shared.recv);
                let _ = shared.signal_server();
                error!("Unexpected packet for explain: {:?}", header.packet)
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
        let rc = WaitLatch(MyLatch, events as i32, timeout_ms, PG_WAIT_EXTENSION);
        ResetLatch(MyLatch);
        rc
    };
    check_for_interrupts!();
    if rc & WL_TIMEOUT as i32 != 0 {
        error!("Waiting latch timeout exceeded");
    } else if rc & WL_POSTMASTER_DEATH as i32 != 0 {
        panic!("Postmaster is dead");
    }
}

pub(crate) struct PgNode {
    size: usize,
    node: *mut Node,
}

impl PgNode {
    pub(crate) fn empty(data_size: usize) -> Self {
        let size = std::mem::size_of::<Node>() + data_size;
        let buf_ptr = unsafe { palloc0(size) };
        PgNode {
            size,
            node: buf_ptr as *mut Node,
        }
    }

    pub(crate) fn set_tag(&mut self, tag: NodeTag) {
        unsafe {
            (*self.node).type_ = tag;
        }
    }

    pub(crate) fn set_data(&mut self, data: &[u8]) {
        assert!(data.len() <= self.size - std::mem::size_of::<Node>());
        unsafe {
            let buf = std::slice::from_raw_parts_mut(self.node as *mut u8, self.size);
            buf[std::mem::size_of::<Node>()..].copy_from_slice(data);
        }
    }

    pub(crate) fn data(&self) -> &[u8] {
        unsafe {
            let buf = std::slice::from_raw_parts(self.node as *const u8, self.size);
            &buf[std::mem::size_of::<Node>()..]
        }
    }

    pub(crate) fn mut_node(&self) -> *mut Node {
        self.node
    }
}

fn list_nth(list: *mut List, n: i32) -> *mut ListCell {
    assert_ne!(list, std::ptr::null_mut());
    unsafe {
        assert!(n >= 0 && n < (*list).length);
        (*list).elements.offset(n as isize)
    }
}
