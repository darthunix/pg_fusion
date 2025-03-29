use anyhow::Result as AnyResult;
use libc::c_long;
use pgrx::pg_sys::{
    self, error, fetch_search_path_array, get_namespace_oid, get_relname_relid, palloc0,
    CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, EState, ExplainState,
    InvalidOid, List, ListCell, MyLatch, Node, NodeTag, Oid, ParamListInfo,
    RegisterCustomScanMethods, ResetLatch, TupleTableSlot, WaitLatch, PG_WAIT_EXTENSION,
    WL_LATCH_SET, WL_POSTMASTER_DEATH, WL_TIMEOUT,
};
use pgrx::{check_for_interrupts, pg_guard};
use rmp::decode::{read_array_len, read_bin_len};
use smallvec::SmallVec;
use std::ffi::c_char;
use std::time::Duration;

use crate::error::FusionError;
use crate::ipc::{my_slot, Bus, SlotStream};
use crate::protocol::{consume_header, read_error, send_params, send_query, Direction, Packet};

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

#[repr(C)]
struct ScanState {
    css: CustomScanState,
}

#[pg_guard]
#[no_mangle]
unsafe extern "C" fn create_df_scan_state(cscan: *mut CustomScan) -> *mut Node {
    let wait_stream = || -> SlotStream {
        let stream;
        loop {
            let Some(slot) = Bus::new().slot_locked(my_slot()) else {
                wait_latch(None);
                continue;
            };
            stream = Some(SlotStream::from(slot));
            break;
        }
        stream.expect("Failed to acquire a slot stream")
    };
    let list = (*cscan).custom_private;
    let pattern = (*list_nth(list, 0)).ptr_value as *mut c_char;
    let stream = wait_stream();
    let query = std::ffi::CStr::from_ptr(pattern)
        .to_str()
        .expect("Expected a zero-terminated string");
    if let Err(err) = send_query(my_slot(), stream, query) {
        error!("Failed to send the query: {}", err);
    }
    loop {
        let Some(slot) = Bus::new().slot_locked(my_slot()) else {
            wait_latch(None);
            continue;
        };
        let mut stream = SlotStream::from(slot);
        let header = consume_header(&mut stream).expect("Failed to consume header");
        if header.direction != Direction::ToBackend {
            wait_latch(None);
            continue;
        }
        match header.packet {
            Packet::Failure => {
                let msg = read_error(&mut stream).expect("Failed to read the error message");
                error!("Failed to compile the query: {}", msg);
            }
            Packet::Metadata => todo!(),
            Packet::Ack => break,
            _ => error!("Unexpected packet in backend: {:?}", header.packet),
        }
    }
    let param_list = (*list_nth(list, 1)).ptr_value as ParamListInfo;
    let num_params = unsafe { (*param_list).numParams } as usize;
    if num_params > 0 {
        let params = unsafe { (*param_list).params.as_slice(num_params) };
        let stream = wait_stream();
        if let Err(err) = send_params(my_slot(), stream, params) {
            error!("Failed to send the parameter list: {}", err);
        }
    }
    let css = CustomScanState {
        methods: exec_methods(),
        ..Default::default()
    };
    let state = ScanState { css };
    let mut node = PgNode::empty(std::mem::size_of::<ScanState>());
    node.set_tag(NodeTag::T_CustomScanState);
    node.set_data(unsafe {
        std::slice::from_raw_parts(
            &state as *const _ as *const u8,
            std::mem::size_of::<ScanState>(),
        )
    });
    node.mut_node()
}

unsafe extern "C" fn begin_df_scan(node: *mut CustomScanState, estate: *mut EState, eflags: i32) {
    todo!()
}

unsafe extern "C" fn exec_df_scan(node: *mut CustomScanState) -> *mut TupleTableSlot {
    todo!()
}

unsafe extern "C" fn end_df_scan(node: *mut CustomScanState) {
    todo!()
}

unsafe extern "C" fn explain_df_scan(
    node: *mut CustomScanState,
    ancestors: *mut List,
    es: *mut ExplainState,
) {
    todo!()
}

// We expect that the header is already consumed and the packet type is `Packet::Metadata`.
fn table_oids(stream: &mut SlotStream) -> AnyResult<SmallVec<[Oid; 16]>> {
    let table_not_found = |c_table_name: &[u8]| -> Result<(), FusionError> {
        assert!(!c_table_name.is_empty());
        let table_name = c_table_name[..c_table_name.len() - 1].as_ref();
        match std::str::from_utf8(table_name) {
            Ok(name) => Err(FusionError::NotFound("Table".into(), name.into())),
            Err(_) => Err(FusionError::NotFound(
                "Table".into(),
                format!("{:?}", table_name),
            )),
        }
    };
    let table_num = read_array_len(stream)?;
    let mut oids: SmallVec<[Oid; 16]> = SmallVec::with_capacity(table_num as usize);
    for _ in 0..table_num {
        let elem_num = read_array_len(stream)?;
        match elem_num {
            1 => {
                let table_len = read_bin_len(stream)?;
                let table_name = stream.look_ahead(table_len as usize)?;
                let mut search_path: [Oid; 16] = [InvalidOid; 16];
                let path_len = unsafe {
                    fetch_search_path_array(search_path.as_mut_ptr(), search_path.len() as i32)
                };
                let path = &search_path[..path_len as usize];
                let mut rel_oid = InvalidOid;
                for ns_oid in path {
                    rel_oid =
                        unsafe { get_relname_relid(table_name.as_ptr() as *const c_char, *ns_oid) };
                    if rel_oid != InvalidOid {
                        oids.push(rel_oid);
                        break;
                    }
                }
                if rel_oid == InvalidOid {
                    table_not_found(table_name)?;
                }
                stream.rewind(table_len as usize)?;
            }
            2 => {
                let schema_len = read_bin_len(stream)?;
                let schema_name = stream.look_ahead(schema_len as usize)?;
                // Through an error if schema name not found.
                let ns_oid =
                    unsafe { get_namespace_oid(schema_name.as_ptr() as *const c_char, false) };
                stream.rewind(schema_len as usize)?;
                let table_len = read_bin_len(stream)?;
                let table_name = stream.look_ahead(table_len as usize)?;
                let rel_oid =
                    unsafe { get_relname_relid(table_name.as_ptr() as *const c_char, ns_oid) };
                if rel_oid == InvalidOid {
                    table_not_found(table_name)?;
                }
                stream.rewind(table_len as usize)?;
                oids.push(rel_oid);
            }
            _ => {
                return Err(FusionError::InvalidName(
                    "Table".into(),
                    "support only 'schema.table' format".into(),
                ))?
            }
        }
    }
    Ok(oids)
}

fn wait_latch(timeout: Option<Duration>) {
    let timeout: c_long = timeout
        .map(|t| t.as_millis().try_into().unwrap())
        .unwrap_or(0);
    let events = WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH;
    let rc = unsafe {
        let rc = WaitLatch(MyLatch, events as i32, timeout, PG_WAIT_EXTENSION);
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

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::ipc::Slot;
    use crate::protocol::prepare_table_refs;

    use super::*;
    use datafusion_sql::TableReference;
    use pgrx::prelude::*;
    use pgrx::spi::Spi;
    use std::ffi::c_void;
    use std::ptr::addr_of_mut;

    const SLOT_SIZE: usize = 8204;

    #[pg_test]
    fn test_node() {
        let data = vec![1, 2, 3, 4];
        let mut pg_node = PgNode::empty(data.len());
        pg_node.set_tag(NodeTag::T_CustomScanState);
        pg_node.set_data(&data);
        assert_eq!(pg_node.data(), data.as_slice());
        unsafe {
            let ptr = pg_node.mut_node() as *mut c_void;
            pg_sys::pfree(ptr);
        }
    }

    #[pg_test]
    fn test_table_oids() {
        Spi::run("create table if not exists t1(a int, b text);").unwrap();
        Spi::run("create schema if not exists s1;").unwrap();
        Spi::run("create table if not exists s1.t2(a int, b text);").unwrap();
        let t1_oid = Spi::get_one::<Oid>("select 't1'::regclass::oid;")
            .unwrap()
            .unwrap();
        let t2_oid = Spi::get_one::<Oid>("select 's1.t2'::regclass::oid;")
            .unwrap()
            .unwrap();

        let mut slot_buf: [u8; SLOT_SIZE] = [1; SLOT_SIZE];
        let ptr = addr_of_mut!(slot_buf) as *mut u8;
        Slot::init(ptr, slot_buf.len());
        let slot = Slot::from_bytes(ptr, slot_buf.len());
        let mut stream: SlotStream = slot.into();

        // Check valid tables.
        let t1 = TableReference::bare("t1");
        let t2 = TableReference::partial("s1", "t2");
        let tables = vec![&t1, &t2];
        prepare_table_refs(&mut stream, &tables).unwrap();
        stream.reset();
        let _ = consume_header(&mut stream).unwrap();
        let oids = table_oids(&mut stream).unwrap();
        assert_eq!(oids.len(), 2);
        assert_eq!(oids[0], t1_oid);
        assert_eq!(oids[1], t2_oid);
        stream.reset();

        // Check invalid table.
        let t3 = TableReference::bare("t3");
        let tables = vec![&t3];
        prepare_table_refs(&mut stream, &tables).unwrap();
        stream.reset();
        let _ = consume_header(&mut stream).unwrap();
        let err = table_oids(&mut stream).unwrap_err();
        assert_eq!(err.to_string(), "Table not found: t3");
    }
}
