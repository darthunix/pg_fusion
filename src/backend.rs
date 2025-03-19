use libc::c_long;
use pgrx::pg_sys::{
    error, palloc0, CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, EState,
    ExplainState, List, ListCell, MyLatch, Node, NodeTag, ParamListInfo, RegisterCustomScanMethods,
    ResetLatch, TupleTableSlot, WaitLatch, PG_WAIT_EXTENSION, WL_LATCH_SET, WL_POSTMASTER_DEATH,
    WL_TIMEOUT,
};
use pgrx::{check_for_interrupts, pg_guard};
use std::ffi::c_char;
use std::time::Duration;

use crate::ipc::{my_slot, Bus, SlotHandler, SlotNumber, SlotStream, CURRENT_SLOT};
use crate::protocol::{consume_header, read_error, send_query, Direction, Packet};

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
    let list = (*cscan).custom_private;
    let pattern = (*list_nth(list, 0)).ptr_value as *mut c_char;
    let params = (*list_nth(list, 1)).ptr_value as ParamListInfo;
    let mut stream = None;
    loop {
        wait_latch(None);
        let Some(slot) = Bus::new().slot_locked(my_slot()) else {
            continue;
        };
        stream = Some(SlotStream::from(slot));
        break;
    }
    let query = std::ffi::CStr::from_ptr(pattern)
        .to_str()
        .expect("Expected a zero-terminated string");
    if let Err(err) = send_query(
        my_slot(),
        stream.expect("Failed to acquire a slot stream"),
        query,
    ) {
        error!("Failed to send the query: {}", err);
    }
    loop {
        wait_latch(None);
        let Some(slot) = Bus::new().slot_locked(my_slot()) else {
            continue;
        };
        let mut stream = SlotStream::from(slot);
        let header = consume_header(&mut stream).expect("Failed to consume header");
        if header.direction == Direction::ToWorker {
            continue;
        }
        match header.packet {
            Packet::Failure => {
                let msg = read_error(&mut stream).expect("Failed to read the error message");
                error!("Failed to compile the query: {}", msg);
            }
            Packet::Bind => unimplemented!(),
            Packet::Metadata => unimplemented!(),
            Packet::None => continue,
            Packet::Plan => break,
            _ => error!("Unexpected packet in backend: {:?}", header.packet),
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
    use super::*;
    use pgrx::prelude::*;
    use std::ffi::c_void;

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
}
