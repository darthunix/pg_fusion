use pgrx::pg_sys::{
    palloc0, pfree, CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, EState,
    ExplainState, List, Node, NodeTag, ParamListInfo, RegisterCustomScanMethods, TupleTableSlot,
};
use pgrx::{pg_guard, pg_schema};
use std::cell::OnceCell;
use std::ffi::c_char;

static mut SCAN_METHODS: OnceCell<CustomScanMethods> = OnceCell::new();
static mut EXEC_METHODS: OnceCell<CustomExecMethods> = OnceCell::new();

#[pg_guard]
#[no_mangle]
pub(crate) extern "C" fn init_df_methods() {
    unsafe {
        EXEC_METHODS
            .set(CustomExecMethods {
                CustomName: b"DataFusionScan\0".as_ptr() as *const i8,
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
            })
            .unwrap();

        SCAN_METHODS
            .set(CustomScanMethods {
                CustomName: b"DataFusionScan\0".as_ptr() as *const i8,
                CreateCustomScanState: Some(create_df_scan_state),
            })
            .unwrap();
        RegisterCustomScanMethods(SCAN_METHODS.get().unwrap());
    }
}

#[repr(C)]
struct ScanState {
    css: CustomScanState,
    pattern: *const c_char,
    params: ParamListInfo,
}

unsafe extern "C" fn create_df_scan_state(cscan: *mut CustomScan) -> *mut Node {
    todo!()
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

#[repr(C)]
struct CustomScanStateNode {
    tag: NodeTag,
    state: ScanState,
}

impl CustomScanStateNode {
    fn to_node(state: ScanState) -> *mut Node {
        let size = std::mem::size_of::<CustomScanStateNode>();
        let mut pg_node = PgNode::empty(size);
        pg_node.set_tag(NodeTag::T_CustomScanState);
        pg_node.set_data(unsafe {
            std::slice::from_raw_parts(
                &state as *const _ as *const u8,
                std::mem::size_of::<ScanState>(),
            )
        });
        pg_node.mut_node()
    }
}

struct PgNode {
    size: usize,
    node: *mut Node,
}

impl PgNode {
    fn empty(data_size: usize) -> Self {
        let size = std::mem::size_of::<Node>() + data_size;
        let buf_ptr = unsafe { palloc0(size) };
        PgNode {
            size,
            node: buf_ptr as *mut Node,
        }
    }

    fn set_tag(&mut self, tag: NodeTag) {
        unsafe {
            (*self.node).type_ = tag;
        }
    }

    fn set_data(&mut self, data: &[u8]) {
        assert!(data.len() <= self.size - std::mem::size_of::<Node>());
        unsafe {
            let buf = std::slice::from_raw_parts_mut(self.node as *mut u8, self.size);
            buf[std::mem::size_of::<Node>()..].copy_from_slice(data);
        }
    }

    fn data(&self) -> &[u8] {
        unsafe {
            let buf = std::slice::from_raw_parts(self.node as *const u8, self.size);
            &buf[std::mem::size_of::<Node>()..]
        }
    }

    fn mut_node(&self) -> *mut Node {
        self.node
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
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
            pfree(ptr);
        }
    }
}
