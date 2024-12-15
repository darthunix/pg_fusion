use datafusion::scalar::ScalarValue;
use pgrx::pg_guard;
use pgrx::pg_sys::{
    CustomExecMethods, CustomScan, CustomScanMethods, CustomScanState, EState, ExplainState, List,
    Node, TupleTableSlot,
};
use std::cell::OnceCell;
use std::ffi::c_char;

static mut SCAN_METHODS: OnceCell<CustomScanMethods> = OnceCell::new();
static mut EXEC_METHODS: OnceCell<CustomExecMethods> = OnceCell::new();

#[pg_guard]
#[no_mangle]
pub(crate) extern "C" fn init_datafusion_methods() {}

struct ScanState {
    css: CustomScanState,
    pattern: *const c_char,
    params: ScalarValue,
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
