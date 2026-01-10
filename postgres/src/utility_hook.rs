use pgrx::pg_sys::{
    standard_ProcessUtility, DestReceiver, ParamListInfo, PlannedStmt, ProcessUtility_hook,
    ProcessUtility_hook_type, QueryCompletion, QueryEnvironment,
};
use pgrx::prelude::*;
use std::cell::Cell;
use std::ffi::c_char;

static mut PREV_PROCESS_UTILITY_HOOK: ProcessUtility_hook_type = None;

thread_local! {
    static SKIP_DF_PLANNER_GUARD: Cell<bool> = const { Cell::new(false) };
}

pub fn skip_df_planner() -> bool {
    SKIP_DF_PLANNER_GUARD.with(|f| f.get())
}

#[pg_guard]
#[no_mangle]
#[allow(static_mut_refs)]
pub(crate) extern "C-unwind" fn init_datafusion_utility_hook() {
    unsafe {
        if ProcessUtility_hook.is_some() {
            PREV_PROCESS_UTILITY_HOOK = ProcessUtility_hook;
        }
        ProcessUtility_hook = Some(datafusion_process_utility_hook);
    }
}

#[pg_guard]
#[no_mangle]
unsafe extern "C-unwind" fn datafusion_process_utility_hook(
    pstmt: *mut PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: u32,
    params: ParamListInfo,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    // Detect CREATE TABLE AS (includes SELECT INTO after parse analysis) and guard planner hook.
    let mut set_guard = false;
    unsafe {
        if !pstmt.is_null() {
            let util = (*pstmt).utilityStmt;
            if !util.is_null() {
                let tag = (*util).type_;
                if tag == pgrx::pg_sys::NodeTag::T_CreateTableAsStmt {
                    set_guard = true;
                }
            }
        }
    }
    if set_guard {
        SKIP_DF_PLANNER_GUARD.with(|f| f.set(true));
    }
    if let Some(prev) = PREV_PROCESS_UTILITY_HOOK {
        prev(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    } else {
        standard_ProcessUtility(
            pstmt,
            query_string,
            read_only_tree,
            context,
            params,
            query_env,
            dest,
            qc,
        );
    }
    if set_guard {
        SKIP_DF_PLANNER_GUARD.with(|f| f.set(false));
    }
}
