use pgrx::pg_sys::{
    standard_ProcessUtility, DestReceiver, ParamListInfo, PlannedStmt, ProcessUtility_hook,
    ProcessUtility_hook_type, QueryCompletion, QueryEnvironment,
};
use pgrx::prelude::*;
use std::cell::Cell;
use std::ffi::c_char;

static mut PREV_PROCESS_UTILITY_HOOK: ProcessUtility_hook_type = None;

thread_local! {
    static SKIP_PLANNER_GUARD_DEPTH: Cell<usize> = const { Cell::new(0) };
}

pub(crate) fn skip_planner() -> bool {
    SKIP_PLANNER_GUARD_DEPTH.with(|guard| guard.get() != 0)
}

pub(crate) struct PlannerBypassGuard;

impl PlannerBypassGuard {
    pub(crate) fn enter() -> Self {
        SKIP_PLANNER_GUARD_DEPTH.with(|guard| guard.set(guard.get() + 1));
        Self
    }
}

impl Drop for PlannerBypassGuard {
    fn drop(&mut self) {
        SKIP_PLANNER_GUARD_DEPTH.with(|guard| {
            let current = guard.get();
            assert!(current > 0, "planner bypass guard underflow");
            guard.set(current - 1);
        });
    }
}

pub(crate) fn register_hook() {
    unsafe {
        PREV_PROCESS_UTILITY_HOOK = ProcessUtility_hook;
        ProcessUtility_hook = Some(pg_fusion_process_utility_hook);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_fusion_process_utility_hook(
    pstmt: *mut PlannedStmt,
    query_string: *const c_char,
    read_only_tree: bool,
    context: u32,
    params: ParamListInfo,
    query_env: *mut QueryEnvironment,
    dest: *mut DestReceiver,
    qc: *mut QueryCompletion,
) {
    let mut guarded = false;
    if !pstmt.is_null() {
        let utility = (*pstmt).utilityStmt;
        if !utility.is_null() && (*utility).type_ == pgrx::pg_sys::NodeTag::T_CreateTableAsStmt {
            guarded = true;
        }
    }

    let _guard = guarded.then(PlannerBypassGuard::enter);

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
}
