use anyhow::Result;
use pg_sys::{
    list_make2_impl, planner_hook, planner_hook_type, standard_planner, ListCell, NodeTag,
    ParamListInfo, Plan, PlannedStmt, Query,
};
use pgrx::pg_sys::CmdType::CMD_SELECT;
use pgrx::pg_sys::{palloc0, CustomScan, Node};
use pgrx::prelude::*;
use std::ffi::{c_char, c_int, c_void, CStr};
use std::mem::size_of;
use std::ptr::null_mut;

use crate::ipc::{SlotHandler, CURRENT_SLOT};
use crate::node::{scan_methods, PgNode};
use crate::{ENABLE_DATAFUSION, SEED};

static mut PREV_PLANNER_HOOK: planner_hook_type = None;

fn current_slot() -> &'static SlotHandler {
    unsafe { CURRENT_SLOT.get_or_init(|| SlotHandler::new()) }
}

#[pg_guard]
#[no_mangle]
pub(crate) extern "C" fn init_datafusion_planner_hook() {
    unsafe {
        if planner_hook.is_some() {
            PREV_PLANNER_HOOK = planner_hook;
        }
        planner_hook = Some(datafusion_planner_hook);
    }
}

#[pg_guard]
#[no_mangle]
extern "C" fn datafusion_planner_hook(
    parse: *mut Query,
    query_string: *const c_char,
    cursoroptions: c_int,
    boundparams: ParamListInfo,
) -> *mut PlannedStmt {
    if ENABLE_DATAFUSION.get() {
        let pattern = unsafe { CStr::from_ptr(query_string) };
        info!(
            "DataFusion planner hook called for query: {:?}",
            pattern.to_str().unwrap()
        );
        let slot_id = current_slot().id();
        info!("Slot id: {slot_id}");
        return df_planner(query_string, boundparams);
    }
    unsafe {
        if let Some(prev_hook) = PREV_PLANNER_HOOK {
            prev_hook(parse, query_string, cursoroptions, boundparams)
        } else {
            standard_planner(parse, query_string, cursoroptions, boundparams)
        }
    }
}

#[pg_guard]
fn df_planner(pattern: *const c_char, params: ParamListInfo) -> *mut PlannedStmt {
    let seed: u64 = unsafe { SEED.unwrap() };
    let bytes = unsafe { CStr::from_ptr(pattern).to_bytes() };

    let cscan = pack_args(pattern, params);

    let stmt_ptr = unsafe { palloc0(size_of::<PlannedStmt>()) as *mut PlannedStmt };
    let mut stmt = PlannedStmt::default();
    stmt.type_ = NodeTag::T_PlannedStmt;
    stmt.commandType = CMD_SELECT;
    stmt.queryId = fasthash::murmur2::hash64_with_seed(bytes, seed);
    stmt.hasReturning = false;
    stmt.hasModifyingCTE = false;
    stmt.canSetTag = false;
    stmt.transientPlan = false;
    stmt.dependsOnRole = false;
    stmt.parallelModeNeeded = false;
    stmt.planTree = cscan as *mut Plan;
    stmt.rtable = null_mut();
    stmt.permInfos = null_mut();
    stmt.resultRelations = null_mut();
    stmt.subplans = null_mut();
    stmt.rewindPlanIDs = null_mut();
    stmt.rowMarks = null_mut();
    stmt.relationOids = null_mut();
    stmt.invalItems = null_mut();
    stmt.paramExecTypes = null_mut();
    stmt.utilityStmt = null_mut();
    stmt.stmt_location = -1;
    stmt.stmt_len = 0;
    unsafe {
        std::ptr::write(stmt_ptr, stmt);
    }

    stmt_ptr
}

#[pg_guard]
fn pack_args(pattern: *const c_char, params: ParamListInfo) -> *mut CustomScan {
    let mut cscan = CustomScan::default();
    cscan.scan.plan.type_ = NodeTag::T_CustomScan;
    let lc_query = ListCell {
        ptr_value: pattern as *mut c_void,
    };
    let lc_params = ListCell {
        ptr_value: params as *mut c_void,
    };
    cscan.custom_private = unsafe { list_make2_impl(NodeTag::T_List, lc_query, lc_params) };
    cscan.methods = scan_methods();

    unsafe {
        let ptr = palloc0(size_of::<CustomScan>()) as *mut CustomScan;
        std::ptr::write(ptr, cscan);
        ptr
    }
}
