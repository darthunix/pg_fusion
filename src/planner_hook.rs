use pg_sys::{
    planner_hook, planner_hook_type, standard_planner, ParamListInfo, Plan, PlannedStmt, Query,
};
use pgrx::pg_sys::CustomScan;
use pgrx::prelude::*;
use std::ffi::{c_char, c_int, CStr};

use crate::ipc::{SlotHandler, CURRENT_SLOT};
use crate::ENABLE_DATAFUSION;

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
    }
    unsafe {
        if let Some(prev_hook) = PREV_PLANNER_HOOK {
            prev_hook(parse, query_string, cursoroptions, boundparams)
        } else {
            standard_planner(parse, query_string, cursoroptions, boundparams)
        }
    }
}

// #[pg_guard]
// fn create_plan(pattern: String) -> CustomScan {
//     let mut node = CustomScan::new();
//
//     node
// }
