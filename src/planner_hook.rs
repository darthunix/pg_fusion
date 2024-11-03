use pg_sys::{
    planner_hook, planner_hook_type, standard_planner, ParamListInfo, PlannedStmt, Query,
};
use pgrx::prelude::*;
use std::ffi::{c_char, c_int, CStr};

use crate::ENABLE_DATAFUSION;

static mut PREV_PLANNER_HOOK: planner_hook_type = None;

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
    }
    unsafe {
        if let Some(prev_hook) = PREV_PLANNER_HOOK {
            prev_hook(parse, query_string, cursoroptions, boundparams)
        } else {
            standard_planner(parse, query_string, cursoroptions, boundparams)
        }
    }
}
