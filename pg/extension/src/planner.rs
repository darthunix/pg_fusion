use std::ffi::{c_char, c_int, c_void, CStr};
use std::hash::{Hash, Hasher};
use std::mem::size_of;
use std::ptr::null_mut;

use arrow_schema::DataType;
use datafusion::logical_expr::LogicalPlan;
use pgrx::pg_sys::SysCacheIdentifier::TYPEOID;
use pgrx::pg_sys::{
    list_append_unique_ptr, list_make1_impl, palloc0, planner_hook, planner_hook_type,
    standard_planner, CustomScan, List, ListCell, NodeTag, Oid, ParamListInfo, Plan, PlannedStmt,
    Query,
};
use pgrx::prelude::*;

use crate::custom_scan::scan_methods;
use crate::guc::ENABLE;
use crate::utility_hook::skip_planner;

static mut PREV_PLANNER_HOOK: planner_hook_type = None;

pub fn register_hooks() {
    unsafe {
        PREV_PLANNER_HOOK = planner_hook;
        planner_hook = Some(pg_fusion_planner_hook);
    }
}

#[pg_guard]
unsafe extern "C-unwind" fn pg_fusion_planner_hook(
    parse: *mut Query,
    query_string: *const c_char,
    cursor_options: c_int,
    bound_params: ParamListInfo,
) -> *mut PlannedStmt {
    if ENABLE.get() && !skip_planner() {
        if !parse.is_null()
            && (*parse).commandType == pgrx::pg_sys::CmdType::CMD_SELECT
            && !(*parse).hasModifyingCTE
            && !is_pg_fusion_management_sql(query_string)
        {
            return build_planned_custom_scan(parse, query_string, bound_params);
        }
    }

    if let Some(prev) = PREV_PLANNER_HOOK {
        prev(parse, query_string, cursor_options, bound_params)
    } else {
        standard_planner(parse, query_string, cursor_options, bound_params)
    }
}

unsafe fn is_pg_fusion_management_sql(query_string: *const c_char) -> bool {
    if query_string.is_null() {
        return false;
    }
    let Ok(sql) = CStr::from_ptr(query_string).to_str() else {
        return false;
    };
    let sql = sql.to_ascii_lowercase();
    sql.contains("pg_fusion_metrics(") || sql.contains("pg_fusion_metrics_reset(")
}

#[pg_guard]
unsafe extern "C-unwind" fn build_planned_custom_scan(
    parse: *mut Query,
    query_string: *const c_char,
    bound_params: ParamListInfo,
) -> *mut PlannedStmt {
    if !bound_params.is_null() && (*bound_params).numParams > 0 {
        error!(
            "pg_fusion v1 does not support bind parameters yet; see planner.rs TODO for ParamListInfo -> ScalarValue bridging"
        );
    }

    let sql = select_sql_from_query(parse, query_string);
    let config = crate::host_config().unwrap_or_else(|err| error!("pg_fusion config error: {err}"));
    let built = plan_builder::PlanBuilder::new()
        .with_config(config.plan_builder_config())
        .build(plan_builder::PlanBuildInput {
            sql: &sql,
            params: Vec::new(),
        })
        .unwrap_or_else(|err| error!("pg_fusion planner build failed: {err}"));

    let target_lists = build_custom_scan_target_lists(&built.logical_plan)
        .unwrap_or_else(|err| error!("pg_fusion targetlist build failed: {err}"));
    let custom_scan = pack_custom_scan(&sql, target_lists);

    let stmt_ptr = palloc0(size_of::<PlannedStmt>()) as *mut PlannedStmt;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut hasher);
    let statement = PlannedStmt {
        type_: NodeTag::T_PlannedStmt,
        commandType: pgrx::pg_sys::CmdType::CMD_SELECT,
        queryId: hasher.finish(),
        hasReturning: false,
        hasModifyingCTE: false,
        canSetTag: false,
        transientPlan: false,
        dependsOnRole: false,
        parallelModeNeeded: false,
        planTree: custom_scan as *mut Plan,
        rtable: null_mut(),
        permInfos: null_mut(),
        resultRelations: null_mut(),
        subplans: null_mut(),
        rewindPlanIDs: null_mut(),
        rowMarks: null_mut(),
        relationOids: null_mut(),
        invalItems: null_mut(),
        paramExecTypes: null_mut(),
        utilityStmt: null_mut(),
        stmt_location: -1,
        stmt_len: 0,
        ..Default::default()
    };
    std::ptr::write(stmt_ptr, statement);
    stmt_ptr
}

unsafe fn select_sql_from_query(parse: *mut Query, query_string: *const c_char) -> String {
    let sql = CStr::from_ptr(query_string)
        .to_str()
        .expect("planner query string must be valid UTF-8");
    if parse.is_null() {
        return sql.to_owned();
    }

    if sql.trim_start().to_ascii_uppercase().starts_with("EXPLAIN") {
        let deparsed = pgrx::pg_sys::pg_get_querydef(parse, false);
        if !deparsed.is_null() {
            return CStr::from_ptr(deparsed)
                .to_str()
                .expect("deparsed query text must be valid UTF-8")
                .to_owned();
        }
    }

    sql.to_owned()
}

struct CustomScanTargetLists {
    plan_target_list: *mut List,
    scan_target_list: *mut List,
}

unsafe fn pack_custom_scan(sql: &str, target_lists: CustomScanTargetLists) -> *mut CustomScan {
    let sql_copy = palloc0(sql.len() + 1) as *mut u8;
    std::ptr::copy_nonoverlapping(sql.as_ptr(), sql_copy, sql.len());
    let query = ListCell {
        ptr_value: pgrx::pg_sys::makeString(sql_copy.cast()) as *mut c_void,
    };

    let mut custom_scan = CustomScan::default();
    custom_scan.scan.plan.type_ = NodeTag::T_CustomScan;
    custom_scan.custom_private = list_make1_impl(NodeTag::T_List, query);
    custom_scan.custom_scan_tlist = target_lists.scan_target_list;
    custom_scan.scan.plan.targetlist = target_lists.plan_target_list;
    custom_scan.methods = scan_methods();

    let ptr = palloc0(size_of::<CustomScan>()) as *mut CustomScan;
    std::ptr::write(ptr, custom_scan);
    ptr
}

fn build_custom_scan_target_lists(
    logical_plan: &LogicalPlan,
) -> Result<CustomScanTargetLists, String> {
    let fields = logical_plan.schema().fields();
    let mut plan_target_list: *mut List = std::ptr::null_mut();
    let mut scan_target_list: *mut List = std::ptr::null_mut();
    for (index, field) in fields.iter().enumerate() {
        let oid = type_to_oid(field.data_type())
            .ok_or_else(|| format!("unsupported output type {}", field.data_type()))?;
        unsafe {
            let tuple =
                pgrx::pg_sys::SearchSysCache1(TYPEOID as i32, pgrx::pg_sys::ObjectIdGetDatum(oid));
            if tuple.is_null() {
                return Err(format!("type cache lookup failed for oid {}", oid.to_u32()));
            }
            let typtup = pgrx::pg_sys::GETSTRUCT(tuple) as pgrx::pg_sys::Form_pg_type;
            let attr_number = i16::try_from(index + 1)
                .map_err(|_| "custom scan output has too many columns".to_string())?;
            let typmod = (*typtup).typtypmod;
            let collation = (*typtup).typcollation;
            let plan_expr = pgrx::pg_sys::makeVar(
                pgrx::pg_sys::INDEX_VAR,
                attr_number,
                oid,
                typmod,
                collation,
                0,
            );
            let scan_expr = pgrx::pg_sys::makeNullConst(oid, typmod, collation);
            let name = field.name();
            let plan_entry = pgrx::pg_sys::makeTargetEntry(
                plan_expr as *mut pgrx::pg_sys::Expr,
                attr_number as _,
                pstrdup(name),
                false,
            );
            let scan_entry = pgrx::pg_sys::makeTargetEntry(
                scan_expr as *mut pgrx::pg_sys::Expr,
                attr_number as _,
                pstrdup(name),
                false,
            );
            plan_target_list = list_append_unique_ptr(plan_target_list, plan_entry as *mut c_void);
            scan_target_list = list_append_unique_ptr(scan_target_list, scan_entry as *mut c_void);
            pgrx::pg_sys::ReleaseSysCache(tuple);
        }
    }
    Ok(CustomScanTargetLists {
        plan_target_list,
        scan_target_list,
    })
}

unsafe fn pstrdup(value: &str) -> *mut i8 {
    let ptr = palloc0(value.len() + 1) as *mut u8;
    std::ptr::copy_nonoverlapping(value.as_ptr(), ptr, value.len());
    ptr.cast()
}

fn type_to_oid(data_type: &DataType) -> Option<Oid> {
    match data_type {
        DataType::Boolean => Some(pgrx::pg_sys::BOOLOID),
        DataType::Int16 => Some(pgrx::pg_sys::INT2OID),
        DataType::Int32 => Some(pgrx::pg_sys::INT4OID),
        DataType::Int64 => Some(pgrx::pg_sys::INT8OID),
        DataType::Float32 => Some(pgrx::pg_sys::FLOAT4OID),
        DataType::Float64 => Some(pgrx::pg_sys::FLOAT8OID),
        DataType::Utf8 | DataType::Utf8View => Some(pgrx::pg_sys::TEXTOID),
        DataType::Binary | DataType::BinaryView => Some(pgrx::pg_sys::BYTEAOID),
        DataType::FixedSizeBinary(16) => Some(pgrx::pg_sys::UUIDOID),
        _ => None,
    }
}

// TODO(darthunix): add ParamListInfo -> ScalarValue bridging for bind params in
// the new thin-host planner/custom-scan path. The first cutover intentionally
// matches current effective behavior and does not support backend bind params.
