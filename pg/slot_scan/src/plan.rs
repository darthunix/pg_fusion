use crate::error::ScanError;
use crate::types::{OwnedSpiPlan, PreparedScan, ScanOptions};
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::PgTryBuilder;
use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::panic::AssertUnwindSafe;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlanMetadata {
    pub(crate) parallel_capable: bool,
    pub(crate) planned_workers: usize,
}

/// Parses, plans, and structurally validates trusted SQL for later execution.
///
/// The returned [`PreparedScan`] stores a saved SPI plan and can be reused
/// across multiple `run()` calls. Only a narrow set of read-only scan-oriented
/// plan shapes is accepted.
///
/// This function is intended for compiler-generated, side-effect-free scan SQL,
/// not arbitrary caller-provided `SELECT` text. Validation here is structural:
/// it rejects unsupported PostgreSQL plan shapes, but it does not act as a SQL
/// sandbox or inspect every expression for side effects.
pub fn prepare_scan(sql: &str, options: ScanOptions) -> Result<PreparedScan, ScanError> {
    let c_sql = CString::new(sql).map_err(|_| ScanError::InvalidSql)?;

    with_spi(|| unsafe {
        let plan = pg_sys::SPI_prepare_cursor(
            c_sql.as_ptr(),
            0,
            std::ptr::null_mut(),
            pg_sys::CURSOR_OPT_PARALLEL_OK as i32,
        );
        if plan.is_null() {
            return Err(spi_status_error("SPI_prepare_cursor", pg_sys::SPI_result));
        }
        if !pg_sys::SPI_is_cursor_plan(plan) {
            return Err(ScanError::UnsupportedPlan(
                "prepared SQL must produce tuples for a cursor scan".into(),
            ));
        }

        inspect_spi_plan(plan)?;
        let keep_rc = pg_sys::SPI_keepplan(plan);
        if keep_rc != 0 {
            return Err(spi_status_error("SPI_keepplan", keep_rc));
        }

        Ok(PreparedScan {
            sql: sql.to_string(),
            options,
            plan: OwnedSpiPlan::from_spi_plan(plan),
        })
    })
}

pub(crate) fn with_spi<T>(f: impl FnOnce() -> Result<T, ScanError>) -> Result<T, ScanError> {
    let connected = Cell::new(false);
    let finish_rc = Cell::new(pg_sys::SPI_OK_FINISH as i32);

    let result = PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let connect_rc = pg_sys::SPI_connect();
        if connect_rc != pg_sys::SPI_OK_CONNECT as i32 {
            return Err(spi_status_error("SPI_connect", connect_rc));
        }
        connected.set(true);
        f()
    }))
    .catch_others(|e| Err(scan_error_from_caught_error(e)))
    .finally(|| {
        if connected.get() {
            finish_rc.set(unsafe { pg_sys::SPI_finish() });
            connected.set(false);
        }
    })
    .execute();

    if finish_rc.get() != pg_sys::SPI_OK_FINISH as i32 {
        return Err(spi_status_error("SPI_finish", finish_rc.get()));
    }

    result
}

unsafe fn inspect_spi_plan(plan: pg_sys::SPIPlanPtr) -> Result<PlanMetadata, ScanError> {
    let plan_sources = pg_sys::SPI_plan_get_plan_sources(plan);
    if plan_sources.is_null() || (*plan_sources).length != 1 {
        return Err(ScanError::MultipleStatements);
    }

    let cached_plan = pg_sys::SPI_plan_get_cached_plan(plan);
    if cached_plan.is_null() {
        return Err(ScanError::Postgres(
            "SPI_plan_get_cached_plan returned null".into(),
        ));
    }

    let inspected = (|| -> Result<PlanMetadata, ScanError> {
        let stmt_list = (*cached_plan).stmt_list;
        if stmt_list.is_null() || (*stmt_list).length != 1 {
            return Err(ScanError::MultipleStatements);
        }

        let planned_stmt = list_nth(stmt_list, 0) as *mut pg_sys::PlannedStmt;
        if planned_stmt.is_null() {
            return Err(ScanError::UnsupportedPlan("null planned statement".into()));
        }

        inspect_planned_stmt(planned_stmt)
    })();

    pg_sys::ReleaseCachedPlan(cached_plan, std::ptr::null_mut());
    inspected
}

pub(crate) fn spi_status_error(label: &str, code: i32) -> ScanError {
    unsafe {
        let message = pg_sys::SPI_result_code_string(code);
        if message.is_null() {
            ScanError::Postgres(format!("{label} failed with status {code}"))
        } else {
            let message = CStr::from_ptr(message).to_string_lossy();
            ScanError::Postgres(format!("{label} failed with status {code} ({message})"))
        }
    }
}

pub(crate) fn scan_error_from_caught_error(error: CaughtError) -> ScanError {
    let message = match error {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report.message().to_owned(),
    };
    ScanError::Postgres(message)
}

pub(crate) unsafe fn inspect_planned_stmt(
    planned_stmt: *mut pg_sys::PlannedStmt,
) -> Result<PlanMetadata, ScanError> {
    if (*planned_stmt).commandType != pg_sys::CmdType::CMD_SELECT {
        return Err(ScanError::UnsupportedPlan(
            "only SELECT statements are supported".into(),
        ));
    }
    if (*planned_stmt).hasModifyingCTE {
        return Err(ScanError::UnsupportedPlan(
            "modifying CTEs are not supported".into(),
        ));
    }
    if !(*planned_stmt).subplans.is_null() && (*(*planned_stmt).subplans).length > 0 {
        return Err(ScanError::UnsupportedPlan(
            "subplans are not supported".into(),
        ));
    }
    if !(*planned_stmt).utilityStmt.is_null() {
        return Err(ScanError::UnsupportedPlan(
            "utility statements are not supported".into(),
        ));
    }
    inspect_plan((*planned_stmt).planTree)
}

pub(crate) unsafe fn inspect_plan(plan: *mut pg_sys::Plan) -> Result<PlanMetadata, ScanError> {
    if plan.is_null() {
        return Err(ScanError::UnsupportedPlan("null plan tree".into()));
    }
    if !(*plan).initPlan.is_null() && (*(*plan).initPlan).length > 0 {
        return Err(ScanError::UnsupportedPlan(
            "init plans are not supported".into(),
        ));
    }
    match (*plan).type_ {
        pg_sys::NodeTag::T_Gather => {
            let gather = plan as *mut pg_sys::Gather;
            inspect_plan((*gather).plan.lefttree)?;
            Ok(PlanMetadata {
                parallel_capable: true,
                planned_workers: (*gather).num_workers.max(0) as usize,
            })
        }
        pg_sys::NodeTag::T_GatherMerge => Err(ScanError::UnsupportedPlan(
            "GatherMerge is not supported".into(),
        )),
        pg_sys::NodeTag::T_Result => {
            inspect_optional_plan((*(plan as *mut pg_sys::Result)).plan.lefttree)
        }
        pg_sys::NodeTag::T_Append => {
            let append = plan as *mut pg_sys::Append;
            inspect_plan_list((*append).appendplans)?;
            Ok(PlanMetadata {
                parallel_capable: false,
                planned_workers: 0,
            })
        }
        pg_sys::NodeTag::T_SeqScan
        | pg_sys::NodeTag::T_IndexScan
        | pg_sys::NodeTag::T_IndexOnlyScan => Ok(PlanMetadata {
            parallel_capable: false,
            planned_workers: 0,
        }),
        pg_sys::NodeTag::T_BitmapHeapScan => {
            inspect_optional_plan((*plan).lefttree)?;
            Ok(PlanMetadata {
                parallel_capable: false,
                planned_workers: 0,
            })
        }
        pg_sys::NodeTag::T_BitmapIndexScan => Ok(PlanMetadata {
            parallel_capable: false,
            planned_workers: 0,
        }),
        pg_sys::NodeTag::T_BitmapAnd => {
            inspect_plan_list((*(plan as *mut pg_sys::BitmapAnd)).bitmapplans)?;
            Ok(PlanMetadata {
                parallel_capable: false,
                planned_workers: 0,
            })
        }
        pg_sys::NodeTag::T_BitmapOr => {
            inspect_plan_list((*(plan as *mut pg_sys::BitmapOr)).bitmapplans)?;
            Ok(PlanMetadata {
                parallel_capable: false,
                planned_workers: 0,
            })
        }
        pg_sys::NodeTag::T_Limit => Err(ScanError::UnsupportedPlan(
            "SQL LIMIT must stay outside slot_scan; use local_row_cap instead".into(),
        )),
        pg_sys::NodeTag::T_Sort | pg_sys::NodeTag::T_IncrementalSort => {
            Err(ScanError::UnsupportedPlan("Sort is not supported".into()))
        }
        pg_sys::NodeTag::T_Agg | pg_sys::NodeTag::T_Group | pg_sys::NodeTag::T_WindowAgg => Err(
            ScanError::UnsupportedPlan("aggregate and grouping plans are not supported".into()),
        ),
        pg_sys::NodeTag::T_NestLoop
        | pg_sys::NodeTag::T_MergeJoin
        | pg_sys::NodeTag::T_HashJoin => {
            Err(ScanError::UnsupportedPlan("joins are not supported".into()))
        }
        other => Err(ScanError::UnsupportedPlan(format!(
            "plan node {:?} is not supported",
            other
        ))),
    }
}

unsafe fn inspect_optional_plan(plan: *mut pg_sys::Plan) -> Result<PlanMetadata, ScanError> {
    if plan.is_null() {
        Ok(PlanMetadata {
            parallel_capable: false,
            planned_workers: 0,
        })
    } else {
        inspect_plan(plan)
    }
}

unsafe fn inspect_plan_list(list: *mut pg_sys::List) -> Result<(), ScanError> {
    if list.is_null() {
        return Ok(());
    }
    for idx in 0..(*list).length {
        let plan = list_nth(list, idx) as *mut pg_sys::Plan;
        inspect_plan(plan)?;
    }
    Ok(())
}

unsafe fn list_nth(list: *mut pg_sys::List, n: i32) -> *mut std::ffi::c_void {
    debug_assert!(!list.is_null());
    debug_assert!(n >= 0 && n < (*list).length);
    (*(*list).elements.offset(n as isize)).ptr_value
}
