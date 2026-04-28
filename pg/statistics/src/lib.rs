//! PostgreSQL planner and catalog statistics bridge.
//!
//! This crate is PostgreSQL-specific but engine-agnostic: it does not depend on
//! DataFusion or `join_order`. Callers use it to turn live PostgreSQL planner
//! and catalog state into numeric inputs for higher-level planning.

use std::cell::Cell;
use std::ffi::{CStr, CString};
use std::panic::AssertUnwindSafe;

use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::{PgRelation, PgTryBuilder};
use thiserror::Error;

const DEFAULT_EQUI_JOIN_SELECTIVITY: f64 = 0.01;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EstimateOptions {
    pub parallel_ok: bool,
    pub fast_start: bool,
}

impl Default for EstimateOptions {
    fn default() -> Self {
        Self {
            parallel_ok: true,
            fast_start: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PgScanEstimate {
    pub rows: f64,
    pub width: i32,
    pub bytes: f64,
    pub startup_cost: f64,
    pub total_cost: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PgRelationStats {
    pub relation_oid: u32,
    pub rows: Option<f64>,
    pub pages: Option<i32>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PgColumnStats {
    pub relation_oid: u32,
    pub attnum: i16,
    pub inherited: bool,
    pub null_frac: Option<f64>,
    pub avg_width: Option<i32>,
    pub stadistinct: Option<f64>,
    pub ndv: Option<f64>,
}

impl PgColumnStats {
    pub fn effective_ndv(&self, filtered_rows: f64) -> Option<f64> {
        effective_ndv(self.ndv, filtered_rows)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PgUniqueKey {
    pub relation_oid: u32,
    pub index_oid: u32,
    pub attnums: Vec<i16>,
    pub primary: bool,
    pub nulls_not_distinct: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EquiJoinInput {
    pub left_rows: f64,
    pub right_rows: f64,
    pub left_ndv: Option<f64>,
    pub right_ndv: Option<f64>,
    pub left_null_frac: Option<f64>,
    pub right_null_frac: Option<f64>,
    pub left_unique: bool,
    pub right_unique: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EstimateQuality {
    Full,
    Partial,
    Fallback,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct EquiJoinEstimate {
    pub selectivity: f64,
    pub rows: f64,
    pub quality: EstimateQuality,
}

#[derive(Debug, Error, PartialEq)]
pub enum StatisticsError {
    #[error("SQL contains an interior NUL byte")]
    InvalidSql,
    #[error("{label} failed with status {code}: {message}")]
    SpiStatus {
        label: &'static str,
        code: i32,
        message: String,
    },
    #[error("prepared SQL must produce tuples for a cursor scan")]
    NotCursorPlan,
    #[error("expected exactly one planned statement, got {count}")]
    MultipleStatements { count: usize },
    #[error("only SELECT statements are supported")]
    NonSelectStatement,
    #[error("cached plan is missing a plan tree")]
    MissingPlanTree,
    #[error("failed to open relation oid {relation_oid}: {message}")]
    RelationOpen { relation_oid: u32, message: String },
    #[error("relation oid {relation_oid} was not found")]
    RelationNotFound { relation_oid: u32 },
    #[error("relation oid {relation_oid} attribute {attnum} was not found")]
    AttributeNotFound { relation_oid: u32, attnum: i16 },
    #[error("relation oid {relation_oid} attribute {attnum} is dropped")]
    AttributeDropped { relation_oid: u32, attnum: i16 },
    #[error("PostgreSQL error: {0}")]
    Postgres(String),
}

pub fn estimate_scan_sql(
    sql: &str,
    options: EstimateOptions,
) -> Result<PgScanEstimate, StatisticsError> {
    let c_sql = CString::new(sql).map_err(|_| StatisticsError::InvalidSql)?;

    with_spi(|| unsafe {
        let plan = prepare_cursor_plan(&c_sql, options)?;
        let estimate = (|| -> Result<PgScanEstimate, StatisticsError> {
            if !pg_sys::SPI_is_cursor_plan(plan) {
                return Err(StatisticsError::NotCursorPlan);
            }
            with_cached_single_planned_stmt(plan, |planned_stmt| {
                estimate_planned_stmt(planned_stmt)
            })
        })();

        let free_rc = pg_sys::SPI_freeplan(plan);
        if estimate.is_ok() && free_rc != 0 {
            return Err(spi_status_error("SPI_freeplan", free_rc));
        }

        estimate
    })
}

pub fn load_relation_stats(relation_oid: pg_sys::Oid) -> Result<PgRelationStats, StatisticsError> {
    let relation_oid_u32 = relation_oid.to_u32();
    unsafe {
        let tuple = pg_sys::SearchSysCache1(
            pg_sys::SysCacheIdentifier::RELOID as i32,
            pg_sys::Datum::from(relation_oid_u32),
        );
        if tuple.is_null() {
            return Err(StatisticsError::RelationNotFound {
                relation_oid: relation_oid_u32,
            });
        }

        let class = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_class;
        let rows = ((*class).reltuples >= 0.0).then_some((*class).reltuples as f64);
        let pages = ((*class).relpages >= 0).then_some((*class).relpages);
        pg_sys::ReleaseSysCache(tuple);

        Ok(PgRelationStats {
            relation_oid: relation_oid_u32,
            rows,
            pages,
        })
    }
}

pub fn load_column_stats(
    relation_oid: pg_sys::Oid,
    attnums: &[i16],
) -> Result<Vec<PgColumnStats>, StatisticsError> {
    let relation_stats = load_relation_stats(relation_oid)?;
    attnums
        .iter()
        .copied()
        .map(|attnum| load_one_column_stats(relation_oid, relation_stats.rows, attnum))
        .collect()
}

pub fn load_unique_keys(relation_oid: pg_sys::Oid) -> Result<Vec<PgUniqueKey>, StatisticsError> {
    with_locked_relation(relation_oid, |relation| unsafe {
        let relation_oid_u32 = relation.oid().to_u32();
        let index_list = pg_sys::RelationGetIndexList(relation.as_ptr());
        if index_list.is_null() {
            return Ok(Vec::new());
        }

        let mut keys = Vec::new();
        for index in 0..(*index_list).length {
            let index_oid = list_nth_oid(index_list, index);
            let tuple = pg_sys::SearchSysCache1(
                pg_sys::SysCacheIdentifier::INDEXRELID as i32,
                pg_sys::Datum::from(index_oid.to_u32()),
            );
            if tuple.is_null() {
                continue;
            }

            let form = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_index;
            if (*form).indisunique
                && (*form).indisvalid
                && (*form).indislive
                && !index_has_predicate(tuple)
                && (*form).indnkeyatts > 0
            {
                let attnums = int2vector_values(&(*form).indkey, (*form).indnkeyatts as usize);
                if !attnums.is_empty() && attnums.iter().all(|attnum| *attnum > 0) {
                    keys.push(PgUniqueKey {
                        relation_oid: relation_oid_u32,
                        index_oid: index_oid.to_u32(),
                        attnums,
                        primary: (*form).indisprimary,
                        nulls_not_distinct: (*form).indnullsnotdistinct,
                    });
                }
            }

            pg_sys::ReleaseSysCache(tuple);
        }

        Ok(keys)
    })
}

pub fn normalize_stadistinct(stadistinct: f64, base_rows: Option<f64>) -> Option<f64> {
    if !stadistinct.is_finite() || stadistinct == 0.0 {
        return None;
    }
    if stadistinct > 0.0 {
        Some(stadistinct)
    } else {
        let base_rows = sanitize_rows(base_rows?);
        (base_rows > 0.0).then_some((-stadistinct * base_rows).max(1.0))
    }
}

pub fn effective_ndv(base_ndv: Option<f64>, filtered_rows: f64) -> Option<f64> {
    let ndv = base_ndv?;
    if !ndv.is_finite() || ndv <= 0.0 {
        return None;
    }
    let rows = sanitize_rows(filtered_rows);
    if rows <= 0.0 {
        Some(0.0)
    } else {
        Some(ndv.min(rows).max(1.0))
    }
}

pub fn estimate_equi_join_selectivity(input: EquiJoinInput) -> EquiJoinEstimate {
    let left_rows = sanitize_rows(input.left_rows);
    let right_rows = sanitize_rows(input.right_rows);
    let possible_rows = left_rows * right_rows;
    if possible_rows <= 0.0 {
        return EquiJoinEstimate {
            selectivity: 0.0,
            rows: 0.0,
            quality: EstimateQuality::Full,
        };
    }

    let left_nonnull = nonnull_rows(left_rows, input.left_null_frac);
    let right_nonnull = nonnull_rows(right_rows, input.right_null_frac);
    let left_ndv = effective_ndv(input.left_ndv, left_nonnull);
    let right_ndv = effective_ndv(input.right_ndv, right_nonnull);

    let (mut rows, quality) = match (left_ndv, right_ndv) {
        (Some(left_ndv), Some(right_ndv)) if left_ndv > 0.0 && right_ndv > 0.0 => (
            left_nonnull * right_nonnull / left_ndv.max(right_ndv),
            EstimateQuality::Full,
        ),
        (Some(ndv), None) | (None, Some(ndv)) if ndv > 0.0 => {
            (left_nonnull * right_nonnull / ndv, EstimateQuality::Partial)
        }
        _ => (
            left_nonnull * right_nonnull * DEFAULT_EQUI_JOIN_SELECTIVITY,
            EstimateQuality::Fallback,
        ),
    };

    if input.right_unique {
        rows = rows.min(left_nonnull);
    }
    if input.left_unique {
        rows = rows.min(right_nonnull);
    }
    if input.left_unique && input.right_unique {
        rows = rows.min(left_nonnull.min(right_nonnull));
    }
    rows = rows.clamp(0.0, possible_rows);

    EquiJoinEstimate {
        selectivity: rows / possible_rows,
        rows,
        quality,
    }
}

fn load_one_column_stats(
    relation_oid: pg_sys::Oid,
    base_rows: Option<f64>,
    attnum: i16,
) -> Result<PgColumnStats, StatisticsError> {
    validate_attribute(relation_oid, attnum)?;

    let inherited = lookup_statistic(relation_oid, attnum, true);
    let (inherited, stat) = match inherited {
        Some(stat) => (true, Some(stat)),
        None => (false, lookup_statistic(relation_oid, attnum, false)),
    };

    let relation_oid_u32 = relation_oid.to_u32();
    let Some(stat) = stat else {
        return Ok(PgColumnStats {
            relation_oid: relation_oid_u32,
            attnum,
            inherited: false,
            null_frac: None,
            avg_width: None,
            stadistinct: None,
            ndv: None,
        });
    };

    Ok(PgColumnStats {
        relation_oid: relation_oid_u32,
        attnum,
        inherited,
        null_frac: Some(stat.null_frac),
        avg_width: Some(stat.avg_width),
        stadistinct: Some(stat.stadistinct),
        ndv: normalize_stadistinct(stat.stadistinct, base_rows),
    })
}

#[derive(Clone, Copy, Debug)]
struct RawColumnStats {
    null_frac: f64,
    avg_width: i32,
    stadistinct: f64,
}

fn lookup_statistic(
    relation_oid: pg_sys::Oid,
    attnum: i16,
    inherited: bool,
) -> Option<RawColumnStats> {
    unsafe {
        let tuple = pg_sys::SearchSysCache3(
            pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
            pg_sys::Datum::from(relation_oid.to_u32()),
            pg_sys::Datum::from(attnum),
            pg_sys::Datum::from(inherited),
        );
        if tuple.is_null() {
            return None;
        }

        let stats = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_statistic;
        let result = RawColumnStats {
            null_frac: (*stats).stanullfrac as f64,
            avg_width: (*stats).stawidth,
            stadistinct: (*stats).stadistinct as f64,
        };
        pg_sys::ReleaseSysCache(tuple);
        Some(result)
    }
}

fn validate_attribute(relation_oid: pg_sys::Oid, attnum: i16) -> Result<(), StatisticsError> {
    let relation_oid_u32 = relation_oid.to_u32();
    unsafe {
        let tuple = pg_sys::SearchSysCacheAttNum(relation_oid, attnum);
        if tuple.is_null() {
            return Err(StatisticsError::AttributeNotFound {
                relation_oid: relation_oid_u32,
                attnum,
            });
        }

        let attribute = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_attribute;
        let is_dropped = (*attribute).attisdropped;
        pg_sys::ReleaseSysCache(tuple);

        if is_dropped {
            Err(StatisticsError::AttributeDropped {
                relation_oid: relation_oid_u32,
                attnum,
            })
        } else {
            Ok(())
        }
    }
}

fn estimate_planned_stmt(
    planned_stmt: *mut pg_sys::PlannedStmt,
) -> Result<PgScanEstimate, StatisticsError> {
    unsafe {
        if (*planned_stmt).commandType != pg_sys::CmdType::CMD_SELECT {
            return Err(StatisticsError::NonSelectStatement);
        }
        estimate_plan_tree((*planned_stmt).planTree)
    }
}

fn estimate_plan_tree(plan: *mut pg_sys::Plan) -> Result<PgScanEstimate, StatisticsError> {
    unsafe {
        if plan.is_null() {
            return Err(StatisticsError::MissingPlanTree);
        }

        let rows = sanitize_rows((*plan).plan_rows);
        let width = (*plan).plan_width.max(0);
        Ok(PgScanEstimate {
            rows,
            width,
            bytes: rows * f64::from(width),
            startup_cost: (*plan).startup_cost,
            total_cost: (*plan).total_cost,
        })
    }
}

fn with_spi<T>(f: impl FnOnce() -> Result<T, StatisticsError>) -> Result<T, StatisticsError> {
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
    .catch_others(|error| Err(StatisticsError::Postgres(caught_error_message(error))))
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

unsafe fn prepare_cursor_plan(
    sql: &CStr,
    options: EstimateOptions,
) -> Result<pg_sys::SPIPlanPtr, StatisticsError> {
    let mut cursor_options = 0;
    if options.parallel_ok {
        cursor_options |= pg_sys::CURSOR_OPT_PARALLEL_OK as i32;
    }
    if options.fast_start {
        cursor_options |= pg_sys::CURSOR_OPT_FAST_PLAN as i32;
    }

    let plan = pg_sys::SPI_prepare_cursor(sql.as_ptr(), 0, std::ptr::null_mut(), cursor_options);
    if plan.is_null() {
        Err(spi_status_error("SPI_prepare_cursor", pg_sys::SPI_result))
    } else {
        Ok(plan)
    }
}

unsafe fn with_cached_single_planned_stmt<T>(
    plan: pg_sys::SPIPlanPtr,
    f: impl FnOnce(*mut pg_sys::PlannedStmt) -> Result<T, StatisticsError>,
) -> Result<T, StatisticsError> {
    let plan_sources = pg_sys::SPI_plan_get_plan_sources(plan);
    if plan_sources.is_null() {
        return Err(StatisticsError::MultipleStatements { count: 0 });
    }
    if (*plan_sources).length != 1 {
        return Err(StatisticsError::MultipleStatements {
            count: (*plan_sources).length as usize,
        });
    }

    let cached_plan = pg_sys::SPI_plan_get_cached_plan(plan);
    if cached_plan.is_null() {
        return Err(StatisticsError::Postgres(
            "SPI_plan_get_cached_plan returned null".into(),
        ));
    }

    let result = (|| -> Result<T, StatisticsError> {
        let stmt_list = (*cached_plan).stmt_list;
        if stmt_list.is_null() {
            return Err(StatisticsError::MultipleStatements { count: 0 });
        }
        if (*stmt_list).length != 1 {
            return Err(StatisticsError::MultipleStatements {
                count: (*stmt_list).length as usize,
            });
        }

        let planned_stmt = list_nth(stmt_list, 0) as *mut pg_sys::PlannedStmt;
        if planned_stmt.is_null() {
            return Err(StatisticsError::MissingPlanTree);
        }

        f(planned_stmt)
    })();

    pg_sys::ReleaseCachedPlan(cached_plan, std::ptr::null_mut());
    result
}

fn with_locked_relation<T, F>(relation_oid: pg_sys::Oid, f: F) -> Result<T, StatisticsError>
where
    F: FnOnce(&PgRelation) -> Result<T, StatisticsError>,
{
    let relation_oid_u32 = relation_oid.to_u32();
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let relation = PgRelation::with_lock(relation_oid, pg_sys::AccessShareLock as _);
        f(&relation)
    }))
    .catch_others(|error| {
        Err(StatisticsError::RelationOpen {
            relation_oid: relation_oid_u32,
            message: caught_error_message(error),
        })
    })
    .execute()
}

fn spi_status_error(label: &'static str, code: i32) -> StatisticsError {
    unsafe {
        let message = pg_sys::SPI_result_code_string(code);
        let message = if message.is_null() {
            String::new()
        } else {
            CStr::from_ptr(message).to_string_lossy().into_owned()
        };
        StatisticsError::SpiStatus {
            label,
            code,
            message,
        }
    }
}

fn caught_error_message(error: CaughtError) -> String {
    match error {
        CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        } => report.message().to_owned(),
    }
}

unsafe fn list_nth(list: *mut pg_sys::List, n: i32) -> *mut std::ffi::c_void {
    debug_assert!(!list.is_null());
    debug_assert!(n >= 0 && n < (*list).length);
    (*(*list).elements.offset(n as isize)).ptr_value
}

unsafe fn list_nth_oid(list: *mut pg_sys::List, n: i32) -> pg_sys::Oid {
    (*(*list).elements.offset(n as isize)).oid_value
}

unsafe fn index_has_predicate(tuple: pg_sys::HeapTuple) -> bool {
    let mut is_null = false;
    pg_sys::SysCacheGetAttr(
        pg_sys::SysCacheIdentifier::INDEXRELID as i32,
        tuple,
        pg_sys::Anum_pg_index_indpred as _,
        &mut is_null,
    );
    !is_null
}

unsafe fn int2vector_values(vector: &pg_sys::int2vector, len: usize) -> Vec<i16> {
    std::slice::from_raw_parts(vector.values.as_ptr(), len).to_vec()
}

fn sanitize_rows(rows: f64) -> f64 {
    if rows.is_finite() && rows > 0.0 {
        rows
    } else {
        0.0
    }
}

fn nonnull_rows(rows: f64, null_frac: Option<f64>) -> f64 {
    let null_frac = null_frac
        .filter(|value| value.is_finite())
        .unwrap_or(0.0)
        .clamp(0.0, 1.0);
    rows * (1.0 - null_frac)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_stadistinct_accepts_positive_exact_ndv() {
        assert_eq!(normalize_stadistinct(42.0, Some(1_000.0)), Some(42.0));
    }

    #[test]
    fn normalize_stadistinct_expands_negative_fraction() {
        assert_eq!(normalize_stadistinct(-0.25, Some(1_000.0)), Some(250.0));
    }

    #[test]
    fn normalize_stadistinct_rejects_zero_and_missing_base_rows() {
        assert_eq!(normalize_stadistinct(0.0, Some(1_000.0)), None);
        assert_eq!(normalize_stadistinct(-0.25, None), None);
    }

    #[test]
    fn effective_ndv_clamps_to_filtered_rows() {
        assert_eq!(effective_ndv(Some(1_000.0), 10.0), Some(10.0));
        assert_eq!(effective_ndv(Some(5.0), 10.0), Some(5.0));
    }

    #[test]
    fn estimate_equi_join_selectivity_uses_max_ndv() {
        let estimate = estimate_equi_join_selectivity(EquiJoinInput {
            left_rows: 1_000_000.0,
            right_rows: 100.0,
            left_ndv: Some(1_000_000.0),
            right_ndv: Some(100.0),
            left_null_frac: None,
            right_null_frac: None,
            left_unique: false,
            right_unique: true,
        });

        assert_eq!(estimate.quality, EstimateQuality::Full);
        assert!((estimate.rows - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn estimate_equi_join_selectivity_removes_nulls() {
        let estimate = estimate_equi_join_selectivity(EquiJoinInput {
            left_rows: 100.0,
            right_rows: 100.0,
            left_ndv: Some(100.0),
            right_ndv: Some(100.0),
            left_null_frac: Some(0.5),
            right_null_frac: Some(0.0),
            left_unique: false,
            right_unique: false,
        });

        assert!((estimate.rows - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn estimate_equi_join_selectivity_falls_back_without_ndv() {
        let estimate = estimate_equi_join_selectivity(EquiJoinInput {
            left_rows: 100.0,
            right_rows: 100.0,
            left_ndv: None,
            right_ndv: None,
            left_null_frac: None,
            right_null_frac: None,
            left_unique: false,
            right_unique: false,
        });

        assert_eq!(estimate.quality, EstimateQuality::Fallback);
        assert!((estimate.rows - 100.0).abs() < f64::EPSILON);
    }
}
