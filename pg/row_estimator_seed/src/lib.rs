//! PostgreSQL statistics seed bridge for `row_estimator`.
//!
//! `row_estimator_seed` turns live PostgreSQL width statistics into an initial
//! `row_estimator::EstimatorConfig` for one physical encoded page shape.
//!
//! The bridge is intentionally narrow:
//!
//! - input: relation oid, physical projected columns, and a caller default
//! - source: `pg_statistic.stawidth`
//! - output: either a seeded `EstimatorConfig` or the original default
//!
//! Missing statistics are not treated as an error. In that case the crate
//! preserves the caller default unchanged.
//!
//! PostgreSQL `name` columns are intentionally excluded from `stawidth`-based
//! seeding even though they are valid `Utf8View` sources at encode time. Their
//! heap/statistics width reflects fixed `NameData` storage, while the encoder
//! trims trailing NUL bytes and often keeps the resulting payload inline.

use std::panic::AssertUnwindSafe;

use arrow_layout::TypeTag;
use pgrx::pg_sys;
use pgrx::pg_sys::panic::CaughtError;
use pgrx::{PgRelation, PgTryBuilder};
use row_estimator::EstimatorConfig;
use thiserror::Error;

/// Source of one physical projected column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ColumnSourceRef<'a> {
    /// Physical column backed directly by one live PostgreSQL relation attribute.
    RelationAttribute(&'a str),
    /// Synthetic physical column with no backing PostgreSQL attribute.
    Synthetic,
}

/// One physical projected column relevant to row-estimator seeding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProjectedColumnRef<'a> {
    pub source: ColumnSourceRef<'a>,
    pub type_tag: TypeTag,
}

impl<'a> ProjectedColumnRef<'a> {
    /// Construct one relation-backed physical projected column.
    pub const fn relation_attribute(attribute: &'a str, type_tag: TypeTag) -> Self {
        Self {
            source: ColumnSourceRef::RelationAttribute(attribute),
            type_tag,
        }
    }

    /// Construct one synthetic physical projected column.
    pub const fn synthetic(type_tag: TypeTag) -> Self {
        Self {
            source: ColumnSourceRef::Synthetic,
            type_tag,
        }
    }
}

/// Errors returned by PostgreSQL statistics seeding.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum SeedError {
    #[error("failed to open relation oid {relation_oid}: {message}")]
    RelationOpen { relation_oid: u32, message: String },
    #[error("relation oid {relation_oid} is missing attribute {attribute}")]
    AttributeNotFound {
        relation_oid: u32,
        attribute: String,
    },
    #[error(
        "relation oid {relation_oid} attribute {attribute} type oid {actual_oid} is incompatible with layout type {expected:?}"
    )]
    TypeMismatch {
        relation_oid: u32,
        attribute: String,
        expected: TypeTag,
        actual_oid: u32,
    },
}

/// Seed an initial estimator configuration from PostgreSQL width statistics.
///
/// For each projected physical `Utf8View` / `BinaryView` column backed by a
/// relation attribute, the function looks up `pg_statistic.stawidth` when that
/// statistic is a meaningful proxy for encoded tail usage.
///
/// If every such variable-width column has a positive width statistic, the sum
/// becomes `initial_tail_bytes_per_row`. Otherwise `default` is returned
/// unchanged.
pub fn seed_estimator_config(
    relation_oid: pg_sys::Oid,
    columns: &[ProjectedColumnRef<'_>],
    default: EstimatorConfig,
) -> Result<EstimatorConfig, SeedError> {
    match seed_tail_bytes_per_row(relation_oid, columns)? {
        Some(initial_tail_bytes_per_row) => Ok(EstimatorConfig {
            initial_tail_bytes_per_row,
            ..default
        }),
        None => Ok(default),
    }
}

fn seed_tail_bytes_per_row(
    relation_oid: pg_sys::Oid,
    columns: &[ProjectedColumnRef<'_>],
) -> Result<Option<u32>, SeedError> {
    if !columns.iter().any(|column| column.type_tag.is_view()) {
        return Ok(None);
    }
    if columns.iter().any(|column| {
        column.type_tag.is_view() && matches!(column.source, ColumnSourceRef::Synthetic)
    }) {
        return Ok(None);
    }

    with_locked_relation(relation_oid, |relation| {
        let tuple_desc = relation.tuple_desc();
        let relation_oid = relation.oid().to_u32();
        let mut saw_variable_width = false;
        let mut width_sum = 0u64;

        for column in columns {
            if !column.type_tag.is_view() {
                continue;
            }
            saw_variable_width = true;

            let attribute_name = match column.source {
                ColumnSourceRef::RelationAttribute(attribute_name) => attribute_name,
                ColumnSourceRef::Synthetic => return Ok(None),
            };

            let attribute = tuple_desc
                .iter()
                .find(|attribute| !attribute.is_dropped() && attribute.name() == attribute_name)
                .ok_or_else(|| SeedError::AttributeNotFound {
                    relation_oid,
                    attribute: attribute_name.to_owned(),
                })?;

            validate_attribute_type(
                relation_oid,
                attribute_name,
                column.type_tag,
                attribute.atttypid,
            )?;
            if !has_usable_stawidth_seed(attribute.atttypid, column.type_tag) {
                return Ok(None);
            }

            let width = lookup_positive_stawidth(relation.oid(), attribute.num(), true)
                .or_else(|| lookup_positive_stawidth(relation.oid(), attribute.num(), false));
            let Some(width) = width else {
                return Ok(None);
            };
            width_sum = width_sum.saturating_add(u64::from(width));
        }

        if !saw_variable_width {
            return Ok(None);
        }

        Ok(Some(width_sum.min(u64::from(u32::MAX)) as u32))
    })
}

fn with_locked_relation<T, F>(relation_oid: pg_sys::Oid, f: F) -> Result<T, SeedError>
where
    F: FnOnce(&PgRelation) -> Result<T, SeedError>,
{
    let relation_oid_u32 = relation_oid.to_u32();
    PgTryBuilder::new(AssertUnwindSafe(|| unsafe {
        let relation = PgRelation::with_lock(relation_oid, pg_sys::AccessShareLock as _);
        f(&relation)
    }))
    .catch_others(|error| {
        Err(SeedError::RelationOpen {
            relation_oid: relation_oid_u32,
            message: caught_error_message(error),
        })
    })
    .execute()
}

fn validate_attribute_type(
    relation_oid: u32,
    attribute_name: &str,
    type_tag: TypeTag,
    actual_oid: pg_sys::Oid,
) -> Result<(), SeedError> {
    if type_matches_layout(actual_oid, type_tag) {
        Ok(())
    } else {
        Err(SeedError::TypeMismatch {
            relation_oid,
            attribute: attribute_name.to_owned(),
            expected: type_tag,
            actual_oid: actual_oid.to_u32(),
        })
    }
}

fn type_matches_layout(actual_oid: pg_sys::Oid, type_tag: TypeTag) -> bool {
    match type_tag {
        TypeTag::Utf8View => {
            actual_oid == pg_sys::TEXTOID
                || actual_oid == pg_sys::VARCHAROID
                || actual_oid == pg_sys::BPCHAROID
                || actual_oid == pg_sys::NAMEOID
        }
        TypeTag::BinaryView => actual_oid == pg_sys::BYTEAOID,
        _ => false,
    }
}

fn has_usable_stawidth_seed(actual_oid: pg_sys::Oid, type_tag: TypeTag) -> bool {
    match type_tag {
        TypeTag::Utf8View => {
            actual_oid == pg_sys::TEXTOID
                || actual_oid == pg_sys::VARCHAROID
                || actual_oid == pg_sys::BPCHAROID
        }
        TypeTag::BinaryView => actual_oid == pg_sys::BYTEAOID,
        _ => false,
    }
}

fn lookup_positive_stawidth(
    relation_oid: pg_sys::Oid,
    attribute_num: i16,
    stainherit: bool,
) -> Option<u32> {
    unsafe {
        let tuple = pg_sys::SearchSysCache3(
            pg_sys::SysCacheIdentifier::STATRELATTINH as i32,
            pg_sys::Datum::from(relation_oid.to_u32()),
            pg_sys::Datum::from(attribute_num),
            pg_sys::Datum::from(stainherit),
        );
        if tuple.is_null() {
            return None;
        }

        let stats = pg_sys::GETSTRUCT(tuple) as pg_sys::Form_pg_statistic;
        let width = (*stats).stawidth;
        pg_sys::ReleaseSysCache(tuple);

        (width > 0).then_some(width as u32)
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

#[cfg(test)]
fn sum_positive_widths(widths: impl IntoIterator<Item = Option<u32>>) -> Option<u32> {
    let mut total = 0u64;
    for width in widths {
        total = total.checked_add(u64::from(width?)).unwrap_or(u64::MAX);
    }
    Some(total.min(u64::from(u32::MAX)) as u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sum_positive_widths_returns_sum_when_all_present() {
        assert_eq!(sum_positive_widths([Some(7), Some(11), Some(5)]), Some(23));
    }

    #[test]
    fn sum_positive_widths_rejects_missing_width() {
        assert_eq!(sum_positive_widths([Some(7), None, Some(5)]), None);
    }

    #[test]
    fn sum_positive_widths_clamps_overflow() {
        assert_eq!(
            sum_positive_widths([Some(u32::MAX), Some(1)]),
            Some(u32::MAX)
        );
    }

    #[test]
    fn type_matches_layout_accepts_text_like_utf8() {
        assert!(type_matches_layout(pg_sys::TEXTOID, TypeTag::Utf8View));
        assert!(type_matches_layout(pg_sys::VARCHAROID, TypeTag::Utf8View));
        assert!(type_matches_layout(pg_sys::BPCHAROID, TypeTag::Utf8View));
        assert!(type_matches_layout(pg_sys::NAMEOID, TypeTag::Utf8View));
    }

    #[test]
    fn type_matches_layout_accepts_bytea_binary() {
        assert!(type_matches_layout(pg_sys::BYTEAOID, TypeTag::BinaryView));
    }

    #[test]
    fn type_matches_layout_rejects_non_matching_types() {
        assert!(!type_matches_layout(pg_sys::INT4OID, TypeTag::Utf8View));
        assert!(!type_matches_layout(pg_sys::TEXTOID, TypeTag::BinaryView));
        assert!(!type_matches_layout(pg_sys::TEXTOID, TypeTag::Int32));
    }

    #[test]
    fn usable_stawidth_seed_excludes_name_columns() {
        assert!(has_usable_stawidth_seed(pg_sys::TEXTOID, TypeTag::Utf8View));
        assert!(has_usable_stawidth_seed(
            pg_sys::VARCHAROID,
            TypeTag::Utf8View
        ));
        assert!(has_usable_stawidth_seed(
            pg_sys::BPCHAROID,
            TypeTag::Utf8View
        ));
        assert!(has_usable_stawidth_seed(
            pg_sys::BYTEAOID,
            TypeTag::BinaryView
        ));
        assert!(!has_usable_stawidth_seed(
            pg_sys::NAMEOID,
            TypeTag::Utf8View
        ));
    }

    #[test]
    fn seed_returns_default_when_no_variable_width_columns() {
        let default = EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        };
        let columns = [ProjectedColumnRef::relation_attribute("id", TypeTag::Int64)];
        assert_eq!(
            seed_estimator_config(pg_sys::InvalidOid, &columns, default).expect("seed"),
            default
        );
    }

    #[test]
    fn seed_returns_default_for_synthetic_variable_width_columns() {
        let default = EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        };
        let columns = [ProjectedColumnRef::synthetic(TypeTag::Utf8View)];
        assert_eq!(
            seed_estimator_config(pg_sys::InvalidOid, &columns, default).expect("seed"),
            default
        );
    }
}
