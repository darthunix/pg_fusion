//! Adaptive rows-per-page estimator for physical `arrow_layout` blocks.
//!
//! `row_estimator` helps producer-side code choose `max_rows` for the next
//! page before writing tuples into `arrow_layout`.
//!
//! The crate is intentionally generic at the layout layer:
//!
//! - input: `arrow_layout::ColumnSpec` plus `block_size`
//! - state: caller-owned estimator instance, typically one per scan
//! - feedback: completed page observations expressed as total rows plus total
//!   shared-tail bytes used
//!
//! It operates on the physical page shape that will actually be encoded. If an
//! upstream logical scan has an empty projection, the caller must first lower
//! that scan into at least one physical column (for example a synthetic fixed-
//! width sentinel) before constructing [`PageRowEstimator`].
//!
//! For fixed-width-only layouts the estimate is exact. For layouts with
//! `Utf8View` / `BinaryView`, the estimator starts from a configurable initial
//! tail-bytes-per-row prior and then adapts from observed pages.
//!
//! A fresh page that yields `AppendResult { rows_written: 0, full: true }`
//! from `batch_encoder::BatchPageEncoder::append_batch()` is not a normal page
//! observation. Callers must report that case through
//! [`PageRowEstimator::observe_empty_full_page`] so the estimator can back off
//! the next `rows_per_page` guess.

use arrow_layout::{BlockRef, ColumnSpec, LayoutError, LayoutPlan};

/// Configuration for one estimator instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EstimatorConfig {
    /// Initial expected shared-tail bytes per row before any page observations.
    pub initial_tail_bytes_per_row: u32,
}

impl Default for EstimatorConfig {
    fn default() -> Self {
        Self {
            initial_tail_bytes_per_row: 64,
        }
    }
}

/// One completed-page observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageTailObservation {
    /// Number of committed rows in the page.
    pub row_count: u32,
    /// Number of shared-tail bytes used by that page.
    pub tail_bytes_used: u32,
}

/// Current rows-per-page estimate for the next block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PageRowEstimate {
    /// Recommended `max_rows` for the next page.
    pub rows_per_page: u32,
    /// Exact front-region-only upper bound derived from `arrow_layout`.
    pub fixed_row_cap: u32,
    /// Current estimated shared-tail bytes per row.
    pub estimated_tail_bytes_per_row: u32,
    /// Expected shared-tail bytes for `rows_per_page`.
    pub expected_tail_bytes: u32,
}

/// Errors returned by [`PageRowEstimator`].
#[derive(Debug, thiserror::Error)]
pub enum EstimateError {
    #[error("row estimator requires at least one physical column in the encoded page shape")]
    NoPhysicalColumns,
    #[error("page layout with block_size {block_size} cannot fit even one row")]
    LayoutCannotFitAnyRows { block_size: u32 },
    #[error(transparent)]
    Layout(#[from] LayoutError),
    #[error("observed row_count {row_count} exceeds exact fixed row cap {fixed_row_cap}")]
    ObservationRowsExceedFixedCap { row_count: u32, fixed_row_cap: u32 },
    #[error(
        "observed tail_bytes_used {tail_bytes_used} exceeds shared tail capacity {shared_pool_capacity} for row_count {row_count}"
    )]
    ObservationTailTooLarge {
        row_count: u32,
        tail_bytes_used: u32,
        shared_pool_capacity: u32,
    },
    #[error("observed counters overflowed while accumulating page statistics")]
    ObservationOverflow,
    #[error("observed block size {actual} does not match estimator block size {expected}")]
    BlockSizeMismatch { expected: u32, actual: u32 },
    #[error(
        "observed block column count {actual} does not match estimator column count {expected}"
    )]
    ColumnCountMismatch { expected: usize, actual: usize },
    #[error(
        "observed block column {index} mismatch: expected type {expected_type:?} nullable={expected_nullable}, got type {actual_type:?} nullable={actual_nullable}"
    )]
    ColumnMismatch {
        index: usize,
        expected_type: arrow_layout::TypeTag,
        actual_type: arrow_layout::TypeTag,
        expected_nullable: bool,
        actual_nullable: bool,
    },
    #[error("empty-full backoff requires at least one variable-width column")]
    EmptyFullBackoffRequiresVariableWidth,
    #[error("empty-full backoff attempted_rows {attempted_rows} is outside 1..={fixed_row_cap}")]
    InvalidEmptyFullAttempt {
        attempted_rows: u32,
        fixed_row_cap: u32,
    },
    #[error("single row does not fit into page block_size {block_size}")]
    SingleRowDoesNotFitPage { block_size: u32 },
    #[error("internal estimator arithmetic overflowed")]
    ArithmeticOverflow,
}

/// Per-scan adaptive rows-per-page estimator.
#[derive(Clone, Debug)]
pub struct PageRowEstimator {
    specs: Vec<ColumnSpec>,
    block_size: u32,
    fixed_row_cap: u32,
    has_variable_width: bool,
    config: EstimatorConfig,
    observed_rows: u64,
    observed_tail_bytes: u64,
    backoff_row_cap: Option<u32>,
}

impl PageRowEstimator {
    /// Create a new estimator for one physical page layout shape and block size.
    ///
    /// The estimator works on the encoded page shape, not on a higher-level
    /// logical projection. Callers must materialize any synthetic physical
    /// column needed for dummy-projection or row-count-only scans before
    /// invoking this constructor.
    pub fn new(
        specs: &[ColumnSpec],
        block_size: u32,
        config: EstimatorConfig,
    ) -> Result<Self, EstimateError> {
        if specs.is_empty() {
            return Err(EstimateError::NoPhysicalColumns);
        }

        let fixed_row_cap = compute_fixed_row_cap(specs, block_size)?;
        if fixed_row_cap == 0 {
            return Err(EstimateError::LayoutCannotFitAnyRows { block_size });
        }
        let has_variable_width = specs.iter().any(|spec| spec.type_tag.is_view());

        Ok(Self {
            specs: specs.to_vec(),
            block_size,
            fixed_row_cap,
            has_variable_width,
            config,
            observed_rows: 0,
            observed_tail_bytes: 0,
            backoff_row_cap: None,
        })
    }

    /// Return the block size this estimator targets.
    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    /// Return the exact front-region-only row cap for this layout.
    pub fn fixed_row_cap(&self) -> u32 {
        self.fixed_row_cap
    }

    /// Return whether this layout contains any variable-width view columns.
    pub fn has_variable_width(&self) -> bool {
        self.has_variable_width
    }

    /// Estimate the recommended row count for the next page.
    pub fn estimate(&self) -> Result<PageRowEstimate, EstimateError> {
        let estimated_tail_bytes_per_row = self.estimated_tail_bytes_per_row();

        if !self.has_variable_width {
            return Ok(PageRowEstimate {
                rows_per_page: self.fixed_row_cap,
                fixed_row_cap: self.fixed_row_cap,
                estimated_tail_bytes_per_row: 0,
                expected_tail_bytes: 0,
            });
        }

        let (tail_num, tail_den) = self.estimated_tail_ratio();
        let mut low = 0u32;
        let mut high = self.backoff_row_cap.unwrap_or(self.fixed_row_cap);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            let plan = LayoutPlan::new(&self.specs, mid, self.block_size)?;
            let expected_tail_bytes = ceil_div_u64(
                u64::from(mid)
                    .checked_mul(tail_num)
                    .ok_or(EstimateError::ArithmeticOverflow)?,
                tail_den,
            )?;
            if expected_tail_bytes <= u64::from(plan.shared_pool_capacity()) {
                low = mid;
            } else {
                high = mid - 1;
            }
        }

        let rows_per_page = low.max(1);
        let expected_tail_bytes = if low == 0 {
            ceil_div_u64(tail_num, tail_den)?
        } else {
            ceil_div_u64(
                u64::from(rows_per_page)
                    .checked_mul(tail_num)
                    .ok_or(EstimateError::ArithmeticOverflow)?,
                tail_den,
            )?
        };

        Ok(PageRowEstimate {
            rows_per_page,
            fixed_row_cap: self.fixed_row_cap,
            estimated_tail_bytes_per_row,
            expected_tail_bytes: u32::try_from(expected_tail_bytes)
                .map_err(|_| EstimateError::ArithmeticOverflow)?,
        })
    }

    /// Incorporate one non-empty completed page observation.
    ///
    /// `row_count == 0` is ignored. Callers must report the distinct "fresh
    /// page returned `Full` before the first row fit" case through
    /// [`Self::observe_empty_full_page`].
    pub fn observe_page(&mut self, observation: PageTailObservation) -> Result<(), EstimateError> {
        if observation.row_count == 0 || !self.has_variable_width {
            return Ok(());
        }
        if observation.row_count > self.fixed_row_cap {
            return Err(EstimateError::ObservationRowsExceedFixedCap {
                row_count: observation.row_count,
                fixed_row_cap: self.fixed_row_cap,
            });
        }

        let plan = LayoutPlan::new(&self.specs, observation.row_count, self.block_size)?;
        let capacity = plan.shared_pool_capacity();
        if observation.tail_bytes_used > capacity {
            return Err(EstimateError::ObservationTailTooLarge {
                row_count: observation.row_count,
                tail_bytes_used: observation.tail_bytes_used,
                shared_pool_capacity: capacity,
            });
        }

        self.observed_rows = self
            .observed_rows
            .checked_add(u64::from(observation.row_count))
            .ok_or(EstimateError::ObservationOverflow)?;
        self.observed_tail_bytes = self
            .observed_tail_bytes
            .checked_add(u64::from(observation.tail_bytes_used))
            .ok_or(EstimateError::ObservationOverflow)?;
        self.backoff_row_cap = None;
        Ok(())
    }

    /// Report that a fresh page returned `Full` before the first row fit.
    ///
    /// This is the backoff signal for variable-width layouts. Callers should
    /// use the `rows_per_page` from the failed attempt as `attempted_rows`,
    /// then retry with the next [`Self::estimate`] result on a fresh page.
    ///
    /// If `attempted_rows == 1`, the row is too large for the current page
    /// size and the caller must stop retrying.
    pub fn observe_empty_full_page(&mut self, attempted_rows: u32) -> Result<(), EstimateError> {
        if !self.has_variable_width {
            return Err(EstimateError::EmptyFullBackoffRequiresVariableWidth);
        }
        if attempted_rows == 0 || attempted_rows > self.fixed_row_cap {
            return Err(EstimateError::InvalidEmptyFullAttempt {
                attempted_rows,
                fixed_row_cap: self.fixed_row_cap,
            });
        }
        if attempted_rows == 1 {
            return Err(EstimateError::SingleRowDoesNotFitPage {
                block_size: self.block_size,
            });
        }

        let next_cap = attempted_rows.div_ceil(2);
        self.backoff_row_cap = Some(
            self.backoff_row_cap
                .map_or(next_cap, |current| current.min(next_cap)),
        );
        Ok(())
    }

    /// Learn from one completed encoded block.
    pub fn observe_encoded_block(&mut self, block: &[u8]) -> Result<(), EstimateError> {
        let block = BlockRef::open(block)?;
        if block.block_size() != self.block_size {
            return Err(EstimateError::BlockSizeMismatch {
                expected: self.block_size,
                actual: block.block_size(),
            });
        }
        if block.column_count() != self.specs.len() {
            return Err(EstimateError::ColumnCountMismatch {
                expected: self.specs.len(),
                actual: block.column_count(),
            });
        }
        for (index, expected) in self.specs.iter().enumerate() {
            let actual = block.column_layout(index)?;
            let actual_nullable = actual.flags.is_nullable();
            if actual.type_tag != expected.type_tag || actual_nullable != expected.nullable {
                return Err(EstimateError::ColumnMismatch {
                    index,
                    expected_type: expected.type_tag,
                    actual_type: actual.type_tag,
                    expected_nullable: expected.nullable,
                    actual_nullable,
                });
            }
        }

        let tail_bytes_used = u32::try_from(block.allocated_shared_pool()?.len())
            .map_err(|_| EstimateError::ArithmeticOverflow)?;
        self.observe_page(PageTailObservation {
            row_count: block.row_count(),
            tail_bytes_used,
        })
    }

    fn estimated_tail_ratio(&self) -> (u64, u64) {
        if self.observed_rows == 0 {
            (u64::from(self.config.initial_tail_bytes_per_row), 1)
        } else {
            (self.observed_tail_bytes, self.observed_rows)
        }
    }

    fn estimated_tail_bytes_per_row(&self) -> u32 {
        if !self.has_variable_width {
            0
        } else {
            let (num, den) = self.estimated_tail_ratio();
            ceil_div_u64(num, den)
                .ok()
                .and_then(|value| u32::try_from(value).ok())
                .unwrap_or(u32::MAX)
        }
    }
}

fn compute_fixed_row_cap(specs: &[ColumnSpec], block_size: u32) -> Result<u32, EstimateError> {
    let _ = LayoutPlan::new(specs, 0, block_size)?;

    let mut low = 0u32;
    let mut high = block_size
        .checked_mul(8)
        .ok_or(EstimateError::ArithmeticOverflow)?;

    while low < high {
        let mid = low + (high - low).div_ceil(2);
        match LayoutPlan::new(specs, mid, block_size) {
            Ok(_) => low = mid,
            Err(LayoutError::LayoutDoesNotFit { .. }) => high = mid - 1,
            Err(err) => return Err(err.into()),
        }
    }

    Ok(low)
}

fn ceil_div_u64(num: u64, den: u64) -> Result<u64, EstimateError> {
    if den == 0 {
        return Err(EstimateError::ArithmeticOverflow);
    }
    let adjusted = num
        .checked_add(den - 1)
        .ok_or(EstimateError::ArithmeticOverflow)?;
    Ok(adjusted / den)
}

#[cfg(test)]
mod tests;
