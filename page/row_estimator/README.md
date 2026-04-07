# row_estimator

`row_estimator` estimates how many rows should be reserved in the next
physical `arrow_layout` page for one producer-side stream.

The crate is intentionally generic:

- it works from `arrow_layout::ColumnSpec` plus `block_size`
- it has no PostgreSQL or `pgrx` dependency
- it keeps mutable learning state per caller-owned estimator instance
- it learns from completed page observations rather than per-row callbacks

It operates on the physical encoded page shape. If an upstream logical scan has
no projected user columns, the caller must first lower that case into at least
one physical column, such as a synthetic fixed-width sentinel, before creating
the estimator.

The intended flow is:

1. caller creates one `PageRowEstimator` for a scan or producer stream
2. before each fresh page, caller asks `estimate()`
3. caller uses `rows_per_page` as `max_rows` when building `LayoutPlan`
4. if `BatchPageEncoder::append_batch()` returns
   `AppendResult { rows_written: 0, full: true }` on a fresh page, caller
   reports that with `observe_empty_full_page(estimate.rows_per_page)` and
   retries on a fresh page with the next estimate
5. after a non-empty completed page, caller feeds the completed block back
   through `observe_encoded_block()` or passes a manual `PageTailObservation`

For fixed-width layouts the estimate is exact. For layouts with `Utf8View` or
`BinaryView`, the estimator starts from a configurable initial tail-bytes-per-row
prior and then updates it from observed pages.

## Example

```rust,ignore
use arrow_layout::{ColumnSpec, LayoutPlan, TypeTag, init_block};
use row_estimator::{EstimatorConfig, PageRowEstimator};

let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
let mut estimator = PageRowEstimator::new(
    &specs,
    4096,
    EstimatorConfig {
        initial_tail_bytes_per_row: 64,
    },
)?;

let estimate = estimator.estimate()?;
let plan = LayoutPlan::new(&specs, estimate.rows_per_page, 4096)?;
let mut payload = vec![0u8; plan.block_size() as usize];
init_block(&mut payload, &plan)?;

// ... try to fill the page ...

// If append_batch() returned AppendResult { rows_written: 0, full: true } on a
// fresh page:
// estimator.observe_empty_full_page(estimate.rows_per_page)?;
//
// After a non-empty completed page:
estimator.observe_encoded_block(&payload)?;
```
