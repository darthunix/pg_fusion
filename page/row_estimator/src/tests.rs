use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, RecordBatch, StringArray};
use arrow_layout::{init_block, BlockRef, ColumnSpec, LayoutPlan, TypeTag};
use arrow_schema::{DataType, Field, Schema};
use batch_encoder::BatchPageEncoder;

use crate::{EstimateError, EstimatorConfig, PageRowEstimator, PageTailObservation};

#[test]
fn rejects_empty_physical_page_shape() {
    let err = PageRowEstimator::new(&[], 256, EstimatorConfig::default())
        .expect_err("empty physical page shape must be rejected");
    assert!(matches!(err, EstimateError::NoPhysicalColumns));
    assert_eq!(
        err.to_string(),
        "row estimator requires at least one physical column in the encoded page shape"
    );
}

#[test]
fn rejects_layouts_that_cannot_fit_one_row() {
    let specs = [ColumnSpec::new(TypeTag::Int64, false)];
    let block_size = (1..512_u32)
        .find(|&candidate| {
            LayoutPlan::new(&specs, 0, candidate).is_ok()
                && LayoutPlan::new(&specs, 1, candidate).is_err()
        })
        .expect("need a block size that fits zero rows but not one row");

    let err = PageRowEstimator::new(&specs, block_size, EstimatorConfig::default())
        .expect_err("impossible layout must fail at construction");
    assert!(matches!(
        err,
        EstimateError::LayoutCannotFitAnyRows { block_size: actual } if actual == block_size
    ));
}

#[test]
fn fixed_width_layout_is_exact_and_ignores_observations() {
    let specs = [
        ColumnSpec::new(TypeTag::Int64, false),
        ColumnSpec::new(TypeTag::Boolean, true),
    ];
    let mut estimator =
        PageRowEstimator::new(&specs, 256, EstimatorConfig::default()).expect("estimator");

    let estimate = estimator.estimate().expect("estimate");
    assert_eq!(estimate.rows_per_page, estimate.fixed_row_cap);
    assert_eq!(estimate.estimated_tail_bytes_per_row, 0);
    assert_eq!(estimate.expected_tail_bytes, 0);
    assert!(LayoutPlan::new(&specs, estimate.rows_per_page, 256).is_ok());
    assert!(LayoutPlan::new(&specs, estimate.rows_per_page + 1, 256).is_err());

    estimator
        .observe_page(PageTailObservation {
            row_count: 3,
            tail_bytes_used: 99,
        })
        .expect("ignored observation");

    let after = estimator.estimate().expect("estimate after observation");
    assert_eq!(after, estimate);
}

#[test]
fn variable_width_layout_uses_configured_prior_and_adapts() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &specs,
        512,
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("estimator");

    let before = estimator.estimate().expect("initial estimate");
    assert_eq!(before.estimated_tail_bytes_per_row, 64);

    estimator
        .observe_page(PageTailObservation {
            row_count: 4,
            tail_bytes_used: 32,
        })
        .expect("observe");

    let after = estimator.estimate().expect("estimate after observe");
    assert_eq!(after.estimated_tail_bytes_per_row, 8);
    assert!(after.rows_per_page > before.rows_per_page);
}

#[test]
fn zero_row_observation_is_ignored() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &specs,
        256,
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("estimator");

    let before = estimator.estimate().expect("initial estimate");
    estimator
        .observe_page(PageTailObservation {
            row_count: 0,
            tail_bytes_used: 200,
        })
        .expect("zero-row observations are ignored");
    let after = estimator
        .estimate()
        .expect("estimate after zero-row observe");

    assert_eq!(after, before);
}

#[test]
fn empty_full_page_backs_off_next_estimate() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &specs,
        512,
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("estimator");

    let before = estimator.estimate().expect("initial estimate");
    estimator
        .observe_empty_full_page(before.rows_per_page)
        .expect("empty-full backoff");
    let after = estimator.estimate().expect("estimate after backoff");

    assert!(after.rows_per_page < before.rows_per_page);
    assert!(after.rows_per_page <= before.rows_per_page.div_ceil(2));
}

#[test]
fn batch_encoder_retry_path_reaches_estimator_backoff() {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let block_size = 160;
    let mut estimator = PageRowEstimator::new(
        &specs,
        block_size,
        EstimatorConfig {
            initial_tail_bytes_per_row: 18,
        },
    )
    .expect("estimator");

    let initial = estimator.estimate().expect("initial estimate");
    assert_eq!(initial.rows_per_page, 2);

    let initial_plan =
        LayoutPlan::new(&specs, initial.rows_per_page, block_size).expect("initial plan");
    let single_row_plan = LayoutPlan::new(&specs, 1, block_size).expect("single-row plan");
    let retryable_len = usize::try_from(initial_plan.shared_pool_capacity()).expect("capacity") + 4;
    assert!(retryable_len <= usize::try_from(single_row_plan.shared_pool_capacity()).unwrap());
    let retryable = "x".repeat(retryable_len);

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(StringArray::from(vec![Some(retryable.as_str())])) as ArrayRef],
    )
    .expect("batch");

    let mut initial_payload = vec![0u8; initial_plan.block_size() as usize];
    init_block(&mut initial_payload, &initial_plan).expect("init initial block");
    let mut initial_encoder =
        BatchPageEncoder::new(schema.as_ref(), &initial_plan, &mut initial_payload)
            .expect("initial encoder");
    let initial_append = initial_encoder.append_batch(&batch, 0).expect("append");
    assert_eq!(initial_append.rows_written, 0);
    assert!(initial_append.full);

    estimator
        .observe_empty_full_page(initial.rows_per_page)
        .expect("backoff");
    let backed_off = estimator.estimate().expect("backed off estimate");
    assert_eq!(backed_off.rows_per_page, 1);

    let mut retried_payload = vec![0u8; single_row_plan.block_size() as usize];
    init_block(&mut retried_payload, &single_row_plan).expect("init retried block");
    let mut retried_encoder =
        BatchPageEncoder::new(schema.as_ref(), &single_row_plan, &mut retried_payload)
            .expect("retried encoder");
    let retried_append = retried_encoder
        .append_batch(&batch, 0)
        .expect("retry append");
    assert_eq!(retried_append.rows_written, 1);
    assert!(!retried_append.full);
}

#[test]
fn repeated_empty_full_pages_converge_to_one_row() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &specs,
        512,
        EstimatorConfig {
            initial_tail_bytes_per_row: 1,
        },
    )
    .expect("estimator");

    for _ in 0..32 {
        let estimate = estimator.estimate().expect("estimate");
        if estimate.rows_per_page == 1 {
            return;
        }
        estimator
            .observe_empty_full_page(estimate.rows_per_page)
            .expect("empty-full backoff");
    }

    panic!("backoff should converge to one row");
}

#[test]
fn clamps_variable_width_estimate_to_at_least_one_row() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let estimator = PageRowEstimator::new(
        &specs,
        128,
        EstimatorConfig {
            initial_tail_bytes_per_row: 10_000,
        },
    )
    .expect("estimator");

    let estimate = estimator.estimate().expect("estimate");
    assert!(estimate.fixed_row_cap > 0);
    assert_eq!(estimate.rows_per_page, 1);
}

#[test]
fn successful_page_observation_clears_backoff() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &specs,
        512,
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("estimator");

    let before = estimator.estimate().expect("initial estimate");
    estimator
        .observe_empty_full_page(before.rows_per_page)
        .expect("backoff");
    let backed_off = estimator.estimate().expect("backed off estimate");
    assert!(backed_off.rows_per_page < before.rows_per_page);

    estimator
        .observe_page(PageTailObservation {
            row_count: 4,
            tail_bytes_used: 32,
        })
        .expect("successful observation");
    let after = estimator.estimate().expect("estimate after observation");

    assert!(after.rows_per_page > backed_off.rows_per_page);
}

#[test]
fn observe_empty_full_page_rejects_invalid_inputs() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator =
        PageRowEstimator::new(&specs, 256, EstimatorConfig::default()).expect("estimator");

    assert!(matches!(
        estimator.observe_empty_full_page(0),
        Err(EstimateError::InvalidEmptyFullAttempt {
            attempted_rows: 0,
            ..
        })
    ));
    assert!(matches!(
        estimator.observe_empty_full_page(estimator.fixed_row_cap() + 1),
        Err(EstimateError::InvalidEmptyFullAttempt { .. })
    ));
}

#[test]
fn observe_empty_full_page_rejects_fixed_width_layouts() {
    let specs = [ColumnSpec::new(TypeTag::Int64, false)];
    let mut estimator =
        PageRowEstimator::new(&specs, 256, EstimatorConfig::default()).expect("estimator");

    assert!(matches!(
        estimator.observe_empty_full_page(1),
        Err(EstimateError::EmptyFullBackoffRequiresVariableWidth)
    ));
}

#[test]
fn single_row_that_still_does_not_fit_returns_hard_error() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator =
        PageRowEstimator::new(&specs, 256, EstimatorConfig::default()).expect("estimator");

    let err = estimator
        .observe_empty_full_page(1)
        .expect_err("single-row empty-full must be terminal");
    assert!(matches!(
        err,
        EstimateError::SingleRowDoesNotFitPage { block_size: 256 }
    ));
}

#[test]
fn rejects_observations_that_exceed_tail_capacity() {
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator =
        PageRowEstimator::new(&specs, 256, EstimatorConfig::default()).expect("estimator");

    let err = estimator
        .observe_page(PageTailObservation {
            row_count: 1,
            tail_bytes_used: 10_000,
        })
        .expect_err("tail beyond page capacity must fail");

    assert!(matches!(
        err,
        EstimateError::ObservationTailTooLarge { row_count: 1, .. }
    ));
}

#[test]
fn observe_encoded_block_learns_from_completed_block() {
    let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
    let specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &specs,
        512,
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("estimator");

    let before = estimator.estimate().expect("initial estimate");
    let payload = encode_block(
        schema,
        &specs,
        4,
        512,
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![
                Some("abcdefghijklmnop"),
                Some("qrstuvwxyzabcdef"),
                Some("ghijklmnopqrstuv"),
                Some("wxyzabcdefghijkl"),
            ])) as ArrayRef],
        )
        .expect("batch"),
    );

    let block = BlockRef::open(&payload).expect("block");
    assert!(!block
        .allocated_shared_pool()
        .expect("allocated tail")
        .is_empty());

    estimator
        .observe_encoded_block(&payload)
        .expect("learn from encoded block");
    let after = estimator.estimate().expect("estimate after observe");

    assert!(after.rows_per_page > before.rows_per_page);
    assert!(after.estimated_tail_bytes_per_row < before.estimated_tail_bytes_per_row);
}

#[test]
fn observe_encoded_block_rejects_mismatched_layout() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Binary,
        true,
    )]));
    let block_specs = [ColumnSpec::new(TypeTag::BinaryView, true)];
    let payload = encode_block(
        schema,
        &block_specs,
        2,
        256,
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Binary,
                true,
            )])),
            vec![Arc::new(BinaryArray::from(vec![
                Some(&b"abcdefghijklmnop"[..]),
                Some(&b"qrstuvwxyzabcdef"[..]),
            ])) as ArrayRef],
        )
        .expect("batch"),
    );

    let mismatched_specs = [ColumnSpec::new(TypeTag::Utf8View, true)];
    let mut estimator = PageRowEstimator::new(
        &mismatched_specs,
        256,
        EstimatorConfig {
            initial_tail_bytes_per_row: 64,
        },
    )
    .expect("estimator");

    let err = estimator
        .observe_encoded_block(&payload)
        .expect_err("mismatched block must fail");
    assert!(matches!(
        err,
        EstimateError::ColumnMismatch { index: 0, .. }
    ));
}

fn encode_block(
    schema: Arc<Schema>,
    specs: &[ColumnSpec],
    max_rows: u32,
    block_size: u32,
    batch: RecordBatch,
) -> Vec<u8> {
    let plan = LayoutPlan::new(specs, max_rows, block_size).expect("plan");
    let mut payload = vec![0u8; plan.block_size() as usize];
    init_block(&mut payload, &plan).expect("init block");

    let mut encoder = BatchPageEncoder::new(schema.as_ref(), &plan, &mut payload).expect("encoder");
    let appended = encoder.append_batch(&batch, 0).expect("append");
    assert_eq!(appended.rows_written, batch.num_rows());
    assert!(!appended.full);
    encoder.finish().expect("finish");
    payload
}
