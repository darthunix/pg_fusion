use super::*;
use arrow_schema::{DataType, Field, Schema};
use crate::constants::{
    BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS, SHARED_VIEW_BUFFER_INDEX,
};
use crate::raw::{BlockHeader, ColumnDesc};
use std::mem::{align_of, size_of};

#[test]
fn repr_c_sizes_are_stable() {
    assert_eq!(size_of::<ByteView>(), 16);
    assert_eq!(align_of::<ByteView>(), 4);

    assert_eq!(size_of::<ColumnDesc>(), 20);
    assert_eq!(align_of::<ColumnDesc>(), 4);

    assert_eq!(size_of::<BlockHeader>(), 40);
    assert_eq!(align_of::<BlockHeader>(), 4);
}

#[test]
fn plans_mixed_fixed_and_view_schema() {
    let schema = Schema::new(vec![
        Field::new("b", DataType::Boolean, true),
        Field::new("i64", DataType::Int64, true),
        Field::new("uuid", DataType::FixedSizeBinary(16), false),
        Field::new("txt", DataType::Utf8View, true),
        Field::new("bin", DataType::BinaryView, true),
    ]);

    let plan = LayoutPlan::from_arrow_schema(&schema, 64, 4096).expect("plan");
    assert_eq!(plan.max_rows(), 64);
    assert_eq!(plan.columns().len(), 5);
    assert_eq!(plan.front_base(), 140);
    assert!(plan.pool_base() > plan.front_base());
    assert_eq!(plan.front_base() % BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS);
    assert_eq!(plan.pool_base() % BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS);

    for column in plan.columns() {
        assert_eq!(
            column.validity_off % BUFFER_ALIGNMENT,
            BUFFER_ALIGNMENT_BIAS
        );
        assert_eq!(column.values_off % BUFFER_ALIGNMENT, BUFFER_ALIGNMENT_BIAS);
        assert!(column.values_off >= column.validity_off + column.validity_len);
    }
}

#[test]
fn rejects_plain_utf8_and_binary() {
    let schema = Schema::new(vec![
        Field::new("txt", DataType::Utf8, true),
        Field::new("bin", DataType::Binary, true),
    ]);
    let err = LayoutPlan::from_arrow_schema(&schema, 16, 1024).expect_err("unsupported");
    match err {
        LayoutError::UnsupportedArrowType { index, .. } => assert_eq!(index, 0),
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn byte_view_inline_round_trips() {
    let view = ByteView::new_inline(b"hello").expect("inline view");
    assert!(view.is_inline().expect("len"));
    assert_eq!(view.len().expect("len"), 5);
    assert_eq!(view.inline_bytes().expect("inline"), Some(&b"hello"[..]));
    assert_eq!(view.buffer_index().expect("buffer index"), None);
    assert_eq!(view.offset().expect("offset"), None);
    view.validate(0).expect("valid inline view");
}

#[test]
fn byte_view_outline_round_trips() {
    let payload = b"abcdefghijklmnop";
    let view = ByteView::new_outline(payload, 128).expect("outline view");
    assert!(!view.is_inline().expect("len"));
    assert_eq!(view.len().expect("len"), payload.len());
    assert_eq!(view.prefix4(), *b"abcd");
    assert_eq!(
        view.buffer_index().expect("buffer index"),
        Some(SHARED_VIEW_BUFFER_INDEX)
    );
    assert_eq!(view.offset().expect("offset"), Some(128));
    view.validate(256).expect("valid outline view");
}

#[test]
fn rejects_outline_view_past_shared_pool() {
    let view = ByteView::new_outline(b"abcdefghijklmnop", 250).expect("outline view");
    let err = view.validate(256).expect_err("out of bounds");
    match err {
        LayoutError::ViewOffsetOutOfBounds {
            offset,
            len,
            pool_capacity,
        } => {
            assert_eq!(offset, 250);
            assert_eq!(len, 16);
            assert_eq!(pool_capacity, 256);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn validates_header_and_column_descs() {
    let schema = Schema::new(vec![
        Field::new("i32", DataType::Int32, true),
        Field::new("txt", DataType::Utf8View, true),
    ]);
    let plan = LayoutPlan::from_arrow_schema(&schema, 32, 2048).expect("plan");
    let header = plan.block_header();
    let descs: Vec<_> = plan.column_descs().collect();

    let validated = LayoutPlan::validate(&header, &descs).expect("validate");
    assert_eq!(validated, plan);
}

#[test]
fn detects_inconsistent_view_flag() {
    let plan = LayoutPlan::new(
        &[
            ColumnSpec::new(TypeTag::Int32, true),
            ColumnSpec::new(TypeTag::Utf8View, true),
        ],
        8,
        1024,
    )
    .expect("plan");
    let header = plan.block_header();
    let mut descs: Vec<_> = plan.column_descs().collect();
    descs[0].flags |= ColumnFlags::VIEW.bits();

    let err = LayoutPlan::validate(&header, &descs).expect_err("inconsistent flags");
    match err {
        LayoutError::InconsistentViewFlag {
            index, type_tag, ..
        } => {
            assert_eq!(index, 0);
            assert_eq!(type_tag, TypeTag::Int32);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn detects_too_small_block() {
    let err = LayoutPlan::new(
        &[
            ColumnSpec::new(TypeTag::Int64, true),
            ColumnSpec::new(TypeTag::Utf8View, true),
        ],
        128,
        64,
    )
    .expect_err("too small");
    match err {
        LayoutError::LayoutDoesNotFit {
            block_size,
            required,
        } => {
            assert_eq!(block_size, 64);
            assert!(required > block_size);
        }
        other => panic!("unexpected error: {other:?}"),
    }
}
