use super::page_iter::iter_visible_tuples;
use super::visibility::VisibilityMask;
use super::HeapPageBlock;
use datafusion::arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder, Int16Builder,
    Int32Builder, Int64Builder, IntervalMonthDayNanoBuilder, StringBuilder,
    Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::DataType as ArrowDataType;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::common::Result as DFResult;
use datafusion::error::DataFusionError;
use datafusion::scalar::ScalarValue;
use pgrx_pg_sys as pg_sys;
use std::sync::Arc;
use storage::heap::{decode_tuple_project, HeapPage, PgAttrMeta};

pub(super) struct DecodeOutput {
    pub(super) batch: RecordBatch,
    pub(super) decoded_rows: usize,
}

pub(super) fn decode_block_to_batch(
    block: &HeapPageBlock,
    proj_schema: &SchemaRef,
    attrs_full: &[PgAttrMeta],
    proj_indices: Option<&[usize]>,
) -> DFResult<DecodeOutput> {
    if unsafe { HeapPage::from_slice(&block.page) }.is_err() {
        return Ok(DecodeOutput {
            batch: RecordBatch::new_empty(Arc::clone(proj_schema)),
            decoded_rows: 0,
        });
    }

    let total_cols = proj_schema.fields().len();
    let mut builders = make_builders(proj_schema, estimate_row_capacity(block))
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))?;

    let page_hdr = unsafe { &*(block.page.as_ptr() as *const pg_sys::PageHeaderData) } as *const _;
    let vis = if block.vis_len > 0 {
        Some(VisibilityMask::new(&block.vis, block.num_offsets))
    } else {
        None
    };

    let mut decoded_rows = 0usize;
    for tup in iter_visible_tuples(&block.page, vis.as_ref()) {
        let tuple_decoded = if let Some(indices) = proj_indices {
            let iter = unsafe {
                decode_tuple_project(page_hdr, tup.bytes, attrs_full, indices.iter().copied())
            };
            append_tuple_into_builders(iter, &mut builders, total_cols)
        } else {
            let iter = unsafe {
                decode_tuple_project(page_hdr, tup.bytes, attrs_full, 0..attrs_full.len())
            };
            append_tuple_into_builders(iter, &mut builders, total_cols)
        };
        if !tuple_decoded {
            continue;
        }
        decoded_rows += 1;
    }

    Ok(DecodeOutput {
        batch: finish_batch(builders, proj_schema, decoded_rows)?,
        decoded_rows,
    })
}

#[inline]
fn append_tuple_into_builders<I>(
    iter: anyhow::Result<storage::heap::DecodedIter<'_, I>>,
    builders: &mut [ColBuilder],
    total_cols: usize,
) -> bool
where
    I: Iterator<Item = usize>,
{
    let Ok(mut iter) = iter else {
        return false;
    };
    for b in builders.iter_mut().take(total_cols) {
        match iter.next() {
            Some(Ok(v)) => append_scalar(b, v),
            Some(Err(_)) => append_null(b),
            None => append_null(b),
        }
    }
    true
}

#[inline]
fn estimate_row_capacity(block: &HeapPageBlock) -> usize {
    let max_rows = block.num_offsets as usize;
    if block.vis_len == 0 || block.vis.is_empty() || max_rows == 0 {
        return max_rows;
    }
    let needed = max_rows.div_ceil(8);
    let bytes = &block.vis[..block.vis.len().min(needed)];
    let mut visible = 0usize;
    for (i, b) in bytes.iter().copied().enumerate() {
        let mut bits = b.count_ones() as usize;
        // Trim bits beyond num_offsets in the final byte.
        if i + 1 == needed && (max_rows % 8) != 0 {
            let keep = max_rows % 8;
            let mask = (1u8 << keep) - 1;
            bits = (b & mask).count_ones() as usize;
        }
        visible += bits;
    }
    visible.min(max_rows)
}

pub(super) fn attrs_from_schema(schema: &SchemaRef) -> Vec<PgAttrMeta> {
    schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            datafusion::arrow::datatypes::DataType::Boolean => PgAttrMeta {
                atttypid: pg_sys::BOOLOID,
                attlen: 1,
                attalign: b'c',
            },
            datafusion::arrow::datatypes::DataType::Utf8 => PgAttrMeta {
                atttypid: pg_sys::TEXTOID,
                attlen: -1,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Int16 => PgAttrMeta {
                atttypid: pg_sys::INT2OID,
                attlen: 2,
                attalign: b's',
            },
            datafusion::arrow::datatypes::DataType::Int32 => PgAttrMeta {
                atttypid: pg_sys::INT4OID,
                attlen: 4,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Int64 => PgAttrMeta {
                atttypid: pg_sys::INT8OID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Float32 => PgAttrMeta {
                atttypid: pg_sys::FLOAT4OID,
                attlen: 4,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Float64 => PgAttrMeta {
                atttypid: pg_sys::FLOAT8OID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Date32 => PgAttrMeta {
                atttypid: pg_sys::DATEOID,
                attlen: 4,
                attalign: b'i',
            },
            datafusion::arrow::datatypes::DataType::Time64(_) => PgAttrMeta {
                atttypid: pg_sys::TIMEOID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Timestamp(_, _) => PgAttrMeta {
                atttypid: pg_sys::TIMESTAMPOID,
                attlen: 8,
                attalign: b'd',
            },
            datafusion::arrow::datatypes::DataType::Interval(_) => PgAttrMeta {
                atttypid: pg_sys::INTERVALOID,
                attlen: 16,
                attalign: b'd',
            },
            _ => PgAttrMeta {
                atttypid: pg_sys::InvalidOid,
                attlen: -1,
                attalign: b'c',
            },
        })
        .collect()
}

enum ColBuilder {
    Bool(BooleanBuilder),
    I16(Int16Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    Utf8(StringBuilder),
    Date32(Date32Builder),
    Time64Us(Time64MicrosecondBuilder),
    TsUs(TimestampMicrosecondBuilder),
    Interval(IntervalMonthDayNanoBuilder),
}

fn make_builders(schema: &SchemaRef, capacity: usize) -> Result<Vec<ColBuilder>, DataFusionError> {
    let mut out = Vec::with_capacity(schema.fields().len());
    for f in schema.fields().iter() {
        let b = match f.data_type() {
            ArrowDataType::Boolean => ColBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
            ArrowDataType::Int16 => ColBuilder::I16(Int16Builder::with_capacity(capacity)),
            ArrowDataType::Int32 => ColBuilder::I32(Int32Builder::with_capacity(capacity)),
            ArrowDataType::Int64 => ColBuilder::I64(Int64Builder::with_capacity(capacity)),
            ArrowDataType::Float32 => ColBuilder::F32(Float32Builder::with_capacity(capacity)),
            ArrowDataType::Float64 => ColBuilder::F64(Float64Builder::with_capacity(capacity)),
            ArrowDataType::Utf8 => {
                let bytes_capacity = capacity.saturating_mul(4);
                ColBuilder::Utf8(StringBuilder::with_capacity(capacity, bytes_capacity))
            }
            ArrowDataType::Date32 => ColBuilder::Date32(Date32Builder::with_capacity(capacity)),
            ArrowDataType::Time64(_) => {
                ColBuilder::Time64Us(Time64MicrosecondBuilder::with_capacity(capacity))
            }
            ArrowDataType::Timestamp(_, _) => {
                ColBuilder::TsUs(TimestampMicrosecondBuilder::with_capacity(capacity))
            }
            ArrowDataType::Interval(_) => {
                ColBuilder::Interval(IntervalMonthDayNanoBuilder::with_capacity(capacity))
            }
            other => {
                return Err(DataFusionError::Execution(format!(
                    "unsupported type in builder: {other:?}"
                )))
            }
        };
        out.push(b);
    }
    Ok(out)
}

fn append_scalar(b: &mut ColBuilder, v: ScalarValue) {
    match (b, v) {
        (ColBuilder::Bool(b), ScalarValue::Boolean(Some(x))) => b.append_value(x),
        (ColBuilder::Bool(b), _) => b.append_null(),

        (ColBuilder::I16(b), ScalarValue::Int16(Some(x))) => b.append_value(x),
        (ColBuilder::I16(b), _) => b.append_null(),

        (ColBuilder::I32(b), ScalarValue::Int32(Some(x))) => b.append_value(x),
        (ColBuilder::I32(b), ScalarValue::Date32(Some(x))) => b.append_value(x),
        (ColBuilder::I32(b), _) => b.append_null(),

        (ColBuilder::I64(b), ScalarValue::Int64(Some(x))) => b.append_value(x),
        (ColBuilder::I64(b), _) => b.append_null(),

        (ColBuilder::F32(b), ScalarValue::Float32(Some(x))) => b.append_value(x),
        (ColBuilder::F32(b), _) => b.append_null(),

        (ColBuilder::F64(b), ScalarValue::Float64(Some(x))) => b.append_value(x),
        (ColBuilder::F64(b), _) => b.append_null(),

        (ColBuilder::Utf8(b), ScalarValue::Utf8(Some(s))) => b.append_value(&s),
        (ColBuilder::Utf8(b), _) => b.append_null(),

        (ColBuilder::Date32(b), ScalarValue::Date32(Some(x))) => b.append_value(x),
        (ColBuilder::Date32(b), _) => b.append_null(),

        (ColBuilder::Time64Us(b), ScalarValue::Time64Microsecond(Some(x))) => b.append_value(x),
        (ColBuilder::Time64Us(b), _) => b.append_null(),

        (ColBuilder::TsUs(b), ScalarValue::TimestampMicrosecond(Some(x), _)) => b.append_value(x),
        (ColBuilder::TsUs(b), _) => b.append_null(),

        (ColBuilder::Interval(b), ScalarValue::IntervalMonthDayNano(Some(x))) => b.append_value(x),
        (ColBuilder::Interval(b), _) => b.append_null(),
    }
}

fn append_null(b: &mut ColBuilder) {
    match b {
        ColBuilder::Bool(b) => b.append_null(),
        ColBuilder::I16(b) => b.append_null(),
        ColBuilder::I32(b) => b.append_null(),
        ColBuilder::I64(b) => b.append_null(),
        ColBuilder::F32(b) => b.append_null(),
        ColBuilder::F64(b) => b.append_null(),
        ColBuilder::Utf8(b) => b.append_null(),
        ColBuilder::Date32(b) => b.append_null(),
        ColBuilder::Time64Us(b) => b.append_null(),
        ColBuilder::TsUs(b) => b.append_null(),
        ColBuilder::Interval(b) => b.append_null(),
    }
}

fn finish_batch(
    builders: Vec<ColBuilder>,
    schema: &SchemaRef,
    rows: usize,
) -> DFResult<RecordBatch> {
    let mut arrs = Vec::with_capacity(builders.len());
    for b in builders {
        arrs.push(finish_builder(b));
    }

    if schema.fields().is_empty() {
        let opts = RecordBatchOptions::new().with_row_count(Some(rows));
        RecordBatch::try_new_with_options(Arc::clone(schema), vec![], &opts)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))
    } else {
        RecordBatch::try_new(Arc::clone(schema), arrs)
            .map_err(|e| datafusion::error::DataFusionError::Execution(format!("{e}")))
    }
}

fn finish_builder(b: ColBuilder) -> ArrayRef {
    match b {
        ColBuilder::Bool(mut b) => Arc::new(b.finish()),
        ColBuilder::I16(mut b) => Arc::new(b.finish()),
        ColBuilder::I32(mut b) => Arc::new(b.finish()),
        ColBuilder::I64(mut b) => Arc::new(b.finish()),
        ColBuilder::F32(mut b) => Arc::new(b.finish()),
        ColBuilder::F64(mut b) => Arc::new(b.finish()),
        ColBuilder::Utf8(mut b) => Arc::new(b.finish()),
        ColBuilder::Date32(mut b) => Arc::new(b.finish()),
        ColBuilder::Time64Us(mut b) => Arc::new(b.finish()),
        ColBuilder::TsUs(mut b) => Arc::new(b.finish()),
        ColBuilder::Interval(mut b) => Arc::new(b.finish()),
    }
}

#[cfg(test)]
mod tests {
    use super::attrs_from_schema;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn attrs_from_schema_maps_known_types() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
            Field::new("c", DataType::Date32, true),
        ]));

        let attrs = attrs_from_schema(&schema);
        assert_eq!(attrs.len(), 3);
        assert_eq!(attrs[0].attlen, 8);
        assert_eq!(attrs[1].attlen, -1);
        assert_eq!(attrs[2].attlen, 4);
    }
}
