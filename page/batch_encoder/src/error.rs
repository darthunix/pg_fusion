use arrow_layout::{LayoutError, TypeTag};
use arrow_schema::DataType;
use thiserror::Error;

/// Configuration-time failures when binding one input Arrow schema to an
/// initialized `arrow_layout` block.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("schema has {schema_cols} columns but layout plan has {plan_cols}")]
    SchemaPlanColumnCountMismatch {
        schema_cols: usize,
        plan_cols: usize,
    },
    #[error("initialized block has {block_cols} columns but layout plan has {plan_cols}")]
    PlanBlockColumnCountMismatch { plan_cols: usize, block_cols: usize },
    #[error("layout plan block_size {plan_block_size} does not match payload block_size {block_block_size}")]
    PlanBlockSizeMismatch {
        plan_block_size: u32,
        block_block_size: u32,
    },
    #[error(
        "layout plan max_rows {plan_max_rows} does not match payload max_rows {block_max_rows}"
    )]
    PlanMaxRowsMismatch {
        plan_max_rows: u32,
        block_max_rows: u32,
    },
    #[error(
        "column {index} schema type {data_type} is incompatible with layout type {type_tag:?}"
    )]
    SchemaPlanTypeMismatch {
        index: usize,
        data_type: String,
        type_tag: TypeTag,
    },
    #[error("column {index} schema nullability {schema_nullable} does not match layout nullability {layout_nullable}")]
    SchemaPlanNullabilityMismatch {
        index: usize,
        schema_nullable: bool,
        layout_nullable: bool,
    },
    #[error("column {index} layout in the initialized block does not match the supplied plan")]
    PlanBlockColumnLayoutMismatch { index: usize },
    #[error("unsupported Arrow type at column {index}: {data_type}")]
    UnsupportedArrowType { index: usize, data_type: DataType },
    #[error("invalid layout block: {0}")]
    Layout(#[from] LayoutError),
}

/// Row-encoding failures while appending `RecordBatch` rows into one block.
#[derive(Debug, Error)]
pub enum EncodeError {
    #[error("batch schema does not match the encoder schema")]
    BatchSchemaMismatch,
    #[error("start_row {start_row} is out of bounds for batch with {row_count} rows")]
    StartRowOutOfBounds { start_row: usize, row_count: usize },
    #[error(
        "row {row} requires {required_tail} tail bytes but an empty page only has {page_tail_capacity}"
    )]
    RowTooLargeForPage {
        row: usize,
        required_tail: u32,
        page_tail_capacity: u32,
    },
    #[error("row value at column {index} is too large for ByteView encoding: {len} bytes")]
    RowValueTooLarge { index: usize, len: usize },
    #[error("column {index} ran out of tail space after exact-fit planning at row {row}")]
    UnexpectedFull { index: usize, row: usize },
    #[error("layout write failed: {0}")]
    Layout(#[from] LayoutError),
}
