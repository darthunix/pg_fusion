//! Shared zero-copy Arrow page layout contract.
//!
//! `page_arrow_layout` defines the binary page layout for a future non-IPC
//! page format shared by producer-side and consumer-side crates.

mod error;

#[cfg(test)]
mod tests;

use arrow_schema::{DataType, Schema};
pub use error::LayoutError;
use std::mem::size_of;

pub const BLOCK_MAGIC: u32 = 0x3242_4150; // "PAB2"
pub const BLOCK_VERSION: u16 = 1;
pub const BUFFER_ALIGNMENT: u32 = 16;
pub const VIEW_INLINE_LEN: usize = 12;
pub const VIEW_PREFIX_LEN: usize = 4;
pub const SHARED_VIEW_BUFFER_INDEX: i32 = 0;
pub const UUID_WIDTH_BYTES: u32 = 16;

/// Raw block-level flags stored in [`BlockHeader::flags`].
///
/// No block flags are defined in v1; the type exists to keep the on-page
/// representation explicit and extensible.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BlockFlags(u16);

impl BlockFlags {
    pub const NONE: Self = Self(0);

    pub const fn bits(self) -> u16 {
        self.0
    }
}

/// Raw per-column flags stored in [`ColumnDesc::flags`].
///
/// These bits describe nullability and whether the column uses the `ByteView`
/// representation backed by the shared tail pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ColumnFlags(u16);

impl ColumnFlags {
    pub const NONE: Self = Self(0);
    pub const NULLABLE: Self = Self(1 << 0);
    pub const VIEW: Self = Self(1 << 1);

    pub const fn bits(self) -> u16 {
        self.0
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn is_nullable(self) -> bool {
        self.contains(Self::NULLABLE)
    }

    pub const fn is_view(self) -> bool {
        self.contains(Self::VIEW)
    }
}

impl std::ops::BitOr for ColumnFlags {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        Self(self.0 | rhs.0)
    }
}

impl std::ops::BitOrAssign for ColumnFlags {
    fn bitor_assign(&mut self, rhs: Self) {
        self.0 |= rhs.0;
    }
}

/// Stable on-page type tag stored in [`ColumnDesc::type_tag`].
///
/// This is the layout-level type surface accepted by v1 of the raw page
/// format. It is intentionally narrower than Arrow's full type system.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum TypeTag {
    /// Bit-packed boolean values plus a validity bitmap.
    Boolean = 1,
    /// Little-endian signed 16-bit integers.
    Int16 = 2,
    /// Little-endian signed 32-bit integers.
    Int32 = 3,
    /// Little-endian signed 64-bit integers.
    Int64 = 4,
    /// Little-endian IEEE754 32-bit floats.
    Float32 = 5,
    /// Little-endian IEEE754 64-bit floats.
    Float64 = 6,
    /// Fixed-width 16-byte values intended for PostgreSQL `uuid`.
    Uuid = 7,
    /// 16-byte [`ByteView`] slots for UTF-8 string views.
    Utf8View = 8,
    /// 16-byte [`ByteView`] slots for binary views.
    BinaryView = 9,
}

impl TypeTag {
    pub fn from_raw(raw: u16) -> Result<Self, LayoutError> {
        match raw {
            1 => Ok(Self::Boolean),
            2 => Ok(Self::Int16),
            3 => Ok(Self::Int32),
            4 => Ok(Self::Int64),
            5 => Ok(Self::Float32),
            6 => Ok(Self::Float64),
            7 => Ok(Self::Uuid),
            8 => Ok(Self::Utf8View),
            9 => Ok(Self::BinaryView),
            _ => Err(LayoutError::InvalidTypeTag { raw }),
        }
    }

    pub const fn to_raw(self) -> u16 {
        self as u16
    }

    pub const fn is_view(self) -> bool {
        matches!(self, Self::Utf8View | Self::BinaryView)
    }

    pub const fn values_row_width(self) -> Option<u32> {
        match self {
            Self::Boolean => None,
            Self::Int16 => Some(2),
            Self::Int32 | Self::Float32 => Some(4),
            Self::Int64 | Self::Float64 => Some(8),
            Self::Uuid | Self::Utf8View | Self::BinaryView => Some(16),
        }
    }

    pub fn values_reserved_len(self, max_rows: u32) -> Result<u32, LayoutError> {
        match self {
            Self::Boolean => align_up_u32(bitmap_bytes(max_rows), BUFFER_ALIGNMENT),
            Self::Int16 => aligned_mul(max_rows, 2),
            Self::Int32 | Self::Float32 => aligned_mul(max_rows, 4),
            Self::Int64 | Self::Float64 => aligned_mul(max_rows, 8),
            Self::Uuid | Self::Utf8View | Self::BinaryView => aligned_mul(max_rows, 16),
        }
    }

    pub fn from_arrow_data_type(index: usize, data_type: &DataType) -> Result<Self, LayoutError> {
        match data_type {
            DataType::Boolean => Ok(Self::Boolean),
            DataType::Int16 => Ok(Self::Int16),
            DataType::Int32 => Ok(Self::Int32),
            DataType::Int64 => Ok(Self::Int64),
            DataType::Float32 => Ok(Self::Float32),
            DataType::Float64 => Ok(Self::Float64),
            DataType::FixedSizeBinary(width) if *width == UUID_WIDTH_BYTES as i32 => Ok(Self::Uuid),
            DataType::Utf8View => Ok(Self::Utf8View),
            DataType::BinaryView => Ok(Self::BinaryView),
            other => Err(LayoutError::UnsupportedArrowType {
                index,
                data_type: other.to_string(),
            }),
        }
    }
}

/// Logical column declaration used when planning a page layout.
///
/// This is the minimal schema information needed to derive on-page offsets:
/// a layout type tag and whether the column has a validity bitmap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Stable on-page type tag for the column.
    pub type_tag: TypeTag,
    /// Whether the column reserves a validity bitmap.
    pub nullable: bool,
}

impl ColumnSpec {
    pub const fn new(type_tag: TypeTag, nullable: bool) -> Self {
        Self { type_tag, nullable }
    }

    pub fn flags(self) -> ColumnFlags {
        let mut flags = ColumnFlags::NONE;
        if self.nullable {
            flags |= ColumnFlags::NULLABLE;
        }
        if self.type_tag.is_view() {
            flags |= ColumnFlags::VIEW;
        }
        flags
    }
}

/// Computed front-region layout for one column.
///
/// This is not written to the page directly. It is the planner-side view used
/// to derive the corresponding [`ColumnDesc`] and to reason about front-region
/// reserved lengths.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnLayout {
    /// Stable on-page type tag for the column.
    pub type_tag: TypeTag,
    /// Resolved layout flags for the column.
    pub flags: ColumnFlags,
    /// Byte offset from the start of the block to the validity bitmap.
    pub validity_off: u32,
    /// Byte offset from the start of the block to the values buffer or view slots.
    pub values_off: u32,
    /// Reserved length in bytes of the validity bitmap region, including alignment padding.
    pub validity_len: u32,
    /// Reserved length in bytes of the values region, including alignment padding.
    pub values_len: u32,
}

impl ColumnLayout {
    pub const fn desc(self) -> ColumnDesc {
        ColumnDesc {
            type_tag: self.type_tag.to_raw(),
            flags: self.flags.bits(),
            validity_off: self.validity_off,
            values_off: self.values_off,
            null_count: 0,
            reserved0: 0,
        }
    }
}

/// Full planned front-and-tail page layout for one block shape.
///
/// A `LayoutPlan` describes:
///
/// - the fixed header and column-descriptor prefix
/// - the reserved front region for all fixed-size row-indexed buffers
/// - the start of the shared tail arena used by long `ByteView` payloads
///
/// The plan is deterministic for a given schema, `max_rows`, and `block_size`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LayoutPlan {
    block_size: u32,
    max_rows: u32,
    front_base: u32,
    pool_base: u32,
    columns: Vec<ColumnLayout>,
}

impl LayoutPlan {
    pub fn new(specs: &[ColumnSpec], max_rows: u32, block_size: u32) -> Result<Self, LayoutError> {
        let col_count = u16::try_from(specs.len()).map_err(|_| LayoutError::TooManyColumns {
            actual: specs.len(),
        })?;
        let front_base = align_up_u32(
            checked_u32(
                size_of::<BlockHeader>()
                    .checked_add(
                        specs
                            .len()
                            .checked_mul(size_of::<ColumnDesc>())
                            .ok_or(LayoutError::SizeOverflow)?,
                    )
                    .ok_or(LayoutError::SizeOverflow)?,
            )?,
            BUFFER_ALIGNMENT,
        )?;

        let mut cursor = front_base;
        let mut columns = Vec::with_capacity(specs.len());
        for spec in specs {
            let flags = spec.flags();
            let validity_len = align_up_u32(bitmap_bytes(max_rows), BUFFER_ALIGNMENT)?;
            let values_len = spec.type_tag.values_reserved_len(max_rows)?;

            let validity_off = cursor;
            cursor = cursor
                .checked_add(validity_len)
                .ok_or(LayoutError::SizeOverflow)?;
            let values_off = cursor;
            cursor = cursor
                .checked_add(values_len)
                .ok_or(LayoutError::SizeOverflow)?;

            columns.push(ColumnLayout {
                type_tag: spec.type_tag,
                flags,
                validity_off,
                values_off,
                validity_len,
                values_len,
            });
        }

        if cursor > block_size {
            return Err(LayoutError::LayoutDoesNotFit {
                block_size,
                required: cursor,
            });
        }

        let _ = col_count;
        Ok(Self {
            block_size,
            max_rows,
            front_base,
            pool_base: cursor,
            columns,
        })
    }

    pub fn from_arrow_schema(
        schema: &Schema,
        max_rows: u32,
        block_size: u32,
    ) -> Result<Self, LayoutError> {
        let mut specs = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            specs.push(ColumnSpec::new(
                TypeTag::from_arrow_data_type(index, field.data_type())?,
                field.is_nullable(),
            ));
        }
        Self::new(&specs, max_rows, block_size)
    }

    pub fn validate(header: &BlockHeader, descs: &[ColumnDesc]) -> Result<Self, LayoutError> {
        validate_header(header, descs.len())?;
        if usize::from(header.col_count) != descs.len() {
            return Err(LayoutError::ColumnCountMismatch {
                expected: usize::from(header.col_count),
                actual: descs.len(),
            });
        }

        let mut specs = Vec::with_capacity(descs.len());
        for (index, desc) in descs.iter().enumerate() {
            let type_tag = desc.type_tag()?;
            let flags = desc.flags();
            if flags.is_view() != type_tag.is_view() {
                return Err(LayoutError::InconsistentViewFlag {
                    index,
                    type_tag,
                    flags: flags.bits(),
                });
            }
            specs.push(ColumnSpec::new(type_tag, flags.is_nullable()));
        }

        let plan = Self::new(&specs, header.max_rows, header.block_size)?;
        if plan.front_base != header.front_base {
            return Err(LayoutError::FrontBaseMismatch {
                expected: plan.front_base,
                actual: header.front_base,
            });
        }
        if plan.pool_base != header.pool_base {
            return Err(LayoutError::PoolBaseMismatch {
                expected: plan.pool_base,
                actual: header.pool_base,
            });
        }

        for (index, (expected, actual)) in plan.columns.iter().zip(descs).enumerate() {
            let expected = expected.desc();
            if actual.type_tag != expected.type_tag
                || actual.flags != expected.flags
                || actual.validity_off != expected.validity_off
                || actual.values_off != expected.values_off
            {
                return Err(LayoutError::ColumnDescMismatch { index });
            }
        }

        Ok(plan)
    }

    pub fn block_header(&self, schema_id: u32) -> BlockHeader {
        BlockHeader {
            magic: BLOCK_MAGIC,
            version: BLOCK_VERSION,
            flags: BlockFlags::NONE.bits(),
            block_size: self.block_size,
            schema_id,
            max_rows: self.max_rows,
            row_count: 0,
            col_count: u16::try_from(self.columns.len()).expect("validated at construction"),
            reserved0: 0,
            front_base: self.front_base,
            pool_base: self.pool_base,
            tail_cursor: self.block_size,
            reserved1: 0,
        }
    }

    pub fn block_size(&self) -> u32 {
        self.block_size
    }

    pub fn max_rows(&self) -> u32 {
        self.max_rows
    }

    pub fn front_base(&self) -> u32 {
        self.front_base
    }

    pub fn pool_base(&self) -> u32 {
        self.pool_base
    }

    pub fn shared_pool_capacity(&self) -> u32 {
        self.block_size - self.pool_base
    }

    pub fn columns(&self) -> &[ColumnLayout] {
        &self.columns
    }

    pub fn column_descs(&self) -> impl ExactSizeIterator<Item = ColumnDesc> + '_ {
        self.columns.iter().copied().map(ColumnLayout::desc)
    }
}

/// Raw block header written at the front of each page.
///
/// The header describes the global bounds of the page layout:
///
/// - total block size
/// - schema identity chosen by the caller
/// - maximum and current row counts
/// - the reserved front region start/end
/// - the current tail cursor for the shared view payload pool
///
/// Offsets are relative to the start of the block.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    /// Format magic. Must be [`BLOCK_MAGIC`].
    pub magic: u32,
    /// Format version. Must be [`BLOCK_VERSION`].
    pub version: u16,
    /// Raw block flags interpreted as [`BlockFlags`].
    pub flags: u16,
    /// Total block size in bytes.
    pub block_size: u32,
    /// Caller-defined schema identifier associated with this layout.
    pub schema_id: u32,
    /// Maximum number of rows reserved in the front region.
    pub max_rows: u32,
    /// Number of rows currently populated in the block.
    pub row_count: u32,
    /// Number of column descriptors that immediately follow the header.
    pub col_count: u16,
    /// Reserved for future format extensions. Must be zero in v1.
    pub reserved0: u16,
    /// Start offset of the reserved front region after header and descriptors.
    pub front_base: u32,
    /// First byte of the shared tail pool.
    pub pool_base: u32,
    /// Current tail cursor into the shared tail pool.
    pub tail_cursor: u32,
    /// Reserved for future format extensions. Must be zero in v1.
    pub reserved1: u32,
}

impl BlockHeader {
    pub fn flags(&self) -> BlockFlags {
        BlockFlags(self.flags)
    }

    pub fn shared_pool_capacity(&self) -> Result<u32, LayoutError> {
        self.block_size
            .checked_sub(self.pool_base)
            .ok_or(LayoutError::InvalidHeaderBounds)
    }
}

/// Raw per-column descriptor stored immediately after [`BlockHeader`].
///
/// A descriptor names the stable on-page type and the two front-region buffers
/// used by the column:
///
/// - `validity_off` points to the validity bitmap
/// - `values_off` points either to fixed-width values or to `ByteView` slots
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnDesc {
    /// Stable on-page type tag encoded as [`TypeTag::to_raw`].
    pub type_tag: u16,
    /// Raw per-column flags interpreted as [`ColumnFlags`].
    pub flags: u16,
    /// Byte offset from block start to the validity bitmap.
    pub validity_off: u32,
    /// Byte offset from block start to the values buffer or `ByteView` slots.
    pub values_off: u32,
    /// Number of null rows currently present in the column.
    pub null_count: u32,
    /// Reserved for future format extensions. Must be zero in v1.
    pub reserved0: u32,
}

impl ColumnDesc {
    pub fn type_tag(&self) -> Result<TypeTag, LayoutError> {
        TypeTag::from_raw(self.type_tag)
    }

    pub fn flags(&self) -> ColumnFlags {
        ColumnFlags(self.flags)
    }
}

/// Fixed-size 16-byte view slot used for `Utf8View` and `BinaryView` columns.
///
/// Layout:
///
/// - `len <= 12`: payload is stored inline in `data[..len]`
/// - `len > 12`: payload is stored out-of-line in the shared tail pool, with
///   `data[0..4] = prefix4`, `data[4..8] = buffer_index`, and
///   `data[8..12] = offset-from-pool-base`
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ByteView {
    len: i32,
    data: [u8; VIEW_INLINE_LEN],
}

impl ByteView {
    pub fn new_inline(bytes: &[u8]) -> Result<Self, LayoutError> {
        let len = i32::try_from(bytes.len())
            .map_err(|_| LayoutError::InvalidByteViewLength { len: bytes.len() })?;
        if bytes.len() > VIEW_INLINE_LEN {
            return Err(LayoutError::InlineValueTooLarge {
                len: bytes.len(),
                inline_capacity: VIEW_INLINE_LEN,
            });
        }
        let mut data = [0u8; VIEW_INLINE_LEN];
        data[..bytes.len()].copy_from_slice(bytes);
        Ok(Self { len, data })
    }

    pub fn new_outline(bytes: &[u8], offset: u32) -> Result<Self, LayoutError> {
        let len = i32::try_from(bytes.len())
            .map_err(|_| LayoutError::InvalidByteViewLength { len: bytes.len() })?;
        if bytes.len() <= VIEW_INLINE_LEN {
            return Err(LayoutError::OutlineValueTooSmall {
                len: bytes.len(),
                inline_capacity: VIEW_INLINE_LEN,
            });
        }
        let offset =
            i32::try_from(offset).map_err(|_| LayoutError::InvalidViewOffsetEncoding { offset })?;

        let mut data = [0u8; VIEW_INLINE_LEN];
        let prefix_len = bytes.len().min(VIEW_PREFIX_LEN);
        data[..prefix_len].copy_from_slice(&bytes[..prefix_len]);
        data[4..8].copy_from_slice(&SHARED_VIEW_BUFFER_INDEX.to_le_bytes());
        data[8..12].copy_from_slice(&offset.to_le_bytes());
        Ok(Self { len, data })
    }

    pub fn len(&self) -> Result<usize, LayoutError> {
        if self.len < 0 {
            return Err(LayoutError::NegativeByteViewLength { len: self.len });
        }
        usize::try_from(self.len).map_err(|_| LayoutError::InvalidByteViewLength {
            len: self.len as usize,
        })
    }

    pub fn is_empty(&self) -> Result<bool, LayoutError> {
        Ok(self.len()? == 0)
    }

    pub fn is_inline(&self) -> Result<bool, LayoutError> {
        Ok(self.len()? <= VIEW_INLINE_LEN)
    }

    pub fn inline_bytes(&self) -> Result<Option<&[u8]>, LayoutError> {
        let len = self.len()?;
        if len <= VIEW_INLINE_LEN {
            Ok(Some(&self.data[..len]))
        } else {
            Ok(None)
        }
    }

    pub fn prefix4(&self) -> [u8; VIEW_PREFIX_LEN] {
        self.data[..VIEW_PREFIX_LEN]
            .try_into()
            .expect("constant slice length")
    }

    pub fn buffer_index(&self) -> Result<Option<i32>, LayoutError> {
        if self.is_inline()? {
            return Ok(None);
        }
        Ok(Some(i32::from_le_bytes(
            self.data[4..8].try_into().expect("constant slice length"),
        )))
    }

    pub fn offset(&self) -> Result<Option<u32>, LayoutError> {
        if self.is_inline()? {
            return Ok(None);
        }
        let raw = i32::from_le_bytes(self.data[8..12].try_into().expect("constant slice length"));
        if raw < 0 {
            return Err(LayoutError::NegativeViewOffset { offset: raw });
        }
        Ok(Some(
            u32::try_from(raw).expect("non-negative i32 fits into u32"),
        ))
    }

    pub fn validate(&self, pool_capacity: u32) -> Result<(), LayoutError> {
        let len = self.len()?;
        if len <= VIEW_INLINE_LEN {
            return Ok(());
        }

        let buffer_index = self
            .buffer_index()?
            .expect("validated long view always has buffer index");
        if buffer_index != SHARED_VIEW_BUFFER_INDEX {
            return Err(LayoutError::InvalidViewBufferIndex {
                actual: buffer_index,
            });
        }

        let offset = self
            .offset()?
            .expect("validated long view always has offset");
        let end = offset
            .checked_add(
                u32::try_from(len).map_err(|_| LayoutError::InvalidByteViewLength { len })?,
            )
            .ok_or(LayoutError::SizeOverflow)?;
        if end > pool_capacity {
            return Err(LayoutError::ViewOffsetOutOfBounds {
                offset,
                len: u32::try_from(len).expect("validated conversion"),
                pool_capacity,
            });
        }
        Ok(())
    }
}

/// Returns the number of bytes needed to store a bitmap for `row_count` rows.
pub fn bitmap_bytes(row_count: u32) -> u32 {
    row_count.div_ceil(8)
}

/// Validates the global header invariants for a block instance.
///
/// This checks the fixed block metadata only. It does not validate that the
/// provided [`ColumnDesc`] values match a planned schema; use
/// [`LayoutPlan::validate`] for full reconstruction and validation.
pub fn validate_header(header: &BlockHeader, desc_count: usize) -> Result<(), LayoutError> {
    if header.magic != BLOCK_MAGIC {
        return Err(LayoutError::InvalidMagic {
            expected: BLOCK_MAGIC,
            actual: header.magic,
        });
    }
    if header.version != BLOCK_VERSION {
        return Err(LayoutError::InvalidVersion {
            expected: BLOCK_VERSION,
            actual: header.version,
        });
    }
    if header.row_count > header.max_rows {
        return Err(LayoutError::RowCountExceedsMaxRows {
            row_count: header.row_count,
            max_rows: header.max_rows,
        });
    }
    if usize::from(header.col_count) != desc_count {
        return Err(LayoutError::ColumnCountMismatch {
            expected: usize::from(header.col_count),
            actual: desc_count,
        });
    }
    let expected_front_base = align_up_u32(
        checked_u32(
            size_of::<BlockHeader>()
                .checked_add(
                    desc_count
                        .checked_mul(size_of::<ColumnDesc>())
                        .ok_or(LayoutError::SizeOverflow)?,
                )
                .ok_or(LayoutError::SizeOverflow)?,
        )?,
        BUFFER_ALIGNMENT,
    )?;
    if header.front_base != expected_front_base {
        return Err(LayoutError::FrontBaseMismatch {
            expected: expected_front_base,
            actual: header.front_base,
        });
    }
    if header.front_base > header.pool_base
        || header.pool_base > header.tail_cursor
        || header.tail_cursor > header.block_size
    {
        return Err(LayoutError::InvalidHeaderBounds);
    }
    if header.front_base % BUFFER_ALIGNMENT != 0 || header.pool_base % BUFFER_ALIGNMENT != 0 {
        return Err(LayoutError::MisalignedFrontRegion {
            front_base: header.front_base,
            pool_base: header.pool_base,
            alignment: BUFFER_ALIGNMENT,
        });
    }
    Ok(())
}

fn align_up_u32(value: u32, alignment: u32) -> Result<u32, LayoutError> {
    if alignment == 0 || !alignment.is_power_of_two() {
        return Err(LayoutError::InvalidAlignment { alignment });
    }
    let mask = alignment - 1;
    value
        .checked_add(mask)
        .map(|v| v & !mask)
        .ok_or(LayoutError::SizeOverflow)
}

fn checked_u32(value: usize) -> Result<u32, LayoutError> {
    u32::try_from(value).map_err(|_| LayoutError::SizeOverflow)
}

fn aligned_mul(lhs: u32, rhs: u32) -> Result<u32, LayoutError> {
    align_up_u32(
        lhs.checked_mul(rhs).ok_or(LayoutError::SizeOverflow)?,
        BUFFER_ALIGNMENT,
    )
}
