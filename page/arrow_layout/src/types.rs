//! Public type-level layout descriptors and planning inputs.

use crate::bitmap::bitmap_bytes;
use crate::constants::{BUFFER_ALIGNMENT, UUID_WIDTH_BYTES};
use crate::internals::{align_up_u32, aligned_mul, checked_u32};
use crate::raw::ColumnDesc;
use crate::LayoutError;
use arrow_schema::DataType;

/// Raw block-level flags stored in [`crate::raw::BlockHeader::flags`].
///
/// No block flags are defined in v1; the type exists to keep the on-page
/// representation explicit and extensible.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct BlockFlags(u16);

impl BlockFlags {
    /// No block-level flags are set.
    pub const NONE: Self = Self(0);

    pub(crate) const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Returns the raw on-page bits.
    pub const fn bits(self) -> u16 {
        self.0
    }
}

/// Raw per-column flags stored in [`crate::raw::ColumnDesc::flags`].
///
/// These bits describe nullability and whether the column uses the `ByteView`
/// representation backed by the shared tail pool.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct ColumnFlags(u16);

impl ColumnFlags {
    /// No column-level flags are set.
    pub const NONE: Self = Self(0);
    /// The column has a validity bitmap and may contain nulls.
    pub const NULLABLE: Self = Self(1 << 0);
    /// The column stores 16-byte `ByteView` slots instead of fixed-width values.
    pub const VIEW: Self = Self(1 << 1);

    pub(crate) const fn from_bits(bits: u16) -> Self {
        Self(bits)
    }

    /// Returns the raw on-page bits.
    pub const fn bits(self) -> u16 {
        self.0
    }

    /// Returns whether all bits from `other` are set.
    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    /// Returns whether the column is nullable.
    pub const fn is_nullable(self) -> bool {
        self.contains(Self::NULLABLE)
    }

    /// Returns whether the column stores `ByteView` slots.
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

/// Stable on-page type tag stored in [`crate::raw::ColumnDesc::type_tag`].
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
    /// 16-byte [`crate::ByteView`] slots for UTF-8 string views.
    Utf8View = 8,
    /// 16-byte [`crate::ByteView`] slots for binary views.
    BinaryView = 9,
}

impl TypeTag {
    /// Decodes a raw on-page type tag.
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

    /// Encodes the type tag into its raw on-page representation.
    pub const fn to_raw(self) -> u16 {
        self as u16
    }

    /// Returns whether the type stores `ByteView` slots.
    pub const fn is_view(self) -> bool {
        matches!(self, Self::Utf8View | Self::BinaryView)
    }

    /// Returns the fixed per-row values width for this type, if any.
    pub const fn values_row_width(self) -> Option<u32> {
        match self {
            Self::Boolean => None,
            Self::Int16 => Some(2),
            Self::Int32 | Self::Float32 => Some(4),
            Self::Int64 | Self::Float64 => Some(8),
            Self::Uuid | Self::Utf8View | Self::BinaryView => Some(16),
        }
    }

    /// Returns the reserved values-buffer length for `max_rows`, including
    /// alignment padding where required by the layout.
    pub fn values_reserved_len(self, max_rows: u32) -> Result<u32, LayoutError> {
        match self {
            Self::Boolean => align_up_u32(bitmap_bytes(max_rows), BUFFER_ALIGNMENT),
            Self::Int16 => aligned_mul(max_rows, 2),
            Self::Int32 | Self::Float32 => aligned_mul(max_rows, 4),
            Self::Int64 | Self::Float64 => aligned_mul(max_rows, 8),
            Self::Uuid | Self::Utf8View | Self::BinaryView => aligned_mul(max_rows, 16),
        }
    }

    /// Returns the used values-buffer length for `row_count`, excluding any
    /// trailing reserved slack.
    pub fn values_used_len(self, row_count: u32) -> Result<u32, LayoutError> {
        match self {
            Self::Boolean => Ok(bitmap_bytes(row_count)),
            Self::Int16 => checked_u32(
                usize::try_from(row_count)
                    .map_err(|_| LayoutError::SizeOverflow)?
                    .checked_mul(2)
                    .ok_or(LayoutError::SizeOverflow)?,
            ),
            Self::Int32 | Self::Float32 => checked_u32(
                usize::try_from(row_count)
                    .map_err(|_| LayoutError::SizeOverflow)?
                    .checked_mul(4)
                    .ok_or(LayoutError::SizeOverflow)?,
            ),
            Self::Int64 | Self::Float64 => checked_u32(
                usize::try_from(row_count)
                    .map_err(|_| LayoutError::SizeOverflow)?
                    .checked_mul(8)
                    .ok_or(LayoutError::SizeOverflow)?,
            ),
            Self::Uuid | Self::Utf8View | Self::BinaryView => checked_u32(
                usize::try_from(row_count)
                    .map_err(|_| LayoutError::SizeOverflow)?
                    .checked_mul(16)
                    .ok_or(LayoutError::SizeOverflow)?,
            ),
        }
    }

    /// Maps a supported Arrow data type into the v1 layout surface.
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
    /// Creates a new logical column specification.
    pub const fn new(type_tag: TypeTag, nullable: bool) -> Self {
        Self { type_tag, nullable }
    }

    /// Returns the resolved raw layout flags for this specification.
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
/// to derive the corresponding [`crate::raw::ColumnDesc`] and to reason about
/// reserved front-region lengths.
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
    /// Converts the planned layout into its raw on-page descriptor form.
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
