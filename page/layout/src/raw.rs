//! Advanced raw `#[repr(C)]` structs stored directly in the page.
//!
//! Most callers should use [`crate::LayoutPlan`], [`crate::BlockRef`], and
//! [`crate::BlockMut`] instead of manipulating these structs directly.

use crate::constants::{SHARED_VIEW_BUFFER_INDEX, VIEW_INLINE_LEN, VIEW_PREFIX_LEN};
use crate::{BlockFlags, ColumnFlags, LayoutError, TypeTag};

/// Raw block header written at the front of each page.
///
/// The header describes the global bounds of the page layout:
///
/// - total block size
/// - maximum and current row counts
/// - the reserved front region start/end
/// - the current tail cursor for the shared view payload pool
///
/// Offsets are relative to the start of the block.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockHeader {
    /// Format magic. Must be [`crate::constants::BLOCK_MAGIC`].
    pub magic: u32,
    /// Format version. Must be [`crate::constants::BLOCK_VERSION`].
    pub version: u16,
    /// Raw block flags interpreted as [`BlockFlags`].
    pub flags: u16,
    /// Total block size in bytes.
    pub block_size: u32,
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
    /// Returns the typed block flags.
    pub fn flags(&self) -> BlockFlags {
        BlockFlags::from_bits(self.flags)
    }

    /// Returns the total shared-pool capacity `[pool_base, block_size)`.
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
    /// Decodes the typed on-page type tag.
    pub fn type_tag(&self) -> Result<TypeTag, LayoutError> {
        TypeTag::from_raw(self.type_tag)
    }

    /// Returns the typed column flags.
    pub fn flags(&self) -> ColumnFlags {
        ColumnFlags::from_bits(self.flags)
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
    /// Builds an inline view value whose entire payload fits inside the slot.
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

    /// Builds an out-of-line view pointing into the shared tail pool.
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

    /// Returns the logical payload length.
    pub fn len(&self) -> Result<usize, LayoutError> {
        if self.len < 0 {
            return Err(LayoutError::NegativeByteViewLength { len: self.len });
        }
        usize::try_from(self.len).map_err(|_| LayoutError::InvalidByteViewLength {
            len: self.len as usize,
        })
    }

    /// Returns whether the logical payload is empty.
    pub fn is_empty(&self) -> Result<bool, LayoutError> {
        Ok(self.len()? == 0)
    }

    /// Returns whether the payload is stored inline.
    pub fn is_inline(&self) -> Result<bool, LayoutError> {
        Ok(self.len()? <= VIEW_INLINE_LEN)
    }

    /// Returns inline payload bytes when the slot is inline.
    pub fn inline_bytes(&self) -> Result<Option<&[u8]>, LayoutError> {
        let len = self.len()?;
        if len <= VIEW_INLINE_LEN {
            Ok(Some(&self.data[..len]))
        } else {
            Ok(None)
        }
    }

    /// Returns the first four payload bytes stored in the slot.
    pub fn prefix4(&self) -> [u8; VIEW_PREFIX_LEN] {
        self.data[..VIEW_PREFIX_LEN]
            .try_into()
            .expect("constant slice length")
    }

    /// Returns the out-of-line shared-buffer index for long values.
    pub fn buffer_index(&self) -> Result<Option<i32>, LayoutError> {
        if self.is_inline()? {
            return Ok(None);
        }
        Ok(Some(i32::from_le_bytes(
            self.data[4..8].try_into().expect("constant slice length"),
        )))
    }

    /// Returns the out-of-line shared-pool offset for long values.
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

    /// Validates that a slot is self-consistent against `pool_capacity`.
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
