//! Public format constants for the raw page layout.

/// Magic value stored in [`crate::raw::BlockHeader::magic`].
pub const BLOCK_MAGIC: u32 = 0x3242_4150; // "PAB2"

/// Layout format version stored in [`crate::raw::BlockHeader::version`].
pub const BLOCK_VERSION: u16 = 1;

/// Base alignment used for front-region buffers in the raw block.
pub const BUFFER_ALIGNMENT: u32 = 16;

/// Fixed alignment bias used so front-region buffers stay physically aligned
/// when the block is embedded inside a `page_transfer` payload.
pub const BUFFER_ALIGNMENT_BIAS: u32 = 12;

/// Inline payload capacity for a [`crate::ByteView`].
pub const VIEW_INLINE_LEN: usize = 12;

/// Prefix bytes copied inline for an out-of-line [`crate::ByteView`].
pub const VIEW_PREFIX_LEN: usize = 4;

/// Shared variadic-buffer index used by all long view values in v1.
pub const SHARED_VIEW_BUFFER_INDEX: i32 = 0;

/// Fixed width in bytes of layout `Uuid` values.
pub const UUID_WIDTH_BYTES: u32 = 16;
