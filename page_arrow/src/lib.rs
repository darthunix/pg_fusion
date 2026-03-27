//! Zero-copy Apache Arrow batch import over `page_transfer`.
//!
//! `page_arrow` consumes a [`page_transfer::ReceivedPage`] whose payload is an
//! Arrow IPC-encoded record batch and returns a plain [`arrow_array::RecordBatch`]
//! backed directly by the shared-memory page bytes.
//!
//! Ordinary imported batches extend page lifetime through Arrow buffer ownership.
//! Zero-buffer batches such as empty-schema or `Null`-only payloads decode as
//! owned Arrow structures and may release the page before `import()` returns.
//! Holding an imported batch does not keep `page_transfer::PageRx` busy for
//! later accepts.

mod error;

#[cfg(test)]
mod tests;

use arrow_array::{Array, RecordBatch};
use arrow_buffer::alloc::Allocation;
use arrow_buffer::Buffer;
use arrow_ipc::reader::FileDecoder;
use arrow_ipc::{root_as_message, Block, MessageHeader, MetadataVersion};
use arrow_schema::{DataType, SchemaRef};
pub use error::{ConfigError, ImportError};
use page_transfer::{MessageKind, ReceivedPage};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex, MutexGuard};

const BATCH_PAGE_MAGIC: u32 = 0x3142_4150; // "PAB1"
const BATCH_PAGE_VERSION: u16 = 1;
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];
const BATCH_PAGE_HEADER_LEN: usize = 12;
const IPC_ALIGNMENT: usize = 16;

/// `page_transfer::MessageKind` for Arrow IPC pages imported by this crate.
pub const ARROW_IPC_BATCH_KIND: MessageKind = 0x4152;

/// Importer for Arrow IPC batches stored in `page_transfer` pages.
#[derive(Clone, Debug)]
pub struct ArrowPageDecoder {
    schema: SchemaRef,
}

impl ArrowPageDecoder {
    /// Create a decoder for pages that should decode into `schema`.
    pub fn new(schema: SchemaRef) -> Result<Self, ConfigError> {
        if schema
            .fields()
            .iter()
            .any(|field| data_type_has_dictionary(field.data_type()))
        {
            return Err(ConfigError::UnsupportedDictionarySchema);
        }

        Ok(Self { schema })
    }

    /// Import one `ReceivedPage` as a zero-copy Arrow `RecordBatch`.
    pub fn import(&self, page: ReceivedPage) -> Result<RecordBatch, ImportError> {
        if page.kind() != ARROW_IPC_BATCH_KIND {
            return Err(ImportError::WrongKind {
                expected: ARROW_IPC_BATCH_KIND,
                actual: page.kind(),
            });
        }
        if page.flags() != 0 {
            return Err(ImportError::UnsupportedFlags {
                actual: page.flags(),
            });
        }

        let (header, expected_ipc_len, meta_len, body_len, metadata_version) = {
            let payload = page.payload();
            let header = decode_batch_page_header(payload)?;
            let ipc = payload
                .get(BATCH_PAGE_HEADER_LEN..)
                .ok_or(ImportError::PayloadTooSmall {
                    expected: BATCH_PAGE_HEADER_LEN,
                    actual: payload.len(),
                })?;

            let meta_len =
                usize::try_from(header.meta_len).map_err(|_| ImportError::InvalidMetaLen {
                    meta_len: header.meta_len,
                    available: ipc.len(),
                })?;
            if meta_len == 0 || meta_len > ipc.len() {
                return Err(ImportError::InvalidMetaLen {
                    meta_len: header.meta_len,
                    available: ipc.len(),
                });
            }
            if meta_len % IPC_ALIGNMENT != 0 {
                return Err(ImportError::MisalignedIpcBody { actual: meta_len });
            }

            let message = parse_encapsulated_message(&ipc[..meta_len])?;
            if message.header_type() != MessageHeader::RecordBatch {
                return Err(ImportError::UnexpectedMessageHeader {
                    actual: format!("{:?}", message.header_type()),
                });
            }
            let batch = message.header_as_record_batch().ok_or_else(|| {
                ImportError::UnexpectedMessageHeader {
                    actual: format!("{:?}", message.header_type()),
                }
            })?;
            if batch.compression().is_some() {
                return Err(ImportError::UnsupportedCompression);
            }
            let metadata_version = message.version();
            if metadata_version != MetadataVersion::V5 {
                return Err(ImportError::UnsupportedMetadataVersion {
                    expected: MetadataVersion::V5,
                    actual: metadata_version,
                });
            }
            let declared_body_len =
                usize::try_from(message.bodyLength()).map_err(|_| ImportError::InvalidBodyLen {
                    actual: message.bodyLength(),
                })?;
            let body_len = align_up_to(declared_body_len, IPC_ALIGNMENT).ok_or(
                ImportError::InvalidBodyLen {
                    actual: message.bodyLength(),
                },
            )?;
            let expected_ipc_len =
                meta_len
                    .checked_add(body_len)
                    .ok_or(ImportError::PayloadRangeOverflow {
                        offset: meta_len,
                        len: body_len,
                    })?;
            if ipc.len() < expected_ipc_len {
                return Err(ImportError::TruncatedIpcBody {
                    expected_at_least: expected_ipc_len,
                    actual: ipc.len(),
                });
            }
            if ipc.len() > expected_ipc_len {
                return Err(ImportError::TrailingPayloadBytes {
                    expected: expected_ipc_len,
                    actual: ipc.len(),
                });
            }

            (
                header,
                expected_ipc_len,
                meta_len,
                body_len,
                metadata_version,
            )
        };

        let owner = Arc::new(PageAllocationOwner::new(page));
        let data_buffer = owner.buffer_from_payload(BATCH_PAGE_HEADER_LEN, expected_ipc_len)?;
        let decoder = FileDecoder::new(Arc::clone(&self.schema), metadata_version)
            .with_require_alignment(true);
        let block = Block::new(
            0,
            i32::try_from(meta_len).map_err(|_| ImportError::InvalidMetaLen {
                meta_len: header.meta_len,
                available: expected_ipc_len,
            })?,
            i64::try_from(body_len).expect("usize always fits into i64 on supported targets"),
        );
        let batch = decoder
            .read_record_batch(&block, &data_buffer)?
            .ok_or(ImportError::UnexpectedEmptyMessage)?;
        let zero_buffer_batch = batch
            .columns()
            .iter()
            .all(|column| column.get_buffer_memory_size() == 0);
        drop(data_buffer);
        debug_assert!(
            zero_buffer_batch || Arc::strong_count(&owner) > 1,
            "non-zero-buffer page_arrow import must retain page-backed ownership",
        );
        Ok(batch)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BatchPageHeader {
    meta_len: u32,
}

fn decode_batch_page_header(payload: &[u8]) -> Result<BatchPageHeader, ImportError> {
    if payload.len() < BATCH_PAGE_HEADER_LEN {
        return Err(ImportError::PayloadTooSmall {
            expected: BATCH_PAGE_HEADER_LEN,
            actual: payload.len(),
        });
    }

    let magic = u32::from_le_bytes(payload[0..4].try_into().expect("length checked"));
    if magic != BATCH_PAGE_MAGIC {
        return Err(ImportError::InvalidMagic {
            expected: BATCH_PAGE_MAGIC,
            actual: magic,
        });
    }

    let version = u16::from_le_bytes(payload[4..6].try_into().expect("length checked"));
    if version != BATCH_PAGE_VERSION {
        return Err(ImportError::UnsupportedBatchPageVersion {
            expected: BATCH_PAGE_VERSION,
            actual: version,
        });
    }

    let reserved = u16::from_le_bytes(payload[6..8].try_into().expect("length checked"));
    if reserved != 0 {
        return Err(ImportError::NonZeroReserved { actual: reserved });
    }

    Ok(BatchPageHeader {
        meta_len: u32::from_le_bytes(payload[8..12].try_into().expect("length checked")),
    })
}

fn parse_encapsulated_message(bytes: &[u8]) -> Result<arrow_ipc::Message<'_>, ImportError> {
    if bytes.len() < 4 {
        return Err(ImportError::TruncatedEncapsulatedMessage {
            expected_at_least: 4,
            actual: bytes.len(),
        });
    }

    let (prefix_len, message_len) = if bytes[..4] == CONTINUATION_MARKER {
        if bytes.len() < 8 {
            return Err(ImportError::TruncatedEncapsulatedMessage {
                expected_at_least: 8,
                actual: bytes.len(),
            });
        }
        (
            8usize,
            i32::from_le_bytes(bytes[4..8].try_into().expect("length checked")),
        )
    } else {
        (
            4usize,
            i32::from_le_bytes(bytes[..4].try_into().expect("length checked")),
        )
    };

    if message_len <= 0 {
        return Err(ImportError::InvalidEncapsulatedMessageLength {
            actual: message_len,
        });
    }
    let message_len = usize::try_from(message_len).map_err(|_| {
        ImportError::InvalidEncapsulatedMessageLength {
            actual: message_len,
        }
    })?;
    let end = prefix_len
        .checked_add(message_len)
        .ok_or(ImportError::InvalidEncapsulatedMessageLength { actual: i32::MAX })?;
    if end > bytes.len() {
        return Err(ImportError::TruncatedEncapsulatedMessage {
            expected_at_least: end,
            actual: bytes.len(),
        });
    }

    root_as_message(&bytes[prefix_len..end])
        .map_err(|err| ImportError::InvalidIpcMessage(err.to_string()))
}

fn align_up_to(len: usize, alignment: usize) -> Option<usize> {
    let remainder = len % alignment;
    if remainder == 0 {
        Some(len)
    } else {
        len.checked_add(alignment - remainder)
    }
}

fn data_type_has_dictionary(data_type: &DataType) -> bool {
    match data_type {
        DataType::Dictionary(_, _) => true,
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::Map(field, _) => data_type_has_dictionary(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| data_type_has_dictionary(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| data_type_has_dictionary(field.data_type())),
        DataType::RunEndEncoded(run_ends, values) => {
            data_type_has_dictionary(run_ends.data_type())
                || data_type_has_dictionary(values.data_type())
        }
        _ => false,
    }
}

struct PageAllocationOwner {
    page: Mutex<Option<ReceivedPage>>,
}

impl PageAllocationOwner {
    fn new(page: ReceivedPage) -> Self {
        Self {
            page: Mutex::new(Some(page)),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Option<ReceivedPage>> {
        self.page.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn buffer_from_payload(
        self: &Arc<Self>,
        offset: usize,
        len: usize,
    ) -> Result<Buffer, ImportError> {
        let ptr = {
            let guard = self.lock();
            let page = guard
                .as_ref()
                .expect("page owner must retain the page for the buffer lifetime");
            let payload = page.payload();
            let end = offset
                .checked_add(len)
                .ok_or(ImportError::PayloadRangeOverflow { offset, len })?;
            let slice = payload
                .get(offset..end)
                .ok_or(ImportError::PayloadOutOfBounds {
                    payload_len: payload.len(),
                    offset,
                    len,
                })?;
            NonNull::new(slice.as_ptr() as *mut u8).expect("slice pointers are never null")
        };

        let allocation: Arc<dyn Allocation> = self.clone();
        // The page remains owned by `allocation` until the last Arrow buffer drops.
        Ok(unsafe { Buffer::from_custom_allocation(ptr, len, allocation) })
    }
}
