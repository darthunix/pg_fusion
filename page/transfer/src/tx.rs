use crate::error::{write_zero, TxError};
use crate::page::{encode_page_header, MessageKind, PageHeader, PAGE_HEADER_LEN};
use crate::wire::{CloseFrame, OwnedFrame, PageFrame};
use pool::{PageDescriptor, PageLease, PagePool};
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

const MAX_PAYLOAD_LEN: usize = u32::MAX as usize;

pub(crate) fn validate_page_size(page_size: usize) -> Result<(), TxError> {
    if page_size < PAGE_HEADER_LEN {
        return Err(TxError::PageTooSmall {
            required: PAGE_HEADER_LEN,
            actual: page_size,
        });
    }
    let payload_capacity = page_size - PAGE_HEADER_LEN;
    if payload_capacity > MAX_PAYLOAD_LEN {
        return Err(TxError::PayloadCapacityTooLarge {
            capacity: payload_capacity,
            max: MAX_PAYLOAD_LEN,
        });
    }
    Ok(())
}

/// Sender-side page handoff handle.
///
/// `PageTx` is cheap to clone, runtime-agnostic, and safe to use from both
/// synchronous and asynchronous code. It enforces a single in-flight page or
/// close frame at a time for one sender stream.
#[derive(Clone)]
pub struct PageTx {
    inner: Arc<TxInner>,
}

struct TxInner {
    pool: PagePool,
    phase: AtomicU8,
    next_transfer_id: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum TxPhase {
    Idle = 0,
    Writing = 1,
    OutboundPending = 2,
    Closed = 3,
}

impl TxPhase {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Idle,
            1 => Self::Writing,
            2 => Self::OutboundPending,
            3 => Self::Closed,
            _ => unreachable!("invalid tx phase {raw}"),
        }
    }
}

impl TxInner {
    fn current_phase(&self) -> TxPhase {
        TxPhase::from_raw(self.phase.load(Ordering::Acquire))
    }

    fn enter_writing(&self) -> Result<(), TxError> {
        loop {
            match self.current_phase() {
                TxPhase::Idle => {
                    if self
                        .phase
                        .compare_exchange(
                            TxPhase::Idle as u8,
                            TxPhase::Writing as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                TxPhase::Closed => return Err(TxError::Closed),
                TxPhase::Writing | TxPhase::OutboundPending => return Err(TxError::Busy),
            }
        }
    }

    fn reserve_transfer_id(&self) -> u64 {
        self.next_transfer_id
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(1))
            })
            .expect("transfer id update closure always succeeds")
    }

    fn finish_writer(&self) -> u64 {
        debug_assert_eq!(self.current_phase(), TxPhase::Writing);
        let transfer_id = self.reserve_transfer_id();
        self.phase
            .store(TxPhase::OutboundPending as u8, Ordering::Release);
        transfer_id
    }

    fn cancel_writer(&self) {
        debug_assert_eq!(self.current_phase(), TxPhase::Writing);
        self.phase.store(TxPhase::Idle as u8, Ordering::Release);
    }

    fn finish_outbound(&self) {
        debug_assert_eq!(self.current_phase(), TxPhase::OutboundPending);
        self.phase.store(TxPhase::Idle as u8, Ordering::Release);
    }

    fn close(&self) -> Result<u64, TxError> {
        loop {
            match self.current_phase() {
                TxPhase::Idle => {
                    if self
                        .phase
                        .compare_exchange(
                            TxPhase::Idle as u8,
                            TxPhase::Closed as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(self.reserve_transfer_id());
                    }
                }
                TxPhase::Closed => return Err(TxError::Closed),
                TxPhase::Writing | TxPhase::OutboundPending => return Err(TxError::Busy),
            }
        }
    }
}

impl PageTx {
    /// Create a sender over an attached [`PagePool`].
    pub fn new(pool: PagePool) -> Self {
        Self {
            inner: Arc::new(TxInner {
                pool,
                phase: AtomicU8::new(TxPhase::Idle as u8),
                next_transfer_id: AtomicU64::new(1),
            }),
        }
    }

    /// Acquire one writable page and start a new in-page message.
    ///
    /// The returned [`PageWriter`] owns the lease and may be moved across
    /// threads or held across `.await` points. Only one writer or unsent
    /// outbound page may exist per `PageTx` at a time.
    pub fn begin(&self, kind: MessageKind, flags: u16) -> Result<PageWriter, TxError> {
        validate_page_size(self.inner.pool.page_size())?;

        self.inner.enter_writing()?;

        let lease = match self.inner.pool.try_acquire() {
            Ok(lease) => lease,
            Err(err) => {
                self.inner.cancel_writer();
                return Err(TxError::Acquire(err));
            }
        };

        Ok(PageWriter {
            tx: self.clone(),
            lease: Some(lease),
            kind,
            flags,
            position: 0,
            finished: false,
        })
    }

    /// Emit the terminal close frame for this sender stream.
    ///
    /// After a successful close, subsequent `begin()` and `close()` calls
    /// return [`TxError::Closed`].
    pub fn close(&self) -> Result<OwnedFrame, TxError> {
        let transfer_id = self.inner.close()?;
        Ok(OwnedFrame::Close(CloseFrame { transfer_id }))
    }
}

/// Owned writable page message builder.
///
/// Payload bytes are appended after a reserved in-page header prefix. The page
/// is returned to the pool on drop unless [`PageWriter::finish`] detaches it
/// into an [`OutboundPage`].
pub struct PageWriter {
    tx: PageTx,
    lease: Option<PageLease>,
    kind: MessageKind,
    flags: u16,
    position: usize,
    finished: bool,
}

impl PageWriter {
    /// Return the full writable payload region after the in-page header.
    ///
    /// Structured producers can fill this slice directly and later call
    /// [`PageWriter::finish_with_payload_len`] with the number of bytes they
    /// populated.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        let len = self.payload_capacity();
        let start = PAGE_HEADER_LEN;
        let end = start + len;
        &mut self
            .lease
            .as_mut()
            .expect("page writer must hold a lease while active")
            .bytes_mut()[start..end]
    }

    /// Return how many payload bytes still fit into this page.
    pub fn remaining(&self) -> usize {
        self.payload_capacity().saturating_sub(self.position)
    }

    /// Return the current payload length written so far.
    pub fn position(&self) -> usize {
        self.position
    }

    /// Return the caller-defined message kind that will be written to the
    /// in-page header on [`PageWriter::finish`].
    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    /// Return the caller-defined message flags that will be written to the
    /// in-page header on [`PageWriter::finish`].
    pub fn flags(&self) -> u16 {
        self.flags
    }

    /// Finalize the in-page header, detach the page, and return an outbound
    /// control object ready for transport encoding.
    pub fn finish(self) -> Result<OutboundPage, TxError> {
        let payload_len = self.position;
        self.finish_with_payload_len(payload_len)
    }

    /// Finalize the in-page header using an explicit payload length.
    ///
    /// This is the low-level path for producers that write directly into the
    /// payload slice returned by [`PageWriter::payload_mut`].
    pub fn finish_with_payload_len(mut self, payload_len: usize) -> Result<OutboundPage, TxError> {
        let payload_capacity = self.payload_capacity();
        if payload_len > payload_capacity {
            return Err(TxError::PayloadExceedsCapacity {
                actual: payload_len,
                capacity: payload_capacity,
            });
        }
        let mut lease = self
            .lease
            .take()
            .expect("page writer must hold a lease until finish");
        let payload_len = u32::try_from(payload_len).map_err(|_| TxError::PayloadTooLarge {
            actual: payload_len,
            max: MAX_PAYLOAD_LEN,
        })?;
        let header = PageHeader {
            kind: self.kind,
            flags: self.flags,
            payload_len,
        };
        encode_page_header(header, &mut lease.bytes_mut()[..PAGE_HEADER_LEN])?;
        let descriptor = match lease.into_descriptor() {
            Ok(descriptor) => descriptor,
            Err(err) => return Err(TxError::Detach(err)),
        };
        let transfer_id = self.tx.inner.finish_writer();
        self.finished = true;
        Ok(OutboundPage {
            tx: self.tx.clone(),
            descriptor,
            transfer_id,
            sent: false,
        })
    }

    fn payload_capacity(&self) -> usize {
        self.lease
            .as_ref()
            .expect("page writer must hold a lease while active")
            .bytes()
            .len()
            .saturating_sub(PAGE_HEADER_LEN)
    }
}

impl Write for PageWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let remaining = self.remaining();
        if remaining == 0 {
            return Ok(0);
        }

        let to_copy = remaining.min(buf.len());
        let start = PAGE_HEADER_LEN + self.position;
        let end = start + to_copy;
        self.lease
            .as_mut()
            .expect("page writer must hold a lease while active")
            .bytes_mut()[start..end]
            .copy_from_slice(&buf[..to_copy]);
        self.position += to_copy;
        Ok(to_copy)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_all(&mut self, mut buf: &[u8]) -> io::Result<()> {
        while !buf.is_empty() {
            let written = self.write(buf)?;
            if written == 0 {
                return Err(write_zero());
            }
            buf = &buf[written..];
        }
        Ok(())
    }
}

impl Drop for PageWriter {
    fn drop(&mut self) {
        if !self.finished {
            let lease = self.lease.take();
            drop(lease);
            self.tx.inner.cancel_writer();
        }
    }
}

/// Detached page ready to be encoded into a transport frame.
///
/// Dropping an `OutboundPage` before [`OutboundPage::mark_sent`] rolls the page
/// back into the local pool. Call `mark_sent()` only after the carrier write
/// has succeeded.
pub struct OutboundPage {
    tx: PageTx,
    descriptor: PageDescriptor,
    transfer_id: u64,
    sent: bool,
}

impl OutboundPage {
    /// Return the sender-assigned transfer sequence number.
    pub fn transfer_id(&self) -> u64 {
        self.transfer_id
    }

    /// Return the detached page descriptor to be carried in the transport
    /// frame.
    pub fn descriptor(&self) -> PageDescriptor {
        self.descriptor
    }

    /// Build the transport frame that hands this page to the receiver.
    pub fn frame(&self) -> OwnedFrame {
        OwnedFrame::Page(PageFrame {
            transfer_id: self.transfer_id,
            descriptor: self.descriptor,
        })
    }

    /// Mark the page as successfully handed to the carrier.
    ///
    /// After this call the local sender no longer rolls the page back on drop.
    pub fn mark_sent(mut self) {
        self.sent = true;
        self.tx.inner.finish_outbound();
    }
}

impl Drop for OutboundPage {
    fn drop(&mut self) {
        if !self.sent {
            let _ = self.tx.inner.pool.release(self.descriptor);
            self.tx.inner.finish_outbound();
        }
    }
}
