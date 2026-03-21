use crate::error::{InvalidPageError, RxError};
use crate::page::{decode_page_header, MessageKind, PAGE_HEADER_LEN};
use crate::wire::OwnedFrame;
use page_pool::{PageDescriptor, PagePool};
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

/// Receiver-side page handoff handle.
///
/// `PageRx` is cheap to clone and accepts one ordered sender stream. It
/// validates transfer ordering, detached page ownership, and the in-page
/// message header before returning a [`ReceivedPage`].
#[derive(Clone)]
pub struct PageRx {
    inner: Arc<RxInner>,
}

struct RxInner {
    pool: PagePool,
    phase: AtomicU8,
    next_transfer_id: AtomicU64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
enum RxPhase {
    Idle = 0,
    Accepting = 1,
    PageActive = 2,
    Closed = 3,
}

impl RxPhase {
    fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Idle,
            1 => Self::Accepting,
            2 => Self::PageActive,
            3 => Self::Closed,
            _ => unreachable!("invalid rx phase {raw}"),
        }
    }
}

impl RxInner {
    fn current_phase(&self) -> RxPhase {
        RxPhase::from_raw(self.phase.load(Ordering::Acquire))
    }

    fn begin_accept(&self) -> Result<(), RxError> {
        loop {
            match self.current_phase() {
                RxPhase::Idle => {
                    if self
                        .phase
                        .compare_exchange(
                            RxPhase::Idle as u8,
                            RxPhase::Accepting as u8,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                RxPhase::Closed => return Err(RxError::Closed),
                RxPhase::Accepting | RxPhase::PageActive => return Err(RxError::Busy),
            }
        }
    }

    fn expected_transfer_id(&self) -> u64 {
        self.next_transfer_id.load(Ordering::Acquire)
    }

    fn advance_transfer_id(&self) {
        let _ =
            self.next_transfer_id
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_add(1))
                });
    }

    fn cancel_accept(&self) {
        debug_assert_eq!(self.current_phase(), RxPhase::Accepting);
        self.phase.store(RxPhase::Idle as u8, Ordering::Release);
    }

    fn finish_close(&self) {
        debug_assert_eq!(self.current_phase(), RxPhase::Accepting);
        self.phase.store(RxPhase::Closed as u8, Ordering::Release);
    }

    fn finish_page_accept(&self) {
        debug_assert_eq!(self.current_phase(), RxPhase::Accepting);
        self.phase
            .store(RxPhase::PageActive as u8, Ordering::Release);
    }

    fn finish_page(&self) {
        debug_assert_eq!(self.current_phase(), RxPhase::PageActive);
        self.phase.store(RxPhase::Idle as u8, Ordering::Release);
    }
}

impl PageRx {
    /// Create a receiver over an attached [`PagePool`].
    pub fn new(pool: PagePool) -> Self {
        Self {
            inner: Arc::new(RxInner {
                pool,
                phase: AtomicU8::new(RxPhase::Idle as u8),
                next_transfer_id: AtomicU64::new(1),
            }),
        }
    }

    /// Accept one decoded transport frame.
    ///
    /// `Page` frames yield a [`ReceivedPage`]. `Close` is terminal and moves
    /// the receiver into the closed state. If page-header validation fails
    /// after the detached descriptor has been accepted locally, the temporary
    /// page guard releases the page back to the pool before returning the
    /// error.
    pub fn accept(&self, frame: OwnedFrame) -> Result<ReceiveEvent, RxError> {
        self.inner.begin_accept()?;

        let actual = frame.transfer_id();
        let expected = self.inner.expected_transfer_id();
        if actual != expected {
            self.inner.cancel_accept();
            return Err(RxError::UnexpectedTransferId { expected, actual });
        }

        match frame {
            OwnedFrame::Close(_) => {
                self.inner.advance_transfer_id();
                self.inner.finish_close();
                Ok(ReceiveEvent::Closed)
            }
            OwnedFrame::Page(frame) => {
                let page = match ReceivedPage::new(self.clone(), frame.descriptor) {
                    Ok(page) => page,
                    Err(err) => {
                        self.inner.cancel_accept();
                        return Err(err);
                    }
                };
                self.inner.advance_transfer_id();
                self.inner.finish_page_accept();
                Ok(ReceiveEvent::Page(page))
            }
        }
    }
}

/// Result of accepting one transport frame.
pub enum ReceiveEvent {
    Page(ReceivedPage),
    Closed,
}

struct PendingReceivedPage {
    rx: PageRx,
    descriptor: PageDescriptor,
    armed: bool,
}

impl PendingReceivedPage {
    fn new(rx: PageRx, descriptor: PageDescriptor) -> Self {
        Self {
            rx,
            descriptor,
            armed: true,
        }
    }

    fn bytes(&self) -> Result<&[u8], RxError> {
        unsafe { self.rx.inner.pool.page_bytes(self.descriptor) }.map_err(RxError::Access)
    }

    fn into_received_page(
        mut self,
        kind: MessageKind,
        flags: u16,
        payload_len: usize,
    ) -> ReceivedPage {
        self.armed = false;
        ReceivedPage {
            rx: self.rx.clone(),
            descriptor: self.descriptor,
            kind,
            flags,
            payload_len,
            released: false,
        }
    }
}

impl Drop for PendingReceivedPage {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.rx.inner.pool.release(self.descriptor);
        }
    }
}

pub struct ReceivedPage {
    rx: PageRx,
    descriptor: PageDescriptor,
    kind: MessageKind,
    flags: u16,
    payload_len: usize,
    released: bool,
}

impl ReceivedPage {
    fn new(rx: PageRx, descriptor: PageDescriptor) -> Result<Self, RxError> {
        let pending = PendingReceivedPage::new(rx, descriptor);
        let bytes = pending.bytes()?;
        let header = decode_page_header(&bytes[..PAGE_HEADER_LEN]).map_err(RxError::InvalidPage)?;
        let capacity = bytes.len().saturating_sub(PAGE_HEADER_LEN);
        let payload_len = header.payload_len as usize;
        if payload_len > capacity {
            return Err(RxError::InvalidPage(InvalidPageError::PayloadTooLarge {
                payload_len: header.payload_len,
                capacity,
            }));
        }

        Ok(pending.into_received_page(header.kind, header.flags, payload_len))
    }

    /// Return the in-page message kind.
    pub fn kind(&self) -> MessageKind {
        self.kind
    }

    /// Return the in-page message flags.
    pub fn flags(&self) -> u16 {
        self.flags
    }

    /// Return the detached descriptor currently owned by this received page.
    pub fn descriptor(&self) -> PageDescriptor {
        self.descriptor
    }

    /// Borrow the validated message payload bytes.
    pub fn payload(&self) -> &[u8] {
        let bytes = unsafe {
            self.rx
                .inner
                .pool
                .page_bytes(self.descriptor)
                .expect("received page descriptor must remain valid until release")
        };
        &bytes[PAGE_HEADER_LEN..PAGE_HEADER_LEN + self.payload_len]
    }

    /// Release the detached page back to the pool.
    pub fn release(mut self) -> Result<(), RxError> {
        self.rx
            .inner
            .pool
            .release(self.descriptor)
            .map_err(RxError::Release)?;
        self.released = true;
        self.rx.inner.finish_page();
        Ok(())
    }
}

impl Drop for ReceivedPage {
    fn drop(&mut self) {
        if !self.released {
            let _ = self.rx.inner.pool.release(self.descriptor);
            self.rx.inner.finish_page();
        }
    }
}
