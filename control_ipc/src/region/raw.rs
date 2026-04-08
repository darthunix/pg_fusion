use super::{CommitOutcome, ControlRx, ControlTx, FrameWriter};
use crate::error::{RxError, TxError};
use crate::ring::signal_peer;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, Ordering};

impl<'a> ControlTx<'a> {
    /// Reserves space for one payload-sized frame in the ring.
    pub fn try_reserve(&mut self, payload_len: usize) -> Result<FrameWriter<'_>, TxError> {
        self.ring
            .try_reserve(self.ready_flag, self.peer_pid, payload_len)
    }
}

impl<'a> ControlRx<'a> {
    /// Borrows the next queued frame without consuming it.
    pub fn peek_frame(&self) -> Result<Option<&[u8]>, RxError> {
        self.ring.peek_frame()
    }

    /// Consumes the frame last observed by [`ControlRx::peek_frame`].
    pub fn consume_frame(&mut self) -> Result<(), RxError> {
        self.ring.consume_frame(self.ready_flag)
    }

    #[cfg(test)]
    pub(crate) fn consume_frame_with_post_clear_hook<F>(&mut self, hook: F) -> Result<(), RxError>
    where
        F: FnOnce(),
    {
        self.ring
            .consume_frame_with_post_clear_hook(self.ready_flag, hook)
    }
}

impl<'a> FrameWriter<'a> {
    pub(crate) fn new(
        data: NonNull<u8>,
        tail: &'a AtomicU32,
        ready_flag: &'a AtomicBool,
        peer_pid: &'a AtomicI32,
        payload_start: u32,
        payload_len: u32,
        publish_tail: u32,
    ) -> Self {
        Self {
            data,
            tail,
            ready_flag,
            peer_pid,
            payload_start,
            payload_len,
            publish_tail,
            committed: false,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the mutable payload area for the reserved frame.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(
                self.data.as_ptr().add(self.payload_start as usize),
                self.payload_len as usize,
            )
        }
    }

    /// Publishes the frame and returns the notification outcome.
    ///
    /// Once this method returns, the frame is already visible to the peer,
    /// regardless of whether signaling succeeded.
    pub fn commit(mut self) -> CommitOutcome {
        self.tail.store(self.publish_tail, Ordering::Release);
        self.ready_flag.store(true, Ordering::Release);
        self.committed = true;
        match signal_peer(self.peer_pid) {
            Ok(true) => CommitOutcome::Notified,
            Ok(false) => CommitOutcome::PeerMissing,
            Err(err) => CommitOutcome::NotifyFailed(err),
        }
    }
}

impl Drop for FrameWriter<'_> {
    fn drop(&mut self) {
        if !self.committed {
            // Uncommitted writes stay invisible because the ring tail was not
            // advanced.
        }
    }
}
