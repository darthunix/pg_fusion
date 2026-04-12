use super::{CommitOutcome, ControlRx, ControlTx};
use crate::error::{RxError, TxError};

impl<'a> ControlTx<'a> {
    /// Copies one payload into the ring and publishes it as a frame.
    pub fn send_frame(&mut self, payload: &[u8]) -> Result<CommitOutcome, TxError> {
        self.ring
            .send_frame(self.ready_flag, self.peer_pid, payload)
    }
}

impl<'a> ControlRx<'a> {
    /// Copies the next queued frame into `dst` and consumes it.
    ///
    /// Returns `Ok(None)` when the ring is empty.
    pub fn recv_frame_into(&mut self, dst: &mut [u8]) -> Result<Option<usize>, RxError> {
        self.ring.recv_frame_into(self.ready_flag, dst)
    }
}
