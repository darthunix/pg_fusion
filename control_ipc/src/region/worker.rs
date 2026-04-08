use super::layout::bank_index_for_generation;
use super::{
    CommitOutcome, ControlRegion, ControlRx, ControlTx, ReadySlots, WorkerControl,
    WorkerFrameWriter, WorkerRx, WorkerSlot, WorkerTx, LEASE_STATE_LEASED,
};
use crate::error::{SlotError, WorkerRxError, WorkerTxError};
use std::sync::atomic::Ordering;

impl WorkerControl {
    /// Attaches a worker-side process-local handle to the region.
    pub fn attach(region: &ControlRegion) -> Self {
        Self { region: *region }
    }

    /// Updates the published worker PID without switching generations.
    ///
    /// Prefer [`WorkerControl::activate_generation`] for worker startup and
    /// restart paths.
    pub fn set_worker_pid(&mut self, pid: i32) {
        self.region.worker_pid_cell().store(pid, Ordering::Release);
    }

    /// Clears the published worker PID without changing the active generation.
    pub fn clear_worker_pid(&mut self) {
        self.region.worker_pid_cell().store(0, Ordering::Release);
    }

    /// Activates a fresh worker generation.
    ///
    /// This is the worker-side entry point for startup and restart. Switching
    /// generations invalidates all handles from the previous generation.
    pub fn activate_generation(&self, pid: i32) -> u64 {
        self.region.activate_worker_generation(pid)
    }

    /// Claims one slot for worker-side send/receive in the current generation.
    pub fn slot(&self, slot_id: u32) -> Result<WorkerSlot<'_>, SlotError> {
        let (generation, bank_index, claimed_epoch) =
            self.region.claim_worker_slot_epoch(slot_id)?;
        Ok(WorkerSlot {
            region: &self.region,
            generation,
            bank_index,
            slot_id,
            claimed_epoch,
        })
    }

    pub fn ready_slots(&self) -> ReadySlots<'_> {
        let generation = self.region.region_generation();
        ReadySlots {
            control: self,
            generation,
            bank_index: if generation == 0 {
                None
            } else {
                Some(bank_index_for_generation(generation))
            },
            next: 0,
        }
    }
}

impl<'a> Iterator for ReadySlots<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let bank_index = self.bank_index?;
        while self.next < self.control.region.slot_count {
            if self.control.region.region_generation() != self.generation {
                return None;
            }
            let slot_id = self.next;
            self.next += 1;
            let slot = unsafe {
                self.control
                    .region
                    .slot_view_in_bank_unchecked(slot_id, bank_index)
            };
            let session_epoch = slot.session_epoch.load(Ordering::Acquire);
            let claim_epoch = slot.worker_claim_epoch.load(Ordering::Acquire);
            if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED
                || session_epoch == 0
                || claim_epoch == super::WORKER_CLAIM_BLOCKED
                || claim_epoch == session_epoch
            {
                continue;
            }
            if slot.to_worker_ready.load(Ordering::Acquire)
                || slot.backend_to_worker.has_pending_frame()
            {
                return Some(slot_id);
            }
        }
        None
    }
}

impl<'a> WorkerSlot<'a> {
    /// Returns the claimed region generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the claimed slot id.
    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    /// Returns the claimed session epoch.
    pub fn session_epoch(&self) -> u64 {
        self.claimed_epoch
    }

    pub fn backend_pid(&self) -> i32 {
        let slot = unsafe {
            self.region
                .slot_view_in_bank_unchecked(self.slot_id, self.bank_index)
        };
        slot.backend_pid.load(Ordering::Acquire)
    }

    /// Creates a worker-side receiver for backend-to-worker frames.
    pub fn from_backend_rx(&mut self) -> Result<WorkerRx<'_, 'a>, SlotError> {
        let slot = self.validate_current_claim()?;
        Ok(WorkerRx {
            slot: &*self,
            inner: ControlRx {
                ring: slot.backend_to_worker,
                ready_flag: slot.to_worker_ready,
            },
        })
    }

    /// Creates a worker-side sender for worker-to-backend frames.
    pub fn to_backend_tx(&mut self) -> Result<WorkerTx<'_, 'a>, SlotError> {
        let slot = self.validate_current_claim()?;
        Ok(WorkerTx {
            slot: &*self,
            inner: ControlTx {
                ring: slot.worker_to_backend,
                ready_flag: slot.to_backend_ready,
                peer_pid: slot.backend_pid,
            },
        })
    }

    pub(super) fn validate_current_claim(&self) -> Result<super::SlotView<'a>, SlotError> {
        let current_generation = self.region.region_generation();
        if current_generation != self.generation {
            return Err(SlotError::StaleGeneration {
                slot_id: self.slot_id,
                claimed_generation: self.generation,
                current_generation,
            });
        }

        let slot = self
            .region
            .slot_view_in_bank(self.slot_id, self.bank_index)?;
        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED {
            return Err(SlotError::NoActiveSession {
                slot_id: self.slot_id,
            });
        }
        let current_epoch = slot.session_epoch.load(Ordering::Acquire);
        if current_epoch != self.claimed_epoch {
            return Err(SlotError::StaleEpoch {
                slot_id: self.slot_id,
                claimed_epoch: self.claimed_epoch,
                current_epoch,
            });
        }
        let current_claim_epoch = slot.worker_claim_epoch.load(Ordering::Acquire);
        if current_claim_epoch != self.claimed_epoch {
            return Err(SlotError::ClaimLost {
                slot_id: self.slot_id,
                claimed_epoch: self.claimed_epoch,
                current_claim_epoch,
            });
        }
        Ok(slot)
    }
}

impl Drop for WorkerSlot<'_> {
    fn drop(&mut self) {
        self.region.release_worker_slot_claim(
            self.slot_id,
            self.generation,
            self.bank_index,
            self.claimed_epoch,
        );
    }
}

impl<'slot, 'region> WorkerTx<'slot, 'region> {
    /// Reserves one outbound frame after revalidating the worker slot claim.
    pub fn try_reserve<'tx>(
        &'tx mut self,
        payload_len: usize,
    ) -> Result<WorkerFrameWriter<'tx, 'slot, 'region>, WorkerTxError> {
        self.slot
            .validate_current_claim()
            .map_err(WorkerTxError::Slot)?;
        let inner = self
            .inner
            .try_reserve(payload_len)
            .map_err(WorkerTxError::Ring)?;
        Ok(WorkerFrameWriter {
            slot: self.slot,
            inner,
        })
    }
}

impl<'slot, 'region> WorkerRx<'slot, 'region> {
    /// Borrows the next backend-to-worker frame after revalidating the claim.
    pub fn peek_frame(&self) -> Result<Option<&[u8]>, WorkerRxError> {
        self.slot
            .validate_current_claim()
            .map_err(WorkerRxError::Slot)?;
        self.inner.peek_frame().map_err(WorkerRxError::Ring)
    }

    /// Consumes the frame last observed by [`WorkerRx::peek_frame`].
    pub fn consume_frame(&mut self) -> Result<(), WorkerRxError> {
        self.slot
            .validate_current_claim()
            .map_err(WorkerRxError::Slot)?;
        self.inner.consume_frame().map_err(WorkerRxError::Ring)
    }

    #[cfg(test)]
    pub(crate) fn consume_frame_with_post_clear_hook<F>(
        &mut self,
        hook: F,
    ) -> Result<(), WorkerRxError>
    where
        F: FnOnce(),
    {
        self.slot
            .validate_current_claim()
            .map_err(WorkerRxError::Slot)?;
        self.inner
            .consume_frame_with_post_clear_hook(hook)
            .map_err(WorkerRxError::Ring)
    }
}

impl<'tx, 'slot, 'region> WorkerFrameWriter<'tx, 'slot, 'region> {
    /// Returns the mutable payload area for the reserved worker frame.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.inner.payload_mut()
    }

    /// Publishes the frame after revalidating the worker slot claim.
    pub fn commit(self) -> Result<CommitOutcome, WorkerTxError> {
        self.slot
            .validate_current_claim()
            .map_err(WorkerTxError::Slot)?;
        Ok(self.inner.commit())
    }
}
