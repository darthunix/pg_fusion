use super::layout::bank_index_for_generation;
use super::{
    BackendFrameWriter, BackendRx, BackendSlotLease, BackendTx, CommitOutcome, ControlRegion,
    ControlRx, ControlTx, LEASE_STATE_FREE, LEASE_STATE_LEASED, WORKER_CLAIM_BLOCKED,
    WORKER_CLAIM_NONE,
};
use crate::error::{AcquireError, BackendRxError, BackendTxError, BeginSessionError};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

impl BackendSlotLease {
    /// Acquires one backend slot from the currently active worker generation.
    ///
    /// Acquisition fails with [`AcquireError::WorkerOffline`] until the worker
    /// has activated at least one generation.
    pub fn acquire(region: &ControlRegion) -> Result<Self, AcquireError> {
        let generation = region.region_generation();
        if generation == 0 || region.worker_pid_cell().load(Ordering::Acquire) <= 0 {
            return Err(AcquireError::WorkerOffline);
        }

        let bank_index = bank_index_for_generation(generation);
        let slot_id = region.acquire_slot_in_bank(bank_index)?;
        if region.region_generation() != generation {
            region.release_slot_in_bank(bank_index, slot_id);
            return Err(AcquireError::WorkerOffline);
        }

        region.clear_slot_in_bank(bank_index, slot_id, true, false);
        let slot = unsafe { region.slot_view_in_bank_unchecked(slot_id, bank_index) };
        slot.backend_pid
            .store(ControlRegion::current_process_pid(), Ordering::Release);
        slot.worker_claim_epoch
            .store(WORKER_CLAIM_BLOCKED, Ordering::Release);
        slot.lease_state
            .store(LEASE_STATE_LEASED, Ordering::Release);

        Ok(Self {
            region: *region,
            generation,
            bank_index,
            slot_id,
            active: true,
        })
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn backend_pid(&self) -> i32 {
        let slot = unsafe {
            self.region
                .slot_view_in_bank_unchecked(self.slot_id, self.bank_index)
        };
        slot.backend_pid.load(Ordering::Acquire)
    }

    pub fn session_epoch(&self) -> u64 {
        let slot = unsafe {
            self.region
                .slot_view_in_bank_unchecked(self.slot_id, self.bank_index)
        };
        slot.session_epoch.load(Ordering::Acquire)
    }

    pub fn begin_session(&mut self) -> Result<u64, BeginSessionError> {
        let slot = self
            .region
            .validate_lease(self.slot_id, self.generation, self.bank_index, self.active)
            .map_err(BeginSessionError::Lease)?;
        match slot.worker_claim_epoch.load(Ordering::Acquire) {
            WORKER_CLAIM_BLOCKED => {}
            WORKER_CLAIM_NONE => match slot.worker_claim_epoch.compare_exchange(
                WORKER_CLAIM_NONE,
                WORKER_CLAIM_BLOCKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) | Err(WORKER_CLAIM_BLOCKED) => {}
                Err(claimed_epoch) => {
                    return Err(BeginSessionError::WorkerClaimed {
                        slot_id: self.slot_id,
                        claimed_epoch,
                    });
                }
            },
            claimed_epoch => {
                return Err(BeginSessionError::WorkerClaimed {
                    slot_id: self.slot_id,
                    claimed_epoch,
                });
            }
        }

        slot.backend_to_worker.clear();
        slot.worker_to_backend.clear();
        slot.to_worker_ready.store(false, Ordering::Release);
        slot.to_backend_ready.store(false, Ordering::Release);
        let new_epoch = slot.session_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        slot.worker_claim_epoch
            .store(WORKER_CLAIM_NONE, Ordering::Release);
        Ok(new_epoch)
    }

    /// Releases the leased slot back to the current generation's freelist.
    ///
    /// Embedding code should call this from normal backend exit hooks. If the
    /// worker has already switched to a new generation, release becomes a
    /// no-op because the old bank is already stale.
    pub fn release(&mut self) {
        if !self.active {
            return;
        }
        if self.region.region_generation() != self.generation {
            self.active = false;
            return;
        }

        let slot = unsafe {
            self.region
                .slot_view_in_bank_unchecked(self.slot_id, self.bank_index)
        };
        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED {
            self.active = false;
            return;
        }

        self.region
            .clear_slot_in_bank(self.bank_index, self.slot_id, true, false);
        slot.lease_state.store(LEASE_STATE_FREE, Ordering::Release);
        self.region
            .release_slot_in_bank(self.bank_index, self.slot_id);
        self.active = false;
    }

    pub fn to_worker_tx(&mut self) -> BackendTx<'_, '_> {
        let slot = unsafe {
            self.region
                .slot_view_in_bank_unchecked(self.slot_id, self.bank_index)
        };
        BackendTx {
            lease: &*self,
            inner: ControlTx {
                ring: slot.backend_to_worker,
                ready_flag: slot.to_worker_ready,
                peer_pid: slot.worker_pid,
            },
        }
    }

    pub fn from_worker_rx(&mut self) -> BackendRx<'_, '_> {
        let slot = unsafe {
            self.region
                .slot_view_in_bank_unchecked(self.slot_id, self.bank_index)
        };
        BackendRx {
            lease: &*self,
            inner: ControlRx {
                ring: slot.worker_to_backend,
                ready_flag: slot.to_backend_ready,
            },
        }
    }
}

impl Drop for BackendSlotLease {
    fn drop(&mut self) {
        self.release();
    }
}

impl<'lease, 'region> BackendTx<'lease, 'region> {
    /// Reserves one outbound frame after revalidating the backend lease.
    pub fn try_reserve<'tx>(
        &'tx mut self,
        payload_len: usize,
    ) -> Result<BackendFrameWriter<'tx, 'lease, 'region>, BackendTxError> {
        self.lease
            .region
            .validate_active_backend_session(
                self.lease.slot_id,
                self.lease.generation,
                self.lease.bank_index,
                self.lease.active,
            )
            .map_err(BackendTxError::Lease)?;
        let inner = self
            .inner
            .try_reserve(payload_len)
            .map_err(BackendTxError::Ring)?;
        Ok(BackendFrameWriter {
            lease: self.lease,
            inner,
            _marker: PhantomData,
        })
    }
}

impl<'lease, 'region> BackendRx<'lease, 'region> {
    /// Borrows the next worker-to-backend frame after revalidating the lease.
    pub fn peek_frame(&self) -> Result<Option<&[u8]>, BackendRxError> {
        self.lease
            .region
            .validate_lease(
                self.lease.slot_id,
                self.lease.generation,
                self.lease.bank_index,
                self.lease.active,
            )
            .map_err(BackendRxError::Lease)?;
        self.inner.peek_frame().map_err(BackendRxError::Ring)
    }

    /// Consumes the frame last observed by [`BackendRx::peek_frame`].
    pub fn consume_frame(&mut self) -> Result<(), BackendRxError> {
        self.lease
            .region
            .validate_lease(
                self.lease.slot_id,
                self.lease.generation,
                self.lease.bank_index,
                self.lease.active,
            )
            .map_err(BackendRxError::Lease)?;
        self.inner.consume_frame().map_err(BackendRxError::Ring)
    }
}

impl<'tx, 'lease, 'region> BackendFrameWriter<'tx, 'lease, 'region> {
    /// Returns the mutable payload area for the reserved backend frame.
    pub fn payload_mut(&mut self) -> &mut [u8] {
        self.inner.payload_mut()
    }

    /// Publishes the frame after revalidating the backend lease.
    pub fn commit(self) -> Result<CommitOutcome, BackendTxError> {
        self.lease
            .region
            .validate_active_backend_session(
                self.lease.slot_id,
                self.lease.generation,
                self.lease.bank_index,
                self.lease.active,
            )
            .map_err(BackendTxError::Lease)?;
        Ok(self.inner.commit())
    }
}
