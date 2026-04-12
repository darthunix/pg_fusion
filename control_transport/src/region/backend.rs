use super::{
    BackendRx, BackendSlotLease, BackendTx, ControlRx, ControlTx, TransportRegion,
    LEASE_STATE_LEASED, OWNER_BACKEND, WORKER_STATE_ONLINE,
};
use crate::error::{AcquireError, BackendRxError, BackendTxError, LeaseError};
#[cfg(test)]
use std::cell::RefCell;
use std::sync::atomic::Ordering;

#[cfg(test)]
thread_local! {
    static BACKEND_ACQUIRE_PUBLISH_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

impl BackendSlotLease {
    /// Acquires one backend slot from the currently active worker generation.
    pub fn acquire(region: &TransportRegion) -> Result<Self, AcquireError> {
        let generation = region.region_generation();
        if generation == 0
            || region.worker_state_cell().load(Ordering::Acquire) != WORKER_STATE_ONLINE
        {
            return Err(AcquireError::WorkerOffline);
        }

        let slot_id = region.acquire_slot()?;
        if region.region_generation() != generation
            || region.worker_state_cell().load(Ordering::Acquire) != WORKER_STATE_ONLINE
        {
            region.release_slot(slot_id);
            return Err(AcquireError::WorkerOffline);
        }

        region.clear_slot(slot_id, true);
        let slot = unsafe { region.slot_view_unchecked(slot_id) };
        slot.slot_generation.store(generation, Ordering::Release);
        slot.owner_mask.store(OWNER_BACKEND, Ordering::Release);
        slot.backend_pid
            .store(TransportRegion::current_process_pid(), Ordering::Release);
        slot.lease_state
            .store(LEASE_STATE_LEASED, Ordering::Release);

        #[cfg(test)]
        region.run_backend_acquire_publish_hook_for_tests();

        let mut lease = Self {
            region: *region,
            generation,
            slot_id,
            active: true,
        };
        if region.region_generation() != generation
            || region.worker_state_cell().load(Ordering::Acquire) != WORKER_STATE_ONLINE
        {
            lease.release();
            return Err(AcquireError::WorkerOffline);
        }

        Ok(lease)
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn backend_pid(&self) -> i32 {
        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
        slot.backend_pid.load(Ordering::Acquire)
    }

    /// Clears both transport rings and ready flags while preserving the lease.
    pub fn clear_transport(&mut self) -> Result<(), LeaseError> {
        self.region
            .validate_lease(self.slot_id, self.generation, self.active)?;
        self.region.clear_slot(self.slot_id, false);
        self.region
            .validate_lease(self.slot_id, self.generation, self.active)?;
        Ok(())
    }

    /// Releases the leased slot. The slot only returns to the freelist after
    /// both backend and worker owners have detached.
    pub fn release(&mut self) {
        if !self.active {
            return;
        }

        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
        if slot.slot_generation.load(Ordering::Acquire) != self.generation {
            self.active = false;
            return;
        }
        if slot.owner_mask.load(Ordering::Acquire) & OWNER_BACKEND == 0 {
            self.active = false;
            return;
        }

        let previous = slot.owner_mask.fetch_and(!OWNER_BACKEND, Ordering::AcqRel);
        if previous & OWNER_BACKEND == 0 {
            self.active = false;
            return;
        }

        slot.backend_pid.store(0, Ordering::Release);
        if previous & !OWNER_BACKEND == 0 {
            let _ = self.region.try_finalize_slot(self.slot_id, self.generation);
        }
        self.active = false;
    }

    pub fn to_worker_tx(&mut self) -> BackendTx<'_, '_> {
        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
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
        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
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

#[cfg(test)]
impl TransportRegion {
    pub(crate) fn set_backend_acquire_publish_hook_for_tests<F>(&self, hook: F)
    where
        F: FnOnce() + 'static,
    {
        let _ = self;
        BACKEND_ACQUIRE_PUBLISH_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(hook));
        });
    }

    fn run_backend_acquire_publish_hook_for_tests(&self) {
        let _ = self;
        BACKEND_ACQUIRE_PUBLISH_HOOK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
    }
}

impl<'lease, 'region> BackendTx<'lease, 'region> {
    /// Copies one frame into the backend-to-worker ring.
    pub fn send_frame(&mut self, payload: &[u8]) -> Result<super::CommitOutcome, BackendTxError> {
        self.lease
            .region
            .validate_lease(self.lease.slot_id, self.lease.generation, self.lease.active)
            .map_err(BackendTxError::Lease)?;
        let outcome = self
            .inner
            .send_frame(payload)
            .map_err(BackendTxError::Ring)?;
        self.lease
            .region
            .validate_lease(self.lease.slot_id, self.lease.generation, self.lease.active)
            .map_err(BackendTxError::Lease)?;
        Ok(outcome)
    }
}

impl<'lease, 'region> BackendRx<'lease, 'region> {
    /// Copies the next worker-to-backend frame into `dst`.
    pub fn recv_frame_into(&mut self, dst: &mut [u8]) -> Result<Option<usize>, BackendRxError> {
        self.lease
            .region
            .validate_lease(self.lease.slot_id, self.lease.generation, self.lease.active)
            .map_err(BackendRxError::Lease)?;
        let received = self
            .inner
            .recv_frame_into(dst)
            .map_err(BackendRxError::Ring)?;
        self.lease
            .region
            .validate_lease(self.lease.slot_id, self.lease.generation, self.lease.active)
            .map_err(BackendRxError::Lease)?;
        Ok(received)
    }
}
