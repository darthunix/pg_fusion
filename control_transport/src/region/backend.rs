use super::{
    BackendRx, BackendSlotLease, BackendTx, ControlRx, ControlTx, LeaseIncarnation, SlotMeta,
    TransportRegion, LEASE_STATE_LEASED, OWNER_BACKEND,
};
use crate::error::{AcquireError, BackendRxError, BackendTxError};
#[cfg(test)]
use std::cell::RefCell;
use std::sync::atomic::Ordering;

#[cfg(test)]
thread_local! {
    static BACKEND_ACQUIRE_PUBLISH_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

fn generation_is_online(region: &TransportRegion, generation: u64) -> bool {
    region.load_region_meta().is_online_generation(generation)
}

fn reserve_backend_slot(region: &TransportRegion, generation: u64) -> Result<u32, AcquireError> {
    loop {
        let (slot_id, popped_epoch) = match region.pop_freelist_token_for_acquire() {
            Ok(reserved) => reserved,
            Err(AcquireError::Empty) => {
                if let Err((slot_id, err)) = region.reap_current_generation_dead_backend_slots() {
                    return Err(AcquireError::BackendProbeFailed {
                        slot_id,
                        error_kind: err.kind(),
                        raw_os_error: err.raw_os_error(),
                    });
                }
                region.pop_freelist_token_for_acquire()?
            }
            Err(err) => return Err(err),
        };

        if !generation_is_online(region, generation) {
            let slot = unsafe { region.slot_view_unchecked(slot_id) };
            let _ = region.abort_reserved_slot_acquire(slot_id, slot);
            return Err(AcquireError::WorkerOffline);
        }

        let slot = unsafe { region.slot_view_unchecked(slot_id) };
        if region.reserve_popped_slot_for_acquire(slot_id, slot, popped_epoch) {
            return Ok(slot_id);
        }
    }
}

fn publish_backend_lease(
    region: &TransportRegion,
    slot_id: u32,
    generation: u64,
) -> Option<LeaseIncarnation> {
    let lease_epoch = region.allocate_lease_epoch();
    let slot = unsafe { region.slot_view_unchecked(slot_id) };
    region.clear_slot(slot_id);
    slot.slot_generation.store(generation, Ordering::Release);
    slot.backend_pid
        .store(TransportRegion::current_process_pid(), Ordering::Release);
    let leased_meta = SlotMeta::new(LEASE_STATE_LEASED, lease_epoch, OWNER_BACKEND);
    if slot
        .slot_meta
        .compare_exchange(
            SlotMeta::ACQUIRE_RESERVED.raw(),
            leased_meta.raw(),
            Ordering::AcqRel,
            Ordering::Acquire,
        )
        .is_err()
    {
        let _ = region.abort_reserved_slot_acquire(slot_id, slot);
        return None;
    }
    Some(LeaseIncarnation::new(generation, lease_epoch))
}

impl BackendSlotLease {
    /// Acquires one backend slot from the currently active worker generation.
    pub fn acquire(region: &TransportRegion) -> Result<Self, AcquireError> {
        let region_meta = region.load_region_meta();
        let generation = region_meta.generation();
        if !region_meta.is_online_generation(generation) {
            return Err(AcquireError::WorkerOffline);
        }

        let slot_id = reserve_backend_slot(region, generation)?;
        let Some(incarnation) = publish_backend_lease(region, slot_id, generation) else {
            return Err(AcquireError::WorkerOffline);
        };

        #[cfg(test)]
        region.run_backend_acquire_publish_hook_for_tests();

        let mut lease = Self {
            region: *region,
            incarnation,
            slot_id,
            active: true,
        };
        if !generation_is_online(region, generation) {
            lease.release();
            return Err(AcquireError::WorkerOffline);
        }

        Ok(lease)
    }

    pub fn generation(&self) -> u64 {
        self.incarnation.generation
    }

    pub fn backend_lease_id(&self) -> super::BackendLeaseId {
        self.incarnation.into()
    }

    pub fn backend_lease_slot(&self) -> super::BackendLeaseSlot {
        super::BackendLeaseSlot::new(self.slot_id, self.backend_lease_id())
    }

    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn backend_pid(&self) -> i32 {
        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
        slot.backend_pid.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn lease_epoch_for_tests(&self) -> u64 {
        self.incarnation.lease_epoch
    }

    /// Releases the leased slot. The slot only returns to the freelist after
    /// both backend and worker owners have detached.
    pub fn release(&mut self) {
        if !self.active {
            return;
        }

        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
        if slot.slot_generation.load(Ordering::Acquire) != self.incarnation.generation {
            self.active = false;
            return;
        }
        if !SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire)).has_backend_owner() {
            self.active = false;
            return;
        }

        let mutation =
            self.region
                .clear_owner_bits_if_matching(slot, self.incarnation, OWNER_BACKEND);
        if !mutation.changed() {
            self.active = false;
            return;
        }

        slot.backend_pid.store(0, Ordering::Release);
        self.region
            .finalize_if_ownerless(self.slot_id, self.incarnation, mutation);
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
    ///
    /// A post-send lease validation error does not imply that the frame was
    /// rolled back; the payload may already be published locally and later be
    /// treated as lost traffic by higher layers.
    pub fn send_frame(&mut self, payload: &[u8]) -> Result<super::CommitOutcome, BackendTxError> {
        self.lease
            .region
            .validate_lease(
                self.lease.slot_id,
                self.lease.incarnation,
                self.lease.active,
            )
            .map_err(BackendTxError::Lease)?;
        let outcome = self
            .inner
            .send_frame(payload)
            .map_err(BackendTxError::Ring)?;
        self.lease
            .region
            .validate_lease(
                self.lease.slot_id,
                self.lease.incarnation,
                self.lease.active,
            )
            .map_err(BackendTxError::Lease)?;
        Ok(outcome)
    }
}

impl<'lease, 'region> BackendRx<'lease, 'region> {
    /// Copies the next worker-to-backend frame into `dst`.
    pub fn recv_frame_into(&mut self, dst: &mut [u8]) -> Result<Option<usize>, BackendRxError> {
        self.lease
            .region
            .validate_lease(
                self.lease.slot_id,
                self.lease.incarnation,
                self.lease.active,
            )
            .map_err(BackendRxError::Lease)?;
        let received = self
            .inner
            .recv_frame_into(dst)
            .map_err(BackendRxError::Ring)?;
        self.lease
            .region
            .validate_lease(
                self.lease.slot_id,
                self.lease.incarnation,
                self.lease.active,
            )
            .map_err(BackendRxError::Lease)?;
        Ok(received)
    }
}
