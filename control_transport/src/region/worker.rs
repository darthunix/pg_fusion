use super::{
    ControlRx, ControlTx, ReadySlots, SlotMeta, WorkerRx, WorkerSlot, WorkerTransport, WorkerTx,
    LEASE_STATE_LEASED, OWNER_ANY_WORKER,
};
use crate::error::{
    SlotAccessError, WorkerAttachError, WorkerLifecycleError, WorkerRxError, WorkerTxError,
};
use std::sync::atomic::Ordering;

impl WorkerTransport {
    /// Attaches a worker-side process-local handle to the transport region.
    ///
    /// A worker process may attach multiple handles to the same region and may
    /// also attach multiple distinct transport regions in the same PID
    /// lifetime. Each attached region gets its own process-local owner
    /// registry keyed by region identity.
    pub fn attach(region: &super::TransportRegion) -> Result<Self, WorkerAttachError> {
        region.attach_worker_registry()?;
        Ok(Self { region: *region })
    }

    /// Updates the published worker PID hint without switching generations.
    ///
    /// The caller must be the worker process for the currently active
    /// generation and must only publish its own PID hint. This is not a
    /// lifecycle transition and must not race `activate_generation()`.
    pub fn set_worker_pid(&mut self, pid: i32) {
        self.region.worker_pid_cell().store(pid, Ordering::Release);
    }

    /// Clears the published worker PID hint without changing worker liveness.
    ///
    /// The caller must be the current worker process clearing its own hint;
    /// this is not a detach or generation-deactivation primitive.
    pub fn clear_worker_pid(&mut self) {
        self.region.worker_pid_cell().store(0, Ordering::Release);
    }

    /// Publishes a fresh online worker generation.
    pub fn activate_generation(&self, pid: i32) -> Result<u64, WorkerLifecycleError> {
        self.region.activate_worker_generation(pid)
    }

    /// Invalidates the current generation and leaves the transport offline.
    pub fn deactivate_generation(&self) -> Result<u64, WorkerLifecycleError> {
        self.region.deactivate_worker_generation()
    }

    /// Releases all worker-owned slots tracked by this process-local handle.
    ///
    /// This is intended for PostgreSQL worker termination callbacks on orderly
    /// shutdown. It is not a hot-restart primitive.
    pub fn release_owned_slots_for_exit(&self) {
        self.region.release_owned_worker_slots_for_exit();
    }

    /// Returns one raw worker-side slot view in the current generation.
    ///
    /// # Safety
    /// The caller must guarantee exclusive raw ownership of the slot and its
    /// ring directions while the returned handle is alive.
    pub unsafe fn slot_unchecked(&self, slot_id: u32) -> Result<WorkerSlot<'_>, SlotAccessError> {
        let generation = self.region.claim_worker_slot(slot_id)?;
        Ok(WorkerSlot {
            region: &self.region,
            incarnation: generation,
            slot_id,
            attached: true,
        })
    }

    pub fn ready_slots(&self) -> ReadySlots<'_> {
        let generation = self.region.load_region_meta().generation();
        ReadySlots {
            transport: self,
            generation,
            next: 0,
        }
    }

    pub fn ready_backend_leases(&self) -> super::ReadyBackendLeases<'_> {
        let generation = self.region.load_region_meta().generation();
        super::ReadyBackendLeases {
            transport: self,
            generation,
            next: 0,
        }
    }

    /// Return the next backend lease peer with pending inbound traffic.
    ///
    /// The caller owns `cursor` and resets it to `0` for each new poll pass.
    /// This avoids holding an iterator borrow across loop bodies that need
    /// mutable transport access.
    pub fn next_ready_backend_lease(&self, cursor: &mut u32) -> Option<super::BackendLeaseSlot> {
        let generation = self.region.load_region_meta().generation();
        if generation == 0 {
            return None;
        }

        while *cursor < self.region.slot_count {
            if !self
                .region
                .load_region_meta()
                .is_online_generation(generation)
            {
                return None;
            }

            let slot_id = *cursor;
            *cursor += 1;
            let slot = unsafe { self.region.slot_view_unchecked(slot_id) };
            let slot_meta = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if slot_meta.lease_state() != LEASE_STATE_LEASED {
                continue;
            }
            if slot.slot_generation.load(Ordering::Acquire) != generation {
                continue;
            }
            if slot_meta.owner_mask() & OWNER_ANY_WORKER != 0 {
                continue;
            }
            if slot.to_worker_ready.load(Ordering::Acquire)
                || slot.backend_to_worker.has_pending_frame()
            {
                return Some(super::BackendLeaseSlot::new(
                    slot_id,
                    super::BackendLeaseId::new(generation, slot_meta.lease_epoch()),
                ));
            }
        }

        None
    }

    pub fn slot_for_backend_lease(
        &self,
        peer: super::BackendLeaseSlot,
    ) -> Result<WorkerSlot<'_>, SlotAccessError> {
        let incarnation = self.region.claim_worker_slot_for_backend_lease(peer)?;
        Ok(WorkerSlot {
            region: &self.region,
            incarnation,
            slot_id: peer.slot_id(),
            attached: true,
        })
    }
}

impl<'a> Iterator for ReadySlots<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        if self.generation == 0 {
            return None;
        }

        while self.next < self.transport.region.slot_count {
            if !self
                .transport
                .region
                .load_region_meta()
                .is_online_generation(self.generation)
            {
                return None;
            }

            let slot_id = self.next;
            self.next += 1;
            let slot = unsafe { self.transport.region.slot_view_unchecked(slot_id) };
            let slot_meta = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if slot_meta.lease_state() != LEASE_STATE_LEASED {
                continue;
            }
            if slot.slot_generation.load(Ordering::Acquire) != self.generation {
                continue;
            }
            if slot_meta.owner_mask() & OWNER_ANY_WORKER != 0 {
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

impl<'a> Iterator for super::ReadyBackendLeases<'a> {
    type Item = super::BackendLeaseSlot;

    fn next(&mut self) -> Option<Self::Item> {
        if self.generation == 0 {
            return None;
        }

        while self.next < self.transport.region.slot_count {
            if !self
                .transport
                .region
                .load_region_meta()
                .is_online_generation(self.generation)
            {
                return None;
            }

            let slot_id = self.next;
            self.next += 1;
            let slot = unsafe { self.transport.region.slot_view_unchecked(slot_id) };
            let slot_meta = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if slot_meta.lease_state() != LEASE_STATE_LEASED {
                continue;
            }
            if slot.slot_generation.load(Ordering::Acquire) != self.generation {
                continue;
            }
            if slot_meta.owner_mask() & OWNER_ANY_WORKER != 0 {
                continue;
            }
            if slot.to_worker_ready.load(Ordering::Acquire)
                || slot.backend_to_worker.has_pending_frame()
            {
                return Some(super::BackendLeaseSlot::new(
                    slot_id,
                    super::BackendLeaseId::new(self.generation, slot_meta.lease_epoch()),
                ));
            }
        }
        None
    }
}

impl<'a> WorkerSlot<'a> {
    pub fn backend_lease_id(&self) -> super::BackendLeaseId {
        self.incarnation.into()
    }

    pub fn backend_lease_slot(&self) -> super::BackendLeaseSlot {
        super::BackendLeaseSlot::new(self.slot_id, self.backend_lease_id())
    }

    pub fn generation(&self) -> u64 {
        self.incarnation.generation
    }

    pub fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub fn backend_pid(&self) -> i32 {
        let slot = unsafe { self.region.slot_view_unchecked(self.slot_id) };
        slot.backend_pid.load(Ordering::Acquire)
    }

    pub fn from_backend_rx(&mut self) -> Result<WorkerRx<'_, 'a>, SlotAccessError> {
        let slot = self.validate_current_access()?;
        Ok(WorkerRx {
            slot: &*self,
            inner: ControlRx {
                ring: slot.backend_to_worker,
                ready_flag: slot.to_worker_ready,
            },
        })
    }

    pub fn to_backend_tx(&mut self) -> Result<WorkerTx<'_, 'a>, SlotAccessError> {
        let slot = self.validate_current_access()?;
        Ok(WorkerTx {
            slot: &*self,
            inner: ControlTx {
                ring: slot.worker_to_backend,
                ready_flag: slot.to_backend_ready,
                peer_pid: slot.backend_pid,
            },
        })
    }

    pub(super) fn validate_current_access(&self) -> Result<super::SlotView<'a>, SlotAccessError> {
        self.region
            .validate_worker_slot_access(self.slot_id, self.incarnation, self.attached)
    }
}

impl Drop for WorkerSlot<'_> {
    fn drop(&mut self) {
        if self.attached {
            if self
                .region
                .remove_local_worker_owner(self.slot_id, self.incarnation)
            {
                self.region
                    .release_worker_slot(self.slot_id, self.incarnation);
            }
            self.attached = false;
        }
    }
}

#[cfg(test)]
impl WorkerTransport {
    pub(crate) fn forget_local_worker_owners_for_tests(&self) {
        self.region.forget_local_worker_owners_for_tests();
    }

    pub(crate) fn set_worker_state_for_tests(&self, state: u32) {
        self.region.set_worker_state_for_tests(state);
    }
}

impl<'slot, 'region> WorkerTx<'slot, 'region> {
    pub fn send_frame(&mut self, payload: &[u8]) -> Result<super::CommitOutcome, WorkerTxError> {
        self.slot
            .validate_current_access()
            .map_err(WorkerTxError::Slot)?;
        let outcome = self
            .inner
            .send_frame(payload)
            .map_err(WorkerTxError::Ring)?;
        self.slot
            .validate_current_access()
            .map_err(WorkerTxError::Slot)?;
        Ok(outcome)
    }
}

impl<'slot, 'region> WorkerRx<'slot, 'region> {
    pub fn recv_frame_into(&mut self, dst: &mut [u8]) -> Result<Option<usize>, WorkerRxError> {
        self.slot
            .validate_current_access()
            .map_err(WorkerRxError::Slot)?;
        let received = self
            .inner
            .recv_frame_into(dst)
            .map_err(WorkerRxError::Ring)?;
        self.slot
            .validate_current_access()
            .map_err(WorkerRxError::Slot)?;
        Ok(received)
    }
}
