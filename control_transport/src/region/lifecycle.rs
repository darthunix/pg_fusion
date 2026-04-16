use super::{
    LeaseIncarnation, RegionMeta, SlotMeta, SlotView, TransportRegion, LEASE_STATE_LEASED,
    OWNER_ANY_WORKER, OWNER_BACKEND, OWNER_WORKER, OWNER_WORKER_PENDING, WORKER_STATE_OFFLINE,
    WORKER_STATE_ONLINE, WORKER_STATE_RESTARTING,
};
use crate::error::{LeaseError, SlotAccessError, WorkerLifecycleError};
use crate::process::probe_pid_alive;
use std::sync::atomic::Ordering;

#[cfg(test)]
use std::cell::RefCell;
#[cfg(all(test, debug_assertions))]
use std::collections::HashMap;
#[cfg(debug_assertions)]
use std::collections::HashSet;
#[cfg(all(test, debug_assertions))]
use std::sync::Arc;
#[cfg(debug_assertions)]
use std::sync::{Mutex, OnceLock};

#[cfg(test)]
thread_local! {
    static WORKER_CLAIM_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

#[cfg(debug_assertions)]
struct LifecycleTransitionGuard {
    region_key: usize,
}

#[derive(Clone, Copy)]
struct RegionSnapshot {
    generation: u64,
    worker_state: u32,
}

impl RegionSnapshot {
    fn is_online(self) -> bool {
        self.generation != 0 && self.worker_state == WORKER_STATE_ONLINE
    }
}

#[derive(Clone, Copy)]
struct SlotSnapshot {
    slot_generation: u64,
    slot_meta: SlotMeta,
    backend_pid: i32,
}

impl SlotSnapshot {
    fn matches_incarnation(self, incarnation: LeaseIncarnation) -> bool {
        self.slot_generation == incarnation.generation
            && self.slot_meta.lease_epoch() == incarnation.lease_epoch
    }

    fn is_leased(self) -> bool {
        self.slot_meta.is_leased()
    }

    fn is_ownerless(self) -> bool {
        self.slot_meta.is_ownerless()
    }

    fn has_backend_owner(self) -> bool {
        self.slot_meta.has_backend_owner()
    }

    fn has_worker_owner(self) -> bool {
        self.slot_meta.has_worker_owner()
    }

    fn has_any_worker_owner(self) -> bool {
        self.slot_meta.has_any_worker_owner()
    }

    fn lease_epoch(self) -> u64 {
        self.slot_meta.lease_epoch()
    }

    fn owner_mask(self) -> u32 {
        self.slot_meta.owner_mask()
    }
}

#[derive(Clone, Copy)]
pub(super) struct OwnerMutationResult {
    changed: bool,
    remaining_mask: u32,
}

impl OwnerMutationResult {
    fn unchanged(owner_mask: u32) -> Self {
        Self {
            changed: false,
            remaining_mask: owner_mask,
        }
    }

    pub(super) fn changed(self) -> bool {
        self.changed
    }

    fn ownerless(self) -> bool {
        self.remaining_mask == 0
    }
}

struct WorkerOwnerReservation<'a> {
    region: &'a TransportRegion,
    slot_id: u32,
    incarnation: LeaseIncarnation,
    kept: bool,
}

impl<'a> WorkerOwnerReservation<'a> {
    fn reserve(
        region: &'a TransportRegion,
        slot_id: u32,
        incarnation: LeaseIncarnation,
    ) -> Option<Self> {
        if !region.insert_local_worker_owner(slot_id, incarnation) {
            return None;
        }
        Some(Self {
            region,
            slot_id,
            incarnation,
            kept: false,
        })
    }

    fn keep(mut self) {
        self.kept = true;
    }
}

impl Drop for WorkerOwnerReservation<'_> {
    fn drop(&mut self) {
        if !self.kept {
            let _ = self
                .region
                .remove_local_worker_owner(self.slot_id, self.incarnation);
        }
    }
}

#[cfg(all(test, debug_assertions))]
type LifecycleTransitionHook = Arc<dyn Fn() + Send + Sync + 'static>;

#[cfg(debug_assertions)]
fn active_lifecycle_transitions() -> &'static Mutex<HashSet<usize>> {
    static ACTIVE: OnceLock<Mutex<HashSet<usize>>> = OnceLock::new();
    ACTIVE.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(all(test, debug_assertions))]
fn lifecycle_transition_hooks() -> &'static Mutex<HashMap<usize, LifecycleTransitionHook>> {
    static HOOKS: OnceLock<Mutex<HashMap<usize, LifecycleTransitionHook>>> = OnceLock::new();
    HOOKS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(debug_assertions)]
impl Drop for LifecycleTransitionGuard {
    fn drop(&mut self) {
        let mut active = active_lifecycle_transitions()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.remove(&self.region_key);
    }
}

impl TransportRegion {
    /// Publishes a fresh worker generation and returns its number.
    ///
    /// If the worker crashes mid-sequence, the region may remain in the
    /// intermediate `RESTARTING` state. That is recoverable: the next worker
    /// startup can call `activate_worker_generation()` again and publish a
    /// newer generation.
    pub fn activate_worker_generation(&self, pid: i32) -> Result<u64, WorkerLifecycleError> {
        #[cfg(debug_assertions)]
        let _transition_guard = self.enter_lifecycle_transition_guard();
        #[cfg(all(test, debug_assertions))]
        self.run_lifecycle_transition_hook_for_tests();

        self.ensure_no_live_local_worker_slots()?;
        let new_generation = self.next_region_generation();
        self.worker_pid_cell().store(0, Ordering::Release);
        self.store_region_meta(RegionMeta::new(new_generation, WORKER_STATE_RESTARTING));
        self.sweep_old_generation_slots(new_generation);
        self.worker_pid_cell().store(pid, Ordering::Release);
        self.store_region_meta(RegionMeta::new(new_generation, WORKER_STATE_ONLINE));
        Ok(new_generation)
    }

    /// Invalidates the current generation and leaves the transport offline.
    pub fn deactivate_worker_generation(&self) -> Result<u64, WorkerLifecycleError> {
        #[cfg(debug_assertions)]
        let _transition_guard = self.enter_lifecycle_transition_guard();
        #[cfg(all(test, debug_assertions))]
        self.run_lifecycle_transition_hook_for_tests();

        self.ensure_no_live_local_worker_slots()?;
        let new_generation = self.next_region_generation();
        self.worker_pid_cell().store(0, Ordering::Release);
        self.store_region_meta(RegionMeta::new(new_generation, WORKER_STATE_OFFLINE));
        Ok(new_generation)
    }

    fn load_region_snapshot(&self) -> RegionSnapshot {
        let region_meta = self.load_region_meta();
        RegionSnapshot {
            generation: region_meta.generation(),
            worker_state: region_meta.worker_state(),
        }
    }

    fn load_slot_snapshot(&self, slot: SlotView<'_>) -> SlotSnapshot {
        SlotSnapshot {
            slot_generation: slot.slot_generation.load(Ordering::Acquire),
            slot_meta: SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire)),
            backend_pid: slot.backend_pid.load(Ordering::Acquire),
        }
    }

    pub(super) fn clear_owner_bits_if_matching(
        &self,
        slot: SlotView<'_>,
        incarnation: LeaseIncarnation,
        clear_bits: u32,
    ) -> OwnerMutationResult {
        let mut current_raw = slot.slot_meta.load(Ordering::Acquire);
        loop {
            let current = SlotMeta::from_raw(current_raw);
            if slot.slot_generation.load(Ordering::Acquire) != incarnation.generation
                || !current.is_leased()
                || current.lease_epoch() != incarnation.lease_epoch
            {
                return OwnerMutationResult::unchanged(current.owner_mask());
            }

            let remaining_mask = current.owner_mask() & !clear_bits;
            if remaining_mask == current.owner_mask() {
                return OwnerMutationResult::unchanged(remaining_mask);
            }

            let next = current.with_owner_mask(remaining_mask);
            match slot.slot_meta.compare_exchange(
                current_raw,
                next.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return OwnerMutationResult {
                        changed: true,
                        remaining_mask,
                    };
                }
                Err(observed) => current_raw = observed,
            }
        }
    }

    pub(super) fn finalize_if_ownerless(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
        mutation: OwnerMutationResult,
    ) {
        if mutation.changed && mutation.ownerless() {
            let _ = self.try_finalize_slot(slot_id, incarnation);
        }
    }

    pub(super) fn validate_lease(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
        active: bool,
    ) -> Result<SlotView<'_>, LeaseError> {
        if !active {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        let region_snapshot = self.load_region_snapshot();
        if region_snapshot.generation != incarnation.generation {
            return Err(LeaseError::StaleGeneration {
                slot_id,
                claimed_generation: incarnation.generation,
                current_generation: region_snapshot.generation,
            });
        }

        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        let slot_snapshot = self.load_slot_snapshot(slot);
        if !slot_snapshot.is_leased() {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        if slot_snapshot.slot_generation != incarnation.generation {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        if slot_snapshot.lease_epoch() != incarnation.lease_epoch {
            return Err(LeaseError::StaleLeaseEpoch {
                slot_id,
                claimed_generation: incarnation.generation,
                claimed_lease_epoch: incarnation.lease_epoch,
                current_lease_epoch: slot_snapshot.lease_epoch(),
            });
        }

        if !slot_snapshot.has_backend_owner() {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        Ok(slot)
    }

    pub(super) fn validate_worker_slot_access(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
        attached: bool,
    ) -> Result<SlotView<'_>, SlotAccessError> {
        if !attached {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        let region_snapshot = self.load_region_snapshot();
        if region_snapshot.generation != incarnation.generation {
            return Err(SlotAccessError::StaleGeneration {
                slot_id,
                claimed_generation: incarnation.generation,
                current_generation: region_snapshot.generation,
            });
        }
        if !region_snapshot.is_online() {
            return Err(SlotAccessError::WorkerOffline);
        }

        let slot = self.slot_view(slot_id)?;
        let slot_snapshot = self.load_slot_snapshot(slot);
        if !slot_snapshot.is_leased() {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        if slot_snapshot.slot_generation != incarnation.generation {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        if slot_snapshot.lease_epoch() != incarnation.lease_epoch {
            return Err(SlotAccessError::StaleLeaseEpoch {
                slot_id,
                claimed_generation: incarnation.generation,
                claimed_lease_epoch: incarnation.lease_epoch,
                current_lease_epoch: slot_snapshot.lease_epoch(),
            });
        }

        if !slot_snapshot.has_worker_owner() || !slot_snapshot.has_backend_owner() {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: incarnation.generation,
            });
        }

        Ok(slot)
    }

    pub(super) fn claim_worker_slot(
        &self,
        slot_id: u32,
    ) -> Result<LeaseIncarnation, SlotAccessError> {
        let region_snapshot = self.load_region_snapshot();
        if !region_snapshot.is_online() {
            return Err(SlotAccessError::WorkerOffline);
        }

        let slot = self.slot_view(slot_id)?;
        let slot_snapshot = self.load_slot_snapshot(slot);
        if !slot_snapshot.is_leased()
            || slot_snapshot.slot_generation != region_snapshot.generation
            || slot_snapshot.lease_epoch() == 0
        {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: region_snapshot.generation,
            });
        }
        if !slot_snapshot.has_backend_owner() {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: region_snapshot.generation,
            });
        }
        if slot_snapshot.has_any_worker_owner() {
            return Err(SlotAccessError::Busy {
                slot_id,
                claimed_generation: region_snapshot.generation,
            });
        }

        let incarnation =
            LeaseIncarnation::new(region_snapshot.generation, slot_snapshot.lease_epoch());
        match self.reap_dead_backend_owner(slot_id, incarnation, slot) {
            Ok(true) => {
                return Err(SlotAccessError::Released {
                    slot_id,
                    claimed_generation: region_snapshot.generation,
                });
            }
            Ok(false) => {}
            Err(err) => {
                return Err(SlotAccessError::BackendProbeFailed {
                    slot_id,
                    claimed_generation: region_snapshot.generation,
                    error_kind: err.kind(),
                    raw_os_error: err.raw_os_error(),
                });
            }
        }

        let local_owner = WorkerOwnerReservation::reserve(self, slot_id, incarnation).ok_or(
            SlotAccessError::Busy {
                slot_id,
                claimed_generation: region_snapshot.generation,
            },
        )?;

        let pending_mask = OWNER_BACKEND | OWNER_WORKER_PENDING;
        let expected_meta =
            SlotMeta::new(LEASE_STATE_LEASED, incarnation.lease_epoch, OWNER_BACKEND);
        let pending_meta = SlotMeta::new(LEASE_STATE_LEASED, incarnation.lease_epoch, pending_mask);
        if slot
            .slot_meta
            .compare_exchange(
                expected_meta.raw(),
                pending_meta.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(self.classify_worker_claim_failure(
                slot_id,
                region_snapshot.generation,
                incarnation,
                slot,
            ));
        }

        #[cfg(test)]
        self.run_worker_claim_hook_for_tests();

        let recheck_region = self.load_region_snapshot();
        let recheck_slot = self.load_slot_snapshot(slot);
        if recheck_region.generation != region_snapshot.generation
            || recheck_region.worker_state != WORKER_STATE_ONLINE
            || recheck_slot.slot_generation != region_snapshot.generation
            || recheck_slot.lease_epoch() != incarnation.lease_epoch
            || !recheck_slot.is_leased()
            || recheck_slot.owner_mask() != pending_mask
        {
            self.rollback_worker_claim(slot_id, incarnation, slot);
            if recheck_region.generation != region_snapshot.generation {
                return Err(SlotAccessError::StaleGeneration {
                    slot_id,
                    claimed_generation: region_snapshot.generation,
                    current_generation: recheck_region.generation,
                });
            }
            if recheck_region.worker_state != WORKER_STATE_ONLINE {
                return Err(SlotAccessError::WorkerOffline);
            }
            if recheck_slot.lease_epoch() != incarnation.lease_epoch {
                return Err(SlotAccessError::StaleLeaseEpoch {
                    slot_id,
                    claimed_generation: region_snapshot.generation,
                    claimed_lease_epoch: incarnation.lease_epoch,
                    current_lease_epoch: recheck_slot.lease_epoch(),
                });
            }
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: region_snapshot.generation,
            });
        }

        match self.backend_owner_alive(recheck_slot.backend_pid) {
            Ok(true) => {}
            Ok(false) => {
                self.rollback_worker_claim(slot_id, incarnation, slot);
                let _ = self.reap_dead_backend_owner(slot_id, incarnation, slot);
                return Err(SlotAccessError::Released {
                    slot_id,
                    claimed_generation: region_snapshot.generation,
                });
            }
            Err(err) => {
                self.rollback_worker_claim(slot_id, incarnation, slot);
                return Err(SlotAccessError::BackendProbeFailed {
                    slot_id,
                    claimed_generation: region_snapshot.generation,
                    error_kind: err.kind(),
                    raw_os_error: err.raw_os_error(),
                });
            }
        }

        let committed_meta = SlotMeta::new(
            LEASE_STATE_LEASED,
            incarnation.lease_epoch,
            OWNER_BACKEND | OWNER_WORKER,
        );
        if slot
            .slot_meta
            .compare_exchange(
                pending_meta.raw(),
                committed_meta.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.rollback_worker_claim(slot_id, incarnation, slot);
            return Err(self.classify_worker_claim_failure(
                slot_id,
                region_snapshot.generation,
                incarnation,
                slot,
            ));
        }

        local_owner.keep();
        Ok(incarnation)
    }

    pub(super) fn release_worker_slot(&self, slot_id: u32, incarnation: LeaseIncarnation) {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        let mutation = self.clear_owner_bits_if_matching(slot, incarnation, OWNER_WORKER);
        if !mutation.changed {
            return;
        }
        self.finalize_if_ownerless(slot_id, incarnation, mutation);
    }

    pub(super) fn release_owned_worker_slots_for_exit(&self) {
        for (slot_id, incarnation) in self.take_local_worker_owners() {
            self.release_worker_slot(slot_id, incarnation);
        }
    }

    pub(super) fn try_finalize_slot(&self, slot_id: u32, incarnation: LeaseIncarnation) -> bool {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        let snapshot = self.load_slot_snapshot(slot);
        if !snapshot.matches_incarnation(incarnation) {
            return false;
        }
        if !snapshot.is_leased() || !snapshot.is_ownerless() {
            return false;
        }
        if slot
            .slot_meta
            .compare_exchange(
                snapshot.slot_meta.raw(),
                SlotMeta::FREE_PENDING.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }

        self.clear_slot(slot_id);
        slot.slot_generation.store(0, Ordering::Release);
        let _ = self.publish_free_slot(slot_id, slot);
        true
    }

    fn rollback_worker_claim(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
        slot: SlotView<'_>,
    ) {
        let mutation = self.clear_owner_bits_if_matching(slot, incarnation, OWNER_WORKER_PENDING);
        self.finalize_if_ownerless(slot_id, incarnation, mutation);
    }

    fn classify_worker_claim_failure(
        &self,
        slot_id: u32,
        claimed_generation: u64,
        incarnation: LeaseIncarnation,
        slot: SlotView<'_>,
    ) -> SlotAccessError {
        let region_snapshot = self.load_region_snapshot();
        if region_snapshot.generation != claimed_generation {
            return SlotAccessError::StaleGeneration {
                slot_id,
                claimed_generation,
                current_generation: region_snapshot.generation,
            };
        }
        if !region_snapshot.is_online() {
            return SlotAccessError::WorkerOffline;
        }
        let slot_snapshot = self.load_slot_snapshot(slot);
        if !slot_snapshot.is_leased() || slot_snapshot.slot_generation != claimed_generation {
            return SlotAccessError::Released {
                slot_id,
                claimed_generation,
            };
        }

        if slot_snapshot.lease_epoch() != incarnation.lease_epoch {
            return SlotAccessError::StaleLeaseEpoch {
                slot_id,
                claimed_generation,
                claimed_lease_epoch: incarnation.lease_epoch,
                current_lease_epoch: slot_snapshot.lease_epoch(),
            };
        }

        if !slot_snapshot.has_backend_owner() {
            return SlotAccessError::Released {
                slot_id,
                claimed_generation,
            };
        }
        if slot_snapshot.has_any_worker_owner() {
            return SlotAccessError::Busy {
                slot_id,
                claimed_generation,
            };
        }

        SlotAccessError::Released {
            slot_id,
            claimed_generation,
        }
    }

    fn backend_owner_alive(&self, pid: i32) -> std::io::Result<bool> {
        let _ = self;
        probe_pid_alive(pid)
    }

    fn reap_dead_backend_owner(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
        slot: SlotView<'_>,
    ) -> std::io::Result<bool> {
        let snapshot = self.load_slot_snapshot(slot);
        if !snapshot.is_leased() || !snapshot.matches_incarnation(incarnation) {
            return Ok(false);
        }
        if !snapshot.has_backend_owner() {
            return Ok(false);
        }
        if self.backend_owner_alive(snapshot.backend_pid)? {
            return Ok(false);
        }

        let mutation = self.clear_owner_bits_if_matching(slot, incarnation, OWNER_BACKEND);
        if !mutation.changed {
            return Ok(false);
        }

        slot.backend_pid.store(0, Ordering::Release);
        self.finalize_if_ownerless(slot_id, incarnation, mutation);
        Ok(true)
    }

    /// Best-effort slow-path reaper for backend admission when the freelist is
    /// empty. If at least one dead slot is reclaimed, later probe failures are
    /// intentionally treated as noise and only a successful reclaim is
    /// reported. The first probe error is returned only when nothing was
    /// reclaimed.
    pub(super) fn reap_current_generation_dead_backend_slots(
        &self,
    ) -> Result<bool, (u32, std::io::Error)> {
        let generation = self.region_generation();
        if generation == 0 {
            return Ok(false);
        }

        let mut reaped_any = false;
        let mut first_err = None;

        for slot_id in 0..self.slot_count {
            let slot = unsafe { self.slot_view_unchecked(slot_id) };
            let snapshot = self.load_slot_snapshot(slot);
            if !snapshot.is_leased() || snapshot.slot_generation != generation {
                continue;
            }

            if snapshot.lease_epoch() == 0 {
                continue;
            }

            match self.reap_dead_backend_owner(
                slot_id,
                LeaseIncarnation::new(generation, snapshot.lease_epoch()),
                slot,
            ) {
                Ok(true) => reaped_any = true,
                Ok(false) => {}
                Err(err) => {
                    if first_err.is_none() {
                        first_err = Some((slot_id, err));
                    }
                }
            }
        }

        if reaped_any {
            return Ok(true);
        }

        if let Some((slot_id, err)) = first_err {
            return Err((slot_id, err));
        }

        Ok(false)
    }

    fn sweep_old_generation_slots(&self, current_generation: u64) {
        for slot_id in 0..self.slot_count {
            let slot = unsafe { self.slot_view_unchecked(slot_id) };
            let mut snapshot = self.load_slot_snapshot(slot);
            if snapshot.slot_generation == 0 || snapshot.slot_generation == current_generation {
                continue;
            }

            let incarnation =
                LeaseIncarnation::new(snapshot.slot_generation, snapshot.lease_epoch());
            if incarnation.lease_epoch == 0 {
                continue;
            }

            if snapshot.has_backend_owner() {
                let _ = self.reap_dead_backend_owner(slot_id, incarnation, slot);
                snapshot = self.load_slot_snapshot(slot);
            }

            if !snapshot.has_any_worker_owner() {
                if snapshot.is_ownerless() && snapshot.is_leased() {
                    let _ = self.try_finalize_slot(slot_id, incarnation);
                }
                continue;
            }

            let mutation = self.clear_owner_bits_if_matching(slot, incarnation, OWNER_ANY_WORKER);
            self.finalize_if_ownerless(slot_id, incarnation, mutation);
        }
    }

    #[cfg(debug_assertions)]
    fn enter_lifecycle_transition_guard(&self) -> LifecycleTransitionGuard {
        let region_key = self.region_key();
        let inserted = {
            let mut active = active_lifecycle_transitions()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            active.insert(region_key)
        };
        assert!(
            inserted,
            "control_transport lifecycle transitions are not reentrant for the same region"
        );
        LifecycleTransitionGuard { region_key }
    }

    #[cfg(test)]
    pub(crate) fn set_worker_claim_hook_for_tests<F>(&self, hook: F)
    where
        F: FnOnce() + 'static,
    {
        let _ = self;
        WORKER_CLAIM_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(hook));
        });
    }

    #[cfg(test)]
    fn run_worker_claim_hook_for_tests(&self) {
        let _ = self;
        WORKER_CLAIM_HOOK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn set_lifecycle_transition_hook_for_tests<F>(&self, hook: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let region_key = self.region_key();
        let mut hooks = lifecycle_transition_hooks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        hooks.insert(region_key, Arc::new(hook));
    }

    #[cfg(all(test, debug_assertions))]
    pub(crate) fn clear_lifecycle_transition_hook_for_tests(&self) {
        let region_key = self.region_key();
        let mut hooks = lifecycle_transition_hooks()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        hooks.remove(&region_key);
    }

    #[cfg(all(test, debug_assertions))]
    fn run_lifecycle_transition_hook_for_tests(&self) {
        let hook = {
            let hooks = lifecycle_transition_hooks()
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            hooks.get(&self.region_key()).cloned()
        };
        if let Some(hook) = hook {
            hook();
        }
    }
}
