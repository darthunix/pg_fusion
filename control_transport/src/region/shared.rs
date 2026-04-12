use super::layout::{
    build_handle, compute_layout, init_global_cells, init_storage, validate_attached_header,
    validate_region, RegionHeader,
};
use super::{
    SlotView, TransportRegion, TransportRegionLayout, CONTROL_TRANSPORT_MAGIC,
    CONTROL_TRANSPORT_VERSION, LEASE_STATE_FREE, LEASE_STATE_LEASED, OWNER_BACKEND, OWNER_WORKER,
    WORKER_STATE_OFFLINE, WORKER_STATE_ONLINE, WORKER_STATE_RESTARTING,
};
use crate::error::{
    AcquireError, AttachError, InitError, LeaseError, SlotAccessError, WorkerLifecycleError,
};
use crate::ring::FramedRing;
use lockfree::{treiber_stack_ptrs, StackError, TreiberStack};
#[cfg(test)]
use std::cell::RefCell;
use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

type WorkerOwnerRegistry = HashMap<usize, Box<[u64]>>;

fn worker_owner_registry() -> &'static Mutex<WorkerOwnerRegistry> {
    static REGISTRY: OnceLock<Mutex<WorkerOwnerRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
thread_local! {
    static WORKER_CLAIM_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

impl TransportRegion {
    /// # Safety
    /// `base` must point to a writable shared-memory region with at least `len`
    /// bytes and alignment matching `layout.align`.
    pub unsafe fn init_in_place(
        base: NonNull<u8>,
        len: usize,
        layout: TransportRegionLayout,
    ) -> Result<Self, InitError> {
        let computed = compute_layout(
            layout.slot_count,
            layout.backend_to_worker_cap,
            layout.worker_to_backend_cap,
        )
        .map_err(InitError::InvalidConfig)?;
        validate_region(base, len, computed.region.align(), computed.region.size()).map_err(
            |(expected, actual, aligned)| {
                if aligned {
                    InitError::RegionTooSmall { expected, actual }
                } else {
                    InitError::BadAlignment { expected, actual }
                }
            },
        )?;

        let base_ptr = base.as_ptr();
        let header_ptr = base_ptr.cast::<RegionHeader>();
        std::ptr::write(
            header_ptr,
            RegionHeader {
                magic: CONTROL_TRANSPORT_MAGIC,
                version: CONTROL_TRANSPORT_VERSION,
                slot_count: layout.slot_count,
                backend_to_worker_cap: layout.backend_to_worker_cap as u32,
                worker_to_backend_cap: layout.worker_to_backend_cap as u32,
                region_size: computed.region.size() as u64,
            },
        );

        init_global_cells(base_ptr, computed);
        init_storage(base_ptr, computed, layout.slot_count);

        Ok(build_handle(base, layout, computed))
    }

    /// # Safety
    /// `base` must point to a previously initialized `control_transport`
    /// region that remains valid for the lifetime of the returned handle.
    pub unsafe fn attach(base: NonNull<u8>, len: usize) -> Result<Self, AttachError> {
        let (layout, computed) = validate_attached_header(base, len)?;
        Ok(build_handle(base, layout, computed))
    }

    pub fn slot_count(&self) -> u32 {
        self.slot_count
    }

    pub fn backend_to_worker_capacity(&self) -> usize {
        self.backend_to_worker_cap
    }

    pub fn worker_to_backend_capacity(&self) -> usize {
        self.worker_to_backend_cap
    }

    pub fn region_generation(&self) -> u64 {
        self.region_generation_cell().load(Ordering::Acquire)
    }

    /// Publishes a fresh worker generation and returns its number.
    pub fn activate_worker_generation(&self, pid: i32) -> Result<u64, WorkerLifecycleError> {
        self.ensure_no_live_local_worker_slots()?;
        let current_generation = self.region_generation();
        let new_generation = current_generation
            .checked_add(1)
            .expect("control_transport region generation overflow");

        self.worker_state_cell()
            .store(WORKER_STATE_RESTARTING, Ordering::Release);
        self.worker_pid_cell().store(0, Ordering::Release);
        self.region_generation_cell()
            .store(new_generation, Ordering::Release);
        self.sweep_old_generation_slots(new_generation);
        self.worker_pid_cell().store(pid, Ordering::Release);
        self.worker_state_cell()
            .store(WORKER_STATE_ONLINE, Ordering::Release);
        Ok(new_generation)
    }

    /// Invalidates the current generation and leaves the transport offline.
    pub fn deactivate_worker_generation(&self) -> Result<u64, WorkerLifecycleError> {
        self.ensure_no_live_local_worker_slots()?;
        let current_generation = self.region_generation();
        let new_generation = current_generation
            .checked_add(1)
            .expect("control_transport region generation overflow");

        self.worker_state_cell()
            .store(WORKER_STATE_OFFLINE, Ordering::Release);
        self.worker_pid_cell().store(0, Ordering::Release);
        self.region_generation_cell()
            .store(new_generation, Ordering::Release);
        Ok(new_generation)
    }

    pub(super) fn region_generation_cell(&self) -> &AtomicU64 {
        unsafe { self.region_generation.as_ref() }
    }

    pub(super) fn worker_state_cell(&self) -> &AtomicU32 {
        unsafe { self.worker_state.as_ref() }
    }

    pub(super) fn worker_pid_cell(&self) -> &AtomicI32 {
        unsafe { self.worker_pid.as_ref() }
    }

    pub(super) fn ensure_local_worker_registry(&self) {
        let mut registry = worker_owner_registry()
            .lock()
            .expect("worker owner registry poisoned");
        let entry = registry
            .entry(self.region_key())
            .or_insert_with(|| vec![0; self.slot_count as usize].into_boxed_slice());
        if entry.len() != self.slot_count as usize {
            *entry = vec![0; self.slot_count as usize].into_boxed_slice();
        }
    }

    pub(super) fn acquire_slot(&self) -> Result<u32, AcquireError> {
        match self.freelist().allocate() {
            Ok(slot_id) => Ok(slot_id),
            Err(StackError::Empty) => Err(AcquireError::Empty),
            Err(StackError::Full) => unreachable!("TreiberStack::allocate never returns Full"),
        }
    }

    pub(super) fn release_slot(&self, slot_id: u32) {
        let _ = self.freelist().release(slot_id);
    }

    pub(super) fn clear_slot(&self, slot_id: u32, clear_backend_pid: bool) {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        slot.backend_to_worker.clear();
        slot.worker_to_backend.clear();
        slot.to_worker_ready.store(false, Ordering::Release);
        slot.to_backend_ready.store(false, Ordering::Release);
        if clear_backend_pid {
            slot.backend_pid.store(0, Ordering::Release);
        }
    }

    pub(super) fn validate_lease(
        &self,
        slot_id: u32,
        generation: u64,
        active: bool,
    ) -> Result<SlotView<'_>, LeaseError> {
        if !active {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }

        let current_generation = self.region_generation();
        if current_generation != generation {
            return Err(LeaseError::StaleGeneration {
                slot_id,
                claimed_generation: generation,
                current_generation,
            });
        }

        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED
            || slot.slot_generation.load(Ordering::Acquire) != generation
            || slot.owner_mask.load(Ordering::Acquire) & OWNER_BACKEND == 0
        {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }
        Ok(slot)
    }

    pub(super) fn validate_worker_slot_access(
        &self,
        slot_id: u32,
        generation: u64,
        attached: bool,
    ) -> Result<SlotView<'_>, SlotAccessError> {
        if !attached {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }

        let current_generation = self.region_generation();
        if current_generation != generation {
            return Err(SlotAccessError::StaleGeneration {
                slot_id,
                claimed_generation: generation,
                current_generation,
            });
        }
        if current_generation == 0
            || self.worker_state_cell().load(Ordering::Acquire) != WORKER_STATE_ONLINE
        {
            return Err(SlotAccessError::WorkerOffline);
        }

        let slot = self.slot_view(slot_id)?;
        let owner_mask = slot.owner_mask.load(Ordering::Acquire);
        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED
            || slot.slot_generation.load(Ordering::Acquire) != generation
            || owner_mask & OWNER_WORKER == 0
            || owner_mask & OWNER_BACKEND == 0
        {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }
        Ok(slot)
    }

    pub(super) fn claim_worker_slot(&self, slot_id: u32) -> Result<u64, SlotAccessError> {
        let generation = self.region_generation();
        if generation == 0
            || self.worker_state_cell().load(Ordering::Acquire) != WORKER_STATE_ONLINE
        {
            return Err(SlotAccessError::WorkerOffline);
        }

        let slot = self.slot_view(slot_id)?;
        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED
            || slot.slot_generation.load(Ordering::Acquire) != generation
            || slot.owner_mask.load(Ordering::Acquire) & OWNER_BACKEND == 0
        {
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }

        let previous = slot.owner_mask.fetch_or(OWNER_WORKER, Ordering::AcqRel);
        if previous & OWNER_WORKER != 0 {
            return Err(SlotAccessError::Busy {
                slot_id,
                claimed_generation: generation,
            });
        }
        if previous & OWNER_BACKEND == 0 {
            self.rollback_worker_claim(slot_id, generation, slot);
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }

        #[cfg(test)]
        self.run_worker_claim_hook_for_tests();

        let current_generation = self.region_generation();
        let worker_state = self.worker_state_cell().load(Ordering::Acquire);
        let slot_generation = slot.slot_generation.load(Ordering::Acquire);
        let lease_state = slot.lease_state.load(Ordering::Acquire);
        let owner_mask = slot.owner_mask.load(Ordering::Acquire);
        if current_generation != generation
            || worker_state != WORKER_STATE_ONLINE
            || slot_generation != generation
            || lease_state != LEASE_STATE_LEASED
            || owner_mask & OWNER_BACKEND == 0
        {
            self.rollback_worker_claim(slot_id, generation, slot);
            if current_generation != generation {
                return Err(SlotAccessError::StaleGeneration {
                    slot_id,
                    claimed_generation: generation,
                    current_generation,
                });
            }
            if worker_state != WORKER_STATE_ONLINE {
                return Err(SlotAccessError::WorkerOffline);
            }
            return Err(SlotAccessError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }

        if !self.insert_local_worker_owner(slot_id, generation) {
            self.rollback_worker_claim(slot_id, generation, slot);
            return Err(SlotAccessError::Busy {
                slot_id,
                claimed_generation: generation,
            });
        }

        Ok(generation)
    }

    pub(super) fn release_worker_slot(&self, slot_id: u32, generation: u64) {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        if slot.slot_generation.load(Ordering::Acquire) != generation {
            return;
        }

        let previous = slot.owner_mask.fetch_and(!OWNER_WORKER, Ordering::AcqRel);
        if previous & OWNER_WORKER == 0 {
            return;
        }

        if previous & !OWNER_WORKER == 0 {
            let _ = self.try_finalize_slot(slot_id, generation);
        }
    }

    fn rollback_worker_claim(&self, slot_id: u32, generation: u64, slot: SlotView<'_>) {
        let previous = slot.owner_mask.fetch_and(!OWNER_WORKER, Ordering::AcqRel);
        if previous & !OWNER_WORKER == 0 {
            let _ = self.try_finalize_slot(slot_id, generation);
        }
    }

    pub(super) fn release_owned_worker_slots_for_exit(&self) {
        for (slot_id, generation) in self.take_local_worker_owners() {
            self.release_worker_slot(slot_id, generation);
        }
    }

    pub(super) fn try_finalize_slot(&self, slot_id: u32, generation: u64) -> bool {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        if slot.slot_generation.load(Ordering::Acquire) != generation {
            return false;
        }
        if slot.owner_mask.load(Ordering::Acquire) != 0 {
            return false;
        }
        if slot
            .lease_state
            .compare_exchange(
                LEASE_STATE_LEASED,
                LEASE_STATE_FREE,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }

        self.clear_slot(slot_id, true);
        slot.slot_generation.store(0, Ordering::Release);
        slot.owner_mask.store(0, Ordering::Release);
        self.release_slot(slot_id);
        true
    }

    pub(super) fn slot_view(&self, slot_id: u32) -> Result<SlotView<'_>, SlotAccessError> {
        if slot_id >= self.slot_count {
            return Err(SlotAccessError::BadSlotId {
                slot_id,
                slot_count: self.slot_count,
            });
        }
        Ok(unsafe { self.slot_view_unchecked(slot_id) })
    }

    pub(super) unsafe fn slot_view_unchecked(&self, slot_id: u32) -> SlotView<'_> {
        let slot_base = self
            .base
            .as_ptr()
            .add(self.computed.slots_offset + slot_id as usize * self.computed.slot_stride);
        let backend_to_worker = FramedRing::from_layout(
            slot_base.add(self.computed.slot_layout.backend_to_worker_offset),
            self.computed.slot_layout.backend_to_worker_layout,
        );
        let worker_to_backend = FramedRing::from_layout(
            slot_base.add(self.computed.slot_layout.worker_to_backend_offset),
            self.computed.slot_layout.worker_to_backend_layout,
        );
        let to_worker_ready = &*(slot_base.add(self.computed.slot_layout.to_worker_ready_offset)
            as *const AtomicBool);
        let to_backend_ready = &*(slot_base.add(self.computed.slot_layout.to_backend_ready_offset)
            as *const AtomicBool);
        let lease_state =
            &*(slot_base.add(self.computed.slot_layout.lease_state_offset) as *const AtomicU32);
        let slot_generation =
            &*(slot_base.add(self.computed.slot_layout.slot_generation_offset) as *const AtomicU64);
        let owner_mask =
            &*(slot_base.add(self.computed.slot_layout.owner_mask_offset) as *const AtomicU32);
        let backend_pid =
            &*(slot_base.add(self.computed.slot_layout.backend_pid_offset) as *const AtomicI32);

        SlotView {
            backend_to_worker,
            worker_to_backend,
            to_worker_ready,
            to_backend_ready,
            lease_state,
            slot_generation,
            owner_mask,
            backend_pid,
            worker_pid: self.worker_pid_cell(),
        }
    }

    pub(super) fn current_process_pid() -> i32 {
        #[cfg(unix)]
        {
            unsafe { libc::getpid() as i32 }
        }

        #[cfg(not(unix))]
        {
            0
        }
    }

    fn sweep_old_generation_slots(&self, current_generation: u64) {
        for slot_id in 0..self.slot_count {
            let slot = unsafe { self.slot_view_unchecked(slot_id) };
            let slot_generation = slot.slot_generation.load(Ordering::Acquire);
            if slot_generation == 0 || slot_generation == current_generation {
                continue;
            }

            let previous = slot.owner_mask.fetch_and(!OWNER_WORKER, Ordering::AcqRel);
            let owners_after = previous & !OWNER_WORKER;
            if owners_after == 0 && slot.lease_state.load(Ordering::Acquire) == LEASE_STATE_LEASED {
                let _ = self.try_finalize_slot(slot_id, slot_generation);
            }
        }
    }

    fn region_key(&self) -> usize {
        self.base.as_ptr() as usize
    }

    fn ensure_no_live_local_worker_slots(&self) -> Result<(), WorkerLifecycleError> {
        let live_slots = self.local_worker_owner_count();
        if live_slots != 0 {
            return Err(WorkerLifecycleError::HandlesAlive { live_slots });
        }
        Ok(())
    }

    fn local_worker_owner_count(&self) -> usize {
        let registry = worker_owner_registry()
            .lock()
            .expect("worker owner registry poisoned");
        registry
            .get(&self.region_key())
            .map(|entries| {
                entries
                    .iter()
                    .filter(|generation| **generation != 0)
                    .count()
            })
            .unwrap_or(0)
    }

    fn insert_local_worker_owner(&self, slot_id: u32, generation: u64) -> bool {
        let mut registry = worker_owner_registry()
            .lock()
            .expect("worker owner registry poisoned");
        let entries = registry
            .entry(self.region_key())
            .or_insert_with(|| vec![0; self.slot_count as usize].into_boxed_slice());
        if entries.len() != self.slot_count as usize {
            *entries = vec![0; self.slot_count as usize].into_boxed_slice();
        }

        let entry = &mut entries[slot_id as usize];
        if *entry != 0 {
            return false;
        }
        *entry = generation;
        true
    }

    pub(super) fn remove_local_worker_owner(&self, slot_id: u32, generation: u64) -> bool {
        let mut registry = worker_owner_registry()
            .lock()
            .expect("worker owner registry poisoned");
        let Some(entries) = registry.get_mut(&self.region_key()) else {
            return false;
        };
        let entry = &mut entries[slot_id as usize];
        if *entry != generation {
            return false;
        }
        *entry = 0;
        true
    }

    fn take_local_worker_owners(&self) -> Vec<(u32, u64)> {
        let mut registry = worker_owner_registry()
            .lock()
            .expect("worker owner registry poisoned");
        let Some(entries) = registry.get_mut(&self.region_key()) else {
            return Vec::new();
        };

        let mut owned = Vec::new();
        for (slot_id, generation) in entries.iter_mut().enumerate() {
            if *generation == 0 {
                continue;
            }
            owned.push((slot_id as u32, *generation));
            *generation = 0;
        }
        owned
    }

    #[cfg(test)]
    pub(super) fn forget_local_worker_owners_for_tests(&self) {
        let mut registry = worker_owner_registry()
            .lock()
            .expect("worker owner registry poisoned");
        if let Some(entries) = registry.get_mut(&self.region_key()) {
            for generation in entries.iter_mut() {
                *generation = 0;
            }
        }
    }

    #[cfg(test)]
    pub(super) fn set_worker_state_for_tests(&self, state: u32) {
        self.worker_state_cell().store(state, Ordering::Release);
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

    fn freelist(&self) -> TreiberStack {
        let freelist_base = unsafe { self.base.as_ptr().add(self.computed.freelist_offset) };
        unsafe {
            let (header, next) = treiber_stack_ptrs(freelist_base, self.computed.stack_layout);
            TreiberStack::attach(header, next)
        }
    }
}
