use super::layout::{
    bank_index_for_generation, build_handle, compute_layout, init_bank, validate_attached_header,
    validate_region, RegionHeader,
};
use super::{
    ControlRegion, ControlRegionLayout, SlotView, BANK_COUNT, CONTROL_IPC_MAGIC,
    CONTROL_IPC_VERSION, LEASE_STATE_LEASED, WORKER_CLAIM_BLOCKED, WORKER_CLAIM_NONE,
};
use crate::error::{AcquireError, AttachError, InitError, LeaseError, SlotError};
use crate::ring::FramedRing;
use lockfree::{treiber_stack_ptrs, StackError, TreiberStack};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering};

impl ControlRegion {
    /// # Safety
    /// `base` must point to a writable shared-memory region with at least `len`
    /// bytes and alignment matching `layout.align`.
    pub unsafe fn init_in_place(
        base: NonNull<u8>,
        len: usize,
        layout: ControlRegionLayout,
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
                magic: CONTROL_IPC_MAGIC,
                version: CONTROL_IPC_VERSION,
                slot_count: layout.slot_count,
                backend_to_worker_cap: layout.backend_to_worker_cap as u32,
                worker_to_backend_cap: layout.worker_to_backend_cap as u32,
                region_size: computed.region.size() as u64,
            },
        );

        let generation_ptr = base_ptr
            .add(computed.region_generation_offset)
            .cast::<AtomicU64>();
        std::ptr::write(generation_ptr, AtomicU64::new(0));

        let worker_pid_ptr = base_ptr.add(computed.worker_pid_offset).cast::<AtomicI32>();
        std::ptr::write(worker_pid_ptr, AtomicI32::new(0));

        for bank_index in 0..BANK_COUNT {
            let bank_base = base_ptr.add(computed.banks_offset + bank_index * computed.bank_stride);
            init_bank(bank_base, computed.bank_layout, layout.slot_count);
        }

        Ok(build_handle(base, layout, computed))
    }

    /// # Safety
    /// `base` must point to a previously initialized `control_ipc` region that
    /// remains valid for the lifetime of the returned handle.
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

    /// Activates a fresh worker generation and returns its generation number.
    ///
    /// This reinitializes the inactive bank from scratch, stores `pid` as the
    /// current worker PID, and then publishes the new `region_generation`.
    /// Every backend or worker handle from the previous generation becomes
    /// stale after this call.
    pub fn activate_worker_generation(&self, pid: i32) -> u64 {
        let current_generation = self.region_generation();
        let new_generation = current_generation
            .checked_add(1)
            .expect("control_ipc region generation overflow");
        let bank_index = bank_index_for_generation(new_generation);
        let bank_base = self.bank_base(bank_index);
        init_bank(bank_base, self.bank_layout, self.slot_count);
        self.worker_pid_cell().store(pid, Ordering::Release);
        self.region_generation_cell()
            .store(new_generation, Ordering::Release);
        new_generation
    }

    pub(super) fn region_generation_cell(&self) -> &AtomicU64 {
        unsafe { self.region_generation.as_ref() }
    }

    pub(super) fn worker_pid_cell(&self) -> &AtomicI32 {
        unsafe { self.worker_pid.as_ref() }
    }

    pub(super) fn active_bank_index(&self) -> Option<usize> {
        let generation = self.region_generation();
        if generation == 0 {
            None
        } else {
            Some(bank_index_for_generation(generation))
        }
    }

    pub(super) fn bank_base(&self, bank_index: usize) -> *mut u8 {
        debug_assert!(bank_index < BANK_COUNT);
        unsafe { self.banks_base.as_ptr().add(bank_index * self.bank_stride) }
    }

    pub(super) fn bank_freelist(&self, bank_index: usize) -> TreiberStack {
        let bank_base = self.bank_base(bank_index);
        let freelist_base = unsafe { bank_base.add(self.bank_layout.freelist_offset) };
        unsafe {
            let (header, next) = treiber_stack_ptrs(freelist_base, self.bank_layout.stack_layout);
            TreiberStack::attach(header, next)
        }
    }

    pub(super) fn acquire_slot_in_bank(&self, bank_index: usize) -> Result<u32, AcquireError> {
        match self.bank_freelist(bank_index).allocate() {
            Ok(slot_id) => Ok(slot_id),
            Err(StackError::Empty) => Err(AcquireError::Empty),
            Err(StackError::Full) => unreachable!("TreiberStack::allocate never returns Full"),
        }
    }

    pub(super) fn release_slot_in_bank(&self, bank_index: usize, slot_id: u32) {
        let _ = self.bank_freelist(bank_index).release(slot_id);
    }

    pub(super) fn clear_slot_in_bank(
        &self,
        bank_index: usize,
        slot_id: u32,
        clear_backend_pid: bool,
        reset_session_epoch: bool,
    ) {
        let slot = unsafe { self.slot_view_in_bank_unchecked(slot_id, bank_index) };
        slot.backend_to_worker.clear();
        slot.worker_to_backend.clear();
        slot.to_worker_ready.store(false, Ordering::Release);
        slot.to_backend_ready.store(false, Ordering::Release);
        slot.worker_claim_epoch
            .store(WORKER_CLAIM_NONE, Ordering::Release);
        if clear_backend_pid {
            slot.backend_pid.store(0, Ordering::Release);
        }
        if reset_session_epoch {
            slot.session_epoch.store(0, Ordering::Release);
        }
    }

    pub(super) fn validate_lease(
        &self,
        slot_id: u32,
        generation: u64,
        bank_index: usize,
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

        let slot = unsafe { self.slot_view_in_bank_unchecked(slot_id, bank_index) };
        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED {
            return Err(LeaseError::Released {
                slot_id,
                claimed_generation: generation,
            });
        }
        Ok(slot)
    }

    pub(super) fn validate_active_backend_session(
        &self,
        slot_id: u32,
        generation: u64,
        bank_index: usize,
        active: bool,
    ) -> Result<SlotView<'_>, LeaseError> {
        let slot = self.validate_lease(slot_id, generation, bank_index, active)?;
        if slot.session_epoch.load(Ordering::Acquire) == 0
            || slot.worker_claim_epoch.load(Ordering::Acquire) == WORKER_CLAIM_BLOCKED
        {
            return Err(LeaseError::NoActiveSession {
                slot_id,
                claimed_generation: generation,
            });
        }
        Ok(slot)
    }

    pub(super) fn claim_worker_slot_epoch(
        &self,
        slot_id: u32,
    ) -> Result<(u64, usize, u64), SlotError> {
        self.claim_worker_slot_epoch_inner(slot_id, None::<fn()>)
    }

    fn claim_worker_slot_epoch_inner<F>(
        &self,
        slot_id: u32,
        mut post_epoch_load_hook: Option<F>,
    ) -> Result<(u64, usize, u64), SlotError>
    where
        F: FnOnce(),
    {
        let generation = self.region_generation();
        if generation == 0 {
            return Err(SlotError::NoActiveSession { slot_id });
        }
        let bank_index = bank_index_for_generation(generation);
        let slot = self.slot_view_in_bank(slot_id, bank_index)?;
        loop {
            if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED {
                return Err(SlotError::NoActiveSession { slot_id });
            }
            let current_epoch = slot.session_epoch.load(Ordering::Acquire);
            if current_epoch == 0 {
                return Err(SlotError::NoActiveSession { slot_id });
            }
            if let Some(hook) = post_epoch_load_hook.take() {
                hook();
            }

            loop {
                let claimed = slot.worker_claim_epoch.load(Ordering::Acquire);
                if claimed == WORKER_CLAIM_BLOCKED {
                    return Err(SlotError::NoActiveSession { slot_id });
                }
                if claimed == current_epoch {
                    return Err(SlotError::Busy {
                        slot_id,
                        session_epoch: current_epoch,
                    });
                }

                match slot.worker_claim_epoch.compare_exchange(
                    claimed,
                    current_epoch,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        let current_generation = self.region_generation();
                        if current_generation != generation {
                            return Err(SlotError::StaleGeneration {
                                slot_id,
                                claimed_generation: generation,
                                current_generation,
                            });
                        }
                        if slot.lease_state.load(Ordering::Acquire) != LEASE_STATE_LEASED {
                            let _ = slot.worker_claim_epoch.compare_exchange(
                                current_epoch,
                                WORKER_CLAIM_NONE,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            );
                            return Err(SlotError::NoActiveSession { slot_id });
                        }
                        let observed_epoch = slot.session_epoch.load(Ordering::Acquire);
                        if observed_epoch != current_epoch {
                            let _ = slot.worker_claim_epoch.compare_exchange(
                                current_epoch,
                                WORKER_CLAIM_NONE,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            );
                            break;
                        }
                        return Ok((generation, bank_index, current_epoch));
                    }
                    Err(observed) if observed == current_epoch => {
                        return Err(SlotError::Busy {
                            slot_id,
                            session_epoch: current_epoch,
                        });
                    }
                    Err(_) => continue,
                }
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn claim_worker_slot_epoch_with_post_epoch_load_hook_for_test<F>(
        &self,
        slot_id: u32,
        hook: F,
    ) -> Result<(u64, usize, u64), SlotError>
    where
        F: FnOnce(),
    {
        self.claim_worker_slot_epoch_inner(slot_id, Some(hook))
    }

    pub(super) fn release_worker_slot_claim(
        &self,
        slot_id: u32,
        generation: u64,
        bank_index: usize,
        claimed_epoch: u64,
    ) {
        if self.region_generation() != generation {
            return;
        }
        let slot = match self.slot_view_in_bank(slot_id, bank_index) {
            Ok(slot) => slot,
            Err(_) => return,
        };
        let _ = slot.worker_claim_epoch.compare_exchange(
            claimed_epoch,
            WORKER_CLAIM_NONE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(super) fn slot_view_in_bank(
        &self,
        slot_id: u32,
        bank_index: usize,
    ) -> Result<SlotView<'_>, SlotError> {
        if slot_id >= self.slot_count {
            return Err(SlotError::BadSlotId {
                slot_id,
                slot_count: self.slot_count,
            });
        }
        Ok(unsafe { self.slot_view_in_bank_unchecked(slot_id, bank_index) })
    }

    pub(super) unsafe fn slot_view_in_bank_unchecked(
        &self,
        slot_id: u32,
        bank_index: usize,
    ) -> SlotView<'_> {
        let bank_base = self.bank_base(bank_index);
        let slot_base = bank_base
            .add(self.bank_layout.slots_offset + slot_id as usize * self.bank_layout.slot_stride);
        let backend_to_worker = FramedRing::from_layout(
            slot_base.add(self.bank_layout.slot_layout.backend_to_worker_offset),
            self.bank_layout.slot_layout.backend_to_worker_layout,
        );
        let worker_to_backend = FramedRing::from_layout(
            slot_base.add(self.bank_layout.slot_layout.worker_to_backend_offset),
            self.bank_layout.slot_layout.worker_to_backend_layout,
        );
        let to_worker_ready = &*(slot_base.add(self.bank_layout.slot_layout.to_worker_ready_offset)
            as *const AtomicBool);
        let to_backend_ready = &*(slot_base
            .add(self.bank_layout.slot_layout.to_backend_ready_offset)
            as *const AtomicBool);
        let worker_claim_epoch = &*(slot_base
            .add(self.bank_layout.slot_layout.worker_claim_epoch_offset)
            as *const AtomicU64);
        let lease_state =
            &*(slot_base.add(self.bank_layout.slot_layout.lease_state_offset) as *const AtomicU32);
        let backend_pid =
            &*(slot_base.add(self.bank_layout.slot_layout.backend_pid_offset) as *const AtomicI32);
        let session_epoch = &*(slot_base.add(self.bank_layout.slot_layout.session_epoch_offset)
            as *const AtomicU64);

        SlotView {
            backend_to_worker,
            worker_to_backend,
            to_worker_ready,
            to_backend_ready,
            worker_claim_epoch,
            lease_state,
            backend_pid,
            session_epoch,
            worker_pid: self.worker_pid_cell(),
        }
    }

    #[cfg(test)]
    pub(crate) fn clear_to_worker_ready_for_test(&self, slot_id: u32) {
        if let Some(bank_index) = self.active_bank_index() {
            let slot = self
                .slot_view_in_bank(slot_id, bank_index)
                .expect("valid slot");
            slot.to_worker_ready.store(false, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn clear_backend_to_worker_with_hook_for_test<F>(&self, slot_id: u32, hook: F)
    where
        F: FnOnce(),
    {
        let bank_index = self.active_bank_index().expect("active bank");
        let slot = self
            .slot_view_in_bank(slot_id, bank_index)
            .expect("valid slot");
        slot.backend_to_worker.clear_with_hook(hook);
    }

    #[cfg(test)]
    pub(crate) fn force_to_backend_ready_for_test(&self, slot_id: u32, ready: bool) {
        if let Some(bank_index) = self.active_bank_index() {
            let slot = self
                .slot_view_in_bank(slot_id, bank_index)
                .expect("valid slot");
            slot.to_backend_ready.store(ready, Ordering::Release);
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
}
