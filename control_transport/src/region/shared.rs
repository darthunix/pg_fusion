use super::layout::{
    build_handle, compute_layout, init_global_cells, init_storage, validate_attached_header,
    validate_region, RegionHeader,
};
use super::{
    LeaseIncarnation, RegionMeta, SlotMeta, SlotView, TransportRegion, TransportRegionLayout,
    CONTROL_TRANSPORT_MAGIC, CONTROL_TRANSPORT_VERSION, REGION_META_MAX_GENERATION,
    SLOT_META_MAX_LEASE_EPOCH, WORKER_STATE_OFFLINE, WORKER_STATE_REINITING,
};
use crate::error::{
    AcquireError, AttachError, InitError, ReinitError, SlotAccessError, WorkerAttachError,
    WorkerLifecycleError,
};
use crate::process::probe_pid_alive;
use crate::ring::FramedRing;
use lockfree::{treiber_stack_ptrs, StackError, TreiberStack};
use portable_atomic::AtomicU128;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU64, Ordering};

#[cfg(test)]
use std::cell::RefCell;

#[cfg(test)]
thread_local! {
    static BACKEND_ACQUIRE_POPPED_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static REINIT_REBUILD_PASS_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
    static FREE_SLOT_PUSHED_HOOK: RefCell<Option<Box<dyn FnOnce()>>> = const { RefCell::new(None) };
}

struct ProcessWorkerRegistry {
    owner_pid: i32,
    region_key: usize,
    slot_count: u32,
    entries: Box<[AtomicU128]>,
}

// This process-local mirror is replaced atomically on PID/region/slot-count
// changes. Replaced registries are intentionally leaked because concurrent
// threads may still hold raw references returned before the swap.
static WORKER_OWNER_REGISTRY: AtomicPtr<ProcessWorkerRegistry> = AtomicPtr::new(ptr::null_mut());

impl ProcessWorkerRegistry {
    fn new(owner_pid: i32, region_key: usize, slot_count: u32) -> Self {
        let entries = (0..slot_count)
            .map(|_| AtomicU128::new(0))
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            owner_pid,
            region_key,
            slot_count,
            entries,
        }
    }

    fn entry(&self, slot_id: u32) -> &AtomicU128 {
        &self.entries[slot_id as usize]
    }
}

fn pack_local_owner(incarnation: LeaseIncarnation) -> u128 {
    ((incarnation.generation as u128) << 64) | incarnation.lease_epoch as u128
}

fn unpack_local_owner(raw: u128) -> LeaseIncarnation {
    if raw == 0 {
        return LeaseIncarnation::FREE;
    }
    LeaseIncarnation::new((raw >> 64) as u64, raw as u64)
}

fn install_or_get_worker_owner_registry(
    owner_pid: i32,
    region_key: usize,
    slot_count: u32,
) -> Result<&'static ProcessWorkerRegistry, WorkerAttachError> {
    let mut ptr = WORKER_OWNER_REGISTRY.load(Ordering::Acquire);
    loop {
        if ptr.is_null() {
            let candidate = Box::new(ProcessWorkerRegistry::new(
                owner_pid, region_key, slot_count,
            ));
            let candidate_ptr = Box::into_raw(candidate);
            match WORKER_OWNER_REGISTRY.compare_exchange(
                ptr::null_mut(),
                candidate_ptr,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(unsafe { &*candidate_ptr }),
                Err(existing) => {
                    unsafe {
                        drop(Box::from_raw(candidate_ptr));
                    }
                    ptr = existing;
                    continue;
                }
            }
        }

        debug_assert!(!ptr.is_null());
        let registry = unsafe { &*ptr };
        if registry.owner_pid == owner_pid
            && registry.region_key == region_key
            && registry.slot_count == slot_count
        {
            return Ok(registry);
        }
        if registry.owner_pid == owner_pid && registry.region_key != region_key {
            return Err(WorkerAttachError::RegionAlreadyAttached {
                existing_region_key: registry.region_key,
                requested_region_key: region_key,
            });
        }

        let candidate = Box::new(ProcessWorkerRegistry::new(
            owner_pid, region_key, slot_count,
        ));
        let candidate_ptr = Box::into_raw(candidate);
        match WORKER_OWNER_REGISTRY.compare_exchange(
            ptr,
            candidate_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Intentionally leak the replaced registry. Other threads may
                // still hold raw references returned before this swap.
                return Ok(unsafe { &*candidate_ptr });
            }
            Err(existing) => {
                unsafe {
                    drop(Box::from_raw(candidate_ptr));
                }
                ptr = existing;
            }
        }
    }
}

fn reset_worker_owner_registry(owner_pid: i32, region_key: usize, slot_count: u32) {
    let mut ptr = WORKER_OWNER_REGISTRY.load(Ordering::Acquire);
    loop {
        if ptr.is_null() {
            return;
        }

        let registry = unsafe { &*ptr };
        if registry.owner_pid != owner_pid || registry.region_key != region_key {
            return;
        }

        let candidate = Box::new(ProcessWorkerRegistry::new(
            owner_pid, region_key, slot_count,
        ));
        let candidate_ptr = Box::into_raw(candidate);
        match WORKER_OWNER_REGISTRY.compare_exchange(
            ptr,
            candidate_ptr,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => {
                // Intentionally leak the replaced registry. Other threads may
                // still hold raw references returned before this swap.
                return;
            }
            Err(existing) => {
                unsafe {
                    drop(Box::from_raw(candidate_ptr));
                }
                ptr = existing;
            }
        }
    }
}

fn load_worker_owner_registry() -> Option<&'static ProcessWorkerRegistry> {
    let ptr = WORKER_OWNER_REGISTRY.load(Ordering::Acquire);
    if ptr.is_null() {
        None
    } else {
        Some(unsafe { &*ptr })
    }
}

fn registry_matches(
    registry: &ProcessWorkerRegistry,
    owner_pid: i32,
    region_key: usize,
    slot_count: u32,
) -> bool {
    registry.owner_pid == owner_pid
        && registry.region_key == region_key
        && registry.slot_count == slot_count
}

impl TransportRegion {
    /// # Safety
    /// `base` must point to an initialized writable shared-memory region with
    /// at least `len` bytes and alignment matching `layout.align`. The mapping
    /// must be fresh: if it already contains a `control_transport` header, use
    /// `reinit_in_place()` for a same-layout reset instead.
    ///
    /// The bytes must already be initialized because this function reads the
    /// existing header magic to reject accidental reinitialization.
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
        if unsafe { (*header_ptr).magic } == CONTROL_TRANSPORT_MAGIC {
            return Err(InitError::AlreadyInitialized);
        }
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
    /// region using the same `layout`. This is a same-layout logical reset
    /// path: it publishes a newer offline generation, clears reusable free
    /// slots in place, and retains old leased slots until exact old-incarnation
    /// release/finalize or later reap.
    pub unsafe fn reinit_in_place(
        base: NonNull<u8>,
        len: usize,
        layout: TransportRegionLayout,
    ) -> Result<Self, ReinitError> {
        let (existing, computed) =
            validate_attached_header(base, len).map_err(ReinitError::InvalidExistingRegion)?;
        if existing != layout {
            return Err(ReinitError::LayoutMismatch {
                existing,
                requested: layout,
            });
        }

        let region = build_handle(base, layout, computed);
        let next_generation = region.next_region_generation();
        region.reset_local_worker_registry_for_reinit();
        region.worker_pid_cell().store(0, Ordering::Release);
        region.store_region_meta(RegionMeta::new(next_generation, WORKER_STATE_REINITING));
        region.reconcile_slots_for_reinit()?;
        region.store_region_meta(RegionMeta::new(next_generation, WORKER_STATE_OFFLINE));
        Ok(region)
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
        self.load_region_meta().generation()
    }

    pub(super) fn next_lease_epoch_cell(&self) -> &AtomicU64 {
        unsafe { self.next_lease_epoch.as_ref() }
    }

    pub(super) fn freelist_epoch_cell(&self) -> &AtomicU64 {
        unsafe { self.freelist_epoch.as_ref() }
    }

    pub(super) fn region_meta_cell(&self) -> &AtomicU64 {
        unsafe { self.region_meta.as_ref() }
    }

    pub(super) fn load_region_meta(&self) -> RegionMeta {
        RegionMeta::from_raw(self.region_meta_cell().load(Ordering::Acquire))
    }

    pub(super) fn store_region_meta(&self, region_meta: RegionMeta) {
        self.region_meta_cell()
            .store(region_meta.raw(), Ordering::Release);
    }

    pub(super) fn worker_pid_cell(&self) -> &AtomicI32 {
        unsafe { self.worker_pid.as_ref() }
    }

    pub(super) fn allocate_lease_epoch(&self) -> u64 {
        self.next_lease_epoch_cell()
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= SLOT_META_MAX_LEASE_EPOCH)
            })
            .expect("control_transport lease epoch overflow")
    }

    pub(super) fn load_freelist_epoch(&self) -> u64 {
        self.freelist_epoch_cell().load(Ordering::Acquire)
    }

    fn rotate_freelist_epoch_for_reinit(&self) -> u64 {
        self.freelist_epoch_cell()
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(1)
                    .filter(|next| *next <= SLOT_META_MAX_LEASE_EPOCH)
            })
            .expect("control_transport freelist epoch overflow")
            + 1
    }

    pub(super) fn next_region_generation(&self) -> u64 {
        self.region_generation()
            .checked_add(1)
            .filter(|next| *next <= REGION_META_MAX_GENERATION)
            .expect("control_transport region generation overflow")
    }

    pub(super) fn attach_worker_registry(&self) -> Result<(), WorkerAttachError> {
        let _ = install_or_get_worker_owner_registry(
            Self::current_process_pid(),
            self.region_key(),
            self.slot_count,
        )?;
        Ok(())
    }

    fn reset_local_worker_registry_for_reinit(&self) {
        reset_worker_owner_registry(
            Self::current_process_pid(),
            self.region_key(),
            self.slot_count,
        );
    }

    fn reconcile_slots_for_reinit(&self) -> Result<(), ReinitError> {
        self.quiesce_slots_for_reinit()?;
        let current_epoch = self.rotate_freelist_epoch_for_reinit();
        self.reset_freelist_empty_for_reinit();
        self.rebuild_freelist_for_reinit(current_epoch)?;
        Ok(())
    }

    fn reset_freelist_empty_for_reinit(&self) {
        self.freelist().reset_empty();
    }

    fn quiesce_slots_for_reinit(&self) -> Result<(), ReinitError> {
        loop {
            let mut saw_live_transitional = false;
            let mut made_progress = false;

            for slot_id in 0..self.slot_count {
                let slot = unsafe { self.slot_view_unchecked(slot_id) };
                let slot_meta = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));

                if slot_meta.is_leased() {
                    let slot_generation = slot.slot_generation.load(Ordering::Acquire);
                    if slot_generation == 0 || slot_meta.lease_epoch() == 0 {
                        if self.adopt_stable_free_slot_for_reinit(slot_id, slot, slot_meta) {
                            made_progress = true;
                        }
                        continue;
                    }

                    if slot_meta.is_ownerless() {
                        let incarnation =
                            LeaseIncarnation::new(slot_generation, slot_meta.lease_epoch());
                        if self.force_finalize_slot_for_reinit(
                            slot_id,
                            incarnation,
                            slot,
                            slot_meta,
                        ) {
                            made_progress = true;
                        }
                    }
                    continue;
                }

                if slot_meta.is_free_popped() {
                    match self.reinit_actor_alive(slot.backend_pid.load(Ordering::Acquire)) {
                        Ok(true) => saw_live_transitional = true,
                        Ok(false) => {
                            if self.adopt_popped_slot_for_reinit(slot_id, slot, slot_meta) {
                                made_progress = true;
                            }
                        }
                        Err(err) => {
                            return Err(ReinitError::BackendProbeFailed {
                                slot_id,
                                error_kind: err.kind(),
                                raw_os_error: err.raw_os_error(),
                            });
                        }
                    }
                    continue;
                }

                if slot_meta.is_acquire_reserved() {
                    match self.reinit_actor_alive(slot.backend_pid.load(Ordering::Acquire)) {
                        Ok(true) => saw_live_transitional = true,
                        Ok(false) => {
                            if self.adopt_reserved_slot_for_reinit(slot_id, slot, slot_meta) {
                                made_progress = true;
                            }
                        }
                        Err(err) => {
                            return Err(ReinitError::BackendProbeFailed {
                                slot_id,
                                error_kind: err.kind(),
                                raw_os_error: err.raw_os_error(),
                            });
                        }
                    }
                    continue;
                }

                if slot_meta.is_free_push_claimed() {
                    match self.reinit_actor_alive(slot.backend_pid.load(Ordering::Acquire)) {
                        Ok(true) => saw_live_transitional = true,
                        Ok(false) => {
                            if self
                                .adopt_free_push_claimed_slot_for_reinit(slot_id, slot, slot_meta)
                            {
                                made_progress = true;
                            }
                        }
                        Err(err) => {
                            return Err(ReinitError::BackendProbeFailed {
                                slot_id,
                                error_kind: err.kind(),
                                raw_os_error: err.raw_os_error(),
                            });
                        }
                    }
                }
            }

            if !saw_live_transitional {
                return Ok(());
            }
            if !made_progress {
                std::thread::yield_now();
            }
        }
    }

    fn rebuild_freelist_for_reinit(&self, current_epoch: u64) -> Result<(), ReinitError> {
        loop {
            let mut saw_live_transitional = false;
            let mut saw_unpublished_free = false;
            let mut made_progress = false;

            for slot_id in 0..self.slot_count {
                let slot = unsafe { self.slot_view_unchecked(slot_id) };
                let slot_meta = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));

                if slot_meta.is_leased() {
                    let slot_generation = slot.slot_generation.load(Ordering::Acquire);
                    if slot_generation == 0 || slot_meta.lease_epoch() == 0 {
                        if self.adopt_stable_free_slot_for_reinit(slot_id, slot, slot_meta) {
                            made_progress = true;
                            saw_unpublished_free = true;
                        }
                        continue;
                    }

                    if slot_meta.is_ownerless() {
                        let incarnation =
                            LeaseIncarnation::new(slot_generation, slot_meta.lease_epoch());
                        if self.force_finalize_slot_for_reinit(
                            slot_id,
                            incarnation,
                            slot,
                            slot_meta,
                        ) {
                            made_progress = true;
                            saw_unpublished_free = true;
                        }
                    }
                    continue;
                }

                if slot_meta.is_free_popped() {
                    match self.reinit_actor_alive(slot.backend_pid.load(Ordering::Acquire)) {
                        Ok(true) => saw_live_transitional = true,
                        Ok(false) => {
                            if self.adopt_popped_slot_for_reinit(slot_id, slot, slot_meta) {
                                made_progress = true;
                                saw_unpublished_free = true;
                            }
                        }
                        Err(err) => {
                            return Err(ReinitError::BackendProbeFailed {
                                slot_id,
                                error_kind: err.kind(),
                                raw_os_error: err.raw_os_error(),
                            });
                        }
                    }
                    continue;
                }

                if slot_meta.is_acquire_reserved() {
                    match self.reinit_actor_alive(slot.backend_pid.load(Ordering::Acquire)) {
                        Ok(true) => saw_live_transitional = true,
                        Ok(false) => {
                            if self.adopt_reserved_slot_for_reinit(slot_id, slot, slot_meta) {
                                made_progress = true;
                                saw_unpublished_free = true;
                            }
                        }
                        Err(err) => {
                            return Err(ReinitError::BackendProbeFailed {
                                slot_id,
                                error_kind: err.kind(),
                                raw_os_error: err.raw_os_error(),
                            });
                        }
                    }
                    continue;
                }

                if slot_meta.is_free_push_claimed() {
                    match self.reinit_actor_alive(slot.backend_pid.load(Ordering::Acquire)) {
                        Ok(true) => saw_live_transitional = true,
                        Ok(false) => {
                            if self
                                .adopt_free_push_claimed_slot_for_reinit(slot_id, slot, slot_meta)
                            {
                                made_progress = true;
                                saw_unpublished_free = true;
                            }
                        }
                        Err(err) => {
                            return Err(ReinitError::BackendProbeFailed {
                                slot_id,
                                error_kind: err.kind(),
                                raw_os_error: err.raw_os_error(),
                            });
                        }
                    }
                    continue;
                }

                if slot_meta.is_current_epoch_published(current_epoch) {
                    continue;
                }

                if slot_meta.is_free_pushed() && slot_meta.lease_epoch() == current_epoch {
                    slot.backend_pid.store(0, Ordering::Release);
                    let _ = slot.slot_meta.compare_exchange(
                        slot_meta.raw(),
                        SlotMeta::free_published(current_epoch).raw(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    made_progress = true;
                    continue;
                }

                if slot_meta.is_republishable_after_reinit(current_epoch) {
                    saw_unpublished_free = true;
                    if self.republish_stable_free_slot_for_reinit(
                        slot_id,
                        slot,
                        slot_meta,
                        current_epoch,
                    ) {
                        made_progress = true;
                    }
                }
            }

            #[cfg(test)]
            if self.run_reinit_rebuild_pass_hook_for_tests() {
                made_progress = true;
                saw_unpublished_free = true;
            }

            if !saw_live_transitional && !saw_unpublished_free {
                return Ok(());
            }
            if !made_progress {
                std::thread::yield_now();
            }
        }
    }

    pub(super) fn force_finalize_slot_for_reinit(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
        slot: SlotView<'_>,
        expected_meta: SlotMeta,
    ) -> bool {
        if slot.slot_generation.load(Ordering::Acquire) != incarnation.generation {
            return false;
        }
        if !expected_meta.is_leased()
            || expected_meta.lease_epoch() != incarnation.lease_epoch
            || !expected_meta.is_ownerless()
        {
            return false;
        }
        if slot
            .slot_meta
            .compare_exchange(
                expected_meta.raw(),
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
        true
    }

    fn adopt_popped_slot_for_reinit(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        expected_meta: SlotMeta,
    ) -> bool {
        if !expected_meta.is_free_popped() {
            return false;
        }
        if slot
            .slot_meta
            .compare_exchange(
                expected_meta.raw(),
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
        true
    }

    fn adopt_reserved_slot_for_reinit(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        expected_meta: SlotMeta,
    ) -> bool {
        if !expected_meta.is_acquire_reserved() {
            return false;
        }
        if slot
            .slot_meta
            .compare_exchange(
                expected_meta.raw(),
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
        true
    }

    fn adopt_free_push_claimed_slot_for_reinit(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        expected_meta: SlotMeta,
    ) -> bool {
        if !expected_meta.is_free_push_claimed() {
            return false;
        }
        if slot
            .slot_meta
            .compare_exchange(
                expected_meta.raw(),
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
        true
    }

    fn adopt_stable_free_slot_for_reinit(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        current: SlotMeta,
    ) -> bool {
        if current.is_leased()
            || current.is_free_popped()
            || current.is_acquire_reserved()
            || current.is_free_push_claimed()
        {
            return false;
        }
        self.clear_slot(slot_id);
        slot.slot_generation.store(0, Ordering::Release);
        slot.slot_meta
            .store(SlotMeta::FREE_PENDING.raw(), Ordering::Release);
        true
    }

    fn republish_stable_free_slot_for_reinit(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        current: SlotMeta,
        current_epoch: u64,
    ) -> bool {
        if current.is_current_epoch_published(current_epoch) {
            return false;
        }

        if !current.is_free_pending() {
            if !current.is_republishable_after_reinit(current_epoch) {
                return false;
            }
            if slot
                .slot_meta
                .compare_exchange(
                    current.raw(),
                    SlotMeta::FREE_PENDING.raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return false;
            }
        }

        self.clear_slot(slot_id);
        slot.slot_generation.store(0, Ordering::Release);

        loop {
            let pending = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if pending.is_current_epoch_published(current_epoch) {
                slot.backend_pid.store(0, Ordering::Release);
                return true;
            }
            if pending.is_free_pushed() && pending.lease_epoch() == current_epoch {
                slot.backend_pid.store(0, Ordering::Release);
                let _ = slot.slot_meta.compare_exchange(
                    pending.raw(),
                    SlotMeta::free_published(current_epoch).raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return true;
            }
            if !pending.is_free_pending() {
                return false;
            }

            slot.backend_pid
                .store(Self::current_process_pid(), Ordering::Release);
            if slot
                .slot_meta
                .compare_exchange(
                    pending.raw(),
                    SlotMeta::free_push_claimed(current_epoch).raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                slot.backend_pid.store(0, Ordering::Release);
                continue;
            }
            if slot
                .slot_meta
                .compare_exchange(
                    SlotMeta::free_push_claimed(current_epoch).raw(),
                    SlotMeta::free_pushed(current_epoch).raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                return false;
            }
            self.release_slot(slot_id);
            slot.backend_pid.store(0, Ordering::Release);
            let _ = slot.slot_meta.compare_exchange(
                SlotMeta::free_pushed(current_epoch).raw(),
                SlotMeta::free_published(current_epoch).raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
            return true;
        }
    }

    fn reinit_actor_alive(&self, pid: i32) -> std::io::Result<bool> {
        if pid == 0 {
            return Ok(false);
        }
        probe_pid_alive(pid)
    }

    pub(super) fn pop_freelist_token_for_acquire(&self) -> Result<(u32, u64), AcquireError> {
        loop {
            let expected_epoch = self.load_freelist_epoch();
            let slot_id = self.acquire_slot()?;
            let slot = unsafe { self.slot_view_unchecked(slot_id) };
            if self.claim_freelist_token_after_pop(slot_id, slot, expected_epoch) {
                return Ok((slot_id, expected_epoch));
            }
        }
    }

    fn claim_freelist_token_after_pop(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        expected_epoch: u64,
    ) -> bool {
        loop {
            let current = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if (current.is_free_published() || current.is_free_pushed())
                && current.lease_epoch() == expected_epoch
            {
                slot.backend_pid
                    .store(Self::current_process_pid(), Ordering::Release);
                match slot.slot_meta.compare_exchange(
                    current.raw(),
                    SlotMeta::free_popped(expected_epoch).raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        #[cfg(test)]
                        self.run_backend_acquire_popped_hook_for_tests();
                        return true;
                    }
                    Err(_) => {
                        slot.backend_pid.store(0, Ordering::Release);
                        continue;
                    }
                }
            }

            if current.is_free_pending() {
                let _ = self.publish_free_slot(slot_id, slot);
            }
            return false;
        }
    }

    pub(super) fn reserve_popped_slot_for_acquire(
        &self,
        slot_id: u32,
        slot: SlotView<'_>,
        expected_epoch: u64,
    ) -> bool {
        loop {
            let current = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if current.is_free_popped() && current.lease_epoch() == expected_epoch {
                match slot.slot_meta.compare_exchange(
                    current.raw(),
                    SlotMeta::ACQUIRE_RESERVED.raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(_) => continue,
                }
            }

            if current.is_free_pending() {
                let _ = self.publish_free_slot(slot_id, slot);
            }
            return false;
        }
    }

    pub(super) fn abort_reserved_slot_acquire(&self, slot_id: u32, slot: SlotView<'_>) -> bool {
        loop {
            let current = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if current.is_free_published()
                || current.is_free_popped()
                || current.is_free_pending()
                || current.is_free_push_claimed()
                || current.is_free_pushed()
            {
                return true;
            }
            if !(current.is_acquire_reserved() || current.is_free_popped()) {
                return false;
            }
            match slot.slot_meta.compare_exchange(
                current.raw(),
                SlotMeta::FREE_PENDING.raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.clear_slot(slot_id);
                    slot.slot_generation.store(0, Ordering::Release);
                    let _ = self.publish_free_slot(slot_id, slot);
                    return true;
                }
                Err(_) => continue,
            }
        }
    }

    pub(super) fn publish_free_slot(&self, slot_id: u32, slot: SlotView<'_>) -> bool {
        loop {
            let current = SlotMeta::from_raw(slot.slot_meta.load(Ordering::Acquire));
            if current.is_free_published() {
                slot.backend_pid.store(0, Ordering::Release);
                return true;
            }
            if current.is_free_pushed() {
                slot.backend_pid.store(0, Ordering::Release);
                let _ = slot.slot_meta.compare_exchange(
                    current.raw(),
                    SlotMeta::free_published(current.lease_epoch()).raw(),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                return true;
            }
            if current.is_free_push_claimed() {
                return false;
            }
            if !current.is_free_pending() {
                return false;
            }
            if self.load_region_meta().is_reiniting() {
                return false;
            }
            let current_epoch = self.load_freelist_epoch();
            slot.backend_pid
                .store(Self::current_process_pid(), Ordering::Release);

            match slot.slot_meta.compare_exchange(
                current.raw(),
                SlotMeta::free_push_claimed(current_epoch).raw(),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.release_slot(slot_id);

                    #[cfg(test)]
                    self.run_free_slot_pushed_hook_for_tests();

                    if slot
                        .slot_meta
                        .compare_exchange(
                            SlotMeta::free_push_claimed(current_epoch).raw(),
                            SlotMeta::free_pushed(current_epoch).raw(),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        return false;
                    }

                    slot.backend_pid.store(0, Ordering::Release);
                    let _ = slot.slot_meta.compare_exchange(
                        SlotMeta::free_pushed(current_epoch).raw(),
                        SlotMeta::free_published(current_epoch).raw(),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    return true;
                }
                Err(_) => {
                    slot.backend_pid.store(0, Ordering::Release);
                    continue;
                }
            }
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

    pub(super) fn clear_slot(&self, slot_id: u32) {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        slot.backend_to_worker.clear();
        slot.worker_to_backend.clear();
        slot.to_worker_ready.store(false, Ordering::Release);
        slot.to_backend_ready.store(false, Ordering::Release);
        slot.backend_pid.store(0, Ordering::Release);
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
        let slot_generation =
            &*(slot_base.add(self.computed.slot_layout.slot_generation_offset) as *const AtomicU64);
        let slot_meta =
            &*(slot_base.add(self.computed.slot_layout.slot_meta_offset) as *const AtomicU64);
        let backend_pid =
            &*(slot_base.add(self.computed.slot_layout.backend_pid_offset) as *const AtomicI32);

        SlotView {
            backend_to_worker,
            worker_to_backend,
            to_worker_ready,
            to_backend_ready,
            slot_generation,
            slot_meta,
            backend_pid,
            worker_pid: self.worker_pid_cell(),
        }
    }

    pub(super) fn current_process_pid() -> i32 {
        #[cfg(test)]
        {
            let override_pid = CURRENT_PROCESS_PID_OVERRIDE.load(Ordering::Acquire);
            if override_pid != 0 {
                return override_pid;
            }
        }

        #[cfg(unix)]
        {
            unsafe { libc::getpid() as i32 }
        }

        #[cfg(not(unix))]
        {
            0
        }
    }

    pub(super) fn region_key(&self) -> usize {
        // The mapped shared-memory base address is a sufficient process-local
        // identity for the attached region. It is not a durable cross-process
        // identifier and is only meaningful together with the current PID.
        self.base.as_ptr() as usize
    }

    fn worker_owner_registry_if_attached(&self) -> Option<&'static ProcessWorkerRegistry> {
        let registry = load_worker_owner_registry()?;
        if !registry_matches(
            registry,
            Self::current_process_pid(),
            self.region_key(),
            self.slot_count,
        ) {
            return None;
        }
        Some(registry)
    }

    fn worker_owner_registry(&self) -> &'static ProcessWorkerRegistry {
        self.worker_owner_registry_if_attached()
            .expect("worker owner registry must already be attached for this region")
    }

    pub(super) fn ensure_no_live_local_worker_slots(&self) -> Result<(), WorkerLifecycleError> {
        let live_slots = self.local_worker_owner_count();
        if live_slots != 0 {
            return Err(WorkerLifecycleError::HandlesAlive { live_slots });
        }
        Ok(())
    }

    fn local_worker_owner_count(&self) -> usize {
        let Some(registry) = self.worker_owner_registry_if_attached() else {
            return 0;
        };
        registry
            .entries
            .iter()
            .filter(|entry| entry.load(Ordering::Acquire) != 0)
            .count()
    }

    pub(super) fn insert_local_worker_owner(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
    ) -> bool {
        let entry = self.worker_owner_registry().entry(slot_id);
        entry
            .compare_exchange(
                0,
                pack_local_owner(incarnation),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(super) fn remove_local_worker_owner(
        &self,
        slot_id: u32,
        incarnation: LeaseIncarnation,
    ) -> bool {
        let Some(registry) = self.worker_owner_registry_if_attached() else {
            return false;
        };
        let entry = registry.entry(slot_id);
        entry
            .compare_exchange(
                pack_local_owner(incarnation),
                0,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(super) fn take_local_worker_owners(&self) -> Vec<(u32, LeaseIncarnation)> {
        let Some(registry) = self.worker_owner_registry_if_attached() else {
            return Vec::new();
        };
        let mut owned = Vec::new();
        for (slot_id, entry) in registry.entries.iter().enumerate() {
            let incarnation = unpack_local_owner(entry.swap(0, Ordering::AcqRel));
            if incarnation.is_free() {
                continue;
            }
            owned.push((slot_id as u32, incarnation));
        }
        owned
    }

    #[cfg(test)]
    pub(super) fn forget_local_worker_owners_for_tests(&self) {
        let Some(registry) = self.worker_owner_registry_if_attached() else {
            return;
        };
        for entry in registry.entries.iter() {
            entry.store(0, Ordering::Release);
        }
    }

    #[cfg(test)]
    pub(crate) fn clear_all_worker_owner_registry_for_tests() {
        let ptr = WORKER_OWNER_REGISTRY.swap(ptr::null_mut(), Ordering::AcqRel);
        if ptr.is_null() {
            return;
        }
        unsafe {
            drop(Box::from_raw(ptr));
        }
    }

    #[cfg(test)]
    pub(crate) fn set_current_process_pid_for_tests(pid: i32) {
        CURRENT_PROCESS_PID_OVERRIDE.store(pid, Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn clear_current_process_pid_for_tests() {
        CURRENT_PROCESS_PID_OVERRIDE.store(0, Ordering::Release);
    }

    #[cfg(test)]
    pub(super) fn set_worker_state_for_tests(&self, state: u32) {
        let generation = self.region_generation();
        self.store_region_meta(RegionMeta::new(generation, state));
    }

    #[cfg(test)]
    pub(crate) fn is_reiniting_for_tests(&self) -> bool {
        self.load_region_meta().is_reiniting()
    }

    #[cfg(test)]
    pub(crate) fn acquire_slot_without_publish_for_tests(&self) -> u32 {
        let (slot_id, _) = self.pop_freelist_token_for_acquire().expect("slot reserve");
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        slot.backend_pid.store(0, Ordering::Release);
        slot_id
    }

    #[cfg(test)]
    pub(crate) fn force_slot_free_pending_without_publish_for_tests(&self, slot_id: u32) {
        let slot = unsafe { self.slot_view_unchecked(slot_id) };
        self.clear_slot(slot_id);
        slot.slot_generation.store(0, Ordering::Release);
        slot.slot_meta
            .store(SlotMeta::FREE_PENDING.raw(), Ordering::Release);
    }

    #[cfg(test)]
    pub(crate) fn set_backend_acquire_popped_hook_for_tests<F>(&self, hook: F)
    where
        F: FnOnce() + 'static,
    {
        let _ = self;
        BACKEND_ACQUIRE_POPPED_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(hook));
        });
    }

    #[cfg(test)]
    fn run_backend_acquire_popped_hook_for_tests(&self) {
        let _ = self;
        BACKEND_ACQUIRE_POPPED_HOOK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
            }
        });
    }

    #[cfg(test)]
    pub(crate) fn set_reinit_rebuild_pass_hook_for_tests<F>(&self, hook: F)
    where
        F: FnOnce() + 'static,
    {
        let _ = self;
        REINIT_REBUILD_PASS_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(hook));
        });
    }

    #[cfg(test)]
    fn run_reinit_rebuild_pass_hook_for_tests(&self) -> bool {
        let _ = self;
        REINIT_REBUILD_PASS_HOOK.with(|slot| {
            if let Some(hook) = slot.borrow_mut().take() {
                hook();
                true
            } else {
                false
            }
        })
    }

    #[cfg(test)]
    pub(crate) fn set_free_slot_pushed_hook_for_tests<F>(&self, hook: F)
    where
        F: FnOnce() + 'static,
    {
        let _ = self;
        FREE_SLOT_PUSHED_HOOK.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(hook));
        });
    }

    #[cfg(test)]
    fn run_free_slot_pushed_hook_for_tests(&self) {
        let _ = self;
        FREE_SLOT_PUSHED_HOOK.with(|slot| {
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

#[cfg(test)]
static CURRENT_PROCESS_PID_OVERRIDE: AtomicI32 = AtomicI32::new(0);
