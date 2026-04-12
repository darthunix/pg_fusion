use super::{
    TransportRegion, TransportRegionLayout, CONTROL_TRANSPORT_MAGIC, CONTROL_TRANSPORT_VERSION,
    LEASE_STATE_FREE, WORKER_STATE_OFFLINE,
};
use crate::error::{AttachError, ConfigError};
use crate::ring::{framed_ring_layout, FramedRing, FramedRingLayout};
use lockfree::{treiber_stack_layout, treiber_stack_ptrs, StackLayout, TreiberStack};
use std::alloc::Layout;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64};

#[repr(C)]
pub(super) struct RegionHeader {
    pub(super) magic: u64,
    pub(super) version: u32,
    pub(super) slot_count: u32,
    pub(super) backend_to_worker_cap: u32,
    pub(super) worker_to_backend_cap: u32,
    pub(super) region_size: u64,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct SlotComputedLayout {
    pub(super) layout: Layout,
    pub(super) backend_to_worker_offset: usize,
    pub(super) worker_to_backend_offset: usize,
    pub(super) to_worker_ready_offset: usize,
    pub(super) to_backend_ready_offset: usize,
    pub(super) lease_state_offset: usize,
    pub(super) slot_generation_offset: usize,
    pub(super) owner_mask_offset: usize,
    pub(super) backend_pid_offset: usize,
    pub(super) backend_to_worker_layout: FramedRingLayout,
    pub(super) worker_to_backend_layout: FramedRingLayout,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ComputedLayout {
    pub(super) region: Layout,
    pub(super) region_generation_offset: usize,
    pub(super) worker_state_offset: usize,
    pub(super) worker_pid_offset: usize,
    pub(super) freelist_offset: usize,
    pub(super) slots_offset: usize,
    pub(super) slot_layout: SlotComputedLayout,
    pub(super) slot_stride: usize,
    pub(super) stack_layout: StackLayout,
}

pub(super) fn build_handle(
    base: NonNull<u8>,
    layout: TransportRegionLayout,
    computed: ComputedLayout,
) -> TransportRegion {
    let base_ptr = base.as_ptr();
    TransportRegion {
        base,
        region_generation: unsafe {
            NonNull::new_unchecked(
                base_ptr
                    .add(computed.region_generation_offset)
                    .cast::<AtomicU64>(),
            )
        },
        worker_state: unsafe {
            NonNull::new_unchecked(
                base_ptr
                    .add(computed.worker_state_offset)
                    .cast::<AtomicU32>(),
            )
        },
        worker_pid: unsafe {
            NonNull::new_unchecked(base_ptr.add(computed.worker_pid_offset).cast::<AtomicI32>())
        },
        slot_count: layout.slot_count,
        backend_to_worker_cap: layout.backend_to_worker_cap,
        worker_to_backend_cap: layout.worker_to_backend_cap,
        computed,
    }
}

pub(super) fn init_storage(base: *mut u8, layout: ComputedLayout, slot_count: u32) {
    unsafe {
        let freelist_base = base.add(layout.freelist_offset);
        let (freelist_header_ptr, freelist_next_ptr) =
            treiber_stack_ptrs(freelist_base, layout.stack_layout);
        let _ = TreiberStack::init_in_place(
            freelist_header_ptr,
            freelist_next_ptr,
            slot_count as usize,
        );

        for slot_id in 0..slot_count {
            let slot_base = base.add(layout.slots_offset + slot_id as usize * layout.slot_stride);
            init_slot(slot_base, layout.slot_layout);
        }
    }
}

fn init_slot(base: *mut u8, layout: SlotComputedLayout) {
    unsafe {
        FramedRing::init_empty_in_place(
            base.add(layout.backend_to_worker_offset),
            layout.backend_to_worker_layout,
        );
        FramedRing::init_empty_in_place(
            base.add(layout.worker_to_backend_offset),
            layout.worker_to_backend_layout,
        );
        std::ptr::write(
            base.add(layout.to_worker_ready_offset).cast::<AtomicBool>(),
            AtomicBool::new(false),
        );
        std::ptr::write(
            base.add(layout.to_backend_ready_offset)
                .cast::<AtomicBool>(),
            AtomicBool::new(false),
        );
        std::ptr::write(
            base.add(layout.lease_state_offset).cast::<AtomicU32>(),
            AtomicU32::new(LEASE_STATE_FREE),
        );
        std::ptr::write(
            base.add(layout.slot_generation_offset).cast::<AtomicU64>(),
            AtomicU64::new(0),
        );
        std::ptr::write(
            base.add(layout.owner_mask_offset).cast::<AtomicU32>(),
            AtomicU32::new(0),
        );
        std::ptr::write(
            base.add(layout.backend_pid_offset).cast::<AtomicI32>(),
            AtomicI32::new(0),
        );
    }
}

pub(super) fn compute_layout(
    slot_count: u32,
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
) -> Result<ComputedLayout, ConfigError> {
    let slot_layout = compute_slot_layout(backend_to_worker_cap, worker_to_backend_cap)?;
    let slot_stride = slot_layout.layout.size();
    let slots_bytes = slot_stride
        .checked_mul(slot_count as usize)
        .ok_or(ConfigError::LayoutOverflow)?;
    let slots = Layout::from_size_align(slots_bytes, slot_layout.layout.align())
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let stack_layout =
        treiber_stack_layout(slot_count as usize).map_err(|_| ConfigError::LayoutOverflow)?;

    let header = Layout::new::<RegionHeader>();
    let region_generation = Layout::new::<AtomicU64>();
    let worker_state = Layout::new::<AtomicU32>();
    let worker_pid = Layout::new::<AtomicI32>();

    let (hg, region_generation_offset) = header
        .extend(region_generation)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (hgs, worker_state_offset) = hg
        .extend(worker_state)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (hgsp, worker_pid_offset) = hgs
        .extend(worker_pid)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (hgspf, freelist_offset) = hgsp
        .extend(stack_layout.layout)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (region, slots_offset) = hgspf
        .extend(slots)
        .map_err(|_| ConfigError::LayoutOverflow)?;

    Ok(ComputedLayout {
        region: region.pad_to_align(),
        region_generation_offset,
        worker_state_offset,
        worker_pid_offset,
        freelist_offset,
        slots_offset,
        slot_layout,
        slot_stride,
        stack_layout,
    })
}

fn compute_slot_layout(
    backend_to_worker_cap: usize,
    worker_to_backend_cap: usize,
) -> Result<SlotComputedLayout, ConfigError> {
    let backend_to_worker_layout = framed_ring_layout(backend_to_worker_cap)?;
    let worker_to_backend_layout = framed_ring_layout(worker_to_backend_cap)?;
    let ready = Layout::new::<AtomicBool>();
    let lease_state = Layout::new::<AtomicU32>();
    let slot_generation = Layout::new::<AtomicU64>();
    let owner_mask = Layout::new::<AtomicU32>();
    let backend_pid = Layout::new::<AtomicI32>();

    let (rings, worker_to_backend_offset) = backend_to_worker_layout
        .layout
        .extend(worker_to_backend_layout.layout)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (rings_ready1, to_worker_ready_offset) = rings
        .extend(ready)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (rings_ready2, to_backend_ready_offset) = rings_ready1
        .extend(ready)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (with_state, lease_state_offset) = rings_ready2
        .extend(lease_state)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (with_generation, slot_generation_offset) = with_state
        .extend(slot_generation)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (with_owner, owner_mask_offset) = with_generation
        .extend(owner_mask)
        .map_err(|_| ConfigError::LayoutOverflow)?;
    let (layout, backend_pid_offset) = with_owner
        .extend(backend_pid)
        .map_err(|_| ConfigError::LayoutOverflow)?;

    Ok(SlotComputedLayout {
        layout: layout.pad_to_align(),
        backend_to_worker_offset: 0,
        worker_to_backend_offset,
        to_worker_ready_offset,
        to_backend_ready_offset,
        lease_state_offset,
        slot_generation_offset,
        owner_mask_offset,
        backend_pid_offset,
        backend_to_worker_layout,
        worker_to_backend_layout,
    })
}

pub(super) fn validate_region(
    base: NonNull<u8>,
    len: usize,
    align: usize,
    expected: usize,
) -> Result<(), (usize, usize, bool)> {
    let actual = base.as_ptr() as usize;
    if actual % align != 0 {
        return Err((align, actual, false));
    }
    if len < expected {
        return Err((expected, len, true));
    }
    Ok(())
}

pub(super) fn validate_attached_header(
    base: NonNull<u8>,
    len: usize,
) -> Result<(TransportRegionLayout, ComputedLayout), AttachError> {
    validate_region(
        base,
        len,
        std::mem::align_of::<RegionHeader>(),
        std::mem::size_of::<RegionHeader>(),
    )
    .map_err(|(expected, actual, aligned)| {
        if aligned {
            AttachError::RegionTooSmall { expected, actual }
        } else {
            AttachError::BadAlignment { expected, actual }
        }
    })?;

    let header = unsafe { &*base.as_ptr().cast::<RegionHeader>() };
    if header.magic != CONTROL_TRANSPORT_MAGIC {
        return Err(AttachError::BadMagic {
            expected: CONTROL_TRANSPORT_MAGIC,
            actual: header.magic,
        });
    }
    if header.version != CONTROL_TRANSPORT_VERSION {
        return Err(AttachError::UnsupportedVersion {
            expected: CONTROL_TRANSPORT_VERSION,
            actual: header.version,
        });
    }

    let layout = TransportRegionLayout::new(
        header.slot_count,
        header.backend_to_worker_cap as usize,
        header.worker_to_backend_cap as usize,
    )
    .map_err(AttachError::InvalidConfig)?;
    let computed = compute_layout(
        layout.slot_count,
        layout.backend_to_worker_cap,
        layout.worker_to_backend_cap,
    )
    .map_err(AttachError::InvalidConfig)?;

    validate_region(base, len, computed.region.align(), computed.region.size()).map_err(
        |(expected, actual, aligned)| {
            if aligned {
                AttachError::LayoutMismatch { expected, actual }
            } else {
                AttachError::BadAlignment { expected, actual }
            }
        },
    )?;
    if header.region_size as usize != computed.region.size() {
        return Err(AttachError::LayoutMismatch {
            expected: header.region_size as usize,
            actual: computed.region.size(),
        });
    }

    Ok((layout, computed))
}

pub(super) fn init_global_cells(base: *mut u8, computed: ComputedLayout) {
    unsafe {
        std::ptr::write(
            base.add(computed.region_generation_offset)
                .cast::<AtomicU64>(),
            AtomicU64::new(0),
        );
        std::ptr::write(
            base.add(computed.worker_state_offset).cast::<AtomicU32>(),
            AtomicU32::new(WORKER_STATE_OFFLINE),
        );
        std::ptr::write(
            base.add(computed.worker_pid_offset).cast::<AtomicI32>(),
            AtomicI32::new(0),
        );
    }
}
