#![allow(dead_code)]

use loom::model::Builder;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ValidateResult {
    Ok,
    Released,
    StaleGeneration,
    StaleEpoch,
    Busy,
}

pub(crate) const FREE_PUBLISHED: u32 = 0;
pub(crate) const FREE: u32 = FREE_PUBLISHED;
pub(crate) const ACQUIRE_RESERVED: u32 = 1;
pub(crate) const LEASED: u32 = 2;
pub(crate) const FREE_PENDING: u32 = 3;
pub(crate) const FREE_PUSH_CLAIMED: u32 = 4;
pub(crate) const FREE_PUSHED: u32 = 5;
pub(crate) const FREE_POPPED: u32 = 6;
pub(crate) const WORKER_STATE_OFFLINE: u32 = 0;
pub(crate) const WORKER_STATE_RESTARTING: u32 = 1;
pub(crate) const WORKER_STATE_ONLINE: u32 = 2;
pub(crate) const WORKER_STATE_REINITING: u32 = 3;
pub(crate) const OWNER_BACKEND: u32 = 1;
pub(crate) const OWNER_WORKER: u32 = 2;
pub(crate) const OWNER_WORKER_PENDING: u32 = 4;
pub(crate) const OWNER_ANY_WORKER: u32 = OWNER_WORKER | OWNER_WORKER_PENDING;
pub(crate) const SLOT_META_OWNER_BITS: u32 = 3;
pub(crate) const SLOT_META_STATE_BITS: u32 = 3;
pub(crate) const SLOT_META_STATE_SHIFT: u32 = SLOT_META_OWNER_BITS;
pub(crate) const SLOT_META_EPOCH_SHIFT: u32 = SLOT_META_OWNER_BITS + SLOT_META_STATE_BITS;
pub(crate) const SLOT_META_OWNER_MASK: u64 = (1u64 << SLOT_META_OWNER_BITS) - 1;
pub(crate) const SLOT_META_STATE_MASK: u64 =
    ((1u64 << SLOT_META_STATE_BITS) - 1) << SLOT_META_STATE_SHIFT;
pub(crate) const REGION_META_STATE_BITS: u32 = 2;
pub(crate) const REGION_META_GENERATION_SHIFT: u32 = REGION_META_STATE_BITS;
pub(crate) const REGION_META_STATE_MASK: u64 = (1u64 << REGION_META_STATE_BITS) - 1;

pub(crate) fn pack_slot_meta(lease_state: u32, lease_epoch: u64, owner_mask: u32) -> u64 {
    (lease_epoch << SLOT_META_EPOCH_SHIFT)
        | ((lease_state as u64) << SLOT_META_STATE_SHIFT)
        | owner_mask as u64
}

pub(crate) fn meta_state(slot_meta: u64) -> u32 {
    ((slot_meta & SLOT_META_STATE_MASK) >> SLOT_META_STATE_SHIFT) as u32
}

pub(crate) fn meta_epoch(slot_meta: u64) -> u64 {
    slot_meta >> SLOT_META_EPOCH_SHIFT
}

pub(crate) fn meta_owner_mask(slot_meta: u64) -> u32 {
    (slot_meta & SLOT_META_OWNER_MASK) as u32
}

pub(crate) fn pack_region_meta(generation: u64, worker_state: u32) -> u64 {
    (generation << REGION_META_GENERATION_SHIFT) | worker_state as u64
}

pub(crate) fn region_generation(region_meta: u64) -> u64 {
    region_meta >> REGION_META_GENERATION_SHIFT
}

pub(crate) fn region_worker_state(region_meta: u64) -> u32 {
    (region_meta & REGION_META_STATE_MASK) as u32
}

pub(crate) fn region_is_online_generation(region_meta: u64, generation: u64) -> bool {
    generation != 0
        && region_generation(region_meta) == generation
        && region_worker_state(region_meta) == WORKER_STATE_ONLINE
}

pub(crate) fn incarnation_matches(
    slot_generation: u64,
    slot_meta: u64,
    expected_generation: u64,
    expected_epoch: u64,
) -> bool {
    meta_state(slot_meta) == LEASED
        && slot_generation == expected_generation
        && meta_epoch(slot_meta) == expected_epoch
}

pub(crate) fn assert_no_worker_on_nonmatching_incarnation(
    slot_generation: u64,
    slot_meta: u64,
    expected_generation: u64,
    expected_epoch: u64,
) {
    if !incarnation_matches(
        slot_generation,
        slot_meta,
        expected_generation,
        expected_epoch,
    ) {
        assert_eq!(
            meta_owner_mask(slot_meta) & OWNER_WORKER,
            0,
            "stale worker ownership leaked into a different incarnation",
        );
    }
}

pub(crate) fn run_model<F>(f: F)
where
    F: Fn() + Sync + Send + 'static,
{
    let mut builder = Builder::new();
    if builder.preemption_bound.is_none() {
        builder.preemption_bound = Some(2);
    }
    builder.check(f);
}
