mod loom_support;

use loom::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;
use loom_support::{
    assert_no_worker_on_nonmatching_incarnation, meta_epoch, meta_owner_mask, meta_state,
    pack_slot_meta, run_model, ValidateResult, FREE, LEASED, OWNER_ANY_WORKER, OWNER_BACKEND,
    OWNER_WORKER, OWNER_WORKER_PENDING,
};

struct ModelSlotMeta {
    slot_generation: AtomicU64,
    slot_meta: AtomicU64,
    backend_alive: AtomicBool,
}

impl ModelSlotMeta {
    fn new_leased(generation: u64, lease_epoch: u64) -> Self {
        Self {
            slot_generation: AtomicU64::new(generation),
            slot_meta: AtomicU64::new(pack_slot_meta(LEASED, lease_epoch, OWNER_BACKEND)),
            backend_alive: AtomicBool::new(true),
        }
    }

    fn claim_worker(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        let current_meta = self.slot_meta.load(Ordering::Acquire);
        if meta_state(current_meta) != LEASED {
            return ValidateResult::Released;
        }
        if self.slot_generation.load(Ordering::Acquire) != generation {
            return ValidateResult::Released;
        }
        if meta_epoch(current_meta) != lease_epoch {
            return ValidateResult::StaleEpoch;
        }
        if !self.backend_alive.load(Ordering::Acquire) {
            return ValidateResult::Released;
        }

        let owner_mask = meta_owner_mask(current_meta);
        if owner_mask & OWNER_BACKEND == 0 {
            return ValidateResult::Released;
        }
        if owner_mask & OWNER_ANY_WORKER != 0 {
            return ValidateResult::Busy;
        }

        let pending_mask = OWNER_BACKEND | OWNER_WORKER_PENDING;
        let expected_meta = pack_slot_meta(LEASED, lease_epoch, OWNER_BACKEND);
        let pending_meta = pack_slot_meta(LEASED, lease_epoch, pending_mask);
        if self
            .slot_meta
            .compare_exchange(
                expected_meta,
                pending_meta,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return self.classify_claim_failure(generation, lease_epoch);
        }

        let current_generation = self.slot_generation.load(Ordering::Acquire);
        let current_meta = self.slot_meta.load(Ordering::Acquire);
        let backend_alive = self.backend_alive.load(Ordering::Acquire);

        if current_generation != generation
            || meta_epoch(current_meta) != lease_epoch
            || meta_state(current_meta) != LEASED
            || meta_owner_mask(current_meta) != pending_mask
            || !backend_alive
        {
            self.rollback_worker_claim(generation, lease_epoch);
            return if meta_epoch(current_meta) != lease_epoch {
                ValidateResult::StaleEpoch
            } else {
                ValidateResult::Released
            };
        }

        let committed_meta = pack_slot_meta(LEASED, lease_epoch, OWNER_BACKEND | OWNER_WORKER);
        if self
            .slot_meta
            .compare_exchange(
                pending_meta,
                committed_meta,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            self.rollback_worker_claim(generation, lease_epoch);
            return self.classify_claim_failure(generation, lease_epoch);
        }

        ValidateResult::Ok
    }

    fn release_backend(&self, generation: u64, lease_epoch: u64) {
        let _ = self.clear_owner_bits_if_matching(generation, lease_epoch, OWNER_BACKEND);
    }

    fn mark_backend_dead(&self) {
        self.backend_alive.store(false, Ordering::Release);
    }

    fn reap_dead_backend_and_finalize(&self, generation: u64, lease_epoch: u64) -> bool {
        if self.slot_generation.load(Ordering::Acquire) != generation {
            return false;
        }
        if meta_epoch(self.slot_meta.load(Ordering::Acquire)) != lease_epoch {
            return false;
        }
        if self.backend_alive.load(Ordering::Acquire) {
            return false;
        }

        let remaining = self.clear_owner_bits_if_matching(generation, lease_epoch, OWNER_BACKEND);
        matches!(remaining, Some(0)) && self.finalize(generation, lease_epoch)
    }

    fn rollback_worker_claim(&self, generation: u64, lease_epoch: u64) {
        let remaining =
            self.clear_owner_bits_if_matching(generation, lease_epoch, OWNER_WORKER_PENDING);
        if remaining == Some(0) {
            let _ = self.finalize(generation, lease_epoch);
        }
    }

    fn clear_owner_bits_if_matching(
        &self,
        generation: u64,
        lease_epoch: u64,
        clear_bits: u32,
    ) -> Option<u32> {
        let mut current_raw = self.slot_meta.load(Ordering::Acquire);
        loop {
            if self.slot_generation.load(Ordering::Acquire) != generation {
                return None;
            }
            if meta_state(current_raw) != LEASED || meta_epoch(current_raw) != lease_epoch {
                return None;
            }

            let current_mask = meta_owner_mask(current_raw);
            let remaining_mask = current_mask & !clear_bits;
            if remaining_mask == current_mask {
                return Some(remaining_mask);
            }

            let next = pack_slot_meta(LEASED, lease_epoch, remaining_mask);
            match self.slot_meta.compare_exchange(
                current_raw,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(remaining_mask),
                Err(observed) => current_raw = observed,
            }
        }
    }

    fn finalize(&self, generation: u64, lease_epoch: u64) -> bool {
        if self.slot_generation.load(Ordering::Acquire) != generation {
            return false;
        }

        let expected = pack_slot_meta(LEASED, lease_epoch, 0);
        if self
            .slot_meta
            .compare_exchange(
                expected,
                pack_slot_meta(FREE, 0, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }

        self.slot_generation.store(0, Ordering::Release);
        true
    }

    fn acquire_backend(&self, generation: u64, lease_epoch: u64) -> bool {
        self.slot_generation.store(generation, Ordering::Release);
        if self
            .slot_meta
            .compare_exchange(
                pack_slot_meta(FREE, 0, 0),
                pack_slot_meta(LEASED, lease_epoch, OWNER_BACKEND),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return false;
        }

        self.backend_alive.store(true, Ordering::Release);
        true
    }

    fn classify_claim_failure(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        if self.slot_generation.load(Ordering::Acquire) != generation {
            return ValidateResult::Released;
        }

        let current_meta = self.slot_meta.load(Ordering::Acquire);
        if meta_state(current_meta) != LEASED {
            return ValidateResult::Released;
        }
        if meta_epoch(current_meta) != lease_epoch {
            return ValidateResult::StaleEpoch;
        }

        let owner_mask = meta_owner_mask(current_meta);
        if owner_mask & OWNER_BACKEND == 0 {
            return ValidateResult::Released;
        }
        if owner_mask & OWNER_ANY_WORKER != 0 {
            return ValidateResult::Busy;
        }

        ValidateResult::Released
    }
}

#[test]
fn claim_vs_backend_release_never_leaves_orphan_worker_owner() {
    run_model(|| {
        let slot = Arc::new(ModelSlotMeta::new_leased(5, 1));

        let claimant = slot.clone();
        let claimant = thread::spawn(move || claimant.claim_worker(5, 1));

        let releaser = slot.clone();
        let releaser = thread::spawn(move || {
            releaser.release_backend(5, 1);
            let _ = releaser.finalize(5, 1);
        });

        let claim_result = claimant.join().expect("claim thread should join cleanly");
        releaser.join().expect("release thread should join cleanly");

        let slot_generation = slot.slot_generation.load(Ordering::Acquire);
        let slot_meta = slot.slot_meta.load(Ordering::Acquire);

        if claim_result != ValidateResult::Ok {
            assert_no_worker_on_nonmatching_incarnation(slot_generation, slot_meta, 5, 1);
            if meta_state(slot_meta) == FREE {
                assert_eq!(meta_owner_mask(slot_meta) & OWNER_ANY_WORKER, 0);
            }
        }
    });
}

#[test]
fn claim_race_with_dead_backend_reap_cannot_touch_fresh_reuse() {
    run_model(|| {
        let slot = Arc::new(ModelSlotMeta::new_leased(9, 1));

        let claimant = slot.clone();
        let claimant = thread::spawn(move || claimant.claim_worker(9, 1));

        let reaper = slot.clone();
        let reaper = thread::spawn(move || {
            reaper.mark_backend_dead();
            let _ = reaper.reap_dead_backend_and_finalize(9, 1);
            thread::yield_now();
            if reaper.finalize(9, 1) {
                assert!(reaper.acquire_backend(9, 2));
            }
        });

        let claim_result = claimant.join().expect("claim thread should join cleanly");
        reaper.join().expect("reap thread should join cleanly");

        let slot_generation = slot.slot_generation.load(Ordering::Acquire);
        let slot_meta = slot.slot_meta.load(Ordering::Acquire);

        if meta_state(slot_meta) == LEASED && meta_epoch(slot_meta) == 2 {
            assert_eq!(
                meta_owner_mask(slot_meta) & OWNER_WORKER,
                0,
                "old claim leaked worker ownership into a fresh incarnation",
            );
            assert_eq!(
                meta_owner_mask(slot_meta) & OWNER_WORKER_PENDING,
                0,
                "pending worker reservation leaked into a fresh incarnation",
            );
        } else if claim_result != ValidateResult::Ok {
            assert_no_worker_on_nonmatching_incarnation(slot_generation, slot_meta, 9, 1);
        }
    });
}
