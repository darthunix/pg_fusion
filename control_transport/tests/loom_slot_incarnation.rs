mod loom_support;

use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;
use loom_support::{
    meta_epoch, meta_owner_mask, meta_state, pack_slot_meta, run_model, ValidateResult, FREE,
    LEASED, OWNER_ANY_WORKER, OWNER_BACKEND, OWNER_WORKER, OWNER_WORKER_PENDING,
};

struct ModelSlot {
    region_generation: AtomicU64,
    slot_generation: AtomicU64,
    slot_meta: AtomicU64,
    registry_generation: AtomicU64,
    registry_epoch: AtomicU64,
}

impl ModelSlot {
    fn new_leased(generation: u64, lease_epoch: u64) -> Self {
        Self {
            region_generation: AtomicU64::new(generation),
            slot_generation: AtomicU64::new(generation),
            slot_meta: AtomicU64::new(pack_slot_meta(LEASED, lease_epoch, OWNER_BACKEND)),
            registry_generation: AtomicU64::new(0),
            registry_epoch: AtomicU64::new(0),
        }
    }

    fn acquire_backend(&self, generation: u64, lease_epoch: u64) -> bool {
        let leased_meta = pack_slot_meta(LEASED, lease_epoch, OWNER_BACKEND);
        self.slot_generation.store(generation, Ordering::Release);
        self.slot_meta
            .compare_exchange(
                pack_slot_meta(FREE, 0, 0),
                leased_meta,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn claim_worker(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        let current_generation = self.region_generation.load(Ordering::Acquire);
        if current_generation != generation {
            return ValidateResult::StaleGeneration;
        }

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

        let current_generation = self.region_generation.load(Ordering::Acquire);
        let current_slot_generation = self.slot_generation.load(Ordering::Acquire);
        let current_meta = self.slot_meta.load(Ordering::Acquire);

        if current_generation != generation {
            self.rollback_worker_claim(generation, lease_epoch);
            return ValidateResult::StaleGeneration;
        }
        if current_slot_generation != generation
            || meta_epoch(current_meta) != lease_epoch
            || meta_state(current_meta) != LEASED
            || meta_owner_mask(current_meta) != pending_mask
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

        self.registry_generation
            .store(generation, Ordering::Release);
        self.registry_epoch.store(lease_epoch, Ordering::Release);
        ValidateResult::Ok
    }

    fn release_backend(&self, generation: u64, lease_epoch: u64) {
        self.clear_owner_bits_if_matching(generation, lease_epoch, OWNER_BACKEND);
    }

    fn release_worker(&self, generation: u64, lease_epoch: u64) {
        self.clear_owner_bits_if_matching(generation, lease_epoch, OWNER_WORKER);
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

    fn validate_backend_handle(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        let current_generation = self.region_generation.load(Ordering::Acquire);
        if current_generation != generation {
            return ValidateResult::StaleGeneration;
        }

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
        if meta_owner_mask(current_meta) & OWNER_BACKEND == 0 {
            return ValidateResult::Released;
        }
        ValidateResult::Ok
    }

    fn validate_worker_handle(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        let current_generation = self.region_generation.load(Ordering::Acquire);
        if current_generation != generation {
            return ValidateResult::StaleGeneration;
        }

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

        let owner_mask = meta_owner_mask(current_meta);
        if owner_mask & OWNER_BACKEND == 0 || owner_mask & OWNER_WORKER == 0 {
            return ValidateResult::Released;
        }
        ValidateResult::Ok
    }

    fn drop_worker_registry_handle(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        let registry_generation = self.registry_generation.load(Ordering::Acquire);
        if registry_generation != generation {
            return ValidateResult::StaleGeneration;
        }

        let registry_epoch = self.registry_epoch.load(Ordering::Acquire);
        if registry_epoch != lease_epoch {
            return ValidateResult::StaleEpoch;
        }

        self.registry_generation.store(0, Ordering::Release);
        self.registry_epoch.store(0, Ordering::Release);
        self.release_worker(generation, lease_epoch);
        ValidateResult::Ok
    }

    fn classify_claim_failure(&self, generation: u64, lease_epoch: u64) -> ValidateResult {
        let current_generation = self.region_generation.load(Ordering::Acquire);
        if current_generation != generation {
            return ValidateResult::StaleGeneration;
        }

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
fn same_generation_reacquire_rejects_old_backend_handle() {
    run_model(|| {
        let slot = Arc::new(ModelSlot::new_leased(7, 1));

        let validator = slot.clone();
        let validator = thread::spawn(move || validator.validate_backend_handle(7, 1));

        let reuser = slot.clone();
        let reuser = thread::spawn(move || {
            reuser.release_backend(7, 1);
            assert!(reuser.finalize(7, 1));
            assert!(reuser.acquire_backend(7, 2));
        });

        let observed = validator.join().expect("validator should join cleanly");
        reuser.join().expect("reuser should join cleanly");

        assert!(matches!(
            observed,
            ValidateResult::Ok | ValidateResult::Released | ValidateResult::StaleEpoch
        ));
        assert_eq!(meta_epoch(slot.slot_meta.load(Ordering::Acquire)), 2);
        assert_eq!(
            slot.validate_backend_handle(7, 1),
            ValidateResult::StaleEpoch
        );
        assert_eq!(slot.validate_backend_handle(7, 2), ValidateResult::Ok);
    });
}

#[test]
fn stale_worker_drop_cannot_clear_new_incarnation() {
    run_model(|| {
        let slot = Arc::new(ModelSlot::new_leased(11, 1));
        assert_eq!(slot.claim_worker(11, 1), ValidateResult::Ok);

        slot.release_backend(11, 1);
        slot.release_worker(11, 1);
        assert!(slot.finalize(11, 1));
        assert!(slot.acquire_backend(11, 2));
        assert_eq!(slot.claim_worker(11, 2), ValidateResult::Ok);

        let stale_drop = slot.clone();
        let stale_drop = thread::spawn(move || stale_drop.drop_worker_registry_handle(11, 1));

        let current_validate = slot.clone();
        let current_validate =
            thread::spawn(move || current_validate.validate_worker_handle(11, 2));

        assert_eq!(
            stale_drop.join().expect("stale drop should join cleanly"),
            ValidateResult::StaleEpoch
        );
        assert_eq!(
            current_validate
                .join()
                .expect("current validate should join cleanly"),
            ValidateResult::Ok
        );
        let current_meta = slot.slot_meta.load(Ordering::Acquire);
        assert_ne!(meta_epoch(current_meta), 1);
        assert_ne!(
            meta_owner_mask(current_meta) & OWNER_WORKER,
            0,
            "stale worker drop cleared the new incarnation",
        );
    });
}
