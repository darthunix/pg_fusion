use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

const FREE: u64 = 0;
const BUILDING: u64 = 1;
const READY: u64 = 2;
const DISABLED: u64 = 3;

const POOL_FREE: u64 = 0;
const POOL_INITIALIZING: u64 = 1;
const POOL_ALLOCATED: u64 = 2;
const POOL_RETIRING: u64 = 3;
const POOL_REF_BITS: u32 = 16;
const POOL_REF_SHIFT: u32 = 2;
const POOL_EPOCH_SHIFT: u32 = POOL_REF_SHIFT + POOL_REF_BITS;
const POOL_REF_MASK: u64 = (1u64 << POOL_REF_BITS) - 1;

fn pack(generation: u64, state: u64) -> u64 {
    (generation << 2) | state
}

fn generation(word: u64) -> u64 {
    word >> 2
}

fn state(word: u64) -> u64 {
    word & 3
}

fn pack_pool(state: u64, refs: u64, epoch: u64) -> u64 {
    state | (refs << POOL_REF_SHIFT) | (epoch << POOL_EPOCH_SHIFT)
}

fn pool_state(word: u64) -> u64 {
    word & 3
}

fn pool_refs(word: u64) -> u64 {
    (word >> POOL_REF_SHIFT) & POOL_REF_MASK
}

fn pool_epoch(word: u64) -> u64 {
    word >> POOL_EPOCH_SHIFT
}

struct ModelSlot {
    lifecycle: AtomicU64,
    bitset: AtomicU64,
}

impl ModelSlot {
    fn new() -> Self {
        Self {
            lifecycle: AtomicU64::new(pack(0, FREE)),
            bitset: AtomicU64::new(0),
        }
    }

    fn acquire_builder(&self) -> Option<u64> {
        loop {
            let current = self.lifecycle.load(Ordering::Acquire);
            match state(current) {
                FREE | DISABLED => {}
                BUILDING | READY => return None,
                _ => unreachable!(),
            }
            let next_generation = generation(current) + 1;
            if self
                .lifecycle
                .compare_exchange(
                    current,
                    pack(next_generation, BUILDING),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.bitset.store(0, Ordering::Relaxed);
                return Some(next_generation);
            }
        }
    }

    fn insert(&self, generation: u64) {
        if self.lifecycle.load(Ordering::Acquire) == pack(generation, BUILDING) {
            self.bitset.fetch_or(1, Ordering::Relaxed);
        }
    }

    fn publish(&self, generation: u64) -> bool {
        self.lifecycle
            .compare_exchange(
                pack(generation, BUILDING),
                pack(generation, READY),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn disable(&self, generation: u64) -> bool {
        self.lifecycle
            .compare_exchange(
                pack(generation, BUILDING),
                pack(generation, DISABLED),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn reject_inserted_key(&self, expected_generation: u64) -> bool {
        let current = self.lifecycle.load(Ordering::Acquire);
        state(current) == READY
            && generation(current) == expected_generation
            && (self.bitset.load(Ordering::Relaxed) & 1) == 0
    }
}

struct ModelPoolSlot {
    meta: AtomicU64,
    exec_id: AtomicU64,
}

impl ModelPoolSlot {
    fn new() -> Self {
        Self {
            meta: AtomicU64::new(pack_pool(POOL_FREE, 0, 0)),
            exec_id: AtomicU64::new(0),
        }
    }

    fn allocate(&self, exec_id: u64) -> bool {
        let mut current = self.meta.load(Ordering::Acquire);
        loop {
            if pool_state(current) != POOL_FREE || pool_refs(current) != 0 {
                return false;
            }
            let epoch = pool_epoch(current) + 1;
            match self.meta.compare_exchange(
                current,
                pack_pool(POOL_INITIALIZING, 0, epoch),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    thread::yield_now();
                    self.exec_id.store(exec_id, Ordering::Release);
                    self.meta
                        .store(pack_pool(POOL_ALLOCATED, 1, epoch), Ordering::Release);
                    return true;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn allocate_publish_and_release(&self) {
        if self.allocate(1) {
            thread::yield_now();
            self.release_owner();
        }
    }

    fn acquire_probe_ref_from(&self, observed: u64) -> bool {
        if pool_state(observed) != POOL_ALLOCATED {
            return false;
        }
        let refs = pool_refs(observed);
        if refs == POOL_REF_MASK {
            return false;
        }
        self.meta
            .compare_exchange(
                observed,
                pack_pool(POOL_ALLOCATED, refs + 1, pool_epoch(observed)),
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    fn lookup_nonmatching_probe(&self) {
        let observed = self.meta.load(Ordering::Acquire);
        if self.acquire_probe_ref_from(observed) && self.exec_id.load(Ordering::Acquire) != 2 {
            self.release_ref();
        }
    }

    fn lookup_nonmatching_probe_after_observation_delay(&self) {
        let observed = self.meta.load(Ordering::Acquire);
        if pool_state(observed) != POOL_ALLOCATED {
            return;
        }
        thread::yield_now();
        if self.acquire_probe_ref_from(observed) && self.exec_id.load(Ordering::Acquire) != 2 {
            self.release_ref();
        }
    }

    fn release_owner(&self) {
        let mut current = self.meta.load(Ordering::Acquire);
        loop {
            if pool_state(current) != POOL_ALLOCATED {
                return;
            }
            let refs = pool_refs(current);
            assert!(refs > 0, "owner ref is missing");
            let epoch = pool_epoch(current);
            let next_refs = refs - 1;
            match self.meta.compare_exchange(
                current,
                pack_pool(POOL_RETIRING, next_refs, epoch),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if next_refs == 0 {
                        self.finish_retire(epoch);
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn release_ref(&self) {
        let mut current = self.meta.load(Ordering::Acquire);
        loop {
            let state = pool_state(current);
            assert!(
                state == POOL_ALLOCATED || state == POOL_RETIRING,
                "ref released from inactive slot",
            );
            let refs = pool_refs(current);
            assert!(refs > 0, "pool reference count underflowed");
            let epoch = pool_epoch(current);
            let next_refs = refs - 1;
            match self.meta.compare_exchange(
                current,
                pack_pool(state, next_refs, epoch),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    if state == POOL_RETIRING && next_refs == 0 {
                        self.finish_retire(epoch);
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn finish_retire(&self, epoch: u64) {
        self.exec_id.store(0, Ordering::Release);
        self.meta
            .store(pack_pool(POOL_FREE, 0, epoch), Ordering::Release);
    }
}

#[test]
fn builder_publication_makes_prior_bit_writes_visible() {
    loom::model(|| {
        let slot = Arc::new(ModelSlot::new());
        let generation = slot.acquire_builder().expect("builder");

        let writer = {
            let slot = slot.clone();
            thread::spawn(move || {
                slot.insert(generation);
                assert!(slot.publish(generation));
            })
        };

        let reader = {
            let slot = slot.clone();
            thread::spawn(move || {
                assert!(
                    !slot.reject_inserted_key(generation),
                    "reader observed Ready without the inserted bit",
                );
            })
        };

        writer.join().expect("writer should join");
        reader.join().expect("reader should join");
    });
}

#[test]
fn second_builder_cannot_clear_while_first_builder_is_active() {
    loom::model(|| {
        let slot = Arc::new(ModelSlot::new());

        let first = {
            let slot = slot.clone();
            thread::spawn(move || {
                if let Some(generation) = slot.acquire_builder() {
                    thread::yield_now();
                    slot.insert(generation);
                    let _ = slot.publish(generation);
                }
            })
        };

        let second = {
            let slot = slot.clone();
            thread::spawn(move || {
                if let Some(generation) = slot.acquire_builder() {
                    slot.insert(generation);
                    let _ = slot.publish(generation);
                }
            })
        };

        first.join().expect("first builder should join");
        second.join().expect("second builder should join");

        let final_state = slot.lifecycle.load(Ordering::Acquire);
        if state(final_state) == READY {
            assert_ne!(
                slot.bitset.load(Ordering::Relaxed) & 1,
                0,
                "a stale builder cleared the ready payload",
            );
        }
    });
}

#[test]
fn stale_disable_cannot_move_lifecycle_backward() {
    loom::model(|| {
        let slot = Arc::new(ModelSlot {
            lifecycle: AtomicU64::new(pack(2, READY)),
            bitset: AtomicU64::new(1),
        });

        let stale = {
            let slot = slot.clone();
            thread::spawn(move || {
                assert!(!slot.disable(1));
            })
        };

        let observer = {
            let slot = slot.clone();
            thread::spawn(move || {
                let current = slot.lifecycle.load(Ordering::Acquire);
                assert!(
                    generation(current) >= 2,
                    "stale transition moved generation backward",
                );
            })
        };

        stale.join().expect("stale actor should join");
        observer.join().expect("observer should join");
    });
}

#[test]
fn pool_initializing_state_blocks_probe_refcount_race() {
    loom::model(|| {
        let slot = Arc::new(ModelPoolSlot::new());

        let allocator = {
            let slot = slot.clone();
            thread::spawn(move || {
                slot.allocate_publish_and_release();
            })
        };

        let probe = {
            let slot = slot.clone();
            thread::spawn(move || {
                slot.lookup_nonmatching_probe();
            })
        };

        allocator.join().expect("allocator should join");
        probe.join().expect("probe should join");

        let final_meta = slot.meta.load(Ordering::Acquire);
        assert_eq!(pool_state(final_meta), POOL_FREE);
        assert_eq!(pool_refs(final_meta), 0);
    });
}

#[test]
fn stale_observed_probe_cannot_mutate_reused_pool_slot() {
    loom::model(|| {
        let slot = Arc::new(ModelPoolSlot::new());
        assert!(slot.allocate(1));

        let stale_probe = {
            let slot = slot.clone();
            thread::spawn(move || {
                slot.lookup_nonmatching_probe_after_observation_delay();
            })
        };

        let recycler = {
            let slot = slot.clone();
            thread::spawn(move || {
                thread::yield_now();
                slot.release_owner();
                let _ = slot.allocate(2);
            })
        };

        stale_probe.join().expect("stale probe should join");
        recycler.join().expect("recycler should join");

        let final_meta = slot.meta.load(Ordering::Acquire);
        match pool_state(final_meta) {
            POOL_FREE => assert_eq!(pool_refs(final_meta), 0),
            POOL_ALLOCATED => assert_eq!(pool_refs(final_meta), 1),
            other => panic!("unexpected final pool state {other}"),
        }
    });
}
