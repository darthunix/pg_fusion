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

fn pack(generation: u64, state: u64) -> u64 {
    (generation << 2) | state
}

fn generation(word: u64) -> u64 {
    word >> 2
}

fn state(word: u64) -> u64 {
    word & 3
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
    state: AtomicU64,
    refs: AtomicU64,
    exec_id: AtomicU64,
}

impl ModelPoolSlot {
    fn new() -> Self {
        Self {
            state: AtomicU64::new(POOL_FREE),
            refs: AtomicU64::new(0),
            exec_id: AtomicU64::new(0),
        }
    }

    fn allocate_publish_and_release(&self) {
        if self
            .state
            .compare_exchange(
                POOL_FREE,
                POOL_INITIALIZING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return;
        }

        thread::yield_now();
        self.refs.store(1, Ordering::Release);
        self.exec_id.store(1, Ordering::Release);
        self.state.store(POOL_ALLOCATED, Ordering::Release);
        thread::yield_now();
        self.release_owner();
    }

    fn lookup_nonmatching_probe(&self) {
        if self.state.load(Ordering::Acquire) != POOL_ALLOCATED {
            return;
        }

        self.refs.fetch_add(1, Ordering::AcqRel);
        let matches = self.state.load(Ordering::Acquire) == POOL_ALLOCATED
            && self.exec_id.load(Ordering::Acquire) == 2;
        if !matches {
            self.release_ref();
        }
    }

    fn release_owner(&self) {
        let _ = self.state.compare_exchange(
            POOL_ALLOCATED,
            POOL_RETIRING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        self.release_ref();
    }

    fn release_ref(&self) {
        let old_refs = self.refs.fetch_sub(1, Ordering::AcqRel);
        assert!(old_refs > 0, "pool reference count underflowed");
        if old_refs == 1 && self.state.load(Ordering::Acquire) == POOL_RETIRING {
            self.exec_id.store(0, Ordering::Release);
            self.state.store(POOL_FREE, Ordering::Release);
        }
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

        assert_eq!(slot.state.load(Ordering::Acquire), POOL_FREE);
        assert_eq!(slot.refs.load(Ordering::Acquire), 0);
    });
}
