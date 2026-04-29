use loom::sync::atomic::{AtomicU64, Ordering};
use loom::sync::Arc;
use loom::thread;

const FREE: u64 = 0;
const BUILDING: u64 = 1;
const READY: u64 = 2;
const DISABLED: u64 = 3;

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
