mod loom_support;

use loom::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use loom::sync::Arc;
use loom::thread;
use loom_support::run_model;

struct ModelRing {
    head: AtomicU32,
    tail: AtomicU32,
    ready: AtomicBool,
    published: AtomicU32,
}

impl ModelRing {
    fn new(head: u32, tail: u32, ready: bool, published: u32) -> Self {
        Self {
            head: AtomicU32::new(head),
            tail: AtomicU32::new(tail),
            ready: AtomicBool::new(ready),
            published: AtomicU32::new(published),
        }
    }

    fn publish(&self) {
        let next_tail = self.tail.load(Ordering::Acquire) + 1;
        self.published.store(next_tail, Ordering::Release);
        self.tail.store(next_tail, Ordering::Release);
        self.ready.store(true, Ordering::Release);
    }

    fn consume_and_maybe_clear(&self) -> bool {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if head == tail {
            return false;
        }

        let published = self.published.load(Ordering::Acquire);
        assert!(
            published >= tail,
            "observed tail {tail} without published marker {published}",
        );

        let next_head = head + 1;
        self.head.store(next_head, Ordering::Release);
        if next_head == tail {
            self.update_ready_after_consume(next_head);
        }
        true
    }

    fn update_ready_after_consume(&self, next_head: u32) {
        let tail_after_head = self.tail.load(Ordering::Acquire);
        if tail_after_head != next_head {
            return;
        }

        self.ready.store(false, Ordering::Release);

        let tail_after_clear = self.tail.load(Ordering::Acquire);
        if tail_after_clear != next_head {
            self.ready.store(true, Ordering::Release);
        }
    }

    fn assert_quiescent_state(&self) {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        let ready = self.ready.load(Ordering::Acquire);
        let published = self.published.load(Ordering::Acquire);

        if tail != head {
            assert!(
                published >= tail,
                "tail {tail} is ahead of published marker {published}",
            );
            if !ready {
                let tail_after_rescan = self.tail.load(Ordering::Acquire);
                assert_eq!(
                    tail_after_rescan, tail,
                    "tail changed during quiescent rescan in the reduced-core model",
                );
            }
        }
    }
}

#[test]
fn clear_then_republish_does_not_lose_ready() {
    run_model(|| {
        let ring = Arc::new(ModelRing::new(0, 1, true, 1));

        let producer = ring.clone();
        let producer = thread::spawn(move || {
            producer.publish();
        });

        let consumer = ring.clone();
        let consumer = thread::spawn(move || {
            consumer.consume_and_maybe_clear();
        });

        producer.join().expect("producer should join cleanly");
        consumer.join().expect("consumer should join cleanly");
        ring.assert_quiescent_state();
    });
}

#[test]
fn observed_tail_never_outpaces_publish_marker() {
    run_model(|| {
        let ring = Arc::new(ModelRing::new(0, 0, false, 0));

        let producer = ring.clone();
        let producer = thread::spawn(move || {
            producer.publish();
        });

        let observer = ring.clone();
        let observer = thread::spawn(move || {
            let head = observer.head.load(Ordering::Acquire);
            let tail = observer.tail.load(Ordering::Acquire);
            if tail != head {
                let published = observer.published.load(Ordering::Acquire);
                assert!(
                    published >= tail,
                    "observed tail {tail} without published marker {published}",
                );
            }
        });

        producer.join().expect("producer should join cleanly");
        observer.join().expect("observer should join cleanly");
        ring.assert_quiescent_state();
    });
}
