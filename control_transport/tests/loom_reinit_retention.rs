mod loom_support;

use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;
use loom_support::{
    meta_epoch, meta_owner_mask, meta_state, pack_region_meta, pack_slot_meta, run_model,
    FREE_PENDING, FREE_POPPED, FREE_PUBLISHED, FREE_PUSHED, FREE_PUSH_CLAIMED, LEASED,
    OWNER_BACKEND, WORKER_STATE_OFFLINE, WORKER_STATE_ONLINE, WORKER_STATE_REINITING,
};

const OLD_GENERATION: u64 = 1;
const REINIT_GENERATION: u64 = 2;
const OLD_EPOCH: u64 = 1;
const OLD_FREELIST_EPOCH: u64 = 1;
const NEW_FREELIST_EPOCH: u64 = 2;

struct ModelSlot {
    region_meta: AtomicU64,
    slot_generation: AtomicU64,
    slot_meta: AtomicU64,
    freelist_epoch: AtomicU64,
    freelist_len: AtomicUsize,
    pop_actor_alive: AtomicUsize,
    push_actor_alive: AtomicUsize,
    stage: AtomicUsize,
}

impl ModelSlot {
    fn new_live_popped_slot() -> Self {
        Self {
            region_meta: AtomicU64::new(pack_region_meta(OLD_GENERATION, WORKER_STATE_ONLINE)),
            slot_generation: AtomicU64::new(0),
            slot_meta: AtomicU64::new(pack_slot_meta(FREE_POPPED, OLD_FREELIST_EPOCH, 0)),
            freelist_epoch: AtomicU64::new(OLD_FREELIST_EPOCH),
            freelist_len: AtomicUsize::new(0),
            pop_actor_alive: AtomicUsize::new(1),
            push_actor_alive: AtomicUsize::new(0),
            stage: AtomicUsize::new(0),
        }
    }

    fn new_retained_old_lease() -> Self {
        Self {
            region_meta: AtomicU64::new(pack_region_meta(OLD_GENERATION, WORKER_STATE_ONLINE)),
            slot_generation: AtomicU64::new(OLD_GENERATION),
            slot_meta: AtomicU64::new(pack_slot_meta(LEASED, OLD_EPOCH, OWNER_BACKEND)),
            freelist_epoch: AtomicU64::new(OLD_FREELIST_EPOCH),
            freelist_len: AtomicUsize::new(0),
            pop_actor_alive: AtomicUsize::new(0),
            push_actor_alive: AtomicUsize::new(0),
            stage: AtomicUsize::new(0),
        }
    }

    fn new_live_post_push_claimed_slot() -> Self {
        Self {
            region_meta: AtomicU64::new(pack_region_meta(OLD_GENERATION, WORKER_STATE_ONLINE)),
            slot_generation: AtomicU64::new(0),
            slot_meta: AtomicU64::new(pack_slot_meta(FREE_PUSH_CLAIMED, OLD_FREELIST_EPOCH, 0)),
            freelist_epoch: AtomicU64::new(OLD_FREELIST_EPOCH),
            freelist_len: AtomicUsize::new(1),
            pop_actor_alive: AtomicUsize::new(0),
            push_actor_alive: AtomicUsize::new(1),
            stage: AtomicUsize::new(0),
        }
    }

    fn begin_reinit_wait_popped(&self) {
        self.region_meta.store(
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_REINITING),
            Ordering::Release,
        );

        loop {
            let current = self.slot_meta.load(Ordering::Acquire);
            if meta_state(current) == FREE_POPPED {
                self.stage.store(1, Ordering::Release);
                if self.pop_actor_alive.load(Ordering::Acquire) != 0 {
                    thread::yield_now();
                    continue;
                }
                if self
                    .slot_meta
                    .compare_exchange(
                        current,
                        pack_slot_meta(FREE_PENDING, 0, 0),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    break;
                }
                continue;
            }
            break;
        }

        self.freelist_epoch
            .store(NEW_FREELIST_EPOCH, Ordering::Release);
        self.freelist_len.store(0, Ordering::Release);

        loop {
            let current = self.slot_meta.load(Ordering::Acquire);
            match meta_state(current) {
                FREE_PENDING => {
                    if self
                        .slot_meta
                        .compare_exchange(
                            current,
                            pack_slot_meta(FREE_PUBLISHED, NEW_FREELIST_EPOCH, 0),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.freelist_len.store(1, Ordering::Release);
                        break;
                    }
                }
                FREE_PUBLISHED if meta_epoch(current) == NEW_FREELIST_EPOCH => {
                    break;
                }
                _ => thread::yield_now(),
            }
        }

        self.region_meta.store(
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_OFFLINE),
            Ordering::Release,
        );
    }

    fn begin_reinit_wait_push_claimed(&self) {
        self.region_meta.store(
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_REINITING),
            Ordering::Release,
        );

        loop {
            let current = self.slot_meta.load(Ordering::Acquire);
            if meta_state(current) == FREE_PUSH_CLAIMED {
                self.stage.store(1, Ordering::Release);
                if self.push_actor_alive.load(Ordering::Acquire) != 0 {
                    thread::yield_now();
                    continue;
                }
            }
            break;
        }

        self.freelist_epoch
            .store(NEW_FREELIST_EPOCH, Ordering::Release);
        self.freelist_len.store(0, Ordering::Release);

        loop {
            let current = self.slot_meta.load(Ordering::Acquire);
            match meta_state(current) {
                FREE_PENDING | FREE_PUBLISHED | FREE_PUSHED => {
                    if self
                        .slot_meta
                        .compare_exchange(
                            current,
                            pack_slot_meta(FREE_PUBLISHED, NEW_FREELIST_EPOCH, 0),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.freelist_len.store(1, Ordering::Release);
                        break;
                    }
                }
                _ => thread::yield_now(),
            }
        }

        self.region_meta.store(
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_OFFLINE),
            Ordering::Release,
        );
    }

    fn begin_reinit_pause_after_first_pass(&self) {
        self.region_meta.store(
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_REINITING),
            Ordering::Release,
        );
        self.freelist_epoch
            .store(NEW_FREELIST_EPOCH, Ordering::Release);
        self.freelist_len.store(0, Ordering::Release);

        self.stage.store(1, Ordering::Release);
        while self.stage.load(Ordering::Acquire) < 2 {
            thread::yield_now();
        }

        loop {
            let current = self.slot_meta.load(Ordering::Acquire);
            match meta_state(current) {
                LEASED => {
                    if meta_owner_mask(current) == 0 {
                        if self
                            .slot_meta
                            .compare_exchange(
                                current,
                                pack_slot_meta(FREE_PENDING, 0, 0),
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                        {
                            self.slot_generation.store(0, Ordering::Release);
                        }
                    } else {
                        thread::yield_now();
                    }
                }
                FREE_PENDING => {
                    if self
                        .slot_meta
                        .compare_exchange(
                            current,
                            pack_slot_meta(FREE_PUBLISHED, NEW_FREELIST_EPOCH, 0),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        self.freelist_len.store(1, Ordering::Release);
                        break;
                    }
                }
                FREE_PUBLISHED if meta_epoch(current) == NEW_FREELIST_EPOCH => break,
                _ => thread::yield_now(),
            }
        }

        self.region_meta.store(
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_OFFLINE),
            Ordering::Release,
        );
    }

    fn abort_popped_acquire(&self) {
        let current = self.slot_meta.load(Ordering::Acquire);
        if meta_state(current) == FREE_POPPED {
            let _ = self.slot_meta.compare_exchange(
                current,
                pack_slot_meta(FREE_PENDING, 0, 0),
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
        self.pop_actor_alive.store(0, Ordering::Release);
    }

    fn old_release_and_finalize(&self) {
        let current = self.slot_meta.load(Ordering::Acquire);
        if meta_state(current) == LEASED && meta_owner_mask(current) == OWNER_BACKEND {
            let ownerless = pack_slot_meta(LEASED, OLD_EPOCH, 0);
            if self
                .slot_meta
                .compare_exchange(current, ownerless, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let _ = self.slot_meta.compare_exchange(
                    ownerless,
                    pack_slot_meta(FREE_PENDING, 0, 0),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
                self.slot_generation.store(0, Ordering::Release);
            }
        }
    }

    fn finish_old_publication(&self) {
        loop {
            let current = self.slot_meta.load(Ordering::Acquire);
            if meta_state(current) == FREE_PUSH_CLAIMED {
                if self
                    .slot_meta
                    .compare_exchange(
                        current,
                        pack_slot_meta(FREE_PUSHED, OLD_FREELIST_EPOCH, 0),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    self.push_actor_alive.store(0, Ordering::Release);
                    let _ = self.slot_meta.compare_exchange(
                        pack_slot_meta(FREE_PUSHED, OLD_FREELIST_EPOCH, 0),
                        pack_slot_meta(FREE_PUBLISHED, OLD_FREELIST_EPOCH, 0),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    );
                    break;
                }
                continue;
            }
            break;
        }
    }
}

#[test]
fn reinit_waits_for_live_popped_token_before_republishing_slot() {
    run_model(|| {
        let slot = Arc::new(ModelSlot::new_live_popped_slot());
        let reinit = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.begin_reinit_wait_popped())
        };

        while slot.stage.load(Ordering::Acquire) == 0 {
            thread::yield_now();
        }
        assert_eq!(
            slot.freelist_len.load(Ordering::Acquire),
            0,
            "reinit must not republish while the popped token is still live",
        );

        let stale = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.abort_popped_acquire())
        };

        stale.join().expect("stale actor join");
        reinit.join().expect("reinit join");

        assert_eq!(
            slot.region_meta.load(Ordering::Acquire),
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_OFFLINE)
        );
        assert_eq!(slot.freelist_len.load(Ordering::Acquire), 1);
        assert_eq!(
            slot.slot_meta.load(Ordering::Acquire),
            pack_slot_meta(FREE_PUBLISHED, NEW_FREELIST_EPOCH, 0)
        );
    });
}

#[test]
fn reinit_republishes_free_pending_created_after_first_rebuild_pass() {
    run_model(|| {
        let slot = Arc::new(ModelSlot::new_retained_old_lease());
        let reinit = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.begin_reinit_pause_after_first_pass())
        };

        while slot.stage.load(Ordering::Acquire) == 0 {
            thread::yield_now();
        }

        let release = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.old_release_and_finalize())
        };
        release.join().expect("release join");
        slot.stage.store(2, Ordering::Release);

        reinit.join().expect("reinit join");

        assert_eq!(
            slot.region_meta.load(Ordering::Acquire),
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_OFFLINE)
        );
        assert_eq!(slot.freelist_len.load(Ordering::Acquire), 1);
        assert_eq!(slot.slot_generation.load(Ordering::Acquire), 0);
        assert_eq!(
            slot.slot_meta.load(Ordering::Acquire),
            pack_slot_meta(FREE_PUBLISHED, NEW_FREELIST_EPOCH, 0)
        );
    });
}

#[test]
fn reinit_waits_for_live_push_claimed_slot_before_rebuilding_freelist() {
    run_model(|| {
        let slot = Arc::new(ModelSlot::new_live_post_push_claimed_slot());
        let reinit = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.begin_reinit_wait_push_claimed())
        };

        while slot.stage.load(Ordering::Acquire) == 0 {
            thread::yield_now();
        }
        assert_eq!(
            slot.freelist_epoch.load(Ordering::Acquire),
            OLD_FREELIST_EPOCH,
            "reinit must not rotate freelist epoch while a push-claimed publisher is still live",
        );

        let publisher = {
            let slot = Arc::clone(&slot);
            thread::spawn(move || slot.finish_old_publication())
        };

        publisher.join().expect("publisher join");
        reinit.join().expect("reinit join");

        assert_eq!(
            slot.region_meta.load(Ordering::Acquire),
            pack_region_meta(REINIT_GENERATION, WORKER_STATE_OFFLINE)
        );
        assert_eq!(
            slot.freelist_epoch.load(Ordering::Acquire),
            NEW_FREELIST_EPOCH
        );
        assert_eq!(slot.freelist_len.load(Ordering::Acquire), 1);
        assert_eq!(
            slot.slot_meta.load(Ordering::Acquire),
            pack_slot_meta(FREE_PUBLISHED, NEW_FREELIST_EPOCH, 0)
        );
    });
}
