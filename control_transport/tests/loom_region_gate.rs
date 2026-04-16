mod loom_support;

use loom::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use loom::sync::Arc;
use loom::thread;
use loom_support::{
    pack_region_meta, region_generation, region_is_online_generation, run_model,
    WORKER_STATE_OFFLINE, WORKER_STATE_ONLINE, WORKER_STATE_RESTARTING,
};

const SLOT_FREE: u64 = 0;
const SLOT_RESERVED: u64 = 1;
const SLOT_LEASED: u64 = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcquireResult {
    Ok(u64),
    WorkerOffline,
    Empty,
}

struct ModelRegionGate {
    region_meta: AtomicU64,
    slot_state: AtomicU64,
    slot_generation: AtomicU64,
    stage: AtomicUsize,
}

impl ModelRegionGate {
    fn new_online(generation: u64) -> Self {
        Self {
            region_meta: AtomicU64::new(pack_region_meta(generation, WORKER_STATE_ONLINE)),
            slot_state: AtomicU64::new(SLOT_FREE),
            slot_generation: AtomicU64::new(0),
            stage: AtomicUsize::new(0),
        }
    }

    fn wait_for_stage(&self, target: usize) {
        while self.stage.load(Ordering::Acquire) < target {
            thread::yield_now();
        }
    }

    fn try_acquire_with_pause(&self) -> AcquireResult {
        let snapshot = self.region_meta.load(Ordering::Acquire);
        let generation = region_generation(snapshot);
        if !region_is_online_generation(snapshot, generation) {
            return AcquireResult::WorkerOffline;
        }

        self.stage.store(1, Ordering::Release);
        self.wait_for_stage(2);

        if self
            .slot_state
            .compare_exchange(
                SLOT_FREE,
                SLOT_RESERVED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return AcquireResult::Empty;
        }

        self.slot_generation.store(generation, Ordering::Release);
        self.slot_state.store(SLOT_LEASED, Ordering::Release);

        let recheck = self.region_meta.load(Ordering::Acquire);
        if !region_is_online_generation(recheck, generation) {
            self.slot_generation.store(0, Ordering::Release);
            self.slot_state.store(SLOT_FREE, Ordering::Release);
            return AcquireResult::WorkerOffline;
        }

        AcquireResult::Ok(generation)
    }

    fn activate_generation(&self, generation: u64) {
        self.region_meta.store(
            pack_region_meta(generation, WORKER_STATE_RESTARTING),
            Ordering::Release,
        );
        self.region_meta.store(
            pack_region_meta(generation, WORKER_STATE_ONLINE),
            Ordering::Release,
        );
    }

    fn deactivate_generation(&self, generation: u64) {
        self.region_meta.store(
            pack_region_meta(generation, WORKER_STATE_OFFLINE),
            Ordering::Release,
        );
    }
}

#[test]
fn acquire_rolls_back_if_deactivate_happens_between_snapshot_and_recheck() {
    run_model(|| {
        let region = Arc::new(ModelRegionGate::new_online(5));

        let acquirer = {
            let region = region.clone();
            thread::spawn(move || region.try_acquire_with_pause())
        };

        let deactivator = {
            let region = region.clone();
            thread::spawn(move || {
                region.wait_for_stage(1);
                region.deactivate_generation(6);
                region.stage.store(2, Ordering::Release);
            })
        };

        let result = acquirer.join().expect("acquirer");
        deactivator.join().expect("deactivator");

        assert_eq!(result, AcquireResult::WorkerOffline);
        assert_eq!(region.slot_state.load(Ordering::Acquire), SLOT_FREE);
        assert_eq!(region.slot_generation.load(Ordering::Acquire), 0);
    });
}

#[test]
fn acquire_rolls_back_if_activate_bumps_generation_between_snapshot_and_recheck() {
    run_model(|| {
        let region = Arc::new(ModelRegionGate::new_online(7));

        let acquirer = {
            let region = region.clone();
            thread::spawn(move || region.try_acquire_with_pause())
        };

        let activator = {
            let region = region.clone();
            thread::spawn(move || {
                region.wait_for_stage(1);
                region.activate_generation(8);
                region.stage.store(2, Ordering::Release);
            })
        };

        let result = acquirer.join().expect("acquirer");
        activator.join().expect("activator");

        assert_eq!(result, AcquireResult::WorkerOffline);
        assert_eq!(region.slot_state.load(Ordering::Acquire), SLOT_FREE);
        assert_eq!(region.slot_generation.load(Ordering::Acquire), 0);
        assert_eq!(
            region.region_meta.load(Ordering::Acquire),
            pack_region_meta(8, WORKER_STATE_ONLINE)
        );
    });
}
