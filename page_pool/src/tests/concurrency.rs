use super::{cfg, init_pool};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
fn threaded_contention_keeps_live_ownership_unique() {
    let (_region, pool) = init_pool(cfg(64, 8));
    let live = Arc::new(Mutex::new(HashSet::<u32>::new()));

    thread::scope(|scope| {
        for thread_id in 0..8u8 {
            let live = Arc::clone(&live);
            scope.spawn(move || {
                for iter in 0..512u16 {
                    let mut lease = loop {
                        match pool.try_acquire() {
                            Ok(lease) => break lease,
                            Err(super::AcquireError::Empty) => thread::yield_now(),
                            Err(err) => panic!("unexpected acquire error: {err:?}"),
                        }
                    };

                    {
                        let mut guard = live.lock().expect("live set lock");
                        assert!(guard.insert(lease.page_id()));
                    }

                    lease.bytes_mut()[0] = thread_id ^ (iter as u8);

                    {
                        let mut guard = live.lock().expect("live set lock");
                        assert!(guard.remove(&lease.page_id()));
                    }
                }
            });
        }
    });

    assert!(live.lock().expect("live set lock").is_empty());
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 0);
    assert_eq!(snapshot.free_pages, 8);
    assert!(snapshot.acquire_ok >= 8 * 512);
}
