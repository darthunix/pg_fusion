use super::{cfg, init_pool, pack_state, AccessError, LeaseState, ReleaseError, MAX_GENERATION};
use std::sync::atomic::Ordering;

#[test]
fn max_generation_release_retires_page() {
    let (_region, pool) = init_pool(cfg(64, 1));
    pool.state_ref(0).store(
        pack_state(MAX_GENERATION - 1, LeaseState::Free),
        Ordering::Release,
    );

    let lease = pool.try_acquire().expect("lease");
    assert_eq!(lease.generation(), MAX_GENERATION);

    let desc = lease.into_descriptor().expect("detach");
    pool.release(desc).expect("retire exhausted page");

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 1);
    assert_eq!(snapshot.free_pages, 0);
    assert_eq!(snapshot.release_ok, 1);
    assert_eq!(snapshot.release_exhausted, 1);

    assert!(matches!(
        pool.try_acquire(),
        Err(super::AcquireError::Empty)
    ));
    assert!(matches!(
        unsafe { pool.page_bytes(desc) },
        Err(AccessError::Exhausted { .. })
    ));
    assert!(matches!(
        pool.release(desc),
        Err(ReleaseError::Exhausted { .. })
    ));
}
