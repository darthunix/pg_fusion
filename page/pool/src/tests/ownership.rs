use super::{cfg, init_pool, AccessError, OwnedRegion, PageDescriptor, PagePool, ReleaseError};

#[test]
fn stale_and_double_release_are_rejected() {
    let (_region, pool) = init_pool(cfg(64, 1));

    let desc1 = pool
        .try_acquire()
        .expect("lease1")
        .into_descriptor()
        .expect("detach1");
    pool.release(desc1).expect("release1");

    let lease2 = pool.try_acquire().expect("lease2");
    let desc2 = lease2.descriptor();
    assert_ne!(desc1.generation, desc2.generation);

    assert!(matches!(
        pool.release(desc1),
        Err(ReleaseError::StaleGeneration { .. })
    ));

    let desc2 = lease2.into_descriptor().expect("detach2");
    pool.release(desc2).expect("release2");
    assert!(matches!(
        pool.release(desc2),
        Err(ReleaseError::NotLeased { .. })
    ));
}

#[test]
fn attached_descriptor_cannot_release_or_access_live_lease() {
    let (_region, pool) = init_pool(cfg(64, 1));

    let mut lease = pool.try_acquire().expect("lease");
    let desc = lease.descriptor();

    assert!(matches!(
        pool.release(desc),
        Err(ReleaseError::LeaseStillAttached { .. })
    ));
    assert!(matches!(
        unsafe { pool.page_bytes(desc) },
        Err(AccessError::LeaseStillAttached { .. })
    ));
    assert!(matches!(
        pool.try_acquire(),
        Err(super::AcquireError::Empty)
    ));

    lease.bytes_mut()[0] = 7;
    let desc = lease.into_descriptor().expect("detach");
    assert_eq!(
        unsafe { pool.page_bytes(desc) }.expect("detached read")[0],
        7
    );
    pool.release(desc).expect("release");

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.release_ok, 1);
    assert_eq!(snapshot.release_exhausted, 0);
    assert_eq!(snapshot.release_still_attached, 1);
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 0);
}

#[test]
fn detached_descriptor_rejects_wrong_pool() {
    let (_region_a, pool_a) = init_pool(cfg(64, 1));
    let (_region_b, pool_b) = init_pool(cfg(64, 1));

    let desc_a = pool_a
        .try_acquire()
        .expect("lease a")
        .into_descriptor()
        .expect("detach a");
    let desc_b = pool_b
        .try_acquire()
        .expect("lease b")
        .into_descriptor()
        .expect("detach b");

    assert_eq!(desc_a.page_id, desc_b.page_id);
    assert_eq!(desc_a.generation, desc_b.generation);
    assert_ne!(desc_a.pool_id, desc_b.pool_id);

    assert!(matches!(
        pool_b.release(desc_a),
        Err(ReleaseError::PoolMismatch { .. })
    ));
    assert!(matches!(
        unsafe { pool_b.page_bytes(desc_a) },
        Err(AccessError::PoolMismatch { .. })
    ));

    pool_a.release(desc_a).expect("release a");
    pool_b.release(desc_b).expect("release b");
}

#[test]
fn reinitialized_region_rejects_stale_descriptor_from_previous_pool_lifetime() {
    let config = cfg(64, 1);
    let layout = PagePool::layout(config).expect("layout");
    let region = OwnedRegion::new(layout);

    let pool_a =
        unsafe { PagePool::init_in_place(region.base, layout.size, config) }.expect("init a");
    let stale_desc = pool_a
        .try_acquire()
        .expect("lease a")
        .into_descriptor()
        .expect("detach a");

    let pool_b =
        unsafe { PagePool::init_in_place(region.base, layout.size, config) }.expect("init b");
    let fresh_desc = pool_b.try_acquire().expect("lease b").descriptor();

    assert_eq!(stale_desc.page_id, fresh_desc.page_id);
    assert_eq!(stale_desc.generation, fresh_desc.generation);
    assert_ne!(stale_desc.pool_id, fresh_desc.pool_id);

    assert!(matches!(
        pool_b.release(stale_desc),
        Err(ReleaseError::PoolMismatch { .. })
    ));
    assert!(matches!(
        unsafe { pool_b.page_bytes(stale_desc) },
        Err(AccessError::PoolMismatch { .. })
    ));
}

#[test]
fn snapshot_counters_and_gauges_are_exact() {
    let (_region, pool) = init_pool(cfg(64, 2));

    let lease_a = pool.try_acquire().expect("lease a");
    let lease_b = pool.try_acquire().expect("lease b");
    assert!(matches!(
        pool.try_acquire(),
        Err(super::AcquireError::Empty)
    ));

    let desc_a = lease_a.into_descriptor().expect("detach a");
    pool.release(desc_a).expect("release a");

    let lease_c = pool.try_acquire().expect("lease c");
    let desc_c_snapshot = lease_c.descriptor();
    assert!(matches!(
        pool.release(desc_a),
        Err(ReleaseError::StaleGeneration { .. })
    ));
    assert!(matches!(
        pool.release(desc_c_snapshot),
        Err(ReleaseError::LeaseStillAttached { .. })
    ));
    assert!(matches!(
        pool.release(PageDescriptor {
            pool_id: desc_c_snapshot.pool_id,
            page_id: 99,
            generation: 0,
        }),
        Err(ReleaseError::BadPageId { .. })
    ));

    let desc_b = lease_b.into_descriptor().expect("detach b");
    let desc_c = lease_c.into_descriptor().expect("detach c");
    pool.release(desc_b).expect("release b");
    pool.release(desc_c).expect("release c");
    assert!(matches!(
        pool.release(desc_c),
        Err(ReleaseError::NotLeased { .. })
    ));

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.page_count, 2);
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 0);
    assert_eq!(snapshot.free_pages, 2);
    assert_eq!(snapshot.high_watermark_pages, 2);
    assert_eq!(snapshot.acquire_ok, 3);
    assert_eq!(snapshot.acquire_empty, 1);
    assert_eq!(snapshot.release_ok, 3);
    assert_eq!(snapshot.release_exhausted, 0);
    assert_eq!(snapshot.release_bad_page, 1);
    assert_eq!(snapshot.release_stale_generation, 1);
    assert_eq!(snapshot.release_still_attached, 1);
    assert_eq!(snapshot.release_not_leased, 1);
    assert_eq!(snapshot.acquire_retry_loops, 0);
    assert_eq!(snapshot.release_retry_loops, 0);
}
