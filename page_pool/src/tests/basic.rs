use super::{cfg, init_pool};
use crate::{AcquireError, ConfigError, PagePool};

#[test]
fn layout_and_config_validation() {
    assert!(matches!(
        crate::PagePoolConfig::new(0, 1),
        Err(ConfigError::ZeroPageSize)
    ));
    assert!(matches!(
        crate::PagePoolConfig::new(64, 0),
        Err(ConfigError::ZeroPageCount)
    ));
    assert!(matches!(
        crate::PagePoolConfig::new(32, 1),
        Err(ConfigError::PageSizeTooSmall { .. })
    ));
    assert!(matches!(
        crate::PagePoolConfig::new(96, 1),
        Err(ConfigError::PageSizeNotAligned { .. })
    ));

    let layout = PagePool::layout(cfg(128, 4)).expect("layout");
    assert_eq!(layout.align, 64);
    assert!(layout.size >= 128 * 4);
}

#[test]
fn init_attach_roundtrip_and_shared_state() {
    let config = cfg(128, 2);
    let (region, pool) = init_pool(config);
    let attached = unsafe { PagePool::attach(region.base, pool.region_size()) }.expect("attach");

    assert_eq!(attached.page_size(), 128);
    assert_eq!(attached.page_count(), 2);

    let mut lease = pool.try_acquire().expect("lease");
    lease.bytes_mut()[0] = 41;
    let desc = lease.into_descriptor().expect("detach");
    let shared = unsafe { attached.page_bytes(desc) }.expect("shared read");
    assert_eq!(shared[0], 41);
    attached
        .release(desc)
        .expect("release from attached handle");

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.acquire_ok, 1);
    assert_eq!(snapshot.release_ok, 1);
    assert_eq!(snapshot.release_exhausted, 0);
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 0);
    assert_eq!(snapshot.free_pages, 2);
}

#[test]
fn acquire_until_empty_and_raii_release() {
    let (_region, pool) = init_pool(cfg(64, 2));

    let lease_a = pool.try_acquire().expect("lease a");
    let lease_b = pool.try_acquire().expect("lease b");
    assert!(matches!(pool.try_acquire(), Err(AcquireError::Empty)));
    assert_eq!(pool.snapshot().leased_pages, 2);

    drop(lease_a);
    assert_eq!(pool.snapshot().leased_pages, 1);
    drop(lease_b);

    let snapshot = pool.snapshot();
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 0);
    assert_eq!(snapshot.free_pages, 2);
    assert_eq!(snapshot.acquire_ok, 2);
    assert_eq!(snapshot.acquire_empty, 1);
    assert_eq!(snapshot.release_ok, 2);
    assert_eq!(snapshot.release_exhausted, 0);
    assert_eq!(snapshot.high_watermark_pages, 2);
}

#[test]
fn into_descriptor_requires_explicit_release() {
    let (_region, pool) = init_pool(cfg(64, 1));

    let lease = pool.try_acquire().expect("lease");
    let desc = lease.into_descriptor().expect("detach");
    assert!(matches!(pool.try_acquire(), Err(AcquireError::Empty)));
    assert!(unsafe { pool.page_bytes(desc) }.is_ok());

    pool.release(desc).expect("explicit release");
    let snapshot = pool.snapshot();
    assert_eq!(snapshot.leased_pages, 0);
    assert_eq!(snapshot.exhausted_pages, 0);
    assert_eq!(snapshot.release_ok, 1);
    assert_eq!(snapshot.release_exhausted, 0);
}
