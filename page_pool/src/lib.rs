//! Shared-memory fixed-page pool with explicit descriptor handoff.
//!
//! `page_pool` manages allocation ownership for fixed-size pages inside a
//! caller-provided shared-memory region. It does not allocate the region, map
//! it, or define when page contents become readable or writable across
//! processes.
//!
//! # Ownership model
//!
//! 1. Compute the required region shape with [`PagePool::layout`].
//! 2. Initialize the region exactly once with [`PagePool::init_in_place`].
//! 3. Attach process-local handles with [`PagePool::attach`].
//! 4. Acquire an attached [`PageLease`] with [`PagePool::try_acquire`].
//! 5. Either drop the lease to return the page automatically, or call
//!    [`PageLease::into_descriptor`] to detach ownership into a
//!    [`PageDescriptor`] for cross-process handoff.
//! 6. Release detached ownership with [`PagePool::release`].
//!
//! A live [`PageLease`] provides safe `bytes()` and `bytes_mut()` access because
//! the pool tracks whether ownership is still attached to that lease. Detached
//! descriptors cannot be released or accessed until the lease has been
//! explicitly detached. Detached descriptors also carry a pool instance
//! identity, so a descriptor from one pool or pool lifetime is rejected by a
//! different pool.
//!
//! # Non-goals
//!
//! This crate intentionally does not provide:
//!
//! - ring buffers or message transport
//! - publish/read/ack protocols
//! - shared reader refcounts
//! - shared-memory allocation or mapping APIs such as `mmap`, `memfd`, or
//!   Postgres `ShmemInitStruct`
//!
//! # Exhausted pages
//!
//! Page generations never wrap. When a page reaches the maximum representable
//! generation, releasing it retires that page into a terminal exhausted state
//! instead of returning it to the freelist. This prevents stale descriptors
//! from becoming valid again after long uptimes.
//!
//! # Example
//!
//! ```rust
//! use page_pool::{PagePool, PagePoolConfig};
//! use std::alloc::{alloc_zeroed, dealloc, Layout};
//! use std::ptr::NonNull;
//!
//! let config = PagePoolConfig::new(4096, 8).unwrap();
//! let region = PagePool::layout(config).unwrap();
//! let layout = Layout::from_size_align(region.size, region.align).unwrap();
//! let base = NonNull::new(unsafe { alloc_zeroed(layout) }).unwrap();
//!
//! let pool = unsafe { PagePool::init_in_place(base, region.size, config) }.unwrap();
//! let mut lease = pool.try_acquire().unwrap();
//! lease.bytes_mut()[0] = 42;
//!
//! let desc = lease.into_descriptor().unwrap();
//! assert_eq!(unsafe { pool.page_bytes(desc).unwrap() }[0], 42);
//! pool.release(desc).unwrap();
//!
//! drop(pool);
//! unsafe { dealloc(base.as_ptr(), layout) };
//! ```

mod error;
mod layout;
mod lease;
mod pool;
mod shm;
mod types;

#[cfg(test)]
mod tests;

pub use error::{
    AccessError, AcquireError, AttachError, ConfigError, DetachError, InitError, ReleaseError,
};
pub use lease::PageLease;
pub use pool::PagePool;
pub use types::{PageDescriptor, PagePoolConfig, PoolSnapshot, RegionLayout};
