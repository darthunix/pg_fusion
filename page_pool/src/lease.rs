use crate::error::DetachError;
use crate::pool::PagePool;
use crate::types::PageDescriptor;
use std::slice;

/// RAII handle for an attached page lease.
///
/// While attached, the lease provides safe access to the page bytes and will
/// return the page to the pool on drop. Use [`PageLease::into_descriptor`] to
/// detach ownership for cross-process handoff.
pub struct PageLease {
    pub(crate) pool: PagePool,
    pub(crate) descriptor: PageDescriptor,
    pub(crate) detached: bool,
}

impl PageLease {
    /// Return a non-owning snapshot of the current lease identity.
    ///
    /// The returned descriptor cannot be released until this lease is detached
    /// with [`PageLease::into_descriptor`]. It includes the current pool
    /// identity so detached APIs can reject misrouted descriptors.
    pub fn descriptor(&self) -> PageDescriptor {
        self.descriptor
    }

    /// Detach this lease into a releasable descriptor.
    ///
    /// After a successful detach, dropping the lease no longer returns the page
    /// to the pool. The caller becomes responsible for handing off or releasing
    /// the returned [`PageDescriptor`].
    pub fn into_descriptor(mut self) -> Result<PageDescriptor, DetachError> {
        self.pool.detach_descriptor(self.descriptor)?;
        self.detached = true;
        Ok(self.descriptor)
    }

    /// Return the stable page slot index.
    pub fn page_id(&self) -> u32 {
        self.descriptor.page_id
    }

    /// Return the current page generation.
    pub fn generation(&self) -> u64 {
        self.descriptor.generation
    }

    /// Borrow the attached page contents immutably.
    pub fn bytes(&self) -> &[u8] {
        unsafe {
            self.pool
                .page_slice_unchecked(self.descriptor.page_id as usize)
        }
    }

    /// Borrow the attached page contents mutably.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        unsafe {
            slice::from_raw_parts_mut(
                self.pool
                    .page_ptr_unchecked(self.descriptor.page_id as usize),
                self.pool.page_size(),
            )
        }
    }
}

impl Drop for PageLease {
    fn drop(&mut self) {
        if !self.detached {
            let _ = self.pool.release_attached(self.descriptor);
        }
    }
}
