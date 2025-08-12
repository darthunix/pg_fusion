use crate::worker::treiber_stack;
use anyhow::Result as AnyResult;
use pgrx::warning;
use std::cell::OnceCell;

/// RAII guard that acquires a free connection id from the global Treiber stack
/// and returns it back on drop.
pub(crate) struct ConnectionHandle(u32);

impl ConnectionHandle {
    pub(crate) fn acquire() -> AnyResult<Self> {
        match treiber_stack().allocate() {
            Ok(id) => Ok(Self(id)),
            Err(e) => anyhow::bail!("No free connection slots: {e:?}"),
        }
    }

    #[inline]
    pub(crate) fn id(&self) -> u32 {
        self.0
    }
}

impl Drop for ConnectionHandle {
    fn drop(&mut self) {
        if let Err(e) = treiber_stack().release(self.0) {
            warning!("Failed to release connection slot {}: {:?}", self.0, e);
        }
    }
}

// Thread-local, one-per-backend handle that returns to the stack on thread exit.
thread_local! {
    static CONNECTION_HANDLE: OnceCell<ConnectionHandle> = OnceCell::new();
}

/// Ensure the per-backend ConnectionHandle is initialized exactly once.
pub(crate) fn acuire_connection() -> AnyResult<()> {
    CONNECTION_HANDLE.with(|cell| {
        if cell.get().is_none() {
            let handle = ConnectionHandle::acquire()?;
            // Ignore error if already set by a race in the same thread (unlikely)
            let _ = cell.set(handle);
        }
        Ok(())
    })
}

/// Get the current backend's connection id, initializing lazily on first use.
pub(crate) fn connection_id() -> AnyResult<u32> {
    CONNECTION_HANDLE.with(|cell| {
        if cell.get().is_none() {
            let handle = ConnectionHandle::acquire()?;
            let _ = cell.set(handle);
        }
        Ok(cell.get().unwrap().id())
    })
}
