use crate::worker::treiber_stack;
use anyhow::Result as AnyResult;
use executor::buffer::LockFreeBuffer;
use executor::layout::{connection_ptrs, socket_ptrs, ConnectionLayout};
use pgrx::warning;
use std::cell::OnceCell;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};

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

pub(crate) struct ConnectionShared<'a> {
    pub flag: &'a AtomicBool,
    pub recv: LockFreeBuffer<'a>,
    pub send: LockFreeBuffer<'a>,
    pub client_pid: &'a AtomicI32,
    pub server_pid: &'a AtomicI32,
}

impl<'a> ConnectionShared<'a> {
    #[inline]
    pub fn server_pid(&self) -> i32 {
        self.server_pid.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn set_client_pid(&self, pid: i32) {
        self.client_pid.store(pid, Ordering::Relaxed)
    }

    /// Send SIGUSR1 to the server process.
    pub fn signal_server(&self) -> AnyResult<()> {
        #[cfg(unix)]
        {
            // Mark this connection as ready in shared memory so the server's
            // runtime (when it starts or is already running) will see our flag
            // on its next poll without requiring a signal.
            self.flag.store(true, Ordering::Release);

            // Best-effort: wait briefly for the worker to publish PID.
            let mut pid = self.server_pid.load(Ordering::Relaxed);
            if pid <= 0 {
                use std::time::Duration;
                for _ in 0..200 {
                    std::thread::sleep(Duration::from_millis(10));
                    pid = self.server_pid.load(Ordering::Relaxed);
                    if pid > 0 {
                        break;
                    }
                }
            }
            if pid <= 0 {
                // Worker not ready yet; we've set the flag so it will be noticed
                // on the first poll after startup. Do not fail the operation.
                return Ok(());
            }

            let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
            if rc == -1 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(())
        }
        #[cfg(not(unix))]
        {
            anyhow::bail!("sending signals is unsupported on this platform")
        }
    }
}

/// Resolve shared-memory regions for a given connection id: flag, recv and send buffers.
pub(crate) fn connection_shared(id: u32) -> AnyResult<ConnectionShared<'static>> {
    let num = crate::max_backends() as usize;
    let id_usize = id as usize;
    if id_usize >= num {
        anyhow::bail!("invalid connection id: {} (max {})", id, num);
    }

    // Get flags slice and choose this connection's flag
    let flags = crate::worker::shared_flags_slice();
    let flag_ref: &'static AtomicBool = unsafe { &*(&flags[id_usize] as *const AtomicBool) };

    // Compute connection buffers from base + layout
    let layout: ConnectionLayout = crate::worker::connections_layout();
    let base = crate::worker::connections_base();
    let conn_base = unsafe { base.add(id_usize * layout.layout.size()) };
    let (recv_base, send_base, client_ptr) = unsafe { connection_ptrs(conn_base, layout) };

    // Resolve recv buffer via socket layout and create buffer views
    let recv_buf_base = unsafe { socket_ptrs(recv_base, layout.recv_socket_layout) };
    let recv = unsafe {
        LockFreeBuffer::from_layout(recv_buf_base, layout.recv_socket_layout.buffer_layout)
    };
    let send = unsafe { LockFreeBuffer::from_layout(send_base, layout.send_buffer_layout) };
    let client_pid_ref: &'static AtomicI32 = unsafe { &*client_ptr };
    let server_pid_ref: &'static AtomicI32 = crate::worker::server_pid_atomic();

    Ok(ConnectionShared {
        flag: flag_ref,
        recv,
        send,
        client_pid: client_pid_ref,
        server_pid: server_pid_ref,
    })
}
