use crate::NotifyError;
use std::io;

#[cfg(test)]
use std::cell::RefCell;

#[cfg(test)]
type SignalHook = Box<dyn Fn(i32) -> Result<bool, NotifyError>>;
#[cfg(test)]
type ProbeHook = Box<dyn Fn(i32) -> io::Result<bool>>;

#[cfg(test)]
thread_local! {
    static SIGNAL_HOOK: RefCell<Option<SignalHook>> = const { RefCell::new(None) };
    static PROBE_HOOK: RefCell<Option<ProbeHook>> = const { RefCell::new(None) };
}

pub(crate) fn signal_pid_usr1(pid: i32) -> Result<bool, NotifyError> {
    #[cfg(test)]
    if let Some(result) = run_signal_hook_for_tests(pid) {
        return result;
    }

    if pid <= 0 {
        return Ok(false);
    }

    #[cfg(unix)]
    {
        let rc = unsafe { libc::kill(pid as libc::pid_t, libc::SIGUSR1) };
        if rc == 0 {
            return Ok(true);
        }

        let err = io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(false);
        }
        Err(NotifyError::Signal(err))
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(NotifyError::Signal(io::Error::new(
            io::ErrorKind::Unsupported,
            "signals are unsupported on this platform",
        )))
    }
}

pub(crate) fn probe_pid_alive(pid: i32) -> io::Result<bool> {
    #[cfg(test)]
    if let Some(result) = run_probe_hook_for_tests(pid) {
        return result;
    }

    if pid <= 0 {
        return Ok(false);
    }

    #[cfg(unix)]
    {
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        if rc == 0 {
            return Ok(true);
        }

        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::EPERM) => Ok(true),
            Some(libc::ESRCH) => Ok(false),
            _ => Err(err),
        }
    }

    #[cfg(not(unix))]
    {
        let _ = pid;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "pid probing is unsupported on this platform",
        ))
    }
}

#[cfg(test)]
pub(crate) fn set_signal_hook_for_tests<F>(hook: F)
where
    F: Fn(i32) -> Result<bool, NotifyError> + 'static,
{
    SIGNAL_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
pub(crate) fn clear_signal_hook_for_tests() {
    SIGNAL_HOOK.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[cfg(test)]
pub(crate) fn set_probe_hook_for_tests<F>(hook: F)
where
    F: Fn(i32) -> io::Result<bool> + 'static,
{
    PROBE_HOOK.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(hook));
    });
}

#[cfg(test)]
pub(crate) fn clear_probe_hook_for_tests() {
    PROBE_HOOK.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

#[cfg(test)]
fn run_signal_hook_for_tests(pid: i32) -> Option<Result<bool, NotifyError>> {
    SIGNAL_HOOK.with(|slot| slot.borrow().as_ref().map(|hook| hook(pid)))
}

#[cfg(test)]
fn run_probe_hook_for_tests(pid: i32) -> Option<io::Result<bool>> {
    PROBE_HOOK.with(|slot| slot.borrow().as_ref().map(|hook| hook(pid)))
}
