use std::path::{Path, PathBuf};
use std::sync::Arc;

use control_transport::BackendLeaseSlot;
use datafusion_execution::{
    disk_manager::{DiskManagerBuilder, DiskManagerMode},
    memory_pool::{FairSpillPool, MemoryPool},
    runtime_env::RuntimeEnvBuilder,
    TaskContext,
};
use tracing::warn;

use crate::error::WorkerRuntimeError;

const WORKER_DIR_PREFIX: &str = "worker-";
const WORKER_DIR_GENERATION_MARKER: &str = "-gen-";
const OWNERSHIP_MARKER: &str = ".pg_fusion_worker_spill";

/// Worker-owned DataFusion spill configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerSpillConfig {
    /// None keeps DataFusion on its default unbounded runtime.
    pub memory_limit_bytes: Option<usize>,
    /// Root directory that contains per-worker-incarnation spill directories.
    /// In PostgreSQL workers this is cluster-scoped below the configured base.
    pub root: PathBuf,
}

impl WorkerSpillConfig {
    pub fn new(memory_limit_bytes: Option<usize>, root: Option<PathBuf>) -> Self {
        Self {
            memory_limit_bytes,
            root: root.unwrap_or_else(default_spill_root),
        }
    }

    pub fn enabled(&self) -> bool {
        self.memory_limit_bytes.is_some()
    }

    pub fn with_cluster_namespace(mut self, cluster_id: impl AsRef<str>) -> Self {
        self.root = self.root.join(format!(
            "cluster-{}",
            safe_path_component(cluster_id.as_ref())
        ));
        self
    }
}

impl Default for WorkerSpillConfig {
    fn default() -> Self {
        Self::new(None, None)
    }
}

/// Worker-local spill directory lifecycle for one transport generation.
#[derive(Debug)]
pub struct WorkerSpillRuntime {
    config: WorkerSpillConfig,
    active_dir: Option<PathBuf>,
    memory_pool: Option<Arc<dyn MemoryPool>>,
    next_execution_serial: u64,
}

impl WorkerSpillRuntime {
    pub fn new(
        config: WorkerSpillConfig,
        worker_pid: i32,
        worker_generation: u64,
    ) -> Result<Self, WorkerRuntimeError> {
        if !config.enabled() {
            return Ok(Self {
                config,
                active_dir: None,
                memory_pool: None,
                next_execution_serial: 0,
            });
        }

        let memory_limit_bytes = config
            .memory_limit_bytes
            .expect("enabled worker spill must have a memory limit");
        let memory_pool = Arc::new(FairSpillPool::new(memory_limit_bytes));
        create_dir_all(&config.root, "create spill root")?;
        let active_dir = config
            .root
            .join(format!("worker-{worker_pid}-gen-{worker_generation}"));
        create_worker_incarnation_dir(&active_dir)?;
        write_ownership_marker(&active_dir)?;
        cleanup_stale_incarnations(&config.root, Some(&active_dir));

        Ok(Self {
            config,
            active_dir: Some(active_dir),
            memory_pool: Some(memory_pool),
            next_execution_serial: 0,
        })
    }

    pub fn config(&self) -> &WorkerSpillConfig {
        &self.config
    }

    pub fn active_dir(&self) -> Option<&Path> {
        self.active_dir.as_deref()
    }

    pub fn execution_dir(
        &mut self,
        peer: BackendLeaseSlot,
        session_epoch: u64,
    ) -> Result<ExecutionSpillDir, WorkerRuntimeError> {
        let Some(active_dir) = &self.active_dir else {
            return Ok(ExecutionSpillDir { path: None });
        };

        let serial = self.next_execution_serial;
        self.next_execution_serial = self.next_execution_serial.wrapping_add(1);
        let lease = peer.lease_id();
        let path = active_dir.join(format!(
            "exec-slot-{}-backendgen-{}-lease-{}-session-{session_epoch}-{serial}",
            peer.slot_id(),
            lease.generation(),
            lease.lease_epoch()
        ));
        create_dir_all(&path, "create execution spill directory")?;
        Ok(ExecutionSpillDir { path: Some(path) })
    }

    pub fn task_context(
        &self,
        spill_dir: &ExecutionSpillDir,
    ) -> Result<Arc<TaskContext>, WorkerRuntimeError> {
        let Some(memory_pool) = &self.memory_pool else {
            return Ok(Arc::new(TaskContext::default()));
        };
        let spill_path = spill_dir.path().ok_or_else(|| {
            WorkerRuntimeError::ProtocolViolation(
                "worker spill is enabled but execution spill directory was not created".into(),
            )
        })?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(memory_pool))
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_mode(DiskManagerMode::Directories(vec![spill_path.to_path_buf()])),
            )
            .build_arc()?;
        Ok(Arc::new(TaskContext::default().with_runtime(runtime)))
    }
}

impl Drop for WorkerSpillRuntime {
    fn drop(&mut self) {
        if let Some(path) = self.active_dir.take() {
            remove_dir_all_best_effort(&path, "remove worker spill incarnation");
        }
    }
}

/// RAII guard for one execution's DataFusion spill directory.
#[derive(Debug)]
pub struct ExecutionSpillDir {
    path: Option<PathBuf>,
}

impl ExecutionSpillDir {
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    pub fn cleanup(mut self) -> Result<(), WorkerRuntimeError> {
        if let Some(path) = self.path.take() {
            remove_dir_all(&path, "remove execution spill directory")?;
        }
        Ok(())
    }
}

impl Drop for ExecutionSpillDir {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            remove_dir_all_best_effort(&path, "remove execution spill directory");
        }
    }
}

fn default_spill_root() -> PathBuf {
    std::env::temp_dir().join("pg_fusion").join("spill")
}

fn cleanup_stale_incarnations(root: &Path, keep: Option<&Path>) {
    let entries = match std::fs::read_dir(root) {
        Ok(entries) => entries,
        Err(err) => {
            warn!(
                component = "worker",
                path = %root.display(),
                error = %err,
                "failed to read pg_fusion spill root for cleanup"
            );
            return;
        }
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) => {
                warn!(
                    component = "worker",
                    path = %root.display(),
                    error = %err,
                    "failed to read one pg_fusion spill directory entry"
                );
                continue;
            }
        };
        let path = entry.path();
        if keep.is_some_and(|keep| path == keep) {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            warn!(
                component = "worker",
                path = %path.display(),
                "failed to inspect pg_fusion spill directory entry"
            );
            continue;
        };
        if file_type.is_dir()
            && is_worker_incarnation_dir(&entry.file_name().to_string_lossy())
            && owns_worker_spill_dir(&path)
        {
            remove_dir_all_best_effort(&path, "remove stale worker spill incarnation");
        }
    }
}

fn is_worker_incarnation_dir(name: &str) -> bool {
    name.starts_with(WORKER_DIR_PREFIX) && name.contains(WORKER_DIR_GENERATION_MARKER)
}

fn owns_worker_spill_dir(path: &Path) -> bool {
    path.join(OWNERSHIP_MARKER).is_file()
}

fn safe_path_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "unknown".into()
    } else {
        out
    }
}

fn write_ownership_marker(path: &Path) -> Result<(), WorkerRuntimeError> {
    let marker = path.join(OWNERSHIP_MARKER);
    std::fs::write(&marker, b"pg_fusion worker spill\n").map_err(|source| {
        WorkerRuntimeError::SpillIo {
            action: "write spill ownership marker",
            path: marker,
            source,
        }
    })
}

fn create_worker_incarnation_dir(path: &Path) -> Result<(), WorkerRuntimeError> {
    match std::fs::create_dir(path) {
        Ok(()) => Ok(()),
        Err(err)
            if err.kind() == std::io::ErrorKind::AlreadyExists && owns_worker_spill_dir(path) =>
        {
            remove_dir_all(path, "remove stale current worker spill incarnation")?;
            std::fs::create_dir(path).map_err(|source| WorkerRuntimeError::SpillIo {
                action: "create worker spill incarnation",
                path: path.to_path_buf(),
                source,
            })
        }
        Err(source) => Err(WorkerRuntimeError::SpillIo {
            action: "create worker spill incarnation",
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn create_dir_all(path: &Path, action: &'static str) -> Result<(), WorkerRuntimeError> {
    std::fs::create_dir_all(path).map_err(|source| WorkerRuntimeError::SpillIo {
        action,
        path: path.to_path_buf(),
        source,
    })
}

fn remove_dir_all(path: &Path, action: &'static str) -> Result<(), WorkerRuntimeError> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(WorkerRuntimeError::SpillIo {
            action,
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn remove_dir_all_best_effort(path: &Path, action: &'static str) {
    if let Err(err) = remove_dir_all(path, action) {
        warn!(
            component = "worker",
            path = %path.display(),
            error = %err,
            "failed to clean pg_fusion spill directory"
        );
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use control_transport::BackendLeaseId;
    use datafusion_execution::memory_pool::MemoryConsumer;

    use super::*;

    #[test]
    fn disabled_runtime_does_not_create_root() {
        let root = unique_root("disabled");
        let config = WorkerSpillConfig::new(None, Some(root.clone()));

        let runtime = WorkerSpillRuntime::new(config, 42, 7).unwrap();

        assert!(runtime.active_dir().is_none());
        assert!(!root.exists());
    }

    #[test]
    fn disabled_runtime_does_not_delete_existing_worker_dirs() {
        let root = unique_root("disabled_existing");
        let stale = root.join("worker-1-gen-1");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::write(stale.join(OWNERSHIP_MARKER), b"old").unwrap();

        let config = WorkerSpillConfig::new(None, Some(root.clone()));
        let runtime = WorkerSpillRuntime::new(config, 42, 7).unwrap();

        assert!(runtime.active_dir().is_none());
        assert!(stale.exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn startup_cleanup_removes_stale_incarnations() {
        let root = unique_root("stale");
        let stale = root.join("worker-1-gen-1");
        let unrelated = root.join("not-a-worker-dir");
        std::fs::create_dir_all(&stale).unwrap();
        std::fs::create_dir_all(&unrelated).unwrap();
        std::fs::write(stale.join(OWNERSHIP_MARKER), b"old").unwrap();
        std::fs::write(stale.join("spill.bin"), b"old").unwrap();

        let config = WorkerSpillConfig::new(Some(1024), Some(root.clone()));
        let runtime = WorkerSpillRuntime::new(config, 2, 3).unwrap();
        let active = runtime.active_dir().unwrap().to_path_buf();

        assert!(!stale.exists());
        assert!(unrelated.exists());
        assert!(active.exists());
        assert!(active.join(OWNERSHIP_MARKER).exists());

        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn startup_cleanup_keeps_unmarked_matching_dirs() {
        let root = unique_root("unmarked");
        let unmarked = root.join("worker-1-gen-1");
        std::fs::create_dir_all(&unmarked).unwrap();
        std::fs::write(unmarked.join("file.bin"), b"not ours").unwrap();

        let config = WorkerSpillConfig::new(Some(1024), Some(root.clone()));
        let runtime = WorkerSpillRuntime::new(config, 2, 3).unwrap();

        assert!(unmarked.exists());

        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn startup_rejects_unmarked_active_incarnation() {
        let root = unique_root("unmarked_active");
        let active = root.join("worker-2-gen-3");
        std::fs::create_dir_all(&active).unwrap();
        std::fs::write(active.join("file.bin"), b"not ours").unwrap();

        let config = WorkerSpillConfig::new(Some(1024), Some(root.clone()));
        let err = WorkerSpillRuntime::new(config, 2, 3).unwrap_err();

        assert!(err
            .to_string()
            .contains("failed to create worker spill incarnation"));
        assert!(active.join("file.bin").exists());

        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn startup_replaces_marked_active_incarnation() {
        let root = unique_root("marked_active");
        let active = root.join("worker-2-gen-3");
        std::fs::create_dir_all(&active).unwrap();
        std::fs::write(active.join(OWNERSHIP_MARKER), b"old").unwrap();
        std::fs::write(active.join("spill.bin"), b"old").unwrap();

        let config = WorkerSpillConfig::new(Some(1024), Some(root.clone()));
        let runtime = WorkerSpillRuntime::new(config, 2, 3).unwrap();

        assert!(runtime.active_dir().unwrap().exists());
        assert!(active.join(OWNERSHIP_MARKER).exists());
        assert!(!active.join("spill.bin").exists());

        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn cluster_namespace_scopes_root() {
        let root = unique_root("namespace");
        let config =
            WorkerSpillConfig::new(Some(1024), Some(root.clone())).with_cluster_namespace("a/b:c");

        assert_eq!(config.root, root.join("cluster-a_b_c"));
    }

    #[test]
    fn execution_dir_cleanup_removes_execution_directory() {
        let root = unique_root("exec");
        let config = WorkerSpillConfig::new(Some(1024), Some(root.clone()));
        let mut runtime = WorkerSpillRuntime::new(config, 5, 8).unwrap();
        let peer = BackendLeaseSlot::new(3, BackendLeaseId::new(9, 10));

        let dir = runtime.execution_dir(peer, 11).unwrap();
        let path = dir.path().unwrap().to_path_buf();
        assert!(path.exists());

        dir.cleanup().unwrap();
        assert!(!path.exists());

        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn task_context_uses_execution_spill_directory() {
        let root = unique_root("task_context");
        let config = WorkerSpillConfig::new(Some(1024 * 1024), Some(root.clone()));
        let mut runtime = WorkerSpillRuntime::new(config, 6, 9).unwrap();
        let peer = BackendLeaseSlot::new(4, BackendLeaseId::new(10, 11));
        let dir = runtime.execution_dir(peer, 12).unwrap();
        let dir_path = dir.path().unwrap().to_path_buf();

        let ctx = runtime.task_context(&dir).unwrap();
        let file = ctx
            .runtime_env()
            .disk_manager
            .create_tmp_file("spill test")
            .unwrap();

        assert!(file.path().starts_with(&dir_path));

        drop(file);
        dir.cleanup().unwrap();
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn task_contexts_share_worker_memory_pool() {
        let root = unique_root("shared_pool");
        let config = WorkerSpillConfig::new(Some(1000), Some(root.clone()));
        let mut runtime = WorkerSpillRuntime::new(config, 6, 9).unwrap();
        let first_peer = BackendLeaseSlot::new(4, BackendLeaseId::new(10, 11));
        let second_peer = BackendLeaseSlot::new(5, BackendLeaseId::new(10, 12));
        let first_dir = runtime.execution_dir(first_peer, 12).unwrap();
        let second_dir = runtime.execution_dir(second_peer, 13).unwrap();
        let first_ctx = runtime.task_context(&first_dir).unwrap();
        let second_ctx = runtime.task_context(&second_dir).unwrap();
        let first_pool = first_ctx.runtime_env().memory_pool.clone();
        let second_pool = second_ctx.runtime_env().memory_pool.clone();

        let first_reservation = MemoryConsumer::new("first").register(&first_pool);
        first_reservation.try_resize(700).unwrap();
        assert_eq!(first_pool.reserved(), 700);
        assert_eq!(second_pool.reserved(), 700);

        let second_reservation = MemoryConsumer::new("second").register(&second_pool);
        assert!(second_reservation.try_resize(400).is_err());
        second_reservation.try_resize(300).unwrap();
        assert_eq!(first_pool.reserved(), 1000);
        assert_eq!(second_pool.reserved(), 1000);

        drop(second_reservation);
        drop(first_reservation);
        assert_eq!(first_pool.reserved(), 0);
        assert_eq!(second_pool.reserved(), 0);

        first_dir.cleanup().unwrap();
        second_dir.cleanup().unwrap();
        drop(runtime);
        let _ = std::fs::remove_dir_all(root);
    }

    fn unique_root(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "pg_fusion_spill_test_{name}_{}_{}",
            std::process::id(),
            nanos
        ))
    }
}
