use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crab_core::{CrabError, CrabResult};

use crate::{
    codex::validate_process_identity, ensure_non_empty_field, CodexAppServerProcess, CodexManager,
    CodexProcessHandle,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeServerHandle {
    pub process_id: u32,
    pub started_at_epoch_ms: u64,
    pub server_base_url: String,
}

pub trait OpenCodeServerProcess: Send + Sync {
    fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle>;
    fn is_server_healthy(&self, handle: &OpenCodeServerHandle) -> bool;
    fn terminate_server(&self, handle: &OpenCodeServerHandle) -> CrabResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeManagerState {
    pub generation: u64,
    pub restart_count: u64,
    pub active_process_id: Option<u32>,
    pub active_started_at_epoch_ms: Option<u64>,
    pub active_server_base_url: Option<String>,
}

#[derive(Debug)]
struct OpenCodeProcessBridge<P: OpenCodeServerProcess> {
    process: Arc<P>,
    handles_by_pid: Arc<Mutex<BTreeMap<u32, OpenCodeServerHandle>>>,
}

impl<P: OpenCodeServerProcess> Clone for OpenCodeProcessBridge<P> {
    fn clone(&self) -> Self {
        Self {
            process: self.process.clone(),
            handles_by_pid: self.handles_by_pid.clone(),
        }
    }
}

impl<P: OpenCodeServerProcess> OpenCodeProcessBridge<P> {
    fn new(process: P) -> Self {
        Self {
            process: Arc::new(process),
            handles_by_pid: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    fn validate_handle(context: &'static str, handle: &OpenCodeServerHandle) -> CrabResult<()> {
        validate_process_identity(context, handle.process_id, handle.started_at_epoch_ms)?;
        ensure_non_empty_field(context, "server_base_url", &handle.server_base_url)
    }

    fn active_handle(&self, process_id: u32) -> Option<OpenCodeServerHandle> {
        self.handles_by_pid
            .lock()
            .expect("lock should succeed")
            .get(&process_id)
            .cloned()
    }

    fn require_active_handle(
        &self,
        context: &'static str,
        process_id: u32,
    ) -> CrabResult<OpenCodeServerHandle> {
        self.active_handle(process_id)
            .ok_or_else(|| CrabError::InvariantViolation {
                context,
                message: format!("missing OpenCode handle for process_id {process_id}"),
            })
    }
}

impl<P: OpenCodeServerProcess> CodexAppServerProcess for OpenCodeProcessBridge<P> {
    fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle> {
        let handle = self.process.spawn_server()?;
        Self::validate_handle("opencode_manager_start", &handle)?;
        self.handles_by_pid
            .lock()
            .expect("lock should succeed")
            .insert(handle.process_id, handle.clone());
        Ok(CodexProcessHandle {
            process_id: handle.process_id,
            started_at_epoch_ms: handle.started_at_epoch_ms,
        })
    }

    fn is_healthy(&self, handle: &CodexProcessHandle) -> bool {
        match self.active_handle(handle.process_id) {
            Some(opencode_handle) => self.process.is_server_healthy(&opencode_handle),
            None => false,
        }
    }

    fn terminate_app_server(&self, handle: &CodexProcessHandle) -> CrabResult<()> {
        let opencode_handle =
            self.require_active_handle("opencode_manager_terminate", handle.process_id)?;
        self.process.terminate_server(&opencode_handle)?;
        self.handles_by_pid
            .lock()
            .expect("lock should succeed")
            .remove(&handle.process_id);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OpenCodeManager<P: OpenCodeServerProcess> {
    lifecycle: CodexManager<OpenCodeProcessBridge<P>>,
    bridge: OpenCodeProcessBridge<P>,
}

impl<P: OpenCodeServerProcess> OpenCodeManager<P> {
    #[must_use]
    pub fn new(process: P) -> Self {
        let bridge = OpenCodeProcessBridge::new(process);
        let lifecycle = CodexManager::new(bridge.clone());
        Self { lifecycle, bridge }
    }

    pub fn ensure_running(&mut self) -> CrabResult<OpenCodeServerHandle> {
        let handle = self.lifecycle.ensure_started()?;
        self.bridge
            .require_active_handle("opencode_manager_ensure_running", handle.process_id)
    }

    pub fn restart(&mut self) -> CrabResult<OpenCodeServerHandle> {
        let handle = self.lifecycle.restart()?;
        self.bridge
            .require_active_handle("opencode_manager_restart", handle.process_id)
    }

    pub fn stop(&mut self) -> CrabResult<()> {
        self.lifecycle.stop()
    }

    #[must_use]
    pub fn state(&self) -> OpenCodeManagerState {
        let inner = self.lifecycle.state();
        OpenCodeManagerState {
            generation: inner.generation,
            restart_count: inner.restart_count,
            active_process_id: inner.active_process_id,
            active_started_at_epoch_ms: inner.active_started_at_epoch_ms,
            active_server_base_url: inner
                .active_process_id
                .and_then(|process_id| self.bridge.active_handle(process_id))
                .map(|handle| handle.server_base_url),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use super::{
        OpenCodeManager, OpenCodeManagerState, OpenCodeProcessBridge, OpenCodeServerHandle,
        OpenCodeServerProcess,
    };
    use crate::{CodexAppServerProcess, CodexProcessHandle};
    use crab_core::{CrabError, CrabResult};

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct FakeServerStats {
        spawn_calls: usize,
        terminate_calls: usize,
        terminated_pid: Option<u32>,
    }

    #[derive(Debug, Clone)]
    struct FakeServer {
        state: Arc<Mutex<FakeServerState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeServerState {
        scripted_starts: VecDeque<CrabResult<OpenCodeServerHandle>>,
        health_by_pid: BTreeMap<u32, bool>,
        terminate_error: Option<CrabError>,
        stats: FakeServerStats,
    }

    impl FakeServer {
        fn from_starts(scripted_starts: Vec<CrabResult<OpenCodeServerHandle>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeServerState {
                    scripted_starts: VecDeque::from(scripted_starts),
                    health_by_pid: BTreeMap::new(),
                    terminate_error: None,
                    stats: FakeServerStats::default(),
                })),
            }
        }

        fn stats(&self) -> FakeServerStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }

        fn set_health(&self, process_id: u32, healthy: bool) {
            self.state
                .lock()
                .expect("lock should succeed")
                .health_by_pid
                .insert(process_id, healthy);
        }

        fn set_terminate_error(&self, error: CrabError) {
            self.state
                .lock()
                .expect("lock should succeed")
                .terminate_error = Some(error);
        }
    }

    impl OpenCodeServerProcess for FakeServer {
        fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle> {
            let mut guard = self.state.lock().expect("lock should succeed");
            guard.stats.spawn_calls += 1;
            let next =
                guard
                    .scripted_starts
                    .pop_front()
                    .ok_or_else(|| CrabError::InvariantViolation {
                        context: "fake_opencode_spawn",
                        message: "missing scripted server start".to_string(),
                    })?;
            if let Ok(handle) = &next {
                guard.health_by_pid.entry(handle.process_id).or_insert(true);
            }
            next
        }

        fn is_server_healthy(&self, handle: &OpenCodeServerHandle) -> bool {
            self.state
                .lock()
                .expect("lock should succeed")
                .health_by_pid
                .get(&handle.process_id)
                .copied()
                .unwrap_or(false)
        }

        fn terminate_server(&self, handle: &OpenCodeServerHandle) -> CrabResult<()> {
            let mut guard = self.state.lock().expect("lock should succeed");
            guard.stats.terminate_calls += 1;
            guard.stats.terminated_pid = Some(handle.process_id);
            if let Some(error) = guard.terminate_error.clone() {
                return Err(error);
            }
            guard.health_by_pid.insert(handle.process_id, false);
            Ok(())
        }
    }

    fn handle(process_id: u32, started_at_epoch_ms: u64, url: &str) -> OpenCodeServerHandle {
        OpenCodeServerHandle {
            process_id,
            started_at_epoch_ms,
            server_base_url: url.to_string(),
        }
    }

    fn assert_state(
        state: OpenCodeManagerState,
        generation: u64,
        restart_count: u64,
        active_pid: Option<u32>,
        active_url: Option<&str>,
    ) {
        assert_eq!(state.generation, generation);
        assert_eq!(state.restart_count, restart_count);
        assert_eq!(state.active_process_id, active_pid);
        assert_eq!(state.active_server_base_url, active_url.map(str::to_string));
    }

    #[test]
    fn ensure_running_starts_once_and_reuses_healthy_server() {
        let fake = FakeServer::from_starts(vec![Ok(handle(11, 100, "http://127.0.0.1:7321"))]);
        let mut manager = OpenCodeManager::new(fake.clone());

        let first = manager
            .ensure_running()
            .expect("first ensure should start server");
        let second = manager
            .ensure_running()
            .expect("healthy server should be reused");

        assert_eq!(first, second);
        assert_state(
            manager.state(),
            1,
            0,
            Some(11),
            Some("http://127.0.0.1:7321"),
        );
        assert_eq!(
            fake.stats(),
            FakeServerStats {
                spawn_calls: 1,
                terminate_calls: 0,
                terminated_pid: None,
            }
        );
    }

    #[test]
    fn ensure_running_restarts_when_health_check_fails() {
        let fake = FakeServer::from_starts(vec![
            Ok(handle(21, 200, "http://127.0.0.1:8123")),
            Ok(handle(22, 300, "http://127.0.0.1:8124")),
        ]);
        let mut manager = OpenCodeManager::new(fake.clone());

        let first = manager
            .ensure_running()
            .expect("initial start should succeed");
        fake.set_health(first.process_id, false);
        let restarted = manager
            .ensure_running()
            .expect("unhealthy server should be restarted");

        assert_eq!(restarted.process_id, 22);
        assert_state(
            manager.state(),
            2,
            1,
            Some(22),
            Some("http://127.0.0.1:8124"),
        );
        assert_eq!(fake.stats().terminate_calls, 1);
    }

    #[test]
    fn restart_and_stop_have_expected_lifecycle_behavior() {
        let fake = FakeServer::from_starts(vec![Ok(handle(31, 400, "http://127.0.0.1:9000"))]);
        let mut manager = OpenCodeManager::new(fake.clone());

        let started = manager.restart().expect("restart while idle should start");
        assert_eq!(started.process_id, 31);
        assert_state(
            manager.state(),
            1,
            0,
            Some(31),
            Some("http://127.0.0.1:9000"),
        );

        manager.stop().expect("stop should terminate active server");
        manager.stop().expect("stop should be idempotent");
        assert_state(manager.state(), 1, 0, None, None);
        assert_eq!(fake.stats().terminate_calls, 1);
    }

    #[test]
    fn restart_propagates_terminate_error() {
        let fake = FakeServer::from_starts(vec![Ok(handle(41, 500, "http://127.0.0.1:9200"))]);
        let mut manager = OpenCodeManager::new(fake.clone());
        manager
            .ensure_running()
            .expect("setup start should succeed for restart error case");

        let error = CrabError::InvariantViolation {
            context: "fake_opencode_terminate",
            message: "boom".to_string(),
        };
        fake.set_terminate_error(error.clone());

        let restart_error = manager
            .restart()
            .expect_err("terminate failure should propagate");
        assert_eq!(restart_error, error);
    }

    #[test]
    fn startup_validation_rejects_invalid_handles() {
        let invalid = vec![
            (
                handle(0, 10, "http://127.0.0.1:1111"),
                CrabError::InvariantViolation {
                    context: "opencode_manager_start",
                    message: "process_id must be non-zero".to_string(),
                },
            ),
            (
                handle(1, 0, "http://127.0.0.1:1111"),
                CrabError::InvariantViolation {
                    context: "opencode_manager_start",
                    message: "started_at_epoch_ms must be non-zero".to_string(),
                },
            ),
            (
                handle(2, 10, " "),
                CrabError::InvariantViolation {
                    context: "opencode_manager_start",
                    message: "server_base_url must not be empty".to_string(),
                },
            ),
        ];

        for (bad_handle, expected) in invalid {
            let fake = FakeServer::from_starts(vec![Ok(bad_handle)]);
            let mut manager = OpenCodeManager::new(fake);
            let start_error = manager
                .ensure_running()
                .expect_err("invalid handle should fail startup");
            assert_eq!(start_error, expected);
        }
    }

    #[test]
    fn propagate_spawn_error_and_state_without_active_server() {
        let spawn_error = CrabError::InvariantViolation {
            context: "fake_opencode_spawn",
            message: "boom".to_string(),
        };
        let fake = FakeServer::from_starts(vec![Err(spawn_error.clone())]);
        let mut manager = OpenCodeManager::new(fake);

        let error = manager
            .ensure_running()
            .expect_err("scripted spawn failure should propagate");
        assert_eq!(error, spawn_error);
        assert_state(manager.state(), 0, 0, None, None);
    }

    #[test]
    fn missing_scripted_start_is_actionable() {
        let fake = FakeServer::from_starts(Vec::new());
        let mut manager = OpenCodeManager::new(fake);

        let error = manager
            .ensure_running()
            .expect_err("missing scripted start should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "fake_opencode_spawn",
                message: "missing scripted server start".to_string(),
            }
        );
    }

    #[test]
    fn bridge_missing_handle_paths_are_actionable() {
        let bridge = OpenCodeProcessBridge::new(FakeServer::from_starts(Vec::new()));
        let unknown = CodexProcessHandle {
            process_id: 777,
            started_at_epoch_ms: 1,
        };

        assert!(!CodexAppServerProcess::is_healthy(&bridge, &unknown));
        let error = CodexAppServerProcess::terminate_app_server(&bridge, &unknown)
            .expect_err("terminating unknown process should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "opencode_manager_terminate",
                message: "missing OpenCode handle for process_id 777".to_string(),
            }
        );
    }
}
