use crab_core::{CrabError, CrabResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexProcessHandle {
    pub process_id: u32,
    pub started_at_epoch_ms: u64,
}

pub trait CodexAppServerProcess: Send + Sync {
    fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle>;
    fn is_healthy(&self, handle: &CodexProcessHandle) -> bool;
    fn terminate_app_server(&self, handle: &CodexProcessHandle) -> CrabResult<()>;
}

pub trait CodexLifecycleManager {
    fn ensure_started(&mut self) -> CrabResult<CodexProcessHandle>;
    fn restart(&mut self) -> CrabResult<CodexProcessHandle>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexManagerState {
    pub generation: u64,
    pub restart_count: u64,
    pub active_process_id: Option<u32>,
    pub active_started_at_epoch_ms: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct CodexManager<P: CodexAppServerProcess> {
    process: P,
    active_handle: Option<CodexProcessHandle>,
    generation: u64,
    restart_count: u64,
}

impl<P: CodexAppServerProcess> CodexManager<P> {
    #[must_use]
    pub fn new(process: P) -> Self {
        Self {
            process,
            active_handle: None,
            generation: 0,
            restart_count: 0,
        }
    }

    pub fn ensure_started(&mut self) -> CrabResult<CodexProcessHandle> {
        match self.active_handle.clone() {
            Some(handle) if self.process.is_healthy(&handle) => Ok(handle),
            Some(_) => self.restart(),
            None => self.spawn_new("codex_manager_start"),
        }
    }

    pub fn restart(&mut self) -> CrabResult<CodexProcessHandle> {
        if let Some(current) = self.active_handle.as_ref() {
            self.process.terminate_app_server(current)?;
            self.restart_count += 1;
        }
        self.active_handle = None;
        self.spawn_new("codex_manager_restart")
    }

    pub fn stop(&mut self) -> CrabResult<()> {
        if let Some(current) = self.active_handle.as_ref() {
            self.process.terminate_app_server(current)?;
            self.active_handle = None;
        }
        Ok(())
    }

    #[must_use]
    pub fn state(&self) -> CodexManagerState {
        CodexManagerState {
            generation: self.generation,
            restart_count: self.restart_count,
            active_process_id: self.active_handle.as_ref().map(|handle| handle.process_id),
            active_started_at_epoch_ms: self
                .active_handle
                .as_ref()
                .map(|handle| handle.started_at_epoch_ms),
        }
    }

    fn spawn_new(&mut self, context: &'static str) -> CrabResult<CodexProcessHandle> {
        let handle = self.process.spawn_app_server()?;
        validate_handle(context, &handle)?;
        self.generation += 1;
        self.active_handle = Some(handle.clone());
        Ok(handle)
    }
}

impl<P: CodexAppServerProcess> CodexLifecycleManager for CodexManager<P> {
    fn ensure_started(&mut self) -> CrabResult<CodexProcessHandle> {
        CodexManager::ensure_started(self)
    }

    fn restart(&mut self) -> CrabResult<CodexProcessHandle> {
        CodexManager::restart(self)
    }
}

fn validate_handle(context: &'static str, handle: &CodexProcessHandle) -> CrabResult<()> {
    validate_process_identity(context, handle.process_id, handle.started_at_epoch_ms)
}

pub(crate) fn validate_process_identity(
    context: &'static str,
    process_id: u32,
    started_at_epoch_ms: u64,
) -> CrabResult<()> {
    if process_id == 0 {
        return Err(CrabError::InvariantViolation {
            context,
            message: "process_id must be non-zero".to_string(),
        });
    }
    if started_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context,
            message: "started_at_epoch_ms must be non-zero".to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use crab_core::{CrabError, CrabResult};

    use super::{
        validate_process_identity, CodexAppServerProcess, CodexLifecycleManager, CodexManager,
        CodexManagerState, CodexProcessHandle,
    };

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct FakeProcessStats {
        spawn_calls: usize,
        terminate_calls: usize,
        last_terminated_pid: Option<u32>,
    }

    #[derive(Debug, Clone)]
    struct FakeCodexProcess {
        state: Arc<Mutex<FakeCodexProcessState>>,
    }

    #[derive(Debug, Clone)]
    struct FakeCodexProcessState {
        scripted_spawns: VecDeque<CrabResult<CodexProcessHandle>>,
        health: BTreeMap<u32, bool>,
        terminate_error: Option<CrabError>,
        stats: FakeProcessStats,
    }

    impl FakeCodexProcess {
        fn with_scripted_spawns(spawns: Vec<CrabResult<CodexProcessHandle>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(FakeCodexProcessState {
                    scripted_spawns: VecDeque::from(spawns),
                    health: BTreeMap::new(),
                    terminate_error: None,
                    stats: FakeProcessStats::default(),
                })),
            }
        }

        fn stats(&self) -> FakeProcessStats {
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
                .health
                .insert(process_id, healthy);
        }

        fn set_terminate_error(&self, error: CrabError) {
            self.state
                .lock()
                .expect("lock should succeed")
                .terminate_error = Some(error);
        }
    }

    impl CodexAppServerProcess for FakeCodexProcess {
        fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.spawn_calls += 1;
            let next =
                state
                    .scripted_spawns
                    .pop_front()
                    .ok_or_else(|| CrabError::InvariantViolation {
                        context: "fake_codex_spawn",
                        message: "missing scripted spawn".to_string(),
                    })?;
            if let Ok(handle) = &next {
                state.health.entry(handle.process_id).or_insert(true);
            }
            next
        }

        fn is_healthy(&self, handle: &CodexProcessHandle) -> bool {
            self.state
                .lock()
                .expect("lock should succeed")
                .health
                .get(&handle.process_id)
                .copied()
                .unwrap_or(false)
        }

        fn terminate_app_server(&self, handle: &CodexProcessHandle) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.terminate_calls += 1;
            state.stats.last_terminated_pid = Some(handle.process_id);
            if let Some(error) = state.terminate_error.clone() {
                return Err(error);
            }
            state.health.insert(handle.process_id, false);
            Ok(())
        }
    }

    fn handle(process_id: u32, started_at_epoch_ms: u64) -> CodexProcessHandle {
        CodexProcessHandle {
            process_id,
            started_at_epoch_ms,
        }
    }

    fn assert_state(
        state: CodexManagerState,
        generation: u64,
        restart_count: u64,
        pid: Option<u32>,
    ) {
        assert_eq!(state.generation, generation);
        assert_eq!(state.restart_count, restart_count);
        assert_eq!(state.active_process_id, pid);
    }

    fn assert_start_error(process: FakeCodexProcess, expected: CrabError) {
        let mut manager = CodexManager::new(process);
        let err = manager
            .ensure_started()
            .expect_err("start should return expected error");
        assert_eq!(err, expected);
    }

    fn terminate_boom_error() -> CrabError {
        CrabError::InvariantViolation {
            context: "fake_codex_terminate",
            message: "boom".to_string(),
        }
    }

    fn started_manager(
        process_id: u32,
        started_at_epoch_ms: u64,
    ) -> (CodexManager<FakeCodexProcess>, FakeCodexProcess) {
        let process = FakeCodexProcess::with_scripted_spawns(vec![Ok(handle(
            process_id,
            started_at_epoch_ms,
        ))]);
        let mut manager = CodexManager::new(process.clone());
        manager
            .ensure_started()
            .expect("initial start should succeed");
        (manager, process)
    }

    #[test]
    fn ensure_started_spawns_once_and_reuses_healthy_process() {
        let process = FakeCodexProcess::with_scripted_spawns(vec![Ok(handle(101, 10))]);
        let mut manager = CodexManager::new(process.clone());

        let first = manager
            .ensure_started()
            .expect("first start should succeed");
        let second = manager
            .ensure_started()
            .expect("second ensure should reuse handle");

        assert_eq!(first, second);
        assert_state(manager.state(), 1, 0, Some(101));
        assert_eq!(
            process.stats(),
            FakeProcessStats {
                spawn_calls: 1,
                terminate_calls: 0,
                last_terminated_pid: None,
            }
        );
    }

    #[test]
    fn ensure_started_restarts_when_process_becomes_unhealthy() {
        let process =
            FakeCodexProcess::with_scripted_spawns(vec![Ok(handle(101, 10)), Ok(handle(202, 20))]);
        let mut manager = CodexManager::new(process.clone());
        let first = manager
            .ensure_started()
            .expect("initial start should succeed");
        process.set_health(first.process_id, false);

        let restarted = manager
            .ensure_started()
            .expect("unhealthy process should restart");
        assert_eq!(restarted.process_id, 202);
        assert_state(manager.state(), 2, 1, Some(202));
        assert_eq!(
            process.stats(),
            FakeProcessStats {
                spawn_calls: 2,
                terminate_calls: 1,
                last_terminated_pid: Some(101),
            }
        );
    }

    #[test]
    fn restart_without_running_process_starts_cleanly() {
        let process = FakeCodexProcess::with_scripted_spawns(vec![Ok(handle(303, 30))]);
        let mut manager = CodexManager::new(process.clone());

        let started = manager.restart().expect("restart should spawn when idle");
        assert_eq!(started.process_id, 303);
        assert_state(manager.state(), 1, 0, Some(303));
        assert_eq!(
            process.stats(),
            FakeProcessStats {
                spawn_calls: 1,
                terminate_calls: 0,
                last_terminated_pid: None,
            }
        );
    }

    #[test]
    fn restart_propagates_terminate_error() {
        let (mut manager, process) = started_manager(404, 40);
        process.set_terminate_error(terminate_boom_error());

        let err = manager
            .restart()
            .expect_err("terminate failure should propagate");
        assert_eq!(err, terminate_boom_error());
    }

    #[test]
    fn stop_terminates_active_process_and_is_idempotent() {
        let process = FakeCodexProcess::with_scripted_spawns(vec![Ok(handle(505, 50))]);
        let mut manager = CodexManager::new(process.clone());
        manager
            .ensure_started()
            .expect("initial start should succeed");

        manager
            .stop()
            .expect("stop should terminate active process");
        assert_state(manager.state(), 1, 0, None);

        manager.stop().expect("second stop should no-op");
        assert_eq!(
            process.stats(),
            FakeProcessStats {
                spawn_calls: 1,
                terminate_calls: 1,
                last_terminated_pid: Some(505),
            }
        );
    }

    #[test]
    fn stop_propagates_terminate_error() {
        let (mut manager, process) = started_manager(606, 60);
        process.set_terminate_error(terminate_boom_error());

        let err = manager
            .stop()
            .expect_err("stop should propagate terminate errors");
        assert_eq!(err, terminate_boom_error());
        assert_state(manager.state(), 1, 0, Some(606));
    }

    #[test]
    fn manager_rejects_invalid_spawned_handle_shape() {
        let mut cases = vec![
            (
                handle(0, 10),
                "codex_manager_start",
                "process_id must be non-zero",
            ),
            (
                handle(101, 0),
                "codex_manager_start",
                "started_at_epoch_ms must be non-zero",
            ),
        ];
        for (invalid_handle, context, message) in cases.drain(..) {
            let process = FakeCodexProcess::with_scripted_spawns(vec![Ok(invalid_handle)]);
            let mut manager = CodexManager::new(process);
            let err = manager
                .ensure_started()
                .expect_err("invalid spawned handle should fail validation");
            assert_eq!(
                err,
                CrabError::InvariantViolation {
                    context,
                    message: message.to_string(),
                }
            );
        }
    }

    #[test]
    fn manager_propagates_spawn_errors() {
        let process =
            FakeCodexProcess::with_scripted_spawns(vec![Err(CrabError::InvariantViolation {
                context: "fake_codex_spawn",
                message: "boom".to_string(),
            })]);
        assert_start_error(
            process,
            CrabError::InvariantViolation {
                context: "fake_codex_spawn",
                message: "boom".to_string(),
            },
        );
    }

    #[test]
    fn manager_errors_when_spawn_script_is_exhausted() {
        let process = FakeCodexProcess::with_scripted_spawns(vec![]);
        assert_start_error(
            process,
            CrabError::InvariantViolation {
                context: "fake_codex_spawn",
                message: "missing scripted spawn".to_string(),
            },
        );
    }

    #[test]
    fn fake_process_marks_unknown_handle_unhealthy() {
        let process = FakeCodexProcess::with_scripted_spawns(vec![]);
        assert!(!process.is_healthy(&handle(999, 1)));
    }

    #[test]
    fn lifecycle_trait_delegates_to_manager_paths() {
        let process = FakeCodexProcess::with_scripted_spawns(vec![
            Ok(handle(101, 10)),
            Ok(handle(202, 20)),
            Ok(handle(303, 30)),
        ]);
        process.set_health(101, false);
        let mut manager = CodexManager::new(process.clone());
        let lifecycle: &mut dyn CodexLifecycleManager = &mut manager;

        let first = lifecycle
            .ensure_started()
            .expect("trait ensure_started should start manager");
        assert_eq!(first.process_id, 101);

        let restarted_via_ensure = lifecycle
            .ensure_started()
            .expect("unhealthy handle should restart via trait");
        assert_eq!(restarted_via_ensure.process_id, 202);

        let restarted = lifecycle
            .restart()
            .expect("trait restart should delegate to manager restart");
        assert_eq!(restarted.process_id, 303);
    }

    #[test]
    fn validate_process_identity_rejects_zero_fields() {
        let zero_pid =
            validate_process_identity("codex_validate_identity", 0, 1).expect_err("pid=0 fails");
        assert_eq!(
            zero_pid,
            CrabError::InvariantViolation {
                context: "codex_validate_identity",
                message: "process_id must be non-zero".to_string(),
            }
        );

        let zero_started = validate_process_identity("codex_validate_identity", 1, 0)
            .expect_err("started_at_epoch_ms=0 fails");
        assert_eq!(
            zero_started,
            CrabError::InvariantViolation {
                context: "codex_validate_identity",
                message: "started_at_epoch_ms must be non-zero".to_string(),
            }
        );
    }
}
