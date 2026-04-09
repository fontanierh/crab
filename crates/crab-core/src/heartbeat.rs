use crate::{BackendKind, CrabError, CrabResult, LaneState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatPolicy {
    pub run_stall_timeout_secs: u64,
    pub backend_stall_timeout_secs: u64,
    pub dispatcher_stall_timeout_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveRunHeartbeat {
    pub logical_session_id: String,
    pub run_id: String,
    pub lane_state: LaneState,
    pub backend: BackendKind,
    pub last_progress_at_epoch_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendHeartbeat {
    pub backend: BackendKind,
    pub is_persistent: bool,
    pub is_healthy: bool,
    pub last_healthy_at_epoch_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DispatcherHeartbeat {
    pub queued_run_count: usize,
    pub active_lane_count: usize,
    pub last_dispatch_at_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatRunAction {
    pub logical_session_id: String,
    pub run_id: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatComponent {
    Run,
    Backend,
    Dispatcher,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatEvent {
    pub component: HeartbeatComponent,
    pub action: String,
    pub logical_session_id: Option<String>,
    pub run_id: Option<String>,
    pub backend: Option<BackendKind>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatOutcome {
    pub cancelled_runs: Vec<HeartbeatRunAction>,
    pub hard_stopped_runs: Vec<HeartbeatRunAction>,
    pub restarted_backends: Vec<BackendKind>,
    pub dispatcher_nudged: bool,
    pub events: Vec<HeartbeatEvent>,
}

pub trait HeartbeatRuntime {
    fn list_active_runs(&self) -> CrabResult<Vec<ActiveRunHeartbeat>>;
    fn request_cancel_active_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<()>;
    fn hard_stop_run_and_rotate(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
        reason: &str,
    ) -> CrabResult<()>;
    fn list_backend_heartbeats(&self) -> CrabResult<Vec<BackendHeartbeat>>;
    fn restart_backend_manager(&mut self, backend: BackendKind) -> CrabResult<()>;
    fn dispatcher_heartbeat(&self) -> CrabResult<DispatcherHeartbeat>;
    fn nudge_dispatcher(&mut self) -> CrabResult<()>;
}

pub fn execute_heartbeat_cycle<R: HeartbeatRuntime>(
    runtime: &mut R,
    policy: &HeartbeatPolicy,
    now_epoch_ms: u64,
) -> CrabResult<HeartbeatOutcome> {
    validate_heartbeat_inputs(policy, now_epoch_ms)?;
    let run_stall_timeout_ms =
        checked_timeout_ms("CRAB_RUN_STALL_TIMEOUT_SECS", policy.run_stall_timeout_secs)?;
    let backend_stall_timeout_ms = checked_timeout_ms(
        "CRAB_BACKEND_STALL_TIMEOUT_SECS",
        policy.backend_stall_timeout_secs,
    )?;
    let dispatcher_stall_timeout_ms = checked_timeout_ms(
        "CRAB_DISPATCHER_STALL_TIMEOUT_SECS",
        policy.dispatcher_stall_timeout_secs,
    )?;

    let mut outcome = HeartbeatOutcome {
        cancelled_runs: Vec::new(),
        hard_stopped_runs: Vec::new(),
        restarted_backends: Vec::new(),
        dispatcher_nudged: false,
        events: Vec::new(),
    };

    let mut active_runs = runtime.list_active_runs()?;
    active_runs.sort_by(|left, right| {
        (left.logical_session_id.as_str(), left.run_id.as_str())
            .cmp(&(right.logical_session_id.as_str(), right.run_id.as_str()))
    });

    for run in active_runs {
        validate_active_run_heartbeat(&run, now_epoch_ms)?;
        if !matches!(run.lane_state, LaneState::Running | LaneState::Cancelling) {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_cycle",
                message: format!(
                    "active run {}/{} has non-active lane state {:?}",
                    run.logical_session_id, run.run_id, run.lane_state
                ),
            });
        }

        let elapsed_since_progress_ms = now_epoch_ms.saturating_sub(run.last_progress_at_epoch_ms);
        if elapsed_since_progress_ms < run_stall_timeout_ms {
            continue;
        }

        let stalled_reason = "stalled_no_progress".to_string();
        match runtime.request_cancel_active_run(&run.logical_session_id, &run.run_id) {
            Ok(()) => {
                outcome.cancelled_runs.push(HeartbeatRunAction {
                    logical_session_id: run.logical_session_id.clone(),
                    run_id: run.run_id.clone(),
                    reason: stalled_reason.clone(),
                });
                outcome.events.push(HeartbeatEvent {
                    component: HeartbeatComponent::Run,
                    action: "cancel_requested_due_to_stall".to_string(),
                    logical_session_id: Some(run.logical_session_id),
                    run_id: Some(run.run_id),
                    backend: None,
                });
            }
            Err(cancel_error) => {
                let hard_stop_reason = format!("cancel_request_failed:{cancel_error}");
                runtime.hard_stop_run_and_rotate(
                    &run.logical_session_id,
                    &run.run_id,
                    &hard_stop_reason,
                )?;
                outcome.hard_stopped_runs.push(HeartbeatRunAction {
                    logical_session_id: run.logical_session_id.clone(),
                    run_id: run.run_id.clone(),
                    reason: hard_stop_reason,
                });
                outcome.events.push(HeartbeatEvent {
                    component: HeartbeatComponent::Run,
                    action: "hard_stop_and_rotate_due_to_cancel_failure".to_string(),
                    logical_session_id: Some(run.logical_session_id),
                    run_id: Some(run.run_id),
                    backend: None,
                });
            }
        }
    }

    let backend_heartbeats = runtime.list_backend_heartbeats()?;
    for backend_heartbeat in backend_heartbeats {
        validate_backend_heartbeat(backend_heartbeat, now_epoch_ms)?;
        if !backend_heartbeat.is_persistent || backend_heartbeat.is_healthy {
            continue;
        }

        let unhealthy_duration_ms =
            now_epoch_ms.saturating_sub(backend_heartbeat.last_healthy_at_epoch_ms);
        if unhealthy_duration_ms < backend_stall_timeout_ms {
            continue;
        }

        runtime.restart_backend_manager(backend_heartbeat.backend)?;
        outcome.restarted_backends.push(backend_heartbeat.backend);
        outcome.events.push(HeartbeatEvent {
            component: HeartbeatComponent::Backend,
            action: "backend_restart_due_to_stall".to_string(),
            logical_session_id: None,
            run_id: None,
            backend: Some(backend_heartbeat.backend),
        });
    }

    let dispatcher_heartbeat = runtime.dispatcher_heartbeat()?;
    validate_dispatcher_heartbeat(dispatcher_heartbeat, now_epoch_ms)?;
    let dispatcher_idle_duration_ms =
        now_epoch_ms.saturating_sub(dispatcher_heartbeat.last_dispatch_at_epoch_ms);
    if dispatcher_heartbeat.queued_run_count > 0
        && dispatcher_heartbeat.active_lane_count == 0
        && dispatcher_idle_duration_ms >= dispatcher_stall_timeout_ms
    {
        runtime.nudge_dispatcher()?;
        outcome.dispatcher_nudged = true;
        outcome.events.push(HeartbeatEvent {
            component: HeartbeatComponent::Dispatcher,
            action: "dispatcher_nudged_due_to_stall".to_string(),
            logical_session_id: None,
            run_id: None,
            backend: None,
        });
    }

    Ok(outcome)
}

fn validate_heartbeat_inputs(policy: &HeartbeatPolicy, now_epoch_ms: u64) -> CrabResult<()> {
    if now_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "heartbeat_cycle",
            message: "now_epoch_ms must be greater than 0".to_string(),
        });
    }

    if policy.run_stall_timeout_secs == 0 {
        return Err(CrabError::InvalidConfig {
            key: "CRAB_RUN_STALL_TIMEOUT_SECS",
            value: policy.run_stall_timeout_secs.to_string(),
            reason: "must be greater than 0",
        });
    }

    if policy.backend_stall_timeout_secs == 0 {
        return Err(CrabError::InvalidConfig {
            key: "CRAB_BACKEND_STALL_TIMEOUT_SECS",
            value: policy.backend_stall_timeout_secs.to_string(),
            reason: "must be greater than 0",
        });
    }

    if policy.dispatcher_stall_timeout_secs == 0 {
        return Err(CrabError::InvalidConfig {
            key: "CRAB_DISPATCHER_STALL_TIMEOUT_SECS",
            value: policy.dispatcher_stall_timeout_secs.to_string(),
            reason: "must be greater than 0",
        });
    }

    Ok(())
}

fn checked_timeout_ms(key: &'static str, timeout_secs: u64) -> CrabResult<u64> {
    timeout_secs
        .checked_mul(1_000)
        .ok_or(CrabError::InvalidConfig {
            key,
            value: timeout_secs.to_string(),
            reason: "must fit in milliseconds as a u64",
        })
}

fn validate_active_run_heartbeat(run: &ActiveRunHeartbeat, now_epoch_ms: u64) -> CrabResult<()> {
    crate::validation::validate_non_empty_text(
        "heartbeat_cycle",
        "logical_session_id",
        &run.logical_session_id,
    )?;
    crate::validation::validate_non_empty_text("heartbeat_cycle", "run_id", &run.run_id)?;
    let _ = now_epoch_ms;
    Ok(())
}

fn validate_backend_heartbeat(heartbeat: BackendHeartbeat, now_epoch_ms: u64) -> CrabResult<()> {
    let _ = (heartbeat, now_epoch_ms);
    Ok(())
}

fn validate_dispatcher_heartbeat(
    heartbeat: DispatcherHeartbeat,
    now_epoch_ms: u64,
) -> CrabResult<()> {
    let _ = (heartbeat, now_epoch_ms);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{BackendKind, CrabError, CrabResult, LaneState};

    use super::{
        execute_heartbeat_cycle, ActiveRunHeartbeat, BackendHeartbeat, DispatcherHeartbeat,
        HeartbeatEvent, HeartbeatOutcome, HeartbeatPolicy, HeartbeatRuntime,
    };

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        list_active_runs_result: CrabResult<Vec<ActiveRunHeartbeat>>,
        list_backend_heartbeats_result: CrabResult<Vec<BackendHeartbeat>>,
        dispatcher_heartbeat_result: CrabResult<DispatcherHeartbeat>,
        cancel_errors: BTreeMap<(String, String), CrabError>,
        hard_stop_result: CrabResult<()>,
        restart_errors: BTreeMap<String, CrabError>,
        nudge_result: CrabResult<()>,
        cancel_calls: Vec<(String, String)>,
        hard_stop_calls: Vec<(String, String, String)>,
        restart_calls: Vec<BackendKind>,
        nudge_call_count: usize,
    }

    impl FakeRuntime {
        fn new() -> Self {
            Self {
                list_active_runs_result: Ok(Vec::new()),
                list_backend_heartbeats_result: Ok(Vec::new()),
                dispatcher_heartbeat_result: Ok(dispatcher_heartbeat(0, 0, 1)),
                cancel_errors: BTreeMap::new(),
                hard_stop_result: Ok(()),
                restart_errors: BTreeMap::new(),
                nudge_result: Ok(()),
                cancel_calls: Vec::new(),
                hard_stop_calls: Vec::new(),
                restart_calls: Vec::new(),
                nudge_call_count: 0,
            }
        }
    }

    impl HeartbeatRuntime for FakeRuntime {
        fn list_active_runs(&self) -> CrabResult<Vec<ActiveRunHeartbeat>> {
            self.list_active_runs_result.clone()
        }

        fn request_cancel_active_run(
            &mut self,
            logical_session_id: &str,
            run_id: &str,
        ) -> CrabResult<()> {
            self.cancel_calls
                .push((logical_session_id.to_string(), run_id.to_string()));
            let key = (logical_session_id.to_string(), run_id.to_string());
            if let Some(error) = self.cancel_errors.get(&key) {
                return Err(error.clone());
            }
            Ok(())
        }

        fn hard_stop_run_and_rotate(
            &mut self,
            logical_session_id: &str,
            run_id: &str,
            reason: &str,
        ) -> CrabResult<()> {
            self.hard_stop_calls.push((
                logical_session_id.to_string(),
                run_id.to_string(),
                reason.to_string(),
            ));
            self.hard_stop_result.clone()
        }

        fn list_backend_heartbeats(&self) -> CrabResult<Vec<BackendHeartbeat>> {
            self.list_backend_heartbeats_result.clone()
        }

        fn restart_backend_manager(&mut self, backend: BackendKind) -> CrabResult<()> {
            self.restart_calls.push(backend);
            if let Some(error) = self.restart_errors.get(backend_name(backend)) {
                return Err(error.clone());
            }
            Ok(())
        }

        fn dispatcher_heartbeat(&self) -> CrabResult<DispatcherHeartbeat> {
            self.dispatcher_heartbeat_result.clone()
        }

        fn nudge_dispatcher(&mut self) -> CrabResult<()> {
            self.nudge_call_count += 1;
            self.nudge_result.clone()
        }
    }

    fn policy() -> HeartbeatPolicy {
        HeartbeatPolicy {
            run_stall_timeout_secs: 90,
            backend_stall_timeout_secs: 30,
            dispatcher_stall_timeout_secs: 20,
        }
    }

    fn active_run(
        logical_session_id: &str,
        run_id: &str,
        lane_state: LaneState,
        last_progress_at_epoch_ms: u64,
    ) -> ActiveRunHeartbeat {
        ActiveRunHeartbeat {
            logical_session_id: logical_session_id.to_string(),
            run_id: run_id.to_string(),
            lane_state,
            backend: BackendKind::Claude,
            last_progress_at_epoch_ms,
        }
    }

    fn backend_heartbeat(
        backend: BackendKind,
        is_persistent: bool,
        is_healthy: bool,
        last_healthy_at_epoch_ms: u64,
    ) -> BackendHeartbeat {
        BackendHeartbeat {
            backend,
            is_persistent,
            is_healthy,
            last_healthy_at_epoch_ms,
        }
    }

    fn dispatcher_heartbeat(
        queued_run_count: usize,
        active_lane_count: usize,
        last_dispatch_at_epoch_ms: u64,
    ) -> DispatcherHeartbeat {
        DispatcherHeartbeat {
            queued_run_count,
            active_lane_count,
            last_dispatch_at_epoch_ms,
        }
    }

    fn boom(context: &'static str) -> CrabError {
        CrabError::InvariantViolation {
            context,
            message: "boom".to_string(),
        }
    }

    fn backend_name(backend: BackendKind) -> &'static str {
        match backend {
            BackendKind::Claude => "claude",
        }
    }

    fn assert_no_actions(outcome: &HeartbeatOutcome) {
        assert!(outcome.cancelled_runs.is_empty());
        assert!(outcome.hard_stopped_runs.is_empty());
        assert!(outcome.restarted_backends.is_empty());
        assert!(!outcome.dispatcher_nudged);
        assert!(outcome.events.is_empty());
    }

    fn assert_single_event_action(events: &[HeartbeatEvent], expected_action: &str) {
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].action, expected_action);
    }

    #[test]
    fn requests_cancel_for_stalled_runs_in_deterministic_order() {
        let mut runtime = FakeRuntime::new();
        runtime.list_active_runs_result = Ok(vec![
            active_run("discord:channel:b", "run-b", LaneState::Running, 1_000),
            active_run("discord:channel:a", "run-a", LaneState::Running, 2_000),
        ]);

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("stalled runs should trigger cancellation requests");

        assert_eq!(
            runtime.cancel_calls,
            vec![
                ("discord:channel:a".to_string(), "run-a".to_string()),
                ("discord:channel:b".to_string(), "run-b".to_string()),
            ]
        );
        assert_eq!(outcome.cancelled_runs.len(), 2);
        assert!(outcome.hard_stopped_runs.is_empty());
        assert_eq!(outcome.events.len(), 2);
        assert_eq!(
            outcome.events[0].action,
            "cancel_requested_due_to_stall".to_string()
        );
    }

    #[test]
    fn hard_stops_and_rotates_when_cancel_request_fails() {
        let mut runtime = FakeRuntime::new();
        runtime.list_active_runs_result = Ok(vec![active_run(
            "discord:channel:a",
            "run-1",
            LaneState::Cancelling,
            1_000,
        )]);
        runtime.cancel_errors.insert(
            ("discord:channel:a".to_string(), "run-1".to_string()),
            boom("request_cancel"),
        );

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("cancel failures should fall back to hard stop");

        assert!(outcome.cancelled_runs.is_empty());
        assert_eq!(outcome.hard_stopped_runs.len(), 1);
        assert_eq!(runtime.hard_stop_calls.len(), 1);
        assert!(runtime.hard_stop_calls[0]
            .2
            .starts_with("cancel_request_failed:"));
        assert_single_event_action(
            &outcome.events,
            "hard_stop_and_rotate_due_to_cancel_failure",
        );
    }

    #[test]
    fn propagates_hard_stop_failures() {
        let mut runtime = FakeRuntime::new();
        runtime.list_active_runs_result = Ok(vec![active_run(
            "discord:channel:a",
            "run-1",
            LaneState::Running,
            1_000,
        )]);
        runtime.cancel_errors.insert(
            ("discord:channel:a".to_string(), "run-1".to_string()),
            boom("request_cancel"),
        );
        runtime.hard_stop_result = Err(boom("hard_stop"));

        let error = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect_err("hard stop failures should fail heartbeat cycle");
        assert_eq!(error, boom("hard_stop"));
    }

    #[test]
    fn ignores_runs_without_stall() {
        let mut runtime = FakeRuntime::new();
        runtime.list_active_runs_result = Ok(vec![active_run(
            "discord:channel:a",
            "run-1",
            LaneState::Running,
            95_000,
        )]);

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("fresh runs should not trigger actions");
        assert_no_actions(&outcome);
    }

    #[test]
    fn rejects_invalid_active_run_snapshots() {
        let mut blank_session_runtime = FakeRuntime::new();
        blank_session_runtime.list_active_runs_result =
            Ok(vec![active_run("   ", "run-1", LaneState::Running, 1_000)]);
        let blank_session_error =
            execute_heartbeat_cycle(&mut blank_session_runtime, &policy(), 100_000)
                .expect_err("blank session id should fail");
        assert_eq!(
            blank_session_error,
            CrabError::InvariantViolation {
                context: "heartbeat_cycle",
                message: "logical_session_id must not be empty".to_string(),
            }
        );

        let mut blank_run_runtime = FakeRuntime::new();
        blank_run_runtime.list_active_runs_result = Ok(vec![active_run(
            "discord:channel:a",
            " ",
            LaneState::Running,
            1_000,
        )]);
        let blank_run_error = execute_heartbeat_cycle(&mut blank_run_runtime, &policy(), 100_000)
            .expect_err("blank run id should fail");
        assert_eq!(
            blank_run_error,
            CrabError::InvariantViolation {
                context: "heartbeat_cycle",
                message: "run_id must not be empty".to_string(),
            }
        );

        let mut invalid_lane_runtime = FakeRuntime::new();
        invalid_lane_runtime.list_active_runs_result = Ok(vec![active_run(
            "discord:channel:a",
            "run-1",
            LaneState::Idle,
            1_000,
        )]);
        let invalid_lane_error =
            execute_heartbeat_cycle(&mut invalid_lane_runtime, &policy(), 100_000)
                .expect_err("non-active lane state should fail");
        assert_eq!(
            invalid_lane_error,
            CrabError::InvariantViolation {
                context: "heartbeat_cycle",
                message: "active run discord:channel:a/run-1 has non-active lane state Idle"
                    .to_string(),
            }
        );
    }

    #[test]
    fn restarts_unhealthy_persistent_backends_in_sorted_order() {
        let mut runtime = FakeRuntime::new();
        runtime.list_backend_heartbeats_result = Ok(vec![backend_heartbeat(
            BackendKind::Claude,
            true,
            false,
            1_000,
        )]);

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("stale unhealthy backends should restart");

        assert_eq!(runtime.restart_calls, vec![BackendKind::Claude]);
        assert_eq!(outcome.restarted_backends, runtime.restart_calls);
        assert_eq!(outcome.events.len(), 1);
        assert_eq!(
            outcome.events[0].action,
            "backend_restart_due_to_stall".to_string()
        );
    }

    #[test]
    fn skips_backend_restart_for_healthy_recent_or_non_persistent_backends() {
        let mut runtime = FakeRuntime::new();
        runtime.list_backend_heartbeats_result = Ok(vec![backend_heartbeat(
            BackendKind::Claude,
            false,
            false,
            1_000,
        )]);

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("healthy backends should be ignored");
        assert_no_actions(&outcome);
        assert!(runtime.restart_calls.is_empty());
    }

    #[test]
    fn propagates_backend_restart_failures() {
        let mut runtime = FakeRuntime::new();
        runtime.list_backend_heartbeats_result = Ok(vec![backend_heartbeat(
            BackendKind::Claude,
            true,
            false,
            1_000,
        )]);
        runtime
            .restart_errors
            .insert("claude".to_string(), boom("restart_backend"));

        let error = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect_err("backend restart failure should fail cycle");
        assert_eq!(error, boom("restart_backend"));
    }

    #[test]
    fn nudges_dispatcher_when_queue_is_stalled() {
        let mut runtime = FakeRuntime::new();
        runtime.dispatcher_heartbeat_result = Ok(dispatcher_heartbeat(3, 0, 1_000));

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("stalled dispatcher should be nudged");

        assert!(outcome.dispatcher_nudged);
        assert_eq!(runtime.nudge_call_count, 1);
        assert_single_event_action(&outcome.events, "dispatcher_nudged_due_to_stall");
    }

    #[test]
    fn skips_dispatcher_nudge_when_not_stalled() {
        let mut runtime = FakeRuntime::new();
        runtime.dispatcher_heartbeat_result = Ok(dispatcher_heartbeat(3, 1, 1_000));

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect("active dispatcher should not be nudged");
        assert_no_actions(&outcome);
        assert_eq!(runtime.nudge_call_count, 0);
    }

    #[test]
    fn propagates_dispatcher_nudge_failures() {
        let mut runtime = FakeRuntime::new();
        runtime.dispatcher_heartbeat_result = Ok(dispatcher_heartbeat(1, 0, 1_000));
        runtime.nudge_result = Err(boom("nudge_dispatcher"));

        let error = execute_heartbeat_cycle(&mut runtime, &policy(), 100_000)
            .expect_err("dispatcher nudge failures should propagate");
        assert_eq!(error, boom("nudge_dispatcher"));
    }

    #[test]
    fn validates_timeouts_and_snapshot_clock_skew() {
        let mut runtime = FakeRuntime::new();

        let now_error =
            execute_heartbeat_cycle(&mut runtime, &policy(), 0).expect_err("now must be > 0");
        assert_eq!(
            now_error,
            CrabError::InvariantViolation {
                context: "heartbeat_cycle",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );

        let mut zero_run_timeout = policy();
        zero_run_timeout.run_stall_timeout_secs = 0;
        let zero_run_error = execute_heartbeat_cycle(&mut runtime, &zero_run_timeout, 1_000)
            .expect_err("run timeout must be > 0");
        assert_eq!(
            zero_run_error,
            CrabError::InvalidConfig {
                key: "CRAB_RUN_STALL_TIMEOUT_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let mut zero_backend_timeout = policy();
        zero_backend_timeout.backend_stall_timeout_secs = 0;
        let zero_backend_error =
            execute_heartbeat_cycle(&mut runtime, &zero_backend_timeout, 1_000)
                .expect_err("backend timeout must be > 0");
        assert_eq!(
            zero_backend_error,
            CrabError::InvalidConfig {
                key: "CRAB_BACKEND_STALL_TIMEOUT_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let mut zero_dispatcher_timeout = policy();
        zero_dispatcher_timeout.dispatcher_stall_timeout_secs = 0;
        let zero_dispatcher_error =
            execute_heartbeat_cycle(&mut runtime, &zero_dispatcher_timeout, 1_000)
                .expect_err("dispatcher timeout must be > 0");
        assert_eq!(
            zero_dispatcher_error,
            CrabError::InvalidConfig {
                key: "CRAB_DISPATCHER_STALL_TIMEOUT_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn heartbeat_cycle_tolerates_last_progress_slightly_in_future() {
        let mut runtime = FakeRuntime::new();
        runtime.list_active_runs_result = Ok(vec![active_run(
            "discord:channel:a",
            "run-1",
            LaneState::Running,
            1_001,
        )]);

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 1_000)
            .expect("slightly future last_progress should be clamped");

        assert_no_actions(&outcome);
        assert!(runtime.cancel_calls.is_empty());
    }

    #[test]
    fn heartbeat_cycle_tolerates_backend_timestamp_in_future() {
        let mut runtime = FakeRuntime::new();
        runtime.list_backend_heartbeats_result = Ok(vec![backend_heartbeat(
            BackendKind::Claude,
            true,
            false,
            1_001,
        )]);

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 1_000)
            .expect("future backend heartbeat should be clamped");

        assert_no_actions(&outcome);
        assert!(runtime.restart_calls.is_empty());
    }

    #[test]
    fn heartbeat_cycle_tolerates_dispatcher_timestamp_in_future() {
        let mut runtime = FakeRuntime::new();
        runtime.dispatcher_heartbeat_result = Ok(dispatcher_heartbeat(1, 0, 1_001));

        let outcome = execute_heartbeat_cycle(&mut runtime, &policy(), 1_000)
            .expect("future dispatcher timestamp should be clamped");

        assert_no_actions(&outcome);
        assert_eq!(runtime.nudge_call_count, 0);
    }

    #[test]
    fn rejects_timeout_overflow() {
        let mut runtime = FakeRuntime::new();
        let mut run_overflow_policy = policy();
        run_overflow_policy.run_stall_timeout_secs = u64::MAX;
        let run_overflow_error = execute_heartbeat_cycle(&mut runtime, &run_overflow_policy, 1_000)
            .expect_err("run timeout overflow should fail");
        assert_eq!(
            run_overflow_error,
            CrabError::InvalidConfig {
                key: "CRAB_RUN_STALL_TIMEOUT_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );

        let mut backend_overflow_policy = policy();
        backend_overflow_policy.backend_stall_timeout_secs = u64::MAX;
        let backend_overflow_error =
            execute_heartbeat_cycle(&mut runtime, &backend_overflow_policy, 1_000)
                .expect_err("backend timeout overflow should fail");
        assert_eq!(
            backend_overflow_error,
            CrabError::InvalidConfig {
                key: "CRAB_BACKEND_STALL_TIMEOUT_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );

        let mut overflow_policy = policy();
        overflow_policy.dispatcher_stall_timeout_secs = u64::MAX;
        let overflow_error = execute_heartbeat_cycle(&mut runtime, &overflow_policy, 1_000)
            .expect_err("dispatcher timeout overflow should fail");
        assert_eq!(
            overflow_error,
            CrabError::InvalidConfig {
                key: "CRAB_DISPATCHER_STALL_TIMEOUT_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );
    }

    #[test]
    fn propagates_runtime_snapshot_errors() {
        let mut active_runs_error_runtime = FakeRuntime::new();
        active_runs_error_runtime.list_active_runs_result = Err(boom("list_active_runs"));
        let active_runs_error =
            execute_heartbeat_cycle(&mut active_runs_error_runtime, &policy(), 1_000)
                .expect_err("active run fetch failures should propagate");
        assert_eq!(active_runs_error, boom("list_active_runs"));

        let mut backend_error_runtime = FakeRuntime::new();
        backend_error_runtime.list_backend_heartbeats_result = Err(boom("list_backend_heartbeats"));
        let backend_error = execute_heartbeat_cycle(&mut backend_error_runtime, &policy(), 1_000)
            .expect_err("backend fetch failures should propagate");
        assert_eq!(backend_error, boom("list_backend_heartbeats"));

        let mut dispatcher_error_runtime = FakeRuntime::new();
        dispatcher_error_runtime.dispatcher_heartbeat_result = Err(boom("dispatcher_heartbeat"));
        let dispatcher_error =
            execute_heartbeat_cycle(&mut dispatcher_error_runtime, &policy(), 1_000)
                .expect_err("dispatcher fetch failures should propagate");
        assert_eq!(dispatcher_error, boom("dispatcher_heartbeat"));
    }

    #[test]
    fn heartbeat_backend_stall_boundary_below_and_at_threshold() {
        let policy = policy();
        let stall_timeout_ms = policy.backend_stall_timeout_secs * 1_000;
        let now = 100_000u64;

        // Just under threshold: backend should NOT be restarted.
        let mut below = FakeRuntime::new();
        below.list_backend_heartbeats_result = Ok(vec![backend_heartbeat(
            BackendKind::Claude,
            true,
            false,
            now - stall_timeout_ms + 1,
        )]);
        let below_outcome = execute_heartbeat_cycle(&mut below, &policy, now).unwrap();
        assert!(
            below_outcome.restarted_backends.is_empty(),
            "backend below stall threshold should not be restarted"
        );

        // Exactly at threshold: backend SHOULD be restarted.
        let mut at = FakeRuntime::new();
        at.list_backend_heartbeats_result = Ok(vec![backend_heartbeat(
            BackendKind::Claude,
            true,
            false,
            now - stall_timeout_ms,
        )]);
        let at_outcome = execute_heartbeat_cycle(&mut at, &policy, now).unwrap();
        assert_eq!(at_outcome.restarted_backends, vec![BackendKind::Claude]);
        assert_eq!(at.restart_calls, vec![BackendKind::Claude]);
    }
}
