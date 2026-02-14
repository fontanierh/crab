use std::collections::BTreeMap;

use crate::{
    CrabError, CrabResult, EventEnvelope, EventKind, EventSource, LaneState, LogicalSession, Run,
    RunStatus,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupReconciliationRecoveredRun {
    pub logical_session_id: String,
    pub run_id: String,
    pub previous_status: RunStatus,
    pub recovered_status: RunStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupReconciliationRepairedPhysicalSession {
    pub logical_session_id: String,
    pub previous_physical_session_id: String,
    pub repaired_physical_session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartupReconciliationOutcome {
    pub recovered_runs: Vec<StartupReconciliationRecoveredRun>,
    /// Sessions whose lane state was repaired to `Idle` on startup so new work can be dispatched.
    ///
    /// Important: this does **not** clear `active_physical_session_id`. Physical sessions are
    /// valuable continuity handles and should be preserved across restarts whenever possible.
    pub repaired_session_ids: Vec<String>,
    /// Sessions whose `active_physical_session_id` was updated to a previous successful physical
    /// session on startup.
    ///
    /// This is intentionally conservative: we only repair when the currently active physical
    /// session has never produced a successful run in the local persisted run history. That keeps
    /// us from regressing legitimate active sessions while still recovering from "orphan" IDs
    /// created during crashes/restarts.
    pub repaired_physical_sessions: Vec<StartupReconciliationRepairedPhysicalSession>,
}

pub trait StartupReconciliationRuntime {
    fn restart_backend_managers(&mut self) -> CrabResult<()>;
    fn list_sessions(&self) -> CrabResult<Vec<LogicalSession>>;
    fn list_runs_for_session(&self, logical_session_id: &str) -> CrabResult<Vec<Run>>;
    fn persist_run(&mut self, run: &Run) -> CrabResult<()>;
    fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64>;
    fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()>;
    /// Repairs the logical session to allow dispatch after a restart.
    ///
    /// Must:
    /// - set `lane_state` to `Idle`
    /// - recompute `queued_run_count`
    ///
    /// Must NOT:
    /// - clear `active_physical_session_id`
    fn repair_session_lane_state(&mut self, logical_session_id: &str) -> CrabResult<()>;
    /// Repairs `active_physical_session_id` to the provided value.
    fn repair_active_physical_session_id(
        &mut self,
        logical_session_id: &str,
        physical_session_id: &str,
    ) -> CrabResult<()>;
}

pub fn execute_startup_reconciliation<R: StartupReconciliationRuntime>(
    runtime: &mut R,
    now_epoch_ms: u64,
    grace_period_ms: u64,
) -> CrabResult<StartupReconciliationOutcome> {
    validate_reconciliation_input(now_epoch_ms, grace_period_ms)?;
    runtime.restart_backend_managers()?;

    let mut sessions = runtime.list_sessions()?;
    sessions.sort_by(|left, right| left.id.cmp(&right.id));

    let mut recovered_runs = Vec::new();
    let mut repaired_session_ids = Vec::new();
    let mut repaired_physical_sessions = Vec::new();

    for session in sessions {
        let mut runs = runtime.list_runs_for_session(&session.id)?;
        runs.sort_by(|left, right| left.id.cmp(&right.id));

        let mut session_has_recovered_run = false;
        let mut has_non_stale_inflight_run = false;
        let mut active_physical_has_succeeded = false;
        let mut active_physical_has_failed = false;
        let mut latest_successful_physical_session: Option<(u64, String)> = None;

        let active_physical_session_id = session.active_physical_session_id.as_deref();
        for run in runs {
            if run.logical_session_id != session.id {
                return Err(CrabError::InvariantViolation {
                    context: "startup_reconciliation",
                    message: format!(
                        "run {} belongs to {}, expected {}",
                        run.id, run.logical_session_id, session.id
                    ),
                });
            }

            if run.status == RunStatus::Succeeded {
                if active_physical_session_id
                    .is_some_and(|active_id| run.physical_session_id.as_deref() == Some(active_id))
                {
                    active_physical_has_succeeded = true;
                }
                if let (Some(completed_at_epoch_ms), Some(physical_session_id)) = (
                    run.completed_at_epoch_ms,
                    run.physical_session_id.as_deref(),
                ) {
                    match &latest_successful_physical_session {
                        Some((latest_completed_at, _))
                            if completed_at_epoch_ms <= *latest_completed_at => {}
                        _ => {
                            latest_successful_physical_session =
                                Some((completed_at_epoch_ms, physical_session_id.to_string()));
                        }
                    }
                }
            }

            if run.status == RunStatus::Failed
                && active_physical_session_id
                    .is_some_and(|active_id| run.physical_session_id.as_deref() == Some(active_id))
            {
                active_physical_has_failed = true;
            }

            if run.status != RunStatus::Running || run.completed_at_epoch_ms.is_some() {
                continue;
            }

            // A non-stale in-flight run means the previous process may still be working; keep the
            // session lane state intact until the run becomes stale and is reconciled.
            if !is_stale_inflight_run(&run, now_epoch_ms, grace_period_ms)? {
                has_non_stale_inflight_run = true;
                continue;
            }

            let mut updated_run = run.clone();
            let previous_status = updated_run.status;
            updated_run.status = RunStatus::Cancelled;
            updated_run.completed_at_epoch_ms = Some(now_epoch_ms);
            runtime.persist_run(&updated_run)?;

            let next_sequence =
                runtime.next_event_sequence(&updated_run.logical_session_id, &updated_run.id)?;
            if next_sequence == 0 {
                return Err(CrabError::InvariantViolation {
                    context: "startup_reconciliation",
                    message: format!(
                        "next event sequence must be greater than 0 for run {}/{}",
                        updated_run.logical_session_id, updated_run.id
                    ),
                });
            }

            let event = build_startup_recovery_event(&updated_run, next_sequence, now_epoch_ms);
            runtime.append_event(&event)?;

            recovered_runs.push(StartupReconciliationRecoveredRun {
                logical_session_id: updated_run.logical_session_id.clone(),
                run_id: updated_run.id.clone(),
                previous_status,
                recovered_status: RunStatus::Cancelled,
            });
            session_has_recovered_run = true;
        }

        if !has_non_stale_inflight_run && session.lane_state == LaneState::Idle {
            if let (Some(active_id), true, false, Some((_, repaired_id))) = (
                active_physical_session_id,
                active_physical_has_failed,
                active_physical_has_succeeded,
                latest_successful_physical_session.as_ref(),
            ) {
                runtime.repair_active_physical_session_id(&session.id, repaired_id)?;
                repaired_physical_sessions.push(StartupReconciliationRepairedPhysicalSession {
                    logical_session_id: session.id.clone(),
                    previous_physical_session_id: active_id.to_string(),
                    repaired_physical_session_id: repaired_id.clone(),
                });
            }
        }

        if should_repair_lane_state(
            &session,
            session_has_recovered_run,
            has_non_stale_inflight_run,
        ) {
            runtime.repair_session_lane_state(&session.id)?;
            repaired_session_ids.push(session.id);
        }
    }

    Ok(StartupReconciliationOutcome {
        recovered_runs,
        repaired_session_ids,
        repaired_physical_sessions,
    })
}

fn validate_reconciliation_input(now_epoch_ms: u64, grace_period_ms: u64) -> CrabResult<()> {
    if now_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "startup_reconciliation",
            message: "now_epoch_ms must be greater than 0".to_string(),
        });
    }
    if grace_period_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "startup_reconciliation",
            message: "grace_period_ms must be greater than 0".to_string(),
        });
    }
    Ok(())
}

fn should_repair_lane_state(
    session: &LogicalSession,
    session_has_recovered_run: bool,
    has_non_stale_inflight_run: bool,
) -> bool {
    session_has_recovered_run
        || (session.lane_state != LaneState::Idle && !has_non_stale_inflight_run)
}

fn is_stale_inflight_run(run: &Run, now_epoch_ms: u64, grace_period_ms: u64) -> CrabResult<bool> {
    debug_assert_eq!(run.status, RunStatus::Running);
    debug_assert!(run.completed_at_epoch_ms.is_none());

    let reference_epoch_ms = run.started_at_epoch_ms.unwrap_or(run.queued_at_epoch_ms);
    let stale_after_epoch_ms =
        reference_epoch_ms
            .checked_add(grace_period_ms)
            .ok_or(CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message: format!(
                    "grace-period overflow while evaluating stale run {}/{}",
                    run.logical_session_id, run.id
                ),
            })?;

    Ok(now_epoch_ms >= stale_after_epoch_ms)
}

fn build_startup_recovery_event(
    run: &Run,
    sequence: u64,
    emitted_at_epoch_ms: u64,
) -> EventEnvelope {
    let payload = BTreeMap::from([
        (
            "state".to_string(),
            format!("{:?}", RunStatus::Cancelled).to_lowercase(),
        ),
        (
            "reason".to_string(),
            "startup_recovered_as_interrupted".to_string(),
        ),
        (
            "previous_status".to_string(),
            format!("{:?}", RunStatus::Running).to_lowercase(),
        ),
    ]);

    EventEnvelope {
        event_id: format!(
            "evt:start-reconcile:{}:{}:{}",
            run.logical_session_id, run.id, sequence
        ),
        run_id: run.id.clone(),
        turn_id: Some(format!("turn:{}", run.id)),
        lane_id: Some(run.logical_session_id.clone()),
        logical_session_id: run.logical_session_id.clone(),
        physical_session_id: run.physical_session_id.clone(),
        backend: Some(run.profile.resolved_profile.backend),
        resolved_model: Some(run.profile.resolved_profile.model.clone()),
        resolved_reasoning_level: Some(
            run.profile
                .resolved_profile
                .reasoning_level
                .as_token()
                .to_string(),
        ),
        profile_source: Some(run.profile.profile_source_token().to_string()),
        sequence,
        emitted_at_epoch_ms,
        source: EventSource::System,
        kind: EventKind::RunState,
        payload,
        profile: Some(run.profile.clone()),
        idempotency_key: Some(format!(
            "startup-reconcile:{}:{}:{}",
            run.logical_session_id, run.id, sequence
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::{
        BackendKind, CrabError, CrabResult, InferenceProfile, ProfileValueSource, ReasoningLevel,
        RunProfileTelemetry, TokenAccounting,
    };

    use super::{
        execute_startup_reconciliation, EventEnvelope, LogicalSession, Run, RunStatus,
        StartupReconciliationOutcome, StartupReconciliationRecoveredRun,
        StartupReconciliationRepairedPhysicalSession, StartupReconciliationRuntime,
    };
    use crate::LaneState;

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        restart_backend_result: CrabResult<()>,
        list_sessions_result: CrabResult<()>,
        list_runs_result: CrabResult<()>,
        sessions: Vec<LogicalSession>,
        runs_by_session: BTreeMap<String, Vec<Run>>,
        next_sequence_result: CrabResult<()>,
        next_sequence: BTreeMap<(String, String), u64>,
        persist_run_result: CrabResult<()>,
        append_event_result: CrabResult<()>,
        repair_session_result: CrabResult<()>,
        repair_active_physical_session_result: CrabResult<()>,
        persisted_runs: Vec<Run>,
        appended_events: Vec<EventEnvelope>,
        repaired_sessions: Vec<String>,
        repaired_active_physical_sessions: Vec<(String, String)>,
        calls: Vec<String>,
    }

    impl FakeRuntime {
        fn new() -> Self {
            Self {
                restart_backend_result: Ok(()),
                list_sessions_result: Ok(()),
                list_runs_result: Ok(()),
                sessions: Vec::new(),
                runs_by_session: BTreeMap::new(),
                next_sequence_result: Ok(()),
                next_sequence: BTreeMap::new(),
                persist_run_result: Ok(()),
                append_event_result: Ok(()),
                repair_session_result: Ok(()),
                repair_active_physical_session_result: Ok(()),
                persisted_runs: Vec::new(),
                appended_events: Vec::new(),
                repaired_sessions: Vec::new(),
                repaired_active_physical_sessions: Vec::new(),
                calls: Vec::new(),
            }
        }
    }

    impl StartupReconciliationRuntime for FakeRuntime {
        fn restart_backend_managers(&mut self) -> CrabResult<()> {
            self.calls.push("restart_backends".to_string());
            self.restart_backend_result.clone()
        }

        fn list_sessions(&self) -> CrabResult<Vec<LogicalSession>> {
            self.list_sessions_result.clone()?;
            Ok(self.sessions.clone())
        }

        fn list_runs_for_session(&self, logical_session_id: &str) -> CrabResult<Vec<Run>> {
            self.list_runs_result.clone()?;
            Ok(self
                .runs_by_session
                .get(logical_session_id)
                .cloned()
                .unwrap_or_default())
        }

        fn persist_run(&mut self, run: &Run) -> CrabResult<()> {
            self.calls
                .push(format!("persist_run:{}/{}", run.logical_session_id, run.id));
            self.persisted_runs.push(run.clone());
            self.persist_run_result.clone()
        }

        fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64> {
            self.next_sequence_result.clone()?;
            Ok(*self
                .next_sequence
                .get(&(logical_session_id.to_string(), run_id.to_string()))
                .unwrap_or(&1))
        }

        fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()> {
            self.calls.push(format!(
                "append_event:{}/{}:{}",
                event.logical_session_id, event.run_id, event.sequence
            ));
            self.appended_events.push(event.clone());
            self.append_event_result.clone()
        }

        fn repair_session_lane_state(&mut self, logical_session_id: &str) -> CrabResult<()> {
            self.calls
                .push(format!("repair_session:{logical_session_id}"));
            self.repaired_sessions.push(logical_session_id.to_string());
            self.repair_session_result.clone()
        }

        fn repair_active_physical_session_id(
            &mut self,
            logical_session_id: &str,
            physical_session_id: &str,
        ) -> CrabResult<()> {
            self.calls.push(format!(
                "repair_active_physical_session:{logical_session_id}:{physical_session_id}"
            ));
            self.repaired_active_physical_sessions.push((
                logical_session_id.to_string(),
                physical_session_id.to_string(),
            ));
            self.repair_active_physical_session_result.clone()
        }
    }

    fn sample_profile() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Codex,
            model: "gpt-5-codex".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        }
    }

    fn sample_run_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: Some(sample_profile()),
            resolved_profile: sample_profile(),
            backend_source: ProfileValueSource::SessionProfile,
            model_source: ProfileValueSource::SessionProfile,
            reasoning_level_source: ProfileValueSource::SessionProfile,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: "111111111111111111".to_string(),
            sender_is_owner: false,
            resolved_owner_profile: None,
        }
    }

    fn sample_token_accounting() -> TokenAccounting {
        TokenAccounting {
            input_tokens: 10,
            output_tokens: 3,
            total_tokens: 13,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        }
    }

    fn session(id: &str, lane_state: LaneState, active_physical: Option<&str>) -> LogicalSession {
        LogicalSession {
            id: id.to_string(),
            active_backend: BackendKind::Codex,
            active_profile: sample_profile(),
            active_physical_session_id: active_physical.map(str::to_string),
            last_successful_checkpoint_id: Some("ckpt-1".to_string()),
            lane_state,
            queued_run_count: 0,
            last_activity_epoch_ms: 1_739_173_200_000,
            token_accounting: sample_token_accounting(),
            has_injected_bootstrap: false,
        }
    }

    fn run(
        logical_session_id: &str,
        id: &str,
        status: RunStatus,
        queued_at_epoch_ms: u64,
        started_at_epoch_ms: Option<u64>,
        completed_at_epoch_ms: Option<u64>,
    ) -> Run {
        run_with_physical(
            logical_session_id,
            id,
            "phys-1",
            status,
            queued_at_epoch_ms,
            started_at_epoch_ms,
            completed_at_epoch_ms,
        )
    }

    fn run_with_physical(
        logical_session_id: &str,
        id: &str,
        physical_session_id: &str,
        status: RunStatus,
        queued_at_epoch_ms: u64,
        started_at_epoch_ms: Option<u64>,
        completed_at_epoch_ms: Option<u64>,
    ) -> Run {
        Run {
            id: id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            physical_session_id: Some(physical_session_id.to_string()),
            status,
            user_input: "hello".to_string(),
            delivery_channel_id: None,
            profile: sample_run_profile_telemetry(),
            queued_at_epoch_ms,
            started_at_epoch_ms,
            completed_at_epoch_ms,
        }
    }

    fn boom(context: &'static str) -> CrabError {
        CrabError::InvariantViolation {
            context,
            message: "boom".to_string(),
        }
    }

    fn insert_running_run(
        runtime: &mut FakeRuntime,
        session_id: &str,
        active_physical_session_id: &str,
        run_logical_session_id: &str,
        run_id: &str,
        queued_at_epoch_ms: u64,
        started_at_epoch_ms: Option<u64>,
    ) {
        runtime.sessions.push(session(
            session_id,
            LaneState::Running,
            Some(active_physical_session_id),
        ));
        runtime.runs_by_session.insert(
            session_id.to_string(),
            vec![run(
                run_logical_session_id,
                run_id,
                RunStatus::Running,
                queued_at_epoch_ms,
                started_at_epoch_ms,
                None,
            )],
        );
    }

    #[test]
    fn reconciles_stale_running_runs_and_repairs_session_lane_state() {
        let mut runtime = FakeRuntime::new();
        insert_running_run(
            &mut runtime,
            "discord:channel:a",
            "phys-a",
            "discord:channel:a",
            "run-1",
            1_739_173_100_000,
            Some(1_739_173_100_100),
        );
        runtime
            .next_sequence
            .insert(("discord:channel:a".to_string(), "run-1".to_string()), 7);

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 90_000)
            .expect("stale run reconciliation should succeed");

        assert_eq!(
            outcome,
            StartupReconciliationOutcome {
                recovered_runs: vec![StartupReconciliationRecoveredRun {
                    logical_session_id: "discord:channel:a".to_string(),
                    run_id: "run-1".to_string(),
                    previous_status: RunStatus::Running,
                    recovered_status: RunStatus::Cancelled,
                }],
                repaired_session_ids: vec!["discord:channel:a".to_string()],
                repaired_physical_sessions: Vec::new(),
            }
        );
        assert_eq!(runtime.persisted_runs.len(), 1);
        assert_eq!(runtime.persisted_runs[0].status, RunStatus::Cancelled);
        assert_eq!(
            runtime.persisted_runs[0].completed_at_epoch_ms,
            Some(1_739_173_300_000)
        );
        assert_eq!(runtime.appended_events.len(), 1);
        let event = &runtime.appended_events[0];
        assert_eq!(event.sequence, 7);
        assert_eq!(event.kind, crate::EventKind::RunState);
        assert_eq!(event.source, crate::EventSource::System);
        assert_eq!(event.turn_id, Some("turn:run-1".to_string()));
        assert_eq!(event.lane_id, Some("discord:channel:a".to_string()));
        assert_eq!(event.physical_session_id, Some("phys-1".to_string()));
        assert_eq!(event.backend, Some(BackendKind::Codex));
        assert_eq!(event.resolved_model, Some("gpt-5-codex".to_string()));
        assert_eq!(event.resolved_reasoning_level, Some("medium".to_string()));
        assert_eq!(event.profile_source, Some("session".to_string()));
        assert_eq!(
            event.payload.get("reason"),
            Some(&"startup_recovered_as_interrupted".to_string())
        );
        assert_eq!(runtime.calls[0], "restart_backends");
        assert_eq!(
            runtime.repaired_sessions,
            vec!["discord:channel:a".to_string()]
        );
    }

    #[test]
    fn uses_queued_timestamp_when_started_timestamp_is_missing() {
        let mut runtime = FakeRuntime::new();
        insert_running_run(
            &mut runtime,
            "discord:channel:a",
            "phys-a",
            "discord:channel:a",
            "run-queued-only",
            1_739_173_000_000,
            None,
        );

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("queued timestamp should be used when started is missing");
        assert_eq!(outcome.recovered_runs.len(), 1);
        assert_eq!(runtime.persisted_runs[0].status, RunStatus::Cancelled);
    }

    #[test]
    fn processes_sessions_and_runs_in_sorted_order() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:z",
            LaneState::Running,
            Some("phys-z"),
        ));
        runtime.sessions.push(session(
            "discord:channel:a",
            LaneState::Running,
            Some("phys-a"),
        ));
        runtime.runs_by_session.insert(
            "discord:channel:a".to_string(),
            vec![
                run(
                    "discord:channel:a",
                    "run-2",
                    RunStatus::Running,
                    1_739_173_000_000,
                    Some(1_739_173_000_200),
                    None,
                ),
                run(
                    "discord:channel:a",
                    "run-1",
                    RunStatus::Running,
                    1_739_173_000_000,
                    Some(1_739_173_000_100),
                    None,
                ),
            ],
        );
        runtime.runs_by_session.insert(
            "discord:channel:z".to_string(),
            vec![run(
                "discord:channel:z",
                "run-3",
                RunStatus::Running,
                1_739_173_000_000,
                Some(1_739_173_000_300),
                None,
            )],
        );
        runtime
            .next_sequence
            .insert(("discord:channel:a".to_string(), "run-1".to_string()), 2);
        runtime
            .next_sequence
            .insert(("discord:channel:a".to_string(), "run-2".to_string()), 3);
        runtime
            .next_sequence
            .insert(("discord:channel:z".to_string(), "run-3".to_string()), 4);

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("reconciliation should run deterministically");

        let recovered_ids: Vec<(String, String)> = outcome
            .recovered_runs
            .iter()
            .map(|recovered| {
                (
                    recovered.logical_session_id.clone(),
                    recovered.run_id.clone(),
                )
            })
            .collect();
        assert_eq!(
            recovered_ids,
            vec![
                ("discord:channel:a".to_string(), "run-1".to_string()),
                ("discord:channel:a".to_string(), "run-2".to_string()),
                ("discord:channel:z".to_string(), "run-3".to_string()),
            ]
        );

        let persisted_ids: Vec<(String, String)> = runtime
            .persisted_runs
            .iter()
            .map(|persisted| (persisted.logical_session_id.clone(), persisted.id.clone()))
            .collect();
        assert_eq!(persisted_ids, recovered_ids);
        assert_eq!(
            outcome.repaired_session_ids,
            vec![
                "discord:channel:a".to_string(),
                "discord:channel:z".to_string()
            ]
        );
    }

    #[test]
    fn leaves_recent_runs_unchanged() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:a",
            LaneState::Idle,
            Some("phys-a"),
        ));
        runtime.runs_by_session.insert(
            "discord:channel:a".to_string(),
            vec![run(
                "discord:channel:a",
                "run-recent",
                RunStatus::Running,
                1_739_173_290_000,
                Some(1_739_173_295_000),
                None,
            )],
        );

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("recent run should not reconcile");
        assert!(outcome.recovered_runs.is_empty());
        assert!(outcome.repaired_session_ids.is_empty());
        assert!(runtime.persisted_runs.is_empty());
        assert!(runtime.appended_events.is_empty());
        assert!(runtime.repaired_sessions.is_empty());
    }

    #[test]
    fn repairs_non_idle_session_lane_states_even_without_stale_run() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:cancelling",
            LaneState::Cancelling,
            Some("phys-c"),
        ));
        runtime.runs_by_session.insert(
            "discord:channel:cancelling".to_string(),
            vec![run(
                "discord:channel:cancelling",
                "run-complete",
                RunStatus::Succeeded,
                1_739_173_000_000,
                Some(1_739_173_010_000),
                Some(1_739_173_020_000),
            )],
        );

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("non-idle lane states should repair on startup");
        assert!(outcome.recovered_runs.is_empty());
        assert_eq!(
            outcome.repaired_session_ids,
            vec!["discord:channel:cancelling".to_string()]
        );
        assert!(outcome.repaired_physical_sessions.is_empty());
        assert_eq!(
            runtime.repaired_sessions,
            vec!["discord:channel:cancelling".to_string()]
        );
    }

    #[test]
    fn repairs_orphan_active_physical_session_id_to_latest_successful_run() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:orphan",
            LaneState::Idle,
            Some("phys-orphan"),
        ));
        runtime.runs_by_session.insert(
            "discord:channel:orphan".to_string(),
            vec![
                run_with_physical(
                    "discord:channel:orphan",
                    "run-older-success",
                    "phys-good",
                    RunStatus::Succeeded,
                    1_739_173_000_000,
                    Some(1_739_173_000_010),
                    Some(1_739_173_000_020),
                ),
                run_with_physical(
                    "discord:channel:orphan",
                    "run-latest-fail",
                    "phys-orphan",
                    RunStatus::Failed,
                    1_739_173_100_000,
                    Some(1_739_173_100_010),
                    Some(1_739_173_100_020),
                ),
            ],
        );

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("orphan physical session repair should succeed");

        assert!(outcome.recovered_runs.is_empty());
        assert!(outcome.repaired_session_ids.is_empty());
        assert_eq!(
            outcome.repaired_physical_sessions,
            vec![StartupReconciliationRepairedPhysicalSession {
                logical_session_id: "discord:channel:orphan".to_string(),
                previous_physical_session_id: "phys-orphan".to_string(),
                repaired_physical_session_id: "phys-good".to_string(),
            }]
        );
        assert_eq!(
            runtime.repaired_active_physical_sessions,
            vec![(
                "discord:channel:orphan".to_string(),
                "phys-good".to_string()
            )]
        );
    }

    #[test]
    fn does_not_repair_active_physical_session_id_when_it_has_succeeded_before() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:active-success",
            LaneState::Idle,
            Some("phys-same"),
        ));
        runtime.runs_by_session.insert(
            "discord:channel:active-success".to_string(),
            vec![
                run_with_physical(
                    "discord:channel:active-success",
                    "run-1",
                    "phys-same",
                    RunStatus::Succeeded,
                    1_739_173_000_000,
                    Some(1_739_173_000_010),
                    Some(1_739_173_000_200),
                ),
                // Older completion timestamp than the first successful run, so it should not win
                // the "latest successful" selection.
                run_with_physical(
                    "discord:channel:active-success",
                    "run-2",
                    "phys-same",
                    RunStatus::Succeeded,
                    1_739_172_900_000,
                    Some(1_739_172_900_010),
                    Some(1_739_172_900_020),
                ),
                run_with_physical(
                    "discord:channel:active-success",
                    "run-3",
                    "phys-same",
                    RunStatus::Failed,
                    1_739_173_100_000,
                    Some(1_739_173_100_010),
                    Some(1_739_173_100_020),
                ),
            ],
        );

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("reconciliation should succeed");

        assert!(outcome.recovered_runs.is_empty());
        assert!(outcome.repaired_session_ids.is_empty());
        assert!(outcome.repaired_physical_sessions.is_empty());
        assert!(runtime.repaired_active_physical_sessions.is_empty());
    }

    #[test]
    fn success_and_failure_tracking_ignores_mismatched_or_missing_physical_session_ids() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:tracking",
            LaneState::Idle,
            Some("phys-active"),
        ));

        let mut missing_physical = run(
            "discord:channel:tracking",
            "run-0",
            RunStatus::Succeeded,
            1_739_173_000_000,
            Some(1_739_173_000_010),
            Some(1_739_173_000_020),
        );
        missing_physical.physical_session_id = None;

        runtime.runs_by_session.insert(
            "discord:channel:tracking".to_string(),
            vec![
                missing_physical,
                run_with_physical(
                    "discord:channel:tracking",
                    "run-1",
                    "phys-other",
                    RunStatus::Succeeded,
                    1_739_173_100_000,
                    Some(1_739_173_100_010),
                    Some(1_739_173_100_020),
                ),
                run_with_physical(
                    "discord:channel:tracking",
                    "run-2",
                    "phys-other",
                    RunStatus::Failed,
                    1_739_173_200_000,
                    Some(1_739_173_200_010),
                    Some(1_739_173_200_020),
                ),
            ],
        );

        let outcome = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect("reconciliation should succeed");

        assert!(outcome.recovered_runs.is_empty());
        assert!(outcome.repaired_session_ids.is_empty());
        assert!(outcome.repaired_physical_sessions.is_empty());
        assert!(runtime.repaired_active_physical_sessions.is_empty());
    }

    #[test]
    fn validates_reconciliation_inputs() {
        let mut runtime = FakeRuntime::new();

        let now_error = execute_startup_reconciliation(&mut runtime, 0, 60_000)
            .expect_err("now timestamp must be > 0");
        assert_eq!(
            now_error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );

        let grace_error = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 0)
            .expect_err("grace period must be > 0");
        assert_eq!(
            grace_error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message: "grace_period_ms must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn restart_failures_propagate_before_other_reconciliation_steps() {
        let mut runtime = FakeRuntime::new();
        runtime.restart_backend_result = Err(boom("restart_backends"));

        let error = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect_err("restart failure should fail reconciliation");
        assert_eq!(error, boom("restart_backends"));
        assert_eq!(runtime.calls, vec!["restart_backends".to_string()]);
    }

    #[test]
    fn rejects_mismatched_run_and_session_identity() {
        let mut runtime = FakeRuntime::new();
        insert_running_run(
            &mut runtime,
            "discord:channel:a",
            "phys-a",
            "discord:channel:b",
            "run-1",
            1_739_173_000_000,
            Some(1_739_173_000_500),
        );

        let error = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect_err("run/session mismatch should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message: "run run-1 belongs to discord:channel:b, expected discord:channel:a"
                    .to_string(),
            }
        );
    }

    #[test]
    fn rejects_zero_next_event_sequence() {
        let mut runtime = FakeRuntime::new();
        insert_running_run(
            &mut runtime,
            "discord:channel:a",
            "phys-a",
            "discord:channel:a",
            "run-1",
            1_739_173_000_000,
            Some(1_739_173_000_100),
        );
        runtime
            .next_sequence
            .insert(("discord:channel:a".to_string(), "run-1".to_string()), 0);

        let error = execute_startup_reconciliation(&mut runtime, 1_739_173_300_000, 60_000)
            .expect_err("zero event sequence should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message:
                    "next event sequence must be greater than 0 for run discord:channel:a/run-1"
                        .to_string(),
            }
        );
    }

    #[test]
    fn propagates_persist_event_and_clear_failures() {
        let mut persist_runtime = FakeRuntime::new();
        insert_running_run(
            &mut persist_runtime,
            "discord:channel:a",
            "phys-a",
            "discord:channel:a",
            "run-1",
            1_739_173_000_000,
            Some(1_739_173_000_100),
        );
        persist_runtime.persist_run_result = Err(boom("persist_run"));
        let persist_error =
            execute_startup_reconciliation(&mut persist_runtime, 1_739_173_300_000, 60_000)
                .expect_err("persist failures should propagate");
        assert_eq!(persist_error, boom("persist_run"));

        let mut append_runtime = FakeRuntime::new();
        append_runtime.sessions.push(session(
            "discord:channel:b",
            LaneState::Running,
            Some("phys-b"),
        ));
        append_runtime.runs_by_session.insert(
            "discord:channel:b".to_string(),
            vec![run(
                "discord:channel:b",
                "run-2",
                RunStatus::Running,
                1_739_173_000_000,
                Some(1_739_173_000_100),
                None,
            )],
        );
        append_runtime.append_event_result = Err(boom("append_event"));
        let append_error =
            execute_startup_reconciliation(&mut append_runtime, 1_739_173_300_000, 60_000)
                .expect_err("append failures should propagate");
        assert_eq!(append_error, boom("append_event"));

        let mut repair_runtime = FakeRuntime::new();
        repair_runtime.sessions.push(session(
            "discord:channel:c",
            LaneState::Cancelling,
            Some("phys-c"),
        ));
        repair_runtime
            .runs_by_session
            .insert("discord:channel:c".to_string(), Vec::new());
        repair_runtime.repair_session_result = Err(boom("repair_session"));
        let repair_error =
            execute_startup_reconciliation(&mut repair_runtime, 1_739_173_300_000, 60_000)
                .expect_err("repair failures should propagate");
        assert_eq!(repair_error, boom("repair_session"));
    }

    #[test]
    fn propagates_list_and_sequence_lookup_failures() {
        let mut list_sessions_runtime = FakeRuntime::new();
        list_sessions_runtime.list_sessions_result = Err(boom("list_sessions"));
        let list_sessions_error =
            execute_startup_reconciliation(&mut list_sessions_runtime, 1_739_173_300_000, 60_000)
                .expect_err("list sessions failures should propagate");
        assert_eq!(list_sessions_error, boom("list_sessions"));

        let mut list_runs_runtime = FakeRuntime::new();
        list_runs_runtime.sessions.push(session(
            "discord:channel:a",
            LaneState::Running,
            Some("phys-a"),
        ));
        list_runs_runtime.list_runs_result = Err(boom("list_runs_for_session"));
        let list_runs_error =
            execute_startup_reconciliation(&mut list_runs_runtime, 1_739_173_300_000, 60_000)
                .expect_err("list runs failures should propagate");
        assert_eq!(list_runs_error, boom("list_runs_for_session"));

        let mut next_sequence_runtime = FakeRuntime::new();
        insert_running_run(
            &mut next_sequence_runtime,
            "discord:channel:b",
            "phys-b",
            "discord:channel:b",
            "run-1",
            1_739_173_000_000,
            Some(1_739_173_000_100),
        );
        next_sequence_runtime.next_sequence_result = Err(boom("next_event_sequence"));
        let next_sequence_error =
            execute_startup_reconciliation(&mut next_sequence_runtime, 1_739_173_300_000, 60_000)
                .expect_err("sequence lookup failures should propagate");
        assert_eq!(next_sequence_error, boom("next_event_sequence"));
    }

    #[test]
    fn detects_overflow_when_computing_stale_deadline() {
        let mut runtime = FakeRuntime::new();
        runtime.sessions.push(session(
            "discord:channel:overflow",
            LaneState::Running,
            Some("phys-o"),
        ));
        runtime.runs_by_session.insert(
            "discord:channel:overflow".to_string(),
            vec![run(
                "discord:channel:overflow",
                "run-overflow",
                RunStatus::Running,
                u64::MAX - 5,
                Some(u64::MAX - 5),
                None,
            )],
        );

        let error = execute_startup_reconciliation(&mut runtime, u64::MAX, 10)
            .expect_err("overflow should be surfaced");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message:
                    "grace-period overflow while evaluating stale run discord:channel:overflow/run-overflow"
                        .to_string(),
            }
        );
    }
}
