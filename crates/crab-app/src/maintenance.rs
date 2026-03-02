use std::collections::BTreeMap;

use crab_core::{
    execute_heartbeat_cycle, execute_startup_reconciliation, ActiveRunHeartbeat, BackendHeartbeat,
    BackendKind, CrabError, CrabResult, DispatcherHeartbeat, EventEnvelope, EventKind, EventSource,
    HeartbeatOutcome, HeartbeatPolicy, HeartbeatRuntime, LaneState, LogicalSession, Run, RunStatus,
    StartupReconciliationOutcome, StartupReconciliationRuntime,
};

use crate::{compose_runtime_with_queue_limit, AppComposition, DEFAULT_LANE_QUEUE_LIMIT};

const STARTUP_GRACE_PERIOD_KEY: &str = "CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS";
const HEARTBEAT_INTERVAL_KEY: &str = "CRAB_HEARTBEAT_INTERVAL_SECS";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatLoopState {
    heartbeat_interval_ms: u64,
    next_heartbeat_due_at_epoch_ms: u64,
    last_dispatch_at_epoch_ms: u64,
}

impl HeartbeatLoopState {
    pub fn new(heartbeat_interval_secs: u64, now_epoch_ms: u64) -> CrabResult<Self> {
        if heartbeat_interval_secs == 0 {
            return Err(CrabError::InvalidConfig {
                key: HEARTBEAT_INTERVAL_KEY,
                value: heartbeat_interval_secs.to_string(),
                reason: "must be greater than 0",
            });
        }
        if now_epoch_ms == 0 {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_loop_state_new",
                message: "now_epoch_ms must be greater than 0".to_string(),
            });
        }

        let heartbeat_interval_ms =
            seconds_to_millis(HEARTBEAT_INTERVAL_KEY, heartbeat_interval_secs)?;
        let next_heartbeat_due_at_epoch_ms = now_epoch_ms
            .checked_add(heartbeat_interval_ms)
            .ok_or(CrabError::InvariantViolation {
                context: "heartbeat_loop_state_new",
                message: "next heartbeat due time overflow".to_string(),
            })?;

        Ok(Self {
            heartbeat_interval_ms,
            next_heartbeat_due_at_epoch_ms,
            last_dispatch_at_epoch_ms: now_epoch_ms,
        })
    }

    #[must_use]
    #[cfg(test)]
    fn next_heartbeat_due_at_epoch_ms(&self) -> u64 {
        self.next_heartbeat_due_at_epoch_ms
    }

    #[must_use]
    pub fn last_dispatch_at_epoch_ms(&self) -> u64 {
        self.last_dispatch_at_epoch_ms
    }

    #[cfg(test)]
    fn record_dispatch(&mut self, dispatched_at_epoch_ms: u64) -> CrabResult<()> {
        if dispatched_at_epoch_ms == 0 {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_loop_state_record_dispatch",
                message: "dispatched_at_epoch_ms must be greater than 0".to_string(),
            });
        }
        self.last_dispatch_at_epoch_ms = dispatched_at_epoch_ms;
        Ok(())
    }

    fn advance_after_due_tick(&mut self, now_epoch_ms: u64) -> CrabResult<()> {
        while self.next_heartbeat_due_at_epoch_ms <= now_epoch_ms {
            self.next_heartbeat_due_at_epoch_ms = self
                .next_heartbeat_due_at_epoch_ms
                .checked_add(self.heartbeat_interval_ms)
                .ok_or(CrabError::InvariantViolation {
                    context: "heartbeat_loop_state_tick",
                    message: "heartbeat schedule overflow".to_string(),
                })?;
        }
        Ok(())
    }
}

pub struct BootRuntime {
    pub composition: AppComposition,
    pub startup_reconciliation: StartupReconciliationOutcome,
    pub heartbeat_loop_state: HeartbeatLoopState,
}

pub fn boot_runtime(
    config: &crab_core::RuntimeConfig,
    bot_user_id: &str,
    now_epoch_ms: u64,
) -> CrabResult<BootRuntime> {
    boot_runtime_with_queue_limit(config, bot_user_id, DEFAULT_LANE_QUEUE_LIMIT, now_epoch_ms)
}

pub fn boot_runtime_with_queue_limit(
    config: &crab_core::RuntimeConfig,
    bot_user_id: &str,
    lane_queue_limit: usize,
    now_epoch_ms: u64,
) -> CrabResult<BootRuntime> {
    let mut composition = compose_runtime_with_queue_limit(config, bot_user_id, lane_queue_limit)?;
    let startup_reconciliation =
        run_startup_reconciliation_on_boot(&mut composition, now_epoch_ms)?;
    let heartbeat_loop_state =
        HeartbeatLoopState::new(composition.heartbeat_policy.interval_secs, now_epoch_ms)?;

    Ok(BootRuntime {
        composition,
        startup_reconciliation,
        heartbeat_loop_state,
    })
}

pub fn run_startup_reconciliation_on_boot(
    composition: &mut AppComposition,
    now_epoch_ms: u64,
) -> CrabResult<StartupReconciliationOutcome> {
    let grace_period_ms = seconds_to_millis(
        STARTUP_GRACE_PERIOD_KEY,
        composition.startup_reconciliation_policy.grace_period_secs,
    )?;
    let mut runtime = StartupRuntimeAdapter { composition };
    execute_startup_reconciliation(&mut runtime, now_epoch_ms, grace_period_ms)
}

pub fn run_heartbeat_if_due(
    composition: &mut AppComposition,
    loop_state: &mut HeartbeatLoopState,
    now_epoch_ms: u64,
) -> CrabResult<Option<HeartbeatOutcome>> {
    if now_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "runtime_heartbeat_tick",
            message: "now_epoch_ms must be greater than 0".to_string(),
        });
    }

    if now_epoch_ms < loop_state.next_heartbeat_due_at_epoch_ms {
        return Ok(None);
    }

    let policy = HeartbeatPolicy {
        run_stall_timeout_secs: composition.heartbeat_policy.run_stall_timeout_secs,
        backend_stall_timeout_secs: composition.heartbeat_policy.backend_stall_timeout_secs,
        dispatcher_stall_timeout_secs: composition.heartbeat_policy.dispatcher_stall_timeout_secs,
    };

    let mut runtime = HeartbeatRuntimeAdapter {
        composition,
        now_epoch_ms,
        last_dispatch_at_epoch_ms: &mut loop_state.last_dispatch_at_epoch_ms,
    };
    let outcome = execute_heartbeat_cycle(&mut runtime, &policy, now_epoch_ms)?;
    loop_state.advance_after_due_tick(now_epoch_ms)?;
    Ok(Some(outcome))
}

struct StartupRuntimeAdapter<'a> {
    composition: &'a mut AppComposition,
}

impl StartupReconciliationRuntime for StartupRuntimeAdapter<'_> {
    fn restart_backend_managers(&mut self) -> CrabResult<()> {
        Ok(())
    }

    fn list_sessions(&self) -> CrabResult<Vec<LogicalSession>> {
        load_sessions(&self.composition.state_stores.session_store)
    }

    fn list_runs_for_session(&self, logical_session_id: &str) -> CrabResult<Vec<Run>> {
        load_runs_for_session(&self.composition.state_stores.run_store, logical_session_id)
    }

    fn persist_run(&mut self, run: &Run) -> CrabResult<()> {
        self.composition.state_stores.run_store.upsert_run(run)
    }

    fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64> {
        next_event_sequence(
            &self.composition.state_stores.event_store,
            logical_session_id,
            run_id,
        )
    }

    fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()> {
        self.composition
            .state_stores
            .event_store
            .append_event(event)
    }

    fn repair_session_lane_state(&mut self, logical_session_id: &str) -> CrabResult<()> {
        let Some(mut session) = self
            .composition
            .state_stores
            .session_store
            .get_session(logical_session_id)?
        else {
            return Err(CrabError::InvariantViolation {
                context: "startup_reconciliation_runtime",
                message: format!("session {logical_session_id} not found"),
            });
        };

        session.lane_state = LaneState::Idle;
        session.queued_run_count = queued_run_count_for_session(
            &self.composition.state_stores.run_store,
            logical_session_id,
        )? as u32;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)
    }

    fn repair_active_physical_session_id(
        &mut self,
        logical_session_id: &str,
        physical_session_id: &str,
    ) -> CrabResult<()> {
        let Some(mut session) = self
            .composition
            .state_stores
            .session_store
            .get_session(logical_session_id)?
        else {
            return Err(CrabError::InvariantViolation {
                context: "startup_reconciliation_runtime",
                message: format!("session {logical_session_id} not found"),
            });
        };

        session.active_physical_session_id = Some(physical_session_id.to_string());
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)
    }
}

struct HeartbeatRuntimeAdapter<'a> {
    composition: &'a mut AppComposition,
    now_epoch_ms: u64,
    last_dispatch_at_epoch_ms: &'a mut u64,
}

impl HeartbeatRuntime for HeartbeatRuntimeAdapter<'_> {
    fn list_active_runs(&self) -> CrabResult<Vec<ActiveRunHeartbeat>> {
        let sessions = load_sessions(&self.composition.state_stores.session_store)?;
        let mut active = Vec::new();

        for session in sessions {
            let runs =
                load_runs_for_session(&self.composition.state_stores.run_store, &session.id)?;
            let running = single_running_run(&session.id, &runs)?;

            match (session.lane_state, running) {
                (LaneState::Running | LaneState::Cancelling, Some(run)) => {
                    let last_progress_at_epoch_ms = last_progress_at_epoch_ms(
                        &self.composition.state_stores.event_store,
                        &run,
                    )?;
                    active.push(ActiveRunHeartbeat {
                        logical_session_id: session.id,
                        run_id: run.id,
                        lane_state: session.lane_state,
                        backend: run.profile.resolved_profile.backend,
                        last_progress_at_epoch_ms,
                    });
                }
                (LaneState::Running | LaneState::Cancelling, None) => {
                    return Err(CrabError::InvariantViolation {
                        context: "heartbeat_runtime_list_active_runs",
                        message: format!(
                            "session {} is {:?} but has no running run",
                            session.id, session.lane_state
                        ),
                    });
                }
                (LaneState::Idle | LaneState::Rotating, Some(run)) => {
                    return Err(CrabError::InvariantViolation {
                        context: "heartbeat_runtime_list_active_runs",
                        message: format!(
                            "run {}/{} is running while lane state is {:?}",
                            session.id, run.id, session.lane_state
                        ),
                    });
                }
                (LaneState::Idle | LaneState::Rotating, None) => {}
            }
        }

        Ok(active)
    }

    fn request_cancel_active_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<()> {
        let mut session = require_session(
            &self.composition.state_stores.session_store,
            logical_session_id,
            "heartbeat_runtime_cancel",
        )?;
        let mut run = require_run(
            &self.composition.state_stores.run_store,
            logical_session_id,
            run_id,
            "heartbeat_runtime_cancel",
        )?;

        if run.status != RunStatus::Running || run.completed_at_epoch_ms.is_some() {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_runtime_cancel",
                message: format!("run {logical_session_id}/{run_id} is not actively running"),
            });
        }

        if session.lane_state == LaneState::Cancelling {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_runtime_cancel",
                message: format!("run {logical_session_id}/{run_id} is already cancelling"),
            });
        }

        if session.lane_state != LaneState::Running {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_runtime_cancel",
                message: format!(
                    "session {} has lane state {:?}; expected running",
                    session.id, session.lane_state
                ),
            });
        }

        session.lane_state = LaneState::Cancelling;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        let mut payload = BTreeMap::new();
        payload.insert(
            "action".to_string(),
            "cancel_requested_due_to_stall".to_string(),
        );
        payload.insert("run_id".to_string(), run.id.clone());
        append_runtime_event(
            &self.composition.state_stores.event_store,
            &run,
            EventKind::Heartbeat,
            payload,
            EventSource::System,
            self.now_epoch_ms,
            "heartbeat_cancel",
        )?;

        run.status = RunStatus::Running;
        self.composition.state_stores.run_store.upsert_run(&run)
    }

    fn hard_stop_run_and_rotate(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
        reason: &str,
    ) -> CrabResult<()> {
        let mut session = require_session(
            &self.composition.state_stores.session_store,
            logical_session_id,
            "heartbeat_runtime_hard_stop",
        )?;
        let mut run = require_run(
            &self.composition.state_stores.run_store,
            logical_session_id,
            run_id,
            "heartbeat_runtime_hard_stop",
        )?;

        run.status = RunStatus::Cancelled;
        run.completed_at_epoch_ms = Some(self.now_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut run_state_payload = BTreeMap::new();
        run_state_payload.insert("state".to_string(), "cancelled".to_string());
        run_state_payload.insert("reason".to_string(), reason.to_string());
        append_runtime_event(
            &self.composition.state_stores.event_store,
            &run,
            EventKind::RunState,
            run_state_payload,
            EventSource::System,
            self.now_epoch_ms,
            "heartbeat_hard_stop_state",
        )?;

        session.active_physical_session_id = None;
        session.lane_state = LaneState::Idle;
        session.last_activity_epoch_ms = self.now_epoch_ms;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count_unchecked(logical_session_id) as u32;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        let mut payload = BTreeMap::new();
        payload.insert(
            "action".to_string(),
            "hard_stop_and_rotate_due_to_cancel_failure".to_string(),
        );
        payload.insert("reason".to_string(), reason.to_string());
        append_runtime_event(
            &self.composition.state_stores.event_store,
            &run,
            EventKind::Heartbeat,
            payload,
            EventSource::System,
            self.now_epoch_ms,
            "heartbeat_hard_stop",
        )
    }

    fn list_backend_heartbeats(&self) -> CrabResult<Vec<BackendHeartbeat>> {
        Ok(vec![BackendHeartbeat {
            backend: BackendKind::Claude,
            is_persistent: false,
            is_healthy: true,
            last_healthy_at_epoch_ms: self.now_epoch_ms,
        }])
    }

    fn restart_backend_manager(&mut self, _backend: BackendKind) -> CrabResult<()> {
        Ok(())
    }

    fn dispatcher_heartbeat(&self) -> CrabResult<DispatcherHeartbeat> {
        Ok(DispatcherHeartbeat {
            queued_run_count: self.composition.scheduler.total_queued_count(),
            active_lane_count: self.composition.scheduler.active_lane_count(),
            last_dispatch_at_epoch_ms: *self.last_dispatch_at_epoch_ms,
        })
    }

    fn nudge_dispatcher(&mut self) -> CrabResult<()> {
        *self.last_dispatch_at_epoch_ms = self.now_epoch_ms;
        Ok(())
    }
}

fn seconds_to_millis(key: &'static str, value_secs: u64) -> CrabResult<u64> {
    value_secs
        .checked_mul(1_000)
        .ok_or(CrabError::InvalidConfig {
            key,
            value: value_secs.to_string(),
            reason: "must fit in milliseconds as a u64",
        })
}

fn load_sessions(session_store: &crab_store::SessionStore) -> CrabResult<Vec<LogicalSession>> {
    let mut sessions = Vec::new();
    let session_ids = session_store.list_session_ids()?;
    for session_id in session_ids {
        if let Some(session) = session_store.get_session(&session_id)? {
            sessions.push(session);
        }
    }
    sessions.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(sessions)
}

fn load_runs_for_session(
    run_store: &crab_store::RunStore,
    logical_session_id: &str,
) -> CrabResult<Vec<Run>> {
    let mut runs = Vec::new();
    let run_ids = run_store.list_run_ids(logical_session_id)?;
    for run_id in run_ids {
        if let Some(run) = run_store.get_run(logical_session_id, &run_id)? {
            runs.push(run);
        }
    }
    runs.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(runs)
}

fn require_session(
    session_store: &crab_store::SessionStore,
    logical_session_id: &str,
    context: &'static str,
) -> CrabResult<LogicalSession> {
    session_store
        .get_session(logical_session_id)?
        .ok_or_else(|| CrabError::InvariantViolation {
            context,
            message: format!("session {logical_session_id} not found"),
        })
}

fn require_run(
    run_store: &crab_store::RunStore,
    logical_session_id: &str,
    run_id: &str,
    context: &'static str,
) -> CrabResult<Run> {
    run_store
        .get_run(logical_session_id, run_id)?
        .ok_or_else(|| CrabError::InvariantViolation {
            context,
            message: format!("run {logical_session_id}/{run_id} not found"),
        })
}

fn single_running_run(logical_session_id: &str, runs: &[Run]) -> CrabResult<Option<Run>> {
    let mut running: Option<Run> = None;
    for run in runs {
        if run.status != RunStatus::Running || run.completed_at_epoch_ms.is_some() {
            continue;
        }
        if let Some(existing) = running.as_ref() {
            return Err(CrabError::InvariantViolation {
                context: "heartbeat_runtime_list_active_runs",
                message: format!(
                    "session {logical_session_id} has multiple running runs: {} and {}",
                    existing.id, run.id
                ),
            });
        }
        running = Some(run.clone());
    }
    Ok(running)
}

fn last_progress_at_epoch_ms(event_store: &crab_store::EventStore, run: &Run) -> CrabResult<u64> {
    let base_progress = run.started_at_epoch_ms.unwrap_or(run.queued_at_epoch_ms);
    let events = event_store.replay_run(&run.logical_session_id, &run.id)?;
    let event_progress = events.last().map_or(0, |event| event.emitted_at_epoch_ms);
    Ok(base_progress.max(event_progress))
}

fn queued_run_count_for_session(
    run_store: &crab_store::RunStore,
    logical_session_id: &str,
) -> CrabResult<usize> {
    let runs = load_runs_for_session(run_store, logical_session_id)?;
    Ok(runs
        .iter()
        .filter(|run| run.status == RunStatus::Queued)
        .count())
}

fn next_event_sequence(
    event_store: &crab_store::EventStore,
    logical_session_id: &str,
    run_id: &str,
) -> CrabResult<u64> {
    let events = event_store.replay_run(logical_session_id, run_id)?;
    Ok(events.last().map_or(1, |event| event.sequence + 1))
}

fn append_runtime_event(
    event_store: &crab_store::EventStore,
    run: &Run,
    kind: EventKind,
    payload: BTreeMap<String, String>,
    source: EventSource,
    emitted_at_epoch_ms: u64,
    event_namespace: &str,
) -> CrabResult<()> {
    let sequence = next_event_sequence(event_store, &run.logical_session_id, &run.id)?;
    let event = EventEnvelope {
        payload,
        kind,
        source,
        sequence,
        emitted_at_epoch_ms,
        event_id: format!(
            "evt:{event_namespace}:{}:{}:{sequence}",
            run.logical_session_id, run.id
        ),
        run_id: run.id.clone(),
        turn_id: None,
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
        profile: Some(run.profile.clone()),
        idempotency_key: Some(format!(
            "{event_namespace}:{}:{}:{sequence}",
            run.logical_session_id, run.id
        )),
    };
    event_store.append_event(&event)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::fs;
    use std::path::{Path, PathBuf};

    use crab_core::{
        BackendKind, CrabError, HeartbeatRuntime, InferenceProfile, LaneState,
        OwnerProfileMetadata, ProfileValueSource, ReasoningLevel, Run, RunProfileTelemetry,
        RunStatus, StartupReconciliationRuntime, TokenAccounting,
    };
    use crab_scheduler::QueuedRun;

    use super::{
        boot_runtime, run_heartbeat_if_due, run_startup_reconciliation_on_boot, seconds_to_millis,
        HeartbeatLoopState,
    };
    use crate::composition::compose_runtime;
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};

    fn sample_profile() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-sonnet-4-5".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        }
    }

    fn sample_telemetry(sender_id: &str) -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: Some(sample_profile()),
            resolved_profile: sample_profile(),
            backend_source: ProfileValueSource::SessionProfile,
            model_source: ProfileValueSource::SessionProfile,
            reasoning_level_source: ProfileValueSource::SessionProfile,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: sender_id.to_string(),
            sender_is_owner: true,
            resolved_owner_profile: Some(OwnerProfileMetadata {
                machine_location: Some("Paris, France".to_string()),
                machine_timezone: Some("Europe/Paris".to_string()),
                default_backend: Some(BackendKind::Claude),
                default_model: Some("claude-sonnet-4-5".to_string()),
                default_reasoning_level: Some(ReasoningLevel::Medium),
            }),
        }
    }

    fn sample_token_accounting() -> TokenAccounting {
        TokenAccounting {
            input_tokens: 100,
            output_tokens: 50,
            total_tokens: 150,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        }
    }

    fn running_session(logical_session_id: &str, now_epoch_ms: u64) -> crab_core::LogicalSession {
        crab_core::LogicalSession {
            id: logical_session_id.to_string(),
            active_backend: BackendKind::Claude,
            active_profile: sample_profile(),
            active_physical_session_id: Some("physical-1".to_string()),
            last_successful_checkpoint_id: None,
            lane_state: LaneState::Running,
            queued_run_count: 0,
            last_activity_epoch_ms: now_epoch_ms,
            token_accounting: sample_token_accounting(),
            has_injected_bootstrap: false,
        }
    }

    fn running_run(logical_session_id: &str, run_id: &str, started_at_epoch_ms: u64) -> Run {
        Run {
            id: run_id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            physical_session_id: Some("physical-1".to_string()),
            status: RunStatus::Running,
            user_input: "hello".to_string(),
            delivery_channel_id: None,
            profile: sample_telemetry("111111111111111111"),
            queued_at_epoch_ms: started_at_epoch_ms.saturating_sub(500),
            started_at_epoch_ms: Some(started_at_epoch_ms),
            completed_at_epoch_ms: None,
        }
    }

    fn hex_encode(bytes: &[u8]) -> String {
        const HEX: [char; 16] = [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
        ];
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            let upper = usize::from(byte >> 4);
            let lower = usize::from(byte & 0x0f);
            output.push(HEX[upper]);
            output.push(HEX[lower]);
        }
        output
    }

    fn state_root(workspace: &TempWorkspace) -> PathBuf {
        workspace.path.join("state")
    }

    fn session_file_path(state_root: &Path, logical_session_id: &str) -> PathBuf {
        state_root.join("sessions").join(format!(
            "{}.json",
            hex_encode(logical_session_id.as_bytes())
        ))
    }

    fn session_index_path(state_root: &Path) -> PathBuf {
        state_root.join("sessions.index.json")
    }

    fn run_session_dir(state_root: &Path, logical_session_id: &str) -> PathBuf {
        state_root
            .join("runs")
            .join(hex_encode(logical_session_id.as_bytes()))
    }

    fn run_file_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        run_session_dir(state_root, logical_session_id)
            .join(format!("{}.json", hex_encode(run_id.as_bytes())))
    }

    fn event_log_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        state_root
            .join("events")
            .join(hex_encode(logical_session_id.as_bytes()))
            .join(format!("{}.jsonl", hex_encode(run_id.as_bytes())))
    }

    fn replace_path_with_directory(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_dir_all(path);
        fs::create_dir_all(path).expect("directory fixture should be creatable");
    }

    fn replace_path_with_file(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_dir_all(path);
        let parent = path.parent().expect("path should have a parent");
        fs::create_dir_all(parent).expect("parent directory should be creatable");
        fs::write(path, b"blocked").expect("file fixture should be writable");
    }

    #[cfg(unix)]
    fn set_unix_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path)
            .expect("path metadata should be readable")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("path permissions should be writable");
    }

    #[cfg(not(unix))]
    fn set_unix_mode(_path: &Path, _mode: u32) {}

    #[test]
    fn heartbeat_loop_state_validates_inputs_and_tracks_dispatch() {
        let interval_error =
            HeartbeatLoopState::new(0, 1).expect_err("zero interval should be rejected");
        assert_eq!(
            interval_error,
            CrabError::InvalidConfig {
                key: "CRAB_HEARTBEAT_INTERVAL_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );

        let now_error = HeartbeatLoopState::new(1, 0).expect_err("zero now should fail");
        assert_eq!(
            now_error,
            CrabError::InvariantViolation {
                context: "heartbeat_loop_state_new",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );

        let mut state = HeartbeatLoopState::new(10, 5_000).expect("state should initialize");
        assert_eq!(state.next_heartbeat_due_at_epoch_ms(), 15_000);
        assert_eq!(state.last_dispatch_at_epoch_ms(), 5_000);

        state
            .record_dispatch(9_000)
            .expect("record dispatch should succeed");
        assert_eq!(state.last_dispatch_at_epoch_ms(), 9_000);

        let dispatch_error = state
            .record_dispatch(0)
            .expect_err("dispatch timestamp must be positive");
        assert_eq!(
            dispatch_error,
            CrabError::InvariantViolation {
                context: "heartbeat_loop_state_record_dispatch",
                message: "dispatched_at_epoch_ms must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn heartbeat_loop_state_reports_overflow_conditions() {
        let millis_overflow =
            HeartbeatLoopState::new(u64::MAX, 1).expect_err("interval millis overflow should fail");
        assert_eq!(
            millis_overflow,
            CrabError::InvalidConfig {
                key: "CRAB_HEARTBEAT_INTERVAL_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );

        let add_overflow =
            HeartbeatLoopState::new(1, u64::MAX).expect_err("next due overflow should fail");
        assert_eq!(
            add_overflow,
            CrabError::InvariantViolation {
                context: "heartbeat_loop_state_new",
                message: "next heartbeat due time overflow".to_string(),
            }
        );
    }

    #[test]
    fn heartbeat_schedule_overflow_paths_are_reported() {
        let mut direct = HeartbeatLoopState {
            heartbeat_interval_ms: 1,
            next_heartbeat_due_at_epoch_ms: u64::MAX,
            last_dispatch_at_epoch_ms: 1,
        };
        let direct_error = direct
            .advance_after_due_tick(u64::MAX)
            .expect_err("direct schedule overflow should fail");
        assert_eq!(
            direct_error,
            CrabError::InvariantViolation {
                context: "heartbeat_loop_state_tick",
                message: "heartbeat schedule overflow".to_string(),
            }
        );

        let workspace = TempWorkspace::new("maintenance", "heartbeat-schedule-overflow");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let mut runtime_state = HeartbeatLoopState {
            heartbeat_interval_ms: 1,
            next_heartbeat_due_at_epoch_ms: u64::MAX,
            last_dispatch_at_epoch_ms: 1,
        };
        let runtime_error = run_heartbeat_if_due(&mut composition, &mut runtime_state, u64::MAX)
            .expect_err("runtime schedule overflow should fail");
        assert_eq!(
            runtime_error,
            CrabError::InvariantViolation {
                context: "heartbeat_loop_state_tick",
                message: "heartbeat schedule overflow".to_string(),
            }
        );
    }

    #[test]
    fn startup_reconciliation_on_boot_recovers_stale_runs() {
        let workspace = TempWorkspace::new("maintenance", "startup-reconciliation");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");

        let logical_session_id = "discord:channel:777";
        let run_id = "run-stale";
        let now_epoch_ms = 1_739_173_400_000;

        composition
            .state_stores
            .session_store
            .upsert_session(&running_session(logical_session_id, now_epoch_ms - 120_000))
            .expect("session should persist");
        composition
            .state_stores
            .run_store
            .upsert_run(&running_run(
                logical_session_id,
                run_id,
                now_epoch_ms - 120_000,
            ))
            .expect("run should persist");

        let outcome = run_startup_reconciliation_on_boot(&mut composition, now_epoch_ms)
            .expect("reconciliation should succeed");
        assert_eq!(outcome.recovered_runs.len(), 1);
        assert_eq!(
            outcome.repaired_session_ids,
            vec![logical_session_id.to_string()]
        );

        let run = composition
            .state_stores
            .run_store
            .get_run(logical_session_id, run_id)
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.status, RunStatus::Cancelled);
        assert_eq!(run.completed_at_epoch_ms, Some(now_epoch_ms));

        let session = composition
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(
            session.active_physical_session_id,
            Some("physical-1".to_string())
        );
        assert_eq!(session.lane_state, LaneState::Idle);

        let events = composition
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("events should replay");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, crab_core::EventKind::RunState);
        assert_eq!(
            events[0].payload.get("reason").map(String::as_str),
            Some("startup_recovered_as_interrupted")
        );
    }

    #[test]
    fn startup_reconciliation_repairs_orphan_active_physical_session_id_when_it_has_failed() {
        let workspace = TempWorkspace::new("maintenance", "startup-repair-orphan-physical");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");

        let logical_session_id = "discord:channel:orphan";
        let now_epoch_ms = 1_739_173_400_000;

        composition
            .state_stores
            .session_store
            .upsert_session(&crab_core::LogicalSession {
                id: logical_session_id.to_string(),
                active_backend: BackendKind::Claude,
                active_profile: sample_profile(),
                active_physical_session_id: Some("physical-orphan".to_string()),
                last_successful_checkpoint_id: None,
                lane_state: LaneState::Idle,
                queued_run_count: 0,
                last_activity_epoch_ms: now_epoch_ms,
                token_accounting: sample_token_accounting(),
                has_injected_bootstrap: false,
            })
            .expect("session should persist");

        composition
            .state_stores
            .run_store
            .upsert_run(&Run {
                id: "run-success".to_string(),
                logical_session_id: logical_session_id.to_string(),
                physical_session_id: Some("physical-good".to_string()),
                status: RunStatus::Succeeded,
                user_input: "hello".to_string(),
                delivery_channel_id: None,
                profile: sample_telemetry("111111111111111111"),
                queued_at_epoch_ms: now_epoch_ms - 20_000,
                started_at_epoch_ms: Some(now_epoch_ms - 19_000),
                completed_at_epoch_ms: Some(now_epoch_ms - 18_000),
            })
            .expect("successful run should persist");

        composition
            .state_stores
            .run_store
            .upsert_run(&Run {
                id: "run-failed".to_string(),
                logical_session_id: logical_session_id.to_string(),
                physical_session_id: Some("physical-orphan".to_string()),
                status: RunStatus::Failed,
                user_input: "hello".to_string(),
                delivery_channel_id: None,
                profile: sample_telemetry("111111111111111111"),
                queued_at_epoch_ms: now_epoch_ms - 10_000,
                started_at_epoch_ms: Some(now_epoch_ms - 9_000),
                completed_at_epoch_ms: Some(now_epoch_ms - 8_000),
            })
            .expect("failed run should persist");

        let outcome = run_startup_reconciliation_on_boot(&mut composition, now_epoch_ms)
            .expect("reconciliation should succeed");

        assert!(outcome.recovered_runs.is_empty());
        assert!(outcome.repaired_session_ids.is_empty());
        assert_eq!(
            outcome.repaired_physical_sessions,
            vec![crab_core::StartupReconciliationRepairedPhysicalSession {
                logical_session_id: logical_session_id.to_string(),
                previous_physical_session_id: "physical-orphan".to_string(),
                repaired_physical_session_id: "physical-good".to_string(),
            }]
        );

        let session = composition
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(
            session.active_physical_session_id,
            Some("physical-good".to_string())
        );
    }

    #[test]
    fn startup_reconciliation_reports_grace_period_overflow() {
        let workspace = TempWorkspace::new("maintenance", "startup-overflow");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        composition.startup_reconciliation_policy.grace_period_secs = u64::MAX;

        let error = run_startup_reconciliation_on_boot(&mut composition, 1_000)
            .expect_err("overflow should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );
    }

    #[test]
    fn boot_runtime_runs_reconciliation_and_prepares_heartbeat_state() {
        let workspace = TempWorkspace::new("maintenance", "boot-runtime");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);

        let booted = boot_runtime(&config, "999", 1_739_173_300_000).expect("boot should succeed");

        assert!(booted.startup_reconciliation.recovered_runs.is_empty());
        assert!(booted
            .startup_reconciliation
            .repaired_session_ids
            .is_empty());
        assert_eq!(
            booted.heartbeat_loop_state.next_heartbeat_due_at_epoch_ms(),
            1_739_173_310_000
        );
        assert_eq!(
            booted.heartbeat_loop_state.last_dispatch_at_epoch_ms(),
            1_739_173_300_000
        );
    }

    #[test]
    fn run_heartbeat_if_due_is_deterministic_and_advances_schedule() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-due");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let mut state =
            HeartbeatLoopState::new(10, 1_739_173_300_000).expect("state should initialize");

        let none = run_heartbeat_if_due(&mut composition, &mut state, 1_739_173_309_000)
            .expect("pre-due tick should succeed");
        assert_eq!(none, None);
        assert_eq!(state.next_heartbeat_due_at_epoch_ms(), 1_739_173_310_000);

        let first = run_heartbeat_if_due(&mut composition, &mut state, 1_739_173_331_000)
            .expect("due heartbeat should run")
            .expect("due heartbeat should produce outcome");
        assert!(first.restarted_backends.is_empty());
        assert_eq!(state.next_heartbeat_due_at_epoch_ms(), 1_739_173_340_000);

        let second = run_heartbeat_if_due(&mut composition, &mut state, 1_739_173_350_000)
            .expect("late heartbeat should still run")
            .expect("late heartbeat should produce outcome");
        assert!(second.restarted_backends.is_empty());
        assert_eq!(state.next_heartbeat_due_at_epoch_ms(), 1_739_173_360_000);

        let zero_now_error = run_heartbeat_if_due(&mut composition, &mut state, 0)
            .expect_err("zero now should fail");
        assert_eq!(
            zero_now_error,
            CrabError::InvariantViolation {
                context: "runtime_heartbeat_tick",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn heartbeat_escalates_from_cancel_request_to_hard_stop() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-escalation");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");

        let logical_session_id = "discord:channel:escalation";
        let run_id = "run-escalate";
        let started_at = 1_739_173_000_000;
        composition
            .state_stores
            .session_store
            .upsert_session(&running_session(logical_session_id, started_at))
            .expect("session should persist");
        composition
            .state_stores
            .run_store
            .upsert_run(&running_run(logical_session_id, run_id, started_at))
            .expect("run should persist");

        let mut state = HeartbeatLoopState::new(1, 1_739_173_300_000)
            .expect("heartbeat state should initialize");

        let first = run_heartbeat_if_due(&mut composition, &mut state, 1_739_173_700_000)
            .expect("first heartbeat should succeed")
            .expect("first heartbeat should run");
        assert_eq!(first.cancelled_runs.len(), 1);
        assert!(first.hard_stopped_runs.is_empty());

        let session_after_first = composition
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session_after_first.lane_state, LaneState::Cancelling);

        let second = run_heartbeat_if_due(&mut composition, &mut state, 1_739_174_400_000)
            .expect("second heartbeat should succeed")
            .expect("second heartbeat should run");
        assert!(second.cancelled_runs.is_empty());
        assert_eq!(second.hard_stopped_runs.len(), 1);

        let run_after_second = composition
            .state_stores
            .run_store
            .get_run(logical_session_id, run_id)
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run_after_second.status, RunStatus::Cancelled);
        assert_eq!(
            run_after_second.completed_at_epoch_ms,
            Some(1_739_174_400_000)
        );

        let session_after_second = composition
            .state_stores
            .session_store
            .get_session(logical_session_id)
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session_after_second.lane_state, LaneState::Idle);
        assert_eq!(session_after_second.active_physical_session_id, None);

        let events = composition
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)
            .expect("events should replay");
        assert!(events.iter().any(|event| {
            event.kind == crab_core::EventKind::RunState
                && event
                    .payload
                    .get("reason")
                    .is_some_and(|reason| reason.starts_with("cancel_request_failed:"))
        }));
    }

    #[test]
    fn heartbeat_dispatcher_nudge_updates_dispatch_timestamp() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-dispatcher");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");

        composition
            .scheduler
            .enqueue(
                "discord:channel:queued",
                QueuedRun {
                    run_id: "run-queued".to_string(),
                },
            )
            .expect("enqueue should succeed");

        let mut state = HeartbeatLoopState::new(1, 1).expect("state should initialize");
        assert_eq!(state.last_dispatch_at_epoch_ms(), 1);

        let outcome = run_heartbeat_if_due(&mut composition, &mut state, 30_000)
            .expect("heartbeat should run")
            .expect("heartbeat should produce outcome");
        assert!(outcome.dispatcher_nudged);
        assert_eq!(state.last_dispatch_at_epoch_ms(), 30_000);
    }

    #[test]
    fn heartbeat_runtime_reports_invalid_active_run_invariants() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-invariants");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");

        let logical_session_id = "discord:channel:broken";
        composition
            .state_stores
            .session_store
            .upsert_session(&running_session(logical_session_id, 1_000))
            .expect("session should persist");

        let mut state = HeartbeatLoopState::new(1, 1).expect("state should initialize");
        let error = run_heartbeat_if_due(&mut composition, &mut state, 100_000)
            .expect_err("missing running run should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "heartbeat_runtime_list_active_runs",
                message: format!(
                    "session {} is Running but has no running run",
                    logical_session_id
                ),
            }
        );
    }

    #[test]
    fn startup_runtime_repair_session_lane_state_requires_existing_session() {
        let workspace = TempWorkspace::new("maintenance", "startup-repair-missing");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");

        let mut runtime = super::StartupRuntimeAdapter {
            composition: &mut composition,
        };
        let error = runtime
            .repair_session_lane_state("discord:channel:missing")
            .expect_err("missing session should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation_runtime",
                message: "session discord:channel:missing not found".to_string(),
            }
        );
    }

    #[test]
    fn startup_runtime_repair_active_physical_session_id_requires_existing_session() {
        let workspace = TempWorkspace::new("maintenance", "startup-repair-active-missing");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let mut runtime = super::StartupRuntimeAdapter {
            composition: &mut composition,
        };
        let error = runtime
            .repair_active_physical_session_id("discord:channel:missing", "physical-any")
            .expect_err("missing session should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation_runtime",
                message: "session discord:channel:missing not found".to_string(),
            }
        );
    }

    #[test]
    fn startup_runtime_adapter_exercises_restart_and_repair_error_paths() {
        {
            let workspace = TempWorkspace::new("maintenance", "startup-restart-backends");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let mut runtime = super::StartupRuntimeAdapter {
                composition: &mut composition,
            };
            StartupReconciliationRuntime::restart_backend_managers(&mut runtime)
                .expect("backend managers restart should succeed");
        }

        {
            let workspace = TempWorkspace::new("maintenance", "startup-repair-session-store-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let sessions_dir = state_root(&workspace).join("sessions");
            replace_path_with_file(&sessions_dir);

            let mut runtime = super::StartupRuntimeAdapter {
                composition: &mut composition,
            };
            let error = runtime
                .repair_session_lane_state("discord:channel:any")
                .expect_err("session store IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "session_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "startup-repair-queued-run-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:queued-run-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");

            let runs_root = state_root(&workspace).join("runs");
            replace_path_with_file(&runs_root);

            let mut runtime = super::StartupRuntimeAdapter {
                composition: &mut composition,
            };
            let error = runtime
                .repair_session_lane_state(logical_session_id)
                .expect_err("queued run count failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "run_store_layout")
            );
        }
    }

    #[test]
    fn heartbeat_rejects_running_run_when_lane_is_idle() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-idle-running-mismatch");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let logical_session_id = "discord:channel:idle";
        let run_id = "run-idle";

        let mut session = running_session(logical_session_id, 1_739_173_300_000);
        session.lane_state = LaneState::Idle;
        composition
            .state_stores
            .session_store
            .upsert_session(&session)
            .expect("session should persist");
        composition
            .state_stores
            .run_store
            .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_200_000))
            .expect("run should persist");

        let mut state =
            HeartbeatLoopState::new(1, 1_739_173_300_000).expect("state should initialize");
        let error = run_heartbeat_if_due(&mut composition, &mut state, 1_739_173_400_000)
            .expect_err("idle/running mismatch should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "heartbeat_runtime_list_active_runs",
                message: format!(
                    "run {}/{} is running while lane state is Idle",
                    logical_session_id, run_id
                ),
            }
        );
    }

    #[test]
    fn heartbeat_ignores_idle_sessions_without_running_runs() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-idle-no-run");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let logical_session_id = "discord:channel:idle-empty";
        let mut session = running_session(logical_session_id, 1_739_173_300_000);
        session.lane_state = LaneState::Idle;
        session.active_physical_session_id = None;
        composition
            .state_stores
            .session_store
            .upsert_session(&session)
            .expect("session should persist");

        let mut state =
            HeartbeatLoopState::new(1, 1_739_173_300_000).expect("state should initialize");
        let outcome = run_heartbeat_if_due(&mut composition, &mut state, 1_739_173_400_000)
            .expect("heartbeat should succeed")
            .expect("heartbeat should run");
        assert!(outcome.cancelled_runs.is_empty());
        assert!(outcome.hard_stopped_runs.is_empty());
    }

    #[test]
    fn heartbeat_runtime_list_active_runs_surfaces_store_and_event_errors() {
        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-list-sessions-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let sessions_dir = state_root(&workspace).join("sessions");
            replace_path_with_file(&sessions_dir);

            let mut dispatch_clock = 1;
            let runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .list_active_runs()
                .expect_err("session list IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "session_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-list-runs-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:runs-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");

            let runs_root = state_root(&workspace).join("runs");
            replace_path_with_file(&runs_root);

            let mut dispatch_clock = 1;
            let runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .list_active_runs()
                .expect_err("run list IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "run_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-list-multiple-running");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:multiple-running";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, "run-a", 1_739_173_300_000))
                .expect("run a should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, "run-b", 1_739_173_300_100))
                .expect("run b should persist");

            let mut dispatch_clock = 1;
            let runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .list_active_runs()
                .expect_err("duplicate running runs should fail");
            assert!(
                matches!(error, CrabError::InvariantViolation { context, message }
                    if context == "heartbeat_runtime_list_active_runs"
                        && message.contains("multiple running runs"))
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-list-events-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:event-io";
            let run_id = "run-event-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");

            let log_path = event_log_path(&state_root(&workspace), logical_session_id, run_id);
            replace_path_with_directory(&log_path);

            let mut dispatch_clock = 1;
            let runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .list_active_runs()
                .expect_err("event replay IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "event_replay_read")
            );
        }
    }

    #[test]
    fn heartbeat_runtime_request_cancel_validates_run_state_and_lane_state() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-cancel-guards");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let logical_session_id = "discord:channel:cancel-guard";
        let run_id = "run-cancel-guard";
        let now_epoch_ms = 1_739_173_400_000;

        composition
            .state_stores
            .session_store
            .upsert_session(&running_session(logical_session_id, now_epoch_ms))
            .expect("session should persist");

        let mut not_running = running_run(logical_session_id, run_id, now_epoch_ms - 10_000);
        not_running.status = RunStatus::Cancelled;
        not_running.completed_at_epoch_ms = Some(now_epoch_ms - 1_000);
        composition
            .state_stores
            .run_store
            .upsert_run(&not_running)
            .expect("run should persist");

        let mut dispatch_clock = now_epoch_ms;
        {
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .request_cancel_active_run(logical_session_id, run_id)
                .expect_err("non-running run should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "heartbeat_runtime_cancel",
                    message: format!("run {logical_session_id}/{run_id} is not actively running"),
                }
            );
        }

        let mut idle_session = running_session(logical_session_id, now_epoch_ms);
        idle_session.lane_state = LaneState::Idle;
        composition
            .state_stores
            .session_store
            .upsert_session(&idle_session)
            .expect("session should persist");
        let running = running_run(logical_session_id, run_id, now_epoch_ms - 5_000);
        composition
            .state_stores
            .run_store
            .upsert_run(&running)
            .expect("run should persist");

        {
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .request_cancel_active_run(logical_session_id, run_id)
                .expect_err("idle lane should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "heartbeat_runtime_cancel",
                    message: format!(
                        "session {} has lane state Idle; expected running",
                        logical_session_id
                    ),
                }
            );
        }
    }

    #[test]
    fn heartbeat_runtime_restart_backend_manager_allows_claude_noop() {
        let workspace = TempWorkspace::new("maintenance", "heartbeat-restart-claude");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let mut composition = compose_runtime(&config, "999").expect("composition should succeed");
        let mut dispatch_clock = 1_739_173_400_000;

        let mut runtime = super::HeartbeatRuntimeAdapter {
            composition: &mut composition,
            now_epoch_ms: 1_739_173_400_000,
            last_dispatch_at_epoch_ms: &mut dispatch_clock,
        };
        runtime
            .restart_backend_manager(BackendKind::Claude)
            .expect("claude restart should be a no-op");
    }

    #[test]
    fn heartbeat_runtime_request_cancel_surfaces_missing_and_io_paths() {
        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-cancel-missing-session");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .request_cancel_active_run("discord:channel:missing", "run-missing")
                .expect_err("missing session should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "heartbeat_runtime_cancel",
                    message: "session discord:channel:missing not found".to_string(),
                }
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-cancel-missing-run");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:cancel-missing-run";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .request_cancel_active_run(logical_session_id, "run-missing")
                .expect_err("missing run should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "heartbeat_runtime_cancel",
                    message: format!("run {logical_session_id}/run-missing not found"),
                }
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-cancel-session-upsert-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:cancel-session-upsert-io";
            let run_id = "run-cancel-session-upsert-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");

            let index_path = session_index_path(&state_root(&workspace));
            replace_path_with_directory(&index_path);

            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .request_cancel_active_run(logical_session_id, run_id)
                .expect_err("session upsert IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "session_index_read")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-cancel-event-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:cancel-event-io";
            let run_id = "run-cancel-event-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");

            let blocked_log_path =
                event_log_path(&state_root(&workspace), logical_session_id, run_id);
            replace_path_with_directory(&blocked_log_path);

            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .request_cancel_active_run(logical_session_id, run_id)
                .expect_err("event append failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "event_replay_read")
            );
        }
    }

    #[test]
    fn heartbeat_runtime_hard_stop_surfaces_missing_and_io_paths() {
        {
            let workspace =
                TempWorkspace::new("maintenance", "heartbeat-hard-stop-missing-session");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .hard_stop_run_and_rotate("discord:channel:missing", "run-missing", "forced")
                .expect_err("missing session should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "heartbeat_runtime_hard_stop",
                    message: "session discord:channel:missing not found".to_string(),
                }
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-hard-stop-missing-run");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:hard-stop-missing-run";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");

            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .hard_stop_run_and_rotate(logical_session_id, "run-missing", "forced")
                .expect_err("missing run should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "heartbeat_runtime_hard_stop",
                    message: format!("run {logical_session_id}/run-missing not found"),
                }
            );
        }

        #[cfg(unix)]
        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-hard-stop-run-upsert-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:hard-stop-run-upsert-io";
            let run_id = "run-hard-stop-run-upsert-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");
            let runs_dir = run_session_dir(&state_root(&workspace), logical_session_id);
            #[cfg(unix)]
            set_unix_mode(&runs_dir, 0o500);

            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .hard_stop_run_and_rotate(logical_session_id, run_id, "forced")
                .expect_err("run upsert IO failure should surface");
            set_unix_mode(&runs_dir, 0o700);
            let _ = error;
        }

        {
            let workspace = TempWorkspace::new("maintenance", "heartbeat-hard-stop-event-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:hard-stop-event-io";
            let run_id = "run-hard-stop-event-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");
            let blocked_log_path =
                event_log_path(&state_root(&workspace), logical_session_id, run_id);
            replace_path_with_directory(&blocked_log_path);

            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .hard_stop_run_and_rotate(logical_session_id, run_id, "forced")
                .expect_err("event append IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "event_replay_read")
            );
        }

        {
            let workspace =
                TempWorkspace::new("maintenance", "heartbeat-hard-stop-session-upsert-io");
            let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
            let mut composition =
                compose_runtime(&config, "999").expect("composition should succeed");
            let logical_session_id = "discord:channel:hard-stop-session-upsert-io";
            let run_id = "run-hard-stop-session-upsert-io";
            composition
                .state_stores
                .session_store
                .upsert_session(&running_session(logical_session_id, 1_739_173_400_000))
                .expect("session should persist");
            composition
                .state_stores
                .run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");
            let index_path = session_index_path(&state_root(&workspace));
            replace_path_with_directory(&index_path);

            let mut dispatch_clock = 1_739_173_400_000;
            let mut runtime = super::HeartbeatRuntimeAdapter {
                composition: &mut composition,
                now_epoch_ms: 1_739_173_400_000,
                last_dispatch_at_epoch_ms: &mut dispatch_clock,
            };
            let error = runtime
                .hard_stop_run_and_rotate(logical_session_id, run_id, "forced")
                .expect_err("session upsert IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "session_index_read")
            );
        }
    }

    #[test]
    fn require_session_and_run_helpers_report_missing_ids() {
        let workspace = TempWorkspace::new("maintenance", "require-helpers");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let composition = compose_runtime(&config, "999").expect("composition should succeed");

        let missing_session = super::require_session(
            &composition.state_stores.session_store,
            "discord:channel:missing",
            "helper_test",
        )
        .expect_err("missing session should fail");
        assert_eq!(
            missing_session,
            CrabError::InvariantViolation {
                context: "helper_test",
                message: "session discord:channel:missing not found".to_string(),
            }
        );

        let missing_run = super::require_run(
            &composition.state_stores.run_store,
            "discord:channel:missing",
            "run-missing",
            "helper_test",
        )
        .expect_err("missing run should fail");
        assert_eq!(
            missing_run,
            CrabError::InvariantViolation {
                context: "helper_test",
                message: "run discord:channel:missing/run-missing not found".to_string(),
            }
        );
    }

    #[test]
    fn helper_load_and_event_functions_surface_io_and_missing_paths() {
        {
            let workspace = TempWorkspace::new("maintenance", "helper-load-sessions-list-io");
            let blocked_root = workspace.path.join("blocked-session-store");
            replace_path_with_file(&blocked_root);
            let session_store = crab_store::SessionStore::new(&blocked_root);
            let error = super::load_sessions(&session_store)
                .expect_err("list session IDs IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "session_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-load-sessions-read-io");
            let store_root = workspace.path.join("session-store");
            let session_store = crab_store::SessionStore::new(&store_root);
            let session_id = "discord:channel:session-read-io";
            session_store
                .upsert_session(&running_session(session_id, 1_739_173_400_000))
                .expect("session should persist");
            let blocked_session_path = session_file_path(&store_root, session_id);
            replace_path_with_directory(&blocked_session_path);

            let error = super::load_sessions(&session_store)
                .expect_err("session read IO failure should surface");
            assert!(matches!(error, CrabError::Io { context, .. } if context == "session_read"));
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-load-sessions-missing");
            let store_root = workspace.path.join("session-store");
            let session_store = crab_store::SessionStore::new(&store_root);
            let session_id = "discord:channel:session-missing";
            session_store
                .upsert_session(&running_session(session_id, 1_739_173_400_000))
                .expect("session should persist");
            fs::remove_file(session_file_path(&store_root, session_id))
                .expect("session file should be removable");

            let sessions = super::load_sessions(&session_store)
                .expect("missing session file should be skipped");
            assert!(sessions.is_empty());
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-load-runs-list-io");
            let blocked_root = workspace.path.join("blocked-run-store");
            replace_path_with_file(&blocked_root);
            let run_store = crab_store::RunStore::new(&blocked_root);
            let error = super::load_runs_for_session(&run_store, "discord:channel:run-list-io")
                .expect_err("list run IDs IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "run_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-load-runs-read-io");
            let store_root = workspace.path.join("run-store");
            let run_store = crab_store::RunStore::new(&store_root);
            let logical_session_id = "discord:channel:run-read-io";
            let run_id = "run-read-io";
            let alternate_id = "run-read-io-alternate";
            run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");
            run_store
                .upsert_run(&running_run(
                    logical_session_id,
                    alternate_id,
                    1_739_173_300_100,
                ))
                .expect("alternate run should persist");
            let canonical_path = run_file_path(&store_root, logical_session_id, run_id);
            let alternate_path = run_file_path(&store_root, logical_session_id, alternate_id);
            let duplicate_target_path =
                run_session_dir(&store_root, logical_session_id).join("duplicate-target.json");
            fs::copy(&canonical_path, &duplicate_target_path)
                .expect("duplicate target file should be writable");
            fs::copy(&alternate_path, &canonical_path)
                .expect("canonical run file should be replaceable");

            let error = super::load_runs_for_session(&run_store, logical_session_id)
                .expect_err("run identity mismatch should surface");
            assert!(
                matches!(error, CrabError::InvariantViolation { context, .. } if context == "run_get_identity_mismatch")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-load-runs-missing");
            let store_root = workspace.path.join("run-store");
            let run_store = crab_store::RunStore::new(&store_root);
            let logical_session_id = "discord:channel:run-missing";
            let run_id = "run-missing";
            run_store
                .upsert_run(&running_run(logical_session_id, run_id, 1_739_173_300_000))
                .expect("run should persist");
            let expected_path = run_file_path(&store_root, logical_session_id, run_id);
            let relocated_path =
                run_session_dir(&store_root, logical_session_id).join("other.json");
            fs::rename(&expected_path, &relocated_path).expect("run file should be movable");

            let runs = super::load_runs_for_session(&run_store, logical_session_id)
                .expect("missing canonical run path should be skipped");
            assert!(runs.is_empty());
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-require-session-io");
            let blocked_root = workspace.path.join("blocked-session-store");
            replace_path_with_file(&blocked_root);
            let session_store = crab_store::SessionStore::new(&blocked_root);
            let error = super::require_session(&session_store, "discord:channel:any", "helper_io")
                .expect_err("session get IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "session_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-require-run-io");
            let blocked_root = workspace.path.join("blocked-run-store");
            replace_path_with_file(&blocked_root);
            let run_store = crab_store::RunStore::new(&blocked_root);
            let error =
                super::require_run(&run_store, "discord:channel:any", "run-any", "helper_io")
                    .expect_err("run get IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "run_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-event-replay-io");
            let blocked_root = workspace.path.join("blocked-event-store");
            replace_path_with_file(&blocked_root);
            let event_store = crab_store::EventStore::new(&blocked_root);
            let run = running_run(
                "discord:channel:event-replay-io",
                "run-event-replay-io",
                1_739_173_300_000,
            );

            let last_progress_error = super::last_progress_at_epoch_ms(&event_store, &run)
                .expect_err("event replay failure should surface");
            assert!(
                matches!(last_progress_error, CrabError::Io { context, .. } if context == "event_store_layout")
            );

            let next_sequence_error =
                super::next_event_sequence(&event_store, &run.logical_session_id, &run.id)
                    .expect_err("next sequence replay failure should surface");
            assert!(
                matches!(next_sequence_error, CrabError::Io { context, .. } if context == "event_store_layout")
            );

            let mut payload = BTreeMap::new();
            payload.insert("kind".to_string(), "heartbeat".to_string());
            let append_error = super::append_runtime_event(
                &event_store,
                &run,
                crab_core::EventKind::Heartbeat,
                payload,
                crab_core::EventSource::System,
                1_739_173_400_000,
                "helper_append",
            )
            .expect_err("append runtime event should fail when replay fails");
            assert!(
                matches!(append_error, CrabError::Io { context, .. } if context == "event_store_layout")
            );
        }

        {
            let workspace = TempWorkspace::new("maintenance", "helper-queued-run-count-io");
            let blocked_root = workspace.path.join("blocked-run-store");
            replace_path_with_file(&blocked_root);
            let run_store = crab_store::RunStore::new(&blocked_root);
            let error =
                super::queued_run_count_for_session(&run_store, "discord:channel:queued-count-io")
                    .expect_err("queued run count IO failure should surface");
            assert!(
                matches!(error, CrabError::Io { context, .. } if context == "run_store_layout")
            );
        }
    }

    #[test]
    fn single_running_run_skips_non_running_entries_and_rejects_duplicates() {
        let logical_session_id = "discord:channel:single-running";
        let mut queued = running_run(logical_session_id, "run-queued", 1_739_173_200_000);
        queued.status = RunStatus::Queued;
        queued.started_at_epoch_ms = None;
        let mut completed = running_run(logical_session_id, "run-completed", 1_739_173_200_100);
        completed.completed_at_epoch_ms = Some(1_739_173_300_000);
        let none = super::single_running_run(logical_session_id, &[queued, completed])
            .expect("non-running entries should be skipped");
        assert_eq!(none, None);

        let first = running_run(logical_session_id, "run-a", 1_739_173_200_200);
        let second = running_run(logical_session_id, "run-b", 1_739_173_200_300);
        let error = super::single_running_run(logical_session_id, &[first, second])
            .expect_err("duplicate running runs should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "heartbeat_runtime_list_active_runs",
                message: format!(
                    "session {} has multiple running runs: run-a and run-b",
                    logical_session_id
                ),
            }
        );
    }

    #[test]
    fn load_helpers_sort_sessions_and_runs_by_id() {
        let workspace = TempWorkspace::new("maintenance", "load-helpers-sort");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let composition = compose_runtime(&config, "999").expect("composition should succeed");
        let now_epoch_ms = 1_739_173_400_000;

        composition
            .state_stores
            .session_store
            .upsert_session(&running_session("discord:channel:b", now_epoch_ms))
            .expect("session b should persist");
        composition
            .state_stores
            .session_store
            .upsert_session(&running_session("discord:channel:a", now_epoch_ms))
            .expect("session a should persist");

        let sessions = super::load_sessions(&composition.state_stores.session_store)
            .expect("sessions should load");
        let session_ids: Vec<String> = sessions.into_iter().map(|session| session.id).collect();
        assert_eq!(
            session_ids,
            vec![
                "discord:channel:a".to_string(),
                "discord:channel:b".to_string()
            ]
        );

        composition
            .state_stores
            .run_store
            .upsert_run(&running_run("discord:channel:a", "run-b", now_epoch_ms))
            .expect("run b should persist");
        composition
            .state_stores
            .run_store
            .upsert_run(&running_run("discord:channel:a", "run-a", now_epoch_ms))
            .expect("run a should persist");

        let runs =
            super::load_runs_for_session(&composition.state_stores.run_store, "discord:channel:a")
                .expect("runs should load");
        let run_ids: Vec<String> = runs.into_iter().map(|run| run.id).collect();
        assert_eq!(run_ids, vec!["run-a".to_string(), "run-b".to_string()]);
    }

    #[test]
    fn helper_seconds_to_millis_validates_overflow() {
        assert_eq!(
            seconds_to_millis("EXAMPLE", 42).expect("conversion should succeed"),
            42_000
        );

        let overflow = seconds_to_millis("EXAMPLE", u64::MAX).expect_err("overflow should fail");
        assert_eq!(
            overflow,
            CrabError::InvalidConfig {
                key: "EXAMPLE",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );
    }

    #[test]
    fn boot_runtime_respects_queue_limit_validation() {
        let workspace = TempWorkspace::new("maintenance", "boot-queue-limit");
        let mut values = HashMap::new();
        values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());
        values.insert(
            "CRAB_WORKSPACE_ROOT".to_string(),
            workspace.path.to_string_lossy().to_string(),
        );
        let config = crab_core::RuntimeConfig::from_map(&values).expect("config should parse");

        let result = super::boot_runtime_with_queue_limit(&config, "999", 0, 1_000);
        assert!(result.is_err());
        let error = result.err().expect("error should be present");

        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_LANE_QUEUE_LIMIT",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn boot_runtime_propagates_heartbeat_interval_validation_errors() {
        let workspace = TempWorkspace::new("maintenance", "boot-heartbeat-interval");
        let mut values = HashMap::new();
        values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());
        values.insert(
            "CRAB_WORKSPACE_ROOT".to_string(),
            workspace.path.to_string_lossy().to_string(),
        );
        values.insert(
            "CRAB_HEARTBEAT_INTERVAL_SECS".to_string(),
            u64::MAX.to_string(),
        );
        let config = crab_core::RuntimeConfig::from_map(&values).expect("config should parse");

        let result = super::boot_runtime_with_queue_limit(&config, "999", 1, 1_000);
        assert!(result.is_err());
        let error = result.err().expect("error should be present");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_HEARTBEAT_INTERVAL_SECS",
                value: u64::MAX.to_string(),
                reason: "must fit in milliseconds as a u64",
            }
        );
    }

    #[test]
    fn boot_runtime_propagates_zero_now_error_from_heartbeat_state() {
        let workspace = TempWorkspace::new("maintenance", "boot-zero-now");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);

        let result = boot_runtime(&config, "999", 0);
        assert!(result.is_err());
        let error = result.err().expect("error should be present");

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_reconciliation",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );
    }
}
