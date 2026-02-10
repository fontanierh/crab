use std::collections::BTreeMap;

use crab_backends::{BackendEvent, BackendEventKind, CodexAppServerProcess, OpenCodeServerProcess};
use crab_core::{
    CrabError, CrabResult, EventEnvelope, EventKind, EventSource, InferenceProfile, LaneState,
    LogicalSession, PhysicalSession, Run, RunProfileTelemetry, RunStatus, TokenAccounting,
};
use crab_discord::{GatewayMessage, RoutingKey};
use crab_scheduler::QueuedRun;

use crate::AppComposition;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuedTurn {
    pub logical_session_id: String,
    pub run_id: String,
    pub message_id: String,
    pub author_id: String,
    pub routing_key: RoutingKey,
    pub queued_run_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchedTurn {
    pub logical_session_id: String,
    pub run_id: String,
    pub turn_id: String,
    pub status: RunStatus,
    pub emitted_event_count: usize,
}

pub trait TurnExecutorRuntime {
    fn now_epoch_ms(&mut self) -> CrabResult<u64>;

    fn resolve_run_profile(
        &mut self,
        logical_session_id: &str,
        author_id: &str,
        user_input: &str,
    ) -> CrabResult<RunProfileTelemetry>;

    fn ensure_physical_session(
        &mut self,
        logical_session_id: &str,
        profile: &InferenceProfile,
        active_physical_session_id: Option<&str>,
    ) -> CrabResult<PhysicalSession>;

    fn build_turn_context(
        &mut self,
        run: &Run,
        logical_session: &LogicalSession,
        physical_session: &PhysicalSession,
    ) -> CrabResult<String>;

    fn execute_backend_turn(
        &mut self,
        physical_session: &mut PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>>;
}

pub struct TurnExecutor<CP, OP, R>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    R: TurnExecutorRuntime,
{
    composition: AppComposition<CP, OP>,
    runtime: R,
}

impl<CP, OP, R> TurnExecutor<CP, OP, R>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    R: TurnExecutorRuntime,
{
    #[must_use]
    pub fn new(composition: AppComposition<CP, OP>, runtime: R) -> Self {
        Self {
            composition,
            runtime,
        }
    }

    #[must_use]
    pub fn composition(&self) -> &AppComposition<CP, OP> {
        &self.composition
    }

    #[must_use]
    pub fn composition_mut(&mut self) -> &mut AppComposition<CP, OP> {
        &mut self.composition
    }

    #[must_use]
    pub fn runtime_mut(&mut self) -> &mut R {
        &mut self.runtime
    }

    pub fn process_gateway_message(
        &mut self,
        message: GatewayMessage,
    ) -> CrabResult<Option<DispatchedTurn>> {
        let maybe_queued = self.enqueue_gateway_message(message)?;
        if maybe_queued.is_none() {
            return Ok(None);
        }
        self.dispatch_next_run()
    }

    pub fn enqueue_gateway_message(
        &mut self,
        message: GatewayMessage,
    ) -> CrabResult<Option<QueuedTurn>> {
        let Some(ingress) = self.composition.gateway_ingress.ingest(message)? else {
            return Ok(None);
        };
        let logical_session_id = ingress
            .routing_key
            .logical_session_id()
            .expect("gateway ingress should yield validated routing keys");
        let run_id = build_run_id(&logical_session_id, &ingress.message_id);
        self.composition.scheduler.enqueue(
            &logical_session_id,
            QueuedRun {
                run_id: run_id.clone(),
            },
        )?;

        let enqueue_result = self.persist_enqueued_run(&logical_session_id, &run_id, &ingress);
        if let Err(error) = enqueue_result {
            let _ = self.composition.scheduler.cancel_queued_run_by_id(&run_id);
            return Err(error);
        }
        let queued_run_count = self
            .composition
            .scheduler
            .queued_count(&logical_session_id)
            .expect("validated logical session id should always be queue-count addressable");

        Ok(Some(QueuedTurn {
            logical_session_id,
            run_id,
            message_id: ingress.message_id,
            author_id: ingress.author_id,
            routing_key: ingress.routing_key,
            queued_run_count,
        }))
    }

    pub fn dispatch_next_run(&mut self) -> CrabResult<Option<DispatchedTurn>> {
        let Some(dispatched) = self.composition.scheduler.try_dispatch_next() else {
            return Ok(None);
        };
        let logical_session_id = dispatched.logical_session_id;
        let run_id = dispatched.run.run_id;

        let execution = self.execute_dispatched_run(&logical_session_id, &run_id);
        if let Err(error) = execution {
            let failure_result = self.mark_run_failed(&logical_session_id, &run_id, &error);
            self.composition
                .scheduler
                .complete_lane(&logical_session_id)
                .expect("dispatched lane must be active until completion");
            failure_result?;
            return Err(error);
        }

        self.composition
            .scheduler
            .complete_lane(&logical_session_id)
            .expect("dispatched lane must be active until completion");
        Ok(Some(
            execution.expect("execution should be available on success"),
        ))
    }

    fn persist_enqueued_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
        ingress: &crab_discord::IngressMessage,
    ) -> CrabResult<()> {
        let now_epoch_ms = self.runtime.now_epoch_ms()?;
        let profile = self.runtime.resolve_run_profile(
            logical_session_id,
            &ingress.author_id,
            &ingress.content,
        )?;
        let run = Run {
            id: run_id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            physical_session_id: None,
            status: RunStatus::Queued,
            user_input: ingress.content.clone(),
            profile: profile.clone(),
            queued_at_epoch_ms: now_epoch_ms,
            started_at_epoch_ms: None,
            completed_at_epoch_ms: None,
        };
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut session = self.load_or_initialize_session(
            logical_session_id,
            &profile.resolved_profile,
            now_epoch_ms,
        )?;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = now_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(&run, "queued", now_epoch_ms, None)
    }

    fn execute_dispatched_run(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<DispatchedTurn> {
        let mut run = self.load_required_run(logical_session_id, run_id)?;
        let started_at_epoch_ms = self.runtime.now_epoch_ms()?;

        run.status = RunStatus::Running;
        run.started_at_epoch_ms = Some(started_at_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut session = self.load_or_initialize_session(
            logical_session_id,
            &run.profile.resolved_profile,
            started_at_epoch_ms,
        )?;
        session.lane_state = LaneState::Running;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = started_at_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(&run, "running", started_at_epoch_ms, None)?;

        let mut physical_session = self.runtime.ensure_physical_session(
            logical_session_id,
            &run.profile.resolved_profile,
            session.active_physical_session_id.as_deref(),
        )?;
        run.physical_session_id = Some(physical_session.id.clone());
        self.composition.state_stores.run_store.upsert_run(&run)?;

        session.active_backend = run.profile.resolved_profile.backend;
        session.active_profile = run.profile.resolved_profile.clone();
        session.active_physical_session_id = Some(physical_session.id.clone());
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        let turn_context = self
            .runtime
            .build_turn_context(&run, &session, &physical_session)?;
        let turn_id = build_turn_id(&run.id);
        let backend_events = self.runtime.execute_backend_turn(
            &mut physical_session,
            &run,
            &turn_id,
            &turn_context,
        )?;

        for backend_event in &backend_events {
            self.append_backend_event(&run, backend_event)?;
        }

        let final_status = derive_final_status(&backend_events);
        let completed_at_epoch_ms = self.runtime.now_epoch_ms()?;
        run.status = final_status;
        run.completed_at_epoch_ms = Some(completed_at_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        session.lane_state = LaneState::Idle;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = completed_at_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(
            &run,
            run_status_token(final_status),
            completed_at_epoch_ms,
            None,
        )?;

        Ok(DispatchedTurn {
            logical_session_id: logical_session_id.to_string(),
            run_id: run.id,
            turn_id,
            status: final_status,
            emitted_event_count: backend_events.len() + 2,
        })
    }

    fn mark_run_failed(
        &mut self,
        logical_session_id: &str,
        run_id: &str,
        cause: &CrabError,
    ) -> CrabResult<()> {
        let now_epoch_ms = self.runtime.now_epoch_ms()?;
        let mut run = self.load_required_run(logical_session_id, run_id)?;
        run.status = RunStatus::Failed;
        run.completed_at_epoch_ms = Some(now_epoch_ms);
        self.composition.state_stores.run_store.upsert_run(&run)?;

        let mut session = self.load_or_initialize_session(
            logical_session_id,
            &run.profile.resolved_profile,
            now_epoch_ms,
        )?;
        session.lane_state = LaneState::Idle;
        session.queued_run_count = self
            .composition
            .scheduler
            .queued_count(logical_session_id)
            .expect("validated logical session id should always be queue-count addressable")
            as u32;
        session.last_activity_epoch_ms = now_epoch_ms;
        self.composition
            .state_stores
            .session_store
            .upsert_session(&session)?;

        self.append_run_state_event(&run, "failed", now_epoch_ms, Some(cause.to_string()))
    }

    fn load_required_run(&self, logical_session_id: &str, run_id: &str) -> CrabResult<Run> {
        self.composition
            .state_stores
            .run_store
            .get_run(logical_session_id, run_id)?
            .ok_or_else(|| CrabError::InvariantViolation {
                context: "turn_executor_run_lookup",
                message: format!("run {logical_session_id}/{run_id} not found"),
            })
    }

    fn load_or_initialize_session(
        &self,
        logical_session_id: &str,
        profile: &InferenceProfile,
        now_epoch_ms: u64,
    ) -> CrabResult<LogicalSession> {
        if let Some(existing) = self
            .composition
            .state_stores
            .session_store
            .get_session(logical_session_id)?
        {
            return Ok(existing);
        }

        Ok(LogicalSession {
            id: logical_session_id.to_string(),
            active_backend: profile.backend,
            active_profile: profile.clone(),
            active_physical_session_id: None,
            last_successful_checkpoint_id: None,
            lane_state: LaneState::Idle,
            queued_run_count: 0,
            last_activity_epoch_ms: now_epoch_ms,
            token_accounting: TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
            },
        })
    }

    fn append_backend_event(&mut self, run: &Run, backend_event: &BackendEvent) -> CrabResult<()> {
        let kind = map_backend_event_kind(backend_event.kind);
        let mut payload = backend_event.payload.clone();
        payload.insert(
            "backend_sequence".to_string(),
            backend_event.sequence.to_string(),
        );
        if let Some(state_token) = backend_state_token(backend_event.kind) {
            payload.insert("state".to_string(), state_token.to_string());
        }

        let emitted_at_epoch_ms = self.runtime.now_epoch_ms()?;
        self.append_event(
            run,
            kind,
            EventSource::Backend,
            payload,
            emitted_at_epoch_ms,
        )
    }

    fn append_run_state_event(
        &mut self,
        run: &Run,
        state: &str,
        emitted_at_epoch_ms: u64,
        reason: Option<String>,
    ) -> CrabResult<()> {
        let mut payload = BTreeMap::new();
        payload.insert("state".to_string(), state.to_string());
        if let Some(reason) = reason {
            payload.insert("reason".to_string(), reason);
        }
        self.append_event(
            run,
            EventKind::RunState,
            EventSource::System,
            payload,
            emitted_at_epoch_ms,
        )
    }

    fn append_event(
        &self,
        run: &Run,
        kind: EventKind,
        source: EventSource,
        payload: BTreeMap<String, String>,
        emitted_at_epoch_ms: u64,
    ) -> CrabResult<()> {
        let sequence = self.next_event_sequence(&run.logical_session_id, &run.id)?;
        let event = EventEnvelope {
            event_id: format!(
                "evt:turn-executor:{}:{}:{}",
                run.logical_session_id, run.id, sequence
            ),
            run_id: run.id.clone(),
            logical_session_id: run.logical_session_id.clone(),
            sequence,
            emitted_at_epoch_ms,
            source,
            kind,
            payload,
            profile: Some(run.profile.clone()),
            idempotency_key: Some(format!(
                "turn-executor:{}:{}:{}",
                run.logical_session_id, run.id, sequence
            )),
        };
        self.composition
            .state_stores
            .event_store
            .append_event(&event)
    }

    fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64> {
        let events = self
            .composition
            .state_stores
            .event_store
            .replay_run(logical_session_id, run_id)?;
        Ok(events.last().map_or(1, |event| event.sequence + 1))
    }
}

fn build_run_id(logical_session_id: &str, message_id: &str) -> String {
    format!("run:{logical_session_id}:{}", message_id.trim())
}

fn build_turn_id(run_id: &str) -> String {
    format!("turn:{run_id}")
}

fn run_status_token(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Running => "running",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Cancelled => "cancelled",
    }
}

fn derive_final_status(events: &[BackendEvent]) -> RunStatus {
    if events
        .iter()
        .any(|event| event.kind == BackendEventKind::Error)
    {
        return RunStatus::Failed;
    }
    if events
        .iter()
        .any(|event| event.kind == BackendEventKind::TurnInterrupted)
    {
        return RunStatus::Cancelled;
    }
    RunStatus::Succeeded
}

fn map_backend_event_kind(kind: BackendEventKind) -> EventKind {
    match kind {
        BackendEventKind::TextDelta => EventKind::TextDelta,
        BackendEventKind::ToolCall => EventKind::ToolCall,
        BackendEventKind::ToolResult => EventKind::ToolResult,
        BackendEventKind::RunNote => EventKind::RunNote,
        BackendEventKind::TurnCompleted | BackendEventKind::TurnInterrupted => EventKind::RunState,
        BackendEventKind::Error => EventKind::Error,
    }
}

fn backend_state_token(kind: BackendEventKind) -> Option<&'static str> {
    match kind {
        BackendEventKind::TurnCompleted => Some("succeeded"),
        BackendEventKind::TurnInterrupted => Some("cancelled"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crab_backends::BackendEventKind;
    use crab_core::{
        BackendKind, CrabError, CrabResult, EventKind, InferenceProfile, LaneState,
        ProfileValueSource, ReasoningLevel, RunProfileTelemetry, RunStatus,
    };
    use crab_discord::{GatewayConversationKind, GatewayMessage};

    use super::{DispatchedTurn, QueuedTurn, TurnExecutor, TurnExecutorRuntime};
    use crate::composition::compose_runtime_with_processes_and_queue_limit;
    use crate::test_support::{
        runtime_config_for_workspace_with_lanes, FakeCodexProcess, FakeOpenCodeProcess,
        TempWorkspace,
    };

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        now_epochs: VecDeque<u64>,
        resolve_profile_results: VecDeque<CrabResult<RunProfileTelemetry>>,
        ensure_session_results: VecDeque<CrabResult<crab_core::PhysicalSession>>,
        build_context_results: VecDeque<CrabResult<String>>,
        execute_turn_results: VecDeque<CrabResult<Vec<crab_backends::BackendEvent>>>,
        ensure_session_sabotage_path: Option<PathBuf>,
        now_epoch_sabotage: Option<(usize, PathBuf)>,
        now_epoch_call_count: usize,
        steps: Vec<String>,
    }

    impl FakeRuntime {
        fn with_backend_events(
            backend_events: Vec<crab_backends::BackendEvent>,
            now_epochs: &[u64],
        ) -> Self {
            let session = crab_core::PhysicalSession {
                id: "physical-1".to_string(),
                logical_session_id: "discord:channel:777".to_string(),
                backend: BackendKind::Codex,
                backend_session_id: "thread-abc".to_string(),
                created_at_epoch_ms: 1_739_173_200_000,
                last_turn_id: None,
            };
            Self {
                now_epochs: VecDeque::from(now_epochs.to_vec()),
                resolve_profile_results: VecDeque::from(vec![Ok(sample_profile_telemetry())]),
                ensure_session_results: VecDeque::from(vec![Ok(session)]),
                build_context_results: VecDeque::from(vec![Ok("context".to_string())]),
                execute_turn_results: VecDeque::from(vec![Ok(backend_events)]),
                ensure_session_sabotage_path: None,
                now_epoch_sabotage: None,
                now_epoch_call_count: 0,
                steps: Vec::new(),
            }
        }

        fn with_ensure_session_sabotage_path(mut self, path: PathBuf) -> Self {
            self.ensure_session_sabotage_path = Some(path);
            self
        }

        fn with_now_epoch_sabotage(mut self, call_index: usize, path: PathBuf) -> Self {
            self.now_epoch_sabotage = Some((call_index, path));
            self
        }

        fn pop_result<T: Clone>(
            queue: &mut VecDeque<CrabResult<T>>,
            context: &'static str,
        ) -> CrabResult<T> {
            match queue.pop_front() {
                Some(result) => result,
                None => Err(CrabError::InvariantViolation {
                    context,
                    message: "missing scripted runtime result".to_string(),
                }),
            }
        }
    }

    impl TurnExecutorRuntime for FakeRuntime {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            self.now_epoch_call_count += 1;
            if let Some((trigger_call, path)) = self.now_epoch_sabotage.as_ref() {
                if *trigger_call == self.now_epoch_call_count {
                    replace_path_with_directory(path);
                }
            }
            match self.now_epochs.pop_front() {
                Some(value) => Ok(value),
                None => Err(CrabError::InvariantViolation {
                    context: "turn_executor_test_clock",
                    message: "missing scripted timestamp".to_string(),
                }),
            }
        }

        fn resolve_run_profile(
            &mut self,
            _logical_session_id: &str,
            _author_id: &str,
            _user_input: &str,
        ) -> CrabResult<RunProfileTelemetry> {
            self.steps.push("resolve_run_profile".to_string());
            Self::pop_result(
                &mut self.resolve_profile_results,
                "turn_executor_test_resolve_profile",
            )
        }

        fn ensure_physical_session(
            &mut self,
            _logical_session_id: &str,
            _profile: &InferenceProfile,
            _active_physical_session_id: Option<&str>,
        ) -> CrabResult<crab_core::PhysicalSession> {
            self.steps.push("ensure_physical_session".to_string());
            if let Some(path) = self.ensure_session_sabotage_path.take() {
                replace_path_with_directory(&path);
            }
            Self::pop_result(
                &mut self.ensure_session_results,
                "turn_executor_test_ensure_session",
            )
        }

        fn build_turn_context(
            &mut self,
            _run: &crab_core::Run,
            _logical_session: &crab_core::LogicalSession,
            _physical_session: &crab_core::PhysicalSession,
        ) -> CrabResult<String> {
            self.steps.push("build_turn_context".to_string());
            Self::pop_result(
                &mut self.build_context_results,
                "turn_executor_test_build_context",
            )
        }

        fn execute_backend_turn(
            &mut self,
            _physical_session: &mut crab_core::PhysicalSession,
            _run: &crab_core::Run,
            _turn_id: &str,
            _turn_context: &str,
        ) -> CrabResult<Vec<crab_backends::BackendEvent>> {
            self.steps.push("execute_backend_turn".to_string());
            Self::pop_result(
                &mut self.execute_turn_results,
                "turn_executor_test_execute_turn",
            )
        }
    }

    fn sample_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: InferenceProfile {
                backend: BackendKind::Codex,
                model: "gpt-5-codex".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
            backend_source: ProfileValueSource::GlobalDefault,
            model_source: ProfileValueSource::GlobalDefault,
            reasoning_level_source: ProfileValueSource::GlobalDefault,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: "111111111111111111".to_string(),
            sender_is_owner: false,
            resolved_owner_profile: None,
        }
    }

    fn gateway_message(message_id: &str) -> GatewayMessage {
        GatewayMessage {
            message_id: message_id.to_string(),
            author_id: "111111111111111111".to_string(),
            author_is_bot: false,
            channel_id: "777".to_string(),
            guild_id: Some("555".to_string()),
            thread_id: None,
            content: "ship ws15-t2".to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
        }
    }

    fn backend_event(
        sequence: u64,
        kind: BackendEventKind,
        payload: &[(&str, &str)],
    ) -> crab_backends::BackendEvent {
        crab_backends::BackendEvent {
            sequence,
            kind,
            payload: payload
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        }
    }

    fn build_executor(
        workspace: &TempWorkspace,
        runtime: FakeRuntime,
        lane_queue_limit: usize,
    ) -> TurnExecutor<FakeCodexProcess, FakeOpenCodeProcess, FakeRuntime> {
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let composition = compose_runtime_with_processes_and_queue_limit(
            &config,
            "999999999999999999",
            FakeCodexProcess,
            FakeOpenCodeProcess,
            lane_queue_limit,
        )
        .expect("composition should build");
        TurnExecutor::new(composition, runtime)
    }

    fn invalid_sender_profile_telemetry() -> RunProfileTelemetry {
        let mut profile = sample_profile_telemetry();
        profile.sender_id = " ".to_string();
        profile
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

    fn run_file_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        state_root
            .join("runs")
            .join(hex_encode(logical_session_id.as_bytes()))
            .join(format!("{}.json", hex_encode(run_id.as_bytes())))
    }

    fn event_log_path(state_root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        state_root
            .join("events")
            .join(hex_encode(logical_session_id.as_bytes()))
            .join(format!("{}.jsonl", hex_encode(run_id.as_bytes())))
    }

    fn replace_path_with_directory(path: &Path) {
        if path.exists() {
            if path.is_dir() {
                fs::remove_dir_all(path).expect("existing directory should be removable");
            } else {
                fs::remove_file(path).expect("existing file should be removable");
            }
        }
        fs::create_dir_all(path).expect("directory fixture should be creatable");
    }

    #[test]
    fn process_gateway_message_runs_end_to_end_pipeline_and_finalizes_success() {
        let workspace = TempWorkspace::new("turn-executor", "success");
        let runtime = FakeRuntime::with_backend_events(
            vec![
                backend_event(1, BackendEventKind::TextDelta, &[("text", "hello")]),
                backend_event(2, BackendEventKind::ToolCall, &[("tool", "shell")]),
                backend_event(3, BackendEventKind::ToolResult, &[("status", "ok")]),
                backend_event(4, BackendEventKind::RunNote, &[("note", "thinking")]),
                backend_event(5, BackendEventKind::TurnCompleted, &[("finish", "done")]),
            ],
            &[1, 2, 3, 4, 5, 6, 7, 8],
        );
        let mut executor = build_executor(&workspace, runtime, 8);

        let dispatch = executor
            .process_gateway_message(gateway_message("m-1"))
            .expect("pipeline should succeed")
            .expect("message should dispatch");
        assert_eq!(
            dispatch,
            DispatchedTurn {
                logical_session_id: "discord:channel:777".to_string(),
                run_id: "run:discord:channel:777:m-1".to_string(),
                turn_id: "turn:run:discord:channel:777:m-1".to_string(),
                status: RunStatus::Succeeded,
                emitted_event_count: 7,
            }
        );

        let stored_run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(stored_run.status, RunStatus::Succeeded);
        assert_eq!(stored_run.started_at_epoch_ms, Some(2));
        assert_eq!(stored_run.completed_at_epoch_ms, Some(8));

        let session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.lane_state, LaneState::Idle);
        assert_eq!(session.queued_run_count, 0);
        assert_eq!(
            session.active_physical_session_id,
            Some("physical-1".to_string())
        );

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-1")
            .expect("event replay should succeed");
        assert_eq!(events.len(), 8);
        assert_eq!(events[0].kind, EventKind::RunState);
        assert_eq!(events[0].payload.get("state"), Some(&"queued".to_string()));
        assert_eq!(events[1].kind, EventKind::RunState);
        assert_eq!(events[1].payload.get("state"), Some(&"running".to_string()));
        assert_eq!(events[2].kind, EventKind::TextDelta);
        assert_eq!(events[3].kind, EventKind::ToolCall);
        assert_eq!(events[4].kind, EventKind::ToolResult);
        assert_eq!(events[5].kind, EventKind::RunNote);
        assert_eq!(events[6].kind, EventKind::RunState);
        assert_eq!(
            events[6].payload.get("state"),
            Some(&"succeeded".to_string())
        );
        assert_eq!(events[7].kind, EventKind::RunState);
        assert_eq!(
            events[7].payload.get("state"),
            Some(&"succeeded".to_string())
        );

        assert_eq!(
            executor.runtime_mut().steps,
            vec![
                "resolve_run_profile".to_string(),
                "ensure_physical_session".to_string(),
                "build_turn_context".to_string(),
                "execute_backend_turn".to_string(),
            ]
        );
    }

    #[test]
    fn process_gateway_message_propagates_enqueue_errors() {
        let workspace = TempWorkspace::new("turn-executor", "process-enqueue-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        runtime.resolve_profile_results =
            VecDeque::from(vec![Err(CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            })]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .process_gateway_message(gateway_message("m-process-error"))
            .expect_err("enqueue errors should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_propagates_ingress_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "ingress-validation-error");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let mut invalid = gateway_message("m-invalid");
        invalid.channel_id = " ".to_string();

        let error = executor
            .enqueue_gateway_message(invalid)
            .expect_err("invalid gateway message should fail ingestion");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "gateway_message_validate",
                message: "channel_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_propagates_run_validation_errors_from_profile_resolution() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-run-validation");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        runtime.resolve_profile_results =
            VecDeque::from(vec![Ok(invalid_sender_profile_telemetry())]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-invalid-profile"))
            .expect_err("invalid profile telemetry should fail run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.sender_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_propagates_session_lookup_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-session-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        replace_path_with_directory(&session_path);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-session-io"))
            .expect_err("session lookup read errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn enqueue_propagates_session_persist_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-session-persist-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        replace_path_with_directory(&index_path);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-session-persist-io"))
            .expect_err("session upsert index errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn enqueue_rolls_back_scheduler_entry_when_post_enqueue_persistence_fails() {
        let workspace = TempWorkspace::new("turn-executor", "enqueue-rollback");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[10]);
        runtime.resolve_profile_results =
            VecDeque::from(vec![Err(CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            })]);
        let mut executor = build_executor(&workspace, runtime, 8);

        let error = executor
            .enqueue_gateway_message(gateway_message("m-rollback"))
            .expect_err("resolve profile failures should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "resolve_profile",
                message: "boom".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.total_queued_count(), 0);
    }

    #[test]
    fn enqueue_rejects_queue_overflow_and_preserves_existing_fifo_item() {
        let workspace = TempWorkspace::new("turn-executor", "overflow");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 1);

        let first = executor
            .enqueue_gateway_message(gateway_message("m-1"))
            .expect("first enqueue should succeed")
            .expect("first message should be queued");
        assert_eq!(
            first,
            QueuedTurn {
                logical_session_id: "discord:channel:777".to_string(),
                run_id: "run:discord:channel:777:m-1".to_string(),
                message_id: "m-1".to_string(),
                author_id: "111111111111111111".to_string(),
                routing_key: crab_discord::RoutingKey::Channel {
                    channel_id: "777".to_string(),
                },
                queued_run_count: 1,
            }
        );

        let overflow = executor
            .enqueue_gateway_message(gateway_message("m-2"))
            .expect_err("queue overflow should fail");
        assert_eq!(
            overflow,
            CrabError::InvariantViolation {
                context: "lane_queue_overflow",
                message:
                    "Queue is full for this session (limit=1). Please wait for in-flight runs to complete before retrying.".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.total_queued_count(), 1);
    }

    #[test]
    fn dispatch_propagates_clock_errors_before_marking_run_running() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-start-clock");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-dispatch-clock"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing timestamp should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_running_timestamp_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-running-timestamp");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 9, 11]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-running-timestamp"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("backward start timestamp should fail run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "started_at_epoch_ms must be greater than or equal to queued_at_epoch_ms"
                    .to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_session_lookup_io_errors_while_marking_running() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-session-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-dispatch-session-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        replace_path_with_directory(&session_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("session lookup read errors should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_session_persist_io_errors_while_marking_running() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-running-session-persist-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-running-session-persist-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        replace_path_with_directory(&index_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("session upsert errors should fail dispatch while entering running state");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_run_persist_io_errors_after_physical_session_binding() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-physical-run-persist-io");
        let run_id = "run:discord:channel:777:m-physical-run-persist-io";
        let state_root = state_root(&workspace);
        let run_path = run_file_path(&state_root, "discord:channel:777", run_id);
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3])
            .with_ensure_session_sabotage_path(run_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-physical-run-persist-io"))
            .expect("enqueue should succeed");

        let _error = executor
            .dispatch_next_run()
            .expect_err("run upsert errors after session binding should fail dispatch");
    }

    #[test]
    fn dispatch_propagates_session_persist_io_errors_after_backend_binding() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-backend-session-persist-io");
        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3])
            .with_ensure_session_sabotage_path(index_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-backend-session-persist-io"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("session upsert errors after backend binding should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_ensure_physical_session_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-ensure-session");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        runtime.ensure_session_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "ensure_session",
            message: "cannot reuse backend session".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-ensure-session"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("session hydration errors should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "ensure_session",
                message: "cannot reuse backend session".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_backend_turn_execution_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-execute-turn");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        runtime.execute_turn_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "execute_turn",
            message: "backend refused turn".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-execute-turn"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("turn execution errors should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "execute_turn",
                message: "backend refused turn".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_backend_append_clock_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-append-clock");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TextDelta,
                &[("text", "delta")],
            )],
            &[1, 2],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-append-clock"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing backend event timestamp should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_completion_clock_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-completion-clock");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnCompleted,
                &[("done", "true")],
            )],
            &[1, 2, 3],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-completion-clock"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing completion timestamp should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_completion_timestamp_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-completion-timestamp");
        let runtime = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnCompleted,
                &[("done", "true")],
            )],
            &[1, 5, 6, 4, 7],
        );
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-completion-timestamp"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("backward completion timestamp should fail run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message:
                    "completed_at_epoch_ms must be greater than or equal to started_at_epoch_ms"
                        .to_string(),
            }
        );
    }

    #[test]
    fn dispatch_propagates_completion_session_persist_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-completion-session-persist");
        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4])
            .with_now_epoch_sabotage(3, index_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-completion-session-persist"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("session upsert errors after completion should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_final_run_state_append_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-final-state-append-io");
        let run_id = "run:discord:channel:777:m-final-state-append-io";
        let state_root = state_root(&workspace);
        let log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3, 4])
            .with_now_epoch_sabotage(3, log_path);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-final-state-append-io"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("final run-state event append errors should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_run_lookup_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-run-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-run-io";
        executor
            .enqueue_gateway_message(gateway_message("m-run-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let run_path = run_file_path(&state_root, "discord:channel:777", run_id);
        replace_path_with_directory(&run_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("run lookup read errors should fail dispatch");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "run_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_propagates_running_state_append_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "dispatch-running-append-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2, 3]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-running-append-io";
        executor
            .enqueue_gateway_message(gateway_message("m-running-append-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let log_path = event_log_path(&state_root, "discord:channel:777", run_id);
        replace_path_with_directory(&log_path);

        let error = executor
            .dispatch_next_run()
            .expect_err("running-state event append should fail when log path is a directory");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "event_replay_read",
                ..
            }
        ));
    }

    #[test]
    fn mark_run_failed_propagates_run_validation_errors() {
        let workspace = TempWorkspace::new("turn-executor", "mark-failed-run-validate");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 9]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-mark-failed-validate";
        executor
            .enqueue_gateway_message(gateway_message("m-mark-failed-validate"))
            .expect("enqueue should succeed");

        let error = executor
            .mark_run_failed(
                "discord:channel:777",
                run_id,
                &CrabError::InvariantViolation {
                    context: "test",
                    message: "force failure".to_string(),
                },
            )
            .expect_err("backward completion timestamp should fail failed-run persistence");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message:
                    "completed_at_epoch_ms must be greater than or equal to queued_at_epoch_ms"
                        .to_string(),
            }
        );
    }

    #[test]
    fn mark_run_failed_propagates_session_lookup_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "mark-failed-session-read-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 11]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-mark-failed-session-io";
        executor
            .enqueue_gateway_message(gateway_message("m-mark-failed-session-io"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let session_path = session_file_path(&state_root, "discord:channel:777");
        replace_path_with_directory(&session_path);

        let error = executor
            .mark_run_failed(
                "discord:channel:777",
                run_id,
                &CrabError::InvariantViolation {
                    context: "test",
                    message: "force failure".to_string(),
                },
            )
            .expect_err("session lookup errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                ..
            }
        ));
    }

    #[test]
    fn mark_run_failed_propagates_session_persist_io_errors() {
        let workspace = TempWorkspace::new("turn-executor", "mark-failed-session-upsert-io");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 11]);
        let mut executor = build_executor(&workspace, runtime, 8);
        let run_id = "run:discord:channel:777:m-mark-failed-session-persist";
        executor
            .enqueue_gateway_message(gateway_message("m-mark-failed-session-persist"))
            .expect("enqueue should succeed");

        let state_root = state_root(&workspace);
        let index_path = state_root.join("sessions.index.json");
        replace_path_with_directory(&index_path);

        let error = executor
            .mark_run_failed(
                "discord:channel:777",
                run_id,
                &CrabError::InvariantViolation {
                    context: "test",
                    message: "force failure".to_string(),
                },
            )
            .expect_err("session persist errors should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                ..
            }
        ));
    }

    #[test]
    fn dispatch_marks_run_failed_and_releases_lane_when_runtime_step_errors() {
        let workspace = TempWorkspace::new("turn-executor", "runtime-error");
        let mut runtime = FakeRuntime::with_backend_events(Vec::new(), &[10, 11, 12]);
        runtime.build_context_results = VecDeque::from(vec![Err(CrabError::InvariantViolation {
            context: "build_context",
            message: "cannot build context".to_string(),
        })]);
        let mut executor = build_executor(&workspace, runtime, 8);
        executor
            .enqueue_gateway_message(gateway_message("m-error"))
            .expect("enqueue should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("runtime build errors should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "build_context",
                message: "cannot build context".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.active_lane_count(), 0);

        let run = executor
            .composition()
            .state_stores
            .run_store
            .get_run("discord:channel:777", "run:discord:channel:777:m-error")
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(run.status, RunStatus::Failed);

        let events = executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-error")
            .expect("event replay should succeed");
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].payload.get("state"), Some(&"queued".to_string()));
        assert_eq!(events[1].payload.get("state"), Some(&"running".to_string()));
        assert_eq!(events[2].payload.get("state"), Some(&"failed".to_string()));
        assert!(events[2].payload.contains_key("reason"));
    }

    #[test]
    fn dispatch_derives_cancelled_and_failed_statuses_from_backend_event_stream() {
        let workspace_cancelled = TempWorkspace::new("turn-executor", "cancelled");
        let runtime_cancelled = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::TurnInterrupted,
                &[("reason", "cancelled")],
            )],
            &[1, 2, 3, 4],
        );
        let mut cancelled_executor = build_executor(&workspace_cancelled, runtime_cancelled, 8);
        cancelled_executor
            .enqueue_gateway_message(gateway_message("m-cancel"))
            .expect("enqueue should succeed");
        let cancelled = cancelled_executor
            .dispatch_next_run()
            .expect("dispatch should succeed")
            .expect("run should dispatch");
        assert_eq!(cancelled.status, RunStatus::Cancelled);

        let workspace_failed = TempWorkspace::new("turn-executor", "failed-from-backend");
        let runtime_failed = FakeRuntime::with_backend_events(
            vec![backend_event(
                1,
                BackendEventKind::Error,
                &[("message", "backend exploded")],
            )],
            &[10, 11, 12, 13],
        );
        let mut failed_executor = build_executor(&workspace_failed, runtime_failed, 8);
        failed_executor
            .enqueue_gateway_message(gateway_message("m-failed"))
            .expect("enqueue should succeed");
        let failed = failed_executor
            .dispatch_next_run()
            .expect("dispatch should succeed")
            .expect("run should dispatch");
        assert_eq!(failed.status, RunStatus::Failed);

        let failed_events = failed_executor
            .composition()
            .state_stores
            .event_store
            .replay_run("discord:channel:777", "run:discord:channel:777:m-failed")
            .expect("event replay should succeed");
        assert_eq!(failed_events[2].kind, EventKind::Error);
    }

    #[test]
    fn dispatch_without_pending_work_returns_none_and_bot_messages_are_ignored() {
        let workspace = TempWorkspace::new("turn-executor", "idle");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        let mut executor = build_executor(&workspace, runtime, 8);

        assert!(executor
            .dispatch_next_run()
            .expect("empty dispatch should succeed")
            .is_none());

        let mut bot_message = gateway_message("m-bot");
        bot_message.author_is_bot = true;
        assert!(executor
            .process_gateway_message(bot_message)
            .expect("ingress should succeed")
            .is_none());
        assert_eq!(executor.composition().scheduler.total_queued_count(), 0);
    }

    #[test]
    fn helper_paths_cover_missing_scripts_and_status_tokens() {
        assert_eq!(super::run_status_token(RunStatus::Queued), "queued");
        assert_eq!(super::run_status_token(RunStatus::Running), "running");

        let workspace_missing_profile =
            TempWorkspace::new("turn-executor", "missing-profile-script");
        let mut runtime_missing_profile = FakeRuntime::with_backend_events(Vec::new(), &[1]);
        runtime_missing_profile.resolve_profile_results = VecDeque::new();
        let mut missing_profile_executor =
            build_executor(&workspace_missing_profile, runtime_missing_profile, 8);
        let profile_error = missing_profile_executor
            .enqueue_gateway_message(gateway_message("m-missing-profile"))
            .expect_err("missing profile script should fail");
        assert_eq!(
            profile_error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_resolve_profile",
                message: "missing scripted runtime result".to_string(),
            }
        );

        let workspace_missing_clock = TempWorkspace::new("turn-executor", "missing-clock-script");
        let runtime_missing_clock = FakeRuntime::with_backend_events(Vec::new(), &[]);
        let mut missing_clock_executor =
            build_executor(&workspace_missing_clock, runtime_missing_clock, 8);
        let clock_error = missing_clock_executor
            .enqueue_gateway_message(gateway_message("m-missing-clock"))
            .expect_err("missing clock script should fail");
        assert_eq!(
            clock_error,
            CrabError::InvariantViolation {
                context: "turn_executor_test_clock",
                message: "missing scripted timestamp".to_string(),
            }
        );
    }

    #[test]
    fn dispatch_reports_missing_run_for_unknown_scheduler_entry() {
        let workspace = TempWorkspace::new("turn-executor", "missing-run");
        let runtime = FakeRuntime::with_backend_events(Vec::new(), &[1, 2]);
        let mut executor = build_executor(&workspace, runtime, 8);

        executor
            .composition_mut()
            .scheduler
            .enqueue(
                "discord:channel:777",
                crab_scheduler::QueuedRun {
                    run_id: "missing".to_string(),
                },
            )
            .expect("queue injection should succeed");

        let error = executor
            .dispatch_next_run()
            .expect_err("missing run should fail dispatch");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "turn_executor_run_lookup",
                message: "run discord:channel:777/missing not found".to_string(),
            }
        );
        assert_eq!(executor.composition().scheduler.active_lane_count(), 0);
    }

    #[test]
    fn helper_replace_path_with_directory_handles_existing_directory() {
        let workspace = TempWorkspace::new("turn-executor", "replace-path-dir");
        let target = workspace.path.join("already-directory");
        fs::create_dir_all(&target).expect("directory fixture should be creatable");

        replace_path_with_directory(&target);
        assert!(target.is_dir());
    }
}
