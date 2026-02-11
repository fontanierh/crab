use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use std::{cell::RefCell, thread_local};

use crab_backends::{BackendEvent, BackendEventKind, CodexAppServerProcess, OpenCodeServerProcess};
use crab_core::{
    BackendKind, CrabError, CrabResult, InferenceProfile, OwnerConfig, OwnerProfileMetadata,
    ProfileValueSource, ReasoningLevel, Run, RunProfileTelemetry, RuntimeConfig,
};
use crab_discord::GatewayMessage;

use crate::{boot_runtime_with_processes, run_heartbeat_if_due, TurnExecutor, TurnExecutorRuntime};

pub const DEFAULT_DAEMON_TICK_INTERVAL_MS: u64 = 250;

pub trait DaemonDiscordIo {
    fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>>;

    fn post_message(
        &mut self,
        channel_id: &str,
        delivery_id: &str,
        content: &str,
    ) -> CrabResult<()>;

    fn edit_message(
        &mut self,
        channel_id: &str,
        delivery_id: &str,
        content: &str,
    ) -> CrabResult<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonConfig {
    pub bot_user_id: String,
    pub tick_interval_ms: u64,
    pub max_iterations: Option<u64>,
}

impl DaemonConfig {
    pub fn validate(&self) -> CrabResult<()> {
        if self.bot_user_id.trim().is_empty() {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_BOT_USER_ID",
                value: self.bot_user_id.clone(),
                reason: "must not be empty",
            });
        }
        if self.tick_interval_ms == 0 {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_DAEMON_TICK_INTERVAL_MS",
                value: self.tick_interval_ms.to_string(),
                reason: "must be greater than 0",
            });
        }
        if self
            .max_iterations
            .is_some_and(|iterations| iterations == 0)
        {
            return Err(CrabError::InvalidConfig {
                key: "CRAB_DAEMON_MAX_ITERATIONS",
                value: "0".to_string(),
                reason: "must be greater than 0 when provided",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DaemonLoopStats {
    pub iterations: u64,
    pub ingested_messages: u64,
    pub dispatched_runs: u64,
    pub heartbeat_cycles: u64,
}

pub trait DaemonLoopControl {
    fn now_epoch_ms(&mut self) -> CrabResult<u64>;
    fn should_shutdown(&self) -> bool;
    fn sleep_tick(&mut self, tick_interval_ms: u64) -> CrabResult<()>;
}

#[derive(Debug, Clone)]
pub struct SystemDaemonLoopControl {
    shutdown_flag: Arc<AtomicBool>,
}

impl SystemDaemonLoopControl {
    pub fn install() -> CrabResult<Self> {
        Self::install_with_handler(|signal_flag| {
            ctrlc::set_handler({
                let shutdown_flag = Arc::clone(&signal_flag);
                move || shutdown_flag.store(true, Ordering::SeqCst)
            })
            .map_err(|error| format!("failed to install Ctrl-C handler: {error}"))
        })
    }

    fn install_with_handler(
        handler_installer: fn(Arc<AtomicBool>) -> Result<(), String>,
    ) -> CrabResult<Self> {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        handler_installer(Arc::clone(&shutdown_flag)).map_err(|message| {
            CrabError::InvariantViolation {
                context: "daemon_loop_signal_handler",
                message,
            }
        })?;
        Ok(Self { shutdown_flag })
    }
}

impl DaemonLoopControl for SystemDaemonLoopControl {
    fn now_epoch_ms(&mut self) -> CrabResult<u64> {
        now_epoch_ms()
    }

    fn should_shutdown(&self) -> bool {
        self.shutdown_flag.load(Ordering::SeqCst)
    }

    fn sleep_tick(&mut self, tick_interval_ms: u64) -> CrabResult<()> {
        thread::sleep(Duration::from_millis(tick_interval_ms));
        Ok(())
    }
}

#[derive(Debug)]
pub struct DaemonTurnRuntime<D: DaemonDiscordIo> {
    discord: D,
    owner: OwnerConfig,
    next_session_sequence: u64,
    physical_sessions: BTreeMap<String, crab_core::PhysicalSession>,
}

#[cfg(test)]
type SessionNowEpochMsOverride = fn() -> CrabResult<u64>;

#[cfg(test)]
thread_local! {
    static SESSION_NOW_EPOCH_MS_OVERRIDE: RefCell<Option<SessionNowEpochMsOverride>> = RefCell::new(None);
}

impl<D: DaemonDiscordIo> DaemonTurnRuntime<D> {
    pub fn new(owner: OwnerConfig, discord: D) -> CrabResult<Self> {
        Ok(Self {
            discord,
            owner,
            next_session_sequence: 0,
            physical_sessions: BTreeMap::new(),
        })
    }

    fn session_now_epoch_ms() -> CrabResult<u64> {
        #[cfg(test)]
        if let Some(override_fn) = SESSION_NOW_EPOCH_MS_OVERRIDE.with(|cell| *cell.borrow()) {
            return override_fn();
        }
        now_epoch_ms()
    }

    #[cfg(test)]
    fn set_session_now_epoch_ms_override(override_fn: Option<SessionNowEpochMsOverride>) {
        SESSION_NOW_EPOCH_MS_OVERRIDE.with(|cell| {
            *cell.borrow_mut() = override_fn;
        });
    }

    pub fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
        self.discord.next_gateway_message()
    }

    fn next_physical_session_id(&mut self, logical_session_id: &str) -> String {
        self.next_session_sequence = self.next_session_sequence.saturating_add(1);
        format!(
            "physical:{logical_session_id}:{}",
            self.next_session_sequence
        )
    }

    fn owner_profile_for_sender(&self, sender_is_owner: bool) -> Option<OwnerProfileMetadata> {
        if !sender_is_owner {
            return None;
        }

        let metadata = OwnerProfileMetadata {
            machine_location: self.owner.machine_location.clone(),
            machine_timezone: self.owner.machine_timezone.clone(),
            default_backend: self.owner.profile_defaults.backend,
            default_model: self.owner.profile_defaults.model.clone(),
            default_reasoning_level: self.owner.profile_defaults.reasoning_level,
        };

        if metadata.machine_location.is_none()
            && metadata.machine_timezone.is_none()
            && metadata.default_backend.is_none()
            && metadata.default_model.is_none()
            && metadata.default_reasoning_level.is_none()
        {
            return None;
        }

        Some(metadata)
    }
}

impl<D: DaemonDiscordIo> TurnExecutorRuntime for DaemonTurnRuntime<D> {
    fn now_epoch_ms(&mut self) -> CrabResult<u64> {
        now_epoch_ms()
    }

    fn resolve_run_profile(
        &mut self,
        _logical_session_id: &str,
        author_id: &str,
        _user_input: &str,
    ) -> CrabResult<RunProfileTelemetry> {
        let sender_is_owner = self
            .owner
            .discord_user_ids
            .iter()
            .any(|owner_id| owner_id == author_id);

        let backend = if sender_is_owner {
            self.owner
                .profile_defaults
                .backend
                .unwrap_or(BackendKind::Codex)
        } else {
            BackendKind::Codex
        };
        let model = if sender_is_owner {
            self.owner
                .profile_defaults
                .model
                .clone()
                .unwrap_or_else(|| "auto".to_string())
        } else {
            "auto".to_string()
        };
        let reasoning_level = if sender_is_owner {
            self.owner
                .profile_defaults
                .reasoning_level
                .unwrap_or(ReasoningLevel::Medium)
        } else {
            ReasoningLevel::Medium
        };

        Ok(RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: InferenceProfile {
                backend,
                model,
                reasoning_level,
            },
            backend_source: ProfileValueSource::GlobalDefault,
            model_source: ProfileValueSource::GlobalDefault,
            reasoning_level_source: ProfileValueSource::GlobalDefault,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: author_id.to_string(),
            sender_is_owner,
            resolved_owner_profile: self.owner_profile_for_sender(sender_is_owner),
        })
    }

    fn ensure_physical_session(
        &mut self,
        logical_session_id: &str,
        profile: &InferenceProfile,
        active_physical_session_id: Option<&str>,
    ) -> CrabResult<crab_core::PhysicalSession> {
        if let Some(active_id) = active_physical_session_id {
            if let Some(existing) = self.physical_sessions.get(active_id) {
                return Ok(existing.clone());
            }
        }

        let id = active_physical_session_id
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| self.next_physical_session_id(logical_session_id));
        let session = crab_core::PhysicalSession {
            id: id.clone(),
            logical_session_id: logical_session_id.to_string(),
            backend: profile.backend,
            backend_session_id: format!("backend-session:{id}"),
            created_at_epoch_ms: Self::session_now_epoch_ms()?,
            last_turn_id: None,
        };
        self.physical_sessions.insert(id, session.clone());
        Ok(session)
    }

    fn build_turn_context(
        &mut self,
        run: &Run,
        _logical_session: &crab_core::LogicalSession,
        _physical_session: &crab_core::PhysicalSession,
    ) -> CrabResult<String> {
        Ok(run.user_input.clone())
    }

    fn execute_backend_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
        _turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        physical_session.last_turn_id = Some(turn_id.to_string());
        let response = format!("Crab stub response: {}", run.user_input.trim());
        let input_tokens = u64::try_from(run.user_input.split_whitespace().count())
            .unwrap_or(1)
            .max(1);
        let output_tokens = u64::try_from(response.split_whitespace().count())
            .unwrap_or(1)
            .max(1);
        let total_tokens = input_tokens.saturating_add(output_tokens);

        Ok(vec![
            BackendEvent {
                sequence: 1,
                kind: BackendEventKind::TextDelta,
                payload: BTreeMap::from([("delta".to_string(), response)]),
            },
            BackendEvent {
                sequence: 2,
                kind: BackendEventKind::RunNote,
                payload: BTreeMap::from([
                    (
                        "run_usage_input_tokens".to_string(),
                        input_tokens.to_string(),
                    ),
                    (
                        "run_usage_output_tokens".to_string(),
                        output_tokens.to_string(),
                    ),
                    (
                        "run_usage_total_tokens".to_string(),
                        total_tokens.to_string(),
                    ),
                    ("run_usage_source".to_string(), "daemon_stub".to_string()),
                ]),
            },
            BackendEvent {
                sequence: 3,
                kind: BackendEventKind::TurnCompleted,
                payload: BTreeMap::from([("finish".to_string(), "stub".to_string())]),
            },
        ])
    }

    fn deliver_assistant_output(
        &mut self,
        _run: &Run,
        channel_id: &str,
        message_id: &str,
        edit_generation: u32,
        content: &str,
    ) -> CrabResult<()> {
        if edit_generation == 0 {
            return self.discord.post_message(channel_id, message_id, content);
        }
        self.discord.edit_message(channel_id, message_id, content)
    }
}

pub fn run_daemon_loop_with_transport<CP, OP, D, C>(
    runtime_config: &RuntimeConfig,
    daemon_config: &DaemonConfig,
    codex_process: CP,
    opencode_process: OP,
    discord: D,
    control: &mut C,
) -> CrabResult<DaemonLoopStats>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    D: DaemonDiscordIo,
    C: DaemonLoopControl + ?Sized,
{
    run_daemon_loop_with_transport_and_runtime_builder(
        runtime_config,
        daemon_config,
        codex_process,
        opencode_process,
        discord,
        control,
        DaemonTurnRuntime::new,
    )
}

fn run_daemon_loop_with_transport_and_runtime_builder<CP, OP, D, C>(
    runtime_config: &RuntimeConfig,
    daemon_config: &DaemonConfig,
    codex_process: CP,
    opencode_process: OP,
    discord: D,
    control: &mut C,
    runtime_builder: fn(OwnerConfig, D) -> CrabResult<DaemonTurnRuntime<D>>,
) -> CrabResult<DaemonLoopStats>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    D: DaemonDiscordIo,
    C: DaemonLoopControl + ?Sized,
{
    daemon_config.validate()?;
    let now_epoch_ms = control.now_epoch_ms()?;
    let mut boot = boot_runtime_with_processes(
        runtime_config,
        &daemon_config.bot_user_id,
        codex_process,
        opencode_process,
        now_epoch_ms,
    )?;
    if !boot.startup_reconciliation.recovered_runs.is_empty()
        || !boot.startup_reconciliation.cleared_session_ids.is_empty()
    {
        // `tracing` macros can produce stubborn per-line coverage gaps under `cargo llvm-cov`
        // (cfg(coverage)), even when the behavior is exercised. Keep runtime logs, but exclude
        // them from coverage builds where stdout/stderr output is not the product.
        #[cfg(not(coverage))]
        tracing::warn!(
            recovered_runs = boot.startup_reconciliation.recovered_runs.len(),
            cleared_sessions = boot.startup_reconciliation.cleared_session_ids.len(),
            "startup reconciliation recovered in-flight work"
        );
        #[cfg(not(coverage))]
        tracing::debug!(
            recovered = ?boot.startup_reconciliation.recovered_runs,
            cleared = ?boot.startup_reconciliation.cleared_session_ids,
            "startup reconciliation details"
        );
    } else {
        tracing::info!("startup reconciliation: no in-flight work recovered");
    }
    boot.composition.backends.codex.ensure_started()?;
    boot.composition.backends.opencode.ensure_running()?;

    let mut heartbeat_loop_state = boot.heartbeat_loop_state;
    let runtime = runtime_builder(runtime_config.owner.clone(), discord)?;
    let mut executor = TurnExecutor::new(boot.composition, runtime);
    let mut stats = DaemonLoopStats::default();

    loop {
        if control.should_shutdown() {
            break;
        }
        match daemon_config.max_iterations {
            Some(max_iterations) if stats.iterations >= max_iterations => break,
            _ => {}
        }

        stats.iterations = stats.iterations.saturating_add(1);

        if let Some(message) = executor.runtime_mut().next_gateway_message()? {
            if executor.enqueue_gateway_message(message)?.is_some() {
                stats.ingested_messages = stats.ingested_messages.saturating_add(1);
            }
        }

        while executor.dispatch_next_run()?.is_some() {
            stats.dispatched_runs = stats.dispatched_runs.saturating_add(1);
        }

        let now_epoch_ms = control.now_epoch_ms()?;
        if let Some(outcome) = run_heartbeat_if_due(
            executor.composition_mut(),
            &mut heartbeat_loop_state,
            now_epoch_ms,
        )? {
            stats.heartbeat_cycles = stats.heartbeat_cycles.saturating_add(1);

            let had_actions = !outcome.cancelled_runs.is_empty()
                || !outcome.hard_stopped_runs.is_empty()
                || !outcome.restarted_backends.is_empty()
                || outcome.dispatcher_nudged;
            if had_actions {
                #[cfg(not(coverage))]
                tracing::warn!(
                    cancelled_runs = outcome.cancelled_runs.len(),
                    hard_stopped_runs = outcome.hard_stopped_runs.len(),
                    restarted_backends = outcome.restarted_backends.len(),
                    dispatcher_nudged = outcome.dispatcher_nudged,
                    events = outcome.events.len(),
                    "heartbeat took corrective action"
                );
                #[cfg(not(coverage))]
                tracing::debug!(?outcome, "heartbeat outcome details");
            } else {
                tracing::debug!(events = outcome.events.len(), "heartbeat cycle complete");
            }
        }

        control.sleep_tick(daemon_config.tick_interval_ms)?;
    }

    tracing::info!(
        iterations = stats.iterations,
        ingested = stats.ingested_messages,
        dispatched = stats.dispatched_runs,
        heartbeats = stats.heartbeat_cycles,
        "daemon loop exiting: shutting down backends"
    );
    executor.composition_mut().backends.codex.stop()?;
    executor.composition_mut().backends.opencode.stop()?;
    Ok(stats)
}

fn now_epoch_ms() -> CrabResult<u64> {
    epoch_ms_from_system_time(SystemTime::now())
}

fn epoch_ms_from_system_time(now: SystemTime) -> CrabResult<u64> {
    let duration =
        now.duration_since(UNIX_EPOCH)
            .map_err(|error| CrabError::InvariantViolation {
                context: "daemon_clock_now",
                message: format!("system clock is before unix epoch: {error}"),
            })?;
    epoch_ms_from_duration(duration)
}

fn epoch_ms_from_duration(duration: Duration) -> CrabResult<u64> {
    let millis = duration.as_millis();
    u64::try_from(millis).map_err(|_| CrabError::InvariantViolation {
        context: "daemon_clock_now",
        message: "epoch milliseconds overflow u64".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        epoch_ms_from_duration, epoch_ms_from_system_time, run_daemon_loop_with_transport,
        run_daemon_loop_with_transport_and_runtime_builder, DaemonConfig, DaemonDiscordIo,
        DaemonLoopControl, DaemonLoopStats, DaemonTurnRuntime, SystemDaemonLoopControl,
    };
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};
    use crate::TurnExecutorRuntime;
    use crab_backends::{
        CodexAppServerProcess, CodexProcessHandle, OpenCodeServerHandle, OpenCodeServerProcess,
    };
    use crab_core::{
        BackendKind, CrabError, CrabResult, InferenceProfile, LaneState, LogicalSession,
        OwnerConfig, ProfileValueSource, ReasoningLevel, Run, RunProfileTelemetry, RunStatus,
        TokenAccounting,
    };
    use crab_discord::{GatewayConversationKind, GatewayMessage};
    use crab_store::{RunStore, SessionStore};
    use std::collections::VecDeque;
    #[cfg(unix)]
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Clone, Default)]
    struct TrackingCodexState {
        spawn_calls: usize,
        terminate_calls: usize,
    }

    #[derive(Debug, Clone, Default)]
    struct TrackingOpenCodeState {
        spawn_calls: usize,
        terminate_calls: usize,
    }

    #[derive(Debug, Clone)]
    struct TrackingCodexProcess {
        state: Arc<Mutex<TrackingCodexState>>,
        spawn_error: Option<&'static str>,
        terminate_error: Option<&'static str>,
    }

    #[derive(Debug, Clone)]
    struct TrackingOpenCodeProcess {
        state: Arc<Mutex<TrackingOpenCodeState>>,
        spawn_error: Option<&'static str>,
        terminate_error: Option<&'static str>,
    }

    impl TrackingCodexProcess {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingCodexState::default())),
                spawn_error: None,
                terminate_error: None,
            }
        }

        fn with_spawn_error(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingCodexState::default())),
                spawn_error: Some(message),
                terminate_error: None,
            }
        }

        fn with_terminate_error(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingCodexState::default())),
                spawn_error: None,
                terminate_error: Some(message),
            }
        }

        fn stats(&self) -> TrackingCodexState {
            self.state.lock().expect("lock should succeed").clone()
        }
    }

    impl TrackingOpenCodeProcess {
        fn new() -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingOpenCodeState::default())),
                spawn_error: None,
                terminate_error: None,
            }
        }

        fn with_spawn_error(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingOpenCodeState::default())),
                spawn_error: Some(message),
                terminate_error: None,
            }
        }

        fn with_terminate_error(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingOpenCodeState::default())),
                spawn_error: None,
                terminate_error: Some(message),
            }
        }

        fn stats(&self) -> TrackingOpenCodeState {
            self.state.lock().expect("lock should succeed").clone()
        }
    }

    impl CodexAppServerProcess for TrackingCodexProcess {
        fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle> {
            if let Some(message) = self.spawn_error {
                return Err(CrabError::InvariantViolation {
                    context: "daemon_test_codex_spawn",
                    message: message.to_string(),
                });
            }
            let mut state = self.state.lock().expect("lock should succeed");
            state.spawn_calls += 1;
            Ok(CodexProcessHandle {
                process_id: 111,
                started_at_epoch_ms: 1,
            })
        }

        fn is_healthy(&self, _handle: &CodexProcessHandle) -> bool {
            true
        }

        fn terminate_app_server(&self, _handle: &CodexProcessHandle) -> CrabResult<()> {
            if let Some(message) = self.terminate_error {
                return Err(CrabError::InvariantViolation {
                    context: "daemon_test_codex_terminate",
                    message: message.to_string(),
                });
            }
            let mut state = self.state.lock().expect("lock should succeed");
            state.terminate_calls += 1;
            Ok(())
        }
    }

    impl OpenCodeServerProcess for TrackingOpenCodeProcess {
        fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle> {
            if let Some(message) = self.spawn_error {
                return Err(CrabError::InvariantViolation {
                    context: "daemon_test_opencode_spawn",
                    message: message.to_string(),
                });
            }
            let mut state = self.state.lock().expect("lock should succeed");
            state.spawn_calls += 1;
            Ok(OpenCodeServerHandle {
                process_id: 222,
                started_at_epoch_ms: 1,
                server_base_url: "http://127.0.0.1:4210".to_string(),
            })
        }

        fn is_server_healthy(&self, _handle: &OpenCodeServerHandle) -> bool {
            true
        }

        fn terminate_server(&self, _handle: &OpenCodeServerHandle) -> CrabResult<()> {
            if let Some(message) = self.terminate_error {
                return Err(CrabError::InvariantViolation {
                    context: "daemon_test_opencode_terminate",
                    message: message.to_string(),
                });
            }
            let mut state = self.state.lock().expect("lock should succeed");
            state.terminate_calls += 1;
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct DiscordIoState {
        inbound: VecDeque<CrabResult<Option<GatewayMessage>>>,
        post_results: VecDeque<CrabResult<()>>,
        edit_results: VecDeque<CrabResult<()>>,
        posted: Vec<(String, String, String)>,
        edited: Vec<(String, String, String)>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedDiscordIo {
        state: Arc<Mutex<DiscordIoState>>,
    }

    impl ScriptedDiscordIo {
        fn with_state(state: DiscordIoState) -> Self {
            Self {
                state: Arc::new(Mutex::new(state)),
            }
        }

        fn state(&self) -> DiscordIoState {
            self.state.lock().expect("lock should succeed").clone()
        }
    }

    impl DaemonDiscordIo for ScriptedDiscordIo {
        fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
            self.state
                .lock()
                .expect("lock should succeed")
                .inbound
                .pop_front()
                .unwrap_or(Ok(None))
        }

        fn post_message(
            &mut self,
            channel_id: &str,
            delivery_id: &str,
            content: &str,
        ) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.posted.push((
                channel_id.to_string(),
                delivery_id.to_string(),
                content.to_string(),
            ));
            state.post_results.pop_front().unwrap_or(Ok(()))
        }

        fn edit_message(
            &mut self,
            channel_id: &str,
            delivery_id: &str,
            content: &str,
        ) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.edited.push((
                channel_id.to_string(),
                delivery_id.to_string(),
                content.to_string(),
            ));
            state.edit_results.pop_front().unwrap_or(Ok(()))
        }
    }

    #[derive(Debug, Clone)]
    struct ScriptedControl {
        now_values: VecDeque<u64>,
        shutdown: bool,
        slept: Vec<u64>,
    }

    impl ScriptedControl {
        fn with_now(now_values: Vec<u64>) -> Self {
            Self {
                now_values: VecDeque::from(now_values),
                shutdown: false,
                slept: Vec::new(),
            }
        }
    }

    impl DaemonLoopControl for ScriptedControl {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            self.now_values
                .pop_front()
                .ok_or(CrabError::InvariantViolation {
                    context: "daemon_test_now",
                    message: "missing scripted now value".to_string(),
                })
        }

        fn should_shutdown(&self) -> bool {
            self.shutdown
        }

        fn sleep_tick(&mut self, tick_interval_ms: u64) -> CrabResult<()> {
            self.slept.push(tick_interval_ms);
            Ok(())
        }
    }

    fn gateway_message(message_id: &str, author_id: &str, content: &str) -> GatewayMessage {
        GatewayMessage {
            message_id: message_id.to_string(),
            author_id: author_id.to_string(),
            author_is_bot: false,
            channel_id: "777".to_string(),
            guild_id: Some("555".to_string()),
            thread_id: None,
            content: content.to_string(),
            conversation_kind: GatewayConversationKind::GuildChannel,
        }
    }

    fn install_handler_ok(_flag: Arc<AtomicBool>) -> Result<(), String> {
        Ok(())
    }

    fn install_handler_err(_flag: Arc<AtomicBool>) -> Result<(), String> {
        Err("failed to install Ctrl-C handler: test".to_string())
    }

    fn fail_session_now_epoch_ms() -> CrabResult<u64> {
        Err(CrabError::InvariantViolation {
            context: "daemon_runtime_session_now",
            message: "forced session timestamp failure".to_string(),
        })
    }

    fn runtime_builder_err(
        _owner: OwnerConfig,
        _discord: ScriptedDiscordIo,
    ) -> CrabResult<DaemonTurnRuntime<ScriptedDiscordIo>> {
        Err(CrabError::InvariantViolation {
            context: "daemon_runtime_builder",
            message: "forced runtime build failure".to_string(),
        })
    }

    #[derive(Debug, Clone)]
    struct SleepFailControl {
        now_values: VecDeque<u64>,
    }

    impl SleepFailControl {
        fn with_now(now_values: Vec<u64>) -> Self {
            Self {
                now_values: VecDeque::from(now_values),
            }
        }
    }

    impl DaemonLoopControl for SleepFailControl {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            self.now_values
                .pop_front()
                .ok_or(CrabError::InvariantViolation {
                    context: "daemon_test_now",
                    message: "missing scripted now value".to_string(),
                })
        }

        fn should_shutdown(&self) -> bool {
            false
        }

        fn sleep_tick(&mut self, _tick_interval_ms: u64) -> CrabResult<()> {
            Err(CrabError::InvariantViolation {
                context: "daemon_test_sleep",
                message: "forced sleep failure".to_string(),
            })
        }
    }

    fn sample_run(sender_id: &str) -> Run {
        Run {
            id: "run-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            physical_session_id: None,
            status: RunStatus::Queued,
            user_input: "hello world".to_string(),
            profile: RunProfileTelemetry {
                requested_profile: None,
                resolved_profile: InferenceProfile {
                    backend: BackendKind::Codex,
                    model: "auto".to_string(),
                    reasoning_level: ReasoningLevel::Medium,
                },
                backend_source: ProfileValueSource::GlobalDefault,
                model_source: ProfileValueSource::GlobalDefault,
                reasoning_level_source: ProfileValueSource::GlobalDefault,
                fallback_applied: false,
                fallback_notes: Vec::new(),
                sender_id: sender_id.to_string(),
                sender_is_owner: false,
                resolved_owner_profile: None,
            },
            queued_at_epoch_ms: 1,
            started_at_epoch_ms: None,
            completed_at_epoch_ms: None,
        }
    }

    fn sample_session(
        lane_state: LaneState,
        active_physical_session_id: Option<String>,
    ) -> LogicalSession {
        LogicalSession {
            id: "discord:channel:777".to_string(),
            active_backend: BackendKind::Codex,
            active_profile: InferenceProfile {
                backend: BackendKind::Codex,
                model: "auto".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
            active_physical_session_id,
            last_successful_checkpoint_id: None,
            lane_state,
            queued_run_count: 0,
            last_activity_epoch_ms: 1,
            token_accounting: TokenAccounting {
                input_tokens: 0,
                output_tokens: 0,
                total_tokens: 0,
            },
        }
    }

    #[test]
    fn daemon_config_validation_rejects_invalid_values() {
        let blank_bot = DaemonConfig {
            bot_user_id: " ".to_string(),
            tick_interval_ms: 1,
            max_iterations: None,
        }
        .validate()
        .expect_err("blank bot user should fail");
        assert_eq!(
            blank_bot,
            CrabError::InvalidConfig {
                key: "CRAB_BOT_USER_ID",
                value: " ".to_string(),
                reason: "must not be empty"
            }
        );

        let zero_tick = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 0,
            max_iterations: None,
        }
        .validate()
        .expect_err("zero tick should fail");
        assert_eq!(
            zero_tick,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_TICK_INTERVAL_MS",
                value: "0".to_string(),
                reason: "must be greater than 0"
            }
        );

        let zero_max_iterations = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(0),
        }
        .validate()
        .expect_err("zero max iterations should fail");
        assert_eq!(
            zero_max_iterations,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_MAX_ITERATIONS",
                value: "0".to_string(),
                reason: "must be greater than 0 when provided"
            }
        );
    }

    #[test]
    fn daemon_loop_ingests_dispatches_and_stops_backends() {
        let workspace = TempWorkspace::new("daemon", "dispatch");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(2),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([
                Ok(Some(gateway_message("m-1", "111", "hello world"))),
                Ok(None),
            ]),
            ..DiscordIoState::default()
        });
        let discord_state = discord.clone();
        let codex = TrackingCodexProcess::new();
        let codex_state = codex.clone();
        let opencode = TrackingOpenCodeProcess::new();
        let opencode_state = opencode.clone();
        let mut control = ScriptedControl::with_now(vec![1_000, 1_001, 1_002]);

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        assert_eq!(
            stats,
            DaemonLoopStats {
                iterations: 2,
                ingested_messages: 1,
                dispatched_runs: 1,
                heartbeat_cycles: 0
            }
        );

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert!(discord.posted[0].2.contains("Crab stub response"));

        let codex_stats = codex_state.stats();
        assert_eq!(codex_stats.spawn_calls, 1);
        assert_eq!(codex_stats.terminate_calls, 1);

        let opencode_stats = opencode_state.stats();
        assert_eq!(opencode_stats.spawn_calls, 1);
        assert_eq!(opencode_stats.terminate_calls, 1);
        assert_eq!(control.slept, vec![5, 5]);
    }

    #[test]
    fn daemon_loop_runs_heartbeat_when_due() {
        let workspace = TempWorkspace::new("daemon", "heartbeat");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(2),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(None), Ok(None)]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000, 12_000, 24_000]);

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        assert_eq!(stats.heartbeat_cycles, 2);
    }

    #[test]
    fn daemon_loop_reports_startup_reconciliation_when_session_handle_is_non_idle() {
        let workspace = TempWorkspace::new("daemon", "startup-reconcile-session");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);

        // Seed a session that is non-idle and still has a physical handle; startup reconciliation
        // should clear it, which exercises the "recovered in-flight work" reporting branch.
        let state_root = workspace.path.join("state");
        let session_store = SessionStore::new(state_root);
        let seeded = sample_session(LaneState::Running, Some("phys-1".to_string()));
        session_store
            .upsert_session(&seeded)
            .expect("seed session should persist");

        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(None)]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000, 1_001]);

        run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        let updated = session_store
            .get_session(&seeded.id)
            .expect("session read should succeed")
            .expect("session should exist");
        assert_eq!(updated.active_physical_session_id, None);
        assert_eq!(updated.lane_state, LaneState::Idle);
    }

    #[test]
    fn daemon_loop_reports_heartbeat_actions_when_run_is_stalled() {
        let workspace = TempWorkspace::new("daemon", "heartbeat-actions");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);

        let state_root = workspace.path.join("state");
        let session_store = SessionStore::new(state_root.clone());
        let run_store = RunStore::new(state_root);

        // Avoid startup reconciliation clearing the session: it only clears when a physical handle
        // exists. We still want the lane to be considered active for heartbeat.
        let session = sample_session(LaneState::Running, None);
        session_store
            .upsert_session(&session)
            .expect("seed session should persist");

        let mut run = sample_run("111");
        run.id = "run-stalled-1".to_string();
        run.status = RunStatus::Running;
        run.started_at_epoch_ms = Some(1_000);
        run_store.upsert_run(&run).expect("seed run should persist");

        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(None)]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();

        // Boot at 1s, then jump far enough ahead to exceed CRAB_RUN_STALL_TIMEOUT_SECS (default 90s).
        let mut control = ScriptedControl::with_now(vec![1_000, 101_000]);

        run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        let updated = session_store
            .get_session(&session.id)
            .expect("session read should succeed")
            .expect("session should exist");
        assert_eq!(updated.lane_state, LaneState::Cancelling);
    }

    #[test]
    fn daemon_loop_honors_shutdown_signal_without_iterations() {
        let workspace = TempWorkspace::new("daemon", "shutdown");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: None,
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);
        control.shutdown = true;

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        assert_eq!(stats.iterations, 0);
    }

    #[test]
    fn daemon_loop_propagates_now_source_errors() {
        let workspace = TempWorkspace::new("daemon", "now-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: None,
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("missing now should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_now",
                message,
            } if message == "missing scripted now value"
        ));
    }

    #[test]
    fn daemon_loop_propagates_daemon_config_validation_errors() {
        let workspace = TempWorkspace::new("daemon", "validate-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 0,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("invalid daemon config should fail before loop starts");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_DAEMON_TICK_INTERVAL_MS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn daemon_loop_propagates_boot_runtime_errors() {
        let workspace = TempWorkspace::new("daemon", "boot-error");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        config.max_concurrent_lanes = 0;
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: None,
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("invalid workspace root should fail composition boot");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_MAX_CONCURRENT_LANES",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn daemon_loop_propagates_codex_backend_start_errors() {
        let workspace = TempWorkspace::new("daemon", "codex-start-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::with_spawn_error("forced codex spawn failure");
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("codex start failures should propagate");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_codex_spawn",
                message,
            } if message == "forced codex spawn failure"
        ));
    }

    #[test]
    fn daemon_loop_propagates_opencode_backend_start_errors() {
        let workspace = TempWorkspace::new("daemon", "opencode-start-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::with_spawn_error("forced opencode spawn failure");
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("opencode start failures should propagate");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_opencode_spawn",
                message,
            } if message == "forced opencode spawn failure"
        ));
    }

    #[test]
    fn daemon_loop_sleep_control_propagates_codex_backend_start_errors() {
        let workspace = TempWorkspace::new("daemon", "codex-start-error-sleep-control");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::with_spawn_error("forced codex spawn failure");
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = SleepFailControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("codex start failures should propagate for sleep-fail control");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_codex_spawn",
                message,
            } if message == "forced codex spawn failure"
        ));
    }

    #[test]
    fn daemon_loop_sleep_control_propagates_opencode_backend_start_errors() {
        let workspace = TempWorkspace::new("daemon", "opencode-start-error-sleep-control");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::with_spawn_error("forced opencode spawn failure");
        let mut control = SleepFailControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("opencode start failures should propagate for sleep-fail control");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_opencode_spawn",
                message,
            } if message == "forced opencode spawn failure"
        ));
    }

    #[test]
    fn daemon_loop_propagates_runtime_builder_errors() {
        let workspace = TempWorkspace::new("daemon", "runtime-builder-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport_and_runtime_builder(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
            runtime_builder_err,
        )
        .expect_err("runtime builder failures should propagate");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_runtime_builder",
                message,
            } if message == "forced runtime build failure"
        ));
    }

    #[test]
    fn daemon_loop_propagates_ingress_poll_errors() {
        let workspace = TempWorkspace::new("daemon", "ingress-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Err(CrabError::InvariantViolation {
                context: "daemon_test_ingress_poll",
                message: "forced ingress poll failure".to_string(),
            })]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("ingress poll failures should propagate");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_ingress_poll",
                message,
            } if message == "forced ingress poll failure"
        ));
    }

    #[test]
    fn daemon_loop_propagates_enqueue_errors() {
        let workspace = TempWorkspace::new("daemon", "enqueue-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(gateway_message("m-1", "111", "")))]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("enqueue failures should propagate");
        assert!(error.to_string().contains("must not be empty"));
    }

    #[test]
    fn daemon_loop_propagates_dispatch_errors() {
        let workspace = TempWorkspace::new("daemon", "dispatch-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(gateway_message("m-1", "111", "hello")))]),
            post_results: VecDeque::from([Err(CrabError::InvariantViolation {
                context: "daemon_test_post",
                message: "forced post failure".to_string(),
            })]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("dispatch failures should propagate");
        assert!(error.to_string().contains("forced post failure"));
    }

    #[test]
    fn daemon_loop_propagates_heartbeat_clock_errors() {
        let workspace = TempWorkspace::new("daemon", "heartbeat-clock-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("missing heartbeat clock value should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_now",
                message,
            } if message == "missing scripted now value"
        ));
    }

    #[test]
    fn daemon_loop_propagates_heartbeat_runtime_errors() {
        let workspace = TempWorkspace::new("daemon", "heartbeat-runtime-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000, 0]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("heartbeat runtime invariants should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "runtime_heartbeat_tick",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn daemon_loop_propagates_sleep_errors() {
        let workspace = TempWorkspace::new("daemon", "sleep-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = SleepFailControl::with_now(vec![1_000, 1_001]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("sleep failures should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_sleep",
                message: "forced sleep failure".to_string(),
            }
        );
    }

    #[test]
    fn daemon_loop_propagates_codex_stop_errors() {
        let workspace = TempWorkspace::new("daemon", "codex-stop-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::with_terminate_error("forced codex stop failure");
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000, 1_001]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("codex stop failures should propagate");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_codex_terminate",
                message,
            } if message == "forced codex stop failure"
        ));
    }

    #[test]
    fn daemon_loop_propagates_opencode_stop_errors() {
        let workspace = TempWorkspace::new("daemon", "opencode-stop-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let codex = TrackingCodexProcess::new();
        let opencode =
            TrackingOpenCodeProcess::with_terminate_error("forced opencode stop failure");
        let mut control = ScriptedControl::with_now(vec![1_000, 1_001]);

        let error = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect_err("opencode stop failures should propagate");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_opencode_terminate",
                message,
            } if message == "forced opencode stop failure"
        ));
    }

    #[test]
    fn daemon_runtime_ensure_physical_session_propagates_session_clock_errors() {
        let workspace = TempWorkspace::new("daemon", "session-clock-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let profile = InferenceProfile {
            backend: BackendKind::Codex,
            model: "auto".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        };

        DaemonTurnRuntime::<ScriptedDiscordIo>::set_session_now_epoch_ms_override(Some(
            fail_session_now_epoch_ms,
        ));
        let error = runtime
            .ensure_physical_session("discord:channel:777", &profile, None)
            .expect_err("session clock failures should surface");
        DaemonTurnRuntime::<ScriptedDiscordIo>::set_session_now_epoch_ms_override(None);

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_runtime_session_now",
                message: "forced session timestamp failure".to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_delivery_propagates_post_and_edit_errors() {
        let workspace = TempWorkspace::new("daemon", "delivery-errors");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let run = sample_run("non-owner");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            post_results: VecDeque::from([Err(CrabError::InvariantViolation {
                context: "daemon_test_post",
                message: "post failure".to_string(),
            })]),
            ..DiscordIoState::default()
        });
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let post_error = runtime
            .deliver_assistant_output(&run, "777", "source-msg-1", 0, "payload")
            .expect_err("post failures should surface");
        assert_eq!(
            post_error,
            CrabError::InvariantViolation {
                context: "daemon_test_post",
                message: "post failure".to_string(),
            }
        );

        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            edit_results: VecDeque::from([Err(CrabError::InvariantViolation {
                context: "daemon_test_edit",
                message: "edit failure".to_string(),
            })]),
            ..DiscordIoState::default()
        });
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        runtime
            .deliver_assistant_output(&run, "777", "source-msg-1", 0, "first")
            .expect("initial post should succeed");
        let edit_error = runtime
            .deliver_assistant_output(&run, "777", "source-msg-1", 1, "edit")
            .expect_err("edit failures should surface");
        assert_eq!(
            edit_error,
            CrabError::InvariantViolation {
                context: "daemon_test_edit",
                message: "edit failure".to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_owner_profile_resolution_can_use_owner_defaults() {
        let workspace = TempWorkspace::new("daemon", "owner-profile");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        config.owner.discord_user_ids = vec!["123".to_string()];
        config.owner.profile_defaults.backend = Some(crab_core::BackendKind::OpenCode);
        config.owner.profile_defaults.model = Some("owner-model".to_string());
        config.owner.profile_defaults.reasoning_level = Some(crab_core::ReasoningLevel::High);
        config.owner.machine_location = Some("Paris".to_string());
        config.owner.machine_timezone = Some("Europe/Paris".to_string());

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let telemetry = runtime
            .resolve_run_profile("discord:channel:777", "123", "hello")
            .expect("profile resolution should succeed");

        assert!(telemetry.sender_is_owner);
        assert_eq!(
            telemetry.resolved_profile.backend,
            crab_core::BackendKind::OpenCode
        );
        assert_eq!(telemetry.resolved_profile.model, "owner-model");
        assert_eq!(
            telemetry.resolved_profile.reasoning_level,
            crab_core::ReasoningLevel::High
        );
        assert!(telemetry.resolved_owner_profile.is_some());
    }

    #[test]
    fn daemon_runtime_owner_profile_resolution_omits_empty_owner_metadata() {
        let workspace = TempWorkspace::new("daemon", "owner-empty");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        config.owner.discord_user_ids = vec!["123".to_string()];
        config.owner.profile_defaults.backend = None;
        config.owner.profile_defaults.model = None;
        config.owner.profile_defaults.reasoning_level = None;
        config.owner.machine_location = None;
        config.owner.machine_timezone = None;

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let telemetry = runtime
            .resolve_run_profile("discord:channel:777", "123", "hello")
            .expect("profile resolution should succeed");
        assert!(telemetry.sender_is_owner);
        assert_eq!(telemetry.resolved_profile.backend, BackendKind::Codex);
        assert_eq!(telemetry.resolved_profile.model, "auto");
        assert_eq!(
            telemetry.resolved_profile.reasoning_level,
            ReasoningLevel::Medium
        );
        assert!(telemetry.resolved_owner_profile.is_none());
    }

    #[test]
    fn daemon_runtime_ensure_physical_session_reuses_active_session() {
        let workspace = TempWorkspace::new("daemon", "session-reuse");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let profile = InferenceProfile {
            backend: BackendKind::Codex,
            model: "auto".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        };

        let created = runtime
            .ensure_physical_session("discord:channel:777", &profile, None)
            .expect("initial session should be created");
        let reused = runtime
            .ensure_physical_session("discord:channel:777", &profile, Some(&created.id))
            .expect("existing session should be reused");

        assert_eq!(created, reused);
    }

    #[test]
    fn daemon_runtime_ensure_physical_session_uses_provided_id_when_missing_from_cache() {
        let workspace = TempWorkspace::new("daemon", "session-cache-miss");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let profile = InferenceProfile {
            backend: BackendKind::Codex,
            model: "auto".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        };

        let created = runtime
            .ensure_physical_session("discord:channel:777", &profile, Some("physical:missing:1"))
            .expect("missing active id should be created");
        assert_eq!(created.id, "physical:missing:1");
    }

    #[test]
    fn daemon_runtime_delivery_covers_post_and_edit_paths() {
        let workspace = TempWorkspace::new("daemon", "delivery");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let discord_state = discord.clone();
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        let run = sample_run("non-owner");

        runtime
            .deliver_assistant_output(&run, "777", "delivery-1", 0, "first payload")
            .expect("first post should succeed");
        runtime
            .deliver_assistant_output(&run, "777", "delivery-1", 1, "edited payload")
            .expect("edit should succeed");

        let state = discord_state.state();
        assert_eq!(state.posted.len(), 1);
        assert_eq!(state.edited.len(), 1);
        assert_eq!(
            state.posted[0],
            (
                "777".to_string(),
                "delivery-1".to_string(),
                "first payload".to_string()
            )
        );
        assert_eq!(
            state.edited[0],
            (
                "777".to_string(),
                "delivery-1".to_string(),
                "edited payload".to_string(),
            )
        );
    }

    #[test]
    fn system_daemon_loop_control_install_helper_covers_success_and_error_paths() {
        let installed = SystemDaemonLoopControl::install_with_handler(install_handler_ok)
            .expect("install helper should allow injected success");
        assert!(!installed.should_shutdown());

        let error = SystemDaemonLoopControl::install_with_handler(install_handler_err)
            .expect_err("install helper should surface injected failures");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_loop_signal_handler",
                message: "failed to install Ctrl-C handler: test".to_string(),
            }
        );
    }

    #[test]
    fn system_daemon_loop_control_runtime_methods_work() {
        let shutdown_flag = Arc::new(AtomicBool::new(false));
        let mut control = SystemDaemonLoopControl {
            shutdown_flag: Arc::clone(&shutdown_flag),
        };

        let _ = control.now_epoch_ms().expect("clock should read");
        control.sleep_tick(0).expect("zero sleep should be valid");
        assert!(!control.should_shutdown());

        shutdown_flag.store(true, Ordering::SeqCst);
        assert!(control.should_shutdown());
    }

    #[test]
    fn system_daemon_loop_control_install_executes_ctrlc_registration_path() {
        let first = SystemDaemonLoopControl::install().expect("first ctrlc install should succeed");
        #[cfg(unix)]
        {
            assert!(!first.should_shutdown());
            let mut observed = false;
            for attempt in 0..10 {
                if attempt == 1 {
                    let signal_status = Command::new("kill")
                        .args(["-s", "INT", &std::process::id().to_string()])
                        .status()
                        .expect("sending SIGINT to current process should succeed");
                    assert!(signal_status.success());
                }
                observed = first.should_shutdown();
                if observed {
                    break;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(observed);
        }

        let second =
            SystemDaemonLoopControl::install().expect_err("second ctrlc install should fail");
        assert!(matches!(
            second,
            CrabError::InvariantViolation {
                context: "daemon_loop_signal_handler",
                message,
            } if message.starts_with("failed to install Ctrl-C handler:")
        ));
    }

    #[test]
    fn epoch_conversion_helpers_cover_success_and_error_paths() {
        let success = epoch_ms_from_system_time(UNIX_EPOCH + Duration::from_millis(42))
            .expect("unix offset should convert");
        assert_eq!(success, 42);

        let before_epoch = epoch_ms_from_system_time(UNIX_EPOCH - Duration::from_millis(1))
            .expect_err("pre-epoch time should fail");
        assert!(matches!(
            before_epoch,
            CrabError::InvariantViolation {
                context: "daemon_clock_now",
                message,
            } if message.starts_with("system clock is before unix epoch:")
        ));

        let overflow = epoch_ms_from_duration(Duration::from_secs(u64::MAX))
            .expect_err("overflowing milliseconds should fail");
        assert_eq!(
            overflow,
            CrabError::InvariantViolation {
                context: "daemon_clock_now",
                message: "epoch milliseconds overflow u64".to_string(),
            }
        );
    }
}
