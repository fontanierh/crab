use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
#[cfg(test)]
use std::{cell::RefCell, thread_local};

use crab_backends::{
    map_opencode_inference_profile, normalize_opencode_events, BackendEvent, BackendEventKind,
    CodexAppServerProcess, OpenCodeRawEvent, OpenCodeReasoningMode, OpenCodeServerProcess,
    OpenCodeSessionConfig, OpenCodeTokenUsage, OpenCodeTurnConfig, OpenCodeTurnState,
};
#[cfg(not(coverage))]
use crab_core::{build_context_diagnostics_report, render_context_diagnostics_fixture};
use crab_core::{
    compile_prompt_contract, process_workspace_git_push_queue, render_budgeted_turn_context,
    resolve_inference_profile, resolve_scoped_memory_snippets, resolve_sender_identity,
    resolve_sender_trust_context, BackendKind, ContextAssemblyInput, ContextBudgetPolicy,
    CrabError, CrabResult, InferenceProfile, InferenceProfileResolutionInput, MemoryCitationMode,
    OwnerConfig, PromptContractInput, ReasoningLevel, Run, RunProfileTelemetry, RuntimeConfig,
    ScopedMemorySnippetResolverInput, SenderConversationKind, SenderIdentityInput, TrustSurface,
    AGENTS_FILE_NAME, IDENTITY_FILE_NAME, MEMORY_FILE_NAME, OWNER_MEMORY_SCOPE_DIRECTORY,
    SOUL_FILE_NAME, USER_FILE_NAME,
};
use crab_discord::GatewayMessage;
use crab_store::CheckpointStore;

use crate::{boot_runtime_with_processes, run_heartbeat_if_due, TurnExecutor, TurnExecutorRuntime};

pub const DEFAULT_DAEMON_TICK_INTERVAL_MS: u64 = 250;
const DAEMON_TURN_CONTEXT_READ: &str = "daemon_turn_context_read";
const DAEMON_BACKEND_BRIDGE_EXECUTE: &str = "daemon_backend_bridge_execute";
const DAEMON_BACKEND_BRIDGE_CONTEXT: &str = "daemon_backend_bridge";
const MILLIS_PER_DAY: u64 = 86_400_000;
const OPENCODE_SESSION_PLACEHOLDER_PREFIX: &str = "backend-session:";

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

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpenCodeBridgeTurnResult {
    turn_id: String,
    raw_events: Vec<OpenCodeRawEvent>,
}

trait OpenCodeBridgeRuntime: Send + Sync + std::fmt::Debug {
    fn create_session(
        &self,
        server_base_url: &str,
        config: OpenCodeSessionConfig,
    ) -> CrabResult<String>;

    fn execute_turn(
        &self,
        server_base_url: &str,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<OpenCodeBridgeTurnResult>;
}

#[derive(Debug, Default)]
struct DeterministicOpenCodeBridgeRuntime;

impl OpenCodeBridgeRuntime for DeterministicOpenCodeBridgeRuntime {
    fn create_session(
        &self,
        server_base_url: &str,
        _config: OpenCodeSessionConfig,
    ) -> CrabResult<String> {
        let fingerprint = server_base_url.bytes().fold(0_u64, |acc, byte| {
            acc.wrapping_mul(131).wrapping_add(u64::from(byte))
        });
        Ok(format!("opencode-session-{fingerprint}"))
    }

    fn execute_turn(
        &self,
        _server_base_url: &str,
        session_id: &str,
        _prompt: &str,
        _config: OpenCodeTurnConfig,
    ) -> CrabResult<OpenCodeBridgeTurnResult> {
        let turn_id = format!("opencode-turn-{session_id}");
        Ok(OpenCodeBridgeTurnResult {
            turn_id: turn_id.clone(),
            raw_events: vec![
                OpenCodeRawEvent::AssistantDelta {
                    sequence: 1,
                    text: "OpenCode backend response".to_string(),
                },
                OpenCodeRawEvent::TurnFinished {
                    sequence: 2,
                    turn_id,
                    state: OpenCodeTurnState::Completed,
                    message: Some("completed".to_string()),
                    usage: Some(OpenCodeTokenUsage {
                        input_tokens: 3,
                        output_tokens: 4,
                        total_tokens: 7,
                    }),
                },
            ],
        })
    }
}

#[derive(Debug)]
struct OpenCodeExecutionBridge {
    server_base_url: String,
    runtime: Box<dyn OpenCodeBridgeRuntime>,
    reasoning_mode: OpenCodeReasoningMode,
}

impl OpenCodeExecutionBridge {
    #[cfg(test)]
    fn new(server_base_url: String, runtime: Box<dyn OpenCodeBridgeRuntime>) -> CrabResult<Self> {
        if server_base_url.trim().is_empty() {
            return Err(CrabError::InvariantViolation {
                context: DAEMON_BACKEND_BRIDGE_CONTEXT,
                message: "opencode server_base_url must not be empty".to_string(),
            });
        }
        Ok(Self {
            server_base_url,
            runtime,
            reasoning_mode: OpenCodeReasoningMode::BestEffort,
        })
    }

    fn execute_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        let mapping =
            map_opencode_inference_profile(&run.profile.resolved_profile, self.reasoning_mode);
        let prompt = build_opencode_prompt(turn_context, mapping.guidance_note.as_deref());
        if should_materialize_opencode_session(physical_session) {
            let base_url = &self.server_base_url;
            let session_config = mapping.session_config.clone();
            physical_session.backend_session_id =
                self.runtime.create_session(base_url, session_config)?;
        }

        let send_result = self.runtime.execute_turn(
            &self.server_base_url,
            &physical_session.backend_session_id,
            &prompt,
            mapping.turn_config.clone(),
        );
        let turn_result = match send_result {
            Ok(result) => result,
            Err(error) if should_retry_opencode_session_recovery(&error) => {
                physical_session.backend_session_id = self
                    .runtime
                    .create_session(&self.server_base_url, mapping.session_config)?;
                let base_url = &self.server_base_url;
                let session_id = &physical_session.backend_session_id;
                self.runtime
                    .execute_turn(base_url, session_id, &prompt, mapping.turn_config)?
            }
            Err(error) => return Err(error),
        };

        physical_session.last_turn_id = Some(turn_result.turn_id);
        normalize_opencode_events(&turn_result.raw_events)
    }
}

fn should_materialize_opencode_session(physical_session: &crab_core::PhysicalSession) -> bool {
    physical_session
        .backend_session_id
        .starts_with(OPENCODE_SESSION_PLACEHOLDER_PREFIX)
}

fn should_retry_opencode_session_recovery(error: &CrabError) -> bool {
    match error {
        CrabError::InvariantViolation { context, .. } => {
            context.starts_with("opencode_") || context.starts_with("daemon_opencode_")
        }
        CrabError::Io { context, .. } => *context == "daemon_opencode_transport",
        _ => false,
    }
}

fn build_opencode_prompt(turn_context: &str, guidance_note: Option<&str>) -> String {
    match guidance_note {
        Some(note) if !note.trim().is_empty() => {
            let trimmed_note = note.trim();
            format!("{turn_context}\n\n[opencode_reasoning_guidance]\n{trimmed_note}")
        }
        _ => turn_context.to_string(),
    }
}

fn build_daemon_stub_backend_events(run: &Run) -> Vec<BackendEvent> {
    let response = run.user_input.trim().to_string();
    let input_tokens = u64::try_from(run.user_input.split_whitespace().count())
        .unwrap_or(1)
        .max(1);
    let output_tokens = u64::try_from(response.split_whitespace().count())
        .unwrap_or(1)
        .max(1);
    let total_tokens = input_tokens.saturating_add(output_tokens);

    vec![
        BackendEvent {
            sequence: 1,
            kind: BackendEventKind::TextDelta,
            payload: BTreeMap::from([("text".to_string(), response)]),
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
                (
                    "run_usage_source".to_string(),
                    "daemon_backend_bridge".to_string(),
                ),
            ]),
        },
        BackendEvent {
            sequence: 3,
            kind: BackendEventKind::TurnCompleted,
            payload: BTreeMap::from([("finish".to_string(), "completed".to_string())]),
        },
    ]
}

#[derive(Debug, Default)]
struct DaemonBackendBridge {
    opencode: Option<OpenCodeExecutionBridge>,
}

impl DaemonBackendBridge {
    fn has_opencode_backend_bridge(&self) -> bool {
        self.opencode.is_some()
    }

    #[cfg(test)]
    fn configure_opencode_backend_bridge(
        &mut self,
        server_base_url: String,
        runtime: Box<dyn OpenCodeBridgeRuntime>,
    ) -> CrabResult<()> {
        self.opencode = Some(OpenCodeExecutionBridge::new(server_base_url, runtime)?);
        Ok(())
    }

    fn configure_opencode_backend_bridge_trusted(
        &mut self,
        server_base_url: String,
        runtime: Box<dyn OpenCodeBridgeRuntime>,
    ) {
        debug_assert!(
            !server_base_url.trim().is_empty(),
            "opencode server_base_url must not be empty"
        );
        self.opencode = Some(OpenCodeExecutionBridge {
            server_base_url,
            runtime,
            reasoning_mode: OpenCodeReasoningMode::BestEffort,
        });
    }

    fn execute_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        if run.profile.resolved_profile.backend == BackendKind::OpenCode {
            let Some(opencode_bridge) = self.opencode.as_mut() else {
                return Err(CrabError::InvariantViolation {
                    context: DAEMON_BACKEND_BRIDGE_CONTEXT,
                    message: "opencode backend bridge is not configured".to_string(),
                });
            };
            return opencode_bridge.execute_turn(physical_session, run, turn_context);
        }
        Ok(build_daemon_stub_backend_events(run))
    }
}

impl DaemonBackendExecutionBridge for DaemonBackendBridge {
    fn execute_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        _turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        DaemonBackendBridge::execute_turn(self, physical_session, run, turn_context)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
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
    backend_bridge: Box<dyn DaemonBackendExecutionBridge>,
    next_session_sequence: u64,
    physical_sessions: BTreeMap<String, crab_core::PhysicalSession>,
    turn_context_runtime: Option<TurnContextRuntimeState>,
}

#[derive(Debug, Clone)]
struct TurnContextRuntimeState {
    workspace_root: PathBuf,
    checkpoint_store: CheckpointStore,
    context_budget_policy: ContextBudgetPolicy,
}

trait DaemonBackendExecutionBridge: std::fmt::Debug {
    fn execute_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
}

#[cfg(test)]
type SessionNowEpochMsOverride = fn() -> CrabResult<u64>;

#[cfg(test)]
thread_local! {
    static SESSION_NOW_EPOCH_MS_OVERRIDE: RefCell<Option<SessionNowEpochMsOverride>> = RefCell::new(None);
}

impl<D: DaemonDiscordIo> DaemonTurnRuntime<D> {
    pub fn new(owner: OwnerConfig, discord: D) -> CrabResult<Self> {
        Self::new_with_backend_bridge(owner, discord, Box::<DaemonBackendBridge>::default())
    }

    fn new_with_backend_bridge(
        owner: OwnerConfig,
        discord: D,
        backend_bridge: Box<dyn DaemonBackendExecutionBridge>,
    ) -> CrabResult<Self> {
        Ok(Self {
            discord,
            owner,
            backend_bridge,
            next_session_sequence: 0,
            physical_sessions: BTreeMap::new(),
            turn_context_runtime: None,
        })
    }

    fn configure_turn_context_runtime(
        &mut self,
        workspace_root: PathBuf,
        checkpoint_store: CheckpointStore,
    ) {
        self.turn_context_runtime = Some(TurnContextRuntimeState {
            workspace_root,
            checkpoint_store,
            context_budget_policy: ContextBudgetPolicy::default(),
        });
    }

    fn has_opencode_backend_bridge(&self) -> bool {
        self.backend_bridge
            .as_any()
            .downcast_ref::<DaemonBackendBridge>()
            .is_some_and(DaemonBackendBridge::has_opencode_backend_bridge)
    }

    #[cfg(test)]
    fn configure_opencode_backend_bridge(
        &mut self,
        server_base_url: String,
        runtime: Box<dyn OpenCodeBridgeRuntime>,
    ) -> CrabResult<()> {
        let Some(backend_bridge) = self
            .backend_bridge
            .as_any_mut()
            .downcast_mut::<DaemonBackendBridge>()
        else {
            return Err(CrabError::InvariantViolation {
                context: DAEMON_BACKEND_BRIDGE_CONTEXT,
                message: "runtime backend bridge does not support opencode test configuration"
                    .to_string(),
            });
        };
        backend_bridge.configure_opencode_backend_bridge(server_base_url, runtime)
    }

    fn configure_opencode_backend_bridge_trusted(
        &mut self,
        server_base_url: String,
        runtime: Box<dyn OpenCodeBridgeRuntime>,
    ) {
        if let Some(backend_bridge) = self
            .backend_bridge
            .as_any_mut()
            .downcast_mut::<DaemonBackendBridge>()
        {
            backend_bridge.configure_opencode_backend_bridge_trusted(server_base_url, runtime);
        }
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

    fn build_runtime_turn_context(
        &mut self,
        run: &Run,
        logical_session: &crab_core::LogicalSession,
    ) -> CrabResult<String> {
        let Some(runtime) = self.turn_context_runtime.clone() else {
            return Ok(run.user_input.clone());
        };

        let reference_date = epoch_ms_to_yyyy_mm_dd(self.now_epoch_ms()?)?;
        let memory_scope_directory = memory_scope_directory_for_run(run);
        let memory_snippets =
            resolve_scoped_memory_snippets(&ScopedMemorySnippetResolverInput::with_defaults(
                &runtime.workspace_root,
                &memory_scope_directory,
                true,
                &reference_date,
            ))?;

        let memory_recall_surface = trust_surface_for_logical_session_id(&run.logical_session_id);
        let prompt_contract = compile_prompt_contract(&PromptContractInput {
            backend: run.profile.resolved_profile.backend,
            model: run.profile.resolved_profile.model.clone(),
            reasoning_level: run.profile.resolved_profile.reasoning_level,
            sender_id: run.profile.sender_id.clone(),
            sender_is_owner: run.profile.sender_is_owner,
            owner_profile: run.profile.resolved_owner_profile.clone(),
            memory_tools_enabled: true,
            memory_citation_mode: MemoryCitationMode::Auto,
            memory_recall_surface,
        })?;

        let agents_document = read_workspace_markdown(&runtime.workspace_root, AGENTS_FILE_NAME)?;
        let checkpoint_summary =
            load_latest_checkpoint_summary(logical_session, &runtime.checkpoint_store)?;
        let context_input = ContextAssemblyInput {
            soul_document: read_workspace_markdown(&runtime.workspace_root, SOUL_FILE_NAME)?,
            identity_document: read_workspace_markdown(
                &runtime.workspace_root,
                IDENTITY_FILE_NAME,
            )?,
            agents_document: render_agents_with_prompt_contract(&agents_document, &prompt_contract),
            user_document: read_workspace_markdown(&runtime.workspace_root, USER_FILE_NAME)?,
            memory_document: read_workspace_markdown(&runtime.workspace_root, MEMORY_FILE_NAME)?,
            memory_snippets,
            latest_checkpoint_summary: checkpoint_summary,
            turn_input: run.user_input.clone(),
        };
        let budgeted =
            render_budgeted_turn_context(&context_input, &runtime.context_budget_policy)?;

        #[cfg(not(coverage))]
        {
            let report = build_context_diagnostics_report(&budgeted);
            let fixture = render_context_diagnostics_fixture(&report);
            tracing::debug!(
                logical_session_id = %run.logical_session_id,
                run_id = %run.id,
                context_diagnostics = %fixture,
                "rendered turn context"
            );
        }

        Ok(budgeted.rendered_context)
    }
}

impl<D: DaemonDiscordIo> TurnExecutorRuntime for DaemonTurnRuntime<D> {
    fn now_epoch_ms(&mut self) -> CrabResult<u64> {
        now_epoch_ms()
    }

    fn resolve_run_profile(
        &mut self,
        logical_session_id: &str,
        author_id: &str,
        _user_input: &str,
    ) -> CrabResult<RunProfileTelemetry> {
        let sender = resolve_sender_identity(
            &SenderIdentityInput {
                conversation_kind: conversation_kind_for_logical_session_id(logical_session_id),
                discord_user_id: author_id.to_string(),
                username: None,
            },
            &self.owner,
        )?;
        let trust_context = resolve_sender_trust_context(&sender, &self.owner)?;
        let global_default = InferenceProfile {
            backend: if trust_context.sender_is_owner {
                self.owner
                    .profile_defaults
                    .backend
                    .unwrap_or(BackendKind::Codex)
            } else {
                BackendKind::Codex
            },
            model: if trust_context.sender_is_owner {
                self.owner
                    .profile_defaults
                    .model
                    .clone()
                    .unwrap_or_else(|| "auto".to_string())
            } else {
                "auto".to_string()
            },
            reasoning_level: if trust_context.sender_is_owner {
                self.owner
                    .profile_defaults
                    .reasoning_level
                    .unwrap_or(ReasoningLevel::Medium)
            } else {
                ReasoningLevel::Medium
            },
        };
        let resolved = resolve_inference_profile(&InferenceProfileResolutionInput {
            turn_override: None,
            session_profile: None,
            channel_override: None,
            backend_defaults: Default::default(),
            global_default,
        })?;

        Ok(RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: resolved.profile,
            backend_source: resolved.backend_source,
            model_source: resolved.model_source,
            reasoning_level_source: resolved.reasoning_level_source,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: trust_context.sender_id,
            sender_is_owner: trust_context.sender_is_owner,
            resolved_owner_profile: trust_context.owner_profile,
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
        logical_session: &crab_core::LogicalSession,
        _physical_session: &crab_core::PhysicalSession,
    ) -> CrabResult<String> {
        self.build_runtime_turn_context(run, logical_session)
    }

    fn execute_backend_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        let cache_key = physical_session.id.clone();
        let backend_events = self
            .backend_bridge
            .execute_turn(physical_session, run, turn_id, turn_context)
            .map_err(|error| CrabError::InvariantViolation {
                context: DAEMON_BACKEND_BRIDGE_EXECUTE,
                message: format!(
                    "run {} turn {} backend {:?} bridge execution failed: {}",
                    run.id, turn_id, run.profile.resolved_profile.backend, error
                ),
            })?;
        physical_session.last_turn_id = Some(turn_id.to_string());
        self.physical_sessions
            .insert(cache_key, physical_session.clone());
        Ok(backend_events)
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

fn run_daemon_loop_with_transport_and_runtime_builder<CP, OP, D, C, RB>(
    runtime_config: &RuntimeConfig,
    daemon_config: &DaemonConfig,
    codex_process: CP,
    opencode_process: OP,
    discord: D,
    control: &mut C,
    runtime_builder: RB,
) -> CrabResult<DaemonLoopStats>
where
    CP: CodexAppServerProcess,
    OP: OpenCodeServerProcess,
    D: DaemonDiscordIo,
    C: DaemonLoopControl + ?Sized,
    RB: FnOnce(OwnerConfig, D) -> CrabResult<DaemonTurnRuntime<D>>,
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
    #[cfg(not(coverage))]
    {
        let migration = &boot.composition.state_schema_migration;
        tracing::info!(
            starting_version = migration.starting_version,
            target_version = migration.target_version,
            migrated = migration.migrated,
            "state schema migration evaluated on startup"
        );
        for event in &migration.events {
            tracing::debug!(
                kind = event.kind.as_token(),
                from_version = event.from_version,
                to_version = event.to_version,
                detail = ?event.detail,
                "state schema migration event"
            );
        }
    }
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
    let opencode_handle = boot.composition.backends.opencode.ensure_running()?;

    let mut heartbeat_loop_state = boot.heartbeat_loop_state;
    let mut runtime = runtime_builder(runtime_config.owner.clone(), discord)?;
    runtime.configure_turn_context_runtime(
        boot.composition.startup.workspace_root.clone(),
        boot.composition.state_stores.checkpoint_store.clone(),
    );
    if !runtime.has_opencode_backend_bridge() {
        let bridge_url = opencode_handle.server_base_url.clone();
        let bridge_runtime = Box::new(DeterministicOpenCodeBridgeRuntime);
        runtime.configure_opencode_backend_bridge_trusted(bridge_url, bridge_runtime);
    }
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
            let enqueued = executor.enqueue_gateway_message(message)?.is_some();
            stats.ingested_messages = stats.ingested_messages.saturating_add(u64::from(enqueued));
        }

        while executor.dispatch_next_run()?.is_some() {
            stats.dispatched_runs = stats.dispatched_runs.saturating_add(1);
        }

        let now_epoch_ms = control.now_epoch_ms()?;
        let push_outcome = process_workspace_git_push_queue(
            &executor.composition().startup.workspace_root,
            &executor.composition().state_stores.root,
            &executor.composition().workspace_git,
            now_epoch_ms,
        );
        #[cfg(not(coverage))]
        match push_outcome {
            Ok(outcome) => {
                if outcome.attempted && !outcome.pushed {
                    tracing::warn!(
                        commit_key = ?outcome.commit_key,
                        exhausted = outcome.exhausted,
                        failure = ?outcome.failure,
                        failure_kind = ?outcome.failure_kind,
                        recovery_commands = ?outcome.recovery_commands,
                        next_due_at_epoch_ms = ?outcome.next_due_at_epoch_ms,
                        next_backoff_ms = ?outcome.next_backoff_ms,
                        "workspace git push attempt failed"
                    );
                } else if outcome.attempted && outcome.pushed {
                    tracing::info!(
                        commit_key = ?outcome.commit_key,
                        queue_depth = outcome.queue_depth,
                        "workspace git push succeeded"
                    );
                }
            }
            Err(_error) => {
                tracing::warn!(error = %_error, "workspace git push queue processing failed");
            }
        }
        #[cfg(coverage)]
        let _ = push_outcome;

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
                #[cfg(not(coverage))]
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

fn render_agents_with_prompt_contract(agents_document: &str, prompt_contract: &str) -> String {
    let normalized_agents = agents_document.trim();
    if normalized_agents.is_empty() {
        return prompt_contract.to_string();
    }

    format!("{normalized_agents}\n\n{prompt_contract}")
}

fn read_workspace_markdown(workspace_root: &Path, file_name: &str) -> CrabResult<String> {
    let path = workspace_root.join(file_name);
    match fs::read_to_string(&path) {
        Ok(contents) => Ok(contents),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(error) => Err(CrabError::Io {
            context: DAEMON_TURN_CONTEXT_READ,
            path: Some(path.to_string_lossy().into_owned()),
            message: error.to_string(),
        }),
    }
}

fn load_latest_checkpoint_summary(
    logical_session: &crab_core::LogicalSession,
    checkpoint_store: &CheckpointStore,
) -> CrabResult<Option<String>> {
    if let Some(checkpoint_id) = logical_session.last_successful_checkpoint_id.as_deref() {
        if let Some(checkpoint) =
            checkpoint_store.get_checkpoint(&logical_session.id, checkpoint_id)?
        {
            return Ok(Some(render_checkpoint_summary(&checkpoint)));
        }
    }

    Ok(checkpoint_store
        .latest_checkpoint(&logical_session.id)?
        .map(|checkpoint| render_checkpoint_summary(&checkpoint)))
}

fn render_checkpoint_summary(checkpoint: &crab_core::Checkpoint) -> String {
    format!(
        "checkpoint_id: {}\nrun_id: {}\ncreated_at_epoch_ms: {}\nsummary:\n{}",
        checkpoint.id, checkpoint.run_id, checkpoint.created_at_epoch_ms, checkpoint.summary
    )
}

fn memory_scope_directory_for_run(run: &Run) -> String {
    if run.profile.sender_is_owner {
        return OWNER_MEMORY_SCOPE_DIRECTORY.to_string();
    }

    run.profile.sender_id.clone()
}

fn conversation_kind_for_logical_session_id(logical_session_id: &str) -> SenderConversationKind {
    if logical_session_id.starts_with("discord:dm:") {
        return SenderConversationKind::DirectMessage;
    }
    if logical_session_id.starts_with("discord:thread:") {
        return SenderConversationKind::Thread;
    }
    SenderConversationKind::GuildChannel
}

fn trust_surface_for_logical_session_id(logical_session_id: &str) -> TrustSurface {
    if logical_session_id.starts_with("discord:dm:") {
        return TrustSurface::DirectMessage;
    }
    TrustSurface::SharedDiscord
}

fn epoch_ms_to_yyyy_mm_dd(epoch_ms: u64) -> CrabResult<String> {
    let days_i64 = (epoch_ms / MILLIS_PER_DAY) as i64;
    let (year, month, day) = civil_from_days(days_i64);
    Ok(format!("{year:04}-{month:02}-{day:02}"))
}

fn civil_from_days(days_since_unix_epoch: i64) -> (i64, u64, u64) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - (day_of_era / 1_460) + (day_of_era / 36_524) - (day_of_era / 146_096)) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    if month <= 2 {
        year += 1;
    }
    (year, month as u64, day as u64)
}

#[cfg(test)]
mod tests {
    use super::{
        conversation_kind_for_logical_session_id, epoch_ms_from_duration,
        epoch_ms_from_system_time, epoch_ms_to_yyyy_mm_dd, load_latest_checkpoint_summary,
        memory_scope_directory_for_run, read_workspace_markdown,
        render_agents_with_prompt_contract, run_daemon_loop_with_transport,
        run_daemon_loop_with_transport_and_runtime_builder, trust_surface_for_logical_session_id,
        DaemonBackendExecutionBridge, DaemonConfig, DaemonDiscordIo, DaemonLoopControl,
        DaemonLoopStats, DaemonTurnRuntime, OpenCodeBridgeRuntime, OpenCodeBridgeTurnResult,
        SystemDaemonLoopControl,
    };
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};
    use crate::TurnExecutorRuntime;
    use crab_backends::{
        BackendEvent, BackendEventKind, CodexAppServerProcess, CodexProcessHandle,
        OpenCodeRawEvent, OpenCodeServerHandle, OpenCodeServerProcess, OpenCodeSessionConfig,
        OpenCodeTokenUsage, OpenCodeTurnConfig, OpenCodeTurnState,
    };
    use crab_core::{
        BackendKind, Checkpoint, CrabError, CrabResult, InferenceProfile, LaneState,
        LogicalSession, OwnerConfig, ProfileValueSource, ReasoningLevel, Run, RunProfileTelemetry,
        RunStatus, SenderConversationKind, TokenAccounting, TrustSurface,
    };
    use crab_discord::{GatewayConversationKind, GatewayMessage};
    use crab_store::{CheckpointStore, EventStore, RunStore, SessionStore};
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
        server_base_url: String,
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
                server_base_url: "http://127.0.0.1:4210".to_string(),
            }
        }

        fn with_spawn_error(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingOpenCodeState::default())),
                spawn_error: Some(message),
                terminate_error: None,
                server_base_url: "http://127.0.0.1:4210".to_string(),
            }
        }

        fn with_terminate_error(message: &'static str) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingOpenCodeState::default())),
                spawn_error: None,
                terminate_error: Some(message),
                server_base_url: "http://127.0.0.1:4210".to_string(),
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
                server_base_url: self.server_base_url.clone(),
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

    #[derive(Debug, Clone, Default)]
    struct ScriptedBackendBridgeState {
        execute_calls: Vec<(String, String, String, String)>,
        execute_turn_results: VecDeque<CrabResult<Vec<BackendEvent>>>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedBackendBridge {
        state: Arc<Mutex<ScriptedBackendBridgeState>>,
    }

    impl ScriptedBackendBridge {
        fn with_results(results: Vec<CrabResult<Vec<BackendEvent>>>) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedBackendBridgeState {
                    execute_turn_results: VecDeque::from(results),
                    ..ScriptedBackendBridgeState::default()
                })),
            }
        }

        fn state(&self) -> ScriptedBackendBridgeState {
            self.state.lock().expect("lock should succeed").clone()
        }
    }

    impl DaemonBackendExecutionBridge for ScriptedBackendBridge {
        fn execute_turn(
            &mut self,
            physical_session: &mut crab_core::PhysicalSession,
            run: &Run,
            turn_id: &str,
            turn_context: &str,
        ) -> CrabResult<Vec<BackendEvent>> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.execute_calls.push((
                physical_session.id.clone(),
                run.id.clone(),
                turn_id.to_string(),
                turn_context.to_string(),
            ));
            state.execute_turn_results.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "daemon_test_backend_bridge",
                    message: "missing scripted execute result".to_string(),
                })
            })
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }
    }

    #[derive(Debug, Default)]
    struct MutatingBackendBridge;

    impl DaemonBackendExecutionBridge for MutatingBackendBridge {
        fn execute_turn(
            &mut self,
            physical_session: &mut crab_core::PhysicalSession,
            _run: &Run,
            turn_id: &str,
            _turn_context: &str,
        ) -> CrabResult<Vec<BackendEvent>> {
            physical_session.backend_session_id = format!("backend-session:mutated:{turn_id}");
            Ok(vec![BackendEvent {
                sequence: 1,
                kind: BackendEventKind::TurnCompleted,
                payload: std::collections::BTreeMap::from([(
                    "finish".to_string(),
                    "done".to_string(),
                )]),
            }])
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
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

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct ScriptedOpenCodeBridgeStats {
        create_calls: usize,
        execute_calls: usize,
        seen_session_ids: Vec<String>,
        seen_prompts: Vec<String>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedOpenCodeBridgeRuntime {
        state: Arc<Mutex<ScriptedOpenCodeBridgeState>>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedOpenCodeBridgeState {
        create_results: VecDeque<CrabResult<String>>,
        execute_results: VecDeque<CrabResult<OpenCodeBridgeTurnResult>>,
        stats: ScriptedOpenCodeBridgeStats,
    }

    impl ScriptedOpenCodeBridgeRuntime {
        fn with_results(
            create_results: Vec<CrabResult<String>>,
            execute_results: Vec<CrabResult<OpenCodeBridgeTurnResult>>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedOpenCodeBridgeState {
                    create_results: VecDeque::from(create_results),
                    execute_results: VecDeque::from(execute_results),
                    stats: ScriptedOpenCodeBridgeStats::default(),
                })),
            }
        }

        fn stats(&self) -> ScriptedOpenCodeBridgeStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }
    }

    impl OpenCodeBridgeRuntime for ScriptedOpenCodeBridgeRuntime {
        fn create_session(
            &self,
            _server_base_url: &str,
            _config: OpenCodeSessionConfig,
        ) -> CrabResult<String> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.create_calls += 1;
            state
                .create_results
                .pop_front()
                .unwrap_or(Err(CrabError::InvariantViolation {
                    context: "daemon_test_opencode_bridge_create",
                    message: "missing scripted create_session result".to_string(),
                }))
        }

        fn execute_turn(
            &self,
            _server_base_url: &str,
            session_id: &str,
            prompt: &str,
            _config: OpenCodeTurnConfig,
        ) -> CrabResult<OpenCodeBridgeTurnResult> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.execute_calls += 1;
            state.stats.seen_session_ids.push(session_id.to_string());
            state.stats.seen_prompts.push(prompt.to_string());
            state
                .execute_results
                .pop_front()
                .unwrap_or(Err(CrabError::InvariantViolation {
                    context: "daemon_test_opencode_bridge_execute",
                    message: "missing scripted execute_turn result".to_string(),
                }))
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

    fn scripted_opencode_turn_result(
        turn_id: &str,
        text: &str,
        usage: (u64, u64, u64),
        state: OpenCodeTurnState,
    ) -> OpenCodeBridgeTurnResult {
        OpenCodeBridgeTurnResult {
            turn_id: turn_id.to_string(),
            raw_events: vec![
                OpenCodeRawEvent::AssistantDelta {
                    sequence: 1,
                    text: text.to_string(),
                },
                OpenCodeRawEvent::TurnFinished {
                    sequence: 2,
                    turn_id: turn_id.to_string(),
                    state,
                    message: Some("done".to_string()),
                    usage: Some(OpenCodeTokenUsage {
                        input_tokens: usage.0,
                        output_tokens: usage.1,
                        total_tokens: usage.2,
                    }),
                },
            ],
        }
    }

    #[test]
    fn helper_should_retry_opencode_session_recovery_covers_contexts() {
        assert!(super::should_retry_opencode_session_recovery(
            &CrabError::InvariantViolation {
                context: "opencode_execute_turn",
                message: "recover".to_string(),
            }
        ));
        assert!(super::should_retry_opencode_session_recovery(
            &CrabError::InvariantViolation {
                context: "daemon_opencode_bridge",
                message: "recover".to_string(),
            }
        ));
        assert!(super::should_retry_opencode_session_recovery(
            &CrabError::Io {
                context: "daemon_opencode_transport",
                path: None,
                message: "retry".to_string(),
            }
        ));
        assert!(!super::should_retry_opencode_session_recovery(
            &CrabError::InvariantViolation {
                context: "daemon_codex_bridge",
                message: "do not recover".to_string(),
            }
        ));
        assert!(!super::should_retry_opencode_session_recovery(
            &CrabError::InvalidConfig {
                key: "CRAB_TEST",
                value: "bad".to_string(),
                reason: "test",
            }
        ));
    }

    #[test]
    fn helper_build_opencode_prompt_covers_guidance_and_fallback_paths() {
        let turn_context = "primary context";
        assert_eq!(
            super::build_opencode_prompt(turn_context, None),
            turn_context.to_string()
        );
        assert_eq!(
            super::build_opencode_prompt(turn_context, Some("   ")),
            turn_context.to_string()
        );
        assert_eq!(
            super::build_opencode_prompt(turn_context, Some("  use high effort  ")),
            "primary context\n\n[opencode_reasoning_guidance]\nuse high effort".to_string()
        );
    }

    #[test]
    fn opencode_execution_bridge_rejects_blank_server_base_url() {
        let runtime = ScriptedOpenCodeBridgeRuntime::with_results(Vec::new(), Vec::new());
        let error = super::OpenCodeExecutionBridge::new("   ".to_string(), Box::new(runtime))
            .expect_err("blank opencode server URL should fail bridge construction");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: super::DAEMON_BACKEND_BRIDGE_CONTEXT,
                message: "opencode server_base_url must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn daemon_backend_bridge_rejects_opencode_turn_without_configured_bridge() {
        let mut bridge = super::DaemonBackendBridge::default();
        let mut run = sample_run("111");
        run.profile.resolved_profile.backend = BackendKind::OpenCode;
        let mut physical_session = crab_core::PhysicalSession {
            id: "physical-opencode-1".to_string(),
            logical_session_id: run.logical_session_id.clone(),
            backend: BackendKind::OpenCode,
            backend_session_id: "backend-session:physical-opencode-1".to_string(),
            created_at_epoch_ms: 1,
            last_turn_id: None,
        };

        let error = bridge
            .execute_turn(&mut physical_session, &run, "turn context")
            .expect_err("missing opencode bridge should be explicit");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: super::DAEMON_BACKEND_BRIDGE_CONTEXT,
                message: "opencode backend bridge is not configured".to_string(),
            }
        );
    }

    #[test]
    fn opencode_execution_bridge_materializes_and_recovers_after_transport_error() {
        let bridge_runtime = ScriptedOpenCodeBridgeRuntime::with_results(
            vec![
                Ok("opencode-session-initial".to_string()),
                Ok("opencode-session-recovered".to_string()),
            ],
            vec![
                Err(CrabError::Io {
                    context: "daemon_opencode_transport",
                    path: None,
                    message: "session not found".to_string(),
                }),
                Ok(scripted_opencode_turn_result(
                    "opencode-turn-1",
                    "Recovered bridge output",
                    (5, 6, 11),
                    OpenCodeTurnState::Completed,
                )),
            ],
        );
        let bridge_state = bridge_runtime.clone();
        let mut bridge = super::OpenCodeExecutionBridge::new(
            "http://127.0.0.1:4210".to_string(),
            Box::new(bridge_runtime),
        )
        .expect("bridge should construct with valid URL");

        let mut run = sample_run("111");
        run.profile.resolved_profile.backend = BackendKind::OpenCode;
        run.profile.resolved_profile.reasoning_level = ReasoningLevel::High;
        let mut physical_session = crab_core::PhysicalSession {
            id: "physical-opencode-2".to_string(),
            logical_session_id: run.logical_session_id.clone(),
            backend: BackendKind::OpenCode,
            backend_session_id: "backend-session:physical-opencode-2".to_string(),
            created_at_epoch_ms: 1,
            last_turn_id: None,
        };

        let events = bridge
            .execute_turn(&mut physical_session, &run, "runtime turn context")
            .expect("recoverable transport errors should retry and recover");
        let bridge_stats = bridge_state.stats();
        assert_eq!(bridge_stats.create_calls, 2);
        assert_eq!(bridge_stats.execute_calls, 2);
        assert_eq!(
            bridge_stats.seen_session_ids,
            vec![
                "opencode-session-initial".to_string(),
                "opencode-session-recovered".to_string(),
            ]
        );
        assert!(
            bridge_stats.seen_prompts[0].contains("[opencode_reasoning_guidance]"),
            "bridge prompt should include mapped guidance"
        );
        assert_eq!(
            physical_session.backend_session_id,
            "opencode-session-recovered".to_string()
        );
        assert_eq!(
            physical_session.last_turn_id,
            Some("opencode-turn-1".to_string())
        );
        assert!(events.iter().any(|event| {
            event.payload.get("input_tokens") == Some(&"5".to_string())
                && event.payload.get("output_tokens") == Some(&"6".to_string())
                && event.payload.get("total_tokens") == Some(&"11".to_string())
        }));
    }

    #[test]
    fn opencode_execution_bridge_uses_existing_materialized_session_without_recreating() {
        let bridge_runtime = ScriptedOpenCodeBridgeRuntime::with_results(
            Vec::new(),
            vec![Ok(scripted_opencode_turn_result(
                "opencode-turn-existing",
                "Existing session output",
                (2, 3, 5),
                OpenCodeTurnState::Completed,
            ))],
        );
        let bridge_state = bridge_runtime.clone();
        let mut bridge = super::OpenCodeExecutionBridge::new(
            "http://127.0.0.1:4210".to_string(),
            Box::new(bridge_runtime),
        )
        .expect("bridge should construct with valid URL");

        let mut run = sample_run("111");
        run.profile.resolved_profile.backend = BackendKind::OpenCode;
        let mut physical_session = crab_core::PhysicalSession {
            id: "physical-opencode-existing".to_string(),
            logical_session_id: run.logical_session_id.clone(),
            backend: BackendKind::OpenCode,
            backend_session_id: "opencode-session-existing".to_string(),
            created_at_epoch_ms: 1,
            last_turn_id: None,
        };

        bridge
            .execute_turn(&mut physical_session, &run, "runtime turn context")
            .expect("existing opencode sessions should not force recreation");
        let bridge_stats = bridge_state.stats();
        assert_eq!(bridge_stats.create_calls, 0);
        assert_eq!(bridge_stats.execute_calls, 1);
        assert_eq!(
            bridge_stats.seen_session_ids,
            vec!["opencode-session-existing".to_string()]
        );
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
        let mut control = ScriptedControl::with_now(vec![
            2_000_000_000_000,
            2_000_000_000_001,
            2_000_000_000_002,
        ]);

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
        assert_eq!(discord.posted[0].2, "hello world".to_string());

        let codex_stats = codex_state.stats();
        assert_eq!(codex_stats.spawn_calls, 1);
        assert_eq!(codex_stats.terminate_calls, 1);

        let opencode_stats = opencode_state.stats();
        assert_eq!(opencode_stats.spawn_calls, 1);
        assert_eq!(opencode_stats.terminate_calls, 1);
        assert_eq!(control.slept, vec![5, 5]);
    }

    #[test]
    fn daemon_loop_opencode_default_bridge_uses_backend_events_for_output_and_usage() {
        let workspace = TempWorkspace::new("daemon", "opencode-default-bridge");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.owner.discord_user_ids = vec!["111".to_string()];
        config.owner.profile_defaults.backend = Some(BackendKind::OpenCode);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(2),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([
                Ok(Some(gateway_message("m-op-1", "111", "hello"))),
                Ok(None),
            ]),
            ..DiscordIoState::default()
        });
        let discord_state = discord.clone();
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![10_000, 10_001, 10_002]);

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed for opencode bridge path");
        assert_eq!(stats.dispatched_runs, 1);

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert_eq!(discord.posted[0].2, "OpenCode backend response".to_string());
        assert!(!discord.posted[0].2.contains("Crab stub response"));

        let state_root = workspace.path.join("state");
        let session = SessionStore::new(&state_root)
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.token_accounting.input_tokens, 3);
        assert_eq!(session.token_accounting.output_tokens, 4);
        assert_eq!(session.token_accounting.total_tokens, 7);

        let run_ids = RunStore::new(&state_root)
            .list_run_ids("discord:channel:777")
            .expect("run ids should list");
        assert_eq!(run_ids.len(), 1);
        let events = EventStore::new(&state_root)
            .replay_run("discord:channel:777", &run_ids[0])
            .expect("run replay should succeed");
        assert!(
            events
                .iter()
                .all(|event| event.payload.get("run_usage_source")
                    != Some(&"daemon_stub".to_string()))
        );
        assert!(events.iter().any(|event| {
            event.payload.get("input_tokens") == Some(&"3".to_string())
                && event.payload.get("output_tokens") == Some(&"4".to_string())
                && event.payload.get("total_tokens") == Some(&"7".to_string())
        }));
    }

    #[test]
    fn daemon_loop_opencode_bridge_propagates_non_recoverable_errors() {
        let workspace = TempWorkspace::new("daemon", "opencode-bridge-fatal");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.owner.discord_user_ids = vec!["111".to_string()];
        config.owner.profile_defaults.backend = Some(BackendKind::OpenCode);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(2),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([
                Ok(Some(gateway_message("m-op-err", "111", "hello"))),
                Ok(None),
            ]),
            ..DiscordIoState::default()
        });
        let bridge_runtime = ScriptedOpenCodeBridgeRuntime::with_results(
            vec![Ok("session-op-1".to_string())],
            vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_non_recoverable",
                message: "forced fatal execute error".to_string(),
            })],
        );
        let bridge_state = bridge_runtime.clone();
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![20_000, 20_001, 20_002]);

        let error = run_daemon_loop_with_transport_and_runtime_builder(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
            move |owner, discord| {
                let mut runtime = DaemonTurnRuntime::new(owner, discord)?;
                let bridge_url = "http://127.0.0.1:4210".to_string();
                let bridge = Box::new(bridge_runtime.clone());
                runtime.configure_opencode_backend_bridge(bridge_url, bridge)?;
                Ok(runtime)
            },
        )
        .expect_err("non-recoverable opencode bridge errors should surface");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_backend_bridge_execute",
                message,
            } if message.contains(
                "daemon_test_non_recoverable invariant violation: forced fatal execute error"
            )
        ));
        let bridge_stats = bridge_state.stats();
        assert_eq!(bridge_stats.create_calls, 1);
        assert_eq!(bridge_stats.execute_calls, 1);
    }

    #[test]
    fn daemon_loop_opencode_bridge_recovers_with_new_session_after_recoverable_errors() {
        let workspace = TempWorkspace::new("daemon", "opencode-bridge-recovery");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.owner.discord_user_ids = vec!["111".to_string()];
        config.owner.profile_defaults.backend = Some(BackendKind::OpenCode);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(2),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([
                Ok(Some(gateway_message("m-op-recover", "111", "hello"))),
                Ok(None),
            ]),
            ..DiscordIoState::default()
        });
        let discord_state = discord.clone();
        let bridge_runtime = ScriptedOpenCodeBridgeRuntime::with_results(
            vec![
                Ok("session-op-old".to_string()),
                Ok("session-op-new".to_string()),
            ],
            vec![
                Err(CrabError::InvariantViolation {
                    context: "opencode_send_prompt_response",
                    message: "session not found".to_string(),
                }),
                Ok(scripted_opencode_turn_result(
                    "turn-op-recovered",
                    "Recovered OpenCode response",
                    (8, 5, 13),
                    OpenCodeTurnState::Completed,
                )),
            ],
        );
        let bridge_state = bridge_runtime.clone();
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![30_000, 30_001, 30_002]);

        let stats = run_daemon_loop_with_transport_and_runtime_builder(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
            move |owner, discord| {
                let mut runtime = DaemonTurnRuntime::new(owner, discord)?;
                let bridge_url = "http://127.0.0.1:4210".to_string();
                let bridge = Box::new(bridge_runtime.clone());
                runtime.configure_opencode_backend_bridge(bridge_url, bridge)?;
                Ok(runtime)
            },
        )
        .expect("recoverable opencode bridge failures should recover");
        assert_eq!(stats.dispatched_runs, 1);

        let bridge_stats = bridge_state.stats();
        assert_eq!(bridge_stats.create_calls, 2);
        assert_eq!(bridge_stats.execute_calls, 2);
        assert_eq!(
            bridge_stats.seen_session_ids,
            vec!["session-op-old".to_string(), "session-op-new".to_string()]
        );

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert_eq!(
            discord.posted[0].2,
            "Recovered OpenCode response".to_string()
        );

        let state_root = workspace.path.join("state");
        let session = SessionStore::new(&state_root)
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.token_accounting.total_tokens, 13);
    }

    #[test]
    fn daemon_loop_keeps_dispatching_when_workspace_git_push_fails() {
        let workspace = TempWorkspace::new("daemon", "workspace-git-push-failure");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.workspace_git.enabled = true;
        config.workspace_git.push_policy = crab_core::WorkspaceGitPushPolicy::OnCommit;
        config.workspace_git.remote = Some(
            workspace
                .path
                .join("missing-remote.git")
                .to_string_lossy()
                .to_string(),
        );

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
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![1_000, 1_001, 1_002]);

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should keep running despite push failure");
        assert_eq!(stats.dispatched_runs, 1);

        let queue_path = workspace.path.join("state/workspace_git_push_queue.json");
        let queue = std::fs::read_to_string(queue_path).expect("push queue should persist");
        let parsed: serde_json::Value =
            serde_json::from_str(&queue).expect("queue json should parse");
        let entries = parsed["entries"]
            .as_array()
            .expect("entries should be an array");
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0]["commit_key"]
                .as_str()
                .expect("commit_key should be present"),
            "discord:channel:777|run:discord:channel:777:m-1|run_finalized|none"
        );
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
    fn daemon_runtime_execute_backend_turn_delegates_success_path() {
        let workspace = TempWorkspace::new("daemon", "backend-bridge-success");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let run = sample_run("non-owner");
        let expected_events = vec![
            BackendEvent {
                sequence: 1,
                kind: BackendEventKind::TextDelta,
                payload: std::collections::BTreeMap::from([(
                    "text".to_string(),
                    "delegated output".to_string(),
                )]),
            },
            BackendEvent {
                sequence: 2,
                kind: BackendEventKind::TurnCompleted,
                payload: std::collections::BTreeMap::from([(
                    "finish".to_string(),
                    "done".to_string(),
                )]),
            },
        ];

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let bridge = ScriptedBackendBridge::with_results(vec![Ok(expected_events.clone())]);
        let bridge_state = bridge.clone();
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::new(bridge),
        )
        .expect("runtime builds");

        let active_id = "physical:discord:channel:777:1".to_string();
        let mut physical = runtime
            .ensure_physical_session(&run.logical_session_id, &run.profile.resolved_profile, None)
            .expect("physical session should resolve");
        assert_eq!(physical.id, active_id);

        let events = runtime
            .execute_backend_turn(&mut physical, &run, "turn-1", "turn context")
            .expect("delegated backend turn should succeed");
        assert_eq!(events, expected_events);
        assert_eq!(physical.last_turn_id, Some("turn-1".to_string()));
        let cached = runtime
            .ensure_physical_session(
                &run.logical_session_id,
                &run.profile.resolved_profile,
                Some(&active_id),
            )
            .expect("cached physical session should resolve");
        assert_eq!(cached.last_turn_id, Some("turn-1".to_string()));

        let bridge_state = bridge_state.state();
        assert_eq!(
            bridge_state.execute_calls,
            vec![(
                physical.id,
                run.id,
                "turn-1".to_string(),
                "turn context".to_string(),
            )]
        );
    }

    #[test]
    fn daemon_runtime_execute_backend_turn_maps_bridge_failures() {
        let workspace = TempWorkspace::new("daemon", "backend-bridge-failure");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let run = sample_run("non-owner");
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let bridge =
            ScriptedBackendBridge::with_results(vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_backend_bridge",
                message: "forced backend bridge failure".to_string(),
            })]);
        let bridge_state = bridge.clone();
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::new(bridge),
        )
        .expect("runtime builds");
        let mut physical = runtime
            .ensure_physical_session(&run.logical_session_id, &run.profile.resolved_profile, None)
            .expect("physical session should resolve");

        let error = runtime
            .execute_backend_turn(&mut physical, &run, "turn-2", "turn context")
            .expect_err("delegated backend failures should be mapped");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_backend_bridge_execute",
                message,
            } if message
                .contains("run run-1 turn turn-2 backend Codex bridge execution failed")
                && message.contains(
                    "daemon_test_backend_bridge invariant violation: forced backend bridge failure"
            )
        ));

        let bridge_state = bridge_state.state();
        assert_eq!(bridge_state.execute_calls.len(), 1);
        assert_eq!(physical.last_turn_id, None);
    }

    #[test]
    fn daemon_runtime_execute_backend_turn_surfaces_scripted_bridge_depletion() {
        let workspace = TempWorkspace::new("daemon", "backend-bridge-depletion");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let run = sample_run("non-owner");
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::new(ScriptedBackendBridge::with_results(Vec::new())),
        )
        .expect("runtime builds");
        let mut physical = runtime
            .ensure_physical_session(&run.logical_session_id, &run.profile.resolved_profile, None)
            .expect("physical session should resolve");

        let error = runtime
            .execute_backend_turn(&mut physical, &run, "turn-3", "turn context")
            .expect_err("depleted scripted bridge results should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_backend_bridge_execute",
                message,
            } if message.contains(
                "daemon_test_backend_bridge invariant violation: missing scripted execute result"
            )
        ));
        assert_eq!(physical.last_turn_id, None);
    }

    #[test]
    fn daemon_runtime_execute_backend_turn_persists_bridge_mutations_in_cache() {
        let workspace = TempWorkspace::new("daemon", "backend-bridge-cache-persist");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let run = sample_run("non-owner");
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::<MutatingBackendBridge>::default(),
        )
        .expect("runtime builds");
        let mut physical = runtime
            .ensure_physical_session(&run.logical_session_id, &run.profile.resolved_profile, None)
            .expect("physical session should resolve");
        let active_id = physical.id.clone();
        let initial_backend_session_id = physical.backend_session_id.clone();

        runtime
            .execute_backend_turn(&mut physical, &run, "turn-4", "turn context")
            .expect("mutating bridge turn should succeed");
        assert_ne!(physical.backend_session_id, initial_backend_session_id);
        assert_eq!(physical.last_turn_id, Some("turn-4".to_string()));

        let cached = runtime
            .ensure_physical_session(
                &run.logical_session_id,
                &run.profile.resolved_profile,
                Some(&active_id),
            )
            .expect("cached physical session should resolve");
        assert_eq!(cached.backend_session_id, physical.backend_session_id);
        assert_eq!(cached.last_turn_id, physical.last_turn_id);
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
    fn daemon_runtime_resolve_run_profile_rejects_non_numeric_author_id() {
        let workspace = TempWorkspace::new("daemon", "owner-bad-author");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");

        let error = runtime
            .resolve_run_profile("discord:channel:777", "author-x", "hello")
            .expect_err("non-numeric author ids should fail sender identity validation");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "sender_identity_resolve",
                message: "discord_user_id must contain only digits".to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_build_turn_context_falls_back_when_not_configured() {
        let workspace = TempWorkspace::new("daemon", "context-fallback");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");

        let run = sample_run("123");
        let session = sample_session(LaneState::Idle, None);
        let physical = runtime
            .ensure_physical_session(
                &run.logical_session_id,
                &run.profile.resolved_profile,
                session.active_physical_session_id.as_deref(),
            )
            .expect("physical session should resolve");
        let context = runtime
            .build_turn_context(&run, &session, &physical)
            .expect("context build should succeed");
        assert_eq!(context, run.user_input);
    }

    #[test]
    fn daemon_runtime_build_turn_context_includes_workspace_memory_and_checkpoint() {
        let workspace = TempWorkspace::new("daemon", "context-wiring");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        std::fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        std::fs::create_dir_all(workspace.path.join("memory/users/123"))
            .expect("memory user scope should be creatable");
        std::fs::write(workspace.path.join("SOUL.md"), "Soul profile")
            .expect("SOUL.md should be writable");
        std::fs::write(workspace.path.join("IDENTITY.md"), "Identity profile")
            .expect("IDENTITY.md should be writable");
        std::fs::write(workspace.path.join("AGENTS.md"), "Agent operating rules")
            .expect("AGENTS.md should be writable");
        std::fs::write(workspace.path.join("USER.md"), "Owner profile")
            .expect("USER.md should be writable");
        std::fs::write(workspace.path.join("MEMORY.md"), "Curated memory")
            .expect("MEMORY.md should be writable");
        std::fs::write(
            workspace.path.join("memory/users/123/2026-02-10.md"),
            "User memory entry",
        )
        .expect("user memory file should be writable");

        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        let checkpoint_store = CheckpointStore::new(&state_root);

        let run = sample_run("123");
        checkpoint_store
            .put_checkpoint(&Checkpoint {
                id: "ckpt-1".to_string(),
                logical_session_id: run.logical_session_id.clone(),
                run_id: run.id.clone(),
                created_at_epoch_ms: 1_739_173_200_000,
                summary: "Checkpoint summary".to_string(),
                memory_digest: "digest".to_string(),
                state: std::collections::BTreeMap::new(),
            })
            .expect("checkpoint should persist");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        runtime.configure_turn_context_runtime(workspace.path.clone(), checkpoint_store);

        let mut session = sample_session(LaneState::Idle, None);
        session.last_successful_checkpoint_id = Some("ckpt-1".to_string());
        let physical = runtime
            .ensure_physical_session(
                &run.logical_session_id,
                &run.profile.resolved_profile,
                session.active_physical_session_id.as_deref(),
            )
            .expect("physical session should resolve");
        let context = runtime
            .build_turn_context(&run, &session, &physical)
            .expect("context should render");

        assert!(context.contains("## SOUL.md"));
        assert!(context.contains("Soul profile"));
        assert!(context.contains("## AGENTS.md"));
        assert!(context.contains("## RUNTIME_PROFILE"));
        assert!(context.contains("memory/users/123/2026-02-10.md"));
        assert!(context.contains("checkpoint_id: ckpt-1"));
        assert!(context.contains("## TURN_INPUT"));
        assert!(context.contains("hello world"));
    }

    #[test]
    fn daemon_runtime_build_turn_context_propagates_identity_read_errors() {
        let workspace = TempWorkspace::new("daemon", "context-wiring-identity-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        std::fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        std::fs::create_dir_all(workspace.path.join("memory/users/123"))
            .expect("memory user scope should be creatable");
        std::fs::write(workspace.path.join("SOUL.md"), "Soul profile")
            .expect("SOUL.md should be writable");
        std::fs::create_dir_all(workspace.path.join("IDENTITY.md"))
            .expect("IDENTITY.md directory should be creatable");
        std::fs::write(workspace.path.join("AGENTS.md"), "Agent operating rules")
            .expect("AGENTS.md should be writable");
        std::fs::write(workspace.path.join("USER.md"), "Owner profile")
            .expect("USER.md should be writable");
        std::fs::write(workspace.path.join("MEMORY.md"), "Curated memory")
            .expect("MEMORY.md should be writable");
        std::fs::write(
            workspace.path.join("memory/users/123/2026-02-10.md"),
            "User memory entry",
        )
        .expect("user memory file should be writable");

        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        let checkpoint_store = CheckpointStore::new(&state_root);

        let run = sample_run("123");
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        runtime.configure_turn_context_runtime(workspace.path.clone(), checkpoint_store);

        let session = sample_session(LaneState::Idle, None);
        let physical = runtime
            .ensure_physical_session(
                &run.logical_session_id,
                &run.profile.resolved_profile,
                session.active_physical_session_id.as_deref(),
            )
            .expect("physical session should resolve");
        let error = runtime
            .build_turn_context(&run, &session, &physical)
            .expect_err("IDENTITY.md read failures should propagate");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "daemon_turn_context_read",
                ..
            }
        ));
    }

    #[test]
    fn helper_render_agents_with_prompt_contract_covers_empty_and_non_empty_inputs() {
        assert_eq!(
            render_agents_with_prompt_contract("   ", "prompt contract"),
            "prompt contract".to_string()
        );
        assert_eq!(
            render_agents_with_prompt_contract("agents", "prompt contract"),
            "agents\n\nprompt contract".to_string()
        );
    }

    #[test]
    fn helper_read_workspace_markdown_covers_missing_and_io_error_paths() {
        let workspace = TempWorkspace::new("daemon", "read-workspace-markdown");
        std::fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");

        let missing = read_workspace_markdown(&workspace.path, "MISSING.md")
            .expect("missing files should be treated as empty");
        assert!(missing.is_empty());

        std::fs::create_dir_all(workspace.path.join("BROKEN.md"))
            .expect("directory should be creatable");
        let error = read_workspace_markdown(&workspace.path, "BROKEN.md")
            .expect_err("directory path should surface io error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "daemon_turn_context_read",
                ..
            }
        ));
    }

    #[test]
    fn helper_load_latest_checkpoint_summary_falls_back_when_last_id_is_missing() {
        let workspace = TempWorkspace::new("daemon", "checkpoint-fallback-summary");
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        let checkpoint_store = CheckpointStore::new(&state_root);

        checkpoint_store
            .put_checkpoint(&Checkpoint {
                id: "ckpt-latest".to_string(),
                logical_session_id: "discord:channel:777".to_string(),
                run_id: "run-1".to_string(),
                created_at_epoch_ms: 1_739_173_200_000,
                summary: "Latest checkpoint".to_string(),
                memory_digest: "digest".to_string(),
                state: std::collections::BTreeMap::new(),
            })
            .expect("checkpoint should persist");

        let mut session = sample_session(LaneState::Idle, None);
        session.last_successful_checkpoint_id = Some("ckpt-missing".to_string());
        let summary = load_latest_checkpoint_summary(&session, &checkpoint_store)
            .expect("summary lookup should succeed")
            .expect("latest summary should be available");
        assert!(summary.contains("checkpoint_id: ckpt-latest"));
    }

    #[test]
    fn helper_scope_and_surface_resolution_cover_owner_dm_and_thread_paths() {
        let mut owner_run = sample_run("123");
        owner_run.profile.sender_is_owner = true;
        assert_eq!(
            memory_scope_directory_for_run(&owner_run),
            "owner".to_string()
        );

        assert_eq!(
            conversation_kind_for_logical_session_id("discord:dm:123"),
            SenderConversationKind::DirectMessage
        );
        assert_eq!(
            conversation_kind_for_logical_session_id("discord:thread:999"),
            SenderConversationKind::Thread
        );
        assert_eq!(
            trust_surface_for_logical_session_id("discord:dm:123"),
            TrustSurface::DirectMessage
        );
    }

    #[test]
    fn helper_epoch_date_conversion_supports_large_inputs_deterministically() {
        let max_date = epoch_ms_to_yyyy_mm_dd(u64::MAX)
            .expect("maximum epoch milliseconds should still map to a calendar day");
        assert_eq!(max_date, "584556019-04-03".to_string());
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
