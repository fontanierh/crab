use crab_backends::{
    claude::{ClaudeRawEvent, ClaudeRawEventStream},
    BackendHarness, ClaudeBackend, ClaudeProcess, SessionContext, TurnInput,
};
#[cfg(not(any(test, coverage)))]
use crab_backends::{map_claude_inference_profile, ClaudeThinkingMode};
#[cfg(not(coverage))]
use crab_core::{build_context_diagnostics_report, render_context_diagnostics_fixture};
use crab_core::{
    compile_prompt_contract, detect_workspace_bootstrap_state, read_self_work_session,
    render_budgeted_turn_context, resolve_inference_profile, resolve_scoped_memory_snippets,
    resolve_sender_identity, resolve_sender_trust_context, write_pending_trigger,
    write_self_work_session_atomically, BackendKind, ContextAssemblyInput, ContextBudgetPolicy,
    CrabError, CrabResult, InferenceProfile, InferenceProfileResolutionInput, LaneState,
    MemoryCitationMode, OwnerConfig, PromptContractInput, ReasoningLevel, Run, RunProfileTelemetry,
    RuntimeConfig, ScopedMemorySnippetResolverInput, SelfWorkSession, SelfWorkSessionLock,
    SelfWorkSessionStatus, SenderConversationKind, SenderIdentityInput, TrustSurface,
    WorkspaceBootstrapState, IDENTITY_FILE_NAME, MEMORY_FILE_NAME, OWNER_MEMORY_SCOPE_DIRECTORY,
    SOUL_FILE_NAME, USER_FILE_NAME,
};
use crab_discord::GatewayMessage;
use crab_store::CheckpointStore;
#[cfg(not(any(test, coverage)))]
use futures::channel::mpsc;
use futures::{executor::block_on, stream};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::self_work_cli::{build_expiry_trigger_message, build_wake_trigger_message};
use crate::{
    run_heartbeat_if_due, SelfWorkLaneStatus, TriggerKind, TurnExecutor, TurnExecutorRuntime,
};

pub const DEFAULT_DAEMON_TICK_INTERVAL_MS: u64 = 250;
const DAEMON_TURN_CONTEXT_READ: &str = "daemon_turn_context_read";
const CRABD_INSTANCE_LOCK_CONTEXT: &str = "crabd_instance_lock";
const CRABD_INSTANCE_LOCK_FILE_NAME: &str = "crabd.lock";
const CRABD_STATE_DIRECTORY_NAME: &str = "state";
#[cfg(not(any(test, coverage)))]
const DAEMON_CLAUDE_TRANSPORT_CONTEXT: &str = "daemon_claude_transport";
#[cfg(any(test, not(coverage)))]
const DAEMON_CLAUDE_STREAM_CONTEXT: &str = "daemon_claude_stream";
const MILLIS_PER_DAY: u64 = 86_400_000;
const DAEMON_CLAUDE_FORCE_SEND_ERROR_TOKEN: &str = "force-claude-send-error";
const CRAB_RUNTIME_BRIEF_BASE: &str = "You are an AI coding agent running inside Crab.\n\
\n\
Crab is a harness that sits between Discord and you:\n\
- Discord users send messages.\n\
- Crab decides when to run you and builds one prompt that includes:\n\
  - system context (identity, curated memory, runtime notes)\n\
  - the latest user message\n\
- Your normal assistant text will be delivered back to Discord by Crab.\n\
\n\
Continuity:\n\
- Crab keeps this backend session alive across Discord turns.\n\
- When your context gets large, use the rotate-session skill to checkpoint and rotate.\n\
- CRAB_STATE_DIR is set in your environment.\n\
\n\
Workspace + memory:\n\
- Use only Crab-managed workspace files for long-term context:\n\
  - SOUL.md, IDENTITY.md, USER.md, MEMORY.md, and the memory/ directory.\n\
- During onboarding, ask the owner for missing facts; do not infer identity from unrelated\n\
  directories or other harnesses (for example `.openclaw/`). Treat onboarding as a blank slate\n\
  unless Crab-managed files explicitly say otherwise.\n\
\n\
Discord message formatting:\n\
- Crab splits your output into separate Discord messages at blank lines.\n\
- Always place a blank line between distinct thoughts, answers, or actions.\n\
- Each blank-line-separated section becomes its own Discord message.\n\
\n\
Self-trigger:\n\
- You can schedule yourself to revisit a channel later using the crab-trigger command.\n\
- Example: sleep 1800 && crab-trigger --state-dir \"$CRAB_STATE_DIR\" --channel <channel_id> --message \"Check on deployment\" &\n\
- The command fires immediately; use sleep/cron/at for delays.\n\
- CRAB_STATE_DIR is set in your environment.\n\
\n\
Operating constraints:\n\
- Keep responses actionable and concise.\n\
- Respect owner/operator commands and current session policy.";
#[cfg(all(not(any(test, coverage)), debug_assertions))]
const DAEMON_DETERMINISTIC_CLAUDE_PROCESS_ENV: &str =
    "CRAB_DAEMON_FORCE_DETERMINISTIC_CLAUDE_PROCESS";

#[derive(Debug, Clone, Default)]
pub struct DaemonClaudeProcess {
    #[cfg(not(any(test, coverage)))]
    state: Arc<std::sync::Mutex<DaemonClaudeProcessState>>,
}

#[derive(Debug)]
struct DaemonInstanceLock {
    file: File,
}

impl DaemonInstanceLock {
    fn acquire(runtime_config: &RuntimeConfig) -> CrabResult<Self> {
        let startup = crate::initialize_runtime_startup(runtime_config)?;
        let state_root = startup.workspace_root.join(CRABD_STATE_DIRECTORY_NAME);
        Self::acquire_at(&state_root)
    }

    fn acquire_at(state_root: &Path) -> CrabResult<Self> {
        fs::create_dir_all(state_root).map_err(|error| CrabError::Io {
            context: CRABD_INSTANCE_LOCK_CONTEXT,
            path: Some(state_root.to_string_lossy().to_string()),
            message: error.to_string(),
        })?;
        let lock_path = state_root.join(CRABD_INSTANCE_LOCK_FILE_NAME);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|error| CrabError::Io {
                context: CRABD_INSTANCE_LOCK_CONTEXT,
                path: Some(lock_path.to_string_lossy().to_string()),
                message: error.to_string(),
            })?;
        acquire_lock_on_file(&file, &lock_path)?;
        Ok(Self { file })
    }
}

impl Drop for DaemonInstanceLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;

            let _ = unsafe { libc::flock(self.file.as_raw_fd(), libc::LOCK_UN) };
        }
        #[cfg(not(unix))]
        {
            let _ = &self.file;
        }
    }
}

#[cfg(unix)]
fn acquire_lock_on_file(file: &File, lock_path: &Path) -> CrabResult<()> {
    use std::os::fd::AsRawFd;

    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }

    let error = std::io::Error::last_os_error();
    let message = if error.kind() == std::io::ErrorKind::WouldBlock {
        "another crabd instance is already running for this state directory".to_string()
    } else {
        error.to_string()
    };
    Err(CrabError::Io {
        context: CRABD_INSTANCE_LOCK_CONTEXT,
        path: Some(lock_path.to_string_lossy().to_string()),
        message,
    })
}

#[cfg(not(unix))]
fn acquire_lock_on_file(_file: &File, _lock_path: &Path) -> CrabResult<()> {
    Ok(())
}

#[cfg(not(any(test, coverage)))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ClaudeSessionExecutionConfig {
    model: Option<String>,
    effort: Option<String>,
    initialized: bool,
}

#[cfg(not(any(test, coverage)))]
impl Default for ClaudeSessionExecutionConfig {
    fn default() -> Self {
        Self {
            model: None,
            effort: None,
            initialized: true,
        }
    }
}

#[cfg(test)]
fn parse_claude_stream_lines(stdout: &str) -> CrabResult<Vec<ClaudeRawEvent>> {
    let mut events = Vec::new();
    for line in stdout.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        events.extend(parse_claude_stream_line(trimmed)?);
    }
    if events.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: DAEMON_CLAUDE_STREAM_CONTEXT,
            message: "claude stream produced no assistant/result events".to_string(),
        });
    }
    if !events
        .iter()
        .any(|event| matches!(event, ClaudeRawEvent::TurnCompleted { .. }))
    {
        events.push(ClaudeRawEvent::TurnCompleted {
            stop_reason: "completed".to_string(),
        });
    }
    Ok(events)
}

#[cfg(any(test, not(coverage)))]
fn parse_claude_stream_line(line: &str) -> CrabResult<Vec<ClaudeRawEvent>> {
    let payload: serde_json::Value =
        serde_json::from_str(line).map_err(|error| CrabError::Serialization {
            context: DAEMON_CLAUDE_STREAM_CONTEXT,
            path: None,
            message: format!("invalid stream-json event: {error}"),
        })?;
    let mut events = Vec::new();
    append_claude_events_from_stream_value(&payload, &mut events)?;
    Ok(events)
}

#[cfg(any(test, not(coverage)))]
fn append_claude_events_from_stream_value(
    payload: &serde_json::Value,
    events: &mut Vec<ClaudeRawEvent>,
) -> CrabResult<()> {
    let Some(event_type) = payload.get("type").and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    match event_type {
        "assistant" => {
            append_claude_assistant_events(payload, events)?;
            Ok(())
        }
        "result" => {
            append_claude_result_events(payload, events);
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(any(test, not(coverage)))]
fn append_claude_assistant_events(
    payload: &serde_json::Value,
    events: &mut Vec<ClaudeRawEvent>,
) -> CrabResult<()> {
    let message = payload
        .get("message")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| CrabError::InvariantViolation {
            context: DAEMON_CLAUDE_STREAM_CONTEXT,
            message: "assistant stream event is missing message payload".to_string(),
        })?;
    if let Some(content_items) = message.get("content").and_then(serde_json::Value::as_array) {
        for content_item in content_items {
            append_claude_content_item_event(content_item, events)?;
        }
    }
    if let Some(usage_payload) = message.get("usage") {
        if let Some(usage) = parse_claude_usage_payload(usage_payload) {
            events.push(ClaudeRawEvent::Usage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                total_tokens: usage.total_tokens,
                cache_read_input_tokens: usage.cache_read_input_tokens,
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
            });
        }
    }
    Ok(())
}

#[cfg(any(test, not(coverage)))]
fn append_claude_content_item_event(
    content_item: &serde_json::Value,
    events: &mut Vec<ClaudeRawEvent>,
) -> CrabResult<()> {
    let Some(content_type) = content_item.get("type").and_then(serde_json::Value::as_str) else {
        return Ok(());
    };
    match content_type {
        "text" => {
            if let Some(text) = value_as_non_empty_string(content_item.get("text")) {
                events.push(ClaudeRawEvent::TextDelta { text });
            }
        }
        "tool_use" => {
            let tool_call_id = value_as_non_empty_string(content_item.get("id"));
            let tool_name = value_as_non_empty_string(content_item.get("name"));
            if let (Some(tool_call_id), Some(tool_name)) = (tool_call_id, tool_name) {
                let input_json = content_item
                    .get("input")
                    .cloned()
                    .unwrap_or(serde_json::Value::Object(serde_json::Map::new()))
                    .to_string();
                events.push(ClaudeRawEvent::ToolCall {
                    tool_call_id,
                    tool_name,
                    input_json,
                });
            }
        }
        "tool_result" => {
            let tool_call_id = value_as_non_empty_string(
                content_item
                    .get("tool_use_id")
                    .or_else(|| content_item.get("id")),
            );
            let tool_name = value_as_non_empty_string(
                content_item
                    .get("name")
                    .or_else(|| content_item.get("tool_name")),
            )
            .unwrap_or_else(|| "tool".to_string());
            if let Some(tool_call_id) = tool_call_id {
                let output = value_as_non_empty_string(content_item.get("content"))
                    .or_else(|| value_as_non_empty_string(content_item.get("text")))
                    .unwrap_or_default();
                let is_error = content_item
                    .get("is_error")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                events.push(ClaudeRawEvent::ToolResult {
                    tool_call_id,
                    tool_name,
                    output,
                    is_error,
                });
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(any(test, not(coverage)))]
fn append_claude_result_events(payload: &serde_json::Value, events: &mut Vec<ClaudeRawEvent>) {
    if let Some(usage_payload) = payload.get("usage") {
        if let Some(usage) = parse_claude_usage_payload(usage_payload) {
            events.push(ClaudeRawEvent::Usage {
                input_tokens: usage.input_tokens,
                output_tokens: usage.output_tokens,
                total_tokens: usage.total_tokens,
                cache_read_input_tokens: usage.cache_read_input_tokens,
                cache_creation_input_tokens: usage.cache_creation_input_tokens,
            });
        }
    }
    let subtype = payload
        .get("subtype")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("success")
        .to_ascii_lowercase();
    let is_error = payload
        .get("is_error")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    if is_error || subtype == "error" || subtype == "failure" {
        let message = value_as_non_empty_string(payload.get("result"))
            .or_else(|| value_as_non_empty_string(payload.get("error")))
            .unwrap_or_else(|| "claude stream reported an unspecified error".to_string());
        events.push(ClaudeRawEvent::Error { message });
        return;
    }
    if matches!(subtype.as_str(), "cancelled" | "canceled" | "interrupted") {
        events.push(ClaudeRawEvent::TurnInterrupted {
            reason: subtype.to_string(),
        });
        return;
    }
    let stop_reason = value_as_non_empty_string(payload.get("stop_reason"))
        .unwrap_or_else(|| "completed".to_string());
    events.push(ClaudeRawEvent::TurnCompleted { stop_reason });
}

#[cfg(any(test, not(coverage)))]
struct ClaudeUsageFields {
    input_tokens: u64,
    output_tokens: u64,
    total_tokens: u64,
    cache_read_input_tokens: u64,
    cache_creation_input_tokens: u64,
}

#[cfg(any(test, not(coverage)))]
fn parse_claude_usage_payload(usage_payload: &serde_json::Value) -> Option<ClaudeUsageFields> {
    let input_tokens = value_as_u64(usage_payload.get("input_tokens"))?;
    let output_tokens = value_as_u64(usage_payload.get("output_tokens"))?;
    let total_tokens = value_as_u64(usage_payload.get("total_tokens"))
        .or_else(|| input_tokens.checked_add(output_tokens))?;
    let cache_read_input_tokens =
        value_as_u64(usage_payload.get("cache_read_input_tokens")).unwrap_or(0);
    let cache_creation_input_tokens =
        value_as_u64(usage_payload.get("cache_creation_input_tokens")).unwrap_or(0);
    Some(ClaudeUsageFields {
        input_tokens,
        output_tokens,
        total_tokens,
        cache_read_input_tokens,
        cache_creation_input_tokens,
    })
}

#[cfg(any(test, not(coverage)))]
fn value_as_non_empty_string(value: Option<&serde_json::Value>) -> Option<String> {
    let value = value?;
    match value {
        serde_json::Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        _ => None,
    }
}

#[cfg(any(test, not(coverage)))]
fn value_as_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    if let Some(number) = value.as_u64() {
        return Some(number);
    }
    value.as_str()?.parse::<u64>().ok()
}

fn deterministic_claude_backend_session_id(context: &SessionContext) -> String {
    let normalized = context.logical_session_id.replace(':', "-");
    format!("daemon-claude-{normalized}")
}

fn deterministic_claude_send_turn(input: &TurnInput) -> CrabResult<Vec<ClaudeRawEvent>> {
    if input.run_id.contains(DAEMON_CLAUDE_FORCE_SEND_ERROR_TOKEN) {
        return Err(CrabError::InvariantViolation {
            context: "daemon_claude_send_turn",
            message: "forced claude send failure".to_string(),
        });
    }

    let input_tokens = u64::try_from(input.user_input.split_whitespace().count())
        .unwrap_or(1)
        .max(1);
    let response = "Claude bridge response".to_string();
    let output_tokens = u64::try_from(response.split_whitespace().count())
        .unwrap_or(1)
        .max(1);
    let total_tokens = input_tokens.saturating_add(output_tokens);

    Ok(vec![
        ClaudeRawEvent::TextDelta { text: response },
        ClaudeRawEvent::Usage {
            input_tokens,
            output_tokens,
            total_tokens,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        },
        ClaudeRawEvent::TurnCompleted {
            stop_reason: "end_turn".to_string(),
        },
    ])
}

#[cfg(any(test, coverage))]
impl ClaudeProcess for DaemonClaudeProcess {
    fn create_session(&self, context: &SessionContext) -> CrabResult<String> {
        Ok(deterministic_claude_backend_session_id(context))
    }

    fn send_turn(
        &self,
        _backend_session_id: &str,
        input: &TurnInput,
    ) -> CrabResult<ClaudeRawEventStream> {
        let events = deterministic_claude_send_turn(input)?;
        Ok(Box::pin(stream::iter(events)))
    }

    fn interrupt_turn(&self, _backend_session_id: &str, _turn_id: &str) -> CrabResult<()> {
        Ok(())
    }

    fn end_session(&self, _backend_session_id: &str) -> CrabResult<()> {
        Ok(())
    }
}

#[cfg(not(any(test, coverage)))]
#[derive(Debug, Default)]
struct DaemonClaudeProcessState {
    sessions: BTreeMap<String, ClaudeSessionExecutionConfig>,
    active_interrupt: Option<Arc<AtomicBool>>,
}

#[cfg(not(any(test, coverage)))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeSessionMode {
    Start,
    Resume,
}

#[cfg(not(any(test, coverage)))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaudeTurnFailureKind {
    /// `claude --resume <id>` reported it cannot find the requested session.
    UnknownSessionOnResume,
    /// `claude --session-id <id>` reported that session ID is already in use.
    SessionInUseOnStart,
    Other,
}

#[cfg(not(any(test, coverage)))]
fn claude_fallback_mode(
    attempt_mode: ClaudeSessionMode,
    failure_kind: ClaudeTurnFailureKind,
) -> Option<ClaudeSessionMode> {
    match (attempt_mode, failure_kind) {
        (ClaudeSessionMode::Resume, ClaudeTurnFailureKind::UnknownSessionOnResume) => {
            Some(ClaudeSessionMode::Start)
        }
        (ClaudeSessionMode::Start, ClaudeTurnFailureKind::SessionInUseOnStart) => {
            Some(ClaudeSessionMode::Resume)
        }
        _ => None,
    }
}

#[cfg(not(any(test, coverage)))]
impl ClaudeProcess for DaemonClaudeProcess {
    fn create_session(&self, context: &SessionContext) -> CrabResult<String> {
        if use_deterministic_claude_process_override() {
            return Ok(deterministic_claude_backend_session_id(context));
        }

        let session_id = build_claude_session_id(&context.logical_session_id);
        let mapped_profile = map_claude_inference_profile(&context.profile);
        let effort = match mapped_profile.thinking_mode {
            ClaudeThinkingMode::Low => Some("low".to_string()),
            ClaudeThinkingMode::Medium => Some("medium".to_string()),
            ClaudeThinkingMode::High => Some("high".to_string()),
            ClaudeThinkingMode::Off => None,
        };
        let mut state = self.state.lock().expect("lock should succeed");
        state.sessions.insert(
            session_id.clone(),
            ClaudeSessionExecutionConfig {
                model: mapped_profile.model,
                effort,
                initialized: false,
            },
        );
        Ok(session_id)
    }

    fn send_turn(
        &self,
        backend_session_id: &str,
        input: &TurnInput,
    ) -> CrabResult<ClaudeRawEventStream> {
        if use_deterministic_claude_process_override() {
            let events = deterministic_claude_send_turn(input)?;
            return Ok(Box::pin(stream::iter(events)));
        }

        let (config, mode) = {
            let state = self.state.lock().expect("lock should succeed");
            match state.sessions.get(backend_session_id) {
                Some(config) if config.initialized => (config.clone(), ClaudeSessionMode::Resume),
                Some(config) => (config.clone(), ClaudeSessionMode::Start),
                None => (
                    ClaudeSessionExecutionConfig::default(),
                    ClaudeSessionMode::Resume,
                ),
            }
        };

        let interrupt_flag = Arc::new(AtomicBool::new(false));
        {
            let mut locked = self.state.lock().expect("lock should succeed");
            locked.active_interrupt = Some(Arc::clone(&interrupt_flag));
        }

        let backend_session_id = backend_session_id.to_string();
        let input = input.clone();
        let state = Arc::clone(&self.state);
        let (sender, receiver) = mpsc::unbounded::<ClaudeRawEvent>();
        let thread_interrupt = Arc::clone(&interrupt_flag);

        thread::spawn(move || {
            let mut succeeded = false;
            let mut last_error: Option<CrabError> = None;
            let mut attempt_mode = mode;
            loop {
                let result = run_claude_turn(
                    &backend_session_id,
                    &input,
                    &config,
                    attempt_mode,
                    &sender,
                    &thread_interrupt,
                );
                match result {
                    Ok(()) => {
                        succeeded = true;
                        break;
                    }
                    Err(error) => {
                        let failure_kind = classify_claude_turn_failure(&error);
                        last_error = Some(error);
                        let Some(next_mode) = claude_fallback_mode(attempt_mode, failure_kind)
                        else {
                            break;
                        };
                        attempt_mode = next_mode;
                    }
                }
            }

            if succeeded {
                let mut locked = state.lock().expect("lock should succeed");
                locked
                    .sessions
                    .entry(backend_session_id.clone())
                    .and_modify(|entry| entry.initialized = true)
                    .or_insert_with(|| ClaudeSessionExecutionConfig {
                        initialized: true,
                        ..config
                    });
            }
            if !succeeded {
                let _ = sender.unbounded_send(ClaudeRawEvent::Error {
                    message: last_error
                        .map(|error| error.to_string())
                        .unwrap_or_else(|| "claude turn failed (unknown error)".to_string()),
                });
            }
            // Clear the interrupt flag on thread completion.
            {
                let mut locked = state.lock().expect("lock should succeed");
                locked.active_interrupt = None;
            }
            drop(sender);
        });

        Ok(Box::pin(receiver))
    }

    fn interrupt_turn(&self, _backend_session_id: &str, _turn_id: &str) -> CrabResult<()> {
        let state = self.state.lock().expect("lock should succeed");
        if let Some(flag) = state.active_interrupt.as_ref() {
            flag.store(true, Ordering::SeqCst);
        }
        Ok(())
    }

    fn end_session(&self, backend_session_id: &str) -> CrabResult<()> {
        let mut state = self.state.lock().expect("lock should succeed");
        state.sessions.remove(backend_session_id);
        Ok(())
    }
}

#[cfg(not(any(test, coverage)))]
fn build_claude_session_id(logical_session_id: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);

    let mut primary = DefaultHasher::new();
    logical_session_id.hash(&mut primary);
    now_nanos.hash(&mut primary);
    let first = primary.finish();

    let mut secondary = DefaultHasher::new();
    std::process::id().hash(&mut secondary);
    logical_session_id.len().hash(&mut secondary);
    now_nanos.rotate_left(19).hash(&mut secondary);
    let second = secondary.finish();

    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&first.to_be_bytes());
    bytes[8..].copy_from_slice(&second.to_be_bytes());
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

#[cfg(not(any(test, coverage)))]
fn run_claude_turn(
    backend_session_id: &str,
    input: &TurnInput,
    config: &ClaudeSessionExecutionConfig,
    mode: ClaudeSessionMode,
    sender: &mpsc::UnboundedSender<ClaudeRawEvent>,
    interrupt_flag: &Arc<AtomicBool>,
) -> CrabResult<()> {
    use std::io::{BufRead, BufReader, Read};
    use std::process::Stdio;
    use std::sync::mpsc as std_mpsc;

    let mut command = std::process::Command::new("claude");
    command
        .arg("--print")
        .arg("--verbose")
        .arg("--output-format")
        .arg("stream-json")
        .arg("--include-partial-messages")
        .arg("--dangerously-skip-permissions");
    if let Ok(workspace_root) = std::env::var("CRAB_WORKSPACE_ROOT") {
        let trimmed = workspace_root.trim();
        if !trimmed.is_empty() {
            command.current_dir(trimmed);
            let state_dir = Path::new(trimmed).join("state");
            command.env("CRAB_STATE_DIR", state_dir);
        }
    }
    if let Some(model) = config
        .model
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        command.arg("--model").arg(model);
    }
    if let Some(effort) = config
        .effort
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        command.arg("--effort").arg(effort);
    }
    match mode {
        ClaudeSessionMode::Start => {
            command.arg("--session-id").arg(backend_session_id);
        }
        ClaudeSessionMode::Resume => {
            command.arg("--resume").arg(backend_session_id);
        }
    }
    command.arg(&input.user_input);

    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command.spawn().map_err(|error| CrabError::Io {
        context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
        path: None,
        message: format!("failed to spawn claude process: {error}"),
    })?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| CrabError::InvariantViolation {
            context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
            message: "claude stdout pipe is missing".to_string(),
        })?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| CrabError::InvariantViolation {
            context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
            message: "claude stderr pipe is missing".to_string(),
        })?;

    let stderr_handle = thread::spawn(move || {
        let mut buffer = String::new();
        let _ = stderr.read_to_string(&mut buffer);
        buffer
    });

    let mut emitted_any = false;
    let mut saw_terminal = false;

    let stall_timeout_secs = std::env::var("CRAB_BACKEND_STALL_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(crab_core::config::DEFAULT_BACKEND_STALL_TIMEOUT_SECS);
    let stall_timeout = Duration::from_secs(stall_timeout_secs);

    // Poll interval for checking the interrupt flag between stdout reads.
    let poll_interval = Duration::from_millis(500);

    let (line_tx, line_rx) = std_mpsc::channel::<CrabResult<Option<String>>>();
    let stdout_handle = thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for line in reader.lines() {
            let line = match line {
                Ok(line) => line,
                Err(error) => {
                    let _ = line_tx.send(Err(CrabError::Io {
                        context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
                        path: None,
                        message: format!("failed reading claude stdout: {error}"),
                    }));
                    return;
                }
            };
            if line_tx.send(Ok(Some(line))).is_err() {
                return;
            }
        }
        let _ = line_tx.send(Ok(None));
    });

    let mut idle_since = std::time::Instant::now();
    let mut interrupted = false;
    loop {
        // Check interrupt flag before waiting for the next line.
        if interrupt_flag.load(Ordering::SeqCst) {
            interrupted = true;
            break;
        }

        let maybe_line = match line_rx.recv_timeout(poll_interval) {
            Ok(result) => {
                idle_since = std::time::Instant::now();
                result?
            }
            Err(std_mpsc::RecvTimeoutError::Timeout) => {
                if idle_since.elapsed() >= stall_timeout {
                    let _ = child.kill();
                    let status = child.wait().ok();
                    let stderr_output = stderr_handle.join().unwrap_or_default();
                    let _ = stdout_handle.join();
                    let detail = if !stderr_output.trim().is_empty() {
                        stderr_output.trim().to_string()
                    } else if let Some(status) = status {
                        format!("claude exited after stall with status: {status}")
                    } else {
                        "claude produced no stderr output".to_string()
                    };
                    return Err(CrabError::InvariantViolation {
                        context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
                        message: format!(
                            "claude process stalled (no stdout for {stall_timeout_secs}s) for run {} turn {}: {}",
                            input.run_id, input.turn_id, detail
                        ),
                    });
                }
                continue;
            }
            Err(std_mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let Some(line) = maybe_line else {
            break;
        };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let parsed = parse_claude_stream_line(trimmed)?;
        for event in parsed {
            emitted_any = true;
            if matches!(
                event,
                ClaudeRawEvent::TurnCompleted { .. }
                    | ClaudeRawEvent::TurnInterrupted { .. }
                    | ClaudeRawEvent::Error { .. }
            ) {
                saw_terminal = true;
            }
            let _ = sender.unbounded_send(event);
        }
    }

    if interrupted {
        let _ = child.kill();
        let _ = child.wait();
        let _ = stderr_handle.join();
        let _ = stdout_handle.join();
        let _ = sender.unbounded_send(ClaudeRawEvent::TurnInterrupted {
            reason: "steered".to_string(),
        });
        return Ok(());
    }

    let status = child.wait().map_err(|error| CrabError::Io {
        context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
        path: None,
        message: format!("failed waiting for claude process: {error}"),
    })?;
    let stderr_output = stderr_handle.join().unwrap_or_default();
    let _ = stdout_handle.join();

    if !status.success() {
        let detail = if !stderr_output.trim().is_empty() {
            stderr_output.trim().to_string()
        } else {
            "claude process exited without error output".to_string()
        };
        if !emitted_any {
            return Err(CrabError::InvariantViolation {
                context: DAEMON_CLAUDE_TRANSPORT_CONTEXT,
                message: format!(
                    "claude process failed for run {} turn {}: {}",
                    input.run_id, input.turn_id, detail
                ),
            });
        }
        let _ = sender.unbounded_send(ClaudeRawEvent::Error { message: detail });
        return Ok(());
    }

    if !emitted_any {
        return Err(CrabError::InvariantViolation {
            context: DAEMON_CLAUDE_STREAM_CONTEXT,
            message: "claude stream produced no assistant/result events".to_string(),
        });
    }

    if !saw_terminal {
        let _ = sender.unbounded_send(ClaudeRawEvent::TurnCompleted {
            stop_reason: "completed".to_string(),
        });
    }

    Ok(())
}

#[cfg(not(any(test, coverage)))]
fn is_session_in_use_error(error: &CrabError) -> bool {
    matches!(
        error,
        CrabError::InvariantViolation { context, message }
            if *context == DAEMON_CLAUDE_TRANSPORT_CONTEXT
                && message.to_ascii_lowercase().contains("already in use")
    )
}

#[cfg(not(any(test, coverage)))]
fn is_unknown_session_resume_error(error: &CrabError) -> bool {
    matches!(
        error,
        CrabError::InvariantViolation { context, message }
            if *context == DAEMON_CLAUDE_TRANSPORT_CONTEXT
                && (message.to_ascii_lowercase().contains("could not find session")
                    || message.to_ascii_lowercase().contains("no conversation found"))
    )
}

#[cfg(not(any(test, coverage)))]
fn classify_claude_turn_failure(error: &CrabError) -> ClaudeTurnFailureKind {
    if is_session_in_use_error(error) {
        return ClaudeTurnFailureKind::SessionInUseOnStart;
    }
    if is_unknown_session_resume_error(error) {
        return ClaudeTurnFailureKind::UnknownSessionOnResume;
    }
    ClaudeTurnFailureKind::Other
}

#[derive(Clone)]
struct SharedClaudeProcess {
    inner: Arc<dyn ClaudeProcess>,
}

impl std::fmt::Debug for SharedClaudeProcess {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SharedClaudeProcess")
            .finish_non_exhaustive()
    }
}

impl ClaudeProcess for SharedClaudeProcess {
    fn create_session(&self, context: &SessionContext) -> CrabResult<String> {
        self.inner.create_session(context)
    }

    fn send_turn(
        &self,
        backend_session_id: &str,
        input: &TurnInput,
    ) -> CrabResult<ClaudeRawEventStream> {
        self.inner.send_turn(backend_session_id, input)
    }

    fn interrupt_turn(&self, backend_session_id: &str, turn_id: &str) -> CrabResult<()> {
        self.inner.interrupt_turn(backend_session_id, turn_id)
    }

    fn end_session(&self, backend_session_id: &str) -> CrabResult<()> {
        self.inner.end_session(backend_session_id)
    }
}

#[derive(Clone)]
struct DaemonClaudeExecutionBridge {
    harness: BackendHarness<ClaudeBackend<SharedClaudeProcess>>,
}

impl std::fmt::Debug for DaemonClaudeExecutionBridge {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonClaudeExecutionBridge")
            .finish_non_exhaustive()
    }
}

impl DaemonClaudeExecutionBridge {
    fn with_process(process: Arc<dyn ClaudeProcess>) -> Self {
        Self {
            harness: BackendHarness::new(ClaudeBackend::new(SharedClaudeProcess {
                inner: process,
            })),
        }
    }
}

fn parse_claude_backend_session_id(physical_session_id: &str) -> Option<&str> {
    physical_session_id
        .strip_prefix("claude:")
        .filter(|backend_session_id| !backend_session_id.trim().is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ClaudeSessionStrategy<'a> {
    ReuseCached {
        active_id: &'a str,
    },
    RestoreFromActiveId {
        active_id: &'a str,
        backend_session_id: &'a str,
    },
    CreateNew,
}

fn resolve_claude_session_strategy(
    active_physical_session_id: Option<&str>,
    has_cached_active_session: bool,
) -> ClaudeSessionStrategy<'_> {
    let Some(active_id) = active_physical_session_id else {
        return ClaudeSessionStrategy::CreateNew;
    };
    if has_cached_active_session {
        return ClaudeSessionStrategy::ReuseCached { active_id };
    }
    if let Some(backend_session_id) = parse_claude_backend_session_id(active_id) {
        return ClaudeSessionStrategy::RestoreFromActiveId {
            active_id,
            backend_session_id,
        };
    }
    ClaudeSessionStrategy::CreateNew
}

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
    pub ingested_triggers: u64,
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
    physical_sessions: BTreeMap<String, crab_core::PhysicalSession>,
    turn_context_runtime: Option<TurnContextRuntimeState>,
    claude_bridge: DaemonClaudeExecutionBridge,
}

#[derive(Debug, Clone)]
struct TurnContextRuntimeState {
    workspace_root: PathBuf,
    checkpoint_store: CheckpointStore,
    context_budget_policy: ContextBudgetPolicy,
}

#[cfg(all(not(any(test, coverage)), debug_assertions))]
fn use_deterministic_claude_process_override() -> bool {
    std::env::var(DAEMON_DETERMINISTIC_CLAUDE_PROCESS_ENV)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}

#[cfg(all(not(any(test, coverage)), not(debug_assertions)))]
fn use_deterministic_claude_process_override() -> bool {
    false
}

impl<D: DaemonDiscordIo> DaemonTurnRuntime<D> {
    pub fn new(owner: OwnerConfig, discord: D) -> CrabResult<Self> {
        Self::new_with_claude_process(owner, discord, DaemonClaudeProcess::default())
    }

    fn new_with_claude_process<P>(
        owner: OwnerConfig,
        discord: D,
        claude_process: P,
    ) -> CrabResult<Self>
    where
        P: ClaudeProcess + 'static,
    {
        Ok(Self {
            discord,
            owner,
            physical_sessions: BTreeMap::new(),
            turn_context_runtime: None,
            claude_bridge: DaemonClaudeExecutionBridge::with_process(Arc::new(claude_process)),
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

    fn shutdown_claude_sessions(&mut self) -> CrabResult<()> {
        let session_ids: Vec<String> = self
            .physical_sessions
            .iter()
            .filter(|(_, session)| session.backend == BackendKind::Claude)
            .map(|(session_id, _)| session_id.clone())
            .collect();

        for session_id in session_ids {
            let session = self
                .physical_sessions
                .get(&session_id)
                .cloned()
                .expect("session ids collected from map should resolve");
            session
                .last_turn_id
                .as_deref()
                .map(|turn_id| {
                    block_on(self.claude_bridge.harness.interrupt_turn(&session, turn_id))
                })
                .transpose()?;
            block_on(self.claude_bridge.harness.end_session(&session))?;
            self.physical_sessions.remove(&session_id);
        }

        Ok(())
    }

    pub fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
        self.discord.next_gateway_message()
    }

    fn build_runtime_turn_context(
        &mut self,
        run: &Run,
        logical_session: &crab_core::LogicalSession,
        physical_session: &crab_core::PhysicalSession,
        inject_bootstrap_context: bool,
    ) -> CrabResult<String> {
        #[cfg(coverage)]
        let _ = physical_session;
        let Some(runtime) = self.turn_context_runtime.clone() else {
            return Ok(run.user_input.clone());
        };
        if !inject_bootstrap_context {
            return Ok(run.user_input.clone());
        }

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

        let checkpoint_summary =
            load_latest_checkpoint_summary(logical_session, &runtime.checkpoint_store)?;
        let bootstrap_state = detect_workspace_bootstrap_state(&runtime.workspace_root)?;
        let crab_runtime_brief = render_crab_runtime_brief(run, bootstrap_state);
        let context_input = ContextAssemblyInput {
            soul_document: read_workspace_markdown(&runtime.workspace_root, SOUL_FILE_NAME)?,
            identity_document: read_workspace_markdown(
                &runtime.workspace_root,
                IDENTITY_FILE_NAME,
            )?,
            user_document: read_workspace_markdown(&runtime.workspace_root, USER_FILE_NAME)?,
            memory_document: read_workspace_markdown(&runtime.workspace_root, MEMORY_FILE_NAME)?,
            memory_snippets,
            latest_checkpoint_summary: checkpoint_summary,
            crab_runtime_brief,
            prompt_contract,
            turn_input: run.user_input.clone(),
        };
        let budgeted =
            render_budgeted_turn_context(&context_input, &runtime.context_budget_policy)?;

        #[cfg(not(coverage))]
        {
            let report = build_context_diagnostics_report(&budgeted);
            let fixture = render_context_diagnostics_fixture(&report);
            tracing::info!(
                logical_session_id = %run.logical_session_id,
                physical_session_id = %physical_session.id,
                run_id = %run.id,
                injected_context_tokens = report.rendered_context_tokens,
                injected_context_chars = report.rendered_context_chars,
                "bootstrap context prepared for physical session"
            );
            tracing::debug!(
                logical_session_id = %run.logical_session_id,
                physical_session_id = %physical_session.id,
                run_id = %run.id,
                context_diagnostics = %fixture,
                "rendered turn context"
            );
        }

        Ok(budgeted.rendered_context)
    }

    fn session_now_epoch_ms() -> CrabResult<u64> {
        now_epoch_ms()
    }
}

fn render_crab_runtime_brief(run: &Run, bootstrap_state: WorkspaceBootstrapState) -> String {
    let mut brief = CRAB_RUNTIME_BRIEF_BASE.to_string();
    if bootstrap_state == WorkspaceBootstrapState::PendingBootstrap
        && run.profile.sender_is_owner
        && run.logical_session_id.starts_with("discord:dm:")
    {
        brief.push_str(
            "\nOnboarding is pending. In this owner DM, prioritize gathering:\n\
- who the agent is\n\
- who the owner is\n\
- primary goals\n\
- machine location\n\
- machine timezone\n\
Keep the conversation natural and concise while filling these gaps.",
        );
    }
    brief
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
                    .unwrap_or(BackendKind::Claude)
            } else {
                BackendKind::Claude
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
        let has_cached_active_session = active_physical_session_id
            .is_some_and(|active_id| self.physical_sessions.contains_key(active_id));
        match resolve_claude_session_strategy(active_physical_session_id, has_cached_active_session)
        {
            ClaudeSessionStrategy::ReuseCached { active_id } => {
                let existing = self
                    .physical_sessions
                    .get(active_id)
                    .cloned()
                    .expect("resolve_claude_session_strategy guarantees cached session exists");
                return Ok(existing);
            }
            ClaudeSessionStrategy::RestoreFromActiveId {
                active_id,
                backend_session_id,
            } => {
                let session = crab_core::PhysicalSession {
                    id: active_id.to_string(),
                    logical_session_id: logical_session_id.to_string(),
                    backend: BackendKind::Claude,
                    backend_session_id: backend_session_id.to_string(),
                    created_at_epoch_ms: Self::session_now_epoch_ms()?,
                    last_turn_id: None,
                };
                self.physical_sessions
                    .insert(active_id.to_string(), session.clone());
                return Ok(session);
            }
            ClaudeSessionStrategy::CreateNew => {}
        }

        let session_context = SessionContext {
            logical_session_id: logical_session_id.to_string(),
            profile: profile.clone(),
        };
        let session = block_on(self.claude_bridge.harness.create_session(&session_context))?;
        self.physical_sessions
            .insert(session.id.clone(), session.clone());
        Ok(session)
    }

    fn build_turn_context(
        &mut self,
        run: &Run,
        logical_session: &crab_core::LogicalSession,
        physical_session: &crab_core::PhysicalSession,
        inject_bootstrap_context: bool,
    ) -> CrabResult<String> {
        self.build_runtime_turn_context(
            run,
            logical_session,
            physical_session,
            inject_bootstrap_context,
        )
    }

    fn execute_backend_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<crab_backends::BackendEventStream> {
        let cache_key = physical_session.id.clone();
        let input = TurnInput {
            run_id: run.id.clone(),
            turn_id: turn_id.to_string(),
            user_input: turn_context.to_string(),
        };
        let stream = block_on(
            self.claude_bridge
                .harness
                .send_turn(physical_session, input),
        )?;
        self.physical_sessions
            .insert(cache_key, physical_session.clone());
        Ok(stream)
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

    fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
        self.discord.next_gateway_message()
    }

    fn interrupt_backend_turn(
        &mut self,
        session: &crab_core::PhysicalSession,
        turn_id: &str,
    ) -> CrabResult<()> {
        block_on(self.claude_bridge.harness.interrupt_turn(session, turn_id))
    }
}

pub fn run_daemon_loop_with_transport<D, C>(
    runtime_config: &RuntimeConfig,
    daemon_config: &DaemonConfig,
    discord: D,
    control: &mut C,
) -> CrabResult<DaemonLoopStats>
where
    D: DaemonDiscordIo,
    C: DaemonLoopControl + ?Sized,
{
    run_daemon_loop_with_transport_and_runtime_builder(
        runtime_config,
        daemon_config,
        discord,
        control,
        DaemonTurnRuntime::new,
    )
}

fn run_daemon_loop_with_transport_and_runtime_builder<D, C, RB>(
    runtime_config: &RuntimeConfig,
    daemon_config: &DaemonConfig,
    discord: D,
    control: &mut C,
    runtime_builder: RB,
) -> CrabResult<DaemonLoopStats>
where
    D: DaemonDiscordIo,
    C: DaemonLoopControl + ?Sized,
    RB: FnOnce(OwnerConfig, D) -> CrabResult<DaemonTurnRuntime<D>>,
{
    let _instance_lock = DaemonInstanceLock::acquire(runtime_config)?;
    let mut discord = discord;
    daemon_config.validate()?;
    let now_epoch_ms = control.now_epoch_ms()?;
    let boot = crate::boot_runtime(runtime_config, &daemon_config.bot_user_id, now_epoch_ms)?;
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
        || !boot.startup_reconciliation.repaired_session_ids.is_empty()
        || !boot
            .startup_reconciliation
            .repaired_physical_sessions
            .is_empty()
    {
        // `tracing` macros can produce stubborn per-line coverage gaps under `cargo llvm-cov`
        // (cfg(coverage)), even when the behavior is exercised. Keep runtime logs, but exclude
        // them from coverage builds where stdout/stderr output is not the product.
        #[cfg(not(coverage))]
        tracing::warn!(
            recovered_runs = boot.startup_reconciliation.recovered_runs.len(),
            repaired_sessions = boot.startup_reconciliation.repaired_session_ids.len(),
            repaired_physical_sessions =
                boot.startup_reconciliation.repaired_physical_sessions.len(),
            "startup reconciliation recovered in-flight work"
        );
        #[cfg(not(coverage))]
        tracing::debug!(
            recovered = ?boot.startup_reconciliation.recovered_runs,
            repaired = ?boot.startup_reconciliation.repaired_session_ids,
            repaired_physical = ?boot.startup_reconciliation.repaired_physical_sessions,
            "startup reconciliation details"
        );
    } else {
        tracing::info!("startup reconciliation: no in-flight work recovered");
    }

    // If we had to reconcile stale in-flight work, tell the user(s). This prevents "silent"
    // failure modes where the harness restarts but Discord looks stuck.
    if !boot.startup_reconciliation.recovered_runs.is_empty() {
        let sent = notify_startup_recovered_runs(
            &boot.composition.state_stores.run_store,
            &boot.startup_reconciliation.recovered_runs,
            &mut discord,
        );
        #[cfg(coverage)]
        let _ = &sent;
        #[cfg(not(coverage))]
        if let Err(_error) = sent {
            tracing::warn!(
                error = %_error,
                "failed sending startup reconciliation notifications"
            );
        }
    }

    let mut heartbeat_loop_state = boot.heartbeat_loop_state;
    #[cfg(test)]
    {
        let _ = std::fs::remove_file(boot.composition.startup.workspace_root.join("BOOTSTRAP.md"));
    }
    let mut runtime = runtime_builder(runtime_config.owner.clone(), discord)?;
    runtime.configure_turn_context_runtime(
        boot.composition.startup.workspace_root.clone(),
        boot.composition.state_stores.checkpoint_store.clone(),
    );
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

        for (trigger_path, trigger) in
            crab_core::read_pending_triggers(&executor.composition().state_stores.root)?
        {
            match executor.enqueue_pending_trigger(&trigger.channel_id, &trigger.message) {
                Ok(_) => {
                    crab_core::consume_pending_trigger(&trigger_path)?;
                    stats.ingested_triggers = stats.ingested_triggers.saturating_add(1);
                }
                Err(_error) => {
                    #[cfg(not(coverage))]
                    tracing::warn!(
                        channel_id = %trigger.channel_id,
                        error = %_error,
                        "failed to enqueue pending trigger"
                    );
                }
            }
        }

        // Also consume steering triggers when idle (so they don't pile up).
        // Batch by channel so multiple triggers become one combined run.
        // Use TriggerKind::Pending here: no run was active so no interruption
        // happened; wrapping with "while you were working" would be false.
        {
            let state_root = executor.composition().state_stores.root.clone();
            let steering = crab_core::read_steering_triggers(&state_root)?;
            // Keep on one line: multi-line call sites can produce llvm-cov line-mapping gaps.
            #[rustfmt::skip]
            let (_, consumed) = executor.consume_and_batch_triggers(steering, "", crab_core::consume_steering_trigger, TriggerKind::Pending)?;
            stats.ingested_triggers = stats.ingested_triggers.saturating_add(consumed as u64);

            let graceful = crab_core::read_graceful_steering_triggers(&state_root)?;
            // Keep on one line: multi-line call sites can produce llvm-cov line-mapping gaps.
            #[rustfmt::skip]
            let (_, consumed) = executor.consume_and_batch_triggers(graceful, "", crab_core::consume_graceful_steering_trigger, TriggerKind::Pending)?;
            stats.ingested_triggers = stats.ingested_triggers.saturating_add(consumed as u64);
        }

        while executor.dispatch_next_run()?.is_some() {
            stats.dispatched_runs = stats.dispatched_runs.saturating_add(1);
        }

        let now_epoch_ms = control.now_epoch_ms()?;
        #[rustfmt::skip]
        let _ = evaluate_self_work(&mut executor, now_epoch_ms, runtime_config.self_work.idle_delay_ms)?;

        let heartbeat_outcome = run_heartbeat_if_due(
            executor.composition_mut(),
            &mut heartbeat_loop_state,
            now_epoch_ms,
        );
        if let Some(outcome) = heartbeat_outcome? {
            stats.heartbeat_cycles = stats.heartbeat_cycles.saturating_add(1);
            #[cfg(coverage)]
            let _ = &outcome;

            #[cfg(not(coverage))]
            {
                let had_actions = !outcome.cancelled_runs.is_empty()
                    || !outcome.hard_stopped_runs.is_empty()
                    || !outcome.restarted_backends.is_empty()
                    || outcome.dispatcher_nudged;
                if had_actions {
                    tracing::warn!(
                        cancelled_runs = outcome.cancelled_runs.len(),
                        hard_stopped_runs = outcome.hard_stopped_runs.len(),
                        restarted_backends = outcome.restarted_backends.len(),
                        dispatcher_nudged = outcome.dispatcher_nudged,
                        events = outcome.events.len(),
                        "heartbeat took corrective action"
                    );
                    tracing::debug!(?outcome, "heartbeat outcome details");
                } else {
                    tracing::debug!(events = outcome.events.len(), "heartbeat cycle complete");
                }
            }
        }

        control.sleep_tick(daemon_config.tick_interval_ms)?;
    }

    tracing::info!(
        iterations = stats.iterations,
        ingested = stats.ingested_messages,
        triggers = stats.ingested_triggers,
        dispatched = stats.dispatched_runs,
        heartbeats = stats.heartbeat_cycles,
        "daemon loop exiting: shutting down backends"
    );
    executor.runtime_mut().shutdown_claude_sessions()?;
    Ok(stats)
}

fn evaluate_self_work<R: TurnExecutorRuntime>(
    executor: &mut TurnExecutor<R>,
    now_epoch_ms: u64,
    idle_delay_ms: u64,
) -> CrabResult<u64> {
    let state_root = executor.composition().state_stores.root.clone();
    let Some(session) = read_self_work_session(&state_root)? else {
        return Ok(0);
    };
    if session.status != SelfWorkSessionStatus::Active {
        return Ok(0);
    }

    if now_epoch_ms >= session.end_at_epoch_ms {
        return evaluate_self_work_expiry(executor, &state_root, now_epoch_ms);
    }
    if session.final_trigger_pending {
        return Ok(0);
    }

    let lane_status = executor.self_work_lane_status(&session.channel_id)?;
    if !self_work_lane_is_idle(&lane_status) {
        return Ok(0);
    }

    let idle_baseline_epoch_ms = self_work_idle_baseline_epoch_ms(&session, &lane_status);
    if now_epoch_ms < idle_baseline_epoch_ms.saturating_add(idle_delay_ms) {
        return Ok(0);
    }

    let Some(mut lock) = try_acquire_self_work_lock_for_daemon(&state_root, now_epoch_ms)? else {
        return Ok(0);
    };
    #[rustfmt::skip]
    let mut locked_session = match read_self_work_session(&state_root)? { Some(s) => s, None => return Ok(0) };

    let locked_lane_status = executor.self_work_lane_status(&locked_session.channel_id)?;
    #[allow(clippy::let_unit_value)]
    #[rustfmt::skip]
    let _wake = match wake_due(&locked_session, &locked_lane_status, now_epoch_ms, idle_delay_ms) { true => (), false => return Ok(0) };

    let trigger = crab_core::PendingTrigger {
        channel_id: locked_session.channel_id.clone(),
        message: build_wake_trigger_message(&locked_session, &state_root),
    };
    write_pending_trigger(&state_root, &trigger)?;
    locked_session.last_wake_triggered_at_epoch_ms = Some(now_epoch_ms);
    write_self_work_session_atomically(&state_root, &locked_session)?;
    lock.release()?;
    Ok(1)
}

fn evaluate_self_work_expiry<R: TurnExecutorRuntime>(
    executor: &mut TurnExecutor<R>,
    state_root: &Path,
    now_epoch_ms: u64,
) -> CrabResult<u64> {
    let Some(mut lock) = try_acquire_self_work_lock_for_daemon(state_root, now_epoch_ms)? else {
        return Ok(0);
    };
    let Some(mut session) = read_self_work_session(state_root)? else {
        return Ok(0);
    };
    if session.status != SelfWorkSessionStatus::Active {
        return Ok(0);
    }
    if now_epoch_ms < session.end_at_epoch_ms {
        return Ok(0);
    }
    if !session.final_trigger_pending {
        session.final_trigger_pending = true;
        write_self_work_session_atomically(state_root, &session)?;
    }

    let lane_status = executor.self_work_lane_status(&session.channel_id)?;
    if !self_work_lane_is_idle(&lane_status) {
        return Ok(0);
    }

    let trigger = crab_core::PendingTrigger {
        channel_id: session.channel_id.clone(),
        message: build_expiry_trigger_message(&session),
    };
    write_pending_trigger(state_root, &trigger)?;
    session.status = SelfWorkSessionStatus::Expired;
    session.final_trigger_pending = false;
    session.expired_at_epoch_ms = Some(now_epoch_ms);
    session.last_expiry_triggered_at_epoch_ms = Some(now_epoch_ms);
    write_self_work_session_atomically(state_root, &session)?;
    lock.release()?;
    Ok(1)
}

fn self_work_lane_is_idle(lane_status: &SelfWorkLaneStatus) -> bool {
    !lane_status.has_active_run
        && lane_status.queued_run_count == 0
        && lane_status.persisted_lane_state.unwrap_or(LaneState::Idle) == LaneState::Idle
}

fn try_acquire_self_work_lock_for_daemon(
    state_root: &Path,
    now_epoch_ms: u64,
) -> CrabResult<Option<SelfWorkSessionLock>> {
    match SelfWorkSessionLock::acquire(state_root, now_epoch_ms) {
        Ok(lock) => Ok(Some(lock)),
        Err(CrabError::InvariantViolation {
            context: "self_work_session_lock",
            ..
        }) => Ok(None),
        Err(error) => Err(error),
    }
}

fn self_work_idle_baseline_epoch_ms(
    session: &SelfWorkSession,
    lane_status: &SelfWorkLaneStatus,
) -> u64 {
    session
        .started_at_epoch_ms
        .max(session.last_wake_triggered_at_epoch_ms.unwrap_or(0))
        .max(lane_status.last_activity_epoch_ms.unwrap_or(0))
}

fn wake_due(
    session: &SelfWorkSession,
    lane_status: &SelfWorkLaneStatus,
    now_epoch_ms: u64,
    idle_delay_ms: u64,
) -> bool {
    session.status == SelfWorkSessionStatus::Active
        && !session.final_trigger_pending
        && now_epoch_ms < session.end_at_epoch_ms
        && self_work_lane_is_idle(lane_status)
        && now_epoch_ms
            >= self_work_idle_baseline_epoch_ms(session, lane_status).saturating_add(idle_delay_ms)
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

fn notify_startup_recovered_runs<D: DaemonDiscordIo>(
    run_store: &crab_store::RunStore,
    recovered_runs: &[crab_core::startup_reconciliation::StartupReconciliationRecoveredRun],
    discord: &mut D,
) -> CrabResult<usize> {
    let mut sent = 0usize;
    for recovered in recovered_runs {
        let Some(run) = run_store.get_run(&recovered.logical_session_id, &recovered.run_id)? else {
            continue;
        };
        let Some(channel_id) = run.delivery_channel_id.as_deref() else {
            continue;
        };
        let content = format!(
            "Crab restarted while processing a previous message, so it was marked cancelled. Please resend your last message.\n(run_id: {})",
            run.id
        );
        let delivery_id = format!("startup:recovered:{}", run.id);
        if discord
            .post_message(channel_id, &delivery_id, &content)
            .is_ok()
        {
            sent = sent.saturating_add(1);
        }
    }
    Ok(sent)
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
        acquire_lock_on_file, conversation_kind_for_logical_session_id, epoch_ms_from_duration,
        epoch_ms_from_system_time, epoch_ms_to_yyyy_mm_dd, evaluate_self_work,
        evaluate_self_work_expiry, load_latest_checkpoint_summary, memory_scope_directory_for_run,
        notify_startup_recovered_runs, read_workspace_markdown, self_work_idle_baseline_epoch_ms,
        self_work_lane_is_idle, trust_surface_for_logical_session_id,
        try_acquire_self_work_lock_for_daemon, wake_due, DaemonClaudeProcess, DaemonConfig,
        DaemonDiscordIo, DaemonInstanceLock, DaemonLoopControl, DaemonTurnRuntime,
        SystemDaemonLoopControl, CRABD_INSTANCE_LOCK_FILE_NAME,
    };
    use crate::composition::compose_runtime_with_queue_limit;
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};
    use crate::turn_executor::SelfWorkLaneStatus;
    use crate::{TurnExecutor, TurnExecutorRuntime};
    use crab_backends::{
        claude::{ClaudeRawEvent, ClaudeRawEventStream},
        BackendEvent, BackendEventKind, ClaudeProcess, SessionContext, TurnInput,
    };
    use crab_core::{
        read_pending_triggers, read_self_work_session, write_self_work_session_atomically,
        BackendKind, Checkpoint, CrabError, CrabResult, InferenceProfile, LaneState,
        LogicalSession, ProfileValueSource, ReasoningLevel, Run, RunProfileTelemetry, RunStatus,
        SelfWorkSession, SelfWorkSessionStatus, SenderConversationKind, TokenAccounting,
        TrustSurface, WorkspaceBootstrapState, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
    };
    use crab_discord::GatewayMessage;
    use crab_scheduler::QueuedRun;
    use crab_store::{CheckpointStore, RunStore};
    use futures::executor::block_on;
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::fs;
    #[cfg(unix)]
    use std::os::fd::FromRawFd;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    #[cfg(unix)]
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, UNIX_EPOCH};

    #[derive(Debug, Clone, Default, PartialEq, Eq)]
    struct ScriptedClaudeStats {
        create_calls: usize,
        send_calls: usize,
        interrupt_calls: usize,
        end_calls: usize,
        last_session_context: Option<SessionContext>,
        last_backend_session_id: Option<String>,
        last_turn_input: Option<TurnInput>,
        last_interrupted_turn_id: Option<String>,
        last_ended_backend_session_id: Option<String>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedClaudeState {
        create_results: VecDeque<CrabResult<String>>,
        send_results: VecDeque<CrabResult<Vec<ClaudeRawEvent>>>,
        interrupt_results: VecDeque<CrabResult<()>>,
        end_results: VecDeque<CrabResult<()>>,
        stats: ScriptedClaudeStats,
    }

    #[derive(Debug, Clone)]
    struct ScriptedClaudeProcess {
        state: Arc<Mutex<ScriptedClaudeState>>,
    }

    impl ScriptedClaudeProcess {
        fn with_scripted(
            create_results: Vec<CrabResult<String>>,
            send_results: Vec<CrabResult<Vec<ClaudeRawEvent>>>,
            interrupt_results: Vec<CrabResult<()>>,
            end_results: Vec<CrabResult<()>>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedClaudeState {
                    create_results: VecDeque::from(create_results),
                    send_results: VecDeque::from(send_results),
                    interrupt_results: VecDeque::from(interrupt_results),
                    end_results: VecDeque::from(end_results),
                    stats: ScriptedClaudeStats::default(),
                })),
            }
        }

        fn stats(&self) -> ScriptedClaudeStats {
            self.state
                .lock()
                .expect("lock should succeed")
                .stats
                .clone()
        }
    }

    impl ClaudeProcess for ScriptedClaudeProcess {
        fn create_session(&self, context: &SessionContext) -> CrabResult<String> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.create_calls += 1;
            state.stats.last_session_context = Some(context.clone());
            state.create_results.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "daemon_test_claude_create",
                    message: "missing scripted create result".to_string(),
                })
            })
        }

        fn send_turn(
            &self,
            backend_session_id: &str,
            input: &TurnInput,
        ) -> CrabResult<ClaudeRawEventStream> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.send_calls += 1;
            state.stats.last_backend_session_id = Some(backend_session_id.to_string());
            state.stats.last_turn_input = Some(input.clone());
            let events = state.send_results.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "daemon_test_claude_send",
                    message: "missing scripted send result".to_string(),
                })
            })?;
            Ok(Box::pin(futures::stream::iter(events)))
        }

        fn interrupt_turn(&self, backend_session_id: &str, turn_id: &str) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.interrupt_calls += 1;
            state.stats.last_backend_session_id = Some(backend_session_id.to_string());
            state.stats.last_interrupted_turn_id = Some(turn_id.to_string());
            state.interrupt_results.pop_front().unwrap_or(Ok(()))
        }

        fn end_session(&self, backend_session_id: &str) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.end_calls += 1;
            state.stats.last_ended_backend_session_id = Some(backend_session_id.to_string());
            state.end_results.pop_front().unwrap_or(Ok(()))
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

    fn install_handler_ok(_flag: Arc<AtomicBool>) -> Result<(), String> {
        Ok(())
    }

    fn install_handler_err(_flag: Arc<AtomicBool>) -> Result<(), String> {
        Err("failed to install Ctrl-C handler: test".to_string())
    }

    fn sample_run(sender_id: &str) -> Run {
        Run {
            id: "run-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            physical_session_id: None,
            status: RunStatus::Queued,
            user_input: "hello world".to_string(),
            delivery_channel_id: None,
            profile: RunProfileTelemetry {
                requested_profile: None,
                resolved_profile: InferenceProfile {
                    backend: BackendKind::Claude,
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

    fn claude_profile() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-sonnet".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        }
    }

    fn sample_claude_run(sender_id: &str) -> Run {
        let mut run = sample_run(sender_id);
        run.profile.resolved_profile = claude_profile();
        run
    }

    #[test]
    fn render_crab_runtime_brief_appends_onboarding_guidance_only_for_owner_dm_pending_bootstrap() {
        let mut owner_dm_run = sample_run("424242424242424242");
        owner_dm_run.profile.sender_is_owner = true;
        owner_dm_run.logical_session_id = "discord:dm:424242424242424242".to_string();

        let owner_pending = super::render_crab_runtime_brief(
            &owner_dm_run,
            WorkspaceBootstrapState::PendingBootstrap,
        );
        assert!(owner_pending.contains("You are an AI coding agent running inside Crab"));
        assert!(owner_pending.contains("Onboarding is pending."));
        assert!(owner_pending.contains("- machine timezone"));

        let owner_ready =
            super::render_crab_runtime_brief(&owner_dm_run, WorkspaceBootstrapState::Ready);
        assert!(!owner_ready.contains("Onboarding is pending."));

        let mut owner_channel_run = owner_dm_run.clone();
        owner_channel_run.logical_session_id = "discord:channel:777".to_string();
        let owner_channel_pending = super::render_crab_runtime_brief(
            &owner_channel_run,
            WorkspaceBootstrapState::PendingBootstrap,
        );
        assert!(!owner_channel_pending.contains("Onboarding is pending."));
    }

    fn build_scripted_claude_runtime(
        workspace_label: &str,
        claude_process: ScriptedClaudeProcess,
    ) -> (
        TempWorkspace,
        DaemonTurnRuntime<ScriptedDiscordIo>,
        ScriptedClaudeProcess,
    ) {
        let workspace = TempWorkspace::new("daemon", workspace_label);
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime = DaemonTurnRuntime::new_with_claude_process(
            config.owner.clone(),
            discord,
            claude_process.clone(),
        )
        .expect("runtime should build");
        runtime.configure_turn_context_runtime(
            workspace.path.clone(),
            CheckpointStore::new(workspace.path.join("state")),
        );
        (workspace, runtime, claude_process)
    }

    fn assert_run_usage_note(
        events: &[BackendEvent],
        input_tokens: &str,
        output_tokens: &str,
        total_tokens: &str,
        source: &str,
    ) {
        assert!(events.iter().any(|event| {
            event.kind == BackendEventKind::RunNote
                && event.payload.get("run_usage_input_tokens") == Some(&input_tokens.to_string())
                && event.payload.get("run_usage_output_tokens") == Some(&output_tokens.to_string())
                && event.payload.get("run_usage_total_tokens") == Some(&total_tokens.to_string())
                && event.payload.get("run_usage_source") == Some(&source.to_string())
        }));
    }

    fn sample_session(
        lane_state: LaneState,
        active_physical_session_id: Option<String>,
    ) -> LogicalSession {
        LogicalSession {
            id: "discord:channel:777".to_string(),
            active_backend: BackendKind::Claude,
            active_profile: InferenceProfile {
                backend: BackendKind::Claude,
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
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            has_injected_bootstrap: false,
        }
    }

    #[derive(Debug, Clone, Default)]
    struct SelfWorkTestRuntime;

    impl TurnExecutorRuntime for SelfWorkTestRuntime {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            Err(CrabError::InvariantViolation {
                context: "daemon_self_work_test_runtime",
                message: "clock should not be called in self-work evaluator tests".to_string(),
            })
        }

        fn resolve_run_profile(
            &mut self,
            _logical_session_id: &str,
            _author_id: &str,
            _user_input: &str,
        ) -> CrabResult<RunProfileTelemetry> {
            unreachable!("resolve_run_profile should not be called in self-work evaluator tests")
        }

        fn ensure_physical_session(
            &mut self,
            _logical_session_id: &str,
            _profile: &InferenceProfile,
            _active_physical_session_id: Option<&str>,
        ) -> CrabResult<crab_core::PhysicalSession> {
            unreachable!(
                "ensure_physical_session should not be called in self-work evaluator tests"
            )
        }

        fn build_turn_context(
            &mut self,
            _run: &Run,
            _logical_session: &LogicalSession,
            _physical_session: &crab_core::PhysicalSession,
            _inject_bootstrap_context: bool,
        ) -> CrabResult<String> {
            unreachable!("build_turn_context should not be called in self-work evaluator tests")
        }

        fn execute_backend_turn(
            &mut self,
            _physical_session: &mut crab_core::PhysicalSession,
            _run: &Run,
            _turn_id: &str,
            _turn_context: &str,
        ) -> CrabResult<crab_backends::BackendEventStream> {
            unreachable!("execute_backend_turn should not be called in self-work evaluator tests")
        }

        fn deliver_assistant_output(
            &mut self,
            _run: &Run,
            _channel_id: &str,
            _message_id: &str,
            _edit_generation: u32,
            _content: &str,
        ) -> CrabResult<()> {
            unreachable!(
                "deliver_assistant_output should not be called in self-work evaluator tests"
            )
        }

        fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
            unreachable!("next_gateway_message should not be called in self-work evaluator tests")
        }

        fn interrupt_backend_turn(
            &mut self,
            _session: &crab_core::PhysicalSession,
            _turn_id: &str,
        ) -> CrabResult<()> {
            unreachable!("interrupt_backend_turn should not be called in self-work evaluator tests")
        }
    }

    fn build_self_work_executor(label: &str) -> (TempWorkspace, TurnExecutor<SelfWorkTestRuntime>) {
        let workspace = TempWorkspace::new("daemon", label);
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let composition = compose_runtime_with_queue_limit(&config, "999999999999999999", 8)
            .expect("composition should build");
        let executor = TurnExecutor::new(composition, SelfWorkTestRuntime);
        (workspace, executor)
    }

    fn sample_self_work_session(status: SelfWorkSessionStatus) -> SelfWorkSession {
        SelfWorkSession {
            schema_version: CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
            session_id: "self-work:1739173200000".to_string(),
            channel_id: "777".to_string(),
            goal: "Ship the feature".to_string(),
            started_at_epoch_ms: 1_739_173_200_000,
            started_at_iso8601: "2025-02-10T10:00:00Z".to_string(),
            end_at_epoch_ms: 1_739_173_800_000,
            end_at_iso8601: "2025-02-10T10:10:00Z".to_string(),
            status,
            last_wake_triggered_at_epoch_ms: None,
            final_trigger_pending: false,
            stopped_at_epoch_ms: (status == SelfWorkSessionStatus::Stopped)
                .then_some(1_739_173_500_000),
            expired_at_epoch_ms: (status == SelfWorkSessionStatus::Expired)
                .then_some(1_739_173_800_000),
            last_expiry_triggered_at_epoch_ms: (status == SelfWorkSessionStatus::Expired)
                .then_some(1_739_173_800_000),
        }
    }

    fn self_work_lane_status_fixture(
        persisted_lane_state: Option<LaneState>,
        has_active_run: bool,
        queued_run_count: usize,
        last_activity_epoch_ms: Option<u64>,
    ) -> SelfWorkLaneStatus {
        SelfWorkLaneStatus {
            logical_session_id: "discord:channel:777".to_string(),
            has_active_run,
            queued_run_count,
            persisted_lane_state,
            last_activity_epoch_ms,
        }
    }

    #[test]
    fn resolve_claude_session_strategy_prioritizes_cached_then_restored_then_create() {
        assert_eq!(
            super::resolve_claude_session_strategy(Some("claude:cached"), true),
            super::ClaudeSessionStrategy::ReuseCached {
                active_id: "claude:cached"
            }
        );
        assert_eq!(
            super::resolve_claude_session_strategy(Some("claude:resume-1"), false),
            super::ClaudeSessionStrategy::RestoreFromActiveId {
                active_id: "claude:resume-1",
                backend_session_id: "resume-1",
            }
        );
        assert_eq!(
            super::resolve_claude_session_strategy(Some("physical:missing"), false),
            super::ClaudeSessionStrategy::CreateNew
        );
        assert_eq!(
            super::resolve_claude_session_strategy(None, false),
            super::ClaudeSessionStrategy::CreateNew
        );
    }

    #[test]
    fn claude_stream_parser_maps_text_usage_and_completion() {
        let stream = r#"{"type":"system","subtype":"init","session_id":"session-1"}
{"type":"assistant","message":{"content":[{"type":"text","text":"hello"}],"usage":{"input_tokens":2,"output_tokens":1}}}
{"type":"result","subtype":"success","is_error":false,"result":"hello","usage":{"input_tokens":2,"output_tokens":3}}"#;

        let events = super::parse_claude_stream_lines(stream).expect("claude stream should parse");

        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::TextDelta { text } if text == "hello"
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::Usage {
                    input_tokens: 2,
                    output_tokens: 1,
                    total_tokens: 3,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                }
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::Usage {
                    input_tokens: 2,
                    output_tokens: 3,
                    total_tokens: 5,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                }
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::TurnCompleted { stop_reason } if stop_reason == "completed"
            )
        }));
    }

    #[test]
    fn claude_stream_parser_maps_tool_and_error_events() {
        let stream = r#"{"type":"assistant","message":{"content":[{"type":"tool_use","id":"tool-1","name":"search","input":{"query":"hello"}},{"type":"tool_result","tool_use_id":"tool-1","name":"search","content":"done","is_error":false}]}}
{"type":"result","subtype":"error","is_error":true,"result":"boom"}"#;

        let events = super::parse_claude_stream_lines(stream).expect("claude stream should parse");

        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::ToolCall {
                    tool_call_id,
                    tool_name,
                    ..
                } if tool_call_id == "tool-1" && tool_name == "search"
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::ToolResult {
                    tool_call_id,
                    tool_name,
                    output,
                    is_error
                } if tool_call_id == "tool-1"
                    && tool_name == "search"
                    && output == "done"
                    && !*is_error
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::Error { message } if message == "boom"
            )
        }));
    }

    #[test]
    fn claude_usage_payload_parses_cache_tokens_when_present() {
        let payload: serde_json::Value = serde_json::json!({
            "input_tokens": 3,
            "output_tokens": 5,
            "cache_read_input_tokens": 40000,
            "cache_creation_input_tokens": 7000,
        });
        let usage =
            super::parse_claude_usage_payload(&payload).expect("usage payload should parse");
        assert_eq!(usage.input_tokens, 3);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.total_tokens, 8);
        assert_eq!(usage.cache_read_input_tokens, 40000);
        assert_eq!(usage.cache_creation_input_tokens, 7000);
    }

    #[test]
    fn claude_usage_payload_defaults_cache_tokens_to_zero_when_absent() {
        let payload: serde_json::Value = serde_json::json!({
            "input_tokens": 2,
            "output_tokens": 3,
        });
        let usage =
            super::parse_claude_usage_payload(&payload).expect("usage payload should parse");
        assert_eq!(usage.input_tokens, 2);
        assert_eq!(usage.output_tokens, 3);
        assert_eq!(usage.total_tokens, 5);
        assert_eq!(usage.cache_read_input_tokens, 0);
        assert_eq!(usage.cache_creation_input_tokens, 0);
    }

    #[test]
    fn claude_stream_parser_reports_invalid_assistant_payloads() {
        let stream = r#"{"type":"assistant"}"#;
        let error = super::parse_claude_stream_lines(stream)
            .expect_err("assistant payload missing message should fail parsing");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_claude_stream",
                message: "assistant stream event is missing message payload".to_string(),
            }
        );
    }

    #[test]
    fn claude_stream_parser_reports_invalid_json_payloads() {
        let stream = r#"{"type":"assistant""#;
        let error =
            super::parse_claude_stream_lines(stream).expect_err("invalid json should fail parsing");
        assert!(matches!(
            error,
            CrabError::Serialization {
                context: "daemon_claude_stream",
                path: None,
                message,
            } if message.contains("invalid stream-json event")
        ));
    }

    #[test]
    fn claude_stream_parser_reports_empty_event_stream() {
        let stream = "\n{\"message\":\"ignore\"}\n{\"type\":\"system\"}\n";
        let error = super::parse_claude_stream_lines(stream)
            .expect_err("stream without assistant/result events should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_claude_stream",
                message: "claude stream produced no assistant/result events".to_string(),
            }
        );
    }

    #[test]
    fn claude_stream_parser_covers_content_and_interrupt_edge_paths() {
        let stream = r#"{"type":"assistant","message":{"usage":{"input_tokens":"3","output_tokens":"2"}}}
{"type":"assistant","message":{"content":[{"text":"missing type"},{"type":"tool_use","name":"search","input":{"query":"hello"}},{"type":"tool_result","name":"search","content":"done"},{"type":"other"}]}}
{"type":"assistant","message":{"content":[{"type":"tool_result","tool_use_id":"tool-fallback","text":"fallback text"}]}}
{"type":"result","subtype":"interrupted","is_error":false,"usage":{"input_tokens":"7","output_tokens":"5","total_tokens":"12"}}
{"type":"result","subtype":"error","is_error":true,"result":123}"#;

        let events = super::parse_claude_stream_lines(stream).expect("claude stream should parse");

        let mut has_initial_usage = false;
        for event in &events {
            if let ClaudeRawEvent::Usage {
                input_tokens,
                output_tokens,
                total_tokens,
                ..
            } = event
            {
                if *input_tokens == 3 && *output_tokens == 2 && *total_tokens == 5 {
                    has_initial_usage = true;
                }
            }
        }
        assert!(has_initial_usage);
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::Usage {
                    input_tokens: 7,
                    output_tokens: 5,
                    total_tokens: 12,
                    ..
                }
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::TurnInterrupted { reason } if reason == "interrupted"
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::Error { message }
                    if message == "claude stream reported an unspecified error"
            )
        }));
        assert!(events.iter().any(|event| {
            matches!(
                event,
                ClaudeRawEvent::ToolResult {
                    tool_call_id,
                    tool_name,
                    output,
                    is_error
                } if tool_call_id == "tool-fallback"
                    && tool_name == "tool"
                    && output == "fallback text"
                    && !*is_error
            )
        }));
        assert!(!events
            .iter()
            .any(|event| matches!(event, ClaudeRawEvent::ToolCall { .. })));
    }

    #[test]
    fn daemon_claude_process_default_lifecycle_is_deterministic() {
        let process = DaemonClaudeProcess::default();
        let context = SessionContext {
            logical_session_id: "discord:channel:777".to_string(),
            profile: claude_profile(),
        };
        let backend_session_id = process
            .create_session(&context)
            .expect("default process should create deterministic session id");
        assert_eq!(backend_session_id, "daemon-claude-discord-channel-777");

        let turn_input = TurnInput {
            run_id: "run-default-claude".to_string(),
            turn_id: "turn-1".to_string(),
            user_input: "hello from daemon".to_string(),
        };
        let events = process
            .send_turn(&backend_session_id, &turn_input)
            .expect("default process send_turn should succeed");
        let events = block_on(events.collect::<Vec<_>>());
        assert_eq!(
            events,
            vec![
                ClaudeRawEvent::TextDelta {
                    text: "Claude bridge response".to_string()
                },
                ClaudeRawEvent::Usage {
                    input_tokens: 3,
                    output_tokens: 3,
                    total_tokens: 6,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                },
                ClaudeRawEvent::TurnCompleted {
                    stop_reason: "end_turn".to_string(),
                },
            ]
        );

        let forced_error_input = TurnInput {
            run_id: "run-force-claude-send-error".to_string(),
            turn_id: "turn-2".to_string(),
            user_input: "hello".to_string(),
        };
        let error = process
            .send_turn(&backend_session_id, &forced_error_input)
            .err()
            .expect("forced error token should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_claude_send_turn",
                message: "forced claude send failure".to_string(),
            }
        );

        process
            .interrupt_turn(&backend_session_id, "turn-2")
            .expect("default interrupt should be noop success");
        process
            .end_session(&backend_session_id)
            .expect("default end_session should be noop success");
    }

    #[test]
    fn claude_bridge_debug_impls_are_callable() {
        let shared = super::SharedClaudeProcess {
            inner: Arc::new(DaemonClaudeProcess::default()),
        };
        let shared_debug = format!("{shared:?}");
        assert!(shared_debug.contains("SharedClaudeProcess"));

        let bridge = super::DaemonClaudeExecutionBridge::with_process(Arc::new(
            DaemonClaudeProcess::default(),
        ));
        let bridge_debug = format!("{bridge:?}");
        assert!(bridge_debug.contains("DaemonClaudeExecutionBridge"));
    }

    #[test]
    fn scripted_claude_process_surfaces_missing_scripted_values_and_lifecycle_errors() {
        let process = ScriptedClaudeProcess::with_scripted(
            Vec::new(),
            Vec::new(),
            vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_claude_interrupt",
                message: "forced interrupt failure".to_string(),
            })],
            vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_claude_end",
                message: "forced end failure".to_string(),
            })],
        );

        let context = SessionContext {
            logical_session_id: "discord:channel:777".to_string(),
            profile: claude_profile(),
        };
        let create_error = process
            .create_session(&context)
            .expect_err("missing create script should return deterministic error");
        assert_eq!(
            create_error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_create",
                message: "missing scripted create result".to_string(),
            }
        );

        let send_error = process
            .send_turn(
                "backend-session-1",
                &TurnInput {
                    run_id: "run-1".to_string(),
                    turn_id: "turn-1".to_string(),
                    user_input: "hello".to_string(),
                },
            )
            .err()
            .expect("missing send script should return deterministic error");
        assert_eq!(
            send_error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_send",
                message: "missing scripted send result".to_string(),
            }
        );

        let interrupt_error = process
            .interrupt_turn("backend-session-1", "turn-1")
            .expect_err("scripted interrupt error should surface");
        assert_eq!(
            interrupt_error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_interrupt",
                message: "forced interrupt failure".to_string(),
            }
        );

        let end_error = process
            .end_session("backend-session-1")
            .expect_err("scripted end error should surface");
        assert_eq!(
            end_error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_end",
                message: "forced end failure".to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_claude_bridge_executes_lifecycle_and_usage_flow() {
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Ok("resume-1".to_string())],
            vec![Ok(vec![
                ClaudeRawEvent::TextDelta {
                    text: "Claude says hi".to_string(),
                },
                ClaudeRawEvent::Usage {
                    input_tokens: 5,
                    output_tokens: 7,
                    total_tokens: 12,
                    cache_read_input_tokens: 0,
                    cache_creation_input_tokens: 0,
                },
                ClaudeRawEvent::TurnCompleted {
                    stop_reason: "done".to_string(),
                },
            ])],
            Vec::new(),
            Vec::new(),
        );
        let (_workspace, mut runtime, claude_process) =
            build_scripted_claude_runtime("claude-bridge-lifecycle", claude_process);

        let created = runtime
            .ensure_physical_session(
                "discord:channel:777",
                &claude_profile(),
                Some("physical:missing"),
            )
            .expect("missing non-claude id should create via claude bridge");
        assert_eq!(created.id, "claude:resume-1");
        assert_eq!(created.backend_session_id, "resume-1");
        assert_eq!(created.backend, BackendKind::Claude);

        let reused = runtime
            .ensure_physical_session("discord:channel:777", &claude_profile(), Some(&created.id))
            .expect("active claude id should reuse existing session");
        assert_eq!(reused.id, created.id);

        let run = sample_claude_run("123");
        let mut physical = reused.clone();
        let events = runtime
            .execute_backend_turn(&mut physical, &run, "turn-1", "compiled context")
            .expect("claude bridge execution should succeed");
        let events = block_on(events.collect::<Vec<_>>());
        assert_run_usage_note(&events, "5", "7", "12", "claude");
        assert_eq!(physical.last_turn_id.as_deref(), Some("turn-1"));

        let stats = claude_process.stats();
        assert_eq!(stats.create_calls, 1);
        assert_eq!(stats.send_calls, 1);
        assert_eq!(stats.last_backend_session_id.as_deref(), Some("resume-1"));
    }

    #[test]
    fn daemon_runtime_claude_bridge_restores_session_from_active_id_shape() {
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_claude_create",
                message: "create should not be called when active id contains claude prefix"
                    .to_string(),
            })],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        );
        let (_workspace, mut runtime, claude_process) =
            build_scripted_claude_runtime("claude-session-restore", claude_process);

        let restored = runtime
            .ensure_physical_session(
                "discord:channel:777",
                &claude_profile(),
                Some("claude:resume-from-store"),
            )
            .expect("active claude id should restore from stored shape");
        assert_eq!(restored.id, "claude:resume-from-store");
        assert_eq!(restored.backend_session_id, "resume-from-store");
        assert_eq!(claude_process.stats().create_calls, 0);
    }

    #[test]
    fn daemon_runtime_claude_bridge_surfaces_interruption_and_error_events() {
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Ok("resume-2".to_string())],
            vec![
                Ok(vec![ClaudeRawEvent::TurnInterrupted {
                    reason: "operator requested stop".to_string(),
                }]),
                Ok(vec![ClaudeRawEvent::Error {
                    message: "backend execution failed".to_string(),
                }]),
                Err(CrabError::InvariantViolation {
                    context: "daemon_test_claude_send",
                    message: "forced send failure".to_string(),
                }),
            ],
            Vec::new(),
            Vec::new(),
        );
        let (_workspace, mut runtime, _claude_process) =
            build_scripted_claude_runtime("claude-interrupt-error", claude_process);
        let run = sample_claude_run("123");
        let mut session = runtime
            .ensure_physical_session("discord:channel:777", &claude_profile(), None)
            .expect("claude session should be created");

        let interrupted = runtime
            .execute_backend_turn(&mut session, &run, "turn-1", "context one")
            .expect("interrupted stream should still surface events");
        let interrupted = block_on(interrupted.collect::<Vec<_>>());
        assert!(interrupted
            .iter()
            .any(|event| event.kind == BackendEventKind::TurnInterrupted));
        assert_eq!(session.last_turn_id.as_deref(), Some("turn-1"));

        let errored_event = runtime
            .execute_backend_turn(&mut session, &run, "turn-2", "context two")
            .expect("error events should still be emitted as backend events");
        let errored_event = block_on(errored_event.collect::<Vec<_>>());
        assert!(errored_event.iter().any(|event| {
            event.kind == BackendEventKind::Error
                && event.payload.get("message") == Some(&"backend execution failed".to_string())
        }));
        assert_eq!(session.last_turn_id.as_deref(), Some("turn-2"));

        let send_error = runtime
            .execute_backend_turn(&mut session, &run, "turn-3", "context three")
            .err()
            .expect("send errors should propagate through execute_backend_turn");
        assert_eq!(
            send_error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_send",
                message: "forced send failure".to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_default_claude_process_exercises_claude_runtime_path() {
        let workspace = TempWorkspace::new("daemon", "default-claude-runtime-path");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime should build");

        let mut session = runtime
            .ensure_physical_session("discord:channel:777", &claude_profile(), None)
            .expect("default Claude process should create a Claude-backed physical session");
        assert_eq!(session.id, "claude:daemon-claude-discord-channel-777");
        assert_eq!(session.backend, BackendKind::Claude);

        let run = sample_claude_run("123");

        let events = runtime
            .execute_backend_turn(&mut session, &run, "turn-default", "default context")
            .expect("default Claude process should execute through bridge");
        let events = block_on(events.collect::<Vec<_>>());
        assert_run_usage_note(&events, "2", "3", "5", "claude");

        let mut forced_error_run = sample_claude_run("123");
        forced_error_run.id = "run-force-claude-send-error".to_string();
        let error = runtime
            .execute_backend_turn(&mut session, &forced_error_run, "turn-error", "context")
            .err()
            .expect("forced send failure should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_claude_send_turn",
                message: "forced claude send failure".to_string(),
            }
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
    fn notify_startup_recovered_runs_skips_when_run_is_missing() {
        let workspace = TempWorkspace::new("daemon", "startup-reconcile-missing-run");
        let state_root = workspace.path.join("state");
        let run_store = RunStore::new(state_root);

        let recovered = crab_core::startup_reconciliation::StartupReconciliationRecoveredRun {
            logical_session_id: "discord:dm:111".to_string(),
            run_id: "run:missing".to_string(),
            previous_status: RunStatus::Running,
            recovered_status: RunStatus::Cancelled,
        };

        let mut discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let sent = notify_startup_recovered_runs(&run_store, &[recovered], &mut discord)
            .expect("notification attempt should not fail");

        assert_eq!(sent, 0);
        assert!(discord.state().posted.is_empty());
    }

    #[test]
    fn notify_startup_recovered_runs_skips_when_delivery_channel_is_missing() {
        let workspace = TempWorkspace::new("daemon", "startup-reconcile-missing-channel");
        let state_root = workspace.path.join("state");
        let run_store = RunStore::new(state_root);

        let mut run = sample_claude_run("111");
        run.id = "run:discord:dm:111:stale-missing-channel".to_string();
        run.logical_session_id = "discord:dm:111".to_string();
        run.status = RunStatus::Cancelled;
        run.delivery_channel_id = None;
        run_store.upsert_run(&run).expect("seed run should persist");

        let recovered = crab_core::startup_reconciliation::StartupReconciliationRecoveredRun {
            logical_session_id: run.logical_session_id.clone(),
            run_id: run.id.clone(),
            previous_status: RunStatus::Running,
            recovered_status: RunStatus::Cancelled,
        };

        let mut discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let sent = notify_startup_recovered_runs(&run_store, &[recovered], &mut discord)
            .expect("notification attempt should not fail");

        assert_eq!(sent, 0);
        assert!(discord.state().posted.is_empty());
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
        assert_eq!(telemetry.resolved_profile.backend, BackendKind::Claude);
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
            .build_turn_context(&run, &session, &physical, true)
            .expect("context build should succeed");
        assert_eq!(context, run.user_input);
    }

    #[test]
    fn daemon_runtime_build_turn_context_skips_bootstrap_injection_when_not_requested() {
        let workspace = TempWorkspace::new("daemon", "context-non-bootstrap");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        std::fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        std::fs::create_dir_all(workspace.path.join("IDENTITY.md"))
            .expect("IDENTITY.md directory should be creatable");
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        let checkpoint_store = CheckpointStore::new(&state_root);

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime =
            DaemonTurnRuntime::new(config.owner.clone(), discord).expect("runtime builds");
        runtime.configure_turn_context_runtime(workspace.path.clone(), checkpoint_store);

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
            .build_turn_context(&run, &session, &physical, false)
            .expect("non-bootstrap context build should bypass workspace reads");
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
            .build_turn_context(&run, &session, &physical, true)
            .expect("context should render");

        assert!(context.contains("<crab_turn_context>"));
        assert!(context.contains("<crab_system_context>"));
        assert!(context.contains("<soul_md>"));
        assert!(context.contains("Soul profile"));
        assert!(context.contains("## RUNTIME_PROFILE"));
        assert!(context.contains("<prompt_contract>"));
        assert!(context.contains("memory/users/123/2026-02-10.md"));
        assert!(context.contains("checkpoint_id: ckpt-1"));
        assert!(context.contains("<crab_user_input>"));
        assert!(context.contains("<turn_input>"));
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
            .build_turn_context(&run, &session, &physical, true)
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
            backend: BackendKind::Claude,
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
    fn daemon_runtime_shutdown_claude_sessions_runs_interrupt_and_end_lifecycle() {
        let workspace = TempWorkspace::new("daemon", "claude-shutdown-lifecycle");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Ok("resume-cleanup".to_string())],
            vec![Ok(vec![ClaudeRawEvent::TurnCompleted {
                stop_reason: "done".to_string(),
            }])],
            Vec::new(),
            Vec::new(),
        );
        let mut runtime = DaemonTurnRuntime::new_with_claude_process(
            config.owner.clone(),
            discord,
            claude_process.clone(),
        )
        .expect("runtime should build");
        let run = sample_claude_run("123");
        let mut session = runtime
            .ensure_physical_session("discord:channel:777", &claude_profile(), None)
            .expect("claude session should be created");

        let events = runtime
            .execute_backend_turn(&mut session, &run, "turn-cleanup", "cleanup context")
            .expect("claude turn should succeed before shutdown");
        let _ = block_on(events.collect::<Vec<_>>());
        assert_eq!(session.last_turn_id.as_deref(), Some("turn-cleanup"));

        runtime
            .shutdown_claude_sessions()
            .expect("shutdown should interrupt and end Claude sessions");

        let stats = claude_process.stats();
        assert_eq!(stats.interrupt_calls, 1);
        assert_eq!(stats.end_calls, 1);
        assert_eq!(
            stats.last_interrupted_turn_id.as_deref(),
            Some("turn-cleanup")
        );
        assert_eq!(
            stats.last_ended_backend_session_id.as_deref(),
            Some("resume-cleanup")
        );
    }

    #[test]
    fn daemon_runtime_shutdown_claude_sessions_propagates_interrupt_errors() {
        let workspace = TempWorkspace::new("daemon", "claude-shutdown-interrupt-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Ok("resume-interrupt".to_string())],
            vec![Ok(vec![ClaudeRawEvent::TurnCompleted {
                stop_reason: "done".to_string(),
            }])],
            vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_claude_interrupt",
                message: "forced interrupt failure".to_string(),
            })],
            Vec::new(),
        );
        let mut runtime = DaemonTurnRuntime::new_with_claude_process(
            config.owner.clone(),
            discord,
            claude_process,
        )
        .expect("runtime should build");
        let run = sample_claude_run("123");
        let mut session = runtime
            .ensure_physical_session("discord:channel:777", &claude_profile(), None)
            .expect("claude session should be created");

        let events = runtime
            .execute_backend_turn(&mut session, &run, "turn-cleanup", "cleanup context")
            .expect("claude turn should succeed before shutdown");
        let _ = block_on(events.collect::<Vec<_>>());

        let error = runtime
            .shutdown_claude_sessions()
            .expect_err("interrupt errors should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_interrupt",
                message: "forced interrupt failure".to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_shutdown_claude_sessions_propagates_end_errors() {
        let workspace = TempWorkspace::new("daemon", "claude-shutdown-end-error");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Ok("resume-end".to_string())],
            vec![Ok(vec![ClaudeRawEvent::TurnCompleted {
                stop_reason: "done".to_string(),
            }])],
            Vec::new(),
            vec![Err(CrabError::InvariantViolation {
                context: "daemon_test_claude_end",
                message: "forced end failure".to_string(),
            })],
        );
        let mut runtime = DaemonTurnRuntime::new_with_claude_process(
            config.owner.clone(),
            discord,
            claude_process,
        )
        .expect("runtime should build");
        let run = sample_claude_run("123");
        let mut session = runtime
            .ensure_physical_session("discord:channel:777", &claude_profile(), None)
            .expect("claude session should be created");

        let events = runtime
            .execute_backend_turn(&mut session, &run, "turn-cleanup", "cleanup context")
            .expect("claude turn should succeed before shutdown");
        let _ = block_on(events.collect::<Vec<_>>());

        let error = runtime
            .shutdown_claude_sessions()
            .expect_err("end_session errors should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_test_claude_end",
                message: "forced end failure".to_string(),
            }
        );
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
    fn interrupt_backend_turn_delegates_to_claude_process_for_claude_backend() {
        let claude_process = ScriptedClaudeProcess::with_scripted(
            vec![Ok("claude-session-1".to_string())],
            Vec::new(),
            vec![Ok(())],
            Vec::new(),
        );
        let (_workspace, mut runtime, process) =
            build_scripted_claude_runtime("interrupt-claude-backend", claude_process);

        let session = crab_core::PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            backend: BackendKind::Claude,
            backend_session_id: "claude-session-1".to_string(),
            created_at_epoch_ms: 1_000_000,
            last_turn_id: None,
        };

        runtime
            .interrupt_backend_turn(&session, "turn-1")
            .expect("interrupt should succeed for Claude backend");

        let stats = process.stats();
        assert_eq!(stats.interrupt_calls, 1);
        assert_eq!(stats.last_interrupted_turn_id, Some("turn-1".to_string()));
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

    #[test]
    fn notify_startup_recovered_runs_sends_discord_messages() {
        let workspace = TempWorkspace::new("daemon", "startup-reconcile-happy-path");
        let state_root = workspace.path.join("state");
        let run_store = RunStore::new(&state_root);

        let mut run = sample_claude_run("111");
        run.id = "run:discord:dm:111:stale-with-channel".to_string();
        run.logical_session_id = "discord:dm:111".to_string();
        run.status = RunStatus::Cancelled;
        run.delivery_channel_id = Some("999".to_string());
        run_store.upsert_run(&run).expect("seed run should persist");

        let recovered = crab_core::startup_reconciliation::StartupReconciliationRecoveredRun {
            logical_session_id: run.logical_session_id.clone(),
            run_id: run.id.clone(),
            previous_status: RunStatus::Running,
            recovered_status: RunStatus::Cancelled,
        };

        let mut discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let sent = notify_startup_recovered_runs(&run_store, &[recovered], &mut discord)
            .expect("notification should succeed");

        assert_eq!(sent, 1);
        let state = discord.state();
        assert_eq!(state.posted.len(), 1);
        assert_eq!(state.posted[0].0, "999");
        assert!(state.posted[0].1.starts_with("startup:recovered:"));
        assert!(state.posted[0].2.contains("Crab restarted"));
        assert!(state.posted[0].2.contains(&run.id));
    }

    #[test]
    fn civil_from_days_covers_january() {
        // 1704067200000 ms = 2024-01-01T00:00:00Z
        let date = epoch_ms_to_yyyy_mm_dd(1_704_067_200_000).expect("January date should convert");
        assert_eq!(date, "2024-01-01");
    }

    #[test]
    fn civil_from_days_covers_february() {
        // 1706745600000 ms = 2024-02-01T00:00:00Z
        let date = epoch_ms_to_yyyy_mm_dd(1_706_745_600_000).expect("February date should convert");
        assert_eq!(date, "2024-02-01");
    }

    #[test]
    fn scripted_discord_io_next_gateway_message_returns_queued_messages() {
        let msg1 = GatewayMessage {
            message_id: "m1".to_string(),
            author_id: "222".to_string(),
            author_is_bot: false,
            channel_id: "111".to_string(),
            guild_id: None,
            thread_id: None,
            content: "hello".to_string(),
            conversation_kind: crab_discord::GatewayConversationKind::GuildChannel,
            attachments: Vec::new(),
        };
        let msg2 = GatewayMessage {
            message_id: "m2".to_string(),
            author_id: "444".to_string(),
            author_is_bot: false,
            channel_id: "333".to_string(),
            guild_id: None,
            thread_id: None,
            content: "world".to_string(),
            conversation_kind: crab_discord::GatewayConversationKind::GuildChannel,
            attachments: Vec::new(),
        };
        let mut discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(msg1.clone())), Ok(Some(msg2.clone()))]),
            ..DiscordIoState::default()
        });

        let first = discord
            .next_gateway_message()
            .expect("first call should succeed")
            .expect("first message should be present");
        assert_eq!(first, msg1);

        let second = discord
            .next_gateway_message()
            .expect("second call should succeed")
            .expect("second message should be present");
        assert_eq!(second, msg2);

        let empty = discord
            .next_gateway_message()
            .expect("drained call should succeed");
        assert!(empty.is_none());
    }

    #[test]
    fn evaluate_self_work_emits_wake_trigger_after_idle_delay() {
        let (workspace, mut executor) = build_self_work_executor("self-work-wake");
        let state_root = workspace.path.join("state");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("self-work session should persist");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_380_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 1);

        let triggers = read_pending_triggers(&state_root).expect("triggers should be readable");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].1.channel_id, "777");
        assert!(triggers[0].1.message.contains("event: wake"));

        let session = read_self_work_session(&state_root)
            .expect("session should be readable")
            .expect("session should exist");
        assert_eq!(
            session.last_wake_triggered_at_epoch_ms,
            Some(1_739_173_380_000)
        );
        assert_eq!(session.status, SelfWorkSessionStatus::Active);
    }

    #[test]
    fn self_work_test_runtime_now_epoch_ms_returns_expected_error() {
        let mut runtime = SelfWorkTestRuntime;
        let error = runtime
            .now_epoch_ms()
            .expect_err("self-work test runtime clock should not be used");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_self_work_test_runtime",
                message: "clock should not be called in self-work evaluator tests".to_string(),
            }
        );
    }

    #[test]
    fn self_work_test_runtime_unreachable_methods_panic_when_called() {
        let mut runtime = SelfWorkTestRuntime;
        let run = sample_run("123");
        let logical_session = sample_session(LaneState::Idle, None);
        let mut physical_session = crab_core::PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: logical_session.id.clone(),
            backend: BackendKind::Claude,
            backend_session_id: "claude-session-1".to_string(),
            created_at_epoch_ms: 1_739_173_200_000,
            last_turn_id: None,
        };

        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.resolve_run_profile("discord:channel:777", "123", "hello");
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.ensure_physical_session(
                "discord:channel:777",
                &claude_profile(),
                Some("physical-1"),
            );
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.build_turn_context(&run, &logical_session, &physical_session, false);
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.execute_backend_turn(&mut physical_session, &run, "turn-1", "context");
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.deliver_assistant_output(&run, "777", "m1", 0, "content");
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.next_gateway_message();
        }))
        .is_err());
        assert!(catch_unwind(AssertUnwindSafe(|| {
            let _ = runtime.interrupt_backend_turn(&physical_session, "turn-1");
        }))
        .is_err());
    }

    #[test]
    fn self_work_helpers_cover_idle_state_baseline_and_lock_error_paths() {
        assert!(self_work_lane_is_idle(&self_work_lane_status_fixture(
            None, false, 0, None,
        )));
        assert!(!self_work_lane_is_idle(&self_work_lane_status_fixture(
            Some(LaneState::Running),
            false,
            0,
            None,
        )));
        assert!(!self_work_lane_is_idle(&self_work_lane_status_fixture(
            Some(LaneState::Idle),
            true,
            0,
            None,
        )));
        assert!(!self_work_lane_is_idle(&self_work_lane_status_fixture(
            Some(LaneState::Idle),
            false,
            1,
            None,
        )));

        let mut session = sample_self_work_session(SelfWorkSessionStatus::Active);
        session.last_wake_triggered_at_epoch_ms = Some(1_739_173_350_000);
        let idle_lane =
            self_work_lane_status_fixture(Some(LaneState::Idle), false, 0, Some(1_739_173_360_000));
        let baseline = self_work_idle_baseline_epoch_ms(&session, &idle_lane);
        assert_eq!(baseline, 1_739_173_360_000);
        assert!(wake_due(&session, &idle_lane, 1_739_173_540_000, 180_000));
        assert!(!wake_due(&session, &idle_lane, 1_739_173_539_999, 180_000));

        let mut pending_session = session.clone();
        pending_session.final_trigger_pending = true;
        assert!(!wake_due(
            &pending_session,
            &idle_lane,
            1_739_173_540_000,
            180_000,
        ));
        assert!(!wake_due(
            &sample_self_work_session(SelfWorkSessionStatus::Stopped),
            &idle_lane,
            1_739_173_540_000,
            180_000,
        ));

        let workspace = TempWorkspace::new("daemon", "self-work-lock-error");
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        std::fs::create_dir_all(state_root.join(crab_core::SELF_WORK_SESSION_LOCK_FILE_NAME))
            .expect("lock path directory should be creatable");

        let error = try_acquire_self_work_lock_for_daemon(&state_root, 1_739_173_380_000)
            .expect_err("non-invariant lock acquisition errors should surface");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "self_work_session_lock",
                ..
            }
        ));
    }

    #[test]
    fn evaluate_self_work_skips_when_final_trigger_is_already_pending() {
        let (workspace, mut executor) = build_self_work_executor("self-work-pending");
        let state_root = workspace.path.join("state");
        let mut session = sample_self_work_session(SelfWorkSessionStatus::Active);
        session.final_trigger_pending = true;
        write_self_work_session_atomically(&state_root, &session)
            .expect("self-work session should persist");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_380_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 0);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());
    }

    #[test]
    fn evaluate_self_work_suppresses_wake_when_lane_has_queued_run() {
        let (workspace, mut executor) = build_self_work_executor("self-work-queued");
        let state_root = workspace.path.join("state");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("self-work session should persist");

        executor
            .composition_mut()
            .scheduler
            .enqueue(
                "discord:channel:777",
                QueuedRun {
                    run_id: "queued-run".to_string(),
                },
            )
            .expect("queued run should be accepted");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_380_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 0);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());
    }

    #[test]
    fn evaluate_self_work_suppresses_wake_when_lane_has_active_run() {
        let (workspace, mut executor) = build_self_work_executor("self-work-active");
        let state_root = workspace.path.join("state");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("self-work session should persist");

        executor
            .composition_mut()
            .scheduler
            .enqueue(
                "discord:channel:777",
                QueuedRun {
                    run_id: "active-run".to_string(),
                },
            )
            .expect("queued run should be accepted");
        let dispatched = executor
            .composition_mut()
            .scheduler
            .try_dispatch_next()
            .expect("queued run should dispatch");
        assert_eq!(dispatched.logical_session_id, "discord:channel:777");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_380_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 0);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());
    }

    #[test]
    fn evaluate_self_work_uses_last_wake_to_suppress_restart_refire() {
        let (workspace, mut executor) = build_self_work_executor("self-work-restart");
        let state_root = workspace.path.join("state");
        let mut session = sample_self_work_session(SelfWorkSessionStatus::Active);
        session.last_wake_triggered_at_epoch_ms = Some(1_739_173_300_000);
        write_self_work_session_atomically(&state_root, &session)
            .expect("self-work session should persist");

        let early = evaluate_self_work(&mut executor, 1_739_173_479_999, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(early, 0);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());

        let later = evaluate_self_work(&mut executor, 1_739_173_480_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(later, 1);
        let triggers = read_pending_triggers(&state_root).expect("triggers should be readable");
        assert_eq!(triggers.len(), 1);
        assert!(triggers[0].1.message.contains("event: wake"));
    }

    #[test]
    fn evaluate_self_work_ignores_stopped_session() {
        let (workspace, mut executor) = build_self_work_executor("self-work-stopped");
        let state_root = workspace.path.join("state");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Stopped),
        )
        .expect("self-work session should persist");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_900_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 0);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());
    }

    #[test]
    fn evaluate_self_work_skips_when_lock_is_held() {
        let (workspace, mut executor) = build_self_work_executor("self-work-lock-held");
        let state_root = workspace.path.join("state");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("self-work session should persist");
        let _lock = crab_core::SelfWorkSessionLock::acquire(&state_root, 1_739_173_379_999)
            .expect("lock should be acquirable");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_380_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 0);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());
    }

    #[test]
    fn evaluate_self_work_expiry_returns_zero_when_lock_is_held() {
        let (workspace, mut executor) = build_self_work_executor("self-work-expiry-lock-held");
        let state_root = workspace.path.join("state");
        let _lock = crab_core::SelfWorkSessionLock::acquire(&state_root, 1_739_173_799_999)
            .expect("lock should be acquirable");

        let emitted = evaluate_self_work_expiry(&mut executor, &state_root, 1_739_173_800_000)
            .expect("expiry evaluation should succeed");
        assert_eq!(emitted, 0);
    }

    #[test]
    fn evaluate_self_work_expiry_returns_zero_when_session_is_missing() {
        let (workspace, mut executor) = build_self_work_executor("self-work-expiry-missing");
        let state_root = workspace.path.join("state");

        let emitted = evaluate_self_work_expiry(&mut executor, &state_root, 1_739_173_800_000)
            .expect("expiry evaluation should succeed");
        assert_eq!(emitted, 0);
    }

    #[test]
    fn evaluate_self_work_expiry_ignores_non_active_and_pre_end_sessions() {
        let (stopped_workspace, mut stopped_executor) =
            build_self_work_executor("self-work-expiry-stopped");
        let stopped_state_root = stopped_workspace.path.join("state");
        write_self_work_session_atomically(
            &stopped_state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Stopped),
        )
        .expect("stopped session should persist");

        let stopped = evaluate_self_work_expiry(
            &mut stopped_executor,
            &stopped_state_root,
            1_739_173_800_000,
        )
        .expect("expiry evaluation should succeed");
        assert_eq!(stopped, 0);

        let (future_workspace, mut future_executor) =
            build_self_work_executor("self-work-expiry-future");
        let future_state_root = future_workspace.path.join("state");
        write_self_work_session_atomically(
            &future_state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("active session should persist");

        let before_end =
            evaluate_self_work_expiry(&mut future_executor, &future_state_root, 1_739_173_799_999)
                .expect("expiry evaluation should succeed");
        assert_eq!(before_end, 0);
    }

    #[test]
    fn evaluate_self_work_marks_expiry_pending_then_emits_once_when_lane_is_idle() {
        let (workspace, mut executor) = build_self_work_executor("self-work-expiry");
        let state_root = workspace.path.join("state");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("self-work session should persist");

        let mut running_session = sample_session(LaneState::Running, None);
        running_session.id = "discord:channel:777".to_string();
        executor
            .composition()
            .state_stores
            .session_store
            .upsert_session(&running_session)
            .expect("logical session should persist");
        executor
            .composition_mut()
            .scheduler
            .enqueue(
                "discord:channel:777",
                QueuedRun {
                    run_id: "run-expiry".to_string(),
                },
            )
            .expect("queued run should be accepted");
        let dispatched = executor
            .composition_mut()
            .scheduler
            .try_dispatch_next()
            .expect("queued run should dispatch");
        assert_eq!(dispatched.logical_session_id, "discord:channel:777");

        let busy = evaluate_self_work(&mut executor, 1_739_173_800_000, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(busy, 0);
        let pending_session = read_self_work_session(&state_root)
            .expect("session should be readable")
            .expect("session should exist");
        assert!(pending_session.final_trigger_pending);
        assert_eq!(pending_session.status, SelfWorkSessionStatus::Active);
        assert!(read_pending_triggers(&state_root)
            .expect("triggers should be readable")
            .is_empty());

        executor
            .composition_mut()
            .scheduler
            .complete_lane("discord:channel:777")
            .expect("lane should complete");
        let mut idle_session = executor
            .composition()
            .state_stores
            .session_store
            .get_session("discord:channel:777")
            .expect("logical session read should succeed")
            .expect("logical session should exist");
        idle_session.lane_state = LaneState::Idle;
        executor
            .composition()
            .state_stores
            .session_store
            .upsert_session(&idle_session)
            .expect("logical session should persist");

        let emitted = evaluate_self_work(&mut executor, 1_739_173_800_001, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(emitted, 1);

        let triggers = read_pending_triggers(&state_root).expect("triggers should be readable");
        assert_eq!(triggers.len(), 1);
        assert!(triggers[0].1.message.contains("event: expiry"));

        let expired_session = read_self_work_session(&state_root)
            .expect("session should be readable")
            .expect("session should exist");
        assert_eq!(expired_session.status, SelfWorkSessionStatus::Expired);
        assert!(!expired_session.final_trigger_pending);
        assert_eq!(expired_session.expired_at_epoch_ms, Some(1_739_173_800_001));
        assert_eq!(
            expired_session.last_expiry_triggered_at_epoch_ms,
            Some(1_739_173_800_001)
        );

        let repeated = evaluate_self_work(&mut executor, 1_739_173_800_002, 180_000)
            .expect("self-work evaluation should succeed");
        assert_eq!(repeated, 0);
        assert_eq!(
            read_pending_triggers(&state_root)
                .expect("triggers should be readable")
                .len(),
            1
        );
    }

    // ---- Daemon loop integration tests ----

    struct OneShotControl {
        now: u64,
        shutdown: bool,
    }

    impl DaemonLoopControl for OneShotControl {
        fn now_epoch_ms(&mut self) -> CrabResult<u64> {
            Ok(self.now)
        }
        fn should_shutdown(&self) -> bool {
            self.shutdown
        }
        fn sleep_tick(&mut self, _tick_interval_ms: u64) -> CrabResult<()> {
            Ok(())
        }
    }

    fn daemon_loop_config() -> DaemonConfig {
        DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(1),
        }
    }

    #[cfg(unix)]
    #[test]
    fn daemon_instance_lock_rejects_second_holder_for_same_state_root() {
        let workspace = TempWorkspace::new("daemon", "instance-lock");
        let state_root = workspace.path.join("state");

        let first_lock =
            DaemonInstanceLock::acquire_at(&state_root).expect("first lock should succeed");
        let second = DaemonInstanceLock::acquire_at(&state_root)
            .expect_err("second lock should fail while first is held");
        assert!(matches!(
            second,
            CrabError::Io {
                context: "crabd_instance_lock",
                ..
            }
        ));

        drop(first_lock);

        DaemonInstanceLock::acquire_at(&state_root)
            .expect("lock should be reacquirable after the first holder drops");
    }

    #[test]
    fn daemon_instance_lock_reports_state_root_create_error() {
        let workspace = TempWorkspace::new("daemon", "instance-lock-root-error");
        let state_root = workspace.path.join("state");
        fs::create_dir_all(
            state_root
                .parent()
                .expect("state root should have a parent directory"),
        )
        .expect("state root parent should be creatable");
        fs::write(&state_root, b"not-a-directory")
            .expect("test should be able to block state root creation");

        let error = DaemonInstanceLock::acquire_at(&state_root)
            .expect_err("state root creation should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "crabd_instance_lock",
                path: Some(path),
                ..
            } if path == state_root.display().to_string()
        ));
    }

    #[test]
    fn daemon_instance_lock_reports_lock_file_open_error() {
        let workspace = TempWorkspace::new("daemon", "instance-lock-open-error");
        let state_root = workspace.path.join("state");
        let lock_path = state_root.join(CRABD_INSTANCE_LOCK_FILE_NAME);
        fs::create_dir_all(&lock_path)
            .expect("test should be able to replace lock file path with a directory");

        let error =
            DaemonInstanceLock::acquire_at(&state_root).expect_err("lock file open should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "crabd_instance_lock",
                path: Some(path),
                ..
            } if path == lock_path.display().to_string()
        ));
    }

    #[cfg(unix)]
    #[test]
    fn daemon_instance_lock_reports_non_blocking_lock_system_errors() {
        let workspace = TempWorkspace::new("daemon", "instance-lock-flock-error");
        let lock_path = workspace
            .path
            .join("state")
            .join(CRABD_INSTANCE_LOCK_FILE_NAME);
        let mut file_descriptors = [0; 2];
        let result = unsafe { libc::pipe(file_descriptors.as_mut_ptr()) };
        assert_eq!(result, 0, "pipe creation should succeed");

        let read_end = unsafe { fs::File::from_raw_fd(file_descriptors[0]) };
        let write_end = unsafe { fs::File::from_raw_fd(file_descriptors[1]) };
        drop(write_end);

        let error = acquire_lock_on_file(&read_end, &lock_path)
            .expect_err("flock on a pipe should surface a system error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "crabd_instance_lock",
                path: Some(path),
                ref message,
            } if path == lock_path.display().to_string()
                && message != "another crabd instance is already running for this state directory"
        ));
    }

    #[test]
    fn daemon_loop_runs_single_iteration_with_empty_state() {
        let workspace = TempWorkspace::new("daemon", "loop-empty");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed with empty state");

        assert_eq!(stats.iterations, 1);
        assert_eq!(stats.ingested_messages, 0);
        assert_eq!(stats.ingested_triggers, 0);
    }

    #[test]
    fn daemon_loop_exits_on_shutdown_signal() {
        let workspace = TempWorkspace::new("daemon", "loop-shutdown");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());

        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: true,
        };

        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: None, // no iteration limit - shutdown signal should stop it
        };

        let stats =
            super::run_daemon_loop_with_transport(&config, &daemon_config, discord, &mut control)
                .expect("daemon loop should exit cleanly on shutdown");

        assert_eq!(stats.iterations, 0);
    }

    #[test]
    fn daemon_loop_ingests_gateway_messages() {
        let workspace = TempWorkspace::new("daemon", "loop-gateway");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let msg = GatewayMessage {
            message_id: "m1".to_string(),
            author_id: "222".to_string(),
            author_is_bot: false,
            channel_id: "333".to_string(),
            guild_id: None,
            thread_id: None,
            content: "hello".to_string(),
            conversation_kind: crab_discord::GatewayConversationKind::DirectMessage,
            attachments: Vec::new(),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(msg))]),
            ..DiscordIoState::default()
        });
        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        assert_eq!(stats.iterations, 1);
        assert_eq!(stats.ingested_messages, 1);
    }

    #[test]
    fn daemon_loop_ingests_trigger_and_handles_trigger_error() {
        let workspace = TempWorkspace::new("daemon", "loop-triggers");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");

        // Good trigger
        crab_core::write_pending_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "888".to_string(),
                message: "good trigger".to_string(),
            },
        )
        .expect("trigger should be written");

        // Write a trigger file directly with a blank channel_id to bypass write
        // validation and exercise the enqueue error branch in the daemon loop.
        let triggers_dir = state_root.join("pending_triggers");
        std::fs::create_dir_all(&triggers_dir).expect("triggers dir should be creatable");
        std::fs::write(
            triggers_dir.join("bad_trigger.json"),
            r#"{"channel_id":"  ","message":"bad trigger"}"#,
        )
        .expect("bad trigger file should be writable");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed even with trigger error");

        assert_eq!(stats.iterations, 1);
        // At least the good trigger should be ingested; the bad one should hit the error path
        assert!(stats.ingested_triggers >= 1);
    }

    #[test]
    fn daemon_loop_exercises_startup_reconciliation_notification() {
        let workspace = TempWorkspace::new("daemon", "loop-reconcile");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");

        // Seed a "Running" run so startup reconciliation recovers it.
        let run_store = RunStore::new(&state_root);
        let mut stale_run = sample_claude_run("111");
        stale_run.id = "run:discord:dm:111:stale".to_string();
        stale_run.logical_session_id = "discord:dm:111".to_string();
        stale_run.status = RunStatus::Running;
        stale_run.delivery_channel_id = Some("999".to_string());
        stale_run.started_at_epoch_ms = Some(1);
        stale_run.queued_at_epoch_ms = 1;
        run_store
            .upsert_run(&stale_run)
            .expect("stale run should persist");

        // Seed a matching logical session so reconciliation can find the run.
        let session_store = crab_store::SessionStore::new(&state_root);
        let mut session = sample_session(LaneState::Running, None);
        session.id = "discord:dm:111".to_string();
        session_store
            .upsert_session(&session)
            .expect("session should persist");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let discord_state = discord.clone();

        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed with reconciliation");

        assert_eq!(stats.iterations, 1);

        // The startup recovery notification should have been posted to the delivery channel.
        let state = discord_state.state();
        assert!(
            state
                .posted
                .iter()
                .any(|(ch, _, content)| { ch == "999" && content.contains("Crab restarted") }),
            "expected startup recovery Discord message, got: {:?}",
            state.posted
        );
    }

    #[test]
    fn daemon_loop_exercises_heartbeat_cycle() {
        let workspace = TempWorkspace::new("daemon", "loop-heartbeat");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");

        // Seed a stale running run so heartbeat has work to do.
        let run_store = RunStore::new(&state_root);
        let mut stale = sample_claude_run("111");
        stale.id = "run:discord:channel:777:heartbeat-stale".to_string();
        stale.logical_session_id = "discord:channel:777".to_string();
        stale.status = RunStatus::Running;
        stale.started_at_epoch_ms = Some(1);
        stale.queued_at_epoch_ms = 1;
        run_store.upsert_run(&stale).expect("run should persist");

        let session_store = crab_store::SessionStore::new(&state_root);
        let mut session = sample_session(LaneState::Running, None);
        session.id = "discord:channel:777".to_string();
        session_store
            .upsert_session(&session)
            .expect("session should persist");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());

        // Advance clock to trigger heartbeat (interval is typically 120s).
        struct AdvancingControl {
            call: u64,
            base: u64,
        }
        impl DaemonLoopControl for AdvancingControl {
            fn now_epoch_ms(&mut self) -> CrabResult<u64> {
                self.call += 1;
                Ok(self.base + self.call * 120_000)
            }
            fn should_shutdown(&self) -> bool {
                false
            }
            fn sleep_tick(&mut self, _: u64) -> CrabResult<()> {
                Ok(())
            }
        }

        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 1,
            max_iterations: Some(3),
        };

        let mut control = AdvancingControl {
            call: 0,
            base: 1_739_173_200_000,
        };

        let stats =
            super::run_daemon_loop_with_transport(&config, &daemon_config, discord, &mut control)
                .expect("daemon loop with heartbeat should complete");

        assert!(
            stats.heartbeat_cycles >= 1,
            "expected at least one heartbeat cycle"
        );
    }

    #[test]
    fn daemon_loop_ingests_steering_triggers_when_idle() {
        let workspace = TempWorkspace::new("daemon", "loop-steering-triggers");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");

        // Write a good steering trigger
        crab_core::write_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "888".to_string(),
                message: "steer me".to_string(),
            },
        )
        .expect("steering trigger should be written");

        // Write a bad steering trigger directly to exercise the error branch
        let steering_dir = state_root.join("steering_triggers");
        std::fs::create_dir_all(&steering_dir).expect("steering dir should be creatable");
        std::fs::write(
            steering_dir.join("bad_steer.json"),
            r#"{"channel_id":"  ","message":"bad steering trigger"}"#,
        )
        .expect("bad steering trigger file should be writable");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed even with steering trigger error");

        assert_eq!(stats.iterations, 1);
        // At least the good steering trigger should be ingested
        assert!(
            stats.ingested_triggers >= 1,
            "at least one steering trigger should be ingested"
        );
    }

    #[test]
    fn daemon_loop_ingests_graceful_steering_triggers_when_idle() {
        let workspace = TempWorkspace::new("daemon", "loop-graceful-steering-triggers");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");

        // Write a good graceful steering trigger
        crab_core::write_graceful_steering_trigger(
            &state_root,
            &crab_core::PendingTrigger {
                channel_id: "999".to_string(),
                message: "graceful steer me".to_string(),
            },
        )
        .expect("graceful steering trigger should be written");

        // Write a bad graceful steering trigger to exercise the error branch
        let graceful_dir = state_root.join("graceful_steering");
        std::fs::create_dir_all(&graceful_dir).expect("graceful dir should be creatable");
        std::fs::write(
            graceful_dir.join("bad_graceful.json"),
            r#"{"channel_id":"  ","message":"bad graceful trigger"}"#,
        )
        .expect("bad graceful trigger file should be writable");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut control = OneShotControl {
            now: 1_739_173_200_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed even with graceful trigger error");

        assert_eq!(stats.iterations, 1);
        assert!(
            stats.ingested_triggers >= 1,
            "at least one graceful steering trigger should be ingested"
        );
    }

    #[test]
    fn daemon_loop_evaluates_self_work_and_emits_wake_trigger() {
        let workspace = TempWorkspace::new("daemon", "loop-self-work");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        write_self_work_session_atomically(
            &state_root,
            &sample_self_work_session(SelfWorkSessionStatus::Active),
        )
        .expect("self-work session should persist");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut control = OneShotControl {
            now: 1_739_173_380_000,
            shutdown: false,
        };

        let stats = super::run_daemon_loop_with_transport(
            &config,
            &daemon_loop_config(),
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed with self-work session");

        assert_eq!(stats.iterations, 1);
        let triggers = read_pending_triggers(&state_root).expect("triggers should be readable");
        assert_eq!(triggers.len(), 1);
        assert!(triggers[0].1.message.contains("event: wake"));
    }
}
