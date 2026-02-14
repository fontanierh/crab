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
    claude::{ClaudeRawEvent, ClaudeRawEventStream},
    map_opencode_inference_profile, normalize_opencode_events, recover_opencode_session,
    BackendEvent, BackendHarness, ClaudeBackend, ClaudeProcess, CodexAppServerProcess,
    CodexLifecycleManager, OpenCodeManager, OpenCodeRawEvent, OpenCodeRecoveryPlan,
    OpenCodeRecoveryRuntime, OpenCodeServerHandle, OpenCodeServerProcess, OpenCodeSessionConfig,
    OpenCodeTokenUsage, OpenCodeTurnConfig, OpenCodeTurnState, SessionContext, TurnInput,
};
#[cfg(not(any(test, coverage)))]
use crab_backends::{map_claude_inference_profile, ClaudeThinkingMode};
#[cfg(not(coverage))]
use crab_core::{build_context_diagnostics_report, render_context_diagnostics_fixture};
use crab_core::{
    compile_prompt_contract, detect_workspace_bootstrap_state, process_workspace_git_push_queue,
    render_budgeted_turn_context, resolve_inference_profile, resolve_scoped_memory_snippets,
    resolve_sender_identity, resolve_sender_trust_context, BackendKind, ContextAssemblyInput,
    ContextBudgetPolicy, CrabError, CrabResult, InferenceProfile, InferenceProfileResolutionInput,
    MemoryCitationMode, OwnerConfig, PromptContractInput, ReasoningLevel, Run, RunProfileTelemetry,
    RuntimeConfig, ScopedMemorySnippetResolverInput, SenderConversationKind, SenderIdentityInput,
    TrustSurface, WorkspaceBootstrapState, IDENTITY_FILE_NAME, MEMORY_FILE_NAME,
    OWNER_MEMORY_SCOPE_DIRECTORY, SOUL_FILE_NAME, USER_FILE_NAME,
};
use crab_discord::GatewayMessage;
use crab_store::CheckpointStore;
#[cfg(not(any(test, coverage)))]
use futures::channel::mpsc;
use futures::{executor::block_on, stream};

#[cfg(not(any(test, coverage)))]
use crate::daemon_backend_bridge::CodexAppServerTransport;
#[cfg(any(test, coverage, debug_assertions))]
use crate::daemon_backend_bridge::DeterministicCodexTransport;
use crate::daemon_backend_bridge::{
    CodexDaemonBackendBridge, DaemonBackendBridge as CodexBackendBridge,
};
use crate::{boot_runtime_with_processes, run_heartbeat_if_due, TurnExecutor, TurnExecutorRuntime};

pub const DEFAULT_DAEMON_TICK_INTERVAL_MS: u64 = 250;
const DAEMON_TURN_CONTEXT_READ: &str = "daemon_turn_context_read";
const DAEMON_BACKEND_BRIDGE_EXECUTE: &str = "daemon_backend_bridge_execute";
const DAEMON_BACKEND_BRIDGE_CONTEXT: &str = "daemon_backend_bridge";
const DAEMON_OPENCODE_TRANSPORT_CONTEXT: &str = "daemon_opencode_transport";
#[cfg(not(any(test, coverage)))]
const DAEMON_CLAUDE_TRANSPORT_CONTEXT: &str = "daemon_claude_transport";
#[cfg(any(test, not(coverage)))]
const DAEMON_CLAUDE_STREAM_CONTEXT: &str = "daemon_claude_stream";
const MILLIS_PER_DAY: u64 = 86_400_000;
const OPENCODE_SESSION_PLACEHOLDER_PREFIX: &str = "backend-session:";
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
- Crab typically keeps this same backend session alive across multiple Discord turns.\n\
- Crab may occasionally rotate/restart you after maintenance (checkpoint/compaction).\n\
- Crab can run hidden maintenance turns; those are not user-visible.\n\
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
const DAEMON_DETERMINISTIC_CODEX_TRANSPORT_ENV: &str =
    "CRAB_DAEMON_FORCE_DETERMINISTIC_CODEX_TRANSPORT";
#[cfg(all(not(any(test, coverage)), debug_assertions))]
const DAEMON_DETERMINISTIC_CLAUDE_PROCESS_ENV: &str =
    "CRAB_DAEMON_FORCE_DETERMINISTIC_CLAUDE_PROCESS";

#[derive(Debug, Clone, Default)]
pub struct DaemonClaudeProcess {
    #[cfg(not(any(test, coverage)))]
    state: Arc<std::sync::Mutex<DaemonClaudeProcessState>>,
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

    fn end_session(&self, server_base_url: &str, session_id: &str) -> CrabResult<()>;

    fn execute_turn(
        &self,
        server_base_url: &str,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<OpenCodeBridgeTurnResult>;
}

#[derive(Debug, Default)]
struct HttpOpenCodeBridgeRuntime;

impl HttpOpenCodeBridgeRuntime {
    fn endpoint(server_base_url: &str, route: &str) -> String {
        let base = server_base_url.trim_end_matches('/');
        let route = route.trim_start_matches('/');
        format!("{base}/{route}")
    }

    fn post_json(
        &self,
        server_base_url: &str,
        route: &str,
        payload: &serde_json::Value,
    ) -> CrabResult<serde_json::Value> {
        self.execute_json_request(reqwest::Method::POST, server_base_url, route, Some(payload))
    }

    fn delete_json(&self, server_base_url: &str, route: &str) -> CrabResult<serde_json::Value> {
        self.execute_json_request(reqwest::Method::DELETE, server_base_url, route, None)
    }

    fn execute_json_request(
        &self,
        method: reqwest::Method,
        server_base_url: &str,
        route: &str,
        payload: Option<&serde_json::Value>,
    ) -> CrabResult<serde_json::Value> {
        let endpoint = Self::endpoint(server_base_url, route);
        let client = reqwest::blocking::Client::new();
        let mut request = client.request(method.clone(), &endpoint);
        if let Some(payload) = payload {
            request = request.json(payload);
        }
        let response = request.send().map_err(|error| CrabError::Io {
            context: DAEMON_OPENCODE_TRANSPORT_CONTEXT,
            path: None,
            message: format!("{method} {endpoint} failed: {error}"),
        })?;
        let status = response.status();
        let response_body = response.text().map_err(|error| CrabError::Io {
            context: DAEMON_OPENCODE_TRANSPORT_CONTEXT,
            path: None,
            message: format!("failed reading response body from {endpoint}: {error}"),
        })?;
        if !status.is_success() {
            return Err(CrabError::InvariantViolation {
                context: DAEMON_OPENCODE_TRANSPORT_CONTEXT,
                message: format!(
                    "{method} {endpoint} returned HTTP {} with body {}",
                    status.as_u16(),
                    response_body.trim()
                ),
            });
        }
        if response_body.trim().is_empty() {
            return Ok(serde_json::Value::Object(serde_json::Map::new()));
        }
        serde_json::from_str(&response_body).map_err(|error| CrabError::Serialization {
            context: DAEMON_OPENCODE_TRANSPORT_CONTEXT,
            path: None,
            message: format!("invalid JSON response from {endpoint}: {error}"),
        })
    }

    fn value_at_path<'a>(
        value: &'a serde_json::Value,
        path: &[&str],
    ) -> Option<&'a serde_json::Value> {
        let mut cursor = value;
        for segment in path {
            cursor = cursor.get(*segment)?;
        }
        Some(cursor)
    }

    fn value_as_non_empty_string(value: &serde_json::Value) -> Option<String> {
        value
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned)
    }

    fn value_as_u64(value: &serde_json::Value) -> Option<u64> {
        if let Some(number) = value.as_u64() {
            return Some(number);
        }
        value.as_str()?.parse::<u64>().ok()
    }

    fn extract_required_response_string(
        value: &serde_json::Value,
        context: &'static str,
        field_name: &'static str,
        paths: &[&[&str]],
    ) -> CrabResult<String> {
        for path in paths {
            if let Some(parsed) =
                Self::value_at_path(value, path).and_then(Self::value_as_non_empty_string)
            {
                return Ok(parsed);
            }
        }
        Err(CrabError::InvariantViolation {
            context,
            message: format!("response is missing required field {field_name}"),
        })
    }

    fn extract_first_u64(value: &serde_json::Value, paths: &[&[&str]]) -> Option<u64> {
        for path in paths {
            if let Some(parsed) = Self::value_at_path(value, path).and_then(Self::value_as_u64) {
                return Some(parsed);
            }
        }
        None
    }

    fn extract_usage(value: &serde_json::Value) -> Option<OpenCodeTokenUsage> {
        let input_tokens = Self::extract_first_u64(
            value,
            &[
                &["usage", "input_tokens"],
                &["usage", "inputTokens"],
                &["input_tokens"],
                &["inputTokens"],
                &["info", "tokens", "input"],
                &["info", "usage", "input_tokens"],
                &["info", "usage", "inputTokens"],
            ],
        )?;
        let output_tokens = Self::extract_first_u64(
            value,
            &[
                &["usage", "output_tokens"],
                &["usage", "outputTokens"],
                &["output_tokens"],
                &["outputTokens"],
                &["info", "tokens", "output"],
                &["info", "usage", "output_tokens"],
                &["info", "usage", "outputTokens"],
            ],
        )?;
        let total_tokens = Self::extract_first_u64(
            value,
            &[
                &["usage", "total_tokens"],
                &["usage", "totalTokens"],
                &["total_tokens"],
                &["totalTokens"],
                &["info", "tokens", "total"],
                &["info", "usage", "total_tokens"],
                &["info", "usage", "totalTokens"],
            ],
        )
        .or_else(|| input_tokens.checked_add(output_tokens))?;
        Some(OpenCodeTokenUsage {
            input_tokens,
            output_tokens,
            total_tokens,
        })
    }

    fn collect_delta_from_parts(parts: &[serde_json::Value]) -> String {
        let mut collected = String::new();
        for part in parts {
            if let Some(text) = part
                .get("text")
                .and_then(serde_json::Value::as_str)
                .or_else(|| part.get("delta").and_then(serde_json::Value::as_str))
                .or_else(|| part.get("content").and_then(serde_json::Value::as_str))
            {
                collected.push_str(text);
            } else if let Some(text) = part
                .get("text")
                .and_then(|inner| inner.get("value"))
                .and_then(serde_json::Value::as_str)
            {
                collected.push_str(text);
            }
        }
        collected
    }

    fn extract_text_delta(value: &serde_json::Value) -> Option<String> {
        for path in &[&["output_text"][..], &["text"], &["message", "text"]] {
            if let Some(text) = Self::value_at_path(value, path)
                .and_then(serde_json::Value::as_str)
                .map(str::trim)
                .filter(|text| !text.is_empty())
            {
                return Some(text.to_string());
            }
        }

        for path in &[
            &["parts"][..],
            &["message", "parts"],
            &["assistant", "parts"],
        ] {
            if let Some(parts) =
                Self::value_at_path(value, path).and_then(serde_json::Value::as_array)
            {
                let collected = Self::collect_delta_from_parts(parts);
                if !collected.trim().is_empty() {
                    return Some(collected);
                }
            }
        }

        None
    }

    fn extract_turn_state(value: &serde_json::Value) -> OpenCodeTurnState {
        let state = Self::extract_required_response_string(
            value,
            "opencode_send_prompt_response",
            "state",
            &[
                &["state"],
                &["status"],
                &["info", "state"],
                &["info", "status"],
            ],
        )
        .unwrap_or_else(|_| "completed".to_string());
        match state.to_ascii_lowercase().as_str() {
            "interrupted" | "cancelled" | "canceled" => OpenCodeTurnState::Interrupted,
            "failed" | "error" | "errored" => OpenCodeTurnState::Failed,
            _ => OpenCodeTurnState::Completed,
        }
    }

    fn extract_turn_message(value: &serde_json::Value) -> Option<String> {
        for path in &[&["message"][..], &["error"], &["info", "message"]] {
            if let Some(text) =
                Self::value_at_path(value, path).and_then(Self::value_as_non_empty_string)
            {
                return Some(text);
            }
        }
        None
    }
}

impl OpenCodeBridgeRuntime for HttpOpenCodeBridgeRuntime {
    fn create_session(
        &self,
        server_base_url: &str,
        config: OpenCodeSessionConfig,
    ) -> CrabResult<String> {
        let mut payload = serde_json::Map::new();
        if let Some(model) = config.model.filter(|value| !value.trim().is_empty()) {
            payload.insert("model".to_string(), serde_json::Value::String(model));
        }
        if let Some(reasoning_level) = config
            .reasoning_level
            .filter(|value| !value.trim().is_empty())
        {
            payload.insert(
                "reasoning_level".to_string(),
                serde_json::Value::String(reasoning_level.clone()),
            );
            payload.insert(
                "reasoningLevel".to_string(),
                serde_json::Value::String(reasoning_level),
            );
        }
        let response = self.post_json(
            server_base_url,
            "session",
            &serde_json::Value::Object(payload),
        );
        let response = response?;
        Self::extract_required_response_string(
            &response,
            "opencode_create_session_response",
            "session_id",
            &[&["session_id"], &["sessionId"], &["id"], &["session", "id"]],
        )
    }

    fn end_session(&self, server_base_url: &str, session_id: &str) -> CrabResult<()> {
        let route = format!("session/{session_id}");
        let _ = self.delete_json(server_base_url, &route)?;
        Ok(())
    }

    fn execute_turn(
        &self,
        server_base_url: &str,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<OpenCodeBridgeTurnResult> {
        let mut payload = serde_json::Map::new();
        payload.insert(
            "prompt".to_string(),
            serde_json::Value::String(prompt.to_string()),
        );
        payload.insert(
            "parts".to_string(),
            serde_json::json!([{"type":"text","text":prompt}]),
        );
        if let Some(model) = config.model.filter(|value| !value.trim().is_empty()) {
            payload.insert(
                "model".to_string(),
                serde_json::Value::String(model.clone()),
            );
            payload.insert("modelID".to_string(), serde_json::Value::String(model));
        }
        if let Some(reasoning_level) = config
            .reasoning_level
            .filter(|value| !value.trim().is_empty())
        {
            payload.insert(
                "reasoning_level".to_string(),
                serde_json::Value::String(reasoning_level.clone()),
            );
            payload.insert(
                "reasoningLevel".to_string(),
                serde_json::Value::String(reasoning_level),
            );
        }
        let route = format!("session/{session_id}/message");
        let response = self.post_json(server_base_url, &route, &serde_json::Value::Object(payload));
        let response = response?;
        let turn_id = Self::extract_required_response_string(
            &response,
            "opencode_send_prompt_response",
            "turn_id",
            &[
                &["turn_id"],
                &["turnId"],
                &["id"],
                &["message", "id"],
                &["info", "id"],
            ],
        );
        let turn_id = turn_id?;

        let mut raw_events = Vec::new();
        let mut sequence = 1;
        if let Some(delta) = Self::extract_text_delta(&response) {
            raw_events.push(OpenCodeRawEvent::AssistantDelta {
                sequence,
                text: delta,
            });
            sequence = sequence.saturating_add(1);
        }
        let state = Self::extract_turn_state(&response);
        let message = Self::extract_turn_message(&response)
            .or_else(|| (state == OpenCodeTurnState::Completed).then(|| "completed".to_string()));
        raw_events.push(OpenCodeRawEvent::TurnFinished {
            sequence,
            turn_id: turn_id.clone(),
            state,
            message,
            usage: Self::extract_usage(&response),
        });
        Ok(OpenCodeBridgeTurnResult {
            turn_id,
            raw_events,
        })
    }
}

#[derive(Debug, Clone)]
struct OpenCodeRecoveryBridgeProcess {
    server_base_url: String,
}

impl OpenCodeRecoveryBridgeProcess {
    fn new(server_base_url: String) -> Self {
        Self { server_base_url }
    }
}

impl OpenCodeServerProcess for OpenCodeRecoveryBridgeProcess {
    fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle> {
        Ok(OpenCodeServerHandle {
            process_id: 1,
            started_at_epoch_ms: 1,
            server_base_url: self.server_base_url.clone(),
        })
    }

    fn is_server_healthy(&self, _handle: &OpenCodeServerHandle) -> bool {
        true
    }

    fn terminate_server(&self, _handle: &OpenCodeServerHandle) -> CrabResult<()> {
        Ok(())
    }
}

#[derive(Debug)]
struct OpenCodeRecoveryBridgeRuntimeAdapter<'a> {
    runtime: &'a dyn OpenCodeBridgeRuntime,
    server_base_url: &'a str,
}

impl OpenCodeRecoveryRuntime for OpenCodeRecoveryBridgeRuntimeAdapter<'_> {
    fn create_session(&self, config: OpenCodeSessionConfig) -> CrabResult<String> {
        self.runtime.create_session(self.server_base_url, config)
    }

    fn end_session(&self, session_id: &str) -> CrabResult<()> {
        self.runtime.end_session(self.server_base_url, session_id)
    }

    fn send_prompt(
        &self,
        session_id: &str,
        prompt: &str,
        config: OpenCodeTurnConfig,
    ) -> CrabResult<String> {
        self.runtime
            .execute_turn(self.server_base_url, session_id, prompt, config)
            .map(|result| result.turn_id)
    }
}

#[derive(Debug)]
struct OpenCodeExecutionBridge {
    server_base_url: String,
    runtime: Box<dyn OpenCodeBridgeRuntime>,
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
        })
    }

    fn execute_turn(
        &mut self,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        let mapping = map_opencode_inference_profile(&run.profile.resolved_profile);
        let prompt = turn_context.to_string();
        if should_materialize_opencode_session(physical_session) {
            let recovery_result = self.recover_session_with_helper(
                None,
                mapping.session_config.clone(),
                None,
                mapping.turn_config.clone(),
            );
            let recovery = recovery_result?;
            physical_session.backend_session_id = recovery.new_session_id;
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
                let previous_session_id = physical_session.backend_session_id.clone();
                let recovery_result = self.recover_session_with_helper(
                    Some(previous_session_id.as_str()),
                    mapping.session_config,
                    None,
                    mapping.turn_config.clone(),
                );
                let recovery = recovery_result?;
                #[cfg(not(coverage))]
                if let Some(previous_session_end_error) =
                    recovery.previous_session_end_error.as_deref()
                {
                    tracing::warn!(
                        previous_session_id = %previous_session_id,
                        previous_session_end_error,
                        "opencode session recovery rotated session after end-session warning"
                    );
                }
                physical_session.backend_session_id = recovery.new_session_id;
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

    fn recover_session_with_helper(
        &self,
        previous_session_id: Option<&str>,
        session_config: OpenCodeSessionConfig,
        checkpoint_prompt: Option<String>,
        checkpoint_turn_config: OpenCodeTurnConfig,
    ) -> CrabResult<crab_backends::OpenCodeRecoveryOutcome> {
        let runtime_adapter = OpenCodeRecoveryBridgeRuntimeAdapter {
            runtime: self.runtime.as_ref(),
            server_base_url: &self.server_base_url,
        };
        let plan = OpenCodeRecoveryPlan {
            session_config,
            checkpoint_prompt,
            checkpoint_turn_config,
        };
        let mut manager = OpenCodeManager::new(OpenCodeRecoveryBridgeProcess::new(
            self.server_base_url.clone(),
        ));
        recover_opencode_session(&mut manager, &runtime_adapter, previous_session_id, &plan)
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

#[derive(Debug)]
struct DaemonBackendBridge {
    codex: Box<dyn CodexBackendDebugBridge>,
    opencode: Option<OpenCodeExecutionBridge>,
}

trait CodexBackendDebugBridge: CodexBackendBridge + std::fmt::Debug {}

impl<T: CodexBackendBridge + std::fmt::Debug> CodexBackendDebugBridge for T {}

impl DaemonBackendBridge {
    fn default_codex_bridge() -> CrabResult<Box<dyn CodexBackendDebugBridge>> {
        #[cfg(all(not(any(test, coverage)), debug_assertions))]
        if use_deterministic_codex_transport_override() {
            return Ok(Box::new(CodexDaemonBackendBridge::new(
                DeterministicCodexTransport::default(),
            )));
        }

        #[cfg(not(any(test, coverage)))]
        {
            let transport = CodexAppServerTransport::new()?;
            Ok(Box::new(CodexDaemonBackendBridge::new(transport)))
        }

        #[cfg(any(test, coverage))]
        {
            Ok(Box::new(CodexDaemonBackendBridge::new(
                DeterministicCodexTransport::default(),
            )))
        }
    }

    fn new_default() -> CrabResult<Self> {
        Ok(Self {
            codex: Self::default_codex_bridge()?,
            opencode: None,
        })
    }

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
        });
    }

    fn execute_turn(
        &mut self,
        codex_lifecycle: &mut dyn CodexLifecycleManager,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
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
        self.codex.execute_backend_turn(
            codex_lifecycle,
            physical_session,
            run,
            turn_id,
            turn_context,
        )
    }
}

impl DaemonBackendExecutionBridge for DaemonBackendBridge {
    fn execute_turn(
        &mut self,
        codex_lifecycle: &mut dyn CodexLifecycleManager,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        DaemonBackendBridge::execute_turn(
            self,
            codex_lifecycle,
            physical_session,
            run,
            turn_id,
            turn_context,
        )
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
    claude_bridge: DaemonClaudeExecutionBridge,
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
        codex_lifecycle: &mut dyn CodexLifecycleManager,
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

#[cfg(all(not(any(test, coverage)), debug_assertions))]
fn use_deterministic_codex_transport_override() -> bool {
    std::env::var(DAEMON_DETERMINISTIC_CODEX_TRANSPORT_ENV)
        .ok()
        .is_some_and(|raw| {
            matches!(
                raw.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
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
        let backend_bridge = Box::new(DaemonBackendBridge::new_default()?);
        Self::new_with_backend_bridge_and_claude_process(
            owner,
            discord,
            backend_bridge,
            DaemonClaudeProcess::default(),
        )
    }

    #[cfg(test)]
    fn new_with_backend_bridge(
        owner: OwnerConfig,
        discord: D,
        backend_bridge: Box<dyn DaemonBackendExecutionBridge>,
    ) -> CrabResult<Self> {
        Self::new_with_backend_bridge_and_claude_process(
            owner,
            discord,
            backend_bridge,
            DaemonClaudeProcess::default(),
        )
    }

    #[cfg(test)]
    fn new_with_claude_process<P>(
        owner: OwnerConfig,
        discord: D,
        claude_process: P,
    ) -> CrabResult<Self>
    where
        P: ClaudeProcess + 'static,
    {
        let backend_bridge = Box::new(DaemonBackendBridge::new_default()?);
        Self::new_with_backend_bridge_and_claude_process(
            owner,
            discord,
            backend_bridge,
            claude_process,
        )
    }

    fn new_with_backend_bridge_and_claude_process<P>(
        owner: OwnerConfig,
        discord: D,
        backend_bridge: Box<dyn DaemonBackendExecutionBridge>,
        claude_process: P,
    ) -> CrabResult<Self>
    where
        P: ClaudeProcess + 'static,
    {
        Ok(Self {
            discord,
            owner,
            backend_bridge,
            next_session_sequence: 0,
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
        if profile.backend == BackendKind::Claude {
            let has_cached_active_session = active_physical_session_id
                .is_some_and(|active_id| self.physical_sessions.contains_key(active_id));
            match resolve_claude_session_strategy(
                active_physical_session_id,
                has_cached_active_session,
            ) {
                ClaudeSessionStrategy::ReuseCached { active_id } => {
                    let existing =
                        self.physical_sessions.get(active_id).cloned().expect(
                            "resolve_claude_session_strategy guarantees cached session exists",
                        );
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
            return Ok(session);
        }

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
        codex_lifecycle: &mut dyn CodexLifecycleManager,
        physical_session: &mut crab_core::PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<crab_backends::BackendEventStream> {
        let cache_key = physical_session.id.clone();
        if run.profile.resolved_profile.backend == BackendKind::Claude {
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
            return Ok(stream);
        }

        let backend_events = self
            .backend_bridge
            .execute_turn(
                codex_lifecycle,
                physical_session,
                run,
                turn_id,
                turn_context,
            )
            .map_err(|error| CrabError::InvariantViolation {
                context: DAEMON_BACKEND_BRIDGE_EXECUTE,
                message: format!(
                    "run {} turn {} backend {:?} bridge execution failed: {}",
                    run.id, turn_id, run.profile.resolved_profile.backend, error
                ),
            })?;
        if physical_session.last_turn_id.is_none() {
            physical_session.last_turn_id = Some(turn_id.to_string());
        }
        self.physical_sessions
            .insert(cache_key, physical_session.clone());
        Ok(Box::pin(stream::iter(backend_events)))
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
        if session.backend == BackendKind::Claude {
            block_on(self.claude_bridge.harness.interrupt_turn(session, turn_id))
        } else {
            Ok(())
        }
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
    let mut discord = discord;
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

    boot.composition.backends.codex.ensure_started()?;
    let opencode_handle = boot.composition.backends.opencode.ensure_running()?;

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
    if !runtime.has_opencode_backend_bridge() {
        let bridge_url = opencode_handle.server_base_url.clone();
        let bridge_runtime = Box::new(HttpOpenCodeBridgeRuntime);
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
        triggers = stats.ingested_triggers,
        dispatched = stats.dispatched_runs,
        heartbeats = stats.heartbeat_cycles,
        "daemon loop exiting: shutting down backends"
    );
    executor.runtime_mut().shutdown_claude_sessions()?;
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
        conversation_kind_for_logical_session_id, epoch_ms_from_duration,
        epoch_ms_from_system_time, epoch_ms_to_yyyy_mm_dd, load_latest_checkpoint_summary,
        memory_scope_directory_for_run, notify_startup_recovered_runs, read_workspace_markdown,
        run_daemon_loop_with_transport, run_daemon_loop_with_transport_and_runtime_builder,
        trust_surface_for_logical_session_id, DaemonBackendExecutionBridge, DaemonClaudeProcess,
        DaemonConfig, DaemonDiscordIo, DaemonLoopControl, DaemonLoopStats, DaemonTurnRuntime,
        OpenCodeBridgeRuntime, OpenCodeBridgeTurnResult, SystemDaemonLoopControl,
    };
    use crate::test_support::{runtime_config_for_workspace_with_lanes, TempWorkspace};
    use crate::TurnExecutorRuntime;
    use crab_backends::{
        claude::{ClaudeRawEvent, ClaudeRawEventStream},
        BackendEvent, BackendEventKind, ClaudeProcess, CodexAppServerProcess,
        CodexLifecycleManager, CodexProcessHandle, OpenCodeRawEvent, OpenCodeServerHandle,
        OpenCodeServerProcess, OpenCodeSessionConfig, OpenCodeTokenUsage, OpenCodeTurnConfig,
        OpenCodeTurnState, SessionContext, TurnInput,
    };
    use crab_core::{
        BackendKind, Checkpoint, CrabError, CrabResult, InferenceProfile, LaneState,
        LogicalSession, OwnerConfig, ProfileValueSource, ReasoningLevel, Run, RunProfileTelemetry,
        RunStatus, SenderConversationKind, TokenAccounting, TrustSurface, WorkspaceBootstrapState,
    };
    use crab_discord::{GatewayConversationKind, GatewayMessage};
    use crab_store::{CheckpointStore, EventStore, RunStore, SessionStore};
    use futures::executor::block_on;
    use futures::StreamExt;
    use std::collections::VecDeque;
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::{TcpListener, TcpStream};
    #[cfg(unix)]
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
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

        fn with_server_base_url(server_base_url: String) -> Self {
            Self {
                state: Arc::new(Mutex::new(TrackingOpenCodeState::default())),
                spawn_error: None,
                terminate_error: None,
                server_base_url,
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
            _codex_lifecycle: &mut dyn CodexLifecycleManager,
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
            _codex_lifecycle: &mut dyn CodexLifecycleManager,
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

    #[derive(Debug, Default)]
    struct TurnIdMutatingBackendBridge;

    impl DaemonBackendExecutionBridge for TurnIdMutatingBackendBridge {
        fn execute_turn(
            &mut self,
            _codex_lifecycle: &mut dyn CodexLifecycleManager,
            physical_session: &mut crab_core::PhysicalSession,
            _run: &Run,
            _turn_id: &str,
            _turn_context: &str,
        ) -> CrabResult<Vec<BackendEvent>> {
            physical_session.last_turn_id = Some("backend-turn-id-9".to_string());
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

    #[derive(Debug, Default)]
    struct NoopCodexLifecycle;

    impl CodexLifecycleManager for NoopCodexLifecycle {
        fn ensure_started(&mut self) -> CrabResult<CodexProcessHandle> {
            Ok(CodexProcessHandle {
                process_id: 1,
                started_at_epoch_ms: 1,
            })
        }

        fn restart(&mut self) -> CrabResult<CodexProcessHandle> {
            self.ensure_started()
        }
    }

    #[test]
    fn noop_codex_lifecycle_manager_returns_stable_handle_for_start_and_restart() {
        let mut lifecycle = NoopCodexLifecycle;
        let started = lifecycle
            .ensure_started()
            .expect("ensure_started should return handle");
        assert_eq!(
            started,
            CodexProcessHandle {
                process_id: 1,
                started_at_epoch_ms: 1,
            }
        );
        let restarted = lifecycle
            .restart()
            .expect("restart should return stable handle");
        assert_eq!(
            restarted,
            CodexProcessHandle {
                process_id: 1,
                started_at_epoch_ms: 1,
            }
        );
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

    fn gateway_dm_message(message_id: &str, author_id: &str, content: &str) -> GatewayMessage {
        GatewayMessage {
            message_id: message_id.to_string(),
            author_id: author_id.to_string(),
            author_is_bot: false,
            channel_id: format!("dm-{author_id}"),
            guild_id: None,
            thread_id: None,
            content: content.to_string(),
            conversation_kind: GatewayConversationKind::DirectMessage,
        }
    }

    fn onboarding_capture_payload_json() -> String {
        serde_json::json!({
            "schema_version": "v1",
            "agent_identity": "Crab",
            "owner_identity": "Owner",
            "machine_location": "Living room",
            "machine_timezone": "America/New_York",
            "primary_goals": ["Build reliable automations"]
        })
        .to_string()
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
        end_calls: usize,
        execute_calls: usize,
        seen_session_ids: Vec<String>,
        seen_ended_session_ids: Vec<String>,
        seen_prompts: Vec<String>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedOpenCodeBridgeRuntime {
        state: Arc<Mutex<ScriptedOpenCodeBridgeState>>,
    }

    #[derive(Debug, Clone)]
    struct ScriptedOpenCodeBridgeState {
        create_results: VecDeque<CrabResult<String>>,
        end_results: VecDeque<CrabResult<()>>,
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
                    end_results: VecDeque::new(),
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

        fn end_session(&self, _server_base_url: &str, session_id: &str) -> CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.stats.end_calls += 1;
            state
                .stats
                .seen_ended_session_ids
                .push(session_id.to_string());
            state.end_results.pop_front().unwrap_or(Ok(()))
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

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CapturedHttpRequest {
        method: String,
        path: String,
        body: String,
    }

    #[derive(Debug, Clone)]
    struct ScriptedHttpResponse {
        status_code: u16,
        body: String,
    }

    #[derive(Debug)]
    struct ScriptedHttpServer {
        base_url: String,
        requests: Arc<Mutex<Vec<CapturedHttpRequest>>>,
        handle: Option<JoinHandle<()>>,
    }

    impl ScriptedHttpServer {
        fn start(scripted_responses: Vec<ScriptedHttpResponse>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("listener bind should succeed");
            listener
                .set_nonblocking(false)
                .expect("listener should remain blocking");
            let local_addr = listener.local_addr().expect("local address should resolve");
            let requests = Arc::new(Mutex::new(Vec::new()));
            let requests_state = Arc::clone(&requests);
            let handle = std::thread::spawn(move || {
                for scripted in scripted_responses {
                    let (mut stream, _) = listener.accept().expect("accept should succeed");
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("read timeout should set");
                    let request = read_http_request(&mut stream);
                    requests_state
                        .lock()
                        .expect("lock should succeed")
                        .push(request);
                    write_http_response(&mut stream, scripted);
                }
            });

            Self {
                base_url: format!("http://{local_addr}"),
                requests,
                handle: Some(handle),
            }
        }

        fn base_url(&self) -> String {
            self.base_url.clone()
        }

        fn requests(&self) -> Vec<CapturedHttpRequest> {
            self.requests.lock().expect("lock should succeed").clone()
        }

        fn join(&mut self) {
            if let Some(handle) = self.handle.take() {
                handle.join().expect("http server thread should join");
            }
        }
    }

    impl Drop for ScriptedHttpServer {
        fn drop(&mut self) {
            self.join();
        }
    }

    fn read_http_request(stream: &mut TcpStream) -> CapturedHttpRequest {
        let mut reader = BufReader::new(stream);
        let mut request_line = String::new();
        reader
            .read_line(&mut request_line)
            .expect("request line should parse");
        let request_line = request_line.trim_end();
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default().to_string();
        let path = request_parts.next().unwrap_or_default().to_string();
        let mut content_length = 0usize;
        loop {
            let mut line = String::new();
            reader
                .read_line(&mut line)
                .expect("request header line should parse");
            if line == "\r\n" {
                break;
            }
            let mut header_parts = line.splitn(2, ':');
            let key = header_parts.next().unwrap_or_default().trim();
            let value = header_parts.next().unwrap_or_default().trim();
            if key.eq_ignore_ascii_case("content-length") {
                content_length = value.parse::<usize>().expect("content-length should parse");
            }
        }
        let mut body = vec![0u8; content_length];
        reader
            .read_exact(&mut body)
            .expect("request body should parse");
        CapturedHttpRequest {
            method,
            path,
            body: String::from_utf8(body).expect("request body should be utf-8"),
        }
    }

    fn write_http_response(stream: &mut TcpStream, response: ScriptedHttpResponse) {
        let body_bytes = response.body.into_bytes();
        let headers = format!(
            "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            response.status_code,
            body_bytes.len()
        );
        stream
            .write_all(headers.as_bytes())
            .expect("response headers should write");
        stream
            .write_all(&body_bytes)
            .expect("response body should write");
        stream.flush().expect("response should flush");
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
                cache_read_input_tokens: 0,
                cache_creation_input_tokens: 0,
            },
            has_injected_bootstrap: false,
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
        let mut codex_lifecycle = NoopCodexLifecycle;
        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut physical,
                &run,
                "turn-1",
                "compiled context",
            )
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
        let mut codex_lifecycle = NoopCodexLifecycle;

        let interrupted = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-1",
                "context one",
            )
            .expect("interrupted stream should still surface events");
        let interrupted = block_on(interrupted.collect::<Vec<_>>());
        assert!(interrupted
            .iter()
            .any(|event| event.kind == BackendEventKind::TurnInterrupted));
        assert_eq!(session.last_turn_id.as_deref(), Some("turn-1"));

        let errored_event = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-2",
                "context two",
            )
            .expect("error events should still be emitted as backend events");
        let errored_event = block_on(errored_event.collect::<Vec<_>>());
        assert!(errored_event.iter().any(|event| {
            event.kind == BackendEventKind::Error
                && event.payload.get("message") == Some(&"backend execution failed".to_string())
        }));
        assert_eq!(session.last_turn_id.as_deref(), Some("turn-2"));

        let send_error = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-3",
                "context three",
            )
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
        let mut codex_lifecycle = NoopCodexLifecycle;
        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-default",
                "default context",
            )
            .expect("default Claude process should execute through bridge");
        let events = block_on(events.collect::<Vec<_>>());
        assert_run_usage_note(&events, "2", "3", "5", "claude");

        let mut forced_error_run = sample_claude_run("123");
        forced_error_run.id = "run-force-claude-send-error".to_string();
        let error = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &forced_error_run,
                "turn-error",
                "context",
            )
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
        let mut bridge =
            super::DaemonBackendBridge::new_default().expect("default bridge should construct");
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
        let mut codex_lifecycle = NoopCodexLifecycle;

        let error = bridge
            .execute_turn(
                &mut codex_lifecycle,
                &mut physical_session,
                &run,
                "turn-op-1",
                "turn context",
            )
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
        assert_eq!(bridge_stats.end_calls, 1);
        assert_eq!(bridge_stats.execute_calls, 2);
        assert_eq!(
            bridge_stats.seen_session_ids,
            vec![
                "opencode-session-initial".to_string(),
                "opencode-session-recovered".to_string(),
            ]
        );
        assert_eq!(
            bridge_stats.seen_ended_session_ids,
            vec!["opencode-session-initial".to_string()]
        );
        assert_eq!(
            bridge_stats.seen_prompts[0],
            "runtime turn context".to_string()
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
        assert_eq!(bridge_stats.end_calls, 0);
        assert_eq!(bridge_stats.execute_calls, 1);
        assert_eq!(
            bridge_stats.seen_session_ids,
            vec!["opencode-session-existing".to_string()]
        );
    }

    #[test]
    fn opencode_recovery_bridge_process_exposes_health_and_terminate_paths() {
        let process =
            super::OpenCodeRecoveryBridgeProcess::new("http://127.0.0.1:4210".to_string());
        let handle = process
            .spawn_server()
            .expect("recovery bridge process should spawn deterministic handle");
        assert_eq!(handle.process_id, 1);
        assert!(
            process.is_server_healthy(&handle),
            "recovery bridge process should always report healthy"
        );
        process
            .terminate_server(&handle)
            .expect("recovery bridge process terminate should succeed");
    }

    #[test]
    fn opencode_recovery_runtime_adapter_send_prompt_delegates_to_runtime_execute_turn() {
        let bridge_runtime = ScriptedOpenCodeBridgeRuntime::with_results(
            Vec::new(),
            vec![Ok(scripted_opencode_turn_result(
                "checkpoint-turn-1",
                "checkpoint replay",
                (3, 2, 5),
                OpenCodeTurnState::Completed,
            ))],
        );
        let bridge_state = bridge_runtime.clone();
        let adapter = super::OpenCodeRecoveryBridgeRuntimeAdapter {
            runtime: &bridge_runtime,
            server_base_url: "http://127.0.0.1:4210",
        };

        let turn_id = crab_backends::OpenCodeRecoveryRuntime::send_prompt(
            &adapter,
            "session-checkpoint",
            "restore from checkpoint",
            OpenCodeTurnConfig::default(),
        )
        .expect("send_prompt should return the delegated turn id");
        assert_eq!(turn_id, "checkpoint-turn-1".to_string());

        let stats = bridge_state.stats();
        assert_eq!(stats.execute_calls, 1);
        assert_eq!(
            stats.seen_session_ids,
            vec!["session-checkpoint".to_string()]
        );
        assert_eq!(
            stats.seen_prompts,
            vec!["restore from checkpoint".to_string()]
        );
    }

    #[test]
    fn http_opencode_bridge_runtime_parses_session_turn_events_and_usage() {
        let mut http_server = ScriptedHttpServer::start(vec![
            ScriptedHttpResponse {
                status_code: 201,
                body: serde_json::json!({
                    "id": "session-transport-1"
                })
                .to_string(),
            },
            ScriptedHttpResponse {
                status_code: 200,
                body: serde_json::json!({
                    "id": "turn-transport-1",
                    "status": "completed",
                    "usage": {
                        "input_tokens": 9,
                        "output_tokens": 5,
                        "total_tokens": 14
                    },
                    "parts": [
                        {"type": "text", "text": "real transport output"}
                    ]
                })
                .to_string(),
            },
        ]);

        let runtime = super::HttpOpenCodeBridgeRuntime;
        let server_base_url = http_server.base_url();
        let session_id = runtime
            .create_session(
                &server_base_url,
                OpenCodeSessionConfig {
                    model: Some("open-model".to_string()),
                    reasoning_level: Some("medium".to_string()),
                },
            )
            .expect("transport create_session should succeed");
        assert_eq!(session_id, "session-transport-1".to_string());

        let turn_result = runtime
            .execute_turn(
                &server_base_url,
                &session_id,
                "bridge prompt",
                OpenCodeTurnConfig {
                    model: Some("open-model".to_string()),
                    reasoning_level: Some("high".to_string()),
                },
            )
            .expect("transport execute_turn should succeed");
        assert_eq!(turn_result.turn_id, "turn-transport-1".to_string());
        assert_eq!(
            turn_result.raw_events,
            vec![
                OpenCodeRawEvent::AssistantDelta {
                    sequence: 1,
                    text: "real transport output".to_string(),
                },
                OpenCodeRawEvent::TurnFinished {
                    sequence: 2,
                    turn_id: "turn-transport-1".to_string(),
                    state: OpenCodeTurnState::Completed,
                    message: Some("completed".to_string()),
                    usage: Some(OpenCodeTokenUsage {
                        input_tokens: 9,
                        output_tokens: 5,
                        total_tokens: 14,
                    }),
                },
            ]
        );

        http_server.join();
        let requests = http_server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].path, "/session".to_string());
        assert_eq!(
            requests[1].path,
            "/session/session-transport-1/message".to_string()
        );
        assert!(requests[0].body.contains("\"model\":\"open-model\""));
        assert!(requests[1].body.contains("\"prompt\":\"bridge prompt\""));
        assert!(requests[1].body.contains("\"parts\""));
    }

    #[test]
    fn http_opencode_bridge_runtime_sends_delete_for_end_session() {
        let mut http_server = ScriptedHttpServer::start(vec![ScriptedHttpResponse {
            status_code: 200,
            body: serde_json::json!({
                "ok": true
            })
            .to_string(),
        }]);
        let runtime = super::HttpOpenCodeBridgeRuntime;
        runtime
            .end_session(&http_server.base_url(), "session-end-1")
            .expect("delete end_session should succeed");

        http_server.join();
        let requests = http_server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method, "DELETE".to_string());
        assert_eq!(requests[0].path, "/session/session-end-1".to_string());
    }

    #[test]
    fn http_opencode_bridge_runtime_maps_non_success_http_status_to_transport_error() {
        let mut http_server = ScriptedHttpServer::start(vec![ScriptedHttpResponse {
            status_code: 500,
            body: serde_json::json!({
                "error": "forced failure"
            })
            .to_string(),
        }]);
        let runtime = super::HttpOpenCodeBridgeRuntime;
        let error = runtime
            .create_session(&http_server.base_url(), OpenCodeSessionConfig::default())
            .expect_err("500 response should fail");
        http_server.join();
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: super::DAEMON_OPENCODE_TRANSPORT_CONTEXT,
                message,
            } if message.contains("HTTP 500") && message.contains("forced failure")
        ));
    }

    #[test]
    fn http_opencode_bridge_runtime_maps_connection_failures_to_io_error() {
        let runtime = super::HttpOpenCodeBridgeRuntime;
        let error = runtime
            .create_session("http://127.0.0.1:1", OpenCodeSessionConfig::default())
            .expect_err("connection failures should map to io");
        assert!(matches!(
            error,
            CrabError::Io {
                context: super::DAEMON_OPENCODE_TRANSPORT_CONTEXT,
                path: None,
                message,
            } if message.contains("POST http://127.0.0.1:1/session failed")
        ));
    }

    #[test]
    fn http_opencode_bridge_runtime_maps_truncated_response_body_to_io_error() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("listener bind should succeed");
        let addr = listener.local_addr().expect("local address should resolve");
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept should succeed");
            let mut inbound = [0_u8; 1024];
            let _ = stream.read(&mut inbound);
            stream
                .write_all(
                    b"HTTP/1.1 201 OK\r\nContent-Type: application/json\r\nContent-Length: 8\r\nConnection: close\r\n\r\n{\"id\"",
                )
                .expect("response write should succeed");
            stream.flush().expect("response flush should succeed");
        });

        let runtime = super::HttpOpenCodeBridgeRuntime;
        let error = runtime
            .create_session(&format!("http://{addr}"), OpenCodeSessionConfig::default())
            .expect_err("truncated responses should fail while reading body");
        handle.join().expect("server thread should join");
        assert!(matches!(
            error,
            CrabError::Io {
                context: super::DAEMON_OPENCODE_TRANSPORT_CONTEXT,
                path: None,
                message,
            } if message.contains("failed reading response body")
        ));
    }

    #[test]
    fn http_opencode_bridge_runtime_maps_invalid_json_and_empty_body_shapes() {
        let mut invalid_json_server = ScriptedHttpServer::start(vec![ScriptedHttpResponse {
            status_code: 201,
            body: "{invalid".to_string(),
        }]);
        let runtime = super::HttpOpenCodeBridgeRuntime;
        let invalid_json_error = runtime
            .create_session(
                &invalid_json_server.base_url(),
                OpenCodeSessionConfig::default(),
            )
            .expect_err("invalid json should fail");
        invalid_json_server.join();
        assert!(matches!(
            invalid_json_error,
            CrabError::Serialization {
                context: super::DAEMON_OPENCODE_TRANSPORT_CONTEXT,
                path: None,
                ..
            }
        ));

        let mut empty_body_server = ScriptedHttpServer::start(vec![ScriptedHttpResponse {
            status_code: 201,
            body: String::new(),
        }]);
        let missing_field_error = runtime
            .create_session(
                &empty_body_server.base_url(),
                OpenCodeSessionConfig::default(),
            )
            .expect_err("empty response body should fail required field extraction");
        empty_body_server.join();
        assert_eq!(
            missing_field_error,
            CrabError::InvariantViolation {
                context: "opencode_create_session_response",
                message: "response is missing required field session_id".to_string(),
            }
        );
    }

    #[test]
    fn http_opencode_bridge_runtime_helper_extractors_cover_fallback_paths() {
        let usage_from_strings =
            super::HttpOpenCodeBridgeRuntime::extract_usage(&serde_json::json!({
                "usage": {
                    "input_tokens": "2",
                    "output_tokens": "3",
                    "total_tokens": "5"
                }
            }))
            .expect("string encoded token usage should parse");
        assert_eq!(
            usage_from_strings,
            OpenCodeTokenUsage {
                input_tokens: 2,
                output_tokens: 3,
                total_tokens: 5,
            }
        );
        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_usage(&serde_json::json!({
                "usage": {
                    "output_tokens": 3,
                    "total_tokens": 3
                }
            })),
            None
        );
        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_usage(&serde_json::json!({
                "usage": {
                    "input_tokens": 3,
                    "total_tokens": 3
                }
            })),
            None
        );
        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_usage(&serde_json::json!({
                "usage": {
                    "input_tokens": 4,
                    "output_tokens": 6
                }
            })),
            Some(OpenCodeTokenUsage {
                input_tokens: 4,
                output_tokens: 6,
                total_tokens: 10,
            })
        );

        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_text_delta(&serde_json::json!({
                "output_text": "  text from output  "
            })),
            Some("text from output".to_string())
        );
        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_text_delta(&serde_json::json!({
                "parts": [{"text": {"value": "nested text"}}]
            })),
            Some("nested text".to_string())
        );
        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_text_delta(&serde_json::json!({
                "parts": [{"tool": "shell"}]
            })),
            None
        );
        assert_eq!(
            super::HttpOpenCodeBridgeRuntime::extract_turn_state(&serde_json::json!({})),
            OpenCodeTurnState::Completed
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
                ingested_triggers: 0,
                dispatched_runs: 1,
                heartbeat_cycles: 0
            }
        );

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert!(discord.posted[0].2.contains("Codex bridge response"));

        let session = SessionStore::new(workspace.path.join("state"))
            .get_session("discord:channel:777")
            .expect("session read should succeed")
            .expect("session should exist");
        assert!(session.token_accounting.input_tokens > 0);
        assert_eq!(session.token_accounting.output_tokens, 3);
        assert_eq!(
            session.token_accounting.total_tokens,
            session.token_accounting.input_tokens + session.token_accounting.output_tokens
        );

        let codex_stats = codex_state.stats();
        assert_eq!(codex_stats.spawn_calls, 2);
        assert_eq!(codex_stats.terminate_calls, 2);

        let opencode_stats = opencode_state.stats();
        assert_eq!(opencode_stats.spawn_calls, 1);
        assert_eq!(opencode_stats.terminate_calls, 1);
        assert_eq!(control.slept, vec![5, 5]);
    }

    #[test]
    fn daemon_loop_opencode_default_bridge_uses_backend_events_for_output_and_usage() {
        let mut http_server = ScriptedHttpServer::start(vec![
            ScriptedHttpResponse {
                status_code: 201,
                body: serde_json::json!({
                    "id": "session-op-real"
                })
                .to_string(),
            },
            ScriptedHttpResponse {
                status_code: 200,
                body: serde_json::json!({
                    "id": "turn-op-real",
                    "state": "completed",
                    "message": "completed",
                    "usage": {
                        "input_tokens": 11,
                        "output_tokens": 6,
                        "total_tokens": 17
                    },
                    "parts": [
                        {"type": "text", "text": "OpenCode real backend response"}
                    ]
                })
                .to_string(),
            },
        ]);
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
        let opencode = TrackingOpenCodeProcess::with_server_base_url(http_server.base_url());
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
        assert_eq!(
            discord.posted[0].2,
            "OpenCode real backend response".to_string()
        );
        assert!(!discord.posted[0].2.contains("Crab stub response"));

        let state_root = workspace.path.join("state");
        let session = SessionStore::new(&state_root)
            .get_session("discord:channel:777")
            .expect("session lookup should succeed")
            .expect("session should exist");
        assert_eq!(session.token_accounting.input_tokens, 11);
        assert_eq!(session.token_accounting.output_tokens, 6);
        assert_eq!(session.token_accounting.total_tokens, 17);

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
            event.payload.get("input_tokens") == Some(&"11".to_string())
                && event.payload.get("output_tokens") == Some(&"6".to_string())
                && event.payload.get("total_tokens") == Some(&"17".to_string())
        }));

        http_server.join();
        let requests = http_server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, "POST".to_string());
        assert_eq!(requests[0].path, "/session".to_string());
        assert_eq!(requests[1].method, "POST".to_string());
        assert_eq!(
            requests[1].path,
            "/session/session-op-real/message".to_string()
        );
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
    fn daemon_loop_dispatches_claude_owner_turn_and_shuts_down_claude_session() {
        let workspace = TempWorkspace::new("daemon", "dispatch-claude-owner");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.owner.discord_user_ids = vec!["111".to_string()];
        config.owner.profile_defaults.backend = Some(BackendKind::Claude);
        config.owner.profile_defaults.model = Some("claude-sonnet".to_string());
        config.owner.profile_defaults.reasoning_level = Some(ReasoningLevel::High);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(gateway_message("m-claude", "111", "hello world")))]),
            ..DiscordIoState::default()
        });
        let discord_state = discord.clone();
        let codex = TrackingCodexProcess::new();
        let codex_state = codex.clone();
        let opencode = TrackingOpenCodeProcess::new();
        let opencode_state = opencode.clone();
        let mut control = ScriptedControl::with_now(vec![2_000_000_010_000, 2_000_000_010_001]);

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("Claude owner daemon loop should succeed");
        assert_eq!(stats.dispatched_runs, 1);

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert!(
            discord.posted[0].2.contains("Claude bridge response"),
            "Claude response should be delivered through daemon transport"
        );

        let codex_stats = codex_state.stats();
        assert_eq!(codex_stats.spawn_calls, 1);
        assert_eq!(codex_stats.terminate_calls, 1);

        let opencode_stats = opencode_state.stats();
        assert_eq!(opencode_stats.spawn_calls, 1);
        assert_eq!(opencode_stats.terminate_calls, 1);
    }

    #[test]
    fn daemon_loop_owner_onboarding_capture_emits_conflict_warning_and_run_note() {
        let workspace = TempWorkspace::new("daemon", "onboarding-capture-conflict");
        std::fs::create_dir_all(&workspace.path)
            .expect("workspace root should be creatable for conflict setup");
        std::fs::write(
            workspace.path.join("IDENTITY.md"),
            "# IDENTITY.md\n\n<!-- CRAB:ONBOARDING_MANAGED:START -->\nlegacy",
        )
        .expect("malformed identity markers should be writable for conflict setup");
        std::fs::write(
            workspace.path.join("BOOTSTRAP.md"),
            "Bootstrap remains pending until owner onboarding capture is applied.",
        )
        .expect("bootstrap marker should be writable for pending onboarding setup");

        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.owner.discord_user_ids = vec!["111".to_string()];
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(gateway_dm_message(
                "m-onboarding-capture-daemon",
                "111",
                &onboarding_capture_payload_json(),
            )))]),
            ..DiscordIoState::default()
        });
        let discord_state = discord.clone();
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![2_000_000_020_000, 2_000_000_020_001]);
        let workspace_root_for_runtime = workspace.path.clone();

        let stats = run_daemon_loop_with_transport_and_runtime_builder(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
            move |owner, discord| {
                std::fs::write(
                    workspace_root_for_runtime.join("BOOTSTRAP.md"),
                    "Bootstrap remains pending until owner onboarding capture is applied.",
                )
                .expect("bootstrap marker should be rewritable for onboarding test");
                DaemonTurnRuntime::new(owner, discord)
            },
        )
        .expect("owner onboarding capture should execute in daemon loop");
        assert_eq!(stats.dispatched_runs, 1);

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert!(
            discord.posted[0].2.contains("Onboarding capture applied."),
            "owner onboarding completion should produce confirmation response"
        );
        assert!(
            discord.posted[0].2.contains("Profile conflicts detected"),
            "owner onboarding completion should include conflict warning"
        );

        let run_id = "run:discord:dm:111:m-onboarding-capture-daemon";
        let events = EventStore::new(workspace.path.join("state"))
            .replay_run("discord:dm:111", run_id)
            .expect("event replay should succeed");
        let conflict_event = events
            .iter()
            .find(|event| {
                event.kind == crab_core::EventKind::RunNote
                    && event
                        .payload
                        .get("event")
                        .is_some_and(|value| value == "onboarding_profile_conflicts")
            })
            .expect("conflict run note should be recorded");
        assert!(
            conflict_event
                .payload
                .get("conflict_paths")
                .is_some_and(|value| value.contains("IDENTITY.md")),
            "conflict payload should include the identity file path"
        );
    }

    #[test]
    fn daemon_loop_owner_manual_compact_command_rotates_with_checkpoint_response() {
        let workspace = TempWorkspace::new("daemon", "manual-compact-owner");
        let mut config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        config.owner.discord_user_ids = vec!["111".to_string()];
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(1),
        };
        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(Some(gateway_message(
                "m-manual-compact",
                "111",
                "/compact confirm",
            )))]),
            ..DiscordIoState::default()
        });
        let discord_state = discord.clone();
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
        let mut control = ScriptedControl::with_now(vec![2_000_000_030_000, 2_000_000_030_001]);

        let stats = run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("owner manual compact command should execute in daemon loop");
        assert_eq!(stats.dispatched_runs, 1);

        let discord = discord_state.state();
        assert_eq!(discord.posted.len(), 1);
        assert!(
            discord.posted[0].2.contains("checkpoint:"),
            "manual compact response should include the emitted checkpoint id"
        );
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
        // should repair the lane state to Idle without discarding the physical session handle.
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
        assert_eq!(
            updated.active_physical_session_id,
            Some("phys-1".to_string())
        );
        assert_eq!(updated.lane_state, LaneState::Idle);
    }

    #[test]
    fn daemon_loop_notifies_user_when_startup_reconciliation_recovers_stale_run() {
        let workspace = TempWorkspace::new("daemon", "startup-reconcile-notify");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);

        let state_root = workspace.path.join("state");
        let session_store = SessionStore::new(state_root.clone());
        let run_store = RunStore::new(state_root);

        let logical_session_id = "discord:dm:111";
        let mut session = sample_session(LaneState::Running, Some("phys-1".to_string()));
        session.id = logical_session_id.to_string();
        session.active_backend = BackendKind::Claude;
        session.active_profile = claude_profile();
        session_store
            .upsert_session(&session)
            .expect("seed session should persist");

        let mut run = sample_claude_run("111");
        run.id = "run:discord:dm:111:stale-1".to_string();
        run.logical_session_id = logical_session_id.to_string();
        run.status = RunStatus::Running;
        run.delivery_channel_id = Some("dm-111".to_string());
        run.queued_at_epoch_ms = 1;
        run.started_at_epoch_ms = Some(1);
        run.completed_at_epoch_ms = None;
        run.profile.sender_is_owner = true;
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
        let discord_state = discord.clone();
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();

        // Boot at 200s, tick once at 200s+1ms (run is stale relative to default 90s grace period).
        let mut control = ScriptedControl::with_now(vec![200_000, 200_001]);

        run_daemon_loop_with_transport(
            &config,
            &daemon_config,
            codex,
            opencode,
            discord,
            &mut control,
        )
        .expect("daemon loop should succeed");

        let discord = discord_state.state();
        assert!(discord
            .posted
            .iter()
            .any(|(channel_id, delivery_id, content)| {
                channel_id == "dm-111"
                    && delivery_id.starts_with("startup:recovered:")
                    && content.contains("marked cancelled")
            }));
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

        // Boot at 1s, then jump far enough ahead to exceed CRAB_RUN_STALL_TIMEOUT_SECS (default 600s).
        let mut control = ScriptedControl::with_now(vec![1_000, 701_000]);

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
    fn daemon_loop_polls_and_consumes_pending_triggers() {
        let workspace = TempWorkspace::new("daemon", "trigger-poll");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(2),
        };

        // Seed a pending trigger file before starting the loop.
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(&state_root).expect("state root should be creatable");
        let trigger = crab_core::PendingTrigger {
            channel_id: "777".to_string(),
            message: "scheduled check-in".to_string(),
        };
        let trigger_path =
            crab_core::write_pending_trigger(&state_root, &trigger).expect("write should succeed");
        assert!(trigger_path.exists());

        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(None), Ok(None)]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
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

        assert_eq!(stats.ingested_triggers, 1);
        assert_eq!(stats.dispatched_runs, 1);
        assert!(!trigger_path.exists(), "trigger file should be consumed");
    }

    #[test]
    fn daemon_loop_logs_warning_when_trigger_enqueue_fails() {
        let workspace = TempWorkspace::new("daemon", "trigger-enqueue-fail");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 2);
        let daemon_config = DaemonConfig {
            bot_user_id: "999".to_string(),
            tick_interval_ms: 5,
            max_iterations: Some(2),
        };

        // Seed a trigger file with a blank channel_id that will fail enqueue validation.
        let state_root = workspace.path.join("state");
        std::fs::create_dir_all(state_root.join("pending_triggers"))
            .expect("trigger dir should be creatable");
        let trigger_json = r#"{"channel_id":" ","message":"bad trigger"}"#;
        let trigger_path = state_root.join("pending_triggers/bad-trigger.json");
        std::fs::write(&trigger_path, trigger_json).expect("trigger write should succeed");

        let discord = ScriptedDiscordIo::with_state(DiscordIoState {
            inbound: VecDeque::from([Ok(None), Ok(None)]),
            ..DiscordIoState::default()
        });
        let codex = TrackingCodexProcess::new();
        let opencode = TrackingOpenCodeProcess::new();
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
        .expect("daemon loop should succeed despite trigger enqueue failure");

        // The bad trigger should not have been ingested.
        assert_eq!(stats.ingested_triggers, 0);
        // The trigger file should still exist (not consumed on failure).
        assert!(trigger_path.exists());
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
        let mut codex_lifecycle = NoopCodexLifecycle;

        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut physical,
                &run,
                "turn-1",
                "turn context",
            )
            .expect("delegated backend turn should succeed");
        let events = block_on(events.collect::<Vec<_>>());
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
        let mut codex_lifecycle = NoopCodexLifecycle;

        let error = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut physical,
                &run,
                "turn-2",
                "turn context",
            )
            .err()
            .expect("delegated backend failures should be mapped");
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
        let mut codex_lifecycle = NoopCodexLifecycle;

        let error = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut physical,
                &run,
                "turn-3",
                "turn context",
            )
            .err()
            .expect("depleted scripted bridge results should fail");
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
        let mut codex_lifecycle = NoopCodexLifecycle;

        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut physical,
                &run,
                "turn-4",
                "turn context",
            )
            .expect("mutating bridge turn should succeed");
        let _ = block_on(events.collect::<Vec<_>>());
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
    fn daemon_runtime_execute_backend_turn_preserves_backend_native_turn_id() {
        let workspace = TempWorkspace::new("daemon", "backend-bridge-turn-id-preserve");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let run = sample_run("non-owner");
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::<TurnIdMutatingBackendBridge>::default(),
        )
        .expect("runtime builds");
        assert!(!runtime.has_opencode_backend_bridge());
        runtime.configure_opencode_backend_bridge_trusted(
            "http://127.0.0.1:9999".to_string(),
            Box::new(ScriptedOpenCodeBridgeRuntime::with_results(
                vec![Ok("ignored".to_string())],
                vec![Ok(scripted_opencode_turn_result(
                    "ignored-turn",
                    "ignored",
                    (1, 1, 2),
                    OpenCodeTurnState::Completed,
                ))],
            )),
        );
        let mut physical = runtime
            .ensure_physical_session(&run.logical_session_id, &run.profile.resolved_profile, None)
            .expect("physical session should resolve");
        let active_id = physical.id.clone();
        let mut codex_lifecycle = NoopCodexLifecycle;

        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut physical,
                &run,
                "turn-daemon",
                "turn context",
            )
            .expect("turn execution should succeed");
        let _ = block_on(events.collect::<Vec<_>>());
        assert_eq!(physical.last_turn_id, Some("backend-turn-id-9".to_string()));

        let cached = runtime
            .ensure_physical_session(
                &run.logical_session_id,
                &run.profile.resolved_profile,
                Some(&active_id),
            )
            .expect("cached physical session should resolve");
        assert_eq!(cached.last_turn_id, Some("backend-turn-id-9".to_string()));
    }

    #[test]
    fn daemon_runtime_opencode_test_config_rejects_non_default_backend_bridge() {
        let workspace = TempWorkspace::new("daemon", "opencode-test-config-reject-non-default");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::new(ScriptedBackendBridge::with_results(Vec::new())),
        )
        .expect("runtime builds");

        assert!(!runtime.has_opencode_backend_bridge());

        let error = runtime
            .configure_opencode_backend_bridge(
                "http://127.0.0.1:9999".to_string(),
                Box::new(ScriptedOpenCodeBridgeRuntime::with_results(
                    vec![Ok("ignored".to_string())],
                    vec![Ok(scripted_opencode_turn_result(
                        "ignored-turn",
                        "ignored output",
                        (1, 1, 2),
                        OpenCodeTurnState::Completed,
                    ))],
                )),
            )
            .expect_err("non-default bridge should reject opencode test configuration");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_backend_bridge",
                message: "runtime backend bridge does not support opencode test configuration"
                    .to_string(),
            }
        );
    }

    #[test]
    fn daemon_runtime_opencode_trusted_config_ignores_non_default_backend_bridge() {
        let workspace = TempWorkspace::new("daemon", "opencode-trusted-config-ignore-non-default");
        let config = runtime_config_for_workspace_with_lanes(&workspace.path, 1);
        let discord = ScriptedDiscordIo::with_state(DiscordIoState::default());
        let mut runtime = DaemonTurnRuntime::new_with_backend_bridge(
            config.owner.clone(),
            discord,
            Box::<MutatingBackendBridge>::default(),
        )
        .expect("runtime builds");

        assert!(!runtime.has_opencode_backend_bridge());

        runtime.configure_opencode_backend_bridge_trusted(
            "http://127.0.0.1:9999".to_string(),
            Box::new(ScriptedOpenCodeBridgeRuntime::with_results(
                vec![Ok("ignored".to_string())],
                vec![Ok(scripted_opencode_turn_result(
                    "ignored-turn",
                    "ignored output",
                    (1, 1, 2),
                    OpenCodeTurnState::Completed,
                ))],
            )),
        );

        assert!(!runtime.has_opencode_backend_bridge());
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
        let mut codex_lifecycle = NoopCodexLifecycle;
        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-cleanup",
                "cleanup context",
            )
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
        let mut codex_lifecycle = NoopCodexLifecycle;
        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-cleanup",
                "cleanup context",
            )
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
        let mut codex_lifecycle = NoopCodexLifecycle;
        let events = runtime
            .execute_backend_turn(
                &mut codex_lifecycle,
                &mut session,
                &run,
                "turn-cleanup",
                "cleanup context",
            )
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
    fn interrupt_backend_turn_skips_non_claude_backends() {
        let claude_process =
            ScriptedClaudeProcess::with_scripted(Vec::new(), Vec::new(), Vec::new(), Vec::new());
        let (_workspace, mut runtime, process) =
            build_scripted_claude_runtime("interrupt-non-claude-backend", claude_process);

        let session = crab_core::PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            backend: BackendKind::Codex,
            backend_session_id: "codex-session-1".to_string(),
            created_at_epoch_ms: 1_000_000,
            last_turn_id: None,
        };

        runtime
            .interrupt_backend_turn(&session, "turn-1")
            .expect("interrupt should succeed (no-op) for non-Claude backend");

        let stats = process.stats();
        assert_eq!(
            stats.interrupt_calls, 0,
            "Claude process should not be called for non-Claude backend"
        );
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
