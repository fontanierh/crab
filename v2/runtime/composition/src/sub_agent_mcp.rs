//! Native MCP tools backed by Crab's authenticated sub-agent control plane.

use std::{env, fmt, path::PathBuf};

use agent_client_protocol::{ConnectTo as _, Error, mcp_server::McpServer, role::mcp};
use agent_client_protocol_rmcp::{McpServerExt as _, McpTool};
use agent_host_contract::{SessionReference, SessionStatus};
use agent_host_implementation::{
    CRAB_AGENT_ID_ENV, CRAB_PARENT_SESSION_ID_ENV, CRAB_SESSION_ID_ENV, CRAB_STATE_DIRECTORY_ENV,
    CRAB_SUB_AGENT_ID_ENV, CRAB_WORKING_DIRECTORY_ENV,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sub_agent_host_contract::{
    ContextRealization, InputDisposition, InteractionReceipt, ReadSubAgentEventsRequest,
    SendToChildRequest, SendToParentRequest, SpawnSubAgentRequest, StopSubAgentRequest,
    SubAgentContextMode, SubAgentEventKind, SubAgentEventPage, SubAgentInputMode,
    SubAgentLifecycle, SubAgentReceipt, SubAgentRecord, SubAgentReference, SubAgentStatus,
};

use crate::{ChannelIpcClient, ChannelIpcClientError, native_stdio};

const SERVER_NAME: &str = "crab-sub-agents";
const MAX_EVENT_PAGE: u64 = 1_000;
const DEFAULT_CRASH_RESTART_LIMIT: u64 = 1;

/// Failure to start the Crab sub-agent MCP server from its session context.
#[derive(Debug)]
pub enum SubAgentMcpError {
    /// Required Crab-owned session context was absent or malformed.
    InvalidSessionContext,
    /// The MCP stdio transport stopped unexpectedly.
    Transport,
}

impl fmt::Display for SubAgentMcpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSessionContext => {
                formatter.write_str("Crab MCP session context is unavailable")
            }
            Self::Transport => formatter.write_str("Crab MCP stdio transport failed"),
        }
    }
}

impl std::error::Error for SubAgentMcpError {}

/// Serve six non-blocking sub-agent tools over standard MCP stdio.
pub async fn run_sub_agent_mcp_stdio() -> Result<(), SubAgentMcpError> {
    let context = SessionContext::from_environment()?;
    let server = McpServer::<mcp::Client>::builder(SERVER_NAME)
        .instructions(
            "Spawn and coordinate separately supervised Crab ACP sub-agents. Calls acknowledge \
             acceptance; use read_sub_agent_events for realtime progress and completion.",
        )
        .tool(SpawnTool(context.clone()))
        .tool(SendChildTool(context.clone()))
        .tool(SendParentTool(context.clone()))
        .tool(ReadEventsTool(context.clone()))
        .tool(StatusTool(context.clone()))
        .tool(StopTool(context))
        .build();
    server
        .connect_to(native_stdio())
        .await
        .map_err(|_| SubAgentMcpError::Transport)
}

#[derive(Clone)]
struct SessionContext {
    state_directory: PathBuf,
    session_id: String,
    agent_id: String,
    working_directory: PathBuf,
    child: Option<ChildContext>,
}

#[derive(Clone)]
struct ChildContext {
    sub_agent_id: String,
    parent_session_id: String,
}

impl SessionContext {
    fn from_environment() -> Result<Self, SubAgentMcpError> {
        let state_directory = required_path(CRAB_STATE_DIRECTORY_ENV)?;
        let working_directory = required_path(CRAB_WORKING_DIRECTORY_ENV)?;
        let session_id = required_text(CRAB_SESSION_ID_ENV)?;
        let agent_id = required_text(CRAB_AGENT_ID_ENV)?;
        let sub_agent_id = optional_text(CRAB_SUB_AGENT_ID_ENV)?;
        let parent_session_id = optional_text(CRAB_PARENT_SESSION_ID_ENV)?;
        let child = match (sub_agent_id, parent_session_id) {
            (Some(sub_agent_id), Some(parent_session_id)) => Some(ChildContext {
                sub_agent_id,
                parent_session_id,
            }),
            (None, None) => None,
            _ => return Err(SubAgentMcpError::InvalidSessionContext),
        };
        Ok(Self {
            state_directory,
            session_id,
            agent_id,
            working_directory,
            child,
        })
    }

    fn client(&self) -> Result<ChannelIpcClient, Error> {
        ChannelIpcClient::from_state_directory(&self.state_directory).map_err(ipc_error)
    }
}

fn required_text(name: &'static str) -> Result<String, SubAgentMcpError> {
    env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or(SubAgentMcpError::InvalidSessionContext)
}

fn optional_text(name: &'static str) -> Result<Option<String>, SubAgentMcpError> {
    match env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) => Err(SubAgentMcpError::InvalidSessionContext),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(env::VarError::NotUnicode(_)) => Err(SubAgentMcpError::InvalidSessionContext),
    }
}

fn required_path(name: &'static str) -> Result<PathBuf, SubAgentMcpError> {
    let path = PathBuf::from(required_text(name)?);
    if !path.is_absolute() {
        return Err(SubAgentMcpError::InvalidSessionContext);
    }
    Ok(path)
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SpawnInput {
    /// Stable caller key used to deduplicate a retry.
    client_id: String,
    /// ACP agent ID; omitted means use the current parent agent.
    #[serde(default)]
    agent_id: Option<String>,
    /// Child workspace; omitted means use the current parent workspace.
    #[serde(default)]
    working_directory: Option<PathBuf>,
    /// Start fresh or inherit the complete visible parent history through the current cursor.
    context: SpawnContext,
    /// Exact ACP content blocks for the child task.
    native_task_prompt: Vec<Value>,
    /// Additional non-secret child metadata.
    #[serde(default)]
    metadata: Map<String, Value>,
    /// Number of later runtime crashes across which Crab may resume this exact native session.
    #[serde(default = "default_crash_restart_limit")]
    crash_restart_limit: u64,
}

const fn default_crash_restart_limit() -> u64 {
    DEFAULT_CRASH_RESTART_LIMIT
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
enum SpawnContext {
    Fresh,
    Inherit,
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
enum InputMode {
    Queue,
    Steer,
    InterruptAndSteer,
}

impl From<InputMode> for SubAgentInputMode {
    fn from(value: InputMode) -> Self {
        match value {
            InputMode::Queue => Self::Queue,
            InputMode::Steer => Self::Steer,
            InputMode::InterruptAndSteer => Self::InterruptAndSteer,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendChildInput {
    sub_agent_id: String,
    message_id: String,
    mode: InputMode,
    /// Exact ACP content blocks delivered to the child.
    native_prompt: Vec<Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SendParentInput {
    message_id: String,
    mode: InputMode,
    /// Structured progress or result delivered to this child's parent.
    message: Map<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReadEventsInput {
    sub_agent_id: String,
    #[serde(default)]
    after_sequence: u64,
    #[serde(default = "default_event_limit")]
    limit: u64,
}

const fn default_event_limit() -> u64 {
    100
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StatusInput {
    sub_agent_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StopInput {
    sub_agent_id: String,
    reason: String,
}

#[derive(Clone)]
struct SpawnTool(SessionContext);

impl McpTool<mcp::Client> for SpawnTool {
    type Input = SpawnInput;
    type Output = Value;

    fn name(&self) -> String {
        "spawn_sub_agent".into()
    }

    fn description(&self) -> String {
        "Spawn a separately supervised ACP child with fresh or inherited visible context and a bounded native-session restart policy; returns immediately after its initial task is accepted.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.client_id)?;
        let client = self.0.client()?;
        let (context_mode, boundary, allow_portable_snapshot) = match input.context {
            SpawnContext::Fresh => (SubAgentContextMode::Fresh, None, false),
            SpawnContext::Inherit => {
                let SessionStatus { last_sequence, .. } = client
                    .agent_session_status(SessionReference {
                        session_id: self.0.session_id.clone(),
                    })
                    .await
                    .map_err(ipc_error)?;
                (
                    SubAgentContextMode::InheritParent,
                    Some(last_sequence),
                    true,
                )
            }
        };
        let agent_id = nonempty_or(input.agent_id, &self.0.agent_id)?;
        let working_directory = input
            .working_directory
            .unwrap_or_else(|| self.0.working_directory.clone());
        if !working_directory.is_absolute() {
            return Err(invalid_input());
        }
        let record = client
            .spawn_sub_agent(SpawnSubAgentRequest {
                client_sub_agent_id: input.client_id,
                parent_session_id: self.0.session_id.clone(),
                agent_id,
                working_directory: working_directory.to_string_lossy().into_owned(),
                context_mode,
                parent_context_through_sequence: boundary,
                allow_portable_snapshot,
                native_task_prompt_json: serde_json::to_string(&input.native_task_prompt)
                    .map_err(|_| invalid_input())?,
                metadata_json: serde_json::to_string(&input.metadata)
                    .map_err(|_| invalid_input())?,
                crash_restart_limit: input.crash_restart_limit,
            })
            .await
            .map_err(ipc_error)?;
        Ok(record_json(record))
    }
}

#[derive(Clone)]
struct SendChildTool(SessionContext);

impl McpTool<mcp::Client> for SendChildTool {
    type Input = SendChildInput;
    type Output = Value;

    fn name(&self) -> String {
        "send_to_sub_agent".into()
    }

    fn description(&self) -> String {
        "Queue, steer, or interrupt-and-steer an existing child without waiting for model completion.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.sub_agent_id)?;
        validate_text(&input.message_id)?;
        self.0
            .client()?
            .send_to_child(SendToChildRequest {
                sub_agent_id: input.sub_agent_id,
                client_message_id: input.message_id,
                mode: input.mode.into(),
                native_prompt_json: serde_json::to_string(&input.native_prompt)
                    .map_err(|_| invalid_input())?,
            })
            .await
            .map(interaction_json)
            .map_err(ipc_error)
    }
}

#[derive(Clone)]
struct SendParentTool(SessionContext);

impl McpTool<mcp::Client> for SendParentTool {
    type Input = SendParentInput;
    type Output = Value;

    fn name(&self) -> String {
        "send_to_parent".into()
    }

    fn description(&self) -> String {
        "Send structured progress or a result from this child to its parent using queue, steer, or interrupt-and-steer.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.message_id)?;
        let child = self.0.child.as_ref().ok_or_else(child_only)?;
        debug_assert!(!child.parent_session_id.is_empty());
        self.0
            .client()?
            .send_to_parent(SendToParentRequest {
                sub_agent_id: child.sub_agent_id.clone(),
                client_message_id: input.message_id,
                mode: input.mode.into(),
                message_json: serde_json::to_string(&input.message).map_err(|_| invalid_input())?,
            })
            .await
            .map(interaction_json)
            .map_err(ipc_error)
    }
}

#[derive(Clone)]
struct ReadEventsTool(SessionContext);

impl McpTool<mcp::Client> for ReadEventsTool {
    type Input = ReadEventsInput;
    type Output = Value;

    fn name(&self) -> String {
        "read_sub_agent_events".into()
    }

    fn description(&self) -> String {
        "Read ordered lifecycle, message, thought, tool, terminal, diff, usage, and completion events after an exclusive cursor.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.sub_agent_id)?;
        if input.limit == 0 || input.limit > MAX_EVENT_PAGE {
            return Err(invalid_input());
        }
        let page = self
            .0
            .client()?
            .read_sub_agent_events(ReadSubAgentEventsRequest {
                sub_agent_id: input.sub_agent_id,
                after_sequence: input.after_sequence,
                limit: input.limit,
            })
            .await
            .map_err(ipc_error)?;
        events_json(page)
    }
}

#[derive(Clone)]
struct StatusTool(SessionContext);

impl McpTool<mcp::Client> for StatusTool {
    type Input = StatusInput;
    type Output = Value;

    fn name(&self) -> String {
        "sub_agent_status".into()
    }

    fn description(&self) -> String {
        "Inspect one child lifecycle, context realization, event cursor, and pending bidirectional inputs.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.sub_agent_id)?;
        self.0
            .client()?
            .sub_agent_status(SubAgentReference {
                sub_agent_id: input.sub_agent_id,
            })
            .await
            .map(status_json)
            .map_err(ipc_error)
    }
}

#[derive(Clone)]
struct StopTool(SessionContext);

impl McpTool<mcp::Client> for StopTool {
    type Input = StopInput;
    type Output = Value;

    fn name(&self) -> String {
        "stop_sub_agent".into()
    }

    fn description(&self) -> String {
        "Cooperatively stop one child; retrying a terminal stop is safe and reports accepted false."
            .into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.sub_agent_id)?;
        validate_text(&input.reason)?;
        self.0
            .client()?
            .stop_sub_agent(StopSubAgentRequest {
                sub_agent_id: input.sub_agent_id,
                reason: input.reason,
            })
            .await
            .map(receipt_json)
            .map_err(ipc_error)
    }
}

fn validate_text(value: &str) -> Result<(), Error> {
    if value.trim().is_empty() {
        Err(invalid_input())
    } else {
        Ok(())
    }
}

fn nonempty_or(value: Option<String>, fallback: &str) -> Result<String, Error> {
    let value = value.unwrap_or_else(|| fallback.to_owned());
    validate_text(&value)?;
    Ok(value)
}

fn invalid_input() -> Error {
    Error::invalid_params().data(json!({"kind":"contract","code":"InvalidInput"}))
}

fn child_only() -> Error {
    Error::invalid_params().data(json!({"kind":"domain","code":"ChildSessionRequired"}))
}

fn invalid_output() -> Error {
    Error::internal_error().data(json!({"kind":"internal","code":"InvalidProviderOutput"}))
}

fn ipc_error(error: ChannelIpcClientError) -> Error {
    let (kind, code) = match error {
        ChannelIpcClientError::Io(_) => ("transport".to_owned(), "Unavailable".to_owned()),
        ChannelIpcClientError::Timeout => ("transport".to_owned(), "Timeout".to_owned()),
        ChannelIpcClientError::Protocol(stage) => ("protocol".to_owned(), stage.to_owned()),
        ChannelIpcClientError::Remote { kind, code } => (kind, code),
    };
    Error::internal_error().data(json!({"kind":kind,"code":code}))
}

fn record_json(record: SubAgentRecord) -> Value {
    json!({
        "subAgentId": record.sub_agent_id,
        "parentSessionId": record.parent_session_id,
        "childSessionId": record.child_session_id,
        "nativeChildSessionId": record.native_child_session_id,
        "agentId": record.agent_id,
        "lifecycle": lifecycle_name(&record.lifecycle),
        "contextMode": context_name(&record.context_mode),
        "contextRealization": realization_name(&record.context_realization),
        "contextThroughSequence": record.context_through_sequence,
        "processIdentity": record.process_identity,
        "startedAtMs": record.started_at_ms,
    })
}

fn interaction_json(receipt: InteractionReceipt) -> Value {
    json!({
        "subAgentId": receipt.sub_agent_id,
        "clientMessageId": receipt.client_message_id,
        "disposition": disposition_name(&receipt.disposition),
        "acceptedAtMs": receipt.accepted_at_ms,
    })
}

fn events_json(page: SubAgentEventPage) -> Result<Value, Error> {
    let events = page
        .events
        .into_iter()
        .map(|event| {
            Ok(json!({
                "subAgentId": event.sub_agent_id,
                "sequence": event.sequence,
                "observedAtMs": event.observed_at_ms,
                "kind": event_kind_name(&event.kind),
                "payload": serde_json::from_str::<Value>(&event.payload_json)
                    .map_err(|_| invalid_output())?,
            }))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(json!({
        "events": events,
        "nextSequence": page.next_sequence,
        "caughtUp": page.caught_up,
    }))
}

fn status_json(status: SubAgentStatus) -> Value {
    json!({
        "record": record_json(status.record),
        "lastSequence": status.last_sequence,
        "pendingParentToChild": status.pending_parent_to_child,
        "pendingChildToParent": status.pending_child_to_parent,
        "restartCount": status.restart_count,
        "lastError": status.last_error,
    })
}

fn receipt_json(receipt: SubAgentReceipt) -> Value {
    json!({"accepted":receipt.accepted,"recordedAtMs":receipt.recorded_at_ms})
}

fn lifecycle_name(value: &SubAgentLifecycle) -> &'static str {
    match value {
        SubAgentLifecycle::Starting => "starting",
        SubAgentLifecycle::Running => "running",
        SubAgentLifecycle::Idle => "idle",
        SubAgentLifecycle::Stopping => "stopping",
        SubAgentLifecycle::Completed => "completed",
        SubAgentLifecycle::Failed => "failed",
        SubAgentLifecycle::Unknown { .. } => "unknown",
    }
}

fn context_name(value: &SubAgentContextMode) -> &'static str {
    match value {
        SubAgentContextMode::Fresh => "fresh",
        SubAgentContextMode::InheritParent => "inherit-parent",
        SubAgentContextMode::Unknown { .. } => "unknown",
    }
}

fn realization_name(value: &ContextRealization) -> &'static str {
    match value {
        ContextRealization::FreshSession => "fresh-session",
        ContextRealization::NativeAcpFork => "native-acp-fork",
        ContextRealization::PortableSnapshot => "portable-snapshot",
        ContextRealization::Unknown { .. } => "unknown",
    }
}

fn disposition_name(value: &InputDisposition) -> &'static str {
    match value {
        InputDisposition::StartedForegroundWork => "started-foreground-work",
        InputDisposition::ContributedToActiveWork => "contributed-to-active-work",
        InputDisposition::QueuedForTurnBoundary => "queued-for-turn-boundary",
        InputDisposition::CancelRequestedThenQueued => "cancel-requested-then-queued",
        InputDisposition::Unknown { .. } => "unknown",
    }
}

fn event_kind_name(value: &SubAgentEventKind) -> &'static str {
    match value {
        SubAgentEventKind::Lifecycle => "lifecycle",
        SubAgentEventKind::NativeAcp => "native-acp",
        SubAgentEventKind::ParentToChild => "parent-to-child",
        SubAgentEventKind::ChildToParent => "child-to-parent",
        SubAgentEventKind::Compaction => "compaction",
        SubAgentEventKind::Failed => "failed",
        SubAgentEventKind::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::{InputMode, ReadEventsInput, SpawnContext, default_event_limit};

    #[test]
    fn tool_enums_and_cursor_defaults_are_explicit() {
        assert!(matches!(
            serde_json::from_str::<SpawnContext>(r#""inherit""#).expect("context"),
            SpawnContext::Inherit
        ));
        assert!(matches!(
            serde_json::from_str::<InputMode>(r#""interrupt-and-steer""#).expect("mode"),
            InputMode::InterruptAndSteer
        ));
        let input: ReadEventsInput =
            serde_json::from_str(r#"{"subAgentId":"child"}"#).expect("defaulted cursor request");
        assert_eq!(input.after_sequence, 0);
        assert_eq!(input.limit, default_event_limit());
    }
}
