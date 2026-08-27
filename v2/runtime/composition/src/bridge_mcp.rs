//! Native MCP tools backed by Crab's authenticated bridge control plane.

use std::{collections::HashSet, env, fmt, path::PathBuf};

use agent_client_protocol::{ConnectTo as _, Error, Stdio, mcp_server::McpServer, role::mcp};
use agent_client_protocol_rmcp::{McpServerExt as _, McpTool};
use agent_host_implementation::CRAB_STATE_DIRECTORY_ENV;
use bridge_host_contract::{
    AuthenticationChallenge, AuthenticationMethod, BeginAuthenticationRequest, BridgeAlertTarget,
    BridgeAttachment, BridgeCatalog, BridgeIngressMode, BridgeLifecycle, BridgeOutbound,
    BridgeReceipt, BridgeRecord, BridgeReference, BridgeSpec, BridgeStatus, CredentialLifecycle,
    CredentialStatus, DeliveryLifecycle, DeliveryReceipt, DeliveryReference,
    ImportBridgeContentRequest, ImportedBridgeContent, ReconcileBridgeRequest,
    ReplaceBridgeRequest, SubmitAuthenticationRequest,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::{ChannelIpcClient, ChannelIpcClientError};

const SERVER_NAME: &str = "crab-bridges";

/// Failure to start the Crab bridge MCP server from its session context.
#[derive(Debug)]
pub enum BridgeMcpError {
    /// The Crab state directory was absent or not absolute.
    InvalidSessionContext,
    /// The MCP stdio transport stopped unexpectedly.
    Transport,
}

impl fmt::Display for BridgeMcpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidSessionContext => {
                formatter.write_str("Crab MCP session context is unavailable")
            }
            Self::Transport => formatter.write_str("Crab MCP stdio transport failed"),
        }
    }
}

impl std::error::Error for BridgeMcpError {}

/// Serve the complete safe agent bridge lifecycle over standard MCP stdio.
pub async fn run_bridge_mcp_stdio() -> Result<(), BridgeMcpError> {
    let context = BridgeContext::from_environment()?;
    let server = McpServer::<mcp::Client>::builder(SERVER_NAME)
        .instructions(
            "Install and operate Crab bridges under generic supervision. Bridge output must be \
             deliberately selected; never mirror the native ACP event stream. Configuration must \
             not contain raw credential values.",
        )
        .tool(RegisterTool(context.clone()))
        .tool(ListTool(context.clone()))
        .tool(ReplaceTool(context.clone()))
        .tool(ReconcileTool(context.clone()))
        .tool(ReferenceTool::new(
            context.clone(),
            ReferenceOperation::Status,
        ))
        .tool(BeginAuthenticationTool(context.clone()))
        .tool(SubmitAuthenticationTool(context.clone()))
        .tool(ReferenceTool::new(
            context.clone(),
            ReferenceOperation::ValidateCredentials,
        ))
        .tool(ReferenceTool::new(
            context.clone(),
            ReferenceOperation::InvalidateCredentials,
        ))
        .tool(ImportContentTool(context.clone()))
        .tool(DeliverTool(context.clone()))
        .tool(DeliveryStatusTool(context.clone()))
        .tool(ReferenceTool::new(
            context.clone(),
            ReferenceOperation::Suspend,
        ))
        .tool(ReferenceTool::new(context, ReferenceOperation::Stop))
        .build();
    server
        .connect_to(Stdio::new())
        .await
        .map_err(|_| BridgeMcpError::Transport)
}

#[derive(Clone)]
struct BridgeContext {
    state_directory: PathBuf,
}

impl BridgeContext {
    fn from_environment() -> Result<Self, BridgeMcpError> {
        let value = env::var(CRAB_STATE_DIRECTORY_ENV)
            .ok()
            .filter(|value| !value.trim().is_empty())
            .ok_or(BridgeMcpError::InvalidSessionContext)?;
        let state_directory = PathBuf::from(value);
        if !state_directory.is_absolute() {
            return Err(BridgeMcpError::InvalidSessionContext);
        }
        Ok(Self { state_directory })
    }

    fn client(&self) -> Result<ChannelIpcClient, Error> {
        ChannelIpcClient::from_state_directory(&self.state_directory).map_err(ipc_error)
    }
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
enum AuthenticationMethodInput {
    QrCode,
    PhoneCode,
    Oauth,
    Browser,
    Terminal,
    Manual,
}

impl From<AuthenticationMethodInput> for AuthenticationMethod {
    fn from(value: AuthenticationMethodInput) -> Self {
        match value {
            AuthenticationMethodInput::QrCode => Self::QrCode,
            AuthenticationMethodInput::PhoneCode => Self::PhoneCode,
            AuthenticationMethodInput::Oauth => Self::OAuth,
            AuthenticationMethodInput::Browser => Self::Browser,
            AuthenticationMethodInput::Terminal => Self::Terminal,
            AuthenticationMethodInput::Manual => Self::Manual,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
enum IngressModeInput {
    Queue,
    Steer,
    InterruptAndSteer,
}

impl From<IngressModeInput> for BridgeIngressMode {
    fn from(value: IngressModeInput) -> Self {
        match value {
            IngressModeInput::Queue => Self::Queue,
            IngressModeInput::Steer => Self::Steer,
            IngressModeInput::InterruptAndSteer => Self::InterruptAndSteer,
        }
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct LaunchInput {
    executable: PathBuf,
    #[serde(default)]
    arguments: Vec<String>,
    working_directory: PathBuf,
    #[serde(default)]
    environment_names: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AlertTargetInput {
    channel_id: String,
    lane: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BridgeSpecInput {
    bridge_id: String,
    package_id: String,
    display_name: String,
    launch: LaunchInput,
    /// Service configuration without raw credential values.
    #[serde(default)]
    configuration: Map<String, Value>,
    #[serde(default)]
    authentication_methods: Vec<AuthenticationMethodInput>,
    ingress_mode: IngressModeInput,
    #[serde(default)]
    alert_target: Option<AlertTargetInput>,
    desired_running: bool,
    health_interval_ms: u64,
    credential_validation_interval_ms: u64,
    restart_limit: u64,
    restart_window_ms: u64,
}

impl BridgeSpecInput {
    fn into_contract(self) -> Result<BridgeSpec, Error> {
        validate_text(&self.bridge_id)?;
        validate_text(&self.package_id)?;
        validate_text(&self.display_name)?;
        if !self.launch.executable.is_absolute()
            || !self.launch.working_directory.is_absolute()
            || self.health_interval_ms == 0
            || self.credential_validation_interval_ms == 0
            || self.restart_limit == 0
            || self.restart_window_ms == 0
            || !valid_environment_names(&self.launch.environment_names)
            || self.alert_target.as_ref().is_some_and(|target| {
                target.channel_id.trim().is_empty() || target.lane.trim().is_empty()
            })
        {
            return Err(invalid_input());
        }
        let methods = self
            .authentication_methods
            .into_iter()
            .map(AuthenticationMethod::from)
            .collect::<Vec<_>>();
        if methods
            .iter()
            .map(method_name)
            .collect::<HashSet<_>>()
            .len()
            != methods.len()
        {
            return Err(invalid_input());
        }
        let launch_json = serde_json::to_string(&json!({
            "executable": self.launch.executable,
            "arguments": self.launch.arguments,
            "workingDirectory": self.launch.working_directory,
            "environmentNames": self.launch.environment_names,
        }))
        .map_err(|_| invalid_input())?;
        Ok(BridgeSpec {
            bridge_id: self.bridge_id,
            package_id: self.package_id,
            display_name: self.display_name,
            launch_json,
            configuration_json: serde_json::to_string(&self.configuration)
                .map_err(|_| invalid_input())?,
            authentication_methods: methods,
            ingress_mode: self.ingress_mode.into(),
            alert_target: self.alert_target.map(|target| BridgeAlertTarget {
                channel_id: target.channel_id,
                lane: target.lane,
            }),
            desired_running: self.desired_running,
            health_interval_ms: self.health_interval_ms,
            credential_validation_interval_ms: self.credential_validation_interval_ms,
            restart_limit: self.restart_limit,
            restart_window_ms: self.restart_window_ms,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EmptyInput {}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReplaceInput {
    expected_generation: u64,
    spec: BridgeSpecInput,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReconcileInput {
    bridge_id: String,
    expected_generation: u64,
    desired_running: bool,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ReferenceInput {
    bridge_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BeginAuthenticationInput {
    bridge_id: String,
    #[serde(default)]
    preferred_method: Option<AuthenticationMethodInput>,
    #[serde(default)]
    context: Map<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SubmitAuthenticationInput {
    bridge_id: String,
    challenge_id: String,
    #[serde(default)]
    response: Map<String, Value>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AttachmentInput {
    media_type: String,
    #[serde(default)]
    name: Option<String>,
    content_handle: String,
}

impl AttachmentInput {
    fn into_contract(self) -> Result<BridgeAttachment, Error> {
        validate_text(&self.media_type)?;
        validate_text(&self.content_handle)?;
        if self
            .name
            .as_ref()
            .is_some_and(|name| name.trim().is_empty())
        {
            return Err(invalid_input());
        }
        Ok(BridgeAttachment {
            media_type: self.media_type,
            name: self.name,
            content_handle: self.content_handle,
        })
    }
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeliverInput {
    bridge_id: String,
    message_id: String,
    destination: Map<String, Value>,
    message: Value,
    #[serde(default)]
    attachments: Vec<AttachmentInput>,
    /// Omitted defaults to the stable message ID.
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ImportContentInput {
    bridge_id: String,
    import_id: String,
    source_path: PathBuf,
    media_type: String,
    #[serde(default)]
    name: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DeliveryStatusInput {
    bridge_id: String,
    message_id: String,
}

#[derive(Clone)]
struct RegisterTool(BridgeContext);

impl McpTool<mcp::Client> for RegisterTool {
    type Input = BridgeSpecInput;
    type Output = Value;

    fn name(&self) -> String {
        "register_bridge".into()
    }

    fn description(&self) -> String {
        "Register a strict secret-free bridge package. Prefer desiredRunning false until the package is installed and ready.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        self.0
            .client()?
            .register_bridge(input.into_contract()?)
            .await
            .map(record_json)
            .map_err(ipc_error)
    }
}

#[derive(Clone)]
struct ListTool(BridgeContext);

impl McpTool<mcp::Client> for ListTool {
    type Input = EmptyInput;
    type Output = Value;

    fn name(&self) -> String {
        "list_bridges".into()
    }

    fn description(&self) -> String {
        "List non-secret durable bridge registrations and generations.".into()
    }

    async fn call_tool(
        &self,
        _input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        self.0
            .client()?
            .list_bridges()
            .await
            .map(catalog_json)
            .map_err(ipc_error)
    }
}

#[derive(Clone)]
struct ReplaceTool(BridgeContext);

impl McpTool<mcp::Client> for ReplaceTool {
    type Input = ReplaceInput;
    type Output = Value;

    fn name(&self) -> String {
        "replace_bridge".into()
    }

    fn description(&self) -> String {
        "Replace bridge package, configuration, ingress policy, or restart policy using generation compare-and-swap.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        self.0
            .client()?
            .replace_bridge(ReplaceBridgeRequest {
                expected_generation: input.expected_generation,
                spec: input.spec.into_contract()?,
            })
            .await
            .map(record_json)
            .map_err(ipc_error)
    }
}

#[derive(Clone)]
struct ReconcileTool(BridgeContext);

impl McpTool<mcp::Client> for ReconcileTool {
    type Input = ReconcileInput;
    type Output = Value;

    fn name(&self) -> String {
        "reconcile_bridge".into()
    }

    fn description(&self) -> String {
        "Converge a registered generation toward running or stopped under bounded recovery.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        self.0
            .client()?
            .reconcile_bridge(ReconcileBridgeRequest {
                bridge_id: input.bridge_id,
                expected_generation: input.expected_generation,
                desired_running: input.desired_running,
            })
            .await
            .map(status_json)
            .map_err(ipc_error)?
    }
}

#[derive(Clone, Copy)]
enum ReferenceOperation {
    Status,
    ValidateCredentials,
    InvalidateCredentials,
    Suspend,
    Stop,
}

#[derive(Clone)]
struct ReferenceTool {
    context: BridgeContext,
    operation: ReferenceOperation,
}

impl ReferenceTool {
    fn new(context: BridgeContext, operation: ReferenceOperation) -> Self {
        Self { context, operation }
    }
}

impl McpTool<mcp::Client> for ReferenceTool {
    type Input = ReferenceInput;
    type Output = Value;

    fn name(&self) -> String {
        match self.operation {
            ReferenceOperation::Status => "bridge_status",
            ReferenceOperation::ValidateCredentials => "validate_bridge_credentials",
            ReferenceOperation::InvalidateCredentials => "invalidate_bridge_credentials",
            ReferenceOperation::Suspend => "suspend_bridge",
            ReferenceOperation::Stop => "stop_bridge",
        }
        .into()
    }

    fn description(&self) -> String {
        match self.operation {
            ReferenceOperation::Status => "Inspect truthful lifecycle, health, and restart state.",
            ReferenceOperation::ValidateCredentials => {
                "Actively prove host-owned bridge credentials still work."
            }
            ReferenceOperation::InvalidateCredentials => {
                "Revoke host-owned credentials through an auditable operation."
            }
            ReferenceOperation::Suspend => {
                "Stop the live package while preserving desired state for recovery."
            }
            ReferenceOperation::Stop => "Durably disable and stop one bridge.",
        }
        .into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        let reference = BridgeReference {
            bridge_id: input.bridge_id,
        };
        let client = self.context.client()?;
        match self.operation {
            ReferenceOperation::Status => client
                .bridge_status(reference)
                .await
                .map(status_json)
                .map_err(ipc_error)?,
            ReferenceOperation::ValidateCredentials => client
                .validate_bridge_credentials(reference)
                .await
                .map(credential_json)
                .map_err(ipc_error)?,
            ReferenceOperation::InvalidateCredentials => client
                .invalidate_bridge_credentials(reference)
                .await
                .map(receipt_json)
                .map_err(ipc_error),
            ReferenceOperation::Suspend => client
                .suspend_bridge(reference)
                .await
                .map(status_json)
                .map_err(ipc_error)?,
            ReferenceOperation::Stop => client
                .stop_bridge(reference)
                .await
                .map(receipt_json)
                .map_err(ipc_error),
        }
    }
}

#[derive(Clone)]
struct BeginAuthenticationTool(BridgeContext);

impl McpTool<mcp::Client> for BeginAuthenticationTool {
    type Input = BeginAuthenticationInput;
    type Output = Value;

    fn name(&self) -> String {
        "begin_bridge_authentication".into()
    }

    fn description(&self) -> String {
        "Begin QR, phone-code, OAuth, browser, terminal, or manual package authentication and return its ephemeral presentation.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        self.0
            .client()?
            .begin_bridge_authentication(BeginAuthenticationRequest {
                bridge_id: input.bridge_id,
                preferred_method: input.preferred_method.map(Into::into),
                context_json: serde_json::to_string(&input.context).map_err(|_| invalid_input())?,
            })
            .await
            .map(challenge_json)
            .map_err(ipc_error)?
    }
}

#[derive(Clone)]
struct SubmitAuthenticationTool(BridgeContext);

impl McpTool<mcp::Client> for SubmitAuthenticationTool {
    type Input = SubmitAuthenticationInput;
    type Output = Value;

    fn name(&self) -> String {
        "submit_bridge_authentication".into()
    }

    fn description(&self) -> String {
        "Submit an ephemeral response to the active challenge. Crab stores resulting credentials privately; tool output never contains the handle.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        validate_text(&input.challenge_id)?;
        self.0
            .client()?
            .submit_bridge_authentication(SubmitAuthenticationRequest {
                bridge_id: input.bridge_id,
                challenge_id: input.challenge_id,
                response_json: serde_json::to_string(&input.response)
                    .map_err(|_| invalid_input())?,
            })
            .await
            .map(credential_json)
            .map_err(ipc_error)?
    }
}

#[derive(Clone)]
struct DeliverTool(BridgeContext);

#[derive(Clone)]
struct ImportContentTool(BridgeContext);

impl McpTool<mcp::Client> for ImportContentTool {
    type Input = ImportContentInput;
    type Output = Value;

    fn name(&self) -> String {
        "import_bridge_content".into()
    }

    fn description(&self) -> String {
        "Copy one bounded regular local file into Crab-owned content. Use the returned attachment unchanged with deliver_bridge_message.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        validate_text(&input.import_id)?;
        validate_text(&input.media_type)?;
        if !input.source_path.is_absolute()
            || input
                .name
                .as_ref()
                .is_some_and(|name| name.trim().is_empty())
        {
            return Err(invalid_input());
        }
        self.0
            .client()?
            .import_bridge_content(ImportBridgeContentRequest {
                bridge_id: input.bridge_id,
                import_id: input.import_id,
                source_path: input.source_path.to_string_lossy().into_owned(),
                media_type: input.media_type,
                name: input.name,
            })
            .await
            .map(imported_content_json)
            .map_err(ipc_error)
    }
}

impl McpTool<mcp::Client> for DeliverTool {
    type Input = DeliverInput;
    type Output = Value;

    fn name(&self) -> String {
        "deliver_bridge_message".into()
    }

    fn description(&self) -> String {
        "Deliver one deliberately selected external message. Never use this to mirror native ACP events.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        validate_text(&input.message_id)?;
        let idempotency_key = input
            .idempotency_key
            .unwrap_or_else(|| input.message_id.clone());
        validate_text(&idempotency_key)?;
        let attachments = input
            .attachments
            .into_iter()
            .map(AttachmentInput::into_contract)
            .collect::<Result<Vec<_>, _>>()?;
        self.0
            .client()?
            .deliver_bridge_message(BridgeOutbound {
                bridge_id: input.bridge_id,
                message_id: input.message_id,
                destination_json: serde_json::to_string(&input.destination)
                    .map_err(|_| invalid_input())?,
                message_json: serde_json::to_string(&input.message).map_err(|_| invalid_input())?,
                attachments,
                idempotency_key,
            })
            .await
            .map(delivery_json)
            .map_err(ipc_error)?
    }
}

#[derive(Clone)]
struct DeliveryStatusTool(BridgeContext);

impl McpTool<mcp::Client> for DeliveryStatusTool {
    type Input = DeliveryStatusInput;
    type Output = Value;

    fn name(&self) -> String {
        "bridge_delivery_status".into()
    }

    fn description(&self) -> String {
        "Inspect one durable selected-message delivery by bridge and message ID.".into()
    }

    async fn call_tool(
        &self,
        input: Self::Input,
        _connection: agent_client_protocol::mcp_server::McpConnectionTo<mcp::Client>,
    ) -> Result<Self::Output, Error> {
        validate_text(&input.bridge_id)?;
        validate_text(&input.message_id)?;
        self.0
            .client()?
            .bridge_delivery_status(DeliveryReference {
                bridge_id: input.bridge_id,
                message_id: input.message_id,
            })
            .await
            .map(delivery_json)
            .map_err(ipc_error)?
    }
}

fn valid_environment_names(names: &[String]) -> bool {
    let mut unique = HashSet::new();
    names.iter().all(|name| {
        !name.trim().is_empty() && !name.contains(['=', '\0']) && unique.insert(name.as_str())
    })
}

fn validate_text(value: &str) -> Result<(), Error> {
    if value.trim().is_empty() {
        Err(invalid_input())
    } else {
        Ok(())
    }
}

fn invalid_input() -> Error {
    Error::invalid_params().data(json!({"kind":"contract","code":"InvalidInput"}))
}

fn invalid_output() -> Error {
    Error::internal_error().data(json!({"kind":"internal","code":"InvalidProviderOutput"}))
}

fn ipc_error(error: ChannelIpcClientError) -> Error {
    let (kind, code) = match error {
        ChannelIpcClientError::Io(_) => ("transport".to_owned(), "Unavailable".to_owned()),
        ChannelIpcClientError::Protocol(stage) => ("protocol".to_owned(), stage.to_owned()),
        ChannelIpcClientError::Remote { kind, code } => (kind, code),
    };
    Error::internal_error().data(json!({"kind":kind,"code":code}))
}

fn record_json(record: BridgeRecord) -> Value {
    json!({
        "bridgeId": record.bridge_id,
        "packageId": record.package_id,
        "displayName": record.display_name,
        "lifecycle": lifecycle_name(&record.lifecycle),
        "ingressMode": ingress_name(&record.ingress_mode),
        "alertTarget": record.alert_target.map(|target| json!({
            "channelId": target.channel_id,
            "lane": target.lane,
        })),
        "desiredRunning": record.desired_running,
        "generation": record.generation,
        "registeredAtMs": record.registered_at_ms,
    })
}

fn catalog_json(catalog: BridgeCatalog) -> Value {
    json!({"bridges":catalog.bridges.into_iter().map(record_json).collect::<Vec<_>>()})
}

fn status_json(status: BridgeStatus) -> Result<Value, Error> {
    let last_health = status
        .last_health
        .map(|health| {
            Ok::<Value, Error>(json!({
                "observedAtMs": health.observed_at_ms,
                "processAlive": health.process_alive,
                "serviceConnected": health.service_connected,
                "canReceive": health.can_receive,
                "canSend": health.can_send,
                "credentialLifecycle": credential_name(&health.credential_lifecycle),
                "detail": embedded_json(&health.detail_json)?,
            }))
        })
        .transpose()?;
    Ok(json!({
        "bridgeId": status.bridge_id,
        "lifecycle": lifecycle_name(&status.lifecycle),
        "generation": status.generation,
        "consecutiveFailures": status.consecutive_failures,
        "restartCountInWindow": status.restart_count_in_window,
        "nextRestartAtMs": status.next_restart_at_ms,
        "lastHealth": last_health,
        "lastError": status.last_error,
    }))
}

fn challenge_json(challenge: AuthenticationChallenge) -> Result<Value, Error> {
    Ok(json!({
        "bridgeId": challenge.bridge_id,
        "challengeId": challenge.challenge_id,
        "method": method_name(&challenge.method),
        "expiresAtMs": challenge.expires_at_ms,
        "presentation": embedded_json(&challenge.presentation_json)?,
    }))
}

fn credential_json(status: CredentialStatus) -> Result<Value, Error> {
    Ok(json!({
        "bridgeId": status.bridge_id,
        "lifecycle": credential_name(&status.lifecycle),
        "credentialStored": status.credential_handle.is_some(),
        "validatedAtMs": status.validated_at_ms,
        "expiresAtMs": status.expires_at_ms,
        "accountHint": status.account_hint,
        "detail": embedded_json(&status.detail_json)?,
    }))
}

fn delivery_json(receipt: DeliveryReceipt) -> Result<Value, Error> {
    Ok(json!({
        "bridgeId": receipt.bridge_id,
        "messageId": receipt.message_id,
        "lifecycle": delivery_name(&receipt.lifecycle),
        "externalDeliveryId": receipt.external_delivery_id,
        "attempt": receipt.attempt,
        "updatedAtMs": receipt.updated_at_ms,
        "detail": embedded_json(&receipt.detail_json)?,
    }))
}

fn imported_content_json(content: ImportedBridgeContent) -> Value {
    json!({
        "attachment": {
            "mediaType": content.attachment.media_type,
            "name": content.attachment.name,
            "contentHandle": content.attachment.content_handle,
        },
        "sizeBytes": content.size_bytes,
        "sha256": content.sha256,
    })
}

fn receipt_json(receipt: BridgeReceipt) -> Value {
    json!({"accepted":receipt.accepted,"recordedAtMs":receipt.recorded_at_ms})
}

fn embedded_json(value: &str) -> Result<Value, Error> {
    serde_json::from_str(value).map_err(|_| invalid_output())
}

fn lifecycle_name(value: &BridgeLifecycle) -> &'static str {
    match value {
        BridgeLifecycle::Registered => "registered",
        BridgeLifecycle::Starting => "starting",
        BridgeLifecycle::AwaitingAuthentication => "awaiting-authentication",
        BridgeLifecycle::Healthy => "healthy",
        BridgeLifecycle::Degraded => "degraded",
        BridgeLifecycle::BackingOff => "backing-off",
        BridgeLifecycle::Stopped => "stopped",
        BridgeLifecycle::Failed => "failed",
        BridgeLifecycle::Unknown { .. } => "unknown",
    }
}

fn ingress_name(value: &BridgeIngressMode) -> &'static str {
    match value {
        BridgeIngressMode::Queue => "queue",
        BridgeIngressMode::Steer => "steer",
        BridgeIngressMode::InterruptAndSteer => "interrupt-and-steer",
        BridgeIngressMode::Unknown { .. } => "unknown",
    }
}

fn method_name(value: &AuthenticationMethod) -> &'static str {
    match value {
        AuthenticationMethod::QrCode => "qr-code",
        AuthenticationMethod::PhoneCode => "phone-code",
        AuthenticationMethod::OAuth => "oauth",
        AuthenticationMethod::Browser => "browser",
        AuthenticationMethod::Terminal => "terminal",
        AuthenticationMethod::Manual => "manual",
        AuthenticationMethod::Unknown { .. } => "unknown",
    }
}

fn credential_name(value: &CredentialLifecycle) -> &'static str {
    match value {
        CredentialLifecycle::Missing => "missing",
        CredentialLifecycle::Challenged => "challenged",
        CredentialLifecycle::Validating => "validating",
        CredentialLifecycle::Valid => "valid",
        CredentialLifecycle::Expiring => "expiring",
        CredentialLifecycle::Rejected => "rejected",
        CredentialLifecycle::Revoked => "revoked",
        CredentialLifecycle::Unknown { .. } => "unknown",
    }
}

fn delivery_name(value: &DeliveryLifecycle) -> &'static str {
    match value {
        DeliveryLifecycle::Queued => "queued",
        DeliveryLifecycle::Sending => "sending",
        DeliveryLifecycle::Delivered => "delivered",
        DeliveryLifecycle::Retrying => "retrying",
        DeliveryLifecycle::Rejected => "rejected",
        DeliveryLifecycle::Failed => "failed",
        DeliveryLifecycle::Unknown { .. } => "unknown",
    }
}

#[cfg(test)]
mod tests {
    use bridge_host_contract::{CredentialLifecycle, CredentialStatus};

    use super::{
        AuthenticationMethodInput, IngressModeInput, credential_json, valid_environment_names,
    };

    #[test]
    fn tool_enums_and_environment_names_are_strict() {
        assert!(matches!(
            serde_json::from_str::<AuthenticationMethodInput>(r#""phone-code""#)
                .expect("authentication method"),
            AuthenticationMethodInput::PhoneCode
        ));
        assert!(matches!(
            serde_json::from_str::<IngressModeInput>(r#""interrupt-and-steer""#)
                .expect("ingress mode"),
            IngressModeInput::InterruptAndSteer
        ));
        assert!(valid_environment_names(&["PATH".into()]));
        assert!(!valid_environment_names(&["PATH".into(), "PATH".into()]));
        assert!(!valid_environment_names(&["BAD=VALUE".into()]));
    }

    #[test]
    fn credential_output_never_exposes_the_private_handle() {
        let output = credential_json(CredentialStatus {
            bridge_id: "signal".into(),
            lifecycle: CredentialLifecycle::Valid,
            credential_handle: Some("private:must-never-cross-mcp".into()),
            validated_at_ms: Some(42),
            expires_at_ms: None,
            account_hint: Some("paired".into()),
            detail_json: "{}".into(),
        })
        .expect("credential output is valid");
        let encoded = output.to_string();
        assert_eq!(output["credentialStored"], true);
        assert!(!encoded.contains("must-never-cross-mcp"));
        assert!(output.get("credentialHandle").is_none());
    }
}
