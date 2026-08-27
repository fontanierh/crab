//! Owner-only local transport for selected generated Boxology capabilities.

use std::{
    fmt,
    fs::{self, OpenOptions},
    future::Future,
    io::{self, Read as _, Write as _},
    os::unix::fs::{FileTypeExt as _, OpenOptionsExt as _, PermissionsExt as _},
    path::{Path, PathBuf},
};

use agent_host_contract::{
    AgentDiagnosticPage, AgentHostHandle, AgentSessionCatalog, ListAgentSessionsRequest,
    ReadAgentDiagnosticsRequest, SessionReference, SessionStatus,
};
use boxology_contract::{
    CallContext, CallError, Caller, CancelToken, CapabilityDescriptor, ContractDescriptor,
    ContractError, ContractType, DecodeRole, TraceContext,
    json::{self, Limits},
};
use bridge_host_contract::{
    AuthenticationChallenge, BeginAuthenticationRequest, BridgeCatalog, BridgeHostHandle,
    BridgeOutbound, BridgeReceipt, BridgeRecord, BridgeReference, BridgeSpec, BridgeStatus,
    CredentialStatus, DeliveryReceipt, DeliveryReference, ImportBridgeContentRequest,
    ImportedBridgeContent, ListBridgesRequest, ReconcileBridgeRequest, ReplaceBridgeRequest,
    SubmitAuthenticationRequest, UnregisterBridgeRequest,
};
use channel_gateway_contract::{AttachChannelRequest, ChannelAttachment, ChannelGatewayHandle};
use native_channel_contract::{
    AcceptedTurn, BindingReference, ChannelStatus, ChannelTurn, InterruptReceipt, InterruptRequest,
    NativeChannelHandle, PublishedEventPage, ReplayRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sub_agent_host_contract::{
    InteractionReceipt, ReadSubAgentEventsRequest, SendToChildRequest, SendToParentRequest,
    SpawnSubAgentRequest, StopSubAgentRequest, SubAgentEventPage, SubAgentHostHandle,
    SubAgentReceipt, SubAgentRecord, SubAgentReference, SubAgentStatus,
};
use tokio::{
    io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
    net::{UnixListener, UnixStream},
    sync::watch,
    task::{JoinHandle, JoinSet},
};
use trigger_inbox_contract::{EnqueueTrigger, TriggerInboxHandle, TriggerReceipt};
use uuid::Uuid;

const PROTOCOL_VERSION: u16 = 1;
const MAX_REQUEST_BYTES: usize = 2 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_JSON_DEPTH: usize = 128;
const SOCKET_FILE: &str = "channel-ipc.sock";
const TOKEN_FILE: &str = "channel-ipc.token";

const ATTACH: &str = "channel-gateway.attach_channel";
const ACCEPT_TURN: &str = "native-channel.accept_turn";
const INTERRUPT: &str = "native-channel.interrupt_and_drain";
const CHANNEL_STATUS: &str = "native-channel.channel_status";
const REPLAY: &str = "native-channel.replay_native_events";
const ENQUEUE_TRIGGER: &str = "trigger-inbox.enqueue";
const REGISTER_BRIDGE: &str = "bridge-host.register_bridge";
const LIST_BRIDGES: &str = "bridge-host.list_bridges";
const REPLACE_BRIDGE: &str = "bridge-host.replace_bridge";
const UNREGISTER_BRIDGE: &str = "bridge-host.unregister_bridge";
const RECONCILE_BRIDGE: &str = "bridge-host.reconcile_bridge";
const BEGIN_AUTHENTICATION: &str = "bridge-host.begin_authentication";
const SUBMIT_AUTHENTICATION: &str = "bridge-host.submit_authentication";
const VALIDATE_CREDENTIALS: &str = "bridge-host.validate_credentials";
const INVALIDATE_CREDENTIALS: &str = "bridge-host.invalidate_credentials";
const IMPORT_BRIDGE_CONTENT: &str = "bridge-host.import_content";
const DELIVER_BRIDGE_MESSAGE: &str = "bridge-host.deliver_message";
const BRIDGE_DELIVERY_STATUS: &str = "bridge-host.delivery_status";
const BRIDGE_STATUS: &str = "bridge-host.bridge_status";
const STOP_BRIDGE: &str = "bridge-host.stop_bridge";
const SUSPEND_BRIDGE: &str = "bridge-host.suspend_bridge";
const AGENT_SESSION_STATUS: &str = "agent-host.session_status";
const LIST_AGENT_SESSIONS: &str = "agent-host.list_sessions";
const READ_AGENT_DIAGNOSTICS: &str = "agent-host.read_diagnostics";
const SPAWN_SUB_AGENT: &str = "sub-agent-host.spawn";
const SEND_TO_CHILD: &str = "sub-agent-host.send_to_child";
const SEND_TO_PARENT: &str = "sub-agent-host.send_to_parent";
const READ_SUB_AGENT_EVENTS: &str = "sub-agent-host.read_events";
const SUB_AGENT_STATUS: &str = "sub-agent-host.status";
const STOP_SUB_AGENT: &str = "sub-agent-host.stop";

/// Stable filesystem endpoints for Crab's local capability transport.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelIpcPaths {
    socket: PathBuf,
    token: PathBuf,
}

impl ChannelIpcPaths {
    /// Resolve the socket and token beneath an existing state directory.
    pub fn for_state_directory(directory: impl AsRef<Path>) -> io::Result<Self> {
        let directory = fs::canonicalize(directory)?;
        Ok(Self {
            socket: directory.join(SOCKET_FILE),
            token: directory.join(TOKEN_FILE),
        })
    }

    /// Return the owner-only Unix socket path.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    /// Return the owner-only authentication-token path.
    #[must_use]
    pub fn token(&self) -> &Path {
        &self.token
    }
}

/// Failure to create the owner-only IPC endpoint.
#[derive(Debug)]
pub enum ChannelIpcStartupError {
    /// A filesystem or socket operation failed.
    Io(io::Error),
    /// Another Crab runtime is already listening at the configured path.
    AlreadyRunning,
    /// An existing path at the socket location is not a Unix socket.
    UnsafeSocketPath,
    /// The persisted token is malformed or not owner-only.
    UnsafeTokenFile,
}

impl fmt::Display for ChannelIpcStartupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "local IPC failed: {error}"),
            Self::AlreadyRunning => formatter.write_str("local IPC is already running"),
            Self::UnsafeSocketPath => formatter.write_str("local IPC path is not a Unix socket"),
            Self::UnsafeTokenFile => {
                formatter.write_str("local IPC token is malformed or not owner-only")
            }
        }
    }
}

impl std::error::Error for ChannelIpcStartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ChannelIpcStartupError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// A stable remote or transport failure returned to native UI adapters.
#[derive(Debug)]
pub enum ChannelIpcClientError {
    /// A filesystem or socket operation failed.
    Io(io::Error),
    /// The peer did not speak the exact supported protocol.
    Protocol(&'static str),
    /// Crab rejected the authenticated capability call.
    Remote {
        /// Stable failure category.
        kind: String,
        /// Stable domain or transport code.
        code: String,
    },
}

impl fmt::Display for ChannelIpcClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "local IPC failed: {error}"),
            Self::Protocol(stage) => {
                write!(formatter, "local IPC protocol violation: {stage}")
            }
            Self::Remote { kind, code } => write!(formatter, "local IPC {kind}: {code}"),
        }
    }
}

impl std::error::Error for ChannelIpcClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ChannelIpcClientError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// A lightweight client for Crab's authenticated local Boxology transport.
#[derive(Clone)]
pub struct ChannelIpcClient {
    paths: ChannelIpcPaths,
    authentication: String,
}

impl ChannelIpcClient {
    /// Load the owner-only token for an existing Crab state directory.
    pub fn from_state_directory(
        directory: impl AsRef<Path>,
    ) -> Result<Self, ChannelIpcClientError> {
        let paths = ChannelIpcPaths::for_state_directory(directory)?;
        let authentication = load_token(&paths.token).map_err(|error| match error {
            ChannelIpcStartupError::Io(error) => ChannelIpcClientError::Io(error),
            _ => ChannelIpcClientError::Protocol("token"),
        })?;
        Ok(Self {
            paths,
            authentication,
        })
    }

    /// Idempotently create, reuse or recover one native-channel attachment.
    pub async fn attach_channel(
        &self,
        request: AttachChannelRequest,
    ) -> Result<ChannelAttachment, ChannelIpcClientError> {
        self.invoke(
            ATTACH,
            channel_gateway_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Submit one explicit queue or steer turn.
    pub async fn accept_turn(
        &self,
        request: ChannelTurn,
    ) -> Result<AcceptedTurn, ChannelIpcClientError> {
        self.invoke(
            ACCEPT_TURN,
            native_channel_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Explicitly cancel active work and drain already accepted input.
    pub async fn interrupt_and_drain(
        &self,
        request: InterruptRequest,
    ) -> Result<InterruptReceipt, ChannelIpcClientError> {
        self.invoke(
            INTERRUPT,
            native_channel_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Read one binding's current session and replay position.
    pub async fn channel_status(
        &self,
        request: BindingReference,
    ) -> Result<ChannelStatus, ChannelIpcClientError> {
        self.invoke(
            CHANNEL_STATUS,
            native_channel_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Replay the complete ordered native ACP view after a sequence.
    pub async fn replay_native_events(
        &self,
        request: ReplayRequest,
    ) -> Result<PublishedEventPage, ChannelIpcClientError> {
        self.invoke(
            REPLAY,
            native_channel_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Durably enqueue one bridge, schedule, self-work or operator trigger.
    pub async fn enqueue_trigger(
        &self,
        request: EnqueueTrigger,
    ) -> Result<TriggerReceipt, ChannelIpcClientError> {
        self.invoke(
            ENQUEUE_TRIGGER,
            trigger_inbox_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// List non-secret durable bridge registrations.
    pub async fn list_bridges(&self) -> Result<BridgeCatalog, ChannelIpcClientError> {
        self.invoke(
            LIST_BRIDGES,
            bridge_host_contract::contract_descriptor(),
            ListBridgesRequest {},
        )
        .await
    }

    /// Register one strict, secret-free agent-installed bridge package.
    pub async fn register_bridge(
        &self,
        request: BridgeSpec,
    ) -> Result<BridgeRecord, ChannelIpcClientError> {
        self.invoke(
            REGISTER_BRIDGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Replace package, configuration or policy under generation control.
    pub async fn replace_bridge(
        &self,
        request: ReplaceBridgeRequest,
    ) -> Result<BridgeRecord, ChannelIpcClientError> {
        self.invoke(
            REPLACE_BRIDGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Permanently retire one agent-managed registration under generation control.
    pub async fn unregister_bridge(
        &self,
        request: UnregisterBridgeRequest,
    ) -> Result<BridgeReceipt, ChannelIpcClientError> {
        self.invoke(
            UNREGISTER_BRIDGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Converge one bridge toward its desired state with generation control.
    pub async fn reconcile_bridge(
        &self,
        request: ReconcileBridgeRequest,
    ) -> Result<BridgeStatus, ChannelIpcClientError> {
        self.invoke(
            RECONCILE_BRIDGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Begin a package-owned QR, phone-code or other authentication flow.
    pub async fn begin_bridge_authentication(
        &self,
        request: BeginAuthenticationRequest,
    ) -> Result<AuthenticationChallenge, ChannelIpcClientError> {
        self.invoke(
            BEGIN_AUTHENTICATION,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Submit an ephemeral response to an active authentication challenge.
    pub async fn submit_bridge_authentication(
        &self,
        request: SubmitAuthenticationRequest,
    ) -> Result<CredentialStatus, ChannelIpcClientError> {
        self.invoke(
            SUBMIT_AUTHENTICATION,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Actively validate the credential already held by Crab's credential store.
    pub async fn validate_bridge_credentials(
        &self,
        request: BridgeReference,
    ) -> Result<CredentialStatus, ChannelIpcClientError> {
        self.invoke(
            VALIDATE_CREDENTIALS,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Invalidate stored bridge credentials through the owning capability.
    pub async fn invalidate_bridge_credentials(
        &self,
        request: BridgeReference,
    ) -> Result<BridgeReceipt, ChannelIpcClientError> {
        self.invoke(
            INVALIDATE_CREDENTIALS,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Copy one bounded local file into Crab-owned content for a selected bridge delivery.
    pub async fn import_bridge_content(
        &self,
        request: ImportBridgeContentRequest,
    ) -> Result<ImportedBridgeContent, ChannelIpcClientError> {
        self.invoke(
            IMPORT_BRIDGE_CONTENT,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Deliver one deliberately selected external message with durable deduplication.
    pub async fn deliver_bridge_message(
        &self,
        request: BridgeOutbound,
    ) -> Result<DeliveryReceipt, ChannelIpcClientError> {
        self.invoke(
            DELIVER_BRIDGE_MESSAGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Inspect one durable selected-message delivery.
    pub async fn bridge_delivery_status(
        &self,
        request: DeliveryReference,
    ) -> Result<DeliveryReceipt, ChannelIpcClientError> {
        self.invoke(
            BRIDGE_DELIVERY_STATUS,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Inspect truthful bridge lifecycle and health.
    pub async fn bridge_status(
        &self,
        request: BridgeReference,
    ) -> Result<BridgeStatus, ChannelIpcClientError> {
        self.invoke(
            BRIDGE_STATUS,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Durably disable and stop one bridge.
    pub async fn stop_bridge(
        &self,
        request: BridgeReference,
    ) -> Result<BridgeReceipt, ChannelIpcClientError> {
        self.invoke(
            STOP_BRIDGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Stop the live package while preserving its desired state.
    pub async fn suspend_bridge(
        &self,
        request: BridgeReference,
    ) -> Result<BridgeStatus, ChannelIpcClientError> {
        self.invoke(
            SUSPEND_BRIDGE,
            bridge_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Start one separately supervised ACP child without waiting for its model work to finish.
    pub async fn spawn_sub_agent(
        &self,
        request: SpawnSubAgentRequest,
    ) -> Result<SubAgentRecord, ChannelIpcClientError> {
        self.invoke(
            SPAWN_SUB_AGENT,
            sub_agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Inspect the current parent cursor before creating an inherited child snapshot.
    pub async fn agent_session_status(
        &self,
        request: SessionReference,
    ) -> Result<SessionStatus, ChannelIpcClientError> {
        self.invoke(
            AGENT_SESSION_STATUS,
            agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// List the newest non-secret session identities and private journal cursors.
    pub async fn list_agent_sessions(
        &self,
        request: ListAgentSessionsRequest,
    ) -> Result<AgentSessionCatalog, ChannelIpcClientError> {
        self.invoke(
            LIST_AGENT_SESSIONS,
            agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Read bounded private adapter diagnostics through the owner-authenticated operator seam.
    pub async fn read_agent_diagnostics(
        &self,
        request: ReadAgentDiagnosticsRequest,
    ) -> Result<AgentDiagnosticPage, ChannelIpcClientError> {
        self.invoke(
            READ_AGENT_DIAGNOSTICS,
            agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Durably deliver one queue, steer or interrupt-and-steer input to the child.
    pub async fn send_to_child(
        &self,
        request: SendToChildRequest,
    ) -> Result<InteractionReceipt, ChannelIpcClientError> {
        self.invoke(
            SEND_TO_CHILD,
            sub_agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Durably deliver one non-blocking child progress/result message to the parent.
    pub async fn send_to_parent(
        &self,
        request: SendToParentRequest,
    ) -> Result<InteractionReceipt, ChannelIpcClientError> {
        self.invoke(
            SEND_TO_PARENT,
            sub_agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Read ordered lifecycle, interaction and native ACP events after one cursor.
    pub async fn read_sub_agent_events(
        &self,
        request: ReadSubAgentEventsRequest,
    ) -> Result<SubAgentEventPage, ChannelIpcClientError> {
        self.invoke(
            READ_SUB_AGENT_EVENTS,
            sub_agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Inspect one child and its pending bidirectional work.
    pub async fn sub_agent_status(
        &self,
        request: SubAgentReference,
    ) -> Result<SubAgentStatus, ChannelIpcClientError> {
        self.invoke(
            SUB_AGENT_STATUS,
            sub_agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    /// Cooperatively stop one child; repeated terminal stops remain safe.
    pub async fn stop_sub_agent(
        &self,
        request: StopSubAgentRequest,
    ) -> Result<SubAgentReceipt, ChannelIpcClientError> {
        self.invoke(
            STOP_SUB_AGENT,
            sub_agent_host_contract::contract_descriptor(),
            request,
        )
        .await
    }

    async fn invoke<I, O>(
        &self,
        capability_name: &str,
        contract: &ContractDescriptor,
        input: I,
    ) -> Result<O, ChannelIpcClientError>
    where
        I: ContractType,
        O: ContractType,
    {
        let capability = capability(contract, capability_name)
            .ok_or(ChannelIpcClientError::Protocol("descriptor"))?;
        let encoded = input
            .encode()
            .map_err(|_| ChannelIpcClientError::Protocol("input contract encode"))
            .and_then(|slot| {
                json::encode(&slot, capability.input())
                    .map_err(|_| ChannelIpcClientError::Protocol("input JSON encode"))
            })?;
        let input = RawValue::from_string(
            String::from_utf8(encoded)
                .map_err(|_| ChannelIpcClientError::Protocol("input UTF-8"))?,
        )
        .map_err(|_| ChannelIpcClientError::Protocol("input raw JSON"))?;
        let request_id = Uuid::new_v4().simple().to_string();
        let request = WireRequest {
            protocol_version: PROTOCOL_VERSION,
            request_id: request_id.clone(),
            authentication: self.authentication.clone(),
            capability: capability_name.to_owned(),
            input,
        };
        let request = serde_json::to_vec(&request)
            .map_err(|_| ChannelIpcClientError::Protocol("request encode"))?;
        if request.len() > MAX_REQUEST_BYTES {
            return Err(ChannelIpcClientError::Protocol("request too large"));
        }
        let mut stream = UnixStream::connect(&self.paths.socket).await?;
        stream.write_all(&request).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;
        let response = read_frame(&mut BufReader::new(stream), MAX_RESPONSE_BYTES).await?;
        let response: WireResponse = serde_json::from_slice(&response)
            .map_err(|_| ChannelIpcClientError::Protocol("response decode"))?;
        if response.protocol_version != PROTOCOL_VERSION || response.request_id != request_id {
            return Err(ChannelIpcClientError::Protocol("response identity"));
        }
        match response
            .into_outcome()
            .ok_or(ChannelIpcClientError::Protocol("response outcome"))?
        {
            WireOutcome::Ok { output } => {
                let slot = json::decode(
                    output.get().as_bytes(),
                    capability.output(),
                    DecodeRole::ConsumerOutput,
                    Limits::new(MAX_RESPONSE_BYTES, MAX_JSON_DEPTH),
                )
                .map_err(|_| ChannelIpcClientError::Protocol("output JSON decode"))?;
                O::decode(&slot)
                    .map_err(|_| ChannelIpcClientError::Protocol("output contract decode"))
            }
            WireOutcome::Error { error } => Err(ChannelIpcClientError::Remote {
                kind: error.kind,
                code: error.code,
            }),
        }
    }
}

pub(crate) struct ChannelIpcServer {
    socket_path: PathBuf,
    shutdown: watch::Sender<bool>,
    failed: watch::Receiver<bool>,
    task: Option<JoinHandle<io::Result<()>>>,
}

#[derive(Clone)]
struct IpcCapabilities {
    agent_host: AgentHostHandle,
    channel_gateway: ChannelGatewayHandle,
    native_channel: NativeChannelHandle,
    bridge_host: BridgeHostHandle,
    trigger_inbox: TriggerInboxHandle,
    sub_agent_host: SubAgentHostHandle,
}

impl ChannelIpcServer {
    pub(crate) async fn start(
        paths: ChannelIpcPaths,
        agent_host: AgentHostHandle,
        channel_gateway: ChannelGatewayHandle,
        native_channel: NativeChannelHandle,
        bridge_host: BridgeHostHandle,
        trigger_inbox: TriggerInboxHandle,
        sub_agent_host: SubAgentHostHandle,
    ) -> Result<Self, ChannelIpcStartupError> {
        let authentication = load_or_create_token(&paths.token)?;
        prepare_socket(&paths.socket).await?;
        let listener = UnixListener::bind(&paths.socket)?;
        fs::set_permissions(&paths.socket, fs::Permissions::from_mode(0o600))?;
        let (shutdown, receiver) = watch::channel(false);
        let (failed_sender, failed) = watch::channel(false);
        let socket_path = paths.socket.clone();
        let server_socket = socket_path.clone();
        let capabilities = IpcCapabilities {
            agent_host,
            channel_gateway,
            native_channel,
            bridge_host,
            trigger_inbox,
            sub_agent_host,
        };
        let task = tokio::spawn(async move {
            let result = serve(listener, receiver, authentication, capabilities).await;
            if result.is_err() {
                failed_sender.send_replace(true);
            }
            let _ = fs::remove_file(server_socket);
            result
        });
        Ok(Self {
            socket_path,
            shutdown,
            failed,
            task: Some(task),
        })
    }

    pub(crate) async fn wait_for_failure(&mut self) {
        while !*self.failed.borrow() && self.failed.changed().await.is_ok() {}
    }

    pub(crate) async fn shutdown(&mut self) -> io::Result<()> {
        self.shutdown.send_replace(true);
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        task.await
            .map_err(|error| io::Error::other(error.to_string()))?
    }
}

impl Drop for ChannelIpcServer {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
        if self.task.is_some() {
            let _ = fs::remove_file(&self.socket_path);
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireRequest {
    protocol_version: u16,
    request_id: String,
    authentication: String,
    capability: String,
    input: Box<RawValue>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireResponse {
    protocol_version: u16,
    request_id: String,
    status: WireStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    output: Option<Box<RawValue>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<WireError>,
}

impl WireResponse {
    fn new(request_id: String, outcome: WireOutcome) -> Self {
        let (status, output, error) = match outcome {
            WireOutcome::Ok { output } => (WireStatus::Ok, Some(output), None),
            WireOutcome::Error { error } => (WireStatus::Error, None, Some(error)),
        };
        Self {
            protocol_version: PROTOCOL_VERSION,
            request_id,
            status,
            output,
            error,
        }
    }

    fn into_outcome(self) -> Option<WireOutcome> {
        match (self.status, self.output, self.error) {
            (WireStatus::Ok, Some(output), None) => Some(WireOutcome::Ok { output }),
            (WireStatus::Error, None, Some(error)) => Some(WireOutcome::Error { error }),
            _ => None,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
enum WireStatus {
    Ok,
    Error,
}

enum WireOutcome {
    Ok { output: Box<RawValue> },
    Error { error: WireError },
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireError {
    kind: String,
    code: String,
}

async fn serve(
    listener: UnixListener,
    mut shutdown: watch::Receiver<bool>,
    authentication: String,
    capabilities: IpcCapabilities,
) -> io::Result<()> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                connections.spawn(handle_connection(
                    stream,
                    authentication.clone(),
                    capabilities.clone(),
                ));
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    authentication: String,
    capabilities: IpcCapabilities,
) {
    let (reader, mut writer) = stream.into_split();
    let Ok(frame) = read_frame(&mut BufReader::new(reader), MAX_REQUEST_BYTES).await else {
        return;
    };
    let Ok(request) = serde_json::from_slice::<WireRequest>(&frame) else {
        return;
    };
    let request_id = request.request_id.clone();
    let outcome = if request.protocol_version != PROTOCOL_VERSION
        || request.request_id.is_empty()
        || request.request_id.len() > 128
    {
        wire_failure("protocol", "InvalidEnvelope")
    } else if !constant_time_equal(request.authentication.as_bytes(), authentication.as_bytes()) {
        wire_failure("authentication", "Unauthorized")
    } else {
        dispatch(request, capabilities).await
    };
    let response = WireResponse::new(request_id, outcome);
    let Ok(mut bytes) = serde_json::to_vec(&response) else {
        return;
    };
    if bytes.len() > MAX_RESPONSE_BYTES {
        let fallback = WireResponse::new(
            response.request_id,
            wire_failure("protocol", "ResponseTooLarge"),
        );
        let Ok(fallback) = serde_json::to_vec(&fallback) else {
            return;
        };
        bytes = fallback;
    }
    bytes.push(b'\n');
    let _ = writer.write_all(&bytes).await;
    let _ = writer.flush().await;
}

async fn dispatch(request: WireRequest, capabilities: IpcCapabilities) -> WireOutcome {
    let IpcCapabilities {
        agent_host,
        channel_gateway,
        native_channel,
        bridge_host,
        trigger_inbox,
        sub_agent_host,
    } = capabilities;
    match request.capability.as_str() {
        AGENT_SESSION_STATUS => {
            invoke_agent::<SessionReference, SessionStatus, _, _>(
                &request.input,
                AGENT_SESSION_STATUS,
                |input| agent_host.session_status(call_context(), input),
            )
            .await
        }
        LIST_AGENT_SESSIONS => {
            invoke_agent::<ListAgentSessionsRequest, AgentSessionCatalog, _, _>(
                &request.input,
                LIST_AGENT_SESSIONS,
                |input| agent_host.list_sessions(call_context(), input),
            )
            .await
        }
        READ_AGENT_DIAGNOSTICS => {
            invoke_agent::<ReadAgentDiagnosticsRequest, AgentDiagnosticPage, _, _>(
                &request.input,
                READ_AGENT_DIAGNOSTICS,
                |input| agent_host.read_diagnostics(call_context(), input),
            )
            .await
        }
        ATTACH => {
            let Some(capability) =
                capability(channel_gateway_contract::contract_descriptor(), ATTACH)
            else {
                return wire_failure("internal", "MissingDescriptor");
            };
            let input = match decode_input::<AttachChannelRequest>(&request.input, capability) {
                Ok(input) => input,
                Err(error) => return error,
            };
            match channel_gateway.attach_channel(call_context(), input).await {
                Ok(output) => encode_output(output, capability),
                Err(error) => wire_call_error(error),
            }
        }
        ACCEPT_TURN => {
            invoke_native::<ChannelTurn, AcceptedTurn, _, _>(&request.input, ACCEPT_TURN, |input| {
                native_channel.accept_turn(call_context(), input)
            })
            .await
        }
        INTERRUPT => {
            invoke_native::<InterruptRequest, InterruptReceipt, _, _>(
                &request.input,
                INTERRUPT,
                |input| native_channel.interrupt_and_drain(call_context(), input),
            )
            .await
        }
        CHANNEL_STATUS => {
            invoke_native::<BindingReference, ChannelStatus, _, _>(
                &request.input,
                CHANNEL_STATUS,
                |input| native_channel.channel_status(call_context(), input),
            )
            .await
        }
        REPLAY => {
            invoke_native::<ReplayRequest, PublishedEventPage, _, _>(
                &request.input,
                REPLAY,
                |input| native_channel.replay_native_events(call_context(), input),
            )
            .await
        }
        ENQUEUE_TRIGGER => {
            let Some(capability) = capability(
                trigger_inbox_contract::contract_descriptor(),
                ENQUEUE_TRIGGER,
            ) else {
                return wire_failure("internal", "MissingDescriptor");
            };
            let input = match decode_input::<EnqueueTrigger>(&request.input, capability) {
                Ok(input) => input,
                Err(error) => return error,
            };
            match trigger_inbox.enqueue(call_context(), input).await {
                Ok(output) => encode_output(output, capability),
                Err(error) => wire_call_error(error),
            }
        }
        REGISTER_BRIDGE => {
            invoke_bridge::<BridgeSpec, BridgeRecord, _, _>(
                &request.input,
                REGISTER_BRIDGE,
                |input| bridge_host.register_bridge(call_context(), input),
            )
            .await
        }
        LIST_BRIDGES => {
            invoke_bridge::<ListBridgesRequest, BridgeCatalog, _, _>(
                &request.input,
                LIST_BRIDGES,
                |input| bridge_host.list_bridges(call_context(), input),
            )
            .await
        }
        REPLACE_BRIDGE => {
            invoke_bridge::<ReplaceBridgeRequest, BridgeRecord, _, _>(
                &request.input,
                REPLACE_BRIDGE,
                |input| bridge_host.replace_bridge(call_context(), input),
            )
            .await
        }
        UNREGISTER_BRIDGE => {
            invoke_bridge::<UnregisterBridgeRequest, BridgeReceipt, _, _>(
                &request.input,
                UNREGISTER_BRIDGE,
                |input| bridge_host.unregister_bridge(call_context(), input),
            )
            .await
        }
        RECONCILE_BRIDGE => {
            invoke_bridge::<ReconcileBridgeRequest, BridgeStatus, _, _>(
                &request.input,
                RECONCILE_BRIDGE,
                |input| bridge_host.reconcile_bridge(call_context(), input),
            )
            .await
        }
        BEGIN_AUTHENTICATION => {
            invoke_bridge::<BeginAuthenticationRequest, AuthenticationChallenge, _, _>(
                &request.input,
                BEGIN_AUTHENTICATION,
                |input| bridge_host.begin_authentication(call_context(), input),
            )
            .await
        }
        SUBMIT_AUTHENTICATION => {
            invoke_bridge::<SubmitAuthenticationRequest, CredentialStatus, _, _>(
                &request.input,
                SUBMIT_AUTHENTICATION,
                |input| bridge_host.submit_authentication(call_context(), input),
            )
            .await
        }
        VALIDATE_CREDENTIALS => {
            invoke_bridge::<BridgeReference, CredentialStatus, _, _>(
                &request.input,
                VALIDATE_CREDENTIALS,
                |input| bridge_host.validate_credentials(call_context(), input),
            )
            .await
        }
        INVALIDATE_CREDENTIALS => {
            invoke_bridge::<BridgeReference, BridgeReceipt, _, _>(
                &request.input,
                INVALIDATE_CREDENTIALS,
                |input| bridge_host.invalidate_credentials(call_context(), input),
            )
            .await
        }
        IMPORT_BRIDGE_CONTENT => {
            invoke_bridge::<ImportBridgeContentRequest, ImportedBridgeContent, _, _>(
                &request.input,
                IMPORT_BRIDGE_CONTENT,
                |input| bridge_host.import_content(call_context(), input),
            )
            .await
        }
        DELIVER_BRIDGE_MESSAGE => {
            invoke_bridge::<BridgeOutbound, DeliveryReceipt, _, _>(
                &request.input,
                DELIVER_BRIDGE_MESSAGE,
                |input| bridge_host.deliver_message(call_context(), input),
            )
            .await
        }
        BRIDGE_DELIVERY_STATUS => {
            invoke_bridge::<DeliveryReference, DeliveryReceipt, _, _>(
                &request.input,
                BRIDGE_DELIVERY_STATUS,
                |input| bridge_host.delivery_status(call_context(), input),
            )
            .await
        }
        BRIDGE_STATUS => {
            invoke_bridge::<BridgeReference, BridgeStatus, _, _>(
                &request.input,
                BRIDGE_STATUS,
                |input| bridge_host.bridge_status(call_context(), input),
            )
            .await
        }
        STOP_BRIDGE => {
            invoke_bridge::<BridgeReference, BridgeReceipt, _, _>(
                &request.input,
                STOP_BRIDGE,
                |input| bridge_host.stop_bridge(call_context(), input),
            )
            .await
        }
        SUSPEND_BRIDGE => {
            invoke_bridge::<BridgeReference, BridgeStatus, _, _>(
                &request.input,
                SUSPEND_BRIDGE,
                |input| bridge_host.suspend_bridge(call_context(), input),
            )
            .await
        }
        SPAWN_SUB_AGENT => {
            invoke_sub_agent::<SpawnSubAgentRequest, SubAgentRecord, _, _>(
                &request.input,
                SPAWN_SUB_AGENT,
                |input| sub_agent_host.spawn(call_context(), input),
            )
            .await
        }
        SEND_TO_CHILD => {
            invoke_sub_agent::<SendToChildRequest, InteractionReceipt, _, _>(
                &request.input,
                SEND_TO_CHILD,
                |input| sub_agent_host.send_to_child(call_context(), input),
            )
            .await
        }
        SEND_TO_PARENT => {
            invoke_sub_agent::<SendToParentRequest, InteractionReceipt, _, _>(
                &request.input,
                SEND_TO_PARENT,
                |input| sub_agent_host.send_to_parent(call_context(), input),
            )
            .await
        }
        READ_SUB_AGENT_EVENTS => {
            invoke_sub_agent::<ReadSubAgentEventsRequest, SubAgentEventPage, _, _>(
                &request.input,
                READ_SUB_AGENT_EVENTS,
                |input| sub_agent_host.read_events(call_context(), input),
            )
            .await
        }
        SUB_AGENT_STATUS => {
            invoke_sub_agent::<SubAgentReference, SubAgentStatus, _, _>(
                &request.input,
                SUB_AGENT_STATUS,
                |input| sub_agent_host.status(call_context(), input),
            )
            .await
        }
        STOP_SUB_AGENT => {
            invoke_sub_agent::<StopSubAgentRequest, SubAgentReceipt, _, _>(
                &request.input,
                STOP_SUB_AGENT,
                |input| sub_agent_host.stop(call_context(), input),
            )
            .await
        }
        _ => wire_failure("protocol", "UnknownCapability"),
    }
}

async fn invoke_bridge<I, O, F, Fut>(input: &RawValue, name: &str, invoke: F) -> WireOutcome
where
    I: ContractType,
    O: ContractType,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, CallError<bridge_host_contract::BridgeHostError>>>,
{
    let Some(capability) = capability(bridge_host_contract::contract_descriptor(), name) else {
        return wire_failure("internal", "MissingDescriptor");
    };
    let input = match decode_input::<I>(input, capability) {
        Ok(input) => input,
        Err(error) => return error,
    };
    match invoke(input).await {
        Ok(output) => encode_output(output, capability),
        Err(error) => wire_call_error(error),
    }
}

async fn invoke_native<I, O, F, Fut>(input: &RawValue, name: &str, invoke: F) -> WireOutcome
where
    I: ContractType,
    O: ContractType,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, CallError<native_channel_contract::NativeChannelError>>>,
{
    let Some(capability) = capability(native_channel_contract::contract_descriptor(), name) else {
        return wire_failure("internal", "MissingDescriptor");
    };
    let input = match decode_input::<I>(input, capability) {
        Ok(input) => input,
        Err(error) => return error,
    };
    match invoke(input).await {
        Ok(output) => encode_output(output, capability),
        Err(error) => wire_call_error(error),
    }
}

async fn invoke_sub_agent<I, O, F, Fut>(input: &RawValue, name: &str, invoke: F) -> WireOutcome
where
    I: ContractType,
    O: ContractType,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, CallError<sub_agent_host_contract::SubAgentHostError>>>,
{
    let Some(capability) = capability(sub_agent_host_contract::contract_descriptor(), name) else {
        return wire_failure("internal", "MissingDescriptor");
    };
    let input = match decode_input::<I>(input, capability) {
        Ok(input) => input,
        Err(error) => return error,
    };
    match invoke(input).await {
        Ok(output) => encode_output(output, capability),
        Err(error) => wire_call_error(error),
    }
}

async fn invoke_agent<I, O, F, Fut>(input: &RawValue, name: &str, invoke: F) -> WireOutcome
where
    I: ContractType,
    O: ContractType,
    F: FnOnce(I) -> Fut,
    Fut: Future<Output = Result<O, CallError<agent_host_contract::AgentHostError>>>,
{
    let Some(capability) = capability(agent_host_contract::contract_descriptor(), name) else {
        return wire_failure("internal", "MissingDescriptor");
    };
    let input = match decode_input::<I>(input, capability) {
        Ok(input) => input,
        Err(error) => return error,
    };
    match invoke(input).await {
        Ok(output) => encode_output(output, capability),
        Err(error) => wire_call_error(error),
    }
}

fn decode_input<T: ContractType>(
    input: &RawValue,
    capability: &CapabilityDescriptor,
) -> Result<T, WireOutcome> {
    let slot = json::decode(
        input.get().as_bytes(),
        capability.input(),
        DecodeRole::ProviderInput,
        Limits::new(MAX_REQUEST_BYTES, MAX_JSON_DEPTH),
    )
    .map_err(|_| wire_failure("contract", "InvalidInput"))?;
    T::decode(&slot).map_err(|_| wire_failure("contract", "InvalidInput"))
}

fn encode_output<T: ContractType>(output: T, capability: &CapabilityDescriptor) -> WireOutcome {
    let encoded = output
        .encode()
        .map_err(|_| ())
        .and_then(|slot| json::encode(&slot, capability.output()).map_err(|_| ()))
        .and_then(|bytes| String::from_utf8(bytes).map_err(|_| ()))
        .and_then(|text| RawValue::from_string(text).map_err(|_| ()));
    match encoded {
        Ok(output) => WireOutcome::Ok { output },
        Err(()) => wire_failure("internal", "InvalidProviderOutput"),
    }
}

fn wire_call_error<E: ContractError>(error: CallError<E>) -> WireOutcome {
    match error {
        CallError::Domain(error) => wire_failure("domain", error.error_tag()),
        CallError::Deadline => wire_failure("transport", "Deadline"),
        CallError::Cancelled => wire_failure("transport", "Cancelled"),
        CallError::Unavailable(detail) => wire_failure("transport", detail.code()),
        CallError::ContractViolation(detail) => wire_failure("contract", detail.code()),
        CallError::InvalidResponse(detail) => wire_failure("internal", detail.code()),
        CallError::Internal(detail) => wire_failure("internal", detail.code()),
        _ => wire_failure("internal", "UnknownFailure"),
    }
}

fn wire_failure(kind: &str, code: &str) -> WireOutcome {
    WireOutcome::Error {
        error: WireError {
            kind: kind.to_owned(),
            code: code.to_owned(),
        },
    }
}

fn capability<'a>(
    contract: &'a ContractDescriptor,
    qualified_name: &str,
) -> Option<&'a CapabilityDescriptor> {
    let (_, name) = qualified_name.split_once('.')?;
    contract
        .capabilities()
        .iter()
        .find(|capability| capability.name().as_str() == name)
}

fn call_context() -> CallContext {
    CallContext::new(
        Caller::System("crab-v2-local-ipc"),
        None,
        CancelToken::new(),
        TraceContext::empty(),
        None,
    )
}

async fn read_frame(
    reader: &mut (impl tokio::io::AsyncBufRead + Unpin),
    limit: usize,
) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut bounded = reader.take((limit + 2) as u64);
    let count = bounded.read_until(b'\n', &mut bytes).await?;
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "IPC stream closed before a frame",
        ));
    }
    if bytes.len() > limit + 1 || bytes.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "IPC frame is unterminated or exceeds limit",
        ));
    }
    bytes.pop();
    if bytes.last() == Some(&b'\r') {
        bytes.pop();
    }
    Ok(bytes)
}

async fn prepare_socket(path: &Path) -> Result<(), ChannelIpcStartupError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_socket() {
        return Err(ChannelIpcStartupError::UnsafeSocketPath);
    }
    match UnixStream::connect(path).await {
        Ok(_) => Err(ChannelIpcStartupError::AlreadyRunning),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound
            ) =>
        {
            fs::remove_file(path)?;
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn load_or_create_token(path: &Path) -> Result<String, ChannelIpcStartupError> {
    let token = generate_token()?;
    match OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
    {
        Ok(mut file) => {
            if let Err(error) = file
                .write_all(token.as_bytes())
                .and_then(|()| file.sync_all())
            {
                drop(file);
                let _ = fs::remove_file(path);
                return Err(error.into());
            }
            Ok(token)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => load_token(path),
        Err(error) => Err(error.into()),
    }
}

fn generate_token() -> Result<String, ChannelIpcStartupError> {
    let mut entropy = [0_u8; 32];
    OpenOptions::new()
        .read(true)
        .open("/dev/urandom")?
        .read_exact(&mut entropy)?;
    let mut token = String::with_capacity(64);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in entropy {
        token.push(HEX[(byte >> 4) as usize] as char);
        token.push(HEX[(byte & 0x0f) as usize] as char);
    }
    Ok(token)
}

fn load_token(path: &Path) -> Result<String, ChannelIpcStartupError> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o077 != 0 {
        return Err(ChannelIpcStartupError::UnsafeTokenFile);
    }
    let mut token = String::new();
    OpenOptions::new()
        .read(true)
        .open(path)?
        .read_to_string(&mut token)?;
    if token.len() != 64
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ChannelIpcStartupError::UnsafeTokenFile);
    }
    Ok(token)
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

#[cfg(test)]
mod tests {
    use super::{WireRequest, constant_time_equal};

    #[test]
    fn envelope_is_strict_and_authentication_comparison_is_exact() {
        let valid = r#"{
            "protocolVersion":1,
            "requestId":"request-1",
            "authentication":"token",
            "capability":"native-channel.channel_status",
            "input":{"binding_id":"binding-1"}
        }"#;
        assert!(serde_json::from_str::<WireRequest>(valid).is_ok());
        let unknown = valid.replace("\"input\":", "\"unknown\":true,\"input\":");
        assert!(serde_json::from_str::<WireRequest>(&unknown).is_err());
        assert!(constant_time_equal(b"same", b"same"));
        assert!(!constant_time_equal(b"same", b"diff"));
        assert!(!constant_time_equal(b"same", b"short"));
    }
}
