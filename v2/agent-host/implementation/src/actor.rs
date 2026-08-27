use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use agent_client_protocol::{
    AcpAgent, Agent, ConnectionTo, JsonRpcRequest, JsonRpcResponse, LineDirection, Responder,
    schema::{ProtocolVersion, v1, v2},
};
use boxology_contract::ContractError as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use uuid::Uuid;

use crate::Clock;
use crate::{
    AcpEventDirection, AcpNegotiation, AgentDiagnosticKind, AgentHostError, AgentInputMode,
    AgentLifecycle, AgentSession, CRAB_AGENT_ID_ENV, CRAB_PARENT_SESSION_ID_ENV,
    CRAB_SESSION_ID_ENV, CRAB_STATE_DIRECTORY_ENV, CRAB_SUB_AGENT_ID_ENV,
    CRAB_WORKING_DIRECTORY_ENV, ConfiguredAgent, ConfiguredMcpServer, OperationReceipt,
    PromptAccepted, PromptDisposition, PromptRequest, store::AgentStore,
};

const MAX_NATIVE_PROMPT_BYTES: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcRequest)]
#[request(method = "_session/steering", response = SessionSteeringResponse)]
#[serde(rename_all = "camelCase")]
struct SessionSteeringRequest {
    session_id: v1::SessionId,
    prompt: Vec<v1::ContentBlock>,
    #[serde(rename = "_meta")]
    meta: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonRpcResponse)]
#[serde(rename_all = "camelCase")]
struct SessionSteeringResponse {
    outcome: String,
    #[serde(default)]
    reason: Option<String>,
}

pub(crate) enum SessionCommand {
    Prompt {
        request: PromptRequest,
        reply: oneshot::Sender<Result<PromptAccepted, AgentHostError>>,
    },
    Cancel {
        run_id: String,
        reply: oneshot::Sender<Result<OperationReceipt, AgentHostError>>,
    },
    Close {
        reply: oneshot::Sender<Result<OperationReceipt, AgentHostError>>,
    },
    Detach {
        reply: oneshot::Sender<Result<OperationReceipt, AgentHostError>>,
    },
}

pub(crate) enum SessionLaunch {
    New { bootstrap_prompt: Option<String> },
    Resume { native_session_id: String },
    Fork { native_parent_session_id: String },
}

impl SessionLaunch {
    fn bootstrap_prompt(&self) -> Option<String> {
        match self {
            Self::New { bootstrap_prompt } => bootstrap_prompt.clone(),
            Self::Resume { .. } | Self::Fork { .. } => None,
        }
    }
}

#[derive(Clone)]
pub(crate) struct SessionHandle {
    pub(crate) commands: mpsc::Sender<SessionCommand>,
    /// Serializes lifecycle-changing host calls with prompt acceptance for this session.
    pub(crate) control: Arc<Mutex<()>>,
}

enum ActorSignal {
    V1PromptFinished {
        run_id: String,
        succeeded: bool,
    },
    V2PromptAcknowledged {
        run_id: String,
        client_turn_id: String,
        started_run: bool,
        succeeded: bool,
    },
    V2Idle,
    Fatal,
}

const ACTOR_SIGNAL_QUEUE_CAPACITY: usize = 128;

#[derive(Clone)]
struct ActorSignalSender {
    events: mpsc::Sender<ActorSignal>,
    fatal: watch::Sender<bool>,
}

struct ActorSignalReceiver {
    events: mpsc::Receiver<ActorSignal>,
    fatal: watch::Receiver<bool>,
}

fn actor_signal_channel() -> (ActorSignalSender, ActorSignalReceiver) {
    let (events, event_receiver) = mpsc::channel(ACTOR_SIGNAL_QUEUE_CAPACITY);
    let (fatal, fatal_receiver) = watch::channel(false);
    (
        ActorSignalSender { events, fatal },
        ActorSignalReceiver {
            events: event_receiver,
            fatal: fatal_receiver,
        },
    )
}

impl ActorSignalSender {
    async fn send(&self, signal: ActorSignal) -> Result<(), mpsc::error::SendError<ActorSignal>> {
        self.events.send(signal).await
    }

    fn fatal(&self) {
        self.fatal.send_replace(true);
    }
}

impl ActorSignalReceiver {
    async fn recv(&mut self) -> Option<ActorSignal> {
        if *self.fatal.borrow_and_update() {
            return Some(ActorSignal::Fatal);
        }
        tokio::select! {
            biased;
            _ = self.fatal.changed() => Some(ActorSignal::Fatal),
            signal = self.events.recv() => signal,
        }
    }
}

struct ActorState {
    active_run_id: Option<String>,
}

const DETACH_CANCEL_TIMEOUT: Duration = Duration::from_secs(5);

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_session(
    agent: Arc<ConfiguredAgent>,
    store: Arc<AgentStore>,
    clock: Clock,
    session_id: String,
    working_directory: PathBuf,
    launch: SessionLaunch,
    metadata: Map<String, Value>,
    state_directory: Option<PathBuf>,
) -> (
    SessionHandle,
    oneshot::Receiver<Result<AgentSession, AgentHostError>>,
) {
    let (command_tx, command_rx) = mpsc::channel(128);
    let (opened_tx, opened_rx) = oneshot::channel();
    let handle = SessionHandle {
        commands: command_tx,
        control: Arc::new(Mutex::new(())),
    };
    tokio::spawn(async move {
        let result = match agent.protocol {
            crate::AgentProtocol::V1 => {
                run_v1_session(
                    agent,
                    store.clone(),
                    clock.clone(),
                    session_id.clone(),
                    working_directory,
                    launch,
                    metadata,
                    state_directory,
                    command_rx,
                    opened_tx,
                )
                .await
            }
            crate::AgentProtocol::V2 => {
                run_v2_session(
                    agent,
                    store.clone(),
                    clock.clone(),
                    session_id.clone(),
                    working_directory,
                    launch,
                    metadata,
                    state_directory,
                    command_rx,
                    opened_tx,
                )
                .await
            }
        };
        if let Err(error) = &result {
            let now_ms = clock().unwrap_or_default();
            let _ = store.record_diagnostic(
                &session_id,
                &AgentDiagnosticKind::ActorFailure,
                &format!("agent actor failed: {}", error.error_tag()),
                now_ms,
            );
        }
        if result.is_err()
            && !matches!(
                store.status(&session_id).map(|status| status.lifecycle),
                Ok(AgentLifecycle::Stopped | AgentLifecycle::Detached)
            )
        {
            let now_ms = clock().unwrap_or_default();
            let _ = store.set_lifecycle(&session_id, &AgentLifecycle::Failed, now_ms);
        }
    });
    (handle, opened_rx)
}

#[allow(clippy::too_many_arguments)]
async fn run_v1_session(
    agent: Arc<ConfiguredAgent>,
    store: Arc<AgentStore>,
    clock: Clock,
    session_id: String,
    working_directory: PathBuf,
    launch: SessionLaunch,
    metadata: Map<String, Value>,
    state_directory: Option<PathBuf>,
    command_rx: mpsc::Receiver<SessionCommand>,
    opened_tx: oneshot::Sender<Result<AgentSession, AgentHostError>>,
) -> Result<(), AgentHostError> {
    let (signal_tx, signal_rx) = actor_signal_channel();
    let process = instrumented_process(
        &agent,
        store.clone(),
        clock.clone(),
        session_id.clone(),
        signal_tx.clone(),
    );
    let permission_store = store.clone();
    let permission_clock = clock.clone();
    let permission_session = session_id.clone();
    let permission_signals = signal_tx.clone();

    let result = agent_client_protocol::Client
        .builder()
        .name("crab-v2")
        .on_receive_notification(
            async move |_notification: v1::SessionNotification, _connection| Ok(()),
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: v1::RequestPermissionRequest, responder, _connection| {
                resolve_v1_permission(
                    request,
                    responder,
                    &permission_store,
                    &permission_clock,
                    &permission_session,
                    &permission_signals,
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(process, async move |connection: ConnectionTo<Agent>| {
            let initialized = initialize_v1(
                &connection,
                &store,
                &clock,
                &session_id,
                working_directory,
                metadata,
                &agent.agent_id,
                &agent.session_options,
                &agent.session_mcp_servers,
                agent.steering_extension,
                state_directory.as_deref(),
                &launch,
            )
            .await;
            let session = match initialized {
                Ok(session) => session,
                Err(error) => {
                    let _ = opened_tx.send(Err(error.clone()));
                    return Err(acp_error(error));
                }
            };
            let steering_enabled = matches!(
                session.negotiation.steering,
                crate::SteeringSupport::AgentExtension
            );
            let _ = opened_tx.send(Ok(session.clone()));
            run_v1_loop(
                connection,
                store,
                clock,
                session_id,
                session.native_session_id,
                launch.bootstrap_prompt(),
                command_rx,
                signal_rx,
                signal_tx,
                steering_enabled,
            )
            .await
            .map_err(acp_error)
        })
        .await;
    result.map_err(|_| AgentHostError::TransportFailed)
}

#[allow(clippy::too_many_arguments)]
async fn run_v2_session(
    agent: Arc<ConfiguredAgent>,
    store: Arc<AgentStore>,
    clock: Clock,
    session_id: String,
    working_directory: PathBuf,
    launch: SessionLaunch,
    metadata: Map<String, Value>,
    state_directory: Option<PathBuf>,
    command_rx: mpsc::Receiver<SessionCommand>,
    opened_tx: oneshot::Sender<Result<AgentSession, AgentHostError>>,
) -> Result<(), AgentHostError> {
    let (signal_tx, signal_rx) = actor_signal_channel();
    let process = instrumented_process(
        &agent,
        store.clone(),
        clock.clone(),
        session_id.clone(),
        signal_tx.clone(),
    );
    let update_signals = signal_tx.clone();
    let permission_store = store.clone();
    let permission_clock = clock.clone();
    let permission_session = session_id.clone();
    let permission_signals = signal_tx.clone();

    let result = agent_client_protocol::Client
        .v2()
        .name("crab-v2")
        .on_receive_notification(
            async move |notification: v2::UpdateSessionNotification, _connection| {
                if matches!(
                    notification.update,
                    v2::SessionUpdate::StateUpdate(v2::StateUpdate::Idle(_))
                ) {
                    let _ = update_signals.send(ActorSignal::V2Idle).await;
                }
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        )
        .on_receive_request(
            async move |request: v2::RequestPermissionRequest, responder, _connection| {
                resolve_v2_permission(
                    request,
                    responder,
                    &permission_store,
                    &permission_clock,
                    &permission_session,
                    &permission_signals,
                )
            },
            agent_client_protocol::on_receive_request!(),
        )
        .connect_with(process, async move |connection: ConnectionTo<Agent>| {
            let initialized = initialize_v2(
                &connection,
                &store,
                &clock,
                &session_id,
                working_directory,
                metadata,
                &agent.agent_id,
                &agent.session_options,
                &agent.session_mcp_servers,
                state_directory.as_deref(),
                &launch,
            )
            .await;
            let session = match initialized {
                Ok(session) => session,
                Err(error) => {
                    let _ = opened_tx.send(Err(error.clone()));
                    return Err(acp_error(error));
                }
            };
            let _ = opened_tx.send(Ok(session.clone()));
            run_v2_loop(
                connection,
                store,
                clock,
                session_id,
                session.native_session_id,
                launch.bootstrap_prompt(),
                command_rx,
                signal_rx,
                signal_tx,
            )
            .await
            .map_err(acp_error)
        })
        .await;
    result.map_err(|_| AgentHostError::TransportFailed)
}

fn instrumented_process(
    agent: &ConfiguredAgent,
    store: Arc<AgentStore>,
    clock: Clock,
    session_id: String,
    signals: ActorSignalSender,
) -> AcpAgent {
    AcpAgent::new(agent.process_config()).with_debug(move |line, direction| {
        let direction = match direction {
            LineDirection::Stdin => AcpEventDirection::ClientToAgent,
            LineDirection::Stdout => AcpEventDirection::AgentToClient,
            LineDirection::Stderr => {
                let recorded = clock().and_then(|now_ms| {
                    store.record_diagnostic(
                        &session_id,
                        &AgentDiagnosticKind::AdapterStderr,
                        line,
                        now_ms,
                    )
                });
                if recorded.is_err() {
                    signals.fatal();
                }
                return;
            }
        };
        let recorded = clock()
            .and_then(|now_ms| store.record_native_line(&session_id, direction, line, now_ms));
        if recorded.is_err() {
            signals.fatal();
        }
    })
}

#[allow(clippy::too_many_arguments)]
async fn initialize_v1(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    working_directory: PathBuf,
    metadata: Map<String, Value>,
    agent_id: &str,
    session_options: &BTreeMap<String, String>,
    mcp_servers: &[ConfiguredMcpServer],
    steering_extension: Option<crate::AgentSteeringExtension>,
    state_directory: Option<&std::path::Path>,
    launch: &SessionLaunch,
) -> Result<AgentSession, AgentHostError> {
    let response = connection
        .send_request(v1::InitializeRequest::new(ProtocolVersion::V1).client_info(
            v1::Implementation::new("crab-v2", env!("CARGO_PKG_VERSION")),
        ))
        .block_task()
        .await
        .map_err(|_| AgentHostError::ProtocolNegotiationFailed)?;
    if response.protocol_version != ProtocolVersion::V1 {
        return Err(AgentHostError::UnsupportedProtocolProfile);
    }
    let steering = match steering_extension {
        Some(crate::AgentSteeringExtension::SessionSteeringV1)
            if response
                .meta
                .as_ref()
                .and_then(|meta| meta.get("steering"))
                .and_then(Value::as_object)
                .and_then(|steering| steering.get("supported"))
                .and_then(Value::as_bool)
                == Some(true) =>
        {
            crate::SteeringSupport::AgentExtension
        }
        Some(crate::AgentSteeringExtension::SessionSteeringV1) => {
            return Err(AgentHostError::ProtocolNegotiationFailed);
        }
        None => crate::SteeringSupport::TurnBoundaryQueue,
    };
    let mcp_servers = build_v1_mcp_servers(
        mcp_servers,
        state_directory,
        session_id,
        agent_id,
        &working_directory,
        &metadata,
    )?;
    let native_session_id = match launch {
        SessionLaunch::New { .. } => {
            connection
                .send_request(
                    v1::NewSessionRequest::new(working_directory)
                        .mcp_servers(mcp_servers)
                        .meta(nonempty(metadata)),
                )
                .block_task()
                .await
                .map_err(|_| AgentHostError::TransportFailed)?
                .session_id
        }
        SessionLaunch::Resume { native_session_id } => {
            if response
                .agent_capabilities
                .session_capabilities
                .resume
                .is_none()
            {
                return Err(AgentHostError::SessionResumeUnavailable);
            }
            let native_session_id = v1::SessionId::new(native_session_id.clone());
            connection
                .send_request(
                    v1::ResumeSessionRequest::new(native_session_id.clone(), working_directory)
                        .mcp_servers(mcp_servers)
                        .meta(nonempty(metadata)),
                )
                .block_task()
                .await
                .map_err(|_| AgentHostError::TransportFailed)?;
            native_session_id
        }
        SessionLaunch::Fork {
            native_parent_session_id,
        } => {
            if response
                .agent_capabilities
                .session_capabilities
                .fork
                .is_none()
            {
                return Err(AgentHostError::SessionForkUnavailable);
            }
            connection
                .send_request(
                    v1::ForkSessionRequest::new(
                        v1::SessionId::new(native_parent_session_id.clone()),
                        working_directory,
                    )
                    .mcp_servers(mcp_servers)
                    .meta(nonempty(metadata)),
                )
                .block_task()
                .await
                .map_err(|_| AgentHostError::SessionForkUnavailable)?
                .session_id
        }
    };
    apply_v1_session_options(connection, &native_session_id, session_options).await?;
    let capabilities = serde_json::to_string(&response.agent_capabilities)
        .map_err(|_| AgentHostError::ProtocolNegotiationFailed)?;
    let now_ms = clock()?;
    store.set_ready(
        session_id,
        &native_session_id.to_string(),
        &AcpNegotiation {
            protocol_version: 1,
            protocol_profile: crate::AcpProtocolProfile::V1Stable,
            steering,
            compaction_reporting: crate::CompactionReporting::OpaqueAgentManaged,
            agent_capabilities_json: capabilities,
        },
        now_ms,
    )
}

#[allow(clippy::too_many_arguments)]
async fn initialize_v2(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    working_directory: PathBuf,
    metadata: Map<String, Value>,
    agent_id: &str,
    session_options: &BTreeMap<String, String>,
    mcp_servers: &[ConfiguredMcpServer],
    state_directory: Option<&std::path::Path>,
    launch: &SessionLaunch,
) -> Result<AgentSession, AgentHostError> {
    let response = connection
        .send_request(v2::InitializeRequest::new(
            ProtocolVersion::V2,
            v2::Implementation::new("crab-v2", env!("CARGO_PKG_VERSION")),
        ))
        .block_task()
        .await
        .map_err(|_| AgentHostError::ProtocolNegotiationFailed)?;
    if response.protocol_version != ProtocolVersion::V2 {
        return Err(AgentHostError::UnsupportedProtocolProfile);
    }
    if !mcp_servers.is_empty()
        && response
            .capabilities
            .session
            .as_ref()
            .and_then(|session| session.mcp.as_ref())
            .and_then(|mcp| mcp.stdio.as_ref())
            .is_none()
    {
        return Err(AgentHostError::ProtocolNegotiationFailed);
    }
    let mcp_servers = build_v2_mcp_servers(
        mcp_servers,
        state_directory,
        session_id,
        agent_id,
        &working_directory,
        &metadata,
    )?;
    let native_session_id = match launch {
        SessionLaunch::New { .. } => {
            connection
                .send_request(
                    v2::NewSessionRequest::new(working_directory)
                        .mcp_servers(mcp_servers)
                        .meta(nonempty(metadata)),
                )
                .block_task()
                .await
                .map_err(|_| AgentHostError::TransportFailed)?
                .session_id
        }
        SessionLaunch::Resume { native_session_id } => {
            if response.capabilities.session.is_none() {
                return Err(AgentHostError::SessionResumeUnavailable);
            }
            let native_session_id = v2::SessionId::new(native_session_id.clone());
            connection
                .send_request(
                    v2::ResumeSessionRequest::new(
                        native_session_id.clone(),
                        v2::AbsolutePath::new(working_directory),
                    )
                    .mcp_servers(mcp_servers)
                    .meta(nonempty(metadata)),
                )
                .block_task()
                .await
                .map_err(|_| AgentHostError::TransportFailed)?;
            native_session_id
        }
        SessionLaunch::Fork {
            native_parent_session_id,
        } => {
            if response
                .capabilities
                .session
                .as_ref()
                .and_then(|session| session.fork.as_ref())
                .is_none()
            {
                return Err(AgentHostError::SessionForkUnavailable);
            }
            connection
                .send_request(
                    v2::ForkSessionRequest::new(
                        v2::SessionId::new(native_parent_session_id.clone()),
                        v2::AbsolutePath::new(working_directory),
                    )
                    .mcp_servers(mcp_servers)
                    .meta(nonempty(metadata)),
                )
                .block_task()
                .await
                .map_err(|_| AgentHostError::SessionForkUnavailable)?
                .session_id
        }
    };
    apply_v2_session_options(connection, &native_session_id, session_options).await?;
    let capabilities = serde_json::to_string(&response.capabilities)
        .map_err(|_| AgentHostError::ProtocolNegotiationFailed)?;
    let now_ms = clock()?;
    store.set_ready(
        session_id,
        &native_session_id.to_string(),
        &AcpNegotiation {
            protocol_version: 2,
            protocol_profile: crate::AcpProtocolProfile::V2Draft,
            steering: crate::SteeringSupport::AcpV2ConcurrentPrompt,
            compaction_reporting: crate::CompactionReporting::DraftLifecycleUpdates,
            agent_capabilities_json: capabilities,
        },
        now_ms,
    )
}

fn build_v1_mcp_servers(
    configured: &[ConfiguredMcpServer],
    state_directory: Option<&std::path::Path>,
    session_id: &str,
    agent_id: &str,
    working_directory: &std::path::Path,
    metadata: &Map<String, Value>,
) -> Result<Vec<v1::McpServer>, AgentHostError> {
    configured
        .iter()
        .map(|server| {
            let environment = mcp_environment(
                server,
                state_directory,
                session_id,
                agent_id,
                working_directory,
                metadata,
            )?
            .into_iter()
            .map(|(name, value)| v1::EnvVariable::new(name, value))
            .collect();
            Ok(v1::McpServer::Stdio(
                v1::McpServerStdio::new(&server.name, server.executable.clone())
                    .args(server.arguments.clone())
                    .env(environment),
            ))
        })
        .collect()
}

fn build_v2_mcp_servers(
    configured: &[ConfiguredMcpServer],
    state_directory: Option<&std::path::Path>,
    session_id: &str,
    agent_id: &str,
    working_directory: &std::path::Path,
    metadata: &Map<String, Value>,
) -> Result<Vec<v2::McpServer>, AgentHostError> {
    configured
        .iter()
        .map(|server| {
            let environment = mcp_environment(
                server,
                state_directory,
                session_id,
                agent_id,
                working_directory,
                metadata,
            )?
            .into_iter()
            .map(|(name, value)| v2::EnvVariable::new(name, value))
            .collect();
            Ok(v2::McpServer::Stdio(
                v2::McpServerStdio::new(
                    &server.name,
                    v2::AbsolutePath::new(server.executable.clone()),
                )
                .args(server.arguments.clone())
                .env(environment),
            ))
        })
        .collect()
}

fn mcp_environment(
    server: &ConfiguredMcpServer,
    state_directory: Option<&std::path::Path>,
    session_id: &str,
    agent_id: &str,
    working_directory: &std::path::Path,
    metadata: &Map<String, Value>,
) -> Result<BTreeMap<String, String>, AgentHostError> {
    let state_directory = state_directory.ok_or(AgentHostError::InvalidConfiguration)?;
    let mut environment = server.environment.clone();
    environment.insert(
        CRAB_STATE_DIRECTORY_ENV.into(),
        state_directory.to_string_lossy().into_owned(),
    );
    environment.insert(CRAB_SESSION_ID_ENV.into(), session_id.into());
    environment.insert(CRAB_AGENT_ID_ENV.into(), agent_id.into());
    environment.insert(
        CRAB_WORKING_DIRECTORY_ENV.into(),
        working_directory.to_string_lossy().into_owned(),
    );
    if let Some(context) = metadata.get("crabSubAgent") {
        let Value::Object(context) = context else {
            return Err(AgentHostError::InvalidNativePayload);
        };
        let sub_agent_id = context
            .get("subAgentId")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or(AgentHostError::InvalidNativePayload)?;
        let parent_session_id = context
            .get("parentSessionId")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or(AgentHostError::InvalidNativePayload)?;
        environment.insert(CRAB_SUB_AGENT_ID_ENV.into(), sub_agent_id.into());
        environment.insert(CRAB_PARENT_SESSION_ID_ENV.into(), parent_session_id.into());
    }
    Ok(environment)
}

async fn apply_v1_session_options(
    connection: &ConnectionTo<Agent>,
    native_session_id: &v1::SessionId,
    required: &BTreeMap<String, String>,
) -> Result<(), AgentHostError> {
    for (config_id, value) in required {
        let response = connection
            .send_request(v1::SetSessionConfigOptionRequest::new(
                native_session_id.clone(),
                config_id.clone(),
                v1::SessionConfigOptionValue::value_id(value.clone()),
            ))
            .block_task()
            .await
            .map_err(|_| AgentHostError::ProtocolNegotiationFailed)?;
        let verified = response.config_options.iter().any(|option| {
            option.id.to_string() == *config_id
                && matches!(
                    &option.kind,
                    v1::SessionConfigKind::Select(select)
                        if select.current_value.to_string() == *value
                )
        });
        if !verified {
            return Err(AgentHostError::ProtocolNegotiationFailed);
        }
    }
    Ok(())
}

async fn apply_v2_session_options(
    connection: &ConnectionTo<Agent>,
    native_session_id: &v2::SessionId,
    required: &BTreeMap<String, String>,
) -> Result<(), AgentHostError> {
    for (config_id, value) in required {
        let response = connection
            .send_request(v2::SetSessionConfigOptionRequest::new(
                native_session_id.clone(),
                config_id.clone(),
                v2::SessionConfigOptionValue::id(value.clone()),
            ))
            .block_task()
            .await
            .map_err(|_| AgentHostError::ProtocolNegotiationFailed)?;
        let verified = response.config_options.iter().any(|option| {
            option.config_id.to_string() == *config_id
                && matches!(
                    &option.kind,
                    v2::SessionConfigKind::Select(select)
                        if select.current_value.to_string() == *value
                )
        });
        if !verified {
            return Err(AgentHostError::ProtocolNegotiationFailed);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_v1_loop(
    connection: ConnectionTo<Agent>,
    store: Arc<AgentStore>,
    clock: Clock,
    session_id: String,
    native_session_id: String,
    bootstrap_prompt: Option<String>,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut signals: ActorSignalReceiver,
    signal_tx: ActorSignalSender,
    steering_enabled: bool,
) -> Result<(), AgentHostError> {
    let mut state = ActorState {
        active_run_id: None,
    };
    if let Some(bootstrap) = bootstrap_prompt.filter(|prompt| !prompt.is_empty()) {
        let (reply, _) = oneshot::channel();
        accept_v1_prompt(
            &connection,
            &store,
            &clock,
            &session_id,
            &native_session_id,
            &mut state,
            PromptRequest {
                session_id: session_id.clone(),
                client_turn_id: "__crab_bootstrap__".into(),
                mode: AgentInputMode::Queue,
                native_prompt_json: text_prompt_json(&bootstrap)?,
            },
            reply,
            &signal_tx,
            steering_enabled,
        )
        .await;
    }

    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(SessionCommand::Prompt { request, reply }) => {
                    accept_v1_prompt(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        &mut state, request, reply, &signal_tx, steering_enabled,
                    ).await;
                }
                Some(SessionCommand::Cancel { run_id, reply }) => {
                    let result = cancel_v1(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        &mut state, &run_id,
                    );
                    let _ = reply.send(result);
                }
                Some(SessionCommand::Close { reply }) => {
                    let result = stop_v1(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        state.active_run_id.is_some(),
                    ).await;
                    let completion = result.as_ref().map(|_| ()).map_err(Clone::clone);
                    let _ = reply.send(result);
                    completion?;
                    return Ok(());
                }
                Some(SessionCommand::Detach { reply }) => {
                    let result = detach_v1(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        state.active_run_id.as_deref(), &mut signals,
                    ).await;
                    let completion = result.as_ref().map(|_| ()).map_err(Clone::clone);
                    let _ = reply.send(result);
                    completion?;
                    return Ok(());
                }
                None => {
                    detach_v1(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        state.active_run_id.as_deref(), &mut signals,
                    ).await?;
                    return Ok(());
                }
            },
            signal = signals.recv() => match signal {
                Some(ActorSignal::V1PromptFinished { run_id, succeeded }) => {
                    if state.active_run_id.as_deref() == Some(run_id.as_str()) {
                        store.complete_run(
                            &session_id,
                            &run_id,
                            if succeeded { "Completed" } else { "Failed" },
                            clock()?,
                        )?;
                        state.active_run_id = None;
                        start_next_v1(
                            &connection, &store, &clock, &session_id, &native_session_id,
                            &mut state, &signal_tx,
                        )?;
                    }
                }
                Some(ActorSignal::Fatal) | None => return Err(AgentHostError::TransportFailed),
                _ => {}
            },
            _ = connection.incoming_closed() => return Err(AgentHostError::TransportFailed),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_v2_loop(
    connection: ConnectionTo<Agent>,
    store: Arc<AgentStore>,
    clock: Clock,
    session_id: String,
    native_session_id: String,
    bootstrap_prompt: Option<String>,
    mut commands: mpsc::Receiver<SessionCommand>,
    mut signals: ActorSignalReceiver,
    signal_tx: ActorSignalSender,
) -> Result<(), AgentHostError> {
    let mut state = ActorState {
        active_run_id: None,
    };
    if let Some(bootstrap) = bootstrap_prompt.filter(|prompt| !prompt.is_empty()) {
        let (reply, _) = oneshot::channel();
        accept_v2_prompt(
            &connection,
            &store,
            &clock,
            &session_id,
            &native_session_id,
            &mut state,
            PromptRequest {
                session_id: session_id.clone(),
                client_turn_id: "__crab_bootstrap__".into(),
                mode: AgentInputMode::Queue,
                native_prompt_json: text_prompt_json(&bootstrap)?,
            },
            reply,
            &signal_tx,
        );
    }

    loop {
        tokio::select! {
            biased;
            signal = signals.recv() => match signal {
                Some(ActorSignal::V2Idle) => {
                    if let Some(run_id) = state.active_run_id.take() {
                        store.complete_run(&session_id, &run_id, "Completed", clock()?)?;
                        start_next_v2(
                            &connection, &store, &clock, &session_id, &native_session_id,
                            &mut state, &signal_tx,
                        )?;
                    }
                }
                Some(ActorSignal::V2PromptAcknowledged {
                    run_id,
                    client_turn_id,
                    started_run,
                    succeeded,
                }) if !succeeded => {
                    store.fail_prompt(&session_id, &client_turn_id)?;
                    if started_run && state.active_run_id.as_deref() == Some(run_id.as_str()) {
                        store.complete_run(&session_id, &run_id, "Failed", clock()?)?;
                        state.active_run_id = None;
                        start_next_v2(
                            &connection, &store, &clock, &session_id, &native_session_id,
                            &mut state, &signal_tx,
                        )?;
                    }
                }
                Some(ActorSignal::Fatal) | None => return Err(AgentHostError::TransportFailed),
                _ => {}
            },
            command = commands.recv() => match command {
                Some(SessionCommand::Prompt { request, reply }) => {
                    accept_v2_prompt(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        &mut state, request, reply, &signal_tx,
                    );
                }
                Some(SessionCommand::Cancel { run_id, reply }) => {
                    let result = cancel_v2(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        &mut state, &run_id,
                    );
                    let _ = reply.send(result);
                }
                Some(SessionCommand::Close { reply }) => {
                    let result = stop_v2(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        state.active_run_id.is_some(),
                    ).await;
                    let completion = result.as_ref().map(|_| ()).map_err(Clone::clone);
                    let _ = reply.send(result);
                    completion?;
                    return Ok(());
                }
                Some(SessionCommand::Detach { reply }) => {
                    let result = detach_v2(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        state.active_run_id.is_some(), &mut signals,
                    ).await;
                    let completion = result.as_ref().map(|_| ()).map_err(Clone::clone);
                    let _ = reply.send(result);
                    completion?;
                    return Ok(());
                }
                None => {
                    detach_v2(
                        &connection, &store, &clock, &session_id, &native_session_id,
                        state.active_run_id.is_some(), &mut signals,
                    ).await?;
                    return Ok(());
                }
            },
            _ = connection.incoming_closed() => return Err(AgentHostError::TransportFailed),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_v1_prompt(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    request: PromptRequest,
    reply: oneshot::Sender<Result<PromptAccepted, AgentHostError>>,
    signals: &ActorSignalSender,
    steering_enabled: bool,
) {
    let result = accept_v1_prompt_inner(
        connection,
        store,
        clock,
        session_id,
        native_session_id,
        state,
        request,
        signals,
        steering_enabled,
    )
    .await;
    let _ = reply.send(result);
}

#[allow(clippy::too_many_arguments)]
async fn accept_v1_prompt_inner(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    request: PromptRequest,
    signals: &ActorSignalSender,
    steering_enabled: bool,
) -> Result<PromptAccepted, AgentHostError> {
    validate_prompt(session_id, &request)?;
    let content = parse_v1_prompt(&request.native_prompt_json)?;
    if let Some(accepted) = store.existing_prompt(&request)? {
        return Ok(accepted);
    }
    if matches!(request.mode, AgentInputMode::Unknown { .. }) {
        return Err(AgentHostError::InvalidNativePayload);
    }
    if matches!(request.mode, AgentInputMode::InterruptAndQueue)
        && let Some(interrupted_run_id) = state.active_run_id.clone()
    {
        // Actor command seriality keeps completion signals behind this durable enqueue.
        let cancellation = cancel_v1(
            connection,
            store,
            clock,
            session_id,
            native_session_id,
            state,
            &interrupted_run_id,
        )?;
        let run_id = new_run_id();
        let disposition = PromptDisposition::CancelRequestedThenQueued;
        let (accepted, _) = store.accept_prompt(
            &request,
            &run_id,
            &disposition,
            false,
            Some((&interrupted_run_id, cancellation.recorded_at_ms)),
            clock()?,
        )?;
        return Ok(accepted);
    }
    if matches!(request.mode, AgentInputMode::Steer) && state.active_run_id.is_some() {
        if !steering_enabled {
            return Err(AgentHostError::SteeringUnavailable);
        }
        let active_run_id = state
            .active_run_id
            .clone()
            .expect("busy v1 session has an active run");
        let response = match connection
            .send_request(SessionSteeringRequest {
                session_id: v1::SessionId::new(native_session_id.to_owned()),
                prompt: content.clone(),
                meta: Map::from_iter([(
                    "steering".into(),
                    json!({ "idleBehavior": "promptRequired" }),
                )]),
            })
            .block_task()
            .await
        {
            Ok(response) => response,
            Err(_) => {
                signals.fatal();
                return Err(AgentHostError::TransportFailed);
            }
        };
        match (response.outcome.as_str(), response.reason.as_deref()) {
            ("injected", _) => {
                let disposition = PromptDisposition::ContributedToActiveWork;
                let (accepted, _) = store.accept_prompt(
                    &request,
                    &active_run_id,
                    &disposition,
                    false,
                    None,
                    clock()?,
                )?;
                return Ok(accepted);
            }
            ("promptRequired", Some("noRunningTurn")) => {
                store.complete_run(session_id, &active_run_id, "Completed", clock()?)?;
                state.active_run_id = None;
            }
            _ => {
                signals.fatal();
                return Err(AgentHostError::TransportFailed);
            }
        }
    }
    let run_id = new_run_id();
    let busy = state.active_run_id.is_some();
    let disposition = if busy {
        PromptDisposition::QueuedForTurnBoundary
    } else {
        PromptDisposition::StartedForegroundWork
    };
    let (accepted, inserted) =
        store.accept_prompt(&request, &run_id, &disposition, !busy, None, clock()?)?;
    if !inserted {
        return Ok(accepted);
    }
    if !busy {
        state.active_run_id = Some(run_id.clone());
        dispatch_v1_prompt(
            connection,
            native_session_id,
            content,
            run_id,
            signals.clone(),
        );
    }
    Ok(accepted)
}

#[allow(clippy::too_many_arguments)]
fn accept_v2_prompt(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    request: PromptRequest,
    reply: oneshot::Sender<Result<PromptAccepted, AgentHostError>>,
    signals: &ActorSignalSender,
) {
    let result = accept_v2_prompt_inner(
        connection,
        store,
        clock,
        session_id,
        native_session_id,
        state,
        request,
        signals,
    );
    let _ = reply.send(result);
}

#[allow(clippy::too_many_arguments)]
fn accept_v2_prompt_inner(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    request: PromptRequest,
    signals: &ActorSignalSender,
) -> Result<PromptAccepted, AgentHostError> {
    validate_prompt(session_id, &request)?;
    let content = parse_v2_prompt(&request.native_prompt_json)?;
    if let Some(accepted) = store.existing_prompt(&request)? {
        return Ok(accepted);
    }
    let busy = state.active_run_id.is_some();
    if matches!(request.mode, AgentInputMode::InterruptAndQueue)
        && let Some(interrupted_run_id) = state.active_run_id.clone()
    {
        // Actor command seriality keeps completion signals behind this durable enqueue.
        let cancellation = cancel_v2(
            connection,
            store,
            clock,
            session_id,
            native_session_id,
            state,
            &interrupted_run_id,
        )?;
        let run_id = new_run_id();
        let disposition = PromptDisposition::CancelRequestedThenQueued;
        let (accepted, _) = store.accept_prompt(
            &request,
            &run_id,
            &disposition,
            false,
            Some((&interrupted_run_id, cancellation.recorded_at_ms)),
            clock()?,
        )?;
        return Ok(accepted);
    }
    let (run_id, disposition, activate, dispatch) = match (&request.mode, busy) {
        (AgentInputMode::Queue, true) => (
            new_run_id(),
            PromptDisposition::QueuedForTurnBoundary,
            false,
            false,
        ),
        (AgentInputMode::Steer, true) => (
            state.active_run_id.clone().expect("busy session has run"),
            PromptDisposition::ContributedToActiveWork,
            false,
            true,
        ),
        (
            AgentInputMode::Queue | AgentInputMode::Steer | AgentInputMode::InterruptAndQueue,
            false,
        ) => (
            new_run_id(),
            PromptDisposition::StartedForegroundWork,
            true,
            true,
        ),
        (AgentInputMode::InterruptAndQueue, true) => {
            return Err(AgentHostError::StorageUnavailable);
        }
        (AgentInputMode::Unknown { .. }, _) => {
            return Err(AgentHostError::InvalidNativePayload);
        }
    };
    let (accepted, inserted) =
        store.accept_prompt(&request, &run_id, &disposition, activate, None, clock()?)?;
    if !inserted {
        return Ok(accepted);
    }
    if dispatch {
        if activate {
            state.active_run_id = Some(run_id.clone());
        }
        dispatch_v2_prompt(
            connection,
            native_session_id,
            content,
            run_id,
            request.client_turn_id,
            activate,
            signals.clone(),
        );
    }
    Ok(accepted)
}

fn start_next_v1(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    signals: &ActorSignalSender,
) -> Result<(), AgentHostError> {
    let Some(next) = store.next_queued_prompt(session_id)? else {
        return Ok(());
    };
    let content = parse_v1_prompt(&next.request.native_prompt_json)?;
    store.activate_queued_run(session_id, &next.run_id, clock()?)?;
    state.active_run_id = Some(next.run_id.clone());
    dispatch_v1_prompt(
        connection,
        native_session_id,
        content,
        next.run_id,
        signals.clone(),
    );
    Ok(())
}

fn start_next_v2(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    signals: &ActorSignalSender,
) -> Result<(), AgentHostError> {
    let Some(next) = store.next_queued_prompt(session_id)? else {
        return Ok(());
    };
    let content = parse_v2_prompt(&next.request.native_prompt_json)?;
    store.activate_queued_run(session_id, &next.run_id, clock()?)?;
    state.active_run_id = Some(next.run_id.clone());
    dispatch_v2_prompt(
        connection,
        native_session_id,
        content,
        next.run_id,
        next.request.client_turn_id,
        true,
        signals.clone(),
    );
    Ok(())
}

fn dispatch_v1_prompt(
    connection: &ConnectionTo<Agent>,
    native_session_id: &str,
    content: Vec<v1::ContentBlock>,
    run_id: String,
    signals: ActorSignalSender,
) {
    let request = connection.send_request(v1::PromptRequest::new(
        native_session_id.to_owned(),
        content,
    ));
    tokio::spawn(async move {
        let succeeded = request.block_task().await.is_ok();
        let _ = signals
            .send(ActorSignal::V1PromptFinished { run_id, succeeded })
            .await;
    });
}

fn dispatch_v2_prompt(
    connection: &ConnectionTo<Agent>,
    native_session_id: &str,
    content: Vec<v2::ContentBlock>,
    run_id: String,
    client_turn_id: String,
    started_run: bool,
    signals: ActorSignalSender,
) {
    let request = connection.send_request(v2::PromptRequest::new(
        native_session_id.to_owned(),
        content,
    ));
    tokio::spawn(async move {
        let succeeded = request.block_task().await.is_ok();
        let _ = signals
            .send(ActorSignal::V2PromptAcknowledged {
                run_id,
                client_turn_id,
                started_run,
                succeeded,
            })
            .await;
    });
}

fn cancel_v1(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    run_id: &str,
) -> Result<OperationReceipt, AgentHostError> {
    let now_ms = clock()?;
    if state.active_run_id.as_deref() == Some(run_id) {
        connection
            .send_notification(v1::CancelNotification::new(native_session_id.to_owned()))
            .map_err(|_| AgentHostError::TransportFailed)?;
    } else if !store.cancel_queued_run(session_id, run_id)? {
        return Err(AgentHostError::UnknownRun);
    }
    Ok(OperationReceipt {
        accepted: true,
        recorded_at_ms: now_ms,
    })
}

fn cancel_v2(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    state: &mut ActorState,
    run_id: &str,
) -> Result<OperationReceipt, AgentHostError> {
    let now_ms = clock()?;
    if state.active_run_id.as_deref() == Some(run_id) {
        connection
            .send_notification(v2::CancelSessionNotification::new(
                native_session_id.to_owned(),
            ))
            .map_err(|_| AgentHostError::TransportFailed)?;
    } else if !store.cancel_queued_run(session_id, run_id)? {
        return Err(AgentHostError::UnknownRun);
    }
    Ok(OperationReceipt {
        accepted: true,
        recorded_at_ms: now_ms,
    })
}

async fn stop_v1(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    busy: bool,
) -> Result<OperationReceipt, AgentHostError> {
    let now_ms = clock()?;
    store.set_lifecycle(session_id, &AgentLifecycle::Stopping, now_ms)?;
    if busy {
        connection
            .send_notification(v1::CancelNotification::new(native_session_id.to_owned()))
            .map_err(|_| AgentHostError::TransportFailed)?;
    }
    connection
        .send_request(v1::CloseSessionRequest::new(native_session_id.to_owned()))
        .block_task()
        .await
        .map_err(|_| AgentHostError::TransportFailed)?;
    store.set_lifecycle(session_id, &AgentLifecycle::Stopped, now_ms)?;
    Ok(OperationReceipt {
        accepted: true,
        recorded_at_ms: now_ms,
    })
}

async fn detach_v1(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    active_run_id: Option<&str>,
    signals: &mut ActorSignalReceiver,
) -> Result<OperationReceipt, AgentHostError> {
    let now_ms = clock()?;
    store.set_lifecycle(session_id, &AgentLifecycle::Detaching, now_ms)?;
    if let Some(active_run_id) = active_run_id {
        connection
            .send_notification(v1::CancelNotification::new(native_session_id.to_owned()))
            .map_err(|_| AgentHostError::TransportFailed)?;
        wait_for_v1_cancel(signals, active_run_id).await?;
    }
    store.set_lifecycle(session_id, &AgentLifecycle::Detached, now_ms)?;
    Ok(OperationReceipt {
        accepted: true,
        recorded_at_ms: now_ms,
    })
}

async fn stop_v2(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    busy: bool,
) -> Result<OperationReceipt, AgentHostError> {
    let now_ms = clock()?;
    store.set_lifecycle(session_id, &AgentLifecycle::Stopping, now_ms)?;
    if busy {
        connection
            .send_notification(v2::CancelSessionNotification::new(
                native_session_id.to_owned(),
            ))
            .map_err(|_| AgentHostError::TransportFailed)?;
    }
    connection
        .send_request(v2::CloseSessionRequest::new(native_session_id.to_owned()))
        .block_task()
        .await
        .map_err(|_| AgentHostError::TransportFailed)?;
    store.set_lifecycle(session_id, &AgentLifecycle::Stopped, now_ms)?;
    Ok(OperationReceipt {
        accepted: true,
        recorded_at_ms: now_ms,
    })
}

async fn detach_v2(
    connection: &ConnectionTo<Agent>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    native_session_id: &str,
    busy: bool,
    signals: &mut ActorSignalReceiver,
) -> Result<OperationReceipt, AgentHostError> {
    let now_ms = clock()?;
    store.set_lifecycle(session_id, &AgentLifecycle::Detaching, now_ms)?;
    if busy {
        connection
            .send_notification(v2::CancelSessionNotification::new(
                native_session_id.to_owned(),
            ))
            .map_err(|_| AgentHostError::TransportFailed)?;
        wait_for_v2_idle(signals).await?;
    }
    store.set_lifecycle(session_id, &AgentLifecycle::Detached, now_ms)?;
    Ok(OperationReceipt {
        accepted: true,
        recorded_at_ms: now_ms,
    })
}

async fn wait_for_v1_cancel(
    signals: &mut ActorSignalReceiver,
    active_run_id: &str,
) -> Result<(), AgentHostError> {
    tokio::time::timeout(DETACH_CANCEL_TIMEOUT, async {
        loop {
            match signals.recv().await {
                Some(ActorSignal::V1PromptFinished { run_id, .. }) if run_id == active_run_id => {
                    return Ok(());
                }
                Some(ActorSignal::Fatal) | None => return Err(AgentHostError::TransportFailed),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| AgentHostError::TransportFailed)?
}

async fn wait_for_v2_idle(signals: &mut ActorSignalReceiver) -> Result<(), AgentHostError> {
    tokio::time::timeout(DETACH_CANCEL_TIMEOUT, async {
        loop {
            match signals.recv().await {
                Some(ActorSignal::V2Idle) => return Ok(()),
                Some(ActorSignal::Fatal) | None => return Err(AgentHostError::TransportFailed),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| AgentHostError::TransportFailed)?
}

fn resolve_v1_permission(
    request: v1::RequestPermissionRequest,
    responder: Responder<v1::RequestPermissionResponse>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    signals: &ActorSignalSender,
) -> Result<(), agent_client_protocol::Error> {
    let Some(option) = request
        .options
        .iter()
        .find(|option| matches!(option.kind, v1::PermissionOptionKind::AllowAlways))
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| matches!(option.kind, v1::PermissionOptionKind::AllowOnce))
        })
    else {
        signals.fatal();
        return responder.respond(v1::RequestPermissionResponse::new(
            v1::RequestPermissionOutcome::Cancelled,
        ));
    };
    let response = v1::RequestPermissionResponse::new(v1::RequestPermissionOutcome::Selected(
        v1::SelectedPermissionOutcome::new(option.option_id.clone()),
    ));
    persist_permission(
        &request,
        &response,
        responder.id(),
        store,
        clock,
        session_id,
    )?;
    responder.respond(response)
}

fn resolve_v2_permission(
    request: v2::RequestPermissionRequest,
    responder: Responder<v2::RequestPermissionResponse>,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
    signals: &ActorSignalSender,
) -> Result<(), agent_client_protocol::Error> {
    let Some(option) = request
        .options
        .iter()
        .find(|option| matches!(option.kind, v2::PermissionOptionKind::AllowAlways))
        .or_else(|| {
            request
                .options
                .iter()
                .find(|option| matches!(option.kind, v2::PermissionOptionKind::AllowOnce))
        })
    else {
        signals.fatal();
        return responder.respond(v2::RequestPermissionResponse::new(
            v2::RequestPermissionOutcome::Cancelled,
        ));
    };
    let response = v2::RequestPermissionResponse::new(v2::RequestPermissionOutcome::Selected(
        v2::SelectedPermissionOutcome::new(option.option_id.clone()),
    ));
    persist_permission(
        &request,
        &response,
        responder.id(),
        store,
        clock,
        session_id,
    )?;
    responder.respond(response)
}

fn persist_permission<Request: Serialize, Response: Serialize>(
    request: &Request,
    response: &Response,
    id: &v1::RequestId,
    store: &AgentStore,
    clock: &Clock,
    session_id: &str,
) -> Result<(), agent_client_protocol::Error> {
    let request_id = request_id(id);
    let native_request = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "method": "session/request_permission",
        "params": request,
    }))
    .map_err(|_| acp_error(AgentHostError::InvalidNativePayload))?;
    let native_response = serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": response,
    }))
    .map_err(|_| acp_error(AgentHostError::InvalidNativePayload))?;
    let now_ms = clock().map_err(acp_error)?;
    store
        .record_permission_resolution(
            session_id,
            &request_id,
            &native_request,
            &native_response,
            now_ms,
        )
        .map_err(acp_error)
}

fn validate_prompt(session_id: &str, request: &PromptRequest) -> Result<(), AgentHostError> {
    if request.session_id != session_id
        || request.client_turn_id.trim().is_empty()
        || request.native_prompt_json.trim().is_empty()
        || request.native_prompt_json.len() > MAX_NATIVE_PROMPT_BYTES
    {
        return Err(AgentHostError::InvalidNativePayload);
    }
    Ok(())
}

fn parse_v1_prompt(raw: &str) -> Result<Vec<v1::ContentBlock>, AgentHostError> {
    let content = serde_json::from_str::<Vec<v1::ContentBlock>>(raw)
        .map_err(|_| AgentHostError::InvalidNativePayload)?;
    if content.is_empty() {
        return Err(AgentHostError::InvalidNativePayload);
    }
    Ok(content)
}

fn parse_v2_prompt(raw: &str) -> Result<Vec<v2::ContentBlock>, AgentHostError> {
    let content = serde_json::from_str::<Vec<v2::ContentBlock>>(raw)
        .map_err(|_| AgentHostError::InvalidNativePayload)?;
    if content.is_empty() {
        return Err(AgentHostError::InvalidNativePayload);
    }
    Ok(content)
}

fn text_prompt_json(text: &str) -> Result<String, AgentHostError> {
    serde_json::to_string(&json!([{ "type": "text", "text": text }]))
        .map_err(|_| AgentHostError::InvalidNativePayload)
}

fn nonempty(metadata: Map<String, Value>) -> Option<Map<String, Value>> {
    (!metadata.is_empty()).then_some(metadata)
}

fn new_run_id() -> String {
    format!("run_{}", Uuid::new_v4())
}

fn request_id(id: &v1::RequestId) -> String {
    serde_json::to_value(id)
        .ok()
        .and_then(|value| {
            value
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| Some(value.to_string()))
        })
        .unwrap_or_else(|| "unknown".into())
}

fn acp_error(error: AgentHostError) -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error().data(format!("agent host error: {error:?}"))
}

#[cfg(test)]
mod signal_tests {
    use std::{
        future::{Future as _, poll_fn},
        task::Poll,
    };

    use super::{
        ACTOR_SIGNAL_QUEUE_CAPACITY, ActorSignal, MAX_NATIVE_PROMPT_BYTES, actor_signal_channel,
        validate_prompt,
    };
    use crate::{AgentHostError, AgentInputMode, PromptRequest};

    fn fill_event_queue(sender: &super::ActorSignalSender) {
        for _ in 0..ACTOR_SIGNAL_QUEUE_CAPACITY {
            assert!(sender.events.try_send(ActorSignal::V2Idle).is_ok());
        }
    }

    #[tokio::test]
    async fn event_queue_backpressures_at_its_fixed_capacity() {
        let (sender, mut receiver) = actor_signal_channel();
        fill_event_queue(&sender);
        assert!(matches!(
            sender.events.try_send(ActorSignal::V2Idle),
            Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        ));

        let mut pending = Box::pin(sender.send(ActorSignal::V2Idle));
        let first_poll = poll_fn(|context| Poll::Ready(pending.as_mut().poll(context))).await;
        assert!(first_poll.is_pending());
        assert!(matches!(receiver.recv().await, Some(ActorSignal::V2Idle)));
        pending.await.expect("event resumes after one slot opens");
        assert_eq!(receiver.events.len(), ACTOR_SIGNAL_QUEUE_CAPACITY);
    }

    #[tokio::test]
    async fn fatal_signal_bypasses_a_full_event_queue() {
        let (sender, mut receiver) = actor_signal_channel();
        fill_event_queue(&sender);

        sender.fatal();
        assert!(matches!(receiver.recv().await, Some(ActorSignal::Fatal)));
        assert_eq!(receiver.events.len(), ACTOR_SIGNAL_QUEUE_CAPACITY);
    }

    #[test]
    fn native_prompt_is_rejected_before_parsing_when_over_limit() {
        let request = PromptRequest {
            session_id: "session-1".into(),
            client_turn_id: "turn-1".into(),
            mode: AgentInputMode::Queue,
            native_prompt_json: "x".repeat(MAX_NATIVE_PROMPT_BYTES + 1),
        };

        assert_eq!(
            validate_prompt("session-1", &request),
            Err(AgentHostError::InvalidNativePayload)
        );
    }
}
