//! ACP v1 stdio facade for durable Crab-owned native channels.

use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_client_protocol::{
    Agent, Client, ConnectTo, ConnectionTo, Responder, Stdio, UntypedMessage,
    schema::{
        ProtocolVersion,
        v1::{
            AgentCapabilities, AuthMethod, AuthMethodAgent, AuthenticateRequest,
            AuthenticateResponse, CancelNotification, Implementation, InitializeRequest,
            InitializeResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest,
            NewSessionResponse, PromptRequest, PromptResponse, StopReason,
        },
    },
};
use channel_gateway_contract::AttachChannelRequest;
use native_channel_contract::{
    ChannelInputMode, ChannelTurn, InterruptRequest, NativeChannelEvent, NativeEventDirection,
    NativeEventKind, ReplayRequest,
};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, oneshot};
use uuid::Uuid;

use crate::{ChannelIpcClient, ChannelIpcClientError};

const AUTH_METHOD: &str = "crab-local";
const DEFAULT_ADAPTER: &str = "t3code";
const REPLAY_LIMIT: u64 = 256;
const IDLE_POLL: Duration = Duration::from_millis(25);
const ERROR_POLL: Duration = Duration::from_millis(100);

/// Non-secret attachment policy for one ACP stdio facade process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcpChannelOptions {
    state_directory: PathBuf,
    agent_id: String,
    adapter_id: String,
    bootstrap_prompt: Option<String>,
}

impl AcpChannelOptions {
    /// Select Crab state and the configured agent used by dynamically attached UI sessions.
    #[must_use]
    pub fn new(state_directory: impl Into<PathBuf>, agent_id: impl Into<String>) -> Self {
        Self {
            state_directory: state_directory.into(),
            agent_id: agent_id.into(),
            adapter_id: DEFAULT_ADAPTER.into(),
            bootstrap_prompt: None,
        }
    }

    /// Select the stable native-channel adapter identity; defaults to `t3code`.
    #[must_use]
    pub fn adapter_id(mut self, adapter_id: impl Into<String>) -> Self {
        self.adapter_id = adapter_id.into();
        self
    }

    /// Supply optional context injected only when Crab must open a fresh physical session.
    #[must_use]
    pub fn bootstrap_prompt(mut self, bootstrap_prompt: Option<String>) -> Self {
        self.bootstrap_prompt = bootstrap_prompt;
        self
    }
}

/// A stable startup or ACP transport failure from the stdio facade.
#[derive(Debug)]
pub enum AcpChannelError {
    /// The owner-only Crab IPC endpoint could not be opened.
    LocalIpc(ChannelIpcClientError),
    /// The ACP stdio connection failed.
    Acp(agent_client_protocol::Error),
    /// Required non-secret facade configuration was invalid.
    InvalidConfiguration,
}

impl fmt::Display for AcpChannelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LocalIpc(_) => formatter.write_str("Crab local channel IPC is unavailable"),
            Self::Acp(_) => formatter.write_str("ACP stdio transport failed"),
            Self::InvalidConfiguration => {
                formatter.write_str("ACP channel configuration is invalid")
            }
        }
    }
}

impl std::error::Error for AcpChannelError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LocalIpc(error) => Some(error),
            Self::Acp(error) => Some(error),
            Self::InvalidConfiguration => None,
        }
    }
}

/// Run one standard ACP v1 process over stdin/stdout while Crab retains session ownership.
pub async fn run_acp_channel_stdio(options: AcpChannelOptions) -> Result<(), AcpChannelError> {
    validate_options(&options)?;
    let client = ChannelIpcClient::from_state_directory(&options.state_directory)
        .map_err(AcpChannelError::LocalIpc)?;
    AcpChannelFacade::new(client, options)
        .agent()
        .connect_to(Stdio::new())
        .await
        .map_err(AcpChannelError::Acp)
}

fn validate_options(options: &AcpChannelOptions) -> Result<(), AcpChannelError> {
    if options.agent_id.trim().is_empty() || options.adapter_id.trim().is_empty() {
        return Err(AcpChannelError::InvalidConfiguration);
    }
    Ok(())
}

#[derive(Clone)]
pub(crate) struct AcpChannelFacade {
    client: ChannelIpcClient,
    options: AcpChannelOptions,
    authenticated: Arc<AtomicBool>,
    sessions: Arc<Mutex<HashMap<String, Arc<FacadeSession>>>>,
}

impl AcpChannelFacade {
    pub(crate) fn new(client: ChannelIpcClient, options: AcpChannelOptions) -> Self {
        Self {
            client,
            options,
            authenticated: Arc::new(AtomicBool::new(false)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn agent(self) -> impl ConnectTo<Client> {
        let initialize = self.clone();
        let authenticate = self.clone();
        let new_session = self.clone();
        let load_session = self.clone();
        let prompt = self.clone();
        let cancel = self;
        Agent
            .builder()
            .name("crab-v2-acp-channel")
            .on_receive_request(
                async move |request: InitializeRequest,
                            responder: Responder<InitializeResponse>,
                            _connection: ConnectionTo<Client>| {
                    initialize.handle_initialize(request, responder)
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: AuthenticateRequest,
                            responder: Responder<AuthenticateResponse>,
                            _connection: ConnectionTo<Client>| {
                    authenticate.handle_authenticate(request, responder)
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: NewSessionRequest,
                            responder: Responder<NewSessionResponse>,
                            connection: ConnectionTo<Client>| {
                    new_session
                        .handle_new_session(request, responder, connection)
                        .await
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: LoadSessionRequest,
                            responder: Responder<LoadSessionResponse>,
                            connection: ConnectionTo<Client>| {
                    load_session
                        .handle_load_session(request, responder, connection)
                        .await
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_request(
                async move |request: PromptRequest,
                            responder: Responder<PromptResponse>,
                            _connection: ConnectionTo<Client>| {
                    prompt.handle_prompt(request, responder).await
                },
                agent_client_protocol::on_receive_request!(),
            )
            .on_receive_notification(
                async move |notification: CancelNotification, _connection: ConnectionTo<Client>| {
                    cancel.handle_cancel(notification);
                    Ok(())
                },
                agent_client_protocol::on_receive_notification!(),
            )
    }

    fn handle_initialize(
        &self,
        request: InitializeRequest,
        responder: Responder<InitializeResponse>,
    ) -> Result<(), agent_client_protocol::Error> {
        if request.protocol_version != ProtocolVersion::V1 {
            return responder.respond_with_error(agent_client_protocol::Error::invalid_params());
        }
        responder.respond(
            InitializeResponse::new(ProtocolVersion::V1)
                .agent_capabilities(AgentCapabilities::new().load_session(true))
                .auth_methods(vec![AuthMethod::Agent(
                    AuthMethodAgent::new(AUTH_METHOD, "Crab local runtime")
                        .description("Owner-only Unix socket authentication"),
                )])
                .agent_info(Implementation::new(
                    "crab-v2-acp-channel",
                    env!("CARGO_PKG_VERSION"),
                )),
        )
    }

    fn handle_authenticate(
        &self,
        request: AuthenticateRequest,
        responder: Responder<AuthenticateResponse>,
    ) -> Result<(), agent_client_protocol::Error> {
        if request.method_id.to_string() != AUTH_METHOD {
            return responder.respond_with_error(agent_client_protocol::Error::auth_required());
        }
        self.authenticated.store(true, Ordering::Release);
        responder.respond(AuthenticateResponse::new())
    }

    async fn handle_new_session(
        &self,
        request: NewSessionRequest,
        responder: Responder<NewSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> Result<(), agent_client_protocol::Error> {
        if !self.is_authenticated()
            || !supported_workspace(
                &request.cwd,
                &request.additional_directories,
                request.mcp_servers.len(),
            )
        {
            return responder.respond_with_error(agent_client_protocol::Error::invalid_params());
        }
        let facade_session_id = format!("crab-{}", Uuid::new_v4().simple());
        match self
            .attach_session(&facade_session_id, &request.cwd, &connection, false)
            .await
        {
            Ok(()) => responder.respond(NewSessionResponse::new(facade_session_id)),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn handle_load_session(
        &self,
        request: LoadSessionRequest,
        responder: Responder<LoadSessionResponse>,
        connection: ConnectionTo<Client>,
    ) -> Result<(), agent_client_protocol::Error> {
        if !self.is_authenticated()
            || !supported_workspace(
                &request.cwd,
                &request.additional_directories,
                request.mcp_servers.len(),
            )
        {
            return responder.respond_with_error(agent_client_protocol::Error::invalid_params());
        }
        let facade_session_id = request.session_id.to_string();
        if facade_session_id.trim().is_empty() {
            return responder.respond_with_error(agent_client_protocol::Error::invalid_params());
        }
        match self
            .attach_session(&facade_session_id, &request.cwd, &connection, true)
            .await
        {
            Ok(()) => responder.respond(LoadSessionResponse::new()),
            Err(error) => responder.respond_with_error(error),
        }
    }

    async fn attach_session(
        &self,
        facade_session_id: &str,
        working_directory: &Path,
        connection: &ConnectionTo<Client>,
        replay_existing: bool,
    ) -> Result<(), agent_client_protocol::Error> {
        let working_directory = working_directory
            .to_str()
            .filter(|_| working_directory.is_absolute())
            .ok_or_else(agent_client_protocol::Error::invalid_params)?;
        if let Some(existing) = self.sessions.lock().await.get(facade_session_id).cloned() {
            if existing.working_directory != working_directory {
                return Err(agent_client_protocol::Error::invalid_params());
            }
            return Ok(());
        }
        let attachment = self
            .client
            .attach_channel(AttachChannelRequest {
                channel_id: facade_session_id.to_owned(),
                adapter_id: self.options.adapter_id.clone(),
                agent_id: self.options.agent_id.clone(),
                working_directory: working_directory.to_owned(),
                bootstrap_prompt: self.options.bootstrap_prompt.clone(),
                session_metadata_json: json!({
                    "facade": "crab-v2-acp-channel",
                    "protocolVersion": 1,
                })
                .to_string(),
                native_channel_json: json!({
                    "facade": "crab-v2-acp-channel",
                    "facadeSessionId": facade_session_id,
                })
                .to_string(),
            })
            .await
            .map_err(|_| stable_internal_error())?;
        let session = Arc::new(FacadeSession::new(
            facade_session_id,
            working_directory,
            attachment.binding_id,
            attachment.session_id,
        ));
        if replay_existing {
            while replay_available(&self.client, &session, connection)
                .await
                .map_err(|_| stable_internal_error())?
            {}
        }
        self.sessions
            .lock()
            .await
            .insert(facade_session_id.to_owned(), session.clone());
        spawn_event_pump(self.client.clone(), session, connection.clone());
        Ok(())
    }

    async fn handle_prompt(
        &self,
        request: PromptRequest,
        responder: Responder<PromptResponse>,
    ) -> Result<(), agent_client_protocol::Error> {
        if !self.is_authenticated() {
            return responder.respond_with_error(agent_client_protocol::Error::auth_required());
        }
        let session_id = request.session_id.to_string();
        let Some(session) = self.sessions.lock().await.get(&session_id).cloned() else {
            return responder.respond_with_error(agent_client_protocol::Error::invalid_params());
        };
        let mode = match input_mode(request.meta.as_ref()) {
            Ok(mode) => mode,
            Err(error) => return responder.respond_with_error(error),
        };
        let turn_id = match turn_id(request.meta.as_ref()) {
            Ok(turn_id) => turn_id,
            Err(error) => return responder.respond_with_error(error),
        };
        let native_prompt_json = match serde_json::to_string(&request.prompt) {
            Ok(prompt) => prompt,
            Err(_) => return responder.respond_with_error(stable_internal_error()),
        };
        let accepted = self
            .client
            .accept_turn(ChannelTurn {
                binding_id: session.binding_id.clone(),
                client_turn_id: turn_id,
                received_at_ms: match now_ms() {
                    Ok(now) => now,
                    Err(error) => return responder.respond_with_error(error),
                },
                mode,
                native_prompt_json,
            })
            .await;
        let run_id = match accepted {
            Ok(accepted) => accepted.run_id,
            Err(_) => return responder.respond_with_error(stable_internal_error()),
        };
        tokio::spawn(async move {
            let response = match session.completions.wait(run_id).await {
                RunCompletion::Portable(reason) => responder.respond(PromptResponse::new(reason)),
                RunCompletion::Unavailable => responder.respond_with_error(stable_internal_error()),
            };
            let _ = response;
        });
        Ok(())
    }

    fn handle_cancel(&self, notification: CancelNotification) {
        if !self.is_authenticated() {
            return;
        }
        let sessions = self.sessions.clone();
        let client = self.client.clone();
        tokio::spawn(async move {
            let session_id = notification.session_id.to_string();
            let Some(session) = sessions.lock().await.get(&session_id).cloned() else {
                return;
            };
            let Ok(requested_at_ms) = now_ms() else {
                return;
            };
            let _ = client
                .interrupt_and_drain(InterruptRequest {
                    binding_id: session.binding_id.clone(),
                    expected_session_id: session.physical_session_id.clone(),
                    requested_at_ms,
                    reason: "ACP client requested session cancellation".into(),
                })
                .await;
        });
    }

    fn is_authenticated(&self) -> bool {
        self.authenticated.load(Ordering::Acquire)
    }
}

struct FacadeSession {
    facade_session_id: String,
    working_directory: String,
    binding_id: String,
    physical_session_id: String,
    cursor: AtomicU64,
    completions: RunCompletions,
}

impl FacadeSession {
    fn new(
        facade_session_id: &str,
        working_directory: &str,
        binding_id: String,
        physical_session_id: String,
    ) -> Self {
        Self {
            facade_session_id: facade_session_id.to_owned(),
            working_directory: working_directory.to_owned(),
            binding_id,
            physical_session_id,
            cursor: AtomicU64::new(0),
            completions: RunCompletions::default(),
        }
    }
}

#[derive(Clone, Copy)]
enum RunCompletion {
    Portable(StopReason),
    Unavailable,
}

#[derive(Default)]
struct RunCompletionState {
    finished: HashMap<String, RunCompletion>,
    waiters: HashMap<String, Vec<oneshot::Sender<RunCompletion>>>,
}

#[derive(Default)]
struct RunCompletions(Mutex<RunCompletionState>);

impl RunCompletions {
    async fn finish(&self, run_id: String, completion: RunCompletion) {
        let waiters = {
            let mut state = self.0.lock().await;
            state.finished.insert(run_id.clone(), completion);
            state.waiters.remove(&run_id).unwrap_or_default()
        };
        for waiter in waiters {
            let _ = waiter.send(completion);
        }
    }

    async fn wait(&self, run_id: String) -> RunCompletion {
        let receiver = {
            let mut state = self.0.lock().await;
            if let Some(completion) = state.finished.get(&run_id).copied() {
                return completion;
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.entry(run_id).or_default().push(sender);
            receiver
        };
        receiver.await.unwrap_or(RunCompletion::Unavailable)
    }
}

fn spawn_event_pump(
    client: ChannelIpcClient,
    session: Arc<FacadeSession>,
    connection: ConnectionTo<Client>,
) {
    tokio::spawn(async move {
        loop {
            let replay = replay_available(&client, &session, &connection).await;
            let delay = match replay {
                Ok(true) => Duration::ZERO,
                Ok(false) => IDLE_POLL,
                Err(()) => ERROR_POLL,
            };
            tokio::select! {
                () = connection.incoming_closed() => return,
                () = tokio::time::sleep(delay) => {}
            }
        }
    });
}

async fn replay_available(
    client: &ChannelIpcClient,
    session: &FacadeSession,
    connection: &ConnectionTo<Client>,
) -> Result<bool, ()> {
    let cursor = session.cursor.load(Ordering::Acquire);
    let page = client
        .replay_native_events(ReplayRequest {
            binding_id: session.binding_id.clone(),
            after_sequence: cursor,
            limit: REPLAY_LIMIT,
        })
        .await
        .map_err(|_| ())?;
    for event in &page.events {
        process_event(session, connection, event).await?;
    }
    session.cursor.store(page.next_sequence, Ordering::Release);
    Ok(!page.caught_up)
}

async fn process_event(
    session: &FacadeSession,
    connection: &ConnectionTo<Client>,
    event: &NativeChannelEvent,
) -> Result<(), ()> {
    let Ok(mut native) = serde_json::from_str::<Value>(&event.native_event_json) else {
        return Ok(());
    };
    if matches!(event.direction, Some(NativeEventDirection::AgentToClient))
        && native.get("method").and_then(Value::as_str) == Some("session/update")
    {
        let params = native
            .get_mut("params")
            .and_then(Value::as_object_mut)
            .ok_or(())?;
        params.insert(
            "sessionId".into(),
            Value::String(session.facade_session_id.clone()),
        );
        connection
            .send_notification(UntypedMessage::new("session/update", params).map_err(|_| ())?)
            .map_err(|_| ())?;
    }
    if let Some(run_id) = event.run_id.clone() {
        let completion = if matches!(event.kind, NativeEventKind::RunFinished) {
            Some(
                stop_reason(&native)
                    .map(RunCompletion::Portable)
                    .unwrap_or(RunCompletion::Unavailable),
            )
        } else if matches!(event.direction, Some(NativeEventDirection::AgentToClient))
            && native.get("error").is_some()
        {
            Some(RunCompletion::Unavailable)
        } else {
            None
        };
        if let Some(completion) = completion {
            session.completions.finish(run_id, completion).await;
        }
    }
    Ok(())
}

fn stop_reason(message: &Value) -> Option<StopReason> {
    message
        .pointer("/result/stopReason")
        .or_else(|| message.pointer("/params/update/stopReason"))
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
}

fn supported_workspace(cwd: &Path, additional: &[PathBuf], mcp_server_count: usize) -> bool {
    cwd.is_absolute() && additional.is_empty() && mcp_server_count == 0
}

fn input_mode(
    meta: Option<&Map<String, Value>>,
) -> Result<ChannelInputMode, agent_client_protocol::Error> {
    match crab_meta(meta)
        .and_then(|crab| crab.get("inputMode"))
        .and_then(Value::as_str)
    {
        None | Some("queue") => Ok(ChannelInputMode::Queue),
        Some("steer") => Ok(ChannelInputMode::Steer),
        Some(_) => Err(agent_client_protocol::Error::invalid_params()),
    }
}

fn turn_id(meta: Option<&Map<String, Value>>) -> Result<String, agent_client_protocol::Error> {
    match crab_meta(meta).and_then(|crab| crab.get("turnId")) {
        None => Ok(format!("acp-{}", Uuid::new_v4().simple())),
        Some(Value::String(value)) if !value.trim().is_empty() && value.len() <= 256 => {
            Ok(value.clone())
        }
        Some(_) => Err(agent_client_protocol::Error::invalid_params()),
    }
}

fn crab_meta(meta: Option<&Map<String, Value>>) -> Option<&Map<String, Value>> {
    meta?.get("crab")?.as_object()
}

fn now_ms() -> Result<u64, agent_client_protocol::Error> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| stable_internal_error())?
        .as_millis();
    u64::try_from(millis).map_err(|_| stable_internal_error())
}

fn stable_internal_error() -> agent_client_protocol::Error {
    agent_client_protocol::Error::internal_error()
}

#[cfg(test)]
mod tests {
    use agent_client_protocol::schema::v1::StopReason;
    use serde_json::json;

    use super::{input_mode, stop_reason, turn_id};
    use native_channel_contract::ChannelInputMode;

    #[test]
    fn crab_prompt_metadata_is_explicit_and_strict() {
        let queue = json!({"crab": {"inputMode": "queue", "turnId": "turn-1"}});
        let queue = queue.as_object().expect("metadata object");
        assert!(matches!(
            input_mode(Some(queue)),
            Ok(ChannelInputMode::Queue)
        ));
        assert_eq!(turn_id(Some(queue)).expect("turn id"), "turn-1");

        let steer = json!({"crab": {"inputMode": "steer"}});
        assert!(matches!(
            input_mode(steer.as_object()),
            Ok(ChannelInputMode::Steer)
        ));
        let invalid = json!({"crab": {"inputMode": "interrupt"}});
        assert!(input_mode(invalid.as_object()).is_err());
    }

    #[test]
    fn portable_stop_reasons_are_preserved() {
        assert_eq!(
            stop_reason(&json!({"result": {"stopReason": "max_tokens"}})),
            Some(StopReason::MaxTokens)
        );
        assert_eq!(
            stop_reason(&json!({"params": {"update": {"stopReason": "cancelled"}}})),
            Some(StopReason::Cancelled)
        );
        assert_eq!(stop_reason(&json!({"result": {}})), None);
    }
}
