//! ACP v1 stdio facade for durable Crab-owned native channels.

use std::{
    collections::{HashMap, VecDeque},
    fmt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex, PoisonError, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use agent_client_protocol::{
    Agent, Client, ConnectTo, ConnectionTo, Responder, UntypedMessage,
    schema::{
        ProtocolVersion,
        v1::{
            AgentCapabilities, AuthMethod, AuthMethodAgent, AuthenticateRequest,
            AuthenticateResponse, CancelNotification, Implementation, InitializeRequest,
            InitializeResponse, LoadSessionRequest, LoadSessionResponse, NewSessionRequest,
            NewSessionResponse, PromptRequest, PromptResponse, SessionNotification, StopReason,
        },
        v2::UpdateSessionNotification,
    },
};
use channel_gateway_contract::AttachChannelRequest;
use native_channel_contract::{
    ChannelInputMode, ChannelTurn, InterruptRequest, NativeChannelEvent, NativeEventDirection,
    NativeEventKind, ReplayRequest,
};
use serde_json::{Map, Value, json};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore, oneshot};
use uuid::Uuid;

use crate::{ChannelIpcClient, ChannelIpcClientError, native_stdio};

const AUTH_METHOD: &str = "crab-local";
const DEFAULT_ADAPTER: &str = "t3code";
const REPLAY_LIMIT: u64 = 256;
const EARLY_COMPLETION_CAPACITY: usize = 256;
const MAX_FACADE_SESSIONS: usize = 128;
const MAX_FACADE_SESSION_ID_BYTES: usize = 256;
const MAX_OUTSTANDING_PROMPTS: usize = 128;
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
            Self::LocalIpc(_) => formatter.write_str("Crab local IPC is unavailable"),
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
        .connect_to(native_stdio())
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
    session_attach_locks: Arc<SessionAttachLocks>,
    session_slots: Arc<Semaphore>,
}

impl AcpChannelFacade {
    pub(crate) fn new(client: ChannelIpcClient, options: AcpChannelOptions) -> Self {
        Self {
            client,
            options,
            authenticated: Arc::new(AtomicBool::new(false)),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            session_attach_locks: Arc::new(SessionAttachLocks::default()),
            session_slots: Arc::new(Semaphore::new(MAX_FACADE_SESSIONS)),
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
                            connection: ConnectionTo<Client>| {
                    prompt.handle_prompt(request, responder, connection).await
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
        if !valid_facade_session_id(&facade_session_id) {
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
        if !valid_facade_session_id(facade_session_id) {
            return Err(agent_client_protocol::Error::invalid_params());
        }
        let working_directory = working_directory
            .to_str()
            .filter(|_| working_directory.is_absolute())
            .ok_or_else(agent_client_protocol::Error::invalid_params)?;
        let _attach = self.session_attach_locks.lock(facade_session_id).await;
        if let Some(existing) = self.sessions.lock().await.get(facade_session_id).cloned() {
            if existing.working_directory != working_directory {
                return Err(agent_client_protocol::Error::invalid_params());
            }
            return Ok(());
        }
        let session_slot = self
            .session_slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| stable_internal_error())?;
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
            session_slot,
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
        connection: ConnectionTo<Client>,
    ) -> Result<(), agent_client_protocol::Error> {
        if !self.is_authenticated() {
            return responder.respond_with_error(agent_client_protocol::Error::auth_required());
        }
        let session_id = request.session_id.to_string();
        let Some(session) = self.sessions.lock().await.get(&session_id).cloned() else {
            return responder.respond_with_error(agent_client_protocol::Error::invalid_params());
        };
        let Some(prompt_slot) = session.try_prompt_slot() else {
            return responder.respond_with_error(stable_internal_error());
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
            let _prompt_slot = prompt_slot;
            tokio::select! {
                completion = session.completions.wait(run_id) => {
                    let response = match completion {
                        RunCompletion::Portable(reason) => {
                            responder.respond(PromptResponse::new(reason))
                        }
                        RunCompletion::Unavailable => {
                            responder.respond_with_error(stable_internal_error())
                        }
                    };
                    let _ = response;
                }
                () = connection.incoming_closed() => {}
            }
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

#[derive(Default)]
struct SessionAttachLocks {
    locks: StdMutex<HashMap<String, Weak<Mutex<()>>>>,
}

impl SessionAttachLocks {
    async fn lock(&self, facade_session_id: &str) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(facade_session_id).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(facade_session_id.to_owned(), Arc::downgrade(&lock));
                lock
            }
        };
        lock.lock_owned().await
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.locks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }
}

struct FacadeSession {
    facade_session_id: String,
    working_directory: String,
    binding_id: String,
    physical_session_id: String,
    cursor: AtomicU64,
    completions: RunCompletions,
    prompt_slots: Arc<Semaphore>,
    _session_slot: OwnedSemaphorePermit,
}

impl FacadeSession {
    fn new(
        facade_session_id: &str,
        working_directory: &str,
        binding_id: String,
        physical_session_id: String,
        session_slot: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            facade_session_id: facade_session_id.to_owned(),
            working_directory: working_directory.to_owned(),
            binding_id,
            physical_session_id,
            cursor: AtomicU64::new(0),
            completions: RunCompletions::default(),
            prompt_slots: Arc::new(Semaphore::new(MAX_OUTSTANDING_PROMPTS)),
            _session_slot: session_slot,
        }
    }

    fn try_prompt_slot(&self) -> Option<OwnedSemaphorePermit> {
        self.prompt_slots.clone().try_acquire_owned().ok()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RunCompletion {
    Portable(StopReason),
    Unavailable,
}

#[derive(Default)]
struct RunCompletionState {
    early: HashMap<String, RunCompletion>,
    early_order: VecDeque<String>,
    waiters: HashMap<String, Vec<oneshot::Sender<RunCompletion>>>,
}

impl RunCompletionState {
    fn remember_early(&mut self, run_id: String, completion: RunCompletion) {
        if self.early.insert(run_id.clone(), completion).is_some() {
            self.early_order.retain(|remembered| remembered != &run_id);
        }
        self.early_order.push_back(run_id);
        while self.early_order.len() > EARLY_COMPLETION_CAPACITY {
            if let Some(expired) = self.early_order.pop_front() {
                self.early.remove(&expired);
            }
        }
    }

    fn take_early(&mut self, run_id: &str) -> Option<RunCompletion> {
        let completion = self.early.remove(run_id)?;
        self.early_order.retain(|remembered| remembered != run_id);
        Some(completion)
    }
}

#[derive(Default)]
struct RunCompletions(Mutex<RunCompletionState>);

impl RunCompletions {
    async fn finish(&self, run_id: String, completion: RunCompletion) {
        let waiters = {
            let mut state = self.0.lock().await;
            match state.waiters.remove(&run_id) {
                Some(waiters) => waiters,
                None => {
                    state.remember_early(run_id, completion);
                    return;
                }
            }
        };
        for waiter in waiters {
            let _ = waiter.send(completion);
        }
    }

    async fn wait(&self, run_id: String) -> RunCompletion {
        let receiver = {
            let mut state = self.0.lock().await;
            if let Some(completion) = state.take_early(&run_id) {
                return completion;
            }
            let (sender, receiver) = oneshot::channel();
            state.waiters.entry(run_id).or_default().push(sender);
            receiver
        };
        receiver.await.unwrap_or(RunCompletion::Unavailable)
    }

    #[cfg(test)]
    async fn retained_counts(&self) -> (usize, usize) {
        let state = self.0.lock().await;
        (
            state.early.len(),
            state.waiters.values().map(Vec::len).sum(),
        )
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
    let Ok(native) = serde_json::from_str::<Value>(&event.native_event_json) else {
        return Ok(());
    };
    if matches!(event.direction, Some(NativeEventDirection::AgentToClient))
        && native.get("method").and_then(Value::as_str) == Some("session/update")
        && let Some(params) = native.get("params").and_then(Value::as_object)
    {
        for notification in project_session_update_to_v1(params, &session.facade_session_id) {
            let Value::Object(params) = serde_json::to_value(notification).map_err(|_| ())? else {
                return Err(());
            };
            connection
                .send_notification(UntypedMessage::new("session/update", &params).map_err(|_| ())?)
                .map_err(|_| ())?;
        }
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

fn project_session_update_to_v1(
    params: &Map<String, Value>,
    facade_session_id: &str,
) -> Vec<SessionNotification> {
    let mut params = params.clone();
    params.insert(
        "sessionId".into(),
        Value::String(facade_session_id.to_owned()),
    );
    let value = Value::Object(params);

    if let Ok(notification) = serde_json::from_value::<SessionNotification>(value.clone()) {
        return vec![notification];
    }

    let Ok(notification) = serde_json::from_value::<UpdateSessionNotification>(value) else {
        return Vec::new();
    };
    Vec::<SessionNotification>::try_from(notification).unwrap_or_default()
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

fn valid_facade_session_id(facade_session_id: &str) -> bool {
    !facade_session_id.trim().is_empty() && facade_session_id.len() <= MAX_FACADE_SESSION_ID_BYTES
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
    use std::{
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };

    use agent_client_protocol::schema::v1::{SessionNotification, SessionUpdate, StopReason};
    use serde_json::json;

    use super::{
        EARLY_COMPLETION_CAPACITY, FacadeSession, MAX_FACADE_SESSION_ID_BYTES, MAX_FACADE_SESSIONS,
        MAX_OUTSTANDING_PROMPTS, RunCompletion, RunCompletions, SessionAttachLocks, input_mode,
        project_session_update_to_v1, stop_reason, supported_workspace, turn_id,
        valid_facade_session_id,
    };
    use native_channel_contract::ChannelInputMode;
    use tokio::{sync::Semaphore, time::timeout};

    fn facade_session() -> (Arc<Semaphore>, FacadeSession) {
        let session_slots = Arc::new(Semaphore::new(1));
        let session_slot = session_slots
            .clone()
            .try_acquire_owned()
            .expect("facade session slot");
        let session = FacadeSession::new(
            "facade-session",
            "/tmp/workspace",
            "binding-1".into(),
            "physical-1".into(),
            session_slot,
        );
        (session_slots, session)
    }

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

    #[test]
    fn facade_keeps_workspace_and_tool_authority_inside_crab() {
        let workspace = Path::new("/tmp/crab-workspace");
        assert!(supported_workspace(workspace, &[], 0));
        assert!(!supported_workspace(workspace, &[], 1));
        assert!(!supported_workspace(
            workspace,
            &[PathBuf::from("/tmp/another-workspace")],
            0,
        ));
        assert!(!supported_workspace(Path::new("relative"), &[], 0));
    }

    #[test]
    fn facade_session_ids_are_bounded() {
        assert!(valid_facade_session_id("session-1"));
        assert!(valid_facade_session_id(
            &"a".repeat(MAX_FACADE_SESSION_ID_BYTES)
        ));
        assert!(!valid_facade_session_id("   "));
        assert!(!valid_facade_session_id(
            &"a".repeat(MAX_FACADE_SESSION_ID_BYTES + 1)
        ));
    }

    #[tokio::test]
    async fn facade_attach_locks_serialize_only_matching_session_ids_and_prune() {
        let locks = SessionAttachLocks::default();
        let first = locks.lock("same").await;

        let unrelated = timeout(Duration::from_millis(100), locks.lock("other"))
            .await
            .expect("unrelated session does not wait");
        assert!(
            timeout(Duration::from_millis(20), locks.lock("same"))
                .await
                .is_err()
        );

        drop(first);
        let matching = timeout(Duration::from_millis(100), locks.lock("same"))
            .await
            .expect("matching session proceeds after release");
        drop(matching);
        drop(unrelated);

        let fresh = locks.lock("fresh").await;
        assert_eq!(locks.len(), 1);
        drop(fresh);
    }

    #[test]
    fn v2_only_lifecycle_updates_do_not_escape_the_v1_facade() {
        let state = json!({
            "sessionId": "physical-session",
            "update": {
                "sessionUpdate": "state_update",
                "state": "running"
            }
        });
        let message = json!({
            "sessionId": "physical-session",
            "update": {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "message-1",
                "content": {"type": "text", "text": "still connected"}
            }
        });

        let projected = [state, message]
            .iter()
            .flat_map(|params| {
                project_session_update_to_v1(
                    params.as_object().expect("notification params"),
                    "facade-session",
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].session_id.to_string(), "facade-session");
        assert!(matches!(
            projected[0].update,
            SessionUpdate::AgentMessageChunk(_)
        ));
        let encoded = serde_json::to_value(&projected[0]).expect("v1 notification JSON");
        serde_json::from_value::<SessionNotification>(encoded)
            .expect("strict v1 client accepts projected update");
    }

    #[tokio::test]
    async fn completion_state_is_consumed_in_both_race_orders() {
        let completions = Arc::new(RunCompletions::default());
        completions
            .finish("early".into(), RunCompletion::Portable(StopReason::EndTurn))
            .await;
        assert_eq!(completions.retained_counts().await, (1, 0));
        assert_eq!(
            completions.wait("early".into()).await,
            RunCompletion::Portable(StopReason::EndTurn)
        );
        assert_eq!(completions.retained_counts().await, (0, 0));

        let waiting = completions.clone();
        let waiter = tokio::spawn(async move { waiting.wait("waiting".into()).await });
        while completions.retained_counts().await != (0, 1) {
            tokio::task::yield_now().await;
        }
        completions
            .finish("waiting".into(), RunCompletion::Unavailable)
            .await;
        assert_eq!(
            waiter.await.expect("waiter joins"),
            RunCompletion::Unavailable
        );
        assert_eq!(completions.retained_counts().await, (0, 0));
    }

    #[tokio::test]
    async fn unrelated_run_completions_use_a_bounded_early_cache() {
        let completions = RunCompletions::default();
        for sequence in 0..(EARLY_COMPLETION_CAPACITY * 4) {
            completions
                .finish(format!("external-{sequence}"), RunCompletion::Unavailable)
                .await;
        }
        assert_eq!(
            completions.retained_counts().await,
            (EARLY_COMPLETION_CAPACITY, 0)
        );
    }

    #[test]
    fn facade_session_caps_outstanding_prompt_responders() {
        let (_, session) = facade_session();
        let slots = (0..MAX_OUTSTANDING_PROMPTS)
            .map(|_| session.try_prompt_slot().expect("slot remains"))
            .collect::<Vec<_>>();
        assert!(session.try_prompt_slot().is_none());
        drop(slots);
        assert!(session.try_prompt_slot().is_some());
    }

    #[test]
    fn facade_session_admission_is_hard_capped_and_released() {
        let session_slots = Arc::new(Semaphore::new(MAX_FACADE_SESSIONS));
        let mut sessions = (0..MAX_FACADE_SESSIONS)
            .map(|sequence| {
                let session_slot = session_slots
                    .clone()
                    .try_acquire_owned()
                    .expect("slot remains within cap");
                FacadeSession::new(
                    &format!("facade-{sequence}"),
                    "/tmp/workspace",
                    format!("binding-{sequence}"),
                    format!("physical-{sequence}"),
                    session_slot,
                )
            })
            .collect::<Vec<_>>();
        assert!(session_slots.clone().try_acquire_owned().is_err());

        sessions.pop();
        assert!(session_slots.clone().try_acquire_owned().is_ok());
    }
}
