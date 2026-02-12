use std::collections::{BTreeMap, VecDeque};
#[cfg(not(any(test, coverage)))]
use std::io::{BufRead, BufReader, Write};
#[cfg(not(any(test, coverage)))]
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::{Arc, Mutex};

use crab_backends::{
    decide_unattended_response, map_codex_turn_config, normalize_codex_events,
    recover_codex_session, BackendEvent, CodexInteractiveRequest, CodexLifecycleManager,
    CodexProtocol, CodexRawEvent, CodexRequest, CodexRequestResponse, CodexRpcRequest,
    CodexRpcResponse, CodexRpcTransport, CodexTurnStatus, CodexUnattendedPolicy,
    CodexUserInputQuestion,
};
use crab_core::{BackendKind, CrabError, CrabResult, PhysicalSession, Run};
#[cfg(not(any(test, coverage)))]
use serde_json::{Map, Value};

pub trait DaemonBackendBridge: Send {
    fn execute_backend_turn(
        &mut self,
        codex_lifecycle: &mut dyn CodexLifecycleManager,
        physical_session: &mut PhysicalSession,
        run: &Run,
        turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>>;
}

pub trait CodexDaemonTransport: CodexRpcTransport {
    fn collect_turn_events(&self, thread_id: &str, turn_id: &str)
        -> CrabResult<Vec<CodexRawEvent>>;

    fn submit_unattended_response(&self, response: CodexRequestResponse) -> CrabResult<()>;
}

#[derive(Debug, Clone)]
pub struct CodexDaemonBackendBridge<T: CodexDaemonTransport + Clone> {
    protocol: CodexProtocol<T>,
    transport: T,
    unattended_policy: CodexUnattendedPolicy,
}

impl<T: CodexDaemonTransport + Clone> CodexDaemonBackendBridge<T> {
    #[must_use]
    pub fn new(transport: T) -> Self {
        Self {
            protocol: CodexProtocol::new(transport.clone()),
            transport,
            unattended_policy: CodexUnattendedPolicy::default(),
        }
    }

    fn handle_unattended_requests(&self, raw_events: &[CodexRawEvent]) -> CrabResult<()> {
        for event in raw_events {
            if let CodexRawEvent::Request(request) = event {
                let response = decide_unattended_response(
                    &self.unattended_policy,
                    &interactive_request_from_raw(request),
                )?;
                self.transport.submit_unattended_response(response)?;
            }
        }
        Ok(())
    }
}

impl<T: CodexDaemonTransport + Clone + Send> DaemonBackendBridge for CodexDaemonBackendBridge<T> {
    fn execute_backend_turn(
        &mut self,
        codex_lifecycle: &mut dyn CodexLifecycleManager,
        physical_session: &mut PhysicalSession,
        run: &Run,
        _turn_id: &str,
        turn_context: &str,
    ) -> CrabResult<Vec<BackendEvent>> {
        if run.profile.resolved_profile.backend != BackendKind::Codex {
            return Err(CrabError::InvariantViolation {
                context: "daemon_codex_backend_bridge",
                message: format!(
                    "unsupported backend {:?}; only Codex is wired",
                    run.profile.resolved_profile.backend
                ),
            });
        }

        let previous_thread_id = non_empty_value(physical_session.backend_session_id.as_str());
        let recovery = recover_codex_session(codex_lifecycle, &self.protocol, previous_thread_id)?;
        let thread_id = match recovery {
            crab_backends::CodexRecoveryOutcome::Resumed { thread_id }
            | crab_backends::CodexRecoveryOutcome::Rotated { thread_id, .. } => thread_id,
        };
        physical_session.backend_session_id = thread_id.clone();

        let turn_config = map_codex_turn_config(&run.profile.resolved_profile);
        let backend_turn_id =
            self.protocol
                .turn_start(&thread_id, &[turn_context.to_string()], turn_config)?;
        physical_session.last_turn_id = Some(backend_turn_id.clone());

        let raw_events = self
            .transport
            .collect_turn_events(&thread_id, &backend_turn_id)?;
        self.handle_unattended_requests(&raw_events)?;
        normalize_codex_events(raw_events)
    }
}

#[cfg(not(any(test, coverage)))]
const CODEX_APP_SERVER_CONTEXT: &str = "codex_app_server_transport";
#[cfg(not(any(test, coverage)))]
const CODEX_APP_SERVER_CALL_CONTEXT: &str = "codex_app_server_transport_call";
#[cfg(not(any(test, coverage)))]
const CODEX_APP_SERVER_COLLECT_CONTEXT: &str = "codex_app_server_transport_collect";

#[cfg(not(any(test, coverage)))]
#[derive(Clone)]
pub struct CodexAppServerTransport {
    state: Arc<Mutex<CodexAppServerTransportState>>,
}

#[cfg(not(any(test, coverage)))]
impl std::fmt::Debug for CodexAppServerTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CodexAppServerTransport")
            .finish_non_exhaustive()
    }
}

#[cfg(not(any(test, coverage)))]
impl CodexAppServerTransport {
    pub fn new() -> CrabResult<Self> {
        Ok(Self {
            state: Arc::new(Mutex::new(CodexAppServerTransportState::spawn()?)),
        })
    }
}

#[cfg(not(any(test, coverage)))]
impl Drop for CodexAppServerTransport {
    fn drop(&mut self) {
        if Arc::strong_count(&self.state) != 1 {
            return;
        }
        if let Ok(mut state) = self.state.lock() {
            let _ = state.child.kill();
            let _ = state.child.wait();
        }
    }
}

#[cfg(not(any(test, coverage)))]
impl CodexRpcTransport for CodexAppServerTransport {
    fn call(&self, request: CodexRpcRequest) -> CrabResult<CodexRpcResponse> {
        let mut state = self.state.lock().expect("lock should succeed");
        state.ensure_alive()?;
        state.ensure_initialized()?;

        let request_id = state.next_request_id()?;
        let request_id_key = json_rpc_id_key(&request_id)?;
        let payload = build_rpc_request_payload(&request_id, &request)?;
        state.write_message(&payload)?;

        loop {
            let message = state.read_message()?;
            match message {
                AppServerMessage::Response { id, result, error } => {
                    let id_key = json_rpc_id_key(&id)?;
                    if id_key == request_id_key {
                        if let Some(error_payload) = error {
                            return Err(CrabError::InvariantViolation {
                                context: CODEX_APP_SERVER_CALL_CONTEXT,
                                message: format!(
                                    "request {} failed with error payload {}",
                                    request.method, error_payload
                                ),
                            });
                        }
                        let result_value = result.ok_or_else(|| CrabError::InvariantViolation {
                            context: CODEX_APP_SERVER_CALL_CONTEXT,
                            message: format!(
                                "request {} returned neither result nor error",
                                request.method
                            ),
                        })?;
                        return map_rpc_result_to_protocol_response(&request.method, result_value);
                    }
                    state
                        .pending_responses
                        .insert(id_key, response_from_result_or_error(result, error));
                }
                AppServerMessage::ServerRequest { id, method, params } => {
                    if let Some(buffered) = decode_server_request(id, &method, params)? {
                        state.pending_events.push_back(buffered);
                    }
                }
                AppServerMessage::Notification { method, params } => {
                    if let Some(buffered) = decode_server_notification(&method, params)? {
                        state.pending_events.push_back(buffered);
                    }
                }
            }
        }
    }
}

#[cfg(not(any(test, coverage)))]
impl CodexDaemonTransport for CodexAppServerTransport {
    fn collect_turn_events(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> CrabResult<Vec<CodexRawEvent>> {
        let mut state = self.state.lock().expect("lock should succeed");
        state.ensure_alive()?;
        state.ensure_initialized()?;

        let mut collected = Vec::new();
        let mut completed = state.drain_matching_events(thread_id, turn_id, &mut collected);

        while !completed {
            let message = state.read_message()?;
            match message {
                AppServerMessage::Response { id, result, error } => {
                    let id_key = json_rpc_id_key(&id)?;
                    state
                        .pending_responses
                        .insert(id_key, response_from_result_or_error(result, error));
                }
                AppServerMessage::ServerRequest { id, method, params } => {
                    if let Some(buffered) = decode_server_request(id, &method, params)? {
                        if buffered.matches_turn(thread_id, turn_id) {
                            completed |= buffered.terminal;
                            collected.push(buffered.raw_event);
                        } else {
                            state.pending_events.push_back(buffered);
                        }
                    }
                }
                AppServerMessage::Notification { method, params } => {
                    if let Some(buffered) = decode_server_notification(&method, params)? {
                        if buffered.matches_turn(thread_id, turn_id) {
                            completed |= buffered.terminal;
                            collected.push(buffered.raw_event);
                        } else {
                            state.pending_events.push_back(buffered);
                        }
                    }
                }
            }
        }

        Ok(collected)
    }

    fn submit_unattended_response(&self, response: CodexRequestResponse) -> CrabResult<()> {
        let mut state = self.state.lock().expect("lock should succeed");
        state.ensure_alive()?;
        state.ensure_initialized()?;

        let (request_id, response_payload) = unattended_response_payload(response)?;
        let response_id = state
            .request_ids
            .remove(&request_id)
            .unwrap_or_else(|| Value::String(request_id.clone()));

        let mut payload = Map::new();
        payload.insert("id".to_string(), response_id);
        payload.insert("result".to_string(), response_payload);
        state.write_message(&Value::Object(payload))
    }
}

#[cfg(not(any(test, coverage)))]
struct CodexAppServerTransportState {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_request_id: u64,
    initialized: bool,
    pending_events: VecDeque<BufferedCodexEvent>,
    pending_responses: BTreeMap<String, CrabResult<Value>>,
    request_ids: BTreeMap<String, Value>,
}

#[cfg(not(any(test, coverage)))]
impl CodexAppServerTransportState {
    fn spawn() -> CrabResult<Self> {
        let mut child = Command::new("codex")
            .arg("app-server")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|error| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("failed to spawn codex app-server: {error}"),
            })?;

        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: "codex app-server stdin pipe was not available".to_string(),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: "codex app-server stdout pipe was not available".to_string(),
            })?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_request_id: 0,
            initialized: false,
            pending_events: VecDeque::new(),
            pending_responses: BTreeMap::new(),
            request_ids: BTreeMap::new(),
        })
    }

    fn ensure_alive(&mut self) -> CrabResult<()> {
        match self
            .child
            .try_wait()
            .map_err(|error| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("failed to check codex app-server process state: {error}"),
            })? {
            None => Ok(()),
            Some(status) => Err(CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("codex app-server exited unexpectedly with status {status}"),
            }),
        }
    }

    fn next_request_id(&mut self) -> CrabResult<Value> {
        self.next_request_id =
            self.next_request_id
                .checked_add(1)
                .ok_or_else(|| CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_CONTEXT,
                    message: "request id counter overflow".to_string(),
                })?;
        Ok(Value::from(self.next_request_id))
    }

    fn ensure_initialized(&mut self) -> CrabResult<()> {
        if self.initialized {
            return Ok(());
        }

        let request_id = self.next_request_id()?;
        let request_id_key = json_rpc_id_key(&request_id)?;
        let payload = Value::Object(Map::from_iter([
            ("id".to_string(), request_id.clone()),
            (
                "method".to_string(),
                Value::String("initialize".to_string()),
            ),
            (
                "params".to_string(),
                Value::Object(Map::from_iter([(
                    "clientInfo".to_string(),
                    Value::Object(Map::from_iter([
                        ("name".to_string(), Value::String("crab-daemon".to_string())),
                        (
                            "version".to_string(),
                            Value::String(env!("CARGO_PKG_VERSION").to_string()),
                        ),
                    ])),
                )])),
            ),
        ]));
        self.write_message(&payload)?;

        loop {
            let message = self.read_message()?;
            match message {
                AppServerMessage::Response { id, result, error } => {
                    let id_key = json_rpc_id_key(&id)?;
                    if id_key == request_id_key {
                        if let Some(error_payload) = error {
                            return Err(CrabError::InvariantViolation {
                                context: CODEX_APP_SERVER_CONTEXT,
                                message: format!(
                                    "initialize failed with error payload {}",
                                    error_payload
                                ),
                            });
                        }
                        if result.is_none() {
                            return Err(CrabError::InvariantViolation {
                                context: CODEX_APP_SERVER_CONTEXT,
                                message: "initialize response missing result".to_string(),
                            });
                        }
                        self.initialized = true;
                        return Ok(());
                    }
                    self.pending_responses
                        .insert(id_key, response_from_result_or_error(result, error));
                }
                AppServerMessage::ServerRequest { id, method, params } => {
                    if let Some(buffered) = decode_server_request(id, &method, params)? {
                        self.pending_events.push_back(buffered);
                    }
                }
                AppServerMessage::Notification { method, params } => {
                    if let Some(buffered) = decode_server_notification(&method, params)? {
                        self.pending_events.push_back(buffered);
                    }
                }
            }
        }
    }

    fn write_message(&mut self, payload: &Value) -> CrabResult<()> {
        let line =
            serde_json::to_string(payload).map_err(|error| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("failed to serialize app-server payload: {error}"),
            })?;
        self.stdin
            .write_all(line.as_bytes())
            .map_err(|error| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("failed to write payload to app-server stdin: {error}"),
            })?;
        self.stdin
            .write_all(b"\n")
            .map_err(|error| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("failed to write newline to app-server stdin: {error}"),
            })?;
        self.stdin
            .flush()
            .map_err(|error| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CONTEXT,
                message: format!("failed to flush app-server stdin: {error}"),
            })?;
        Ok(())
    }

    fn read_message(&mut self) -> CrabResult<AppServerMessage> {
        loop {
            let mut line = String::new();
            let bytes_read = self.stdout.read_line(&mut line).map_err(|error| {
                CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_CONTEXT,
                    message: format!("failed reading app-server stdout: {error}"),
                }
            })?;
            if bytes_read == 0 {
                return Err(CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_CONTEXT,
                    message: "app-server stdout closed unexpectedly".to_string(),
                });
            }
            if line.trim().is_empty() {
                continue;
            }
            let value = serde_json::from_str::<Value>(line.trim()).map_err(|error| {
                CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_CONTEXT,
                    message: format!("failed to parse app-server JSON message: {error}"),
                }
            })?;
            return parse_app_server_message(value);
        }
    }

    fn drain_matching_events(
        &mut self,
        thread_id: &str,
        turn_id: &str,
        collected: &mut Vec<CodexRawEvent>,
    ) -> bool {
        let mut completed = false;
        let mut deferred = VecDeque::new();
        while let Some(buffered) = self.pending_events.pop_front() {
            if buffered.matches_turn(thread_id, turn_id) {
                completed |= buffered.terminal;
                collected.push(buffered.raw_event);
            } else {
                deferred.push_back(buffered);
            }
        }
        self.pending_events = deferred;
        completed
    }
}

#[cfg(not(any(test, coverage)))]
enum AppServerMessage {
    Response {
        id: Value,
        result: Option<Value>,
        error: Option<Value>,
    },
    ServerRequest {
        id: Value,
        method: String,
        params: Value,
    },
    Notification {
        method: String,
        params: Value,
    },
}

#[cfg(not(any(test, coverage)))]
#[derive(Debug)]
struct BufferedCodexEvent {
    raw_event: CodexRawEvent,
    thread_id: String,
    turn_id: Option<String>,
    terminal: bool,
}

#[cfg(not(any(test, coverage)))]
impl BufferedCodexEvent {
    fn from_raw_event(raw_event: CodexRawEvent) -> Self {
        let (thread_id, turn_id, terminal) = match &raw_event {
            CodexRawEvent::Notification(notification) => match notification {
                crab_backends::CodexNotification::ThreadStarted { thread_id } => {
                    (thread_id.clone(), None, false)
                }
                crab_backends::CodexNotification::TurnStarted { thread_id, turn_id }
                | crab_backends::CodexNotification::AgentMessageDelta {
                    thread_id, turn_id, ..
                }
                | crab_backends::CodexNotification::ItemCompleted {
                    thread_id, turn_id, ..
                }
                | crab_backends::CodexNotification::TokenUsageUpdated {
                    thread_id, turn_id, ..
                }
                | crab_backends::CodexNotification::Error {
                    thread_id, turn_id, ..
                } => (thread_id.clone(), Some(turn_id.clone()), false),
                crab_backends::CodexNotification::TurnCompleted {
                    thread_id, turn_id, ..
                } => (thread_id.clone(), Some(turn_id.clone()), true),
            },
            CodexRawEvent::Request(request) => match request {
                CodexRequest::CommandExecutionApproval {
                    thread_id, turn_id, ..
                }
                | CodexRequest::FileChangeApproval {
                    thread_id, turn_id, ..
                }
                | CodexRequest::ToolUserInput {
                    thread_id, turn_id, ..
                } => (thread_id.clone(), Some(turn_id.clone()), false),
            },
        };
        Self {
            raw_event,
            thread_id,
            turn_id,
            terminal,
        }
    }

    fn matches_turn(&self, thread_id: &str, turn_id: &str) -> bool {
        if self.thread_id != thread_id {
            return false;
        }
        match self.turn_id.as_deref() {
            Some(event_turn_id) => event_turn_id == turn_id,
            None => true,
        }
    }
}

#[cfg(not(any(test, coverage)))]
fn build_rpc_request_payload(id: &Value, request: &CodexRpcRequest) -> CrabResult<Value> {
    let mut params = Map::new();
    for (key, value) in &request.params {
        params.insert(key.to_string(), Value::String(value.to_string()));
    }
    if request.method == "turn/start" {
        let input_items = request
            .input
            .iter()
            .map(|text| {
                Value::Object(Map::from_iter([
                    ("type".to_string(), Value::String("text".to_string())),
                    ("text".to_string(), Value::String(text.clone())),
                ]))
            })
            .collect::<Vec<_>>();
        params.insert("input".to_string(), Value::Array(input_items));
    }
    let payload = Value::Object(Map::from_iter([
        ("id".to_string(), id.clone()),
        (
            "method".to_string(),
            Value::String(request.method.to_string()),
        ),
        ("params".to_string(), Value::Object(params)),
    ]));
    Ok(payload)
}

#[cfg(not(any(test, coverage)))]
fn map_rpc_result_to_protocol_response(
    method: &str,
    result: Value,
) -> CrabResult<CodexRpcResponse> {
    let mut fields = BTreeMap::new();
    match method {
        "thread/start" | "thread/resume" => {
            let thread_id = get_path_string(&result, &["thread", "id"]).or_else(|| {
                get_path_string(&result, &["threadId"])
                    .or_else(|| get_path_string(&result, &["id"]))
            });
            let thread_id = required_result_field(method, "thread.id", thread_id)?;
            fields.insert("threadId".to_string(), thread_id);
        }
        "turn/start" => {
            let turn_id = get_path_string(&result, &["turn", "id"]).or_else(|| {
                get_path_string(&result, &["turnId"]).or_else(|| get_path_string(&result, &["id"]))
            });
            let turn_id = required_result_field(method, "turn.id", turn_id)?;
            fields.insert("turnId".to_string(), turn_id);
        }
        "turn/interrupt" => {}
        other => {
            return Err(CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_CALL_CONTEXT,
                message: format!("unsupported protocol method {other}"),
            });
        }
    }
    Ok(CodexRpcResponse { fields })
}

#[cfg(not(any(test, coverage)))]
fn required_result_field(
    method: &str,
    field: &'static str,
    value: Option<String>,
) -> CrabResult<String> {
    let value = value
        .map(|candidate| candidate.trim().to_string())
        .filter(|candidate| !candidate.is_empty());
    value.ok_or_else(|| CrabError::InvariantViolation {
        context: CODEX_APP_SERVER_CALL_CONTEXT,
        message: format!("method {method} response missing required field {field}"),
    })
}

#[cfg(not(any(test, coverage)))]
fn parse_app_server_message(value: Value) -> CrabResult<AppServerMessage> {
    let object = value
        .as_object()
        .ok_or_else(|| CrabError::InvariantViolation {
            context: CODEX_APP_SERVER_CONTEXT,
            message: "app-server message must be a JSON object".to_string(),
        })?;

    let method = object
        .get("method")
        .and_then(Value::as_str)
        .map(str::to_string);
    let id = object.get("id").cloned();
    let params = object.get("params").cloned().unwrap_or(Value::Null);
    if let Some(method) = method {
        return match id {
            Some(id) => Ok(AppServerMessage::ServerRequest { id, method, params }),
            None => Ok(AppServerMessage::Notification { method, params }),
        };
    }

    let id = id.ok_or_else(|| CrabError::InvariantViolation {
        context: CODEX_APP_SERVER_CONTEXT,
        message: "response message missing id".to_string(),
    })?;
    let result = object.get("result").cloned();
    let error = object.get("error").cloned();
    Ok(AppServerMessage::Response { id, result, error })
}

#[cfg(not(any(test, coverage)))]
fn response_from_result_or_error(result: Option<Value>, error: Option<Value>) -> CrabResult<Value> {
    if let Some(error_payload) = error {
        return Err(CrabError::InvariantViolation {
            context: CODEX_APP_SERVER_CALL_CONTEXT,
            message: format!("app-server response error payload {}", error_payload),
        });
    }
    result.ok_or_else(|| CrabError::InvariantViolation {
        context: CODEX_APP_SERVER_CALL_CONTEXT,
        message: "app-server response missing result".to_string(),
    })
}

#[cfg(not(any(test, coverage)))]
fn decode_server_request(
    id: Value,
    method: &str,
    params: Value,
) -> CrabResult<Option<BufferedCodexEvent>> {
    let request_id = request_id_value_to_string(&id)?;
    let raw = match method {
        "item/commandExecution/requestApproval" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let item_id = required_path_string(&params, method, &["itemId"])?;
            let command = get_path_string(&params, &["command"]);
            let reason = get_path_string(&params, &["reason"]);
            Some(CodexRawEvent::Request(
                CodexRequest::CommandExecutionApproval {
                    request_id,
                    thread_id,
                    turn_id,
                    item_id,
                    command,
                    reason,
                },
            ))
        }
        "item/fileChange/requestApproval" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let item_id = required_path_string(&params, method, &["itemId"])?;
            let grant_root = get_path_string(&params, &["grantRoot"]);
            let reason = get_path_string(&params, &["reason"]);
            Some(CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                request_id,
                thread_id,
                turn_id,
                item_id,
                grant_root,
                reason,
            }))
        }
        "item/tool/requestUserInput" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let item_id = required_path_string(&params, method, &["itemId"])?;
            let question_count = params
                .get("questions")
                .and_then(Value::as_array)
                .map(std::vec::Vec::len)
                .ok_or_else(|| CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_COLLECT_CONTEXT,
                    message: format!("method {method} params missing questions array"),
                })?;
            Some(CodexRawEvent::Request(CodexRequest::ToolUserInput {
                request_id,
                thread_id,
                turn_id,
                item_id,
                question_count,
            }))
        }
        _ => None,
    };
    Ok(raw.map(BufferedCodexEvent::from_raw_event))
}

#[cfg(not(any(test, coverage)))]
fn decode_server_notification(
    method: &str,
    params: Value,
) -> CrabResult<Option<BufferedCodexEvent>> {
    let raw = match method {
        "thread/started" => {
            let thread_id = required_path_string(&params, method, &["thread", "id"])?;
            Some(CodexRawEvent::Notification(
                crab_backends::CodexNotification::ThreadStarted { thread_id },
            ))
        }
        "turn/started" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turn", "id"])?;
            Some(CodexRawEvent::Notification(
                crab_backends::CodexNotification::TurnStarted { thread_id, turn_id },
            ))
        }
        "item/agentMessage/delta" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let item_id = required_path_string(&params, method, &["itemId"])?;
            let delta = required_path_string(&params, method, &["delta"])?;
            Some(CodexRawEvent::Notification(
                crab_backends::CodexNotification::AgentMessageDelta {
                    thread_id,
                    turn_id,
                    item_id,
                    delta,
                },
            ))
        }
        "item/completed" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let item = params
                .get("item")
                .ok_or_else(|| CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_COLLECT_CONTEXT,
                    message: "item/completed params missing item".to_string(),
                })?;
            let completed_item = decode_completed_item(item)?;
            completed_item.map(|decoded| {
                CodexRawEvent::Notification(crab_backends::CodexNotification::ItemCompleted {
                    thread_id,
                    turn_id,
                    item: decoded,
                })
            })
        }
        "thread/tokenUsage/updated" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let input_tokens =
                required_path_u64(&params, method, &["tokenUsage", "last", "inputTokens"])?;
            let output_tokens =
                required_path_u64(&params, method, &["tokenUsage", "last", "outputTokens"])?;
            let total_tokens =
                required_path_u64(&params, method, &["tokenUsage", "last", "totalTokens"])?;
            Some(CodexRawEvent::Notification(
                crab_backends::CodexNotification::TokenUsageUpdated {
                    thread_id,
                    turn_id,
                    input_tokens,
                    output_tokens,
                    total_tokens,
                },
            ))
        }
        "turn/completed" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turn", "id"])?;
            let status_token = required_path_string(&params, method, &["turn", "status"])?;
            let status = match status_token.as_str() {
                "completed" => CodexTurnStatus::Completed,
                "interrupted" => CodexTurnStatus::Interrupted,
                "failed" => CodexTurnStatus::Failed,
                "inProgress" => CodexTurnStatus::InProgress,
                _ => {
                    return Err(CrabError::InvariantViolation {
                        context: CODEX_APP_SERVER_COLLECT_CONTEXT,
                        message: format!("turn/completed status {status_token} is not supported"),
                    });
                }
            };
            let error_message = get_path_string(&params, &["turn", "error", "message"]);
            Some(CodexRawEvent::Notification(
                crab_backends::CodexNotification::TurnCompleted {
                    thread_id,
                    turn_id,
                    status,
                    error_message,
                },
            ))
        }
        "error" => {
            let thread_id = required_path_string(&params, method, &["threadId"])?;
            let turn_id = required_path_string(&params, method, &["turnId"])?;
            let message = required_path_string(&params, method, &["error", "message"])?;
            let will_retry = params
                .get("willRetry")
                .and_then(Value::as_bool)
                .ok_or_else(|| CrabError::InvariantViolation {
                    context: CODEX_APP_SERVER_COLLECT_CONTEXT,
                    message: "error notification missing willRetry".to_string(),
                })?;
            Some(CodexRawEvent::Notification(
                crab_backends::CodexNotification::Error {
                    thread_id,
                    turn_id,
                    message,
                    will_retry,
                },
            ))
        }
        _ => None,
    };
    Ok(raw.map(BufferedCodexEvent::from_raw_event))
}

#[cfg(not(any(test, coverage)))]
fn decode_completed_item(item: &Value) -> CrabResult<Option<crab_backends::CodexCompletedItem>> {
    let Some(item_type) = item.get("type").and_then(Value::as_str) else {
        return Ok(None);
    };
    match item_type {
        "agentMessage" => {
            let item_id = required_path_string(item, "item/completed", &["id"])?;
            let text = required_path_string(item, "item/completed", &["text"])?;
            Ok(Some(crab_backends::CodexCompletedItem::AgentMessage {
                item_id,
                text,
            }))
        }
        "commandExecution" => {
            let item_id = required_path_string(item, "item/completed", &["id"])?;
            let output_summary = get_path_string(item, &["aggregatedOutput"])
                .filter(|value| !value.trim().is_empty())
                .unwrap_or_else(|| "command execution completed".to_string());
            let is_error = get_path_string(item, &["status"])
                .is_some_and(|status| status == "failed" || status == "declined");
            Ok(Some(crab_backends::CodexCompletedItem::ToolResult {
                item_id,
                tool_name: "commandExecution".to_string(),
                output_summary,
                is_error,
            }))
        }
        "fileChange" => {
            let item_id = required_path_string(item, "item/completed", &["id"])?;
            let changes_count = item
                .get("changes")
                .and_then(Value::as_array)
                .map(std::vec::Vec::len)
                .unwrap_or(0);
            let summary = if changes_count == 0 {
                "file change completed".to_string()
            } else {
                format!("file change completed with {changes_count} change(s)")
            };
            let is_error = get_path_string(item, &["status"])
                .is_some_and(|status| status == "failed" || status == "declined");
            Ok(Some(crab_backends::CodexCompletedItem::ToolResult {
                item_id,
                tool_name: "fileChange".to_string(),
                output_summary: summary,
                is_error,
            }))
        }
        _ => Ok(None),
    }
}

#[cfg(not(any(test, coverage)))]
fn unattended_response_payload(response: CodexRequestResponse) -> CrabResult<(String, Value)> {
    match response {
        CodexRequestResponse::CommandExecutionApproval {
            request_id,
            decision,
        } => Ok((
            request_id,
            Value::Object(Map::from_iter([(
                "decision".to_string(),
                Value::String(approval_decision_token(decision).to_string()),
            )])),
        )),
        CodexRequestResponse::FileChangeApproval {
            request_id,
            decision,
        } => Ok((
            request_id,
            Value::Object(Map::from_iter([(
                "decision".to_string(),
                Value::String(approval_decision_token(decision).to_string()),
            )])),
        )),
        CodexRequestResponse::ToolUserInput {
            request_id,
            answers,
        } => {
            let answers_payload = answers
                .into_iter()
                .map(|(question_id, answer_values)| {
                    (
                        question_id,
                        Value::Object(Map::from_iter([(
                            "answers".to_string(),
                            Value::Array(
                                answer_values
                                    .into_iter()
                                    .map(Value::String)
                                    .collect::<Vec<_>>(),
                            ),
                        )])),
                    )
                })
                .collect::<Map<_, _>>();
            Ok((
                request_id,
                Value::Object(Map::from_iter([(
                    "answers".to_string(),
                    Value::Object(answers_payload),
                )])),
            ))
        }
    }
}

#[cfg(not(any(test, coverage)))]
fn approval_decision_token(decision: crab_backends::CodexApprovalDecision) -> &'static str {
    match decision {
        crab_backends::CodexApprovalDecision::Accept => "accept",
        crab_backends::CodexApprovalDecision::AcceptForSession => "acceptForSession",
        crab_backends::CodexApprovalDecision::Decline => "decline",
    }
}

#[cfg(not(any(test, coverage)))]
fn request_id_value_to_string(request_id: &Value) -> CrabResult<String> {
    match request_id {
        Value::String(value) => Ok(value.clone()),
        Value::Number(value) => Ok(value.to_string()),
        _ => Err(CrabError::InvariantViolation {
            context: CODEX_APP_SERVER_COLLECT_CONTEXT,
            message: format!("request id must be string or number: {request_id}"),
        }),
    }
}

#[cfg(not(any(test, coverage)))]
fn json_rpc_id_key(request_id: &Value) -> CrabResult<String> {
    match request_id {
        Value::String(value) => Ok(format!("s:{value}")),
        Value::Number(value) => Ok(format!("n:{value}")),
        _ => Err(CrabError::InvariantViolation {
            context: CODEX_APP_SERVER_CONTEXT,
            message: format!("rpc id must be string or number: {request_id}"),
        }),
    }
}

#[cfg(not(any(test, coverage)))]
fn get_path_string(root: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = root;
    for key in path {
        cursor = cursor.get(*key)?;
    }
    cursor.as_str().map(str::to_string)
}

#[cfg(not(any(test, coverage)))]
fn required_path_string(root: &Value, method: &str, path: &[&str]) -> CrabResult<String> {
    let rendered_path = path.join(".");
    get_path_string(root, path).ok_or_else(|| CrabError::InvariantViolation {
        context: CODEX_APP_SERVER_COLLECT_CONTEXT,
        message: format!("method {method} params missing {rendered_path}"),
    })
}

#[cfg(not(any(test, coverage)))]
fn required_path_u64(root: &Value, method: &str, path: &[&str]) -> CrabResult<u64> {
    let rendered_path = path.join(".");
    let mut cursor = root;
    for key in path {
        cursor = cursor
            .get(*key)
            .ok_or_else(|| CrabError::InvariantViolation {
                context: CODEX_APP_SERVER_COLLECT_CONTEXT,
                message: format!("method {method} params missing {rendered_path}"),
            })?;
    }
    cursor
        .as_u64()
        .ok_or_else(|| CrabError::InvariantViolation {
            context: CODEX_APP_SERVER_COLLECT_CONTEXT,
            message: format!("method {method} params field {rendered_path} must be u64"),
        })
}

#[cfg(any(test, coverage, debug_assertions))]
#[derive(Debug, Clone, Default)]
pub struct DeterministicCodexTransport {
    state: Arc<Mutex<DeterministicCodexTransportState>>,
}

#[cfg(any(test, coverage, debug_assertions))]
#[derive(Debug, Default)]
struct DeterministicCodexTransportState {
    next_thread_number: u64,
    next_turn_number: u64,
    pending_turns: VecDeque<PendingTurn>,
    submitted_responses: Vec<CodexRequestResponse>,
}

#[cfg(any(test, coverage, debug_assertions))]
#[derive(Debug, Clone)]
struct PendingTurn {
    thread_id: String,
    turn_id: String,
    input: String,
}

#[cfg(any(test, coverage, debug_assertions))]
impl CodexRpcTransport for DeterministicCodexTransport {
    fn call(&self, request: CodexRpcRequest) -> CrabResult<CodexRpcResponse> {
        let mut state = self.state.lock().expect("lock should succeed");

        match request.method.as_str() {
            "thread/start" => {
                state.next_thread_number = state.next_thread_number.saturating_add(1);
                let thread_id = format!("thread-{}", state.next_thread_number);
                Ok(CodexRpcResponse {
                    fields: BTreeMap::from([("threadId".to_string(), thread_id)]),
                })
            }
            "thread/resume" => {
                let thread_id = required_param(&request, "threadId")?;
                Ok(CodexRpcResponse {
                    fields: BTreeMap::from([("threadId".to_string(), thread_id)]),
                })
            }
            "turn/start" => {
                let thread_id = required_param(&request, "threadId")?;
                state.next_turn_number = state.next_turn_number.saturating_add(1);
                let turn_id = format!("turn-{}", state.next_turn_number);
                let input = request.input.join("\n");
                state.pending_turns.push_back(PendingTurn {
                    thread_id,
                    turn_id: turn_id.clone(),
                    input,
                });
                Ok(CodexRpcResponse {
                    fields: BTreeMap::from([("turnId".to_string(), turn_id)]),
                })
            }
            "turn/interrupt" => Ok(CodexRpcResponse::default()),
            method => Err(CrabError::InvariantViolation {
                context: "deterministic_codex_transport_call",
                message: format!("unsupported method {method}"),
            }),
        }
    }
}

#[cfg(any(test, coverage, debug_assertions))]
impl CodexDaemonTransport for DeterministicCodexTransport {
    fn collect_turn_events(
        &self,
        thread_id: &str,
        turn_id: &str,
    ) -> CrabResult<Vec<CodexRawEvent>> {
        let mut state = self.state.lock().expect("lock should succeed");
        let Some(position) = state
            .pending_turns
            .iter()
            .position(|pending| pending.thread_id == thread_id && pending.turn_id == turn_id)
        else {
            return Err(CrabError::InvariantViolation {
                context: "deterministic_codex_transport_collect",
                message: format!("missing pending turn for {thread_id}/{turn_id}"),
            });
        };

        let pending = state
            .pending_turns
            .remove(position)
            .expect("pending turn position should exist");

        let input_tokens = token_count(&pending.input);
        let text = "Codex bridge response".to_string();
        let output_tokens = token_count(&text);
        let total_tokens = input_tokens.saturating_add(output_tokens);

        Ok(vec![
            CodexRawEvent::Notification(crab_backends::CodexNotification::ThreadStarted {
                thread_id: thread_id.to_string(),
            }),
            CodexRawEvent::Notification(crab_backends::CodexNotification::TurnStarted {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
            }),
            CodexRawEvent::Notification(crab_backends::CodexNotification::AgentMessageDelta {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                item_id: "item-1".to_string(),
                delta: text,
            }),
            CodexRawEvent::Notification(crab_backends::CodexNotification::TokenUsageUpdated {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                input_tokens,
                output_tokens,
                total_tokens,
            }),
            CodexRawEvent::Notification(crab_backends::CodexNotification::TurnCompleted {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                status: CodexTurnStatus::Completed,
                error_message: None,
            }),
        ])
    }

    fn submit_unattended_response(&self, response: CodexRequestResponse) -> CrabResult<()> {
        let mut state = self.state.lock().expect("lock should succeed");
        state.submitted_responses.push(response);
        Ok(())
    }
}

#[cfg(any(test, coverage, debug_assertions))]
fn required_param(request: &CodexRpcRequest, key: &'static str) -> CrabResult<String> {
    let value = request
        .params
        .get(key)
        .ok_or_else(|| CrabError::InvariantViolation {
            context: "deterministic_codex_transport_call",
            message: format!("missing required param {key}"),
        })?
        .trim()
        .to_string();
    if value.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "deterministic_codex_transport_call",
            message: format!("required param {key} must not be empty"),
        });
    }
    Ok(value)
}

#[cfg(any(test, coverage, debug_assertions))]
fn token_count(content: &str) -> u64 {
    u64::try_from(content.split_whitespace().count())
        .unwrap_or(1)
        .max(1)
}

fn non_empty_value(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed)
}

fn interactive_request_from_raw(raw_request: &CodexRequest) -> CodexInteractiveRequest {
    match raw_request {
        CodexRequest::CommandExecutionApproval { request_id, .. } => {
            CodexInteractiveRequest::CommandExecutionApproval {
                request_id: request_id.clone(),
            }
        }
        CodexRequest::FileChangeApproval { request_id, .. } => {
            CodexInteractiveRequest::FileChangeApproval {
                request_id: request_id.clone(),
            }
        }
        CodexRequest::ToolUserInput {
            request_id,
            question_count,
            ..
        } => CodexInteractiveRequest::ToolUserInput {
            request_id: request_id.clone(),
            questions: (1..=*question_count)
                .map(|index| CodexUserInputQuestion {
                    id: format!("q-{index}"),
                })
                .collect(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use crab_backends::{
        BackendEventKind, CodexAppServerProcess, CodexNotification, CodexProcessHandle,
    };
    use crab_core::{
        BackendKind, CrabError, InferenceProfile, ReasoningLevel, Run, RunProfileTelemetry,
        RunStatus,
    };

    use super::{
        CodexDaemonBackendBridge, CodexDaemonTransport, CodexRawEvent, CodexRequest,
        CodexRequestResponse, CodexRpcRequest, CodexRpcResponse, CodexRpcTransport,
        CodexTurnStatus, DaemonBackendBridge, DeterministicCodexTransport,
    };

    const TEST_DAEMON_TURN_ID: &str = "turn-daemon";
    const TEST_TURN_CONTEXT: &str = "hello world";

    #[derive(Debug, Clone)]
    struct FakeProcess;

    impl CodexAppServerProcess for FakeProcess {
        fn spawn_app_server(&self) -> crab_core::CrabResult<CodexProcessHandle> {
            Ok(CodexProcessHandle {
                process_id: 101,
                started_at_epoch_ms: 1,
            })
        }

        fn is_healthy(&self, _handle: &CodexProcessHandle) -> bool {
            true
        }

        fn terminate_app_server(&self, _handle: &CodexProcessHandle) -> crab_core::CrabResult<()> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct ScriptedTransport {
        state: Arc<Mutex<ScriptedTransportState>>,
    }

    #[derive(Debug, Default)]
    struct ScriptedTransportState {
        rpc_results: VecDeque<crab_core::CrabResult<CodexRpcResponse>>,
        event_results: VecDeque<crab_core::CrabResult<Vec<CodexRawEvent>>>,
        response_results: VecDeque<crab_core::CrabResult<()>>,
        requests: Vec<CodexRpcRequest>,
        unattended_responses: Vec<CodexRequestResponse>,
    }

    impl ScriptedTransport {
        fn new(
            rpc_results: Vec<crab_core::CrabResult<CodexRpcResponse>>,
            event_results: Vec<crab_core::CrabResult<Vec<CodexRawEvent>>>,
            response_results: Vec<crab_core::CrabResult<()>>,
        ) -> Self {
            Self {
                state: Arc::new(Mutex::new(ScriptedTransportState {
                    rpc_results: VecDeque::from(rpc_results),
                    event_results: VecDeque::from(event_results),
                    response_results: VecDeque::from(response_results),
                    requests: Vec::new(),
                    unattended_responses: Vec::new(),
                })),
            }
        }

        fn requests(&self) -> Vec<CodexRpcRequest> {
            self.state
                .lock()
                .expect("lock should succeed")
                .requests
                .clone()
        }

        fn unattended_responses(&self) -> Vec<CodexRequestResponse> {
            self.state
                .lock()
                .expect("lock should succeed")
                .unattended_responses
                .clone()
        }
    }

    impl CodexRpcTransport for ScriptedTransport {
        fn call(&self, request: CodexRpcRequest) -> crab_core::CrabResult<CodexRpcResponse> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.requests.push(request);
            state.rpc_results.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "scripted_codex_transport_call",
                    message: "missing scripted rpc result".to_string(),
                })
            })
        }
    }

    impl CodexDaemonTransport for ScriptedTransport {
        fn collect_turn_events(
            &self,
            _thread_id: &str,
            _turn_id: &str,
        ) -> crab_core::CrabResult<Vec<CodexRawEvent>> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.event_results.pop_front().unwrap_or_else(|| {
                Err(CrabError::InvariantViolation {
                    context: "scripted_codex_transport_collect",
                    message: "missing scripted event result".to_string(),
                })
            })
        }

        fn submit_unattended_response(
            &self,
            response: CodexRequestResponse,
        ) -> crab_core::CrabResult<()> {
            let mut state = self.state.lock().expect("lock should succeed");
            state.unattended_responses.push(response);
            state.response_results.pop_front().unwrap_or(Ok(()))
        }
    }

    fn sample_run() -> Run {
        Run {
            id: "run-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            physical_session_id: Some("physical-1".to_string()),
            status: RunStatus::Running,
            user_input: "hello world".to_string(),
            delivery_channel_id: None,
            profile: RunProfileTelemetry {
                requested_profile: None,
                resolved_profile: InferenceProfile {
                    backend: BackendKind::Codex,
                    model: "gpt-5-codex".to_string(),
                    reasoning_level: ReasoningLevel::Medium,
                },
                backend_source: crab_core::ProfileValueSource::GlobalDefault,
                model_source: crab_core::ProfileValueSource::GlobalDefault,
                reasoning_level_source: crab_core::ProfileValueSource::GlobalDefault,
                fallback_applied: false,
                fallback_notes: Vec::new(),
                sender_id: "111".to_string(),
                sender_is_owner: false,
                resolved_owner_profile: None,
            },
            queued_at_epoch_ms: 1,
            started_at_epoch_ms: Some(2),
            completed_at_epoch_ms: None,
        }
    }

    fn sample_session() -> crab_core::PhysicalSession {
        crab_core::PhysicalSession {
            id: "physical-1".to_string(),
            logical_session_id: "discord:channel:777".to_string(),
            backend: BackendKind::Codex,
            backend_session_id: "thread-abc".to_string(),
            created_at_epoch_ms: 1,
            last_turn_id: None,
        }
    }

    fn response(fields: &[(&str, &str)]) -> CodexRpcResponse {
        CodexRpcResponse {
            fields: fields
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect::<BTreeMap<_, _>>(),
        }
    }

    fn execute_bridge_turn<T: CodexDaemonTransport + Clone + Send>(
        bridge: &mut CodexDaemonBackendBridge<T>,
        lifecycle: &mut crab_backends::CodexManager<FakeProcess>,
        session: &mut crab_core::PhysicalSession,
    ) -> crab_core::CrabResult<Vec<crab_backends::BackendEvent>> {
        bridge.execute_backend_turn(
            lifecycle,
            session,
            &sample_run(),
            TEST_DAEMON_TURN_ID,
            TEST_TURN_CONTEXT,
        )
    }

    fn execute_bridge_turn_with_run<T: CodexDaemonTransport + Clone + Send>(
        bridge: &mut CodexDaemonBackendBridge<T>,
        lifecycle: &mut crab_backends::CodexManager<FakeProcess>,
        session: &mut crab_core::PhysicalSession,
        run: &Run,
    ) -> crab_core::CrabResult<Vec<crab_backends::BackendEvent>> {
        bridge.execute_backend_turn(
            lifecycle,
            session,
            run,
            TEST_DAEMON_TURN_ID,
            TEST_TURN_CONTEXT,
        )
    }

    fn execute_bridge_turn_error<T: CodexDaemonTransport + Clone + Send>(
        bridge: &mut CodexDaemonBackendBridge<T>,
        lifecycle: &mut crab_backends::CodexManager<FakeProcess>,
        session: &mut crab_core::PhysicalSession,
    ) -> CrabError {
        execute_bridge_turn(bridge, lifecycle, session)
            .expect_err("scripted bridge execution should fail")
    }

    fn init_bridge_runtime<T: CodexDaemonTransport + Clone>(
        transport: T,
    ) -> (
        CodexDaemonBackendBridge<T>,
        crab_backends::CodexManager<FakeProcess>,
        crab_core::PhysicalSession,
    ) {
        (
            CodexDaemonBackendBridge::new(transport),
            crab_backends::CodexManager::new(FakeProcess),
            sample_session(),
        )
    }

    fn completed_turn_event(
        thread_id: &str,
        turn_id: &str,
    ) -> crab_core::CrabResult<Vec<CodexRawEvent>> {
        Ok(vec![CodexRawEvent::Notification(
            CodexNotification::TurnCompleted {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                status: CodexTurnStatus::Completed,
                error_message: None,
            },
        )])
    }

    fn scripted_thread_and_turn_responses(
        thread_id: &str,
        turn_id: &str,
    ) -> Vec<crab_core::CrabResult<CodexRpcResponse>> {
        vec![
            Ok(response(&[("threadId", thread_id)])),
            Ok(response(&[("turnId", turn_id)])),
        ]
    }

    #[test]
    fn codex_bridge_executes_with_deterministic_transport_and_stream_usage() {
        let transport = DeterministicCodexTransport::default();
        let (mut bridge, mut lifecycle, mut session) = init_bridge_runtime(transport);

        let events = execute_bridge_turn(&mut bridge, &mut lifecycle, &mut session)
            .expect("codex bridge should execute");

        assert_eq!(session.backend_session_id, "thread-abc".to_string());
        assert_eq!(session.last_turn_id, Some("turn-1".to_string()));
        assert!(events.iter().any(|event| {
            event.kind == BackendEventKind::TextDelta
                && event.payload.get("delta") == Some(&"Codex bridge response".to_string())
        }));
        assert!(events.iter().any(|event| {
            event.kind == BackendEventKind::RunNote
                && event.payload.get("usage_source") == Some(&"codex".to_string())
        }));
    }

    #[test]
    fn codex_bridge_rotates_thread_when_resume_fails() {
        let transport = ScriptedTransport::new(
            vec![
                Err(CrabError::InvariantViolation {
                    context: "resume",
                    message: "resume failed".to_string(),
                }),
                Ok(response(&[("threadId", "thread-new")])),
                Ok(response(&[("turnId", "turn-new")])),
            ],
            vec![completed_turn_event("thread-new", "turn-new")],
            vec![],
        );
        let observed_transport = transport.clone();
        let (mut bridge, mut lifecycle, mut session) = init_bridge_runtime(transport);

        let events = execute_bridge_turn(&mut bridge, &mut lifecycle, &mut session)
            .expect("resume failure should rotate thread/start");

        assert_eq!(session.backend_session_id, "thread-new".to_string());
        assert_eq!(session.last_turn_id, Some("turn-new".to_string()));
        assert!(events
            .iter()
            .any(|event| event.kind == BackendEventKind::TurnCompleted));

        let requests = observed_transport.requests();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].method, "thread/resume".to_string());
        assert_eq!(requests[1].method, "thread/start".to_string());
        assert_eq!(requests[2].method, "turn/start".to_string());
    }

    #[test]
    fn codex_bridge_handles_unattended_requests_and_surfaces_submit_errors() {
        let transport = ScriptedTransport::new(
            scripted_thread_and_turn_responses("thread-abc", "turn-1"),
            vec![Ok(vec![
                CodexRawEvent::Request(CodexRequest::CommandExecutionApproval {
                    request_id: "req-1".to_string(),
                    thread_id: "thread-abc".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    command: None,
                    reason: None,
                }),
                CodexRawEvent::Request(CodexRequest::FileChangeApproval {
                    request_id: "req-2".to_string(),
                    thread_id: "thread-abc".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-2".to_string(),
                    grant_root: None,
                    reason: None,
                }),
                CodexRawEvent::Request(CodexRequest::ToolUserInput {
                    request_id: "req-3".to_string(),
                    thread_id: "thread-abc".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-3".to_string(),
                    question_count: 2,
                }),
                CodexRawEvent::Notification(CodexNotification::TurnCompleted {
                    thread_id: "thread-abc".to_string(),
                    turn_id: "turn-1".to_string(),
                    status: CodexTurnStatus::Completed,
                    error_message: None,
                }),
            ])],
            vec![
                Ok(()),
                Ok(()),
                Err(CrabError::InvariantViolation {
                    context: "submit_unattended",
                    message: "submission failed".to_string(),
                }),
            ],
        );

        let observed_transport = transport.clone();
        let (mut bridge, mut lifecycle, mut session) = init_bridge_runtime(transport);

        let error = execute_bridge_turn_error(&mut bridge, &mut lifecycle, &mut session);

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "submit_unattended",
                message: "submission failed".to_string(),
            }
        );

        let responses = observed_transport.unattended_responses();
        assert_eq!(responses.len(), 3);
    }

    #[test]
    fn codex_bridge_rejects_non_codex_backend() {
        let transport = DeterministicCodexTransport::default();
        let (mut bridge, mut lifecycle, mut session) = init_bridge_runtime(transport);
        let mut run = sample_run();
        run.profile.resolved_profile.backend = BackendKind::OpenCode;

        let error = execute_bridge_turn_with_run(&mut bridge, &mut lifecycle, &mut session, &run)
            .expect_err("non-codex backend should be rejected");

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "daemon_codex_backend_bridge",
                message: "unsupported backend OpenCode; only Codex is wired".to_string(),
            }
        );
    }

    #[test]
    fn deterministic_transport_reports_actionable_errors_for_invalid_calls() {
        let transport = DeterministicCodexTransport::default();

        let thread_start = transport
            .call(CodexRpcRequest {
                method: "thread/start".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            })
            .expect("thread/start should return a generated thread id");
        assert_eq!(
            thread_start.fields.get("threadId"),
            Some(&"thread-1".to_string())
        );

        let missing_param = transport
            .call(CodexRpcRequest {
                method: "thread/resume".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            })
            .expect_err("missing params should fail");
        assert_eq!(
            missing_param,
            CrabError::InvariantViolation {
                context: "deterministic_codex_transport_call",
                message: "missing required param threadId".to_string(),
            }
        );

        let unsupported_method = transport
            .call(CodexRpcRequest {
                method: "unsupported/method".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            })
            .expect_err("unsupported methods should fail");
        assert_eq!(
            unsupported_method,
            CrabError::InvariantViolation {
                context: "deterministic_codex_transport_call",
                message: "unsupported method unsupported/method".to_string(),
            }
        );

        let missing_turn = transport
            .collect_turn_events("thread-x", "turn-y")
            .expect_err("missing pending turn should fail");
        assert_eq!(
            missing_turn,
            CrabError::InvariantViolation {
                context: "deterministic_codex_transport_collect",
                message: "missing pending turn for thread-x/turn-y".to_string(),
            }
        );

        let _ = transport
            .call(CodexRpcRequest {
                method: "turn/interrupt".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            })
            .expect("turn/interrupt should be a no-op in deterministic transport");
    }

    #[test]
    fn codex_bridge_starts_new_thread_when_backend_session_is_blank() {
        let transport = ScriptedTransport::new(
            scripted_thread_and_turn_responses("thread-fresh", "turn-fresh"),
            vec![completed_turn_event("thread-fresh", "turn-fresh")],
            vec![],
        );
        let observed_transport = transport.clone();
        let (mut bridge, mut lifecycle, mut session) = init_bridge_runtime(transport);
        session.backend_session_id = "   ".to_string();

        execute_bridge_turn(&mut bridge, &mut lifecycle, &mut session)
            .expect("blank backend session id should force thread/start");

        let requests = observed_transport.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].method, "thread/start".to_string());
        assert_eq!(requests[1].method, "turn/start".to_string());
    }

    #[test]
    fn codex_bridge_propagates_unattended_policy_validation_errors() {
        let transport = ScriptedTransport::new(
            scripted_thread_and_turn_responses("thread-abc", "turn-1"),
            vec![Ok(vec![CodexRawEvent::Request(
                CodexRequest::ToolUserInput {
                    request_id: "req-empty".to_string(),
                    thread_id: "thread-abc".to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "item-1".to_string(),
                    question_count: 0,
                },
            )])],
            vec![],
        );
        let (mut bridge, mut lifecycle, mut session) = init_bridge_runtime(transport);
        let error = execute_bridge_turn_error(&mut bridge, &mut lifecycle, &mut session);
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "codex_unattended_policy",
                message: "tool user input request must include at least one question".to_string(),
            }
        );
    }

    #[test]
    fn deterministic_transport_accepts_submit_response_and_rejects_blank_param_values() {
        let transport = DeterministicCodexTransport::default();
        transport
            .submit_unattended_response(CodexRequestResponse::CommandExecutionApproval {
                request_id: "req-1".to_string(),
                decision: crab_backends::CodexApprovalDecision::Accept,
            })
            .expect("deterministic transport should accept unattended responses");

        let blank_param = transport
            .call(CodexRpcRequest {
                method: "thread/resume".to_string(),
                params: BTreeMap::from([("threadId".to_string(), "   ".to_string())]),
                input: Vec::new(),
            })
            .expect_err("blank threadId should fail validation");
        assert_eq!(
            blank_param,
            CrabError::InvariantViolation {
                context: "deterministic_codex_transport_call",
                message: "required param threadId must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn scripted_transport_reports_missing_scripted_rpc_and_event_results() {
        let transport = ScriptedTransport::new(Vec::new(), Vec::new(), Vec::new());

        let rpc_error = transport
            .call(CodexRpcRequest {
                method: "thread/start".to_string(),
                params: BTreeMap::new(),
                input: Vec::new(),
            })
            .expect_err("missing scripted rpc responses should fail");
        assert_eq!(
            rpc_error,
            CrabError::InvariantViolation {
                context: "scripted_codex_transport_call",
                message: "missing scripted rpc result".to_string(),
            }
        );

        let event_error = transport
            .collect_turn_events("thread-1", "turn-1")
            .expect_err("missing scripted event responses should fail");
        assert_eq!(
            event_error,
            CrabError::InvariantViolation {
                context: "scripted_codex_transport_collect",
                message: "missing scripted event result".to_string(),
            }
        );
    }

    #[test]
    fn fake_process_methods_cover_health_and_terminate_paths() {
        let process = FakeProcess;
        let handle = CodexProcessHandle {
            process_id: 7,
            started_at_epoch_ms: 9,
        };
        assert!(process.is_healthy(&handle));
        process
            .terminate_app_server(&handle)
            .expect("terminate should succeed");
    }
}
