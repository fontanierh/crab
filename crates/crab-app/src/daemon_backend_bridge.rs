use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};

use crab_backends::{
    decide_unattended_response, map_codex_turn_config, normalize_codex_events,
    recover_codex_session, BackendEvent, CodexInteractiveRequest, CodexLifecycleManager,
    CodexProtocol, CodexRawEvent, CodexRequest, CodexRequestResponse, CodexRpcRequest,
    CodexRpcResponse, CodexRpcTransport, CodexTurnStatus, CodexUnattendedPolicy,
    CodexUserInputQuestion,
};
use crab_core::{BackendKind, CrabError, CrabResult, PhysicalSession, Run};

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

#[derive(Debug, Clone, Default)]
pub struct DeterministicCodexTransport {
    state: Arc<Mutex<DeterministicCodexTransportState>>,
}

#[derive(Debug, Default)]
struct DeterministicCodexTransportState {
    next_thread_number: u64,
    next_turn_number: u64,
    pending_turns: VecDeque<PendingTurn>,
    submitted_responses: Vec<CodexRequestResponse>,
}

#[derive(Debug, Clone)]
struct PendingTurn {
    thread_id: String,
    turn_id: String,
    input: String,
}

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
