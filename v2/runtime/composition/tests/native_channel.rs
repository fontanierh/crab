use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

extern crate agent_host_contract as boxology_generated_contract;

use agent_host_implementation::{
    AcpEvent, AcpEventDirection, AcpEventKind, AgentCatalog, AgentHostError, AgentInputMode,
    AgentLifecycle, AgentSession, DetachSessionsReport, DetachSessionsRequest,
    DiscoverAgentsRequest, EventPage, OpenSessionRequest, OperationReceipt, PermissionRequest,
    PermissionResolution, PreflightReport, PreflightRequest, PromptAccepted, PromptDisposition,
    PromptRequest, ReadEventsRequest, ResumeSessionRequest, RunReference, SessionReference,
    SessionStatus, generated as agent_host,
};
use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};
use boxology_runtime::CompositionBuilder;
use native_channel_contract::{
    BindChannelRequest, BindingReference, ChannelInputMode, ChannelTurn, ChannelTurnDisposition,
    InterruptRequest, InterruptingTurnRequest, LocateBindingRequest, NativeChannelError,
    NativeEventDirection, RecoverSessionRequest, ReplayRequest,
};
use native_channel_implementation::{NativeChannelState, generated as native_channel};

#[derive(Default)]
struct FakeState {
    active_run_id: Option<String>,
    queued_run_ids: VecDeque<String>,
    events: Vec<AcpEvent>,
    next_run: u64,
}

struct FakeAgentHost {
    state: Arc<Mutex<FakeState>>,
}

#[boxology::implementation]
impl FakeAgentHost {
    async fn discover_agents(
        &self,
        context: CallContext,
        request: DiscoverAgentsRequest,
    ) -> Result<AgentCatalog, AgentHostError> {
        let _ = (context, request);
        Ok(AgentCatalog { agents: Vec::new() })
    }

    async fn preflight(
        &self,
        context: CallContext,
        request: PreflightRequest,
    ) -> Result<PreflightReport, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::UnknownAgent)
    }

    async fn open_session(
        &self,
        context: CallContext,
        request: OpenSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::UnknownAgent)
    }

    async fn resume_session(
        &self,
        context: CallContext,
        request: ResumeSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::SessionResumeUnavailable)
    }

    async fn prompt(
        &self,
        context: CallContext,
        request: PromptRequest,
    ) -> Result<PromptAccepted, AgentHostError> {
        let _ = context;
        if request.session_id != "session-1" {
            return Err(AgentHostError::UnknownSession);
        }
        let mut state = self.state.lock().expect("fake state lock");
        state.next_run += 1;
        let new_run_id = format!("run-{}", state.next_run);
        let (run_id, disposition, emit, interrupted_run_id, cancel_requested_at_ms) =
            match (&request.mode, state.active_run_id.clone()) {
                (AgentInputMode::Queue, None) => {
                    state.active_run_id = Some(new_run_id.clone());
                    (
                        new_run_id,
                        PromptDisposition::StartedForegroundWork,
                        true,
                        None,
                        None,
                    )
                }
                (AgentInputMode::Queue, Some(_)) => {
                    state.queued_run_ids.push_back(new_run_id.clone());
                    (
                        new_run_id,
                        PromptDisposition::QueuedForTurnBoundary,
                        false,
                        None,
                        None,
                    )
                }
                (AgentInputMode::Steer, Some(active)) => (
                    active,
                    PromptDisposition::ContributedToActiveWork,
                    true,
                    None,
                    None,
                ),
                (AgentInputMode::Steer, None) => {
                    return Err(AgentHostError::SteeringUnavailable);
                }
                (AgentInputMode::InterruptAndQueue, None) => {
                    state.active_run_id = Some(new_run_id.clone());
                    (
                        new_run_id,
                        PromptDisposition::StartedForegroundWork,
                        true,
                        None,
                        None,
                    )
                }
                (AgentInputMode::InterruptAndQueue, Some(active)) => {
                    state.queued_run_ids.push_back(new_run_id.clone());
                    (
                        new_run_id,
                        PromptDisposition::CancelRequestedThenQueued,
                        false,
                        Some(active),
                        Some(500),
                    )
                }
                (AgentInputMode::Unknown { .. }, _) => {
                    return Err(AgentHostError::InvalidNativePayload);
                }
            };
        if emit {
            let sequence = state.events.len() as u64 + 1;
            state.events.push(AcpEvent {
                session_id: request.session_id.clone(),
                run_id: Some(run_id.clone()),
                sequence,
                observed_at_ms: sequence * 10,
                kind: AcpEventKind::Message,
                direction: AcpEventDirection::ClientToAgent,
                native_event_json: format!(
                    r#"{{"jsonrpc":"2.0","method":"session/prompt","id":"{}"}}"#,
                    request.client_turn_id
                ),
            });
        }
        Ok(PromptAccepted {
            session_id: request.session_id,
            run_id,
            accepted_at_ms: 100 + state.next_run,
            disposition,
            interrupted_run_id,
            cancel_requested_at_ms,
        })
    }

    async fn read_events(
        &self,
        context: CallContext,
        request: ReadEventsRequest,
    ) -> Result<EventPage, AgentHostError> {
        let _ = context;
        if request.session_id != "session-1" {
            return Err(AgentHostError::UnknownSession);
        }
        let state = self.state.lock().expect("fake state lock");
        let last_sequence = state.events.len() as u64;
        if request.after_sequence > last_sequence || request.limit > 1_000 {
            return Err(AgentHostError::InvalidCursor);
        }
        let events = state
            .events
            .iter()
            .filter(|event| event.sequence > request.after_sequence)
            .take(request.limit as usize)
            .cloned()
            .collect::<Vec<_>>();
        let next_sequence = events
            .last()
            .map_or(request.after_sequence, |event| event.sequence);
        Ok(EventPage {
            events,
            next_sequence,
            caught_up: next_sequence == last_sequence,
        })
    }

    async fn resolve_permission(
        &self,
        context: CallContext,
        request: PermissionRequest,
    ) -> Result<PermissionResolution, AgentHostError> {
        let _ = (context, request);
        Err(AgentHostError::UnknownPermission)
    }

    async fn session_status(
        &self,
        context: CallContext,
        request: SessionReference,
    ) -> Result<SessionStatus, AgentHostError> {
        let _ = context;
        if request.session_id != "session-1" {
            return Err(AgentHostError::UnknownSession);
        }
        let state = self.state.lock().expect("fake state lock");
        Ok(SessionStatus {
            session_id: request.session_id,
            lifecycle: if state.active_run_id.is_some() {
                AgentLifecycle::Busy
            } else {
                AgentLifecycle::Ready
            },
            last_sequence: state.events.len() as u64,
            active_run_id: state.active_run_id.clone(),
        })
    }

    async fn cancel_run(
        &self,
        context: CallContext,
        request: RunReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = context;
        let mut state = self.state.lock().expect("fake state lock");
        if state.active_run_id.as_deref() != Some(request.run_id.as_str()) {
            return Err(AgentHostError::UnknownRun);
        }
        let sequence = state.events.len() as u64 + 1;
        state.events.push(AcpEvent {
            session_id: request.session_id,
            run_id: Some(request.run_id),
            sequence,
            observed_at_ms: sequence * 10,
            kind: AcpEventKind::RunFinished,
            direction: AcpEventDirection::AgentToClient,
            native_event_json: r#"{"jsonrpc":"2.0","method":"crab/run_finished"}"#.into(),
        });
        state.active_run_id = state.queued_run_ids.pop_front();
        Ok(OperationReceipt {
            accepted: true,
            recorded_at_ms: 500,
        })
    }

    async fn close_session(
        &self,
        context: CallContext,
        request: SessionReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = (context, request);
        self.state.lock().expect("fake state lock").active_run_id = None;
        Ok(OperationReceipt {
            accepted: true,
            recorded_at_ms: 600,
        })
    }

    async fn detach_sessions(
        &self,
        context: CallContext,
        request: DetachSessionsRequest,
    ) -> Result<DetachSessionsReport, AgentHostError> {
        let _ = (context, request);
        self.state.lock().expect("fake state lock").active_run_id = None;
        Ok(DetachSessionsReport {
            detached_session_ids: vec!["session-1".into()],
            failed_session_ids: Vec::new(),
        })
    }
}

fn context() -> CallContext {
    CallContext::new(
        Caller::Anonymous,
        None,
        CancelToken::new(),
        TraceContext::empty(),
        None,
    )
}

fn turn(binding_id: &str, id: &str, mode: ChannelInputMode, text: &str) -> ChannelTurn {
    ChannelTurn {
        binding_id: binding_id.into(),
        client_turn_id: id.into(),
        received_at_ms: 10,
        mode,
        native_prompt_json: format!(r#"[{{"type":"text","text":"{text}"}}]"#),
    }
}

#[tokio::test]
async fn native_channel_routes_replays_publishes_and_interrupts_through_import() {
    let fake_state = Arc::new(Mutex::new(FakeState::default()));
    let channel_state = NativeChannelState::open_in_memory().expect("channel state opens");
    let mut builder = CompositionBuilder::new();
    let agent = agent_host::register(&mut builder, FakeAgentHost { state: fake_state });
    let channel = native_channel::register(&mut builder, move |imports| {
        channel_state.connect(imports.agent_host)
    });
    builder.connect(&channel, &agent);
    let handle = builder.handle::<native_channel_contract::NativeChannelHandle>(&channel);
    let _composition = builder.start().expect("graph starts with resolved import");

    let binding = handle
        .bind_channel(
            context(),
            BindChannelRequest {
                channel_id: "native-1".into(),
                adapter_id: "test-ui".into(),
                session_id: "session-1".into(),
                native_channel_json: r#"{"title":"Jim"}"#.into(),
            },
        )
        .await
        .expect("live session binds");
    assert_eq!(binding.native_channel_json, r#"{"title":"Jim"}"#);
    assert_eq!(
        handle
            .inspect_binding(
                context(),
                BindingReference {
                    binding_id: binding.binding_id.clone(),
                },
            )
            .await
            .expect("binding identity is inspectable"),
        binding
    );
    assert_eq!(
        handle
            .find_binding(
                context(),
                LocateBindingRequest {
                    channel_id: "native-1".into(),
                    adapter_id: "test-ui".into(),
                },
            )
            .await
            .expect("binding is locatable by configured identity"),
        binding
    );

    let active = handle
        .accept_turn(
            context(),
            turn(
                &binding.binding_id,
                "turn-1",
                ChannelInputMode::Queue,
                "hold",
            ),
        )
        .await
        .expect("first turn starts");
    assert_eq!(
        active.disposition,
        ChannelTurnDisposition::StartedForegroundWork
    );
    let queued_request = turn(
        &binding.binding_id,
        "turn-2",
        ChannelInputMode::Queue,
        "later",
    );
    let queued = handle
        .accept_turn(context(), queued_request.clone())
        .await
        .expect("second turn queues");
    assert_eq!(
        queued.disposition,
        ChannelTurnDisposition::QueuedForTurnBoundary
    );
    assert_eq!(
        handle
            .accept_turn(context(), queued_request)
            .await
            .expect("retry deduplicates"),
        queued
    );
    assert_eq!(
        handle
            .accept_turn(
                context(),
                turn(
                    &binding.binding_id,
                    "turn-2",
                    ChannelInputMode::Queue,
                    "changed",
                ),
            )
            .await,
        Err(boxology_contract::CallError::Domain(
            NativeChannelError::DuplicateTurnConflict
        ))
    );
    let steered = handle
        .accept_turn(
            context(),
            turn(
                &binding.binding_id,
                "turn-3",
                ChannelInputMode::Steer,
                "now",
            ),
        )
        .await
        .expect("steering reaches active run");
    assert_eq!(
        steered.disposition,
        ChannelTurnDisposition::ContributedToActiveWork
    );
    assert_eq!(steered.run_id, active.run_id);

    let urgent_request = turn(
        &binding.binding_id,
        "turn-urgent",
        ChannelInputMode::Queue,
        "interrupt now",
    );
    let urgent = handle
        .accept_interrupting_turn(
            context(),
            InterruptingTurnRequest {
                turn: urgent_request.clone(),
                reason: "automatic bridge policy".into(),
            },
        )
        .await
        .expect("interrupting input is accepted before cancellation");
    assert_eq!(
        urgent.disposition,
        ChannelTurnDisposition::CancelRequestedThenQueued
    );
    assert_eq!(urgent.interrupted_run_id, Some(active.run_id.clone()));
    assert_eq!(urgent.cancel_requested_at_ms, Some(500));
    assert_eq!(
        handle
            .accept_interrupting_turn(
                context(),
                InterruptingTurnRequest {
                    turn: urgent_request.clone(),
                    reason: "automatic bridge policy".into(),
                },
            )
            .await
            .expect("interrupting retry is stable"),
        urgent
    );
    assert_eq!(
        handle
            .accept_interrupting_turn(
                context(),
                InterruptingTurnRequest {
                    turn: urgent_request.clone(),
                    reason: "changed reason".into(),
                },
            )
            .await,
        Err(boxology_contract::CallError::Domain(
            NativeChannelError::DuplicateTurnConflict
        ))
    );
    assert_eq!(
        handle.accept_turn(context(), urgent_request).await,
        Err(boxology_contract::CallError::Domain(
            NativeChannelError::DuplicateTurnConflict
        ))
    );

    let replay = handle
        .replay_native_events(
            context(),
            ReplayRequest {
                binding_id: binding.binding_id.clone(),
                after_sequence: 0,
                limit: 100,
            },
        )
        .await
        .expect("full event view replays");
    assert_eq!(replay.events.len(), 2);
    assert_eq!(
        replay.events[0].direction,
        Some(NativeEventDirection::ClientToAgent)
    );
    assert_eq!(
        handle
            .publish_native_event(context(), replay.events[1].clone())
            .await,
        Err(boxology_contract::CallError::Domain(
            NativeChannelError::SequenceGap
        ))
    );
    let published = handle
        .publish_native_event(context(), replay.events[0].clone())
        .await
        .expect("first authoritative event publishes");
    assert_eq!(
        handle
            .publish_native_event(context(), replay.events[0].clone())
            .await
            .expect("publication retry is stable"),
        published
    );

    let status = handle
        .channel_status(
            context(),
            BindingReference {
                binding_id: binding.binding_id.clone(),
            },
        )
        .await
        .expect("channel status reads agent state");
    assert_eq!(status.available_sequence, 2);
    assert_eq!(status.pending_input_count, 2);

    let interrupted = handle
        .interrupt_and_drain(
            context(),
            InterruptRequest {
                binding_id: binding.binding_id.clone(),
                expected_session_id: binding.session_id,
                requested_at_ms: 400,
                reason: "user requested".into(),
            },
        )
        .await
        .expect("explicit interrupt cancels active run");
    assert_eq!(interrupted.pending_input_count, 2);
    assert_eq!(interrupted.cancel_requested_at_ms, 500);

    handle
        .unbind_channel(
            context(),
            BindingReference {
                binding_id: binding.binding_id,
            },
        )
        .await
        .expect("binding detaches");
}

#[tokio::test]
async fn failed_binding_recovery_preserves_session_and_publication_cursors() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("native.sqlite");
    let fake_state = Arc::new(Mutex::new(FakeState::default()));
    let (binding_id, published_sequence) = {
        let channel_state = NativeChannelState::open(&database).expect("channel state opens");
        let mut builder = CompositionBuilder::new();
        let agent = agent_host::register(
            &mut builder,
            FakeAgentHost {
                state: fake_state.clone(),
            },
        );
        let channel = native_channel::register(&mut builder, move |imports| {
            channel_state.connect(imports.agent_host)
        });
        builder.connect(&channel, &agent);
        let handle = builder.handle::<native_channel_contract::NativeChannelHandle>(&channel);
        let _composition = builder.start().expect("first graph starts");
        let binding = handle
            .bind_channel(
                context(),
                BindChannelRequest {
                    channel_id: "native-recovery".into(),
                    adapter_id: "test-ui".into(),
                    session_id: "session-1".into(),
                    native_channel_json: "{}".into(),
                },
            )
            .await
            .expect("session binds");
        handle
            .accept_turn(
                context(),
                turn(
                    &binding.binding_id,
                    "recovery-turn",
                    ChannelInputMode::Queue,
                    "hold",
                ),
            )
            .await
            .expect("turn creates an authoritative event");
        let replay = handle
            .replay_native_events(
                context(),
                ReplayRequest {
                    binding_id: binding.binding_id.clone(),
                    after_sequence: 0,
                    limit: 100,
                },
            )
            .await
            .expect("event replays");
        handle
            .publish_native_event(context(), replay.events[0].clone())
            .await
            .expect("event publishes");
        let status = handle
            .channel_status(
                context(),
                BindingReference {
                    binding_id: binding.binding_id.clone(),
                },
            )
            .await
            .expect("cursor reconciles");
        (binding.binding_id, status.binding.published_sequence)
    };

    let channel_state = NativeChannelState::open(&database).expect("channel state reopens");
    let mut builder = CompositionBuilder::new();
    let agent = agent_host::register(&mut builder, FakeAgentHost { state: fake_state });
    let channel = native_channel::register(&mut builder, move |imports| {
        channel_state.connect(imports.agent_host)
    });
    builder.connect(&channel, &agent);
    let handle = builder.handle::<native_channel_contract::NativeChannelHandle>(&channel);
    let _composition = builder.start().expect("restarted graph starts");
    let failed = handle
        .inspect_binding(
            context(),
            BindingReference {
                binding_id: binding_id.clone(),
            },
        )
        .await
        .expect("failed binding remains inspectable");
    assert_eq!(
        failed.lifecycle,
        native_channel_contract::ChannelLifecycle::Failed
    );
    let recovered = handle
        .recover_session(
            context(),
            RecoverSessionRequest {
                binding_id: binding_id.clone(),
                expected_session_id: "session-1".into(),
            },
        )
        .await
        .expect("same live session recovers binding");
    assert_eq!(recovered.session_id, "session-1");
    assert_eq!(recovered.published_sequence, published_sequence);
    assert_eq!(
        handle
            .recover_session(
                context(),
                RecoverSessionRequest {
                    binding_id,
                    expected_session_id: "session-1".into(),
                },
            )
            .await
            .expect("recovery retry is idempotent"),
        recovered
    );
}
