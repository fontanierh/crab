use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

extern crate agent_host_contract as boxology_generated_contract;

use agent_host_implementation::{
    AcpEvent, AcpEventDirection, AcpEventKind, AcpNegotiation, AcpProtocolProfile, AgentCatalog,
    AgentHostError, AgentInputMode, AgentLifecycle, AgentSession, AuthorityAttestation,
    CompactionReporting, DetachSessionsReport, DetachSessionsRequest, DiscoverAgentsRequest,
    EventPage, FilesystemAuthority, NetworkAuthority, OpenSessionRequest, OperationReceipt,
    PermissionAuthority, PermissionRequest, PermissionResolution, PreflightReport,
    PreflightRequest, PromptAccepted, PromptDisposition, PromptRequest, ReadEventsRequest,
    ResumeSessionRequest, RootAuthority, RunReference, SandboxAuthority, SessionReference,
    SessionStatus, SteeringSupport, generated as agent_host,
};
use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};
use boxology_runtime::CompositionBuilder;
use sub_agent_host_contract::{
    ContextRealization, InputDisposition, ReadSubAgentEventsRequest, RecoverSubAgentsRequest,
    SendToChildRequest, SendToParentRequest, SpawnSubAgentRequest, StopSubAgentRequest,
    SubAgentContextMode, SubAgentEventKind, SubAgentHostError, SubAgentInputMode,
    SubAgentLifecycle, SubAgentRecoveryDisposition, SubAgentReference,
};
use sub_agent_host_implementation::{SubAgentHostState, generated as sub_agent_host};

struct FakeSession {
    lifecycle: AgentLifecycle,
    active_run_id: Option<String>,
    events: Vec<AcpEvent>,
}

struct FakeState {
    sessions: HashMap<String, FakeSession>,
    opened: Vec<OpenSessionRequest>,
    next_child: u64,
    next_run: u64,
    resume_mode: FakeResumeMode,
    resume_attempts: Vec<String>,
}

#[derive(Clone, Copy)]
enum FakeResumeMode {
    Success,
    Unavailable,
    IdentityMismatch,
    AuthorityFailure,
}

impl FakeState {
    fn with_parent() -> Self {
        let parent_events = vec![
            event(
                "parent-1",
                1,
                AcpEventDirection::ClientToAgent,
                r#"{"jsonrpc":"2.0","method":"session/prompt","params":{"prompt":[{"type":"text","text":"parent question"}]}}"#,
            ),
            event(
                "parent-1",
                2,
                AcpEventDirection::AgentToClient,
                r#"{"jsonrpc":"2.0","method":"session/update","params":{"update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"parent answer"}}}}"#,
            ),
        ];
        Self {
            sessions: HashMap::from([(
                "parent-1".into(),
                FakeSession {
                    lifecycle: AgentLifecycle::Ready,
                    active_run_id: None,
                    events: parent_events,
                },
            )]),
            opened: Vec::new(),
            next_child: 0,
            next_run: 0,
            resume_mode: FakeResumeMode::Success,
            resume_attempts: Vec::new(),
        }
    }
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
        let _ = context;
        let mut state = self.state.lock().expect("fake state lock");
        state.next_child += 1;
        let session_id = format!("child-{}", state.next_child);
        state.opened.push(request.clone());
        state.sessions.insert(
            session_id.clone(),
            FakeSession {
                lifecycle: AgentLifecycle::Ready,
                active_run_id: None,
                events: Vec::new(),
            },
        );
        Ok(AgentSession {
            session_id: session_id.clone(),
            native_session_id: format!("native-{session_id}"),
            agent_id: request.agent_id,
            negotiation: AcpNegotiation {
                protocol_version: 2,
                protocol_profile: AcpProtocolProfile::V2Draft,
                steering: SteeringSupport::AcpV2ConcurrentPrompt,
                compaction_reporting: CompactionReporting::DraftLifecycleUpdates,
                agent_capabilities_json: "{}".into(),
            },
            authority: authority(),
        })
    }

    async fn resume_session(
        &self,
        context: CallContext,
        request: ResumeSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = context;
        let mut state = self.state.lock().expect("fake state lock");
        state.resume_attempts.push(request.session_id.clone());
        let mode = state.resume_mode;
        match mode {
            FakeResumeMode::Unavailable => Err(AgentHostError::SessionResumeUnavailable),
            FakeResumeMode::AuthorityFailure => Err(AgentHostError::AuthorityUnavailable),
            FakeResumeMode::Success | FakeResumeMode::IdentityMismatch => {
                let session = state
                    .sessions
                    .get_mut(&request.session_id)
                    .ok_or(AgentHostError::UnknownSession)?;
                session.lifecycle = AgentLifecycle::Ready;
                session.active_run_id = None;
                Ok(AgentSession {
                    session_id: request.session_id.clone(),
                    native_session_id: if matches!(mode, FakeResumeMode::IdentityMismatch) {
                        "rewritten-native-session".into()
                    } else {
                        format!("native-{}", request.session_id)
                    },
                    agent_id: "fake-agent".into(),
                    negotiation: AcpNegotiation {
                        protocol_version: 2,
                        protocol_profile: AcpProtocolProfile::V2Draft,
                        steering: SteeringSupport::AcpV2ConcurrentPrompt,
                        compaction_reporting: CompactionReporting::DraftLifecycleUpdates,
                        agent_capabilities_json: "{}".into(),
                    },
                    authority: authority(),
                })
            }
        }
    }

    async fn prompt(
        &self,
        context: CallContext,
        request: PromptRequest,
    ) -> Result<PromptAccepted, AgentHostError> {
        let _ = context;
        let mut state = self.state.lock().expect("fake state lock");
        state.next_run += 1;
        let new_run_id = format!("run-{}", state.next_run);
        let session = state
            .sessions
            .get_mut(&request.session_id)
            .ok_or(AgentHostError::UnknownSession)?;
        let (run_id, disposition) = match (&request.mode, session.active_run_id.clone()) {
            (AgentInputMode::Queue, None) => {
                session.lifecycle = AgentLifecycle::Busy;
                session.active_run_id = Some(new_run_id.clone());
                (new_run_id, PromptDisposition::StartedForegroundWork)
            }
            (AgentInputMode::Queue, Some(_)) => {
                (new_run_id, PromptDisposition::QueuedForTurnBoundary)
            }
            (AgentInputMode::Steer, Some(active)) => {
                (active, PromptDisposition::ContributedToActiveWork)
            }
            (AgentInputMode::Steer, None) => return Err(AgentHostError::SteeringUnavailable),
            (AgentInputMode::Unknown { .. }, _) => {
                return Err(AgentHostError::InvalidNativePayload);
            }
        };
        let sequence = session.events.len() as u64 + 1;
        session.events.push(AcpEvent {
            session_id: request.session_id.clone(),
            run_id: Some(run_id.clone()),
            sequence,
            observed_at_ms: 1_000 + sequence,
            kind: AcpEventKind::Message,
            direction: AcpEventDirection::ClientToAgent,
            native_event_json: serde_json::json!({
                "jsonrpc": "2.0",
                "method": "session/prompt",
                "id": request.client_turn_id,
                "params": { "prompt": serde_json::from_str::<serde_json::Value>(&request.native_prompt_json).expect("test prompt is JSON") },
            })
            .to_string(),
        });
        Ok(PromptAccepted {
            session_id: request.session_id,
            run_id,
            accepted_at_ms: 2_000 + state.next_run,
            disposition,
        })
    }

    async fn read_events(
        &self,
        context: CallContext,
        request: ReadEventsRequest,
    ) -> Result<EventPage, AgentHostError> {
        let _ = context;
        let state = self.state.lock().expect("fake state lock");
        let session = state
            .sessions
            .get(&request.session_id)
            .ok_or(AgentHostError::UnknownSession)?;
        let last_sequence = session.events.len() as u64;
        if request.limit == 0 || request.limit > 1_000 || request.after_sequence > last_sequence {
            return Err(AgentHostError::InvalidCursor);
        }
        let events = session
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
            caught_up: next_sequence >= last_sequence,
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
        let state = self.state.lock().expect("fake state lock");
        let session = state
            .sessions
            .get(&request.session_id)
            .ok_or(AgentHostError::UnknownSession)?;
        Ok(SessionStatus {
            session_id: request.session_id,
            lifecycle: session.lifecycle.clone(),
            last_sequence: session.events.len() as u64,
            active_run_id: session.active_run_id.clone(),
        })
    }

    async fn cancel_run(
        &self,
        context: CallContext,
        request: RunReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = context;
        let mut state = self.state.lock().expect("fake state lock");
        let session = state
            .sessions
            .get_mut(&request.session_id)
            .ok_or(AgentHostError::UnknownSession)?;
        if session.active_run_id.as_deref() != Some(request.run_id.as_str()) {
            return Err(AgentHostError::UnknownRun);
        }
        session.active_run_id = None;
        session.lifecycle = AgentLifecycle::Ready;
        let sequence = session.events.len() as u64 + 1;
        session.events.push(AcpEvent {
            session_id: request.session_id,
            run_id: Some(request.run_id),
            sequence,
            observed_at_ms: 3_000 + sequence,
            kind: AcpEventKind::RunFinished,
            direction: AcpEventDirection::AgentToClient,
            native_event_json: r#"{"jsonrpc":"2.0","method":"crab/run_finished"}"#.into(),
        });
        Ok(OperationReceipt {
            accepted: true,
            recorded_at_ms: 3_000,
        })
    }

    async fn close_session(
        &self,
        context: CallContext,
        request: SessionReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = context;
        let mut state = self.state.lock().expect("fake state lock");
        let session = state
            .sessions
            .get_mut(&request.session_id)
            .ok_or(AgentHostError::UnknownSession)?;
        session.active_run_id = None;
        session.lifecycle = AgentLifecycle::Stopped;
        Ok(OperationReceipt {
            accepted: true,
            recorded_at_ms: 4_000,
        })
    }

    async fn detach_sessions(
        &self,
        context: CallContext,
        request: DetachSessionsRequest,
    ) -> Result<DetachSessionsReport, AgentHostError> {
        let _ = (context, request);
        let mut state = self.state.lock().expect("fake state lock");
        let mut detached_session_ids = state.sessions.keys().cloned().collect::<Vec<_>>();
        detached_session_ids.sort();
        for session in state.sessions.values_mut() {
            session.active_run_id = None;
            session.lifecycle = AgentLifecycle::Detached;
        }
        Ok(DetachSessionsReport {
            detached_session_ids,
            failed_session_ids: Vec::new(),
        })
    }
}

fn authority() -> AuthorityAttestation {
    AuthorityAttestation {
        sandbox: SandboxAuthority::DisabledAndVerified,
        permissions: PermissionAuthority::YoloAndVerified,
        filesystem: FilesystemAuthority::UnrestrictedAndVerified,
        network: NetworkAuthority::UnrestrictedAndVerified,
        root: RootAuthority::PasswordlessSudoAndVerified,
        verified_at_ms: 1,
        evidence_json: "{}".into(),
    }
}

fn event(
    session_id: &str,
    sequence: u64,
    direction: AcpEventDirection,
    native_event_json: &str,
) -> AcpEvent {
    AcpEvent {
        session_id: session_id.into(),
        run_id: Some("parent-run".into()),
        sequence,
        observed_at_ms: sequence,
        kind: AcpEventKind::Message,
        direction,
        native_event_json: native_event_json.into(),
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

fn spawn_request(client_id: &str, context_mode: SubAgentContextMode) -> SpawnSubAgentRequest {
    let inherited = matches!(context_mode, SubAgentContextMode::InheritParent);
    SpawnSubAgentRequest {
        client_sub_agent_id: client_id.into(),
        parent_session_id: "parent-1".into(),
        agent_id: "fake-agent".into(),
        working_directory: "/tmp".into(),
        context_mode,
        parent_context_through_sequence: inherited.then_some(2),
        allow_portable_snapshot: inherited,
        native_task_prompt_json: r#"[{"type":"text","text":"child task"}]"#.into(),
        metadata_json: r#"{"purpose":"integration-test"}"#.into(),
        crash_restart_limit: 0,
    }
}

#[tokio::test]
async fn sub_agent_host_spawns_both_context_modes_and_routes_live_bidirectionally() {
    let fake_state = Arc::new(Mutex::new(FakeState::with_parent()));
    let host_state = SubAgentHostState::open_in_memory().expect("sub-agent state opens");
    let mut builder = CompositionBuilder::new();
    let agent = agent_host::register(
        &mut builder,
        FakeAgentHost {
            state: fake_state.clone(),
        },
    );
    let host = sub_agent_host::register(&mut builder, move |imports| {
        host_state.connect(imports.agent_host)
    });
    builder.connect(&host, &agent);
    let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
    let _composition = builder.start().expect("graph starts");

    let inherited_request = spawn_request("inherited", SubAgentContextMode::InheritParent);
    let inherited = handle
        .spawn(context(), inherited_request.clone())
        .await
        .expect("inherited child starts");
    assert_eq!(
        inherited.context_realization,
        ContextRealization::PortableSnapshot
    );
    assert_eq!(
        handle
            .spawn(context(), inherited_request.clone())
            .await
            .expect("spawn retry deduplicates"),
        inherited
    );
    {
        let state = fake_state.lock().expect("fake state lock");
        let bootstrap = state.opened[0]
            .bootstrap_prompt
            .as_deref()
            .expect("inherited child receives bootstrap");
        assert!(bootstrap.contains("through_sequence=\"2\""));
        assert!(bootstrap.contains("parent question"));
        assert!(bootstrap.contains("parent answer"));
    }

    let queued = handle
        .send_to_child(
            context(),
            SendToChildRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                client_message_id: "parent-queue".into(),
                mode: SubAgentInputMode::Queue,
                native_prompt_json: r#"[{"type":"text","text":"later"}]"#.into(),
            },
        )
        .await
        .expect("parent queues to busy child");
    assert_eq!(queued.disposition, InputDisposition::QueuedForTurnBoundary);
    let steered = handle
        .send_to_child(
            context(),
            SendToChildRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                client_message_id: "parent-steer".into(),
                mode: SubAgentInputMode::Steer,
                native_prompt_json: r#"[{"type":"text","text":"steer now"}]"#.into(),
            },
        )
        .await
        .expect("parent steers child");
    assert_eq!(
        steered.disposition,
        InputDisposition::ContributedToActiveWork
    );
    let interrupted = handle
        .send_to_child(
            context(),
            SendToChildRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                client_message_id: "parent-interrupt".into(),
                mode: SubAgentInputMode::InterruptAndSteer,
                native_prompt_json: r#"[{"type":"text","text":"replace work"}]"#.into(),
            },
        )
        .await
        .expect("parent interrupts child");
    assert_eq!(
        interrupted.disposition,
        InputDisposition::CancelRequestedThenQueued
    );

    let child_progress = SendToParentRequest {
        sub_agent_id: inherited.sub_agent_id.clone(),
        client_message_id: "child-progress".into(),
        mode: SubAgentInputMode::Queue,
        message_json: r#"{"progress":"halfway"}"#.into(),
    };
    let delivered = handle
        .send_to_parent(context(), child_progress.clone())
        .await
        .expect("child sends progress to parent");
    assert_eq!(
        delivered.disposition,
        InputDisposition::StartedForegroundWork
    );
    assert_eq!(
        handle
            .send_to_parent(context(), child_progress)
            .await
            .expect("child delivery retry deduplicates"),
        delivered
    );
    let concurrent_child = handle.send_to_child(
        context(),
        SendToChildRequest {
            sub_agent_id: inherited.sub_agent_id.clone(),
            client_message_id: "concurrent-to-child".into(),
            mode: SubAgentInputMode::Steer,
            native_prompt_json: r#"[{"type":"text","text":"parallel parent update"}]"#.into(),
        },
    );
    let concurrent_parent = handle.send_to_parent(
        context(),
        SendToParentRequest {
            sub_agent_id: inherited.sub_agent_id.clone(),
            client_message_id: "concurrent-to-parent".into(),
            mode: SubAgentInputMode::Steer,
            message_json: r#"{"progress":"parallel child update"}"#.into(),
        },
    );
    let (to_child, to_parent) = tokio::join!(concurrent_child, concurrent_parent);
    assert_eq!(
        to_child
            .expect("concurrent child input is accepted")
            .disposition,
        InputDisposition::ContributedToActiveWork
    );
    assert_eq!(
        to_parent
            .expect("concurrent parent input is accepted")
            .disposition,
        InputDisposition::ContributedToActiveWork
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let page = handle
                .read_events(
                    context(),
                    ReadSubAgentEventsRequest {
                        sub_agent_id: inherited.sub_agent_id.clone(),
                        after_sequence: 0,
                        limit: 100,
                    },
                )
                .await
                .expect("sub-agent events read");
            if page
                .events
                .iter()
                .any(|event| event.kind == SubAgentEventKind::NativeAcp)
                && page
                    .events
                    .iter()
                    .any(|event| event.kind == SubAgentEventKind::ChildToParent)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("event pump catches up");

    let stopped = handle
        .stop(
            context(),
            StopSubAgentRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                reason: "test complete".into(),
            },
        )
        .await
        .expect("child stops");
    assert!(stopped.accepted);
    assert_eq!(
        handle
            .status(
                context(),
                SubAgentReference {
                    sub_agent_id: inherited.sub_agent_id.clone(),
                },
            )
            .await
            .expect("stopped status reads")
            .record
            .lifecycle,
        SubAgentLifecycle::Completed
    );
    let stopped_retry = handle
        .spawn(context(), inherited_request)
        .await
        .expect("spawn retry resolves after child termination");
    assert_eq!(stopped_retry.sub_agent_id, inherited.sub_agent_id);
    assert_eq!(stopped_retry.lifecycle, SubAgentLifecycle::Completed);

    let fresh = handle
        .spawn(
            context(),
            spawn_request("fresh", SubAgentContextMode::Fresh),
        )
        .await
        .expect("fresh child starts");
    assert_eq!(fresh.context_realization, ContextRealization::FreshSession);
    let state = fake_state.lock().expect("fake state lock");
    assert!(state.opened[1].bootstrap_prompt.is_none());
}

#[tokio::test]
async fn inherited_context_fails_closed_and_restart_policy_is_accepted() {
    let fake_state = Arc::new(Mutex::new(FakeState::with_parent()));
    let host_state = SubAgentHostState::open_in_memory().expect("sub-agent state opens");
    let mut builder = CompositionBuilder::new();
    let agent = agent_host::register(&mut builder, FakeAgentHost { state: fake_state });
    let host = sub_agent_host::register(&mut builder, move |imports| {
        host_state.connect(imports.agent_host)
    });
    builder.connect(&host, &agent);
    let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
    let _composition = builder.start().expect("graph starts");

    let mut native_only = spawn_request("native-only", SubAgentContextMode::InheritParent);
    native_only.allow_portable_snapshot = false;
    assert_eq!(
        handle.spawn(context(), native_only).await,
        Err(boxology_contract::CallError::Domain(
            SubAgentHostError::PortableSnapshotForbidden
        ))
    );

    let mut restart = spawn_request("restart", SubAgentContextMode::Fresh);
    restart.crash_restart_limit = 1;
    let restart = handle
        .spawn(context(), restart)
        .await
        .expect("bounded restart policy is accepted");
    assert_eq!(restart.lifecycle, SubAgentLifecycle::Running);
}

#[tokio::test]
async fn file_backed_recovery_preserves_identity_journal_cursor_and_budget() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("sub-agents.sqlite");
    let fake_state = Arc::new(Mutex::new(FakeState::with_parent()));
    let (record, before_status) = {
        let host_state = SubAgentHostState::open(&database).expect("sub-agent state opens");
        let mut builder = CompositionBuilder::new();
        let agent = agent_host::register(
            &mut builder,
            FakeAgentHost {
                state: fake_state.clone(),
            },
        );
        let host = sub_agent_host::register(&mut builder, move |imports| {
            host_state.connect(imports.agent_host)
        });
        builder.connect(&host, &agent);
        let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
        let _composition = builder.start().expect("first graph starts");
        let mut request = spawn_request("recoverable", SubAgentContextMode::Fresh);
        request.crash_restart_limit = 1;
        let record = handle
            .spawn(context(), request)
            .await
            .expect("recoverable child starts");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = handle
                    .read_events(
                        context(),
                        ReadSubAgentEventsRequest {
                            sub_agent_id: record.sub_agent_id.clone(),
                            after_sequence: 0,
                            limit: 100,
                        },
                    )
                    .await
                    .expect("events read before restart");
                if page
                    .events
                    .iter()
                    .any(|event| event.kind == SubAgentEventKind::NativeAcp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("initial child cursor catches up");
        let status = handle
            .status(
                context(),
                SubAgentReference {
                    sub_agent_id: record.sub_agent_id.clone(),
                },
            )
            .await
            .expect("status reads before restart");
        assert_eq!(status.restart_count, 0);
        (record, status)
    };

    {
        let host_state = SubAgentHostState::open(&database).expect("sub-agent state reopens");
        let mut builder = CompositionBuilder::new();
        let agent = agent_host::register(
            &mut builder,
            FakeAgentHost {
                state: fake_state.clone(),
            },
        );
        let host = sub_agent_host::register(&mut builder, move |imports| {
            host_state.connect(imports.agent_host)
        });
        builder.connect(&host, &agent);
        let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
        let _composition = builder.start().expect("restarted graph starts");
        assert_eq!(
            handle
                .status(
                    context(),
                    SubAgentReference {
                        sub_agent_id: record.sub_agent_id.clone(),
                    },
                )
                .await
                .expect("failed child remains inspectable")
                .record
                .lifecycle,
            SubAgentLifecycle::Failed
        );
        let report = handle
            .recover(context(), RecoverSubAgentsRequest {})
            .await
            .expect("exact child session resumes");
        assert_eq!(report.recoveries.len(), 1);
        assert_eq!(report.recoveries[0].sub_agent_id, record.sub_agent_id);
        assert_eq!(
            report.recoveries[0].child_session_id,
            record.child_session_id
        );
        assert_eq!(
            report.recoveries[0].disposition,
            SubAgentRecoveryDisposition::Resumed
        );
        let recovered = handle
            .status(
                context(),
                SubAgentReference {
                    sub_agent_id: record.sub_agent_id.clone(),
                },
            )
            .await
            .expect("recovered status reads");
        assert_eq!(recovered.record.parent_session_id, record.parent_session_id);
        assert_eq!(recovered.record.child_session_id, record.child_session_id);
        assert_eq!(
            recovered.record.native_child_session_id,
            record.native_child_session_id
        );
        assert_eq!(recovered.restart_count, 1);
        assert_eq!(recovered.last_sequence, before_status.last_sequence + 1);
        assert_eq!(recovered.pending_parent_to_child, 0);
        assert_eq!(recovered.pending_child_to_parent, 0);
        assert!(
            matches!(
                recovered.record.lifecycle,
                SubAgentLifecycle::Idle | SubAgentLifecycle::Running
            ),
            "recovered child must be available"
        );
        {
            let state = fake_state.lock().expect("fake state lock");
            assert_eq!(state.opened.len(), 1, "recovery must not open a child");
            assert_eq!(state.next_run, 1, "initial task must not replay");
            assert_eq!(
                state.resume_attempts.as_slice(),
                std::slice::from_ref(&record.child_session_id)
            );
        }
        assert!(
            handle
                .recover(context(), RecoverSubAgentsRequest {})
                .await
                .expect("recovery retry is idempotent")
                .recoveries
                .is_empty()
        );
        handle
            .send_to_child(
                context(),
                SendToChildRequest {
                    sub_agent_id: record.sub_agent_id.clone(),
                    client_message_id: "after-recovery".into(),
                    mode: SubAgentInputMode::Queue,
                    native_prompt_json: r#"[{"type":"text","text":"continue"}]"#.into(),
                },
            )
            .await
            .expect("recovered child accepts new work");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = handle
                    .read_events(
                        context(),
                        ReadSubAgentEventsRequest {
                            sub_agent_id: record.sub_agent_id.clone(),
                            after_sequence: before_status.last_sequence,
                            limit: 100,
                        },
                    )
                    .await
                    .expect("continued events read");
                if page.events.iter().any(|event| {
                    event.kind == SubAgentEventKind::NativeAcp
                        && event.payload_json.contains("\"childSequence\":2")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("event pump continues from the persisted child cursor");
    }

    let attempts_before_budget = fake_state
        .lock()
        .expect("fake state lock")
        .resume_attempts
        .len();
    let host_state = SubAgentHostState::open(&database).expect("sub-agent state reopens again");
    let mut builder = CompositionBuilder::new();
    let agent = agent_host::register(
        &mut builder,
        FakeAgentHost {
            state: fake_state.clone(),
        },
    );
    let host = sub_agent_host::register(&mut builder, move |imports| {
        host_state.connect(imports.agent_host)
    });
    builder.connect(&host, &agent);
    let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
    let _composition = builder.start().expect("budget graph starts");
    let report = handle
        .recover(context(), RecoverSubAgentsRequest {})
        .await
        .expect("exhausted budget is reported");
    assert_eq!(
        report.recoveries[0].disposition,
        SubAgentRecoveryDisposition::RestartBudgetExhausted
    );
    assert_eq!(
        fake_state
            .lock()
            .expect("fake state lock")
            .resume_attempts
            .len(),
        attempts_before_budget,
        "budget exhaustion must not touch agent-host"
    );
}

async fn assert_recovery_failure(
    client_id: &str,
    restart_limit: u64,
    mode: FakeResumeMode,
    remove_parent: bool,
    expected: SubAgentRecoveryDisposition,
    expected_attempts: usize,
    expected_restart_count: u64,
) {
    let directory = tempfile::tempdir().expect("temporary directory");
    let database = directory.path().join("sub-agents.sqlite");
    let fake_state = Arc::new(Mutex::new(FakeState::with_parent()));
    let record = {
        let host_state = SubAgentHostState::open(&database).expect("sub-agent state opens");
        let mut builder = CompositionBuilder::new();
        let agent = agent_host::register(
            &mut builder,
            FakeAgentHost {
                state: fake_state.clone(),
            },
        );
        let host = sub_agent_host::register(&mut builder, move |imports| {
            host_state.connect(imports.agent_host)
        });
        builder.connect(&host, &agent);
        let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
        let _composition = builder.start().expect("first graph starts");
        let mut request = spawn_request(client_id, SubAgentContextMode::Fresh);
        request.crash_restart_limit = restart_limit;
        handle
            .spawn(context(), request)
            .await
            .expect("child starts")
    };
    {
        let mut state = fake_state.lock().expect("fake state lock");
        state.resume_mode = mode;
        if remove_parent {
            state.sessions.remove("parent-1");
        }
    }
    let host_state = SubAgentHostState::open(&database).expect("sub-agent state reopens");
    let mut builder = CompositionBuilder::new();
    let agent = agent_host::register(
        &mut builder,
        FakeAgentHost {
            state: fake_state.clone(),
        },
    );
    let host = sub_agent_host::register(&mut builder, move |imports| {
        host_state.connect(imports.agent_host)
    });
    builder.connect(&host, &agent);
    let handle = builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&host);
    let _composition = builder.start().expect("recovery graph starts");
    let report = handle
        .recover(context(), RecoverSubAgentsRequest {})
        .await
        .expect("failure is reported without aborting recovery");
    assert_eq!(report.recoveries.len(), 1);
    assert_eq!(report.recoveries[0].disposition, expected);
    let status = handle
        .status(
            context(),
            SubAgentReference {
                sub_agent_id: record.sub_agent_id,
            },
        )
        .await
        .expect("failed record remains inspectable");
    assert_eq!(status.record.lifecycle, SubAgentLifecycle::Failed);
    assert_eq!(status.restart_count, expected_restart_count);
    assert!(status.last_error.is_some());
    let state = fake_state.lock().expect("fake state lock");
    assert_eq!(state.resume_attempts.len(), expected_attempts);
    assert_eq!(
        state.opened.len(),
        1,
        "failure must never open a replacement"
    );
}

#[tokio::test]
async fn recovery_failures_are_explicit_and_never_open_replacements() {
    assert_recovery_failure(
        "disabled",
        0,
        FakeResumeMode::Success,
        false,
        SubAgentRecoveryDisposition::RecoveryDisabled,
        0,
        0,
    )
    .await;
    assert_recovery_failure(
        "unavailable",
        1,
        FakeResumeMode::Unavailable,
        false,
        SubAgentRecoveryDisposition::SessionUnavailable,
        1,
        1,
    )
    .await;
    assert_recovery_failure(
        "identity",
        1,
        FakeResumeMode::IdentityMismatch,
        false,
        SubAgentRecoveryDisposition::IdentityMismatch,
        1,
        1,
    )
    .await;
    assert_recovery_failure(
        "authority",
        1,
        FakeResumeMode::AuthorityFailure,
        false,
        SubAgentRecoveryDisposition::Failed,
        1,
        1,
    )
    .await;
    assert_recovery_failure(
        "parent",
        1,
        FakeResumeMode::Success,
        true,
        SubAgentRecoveryDisposition::ParentUnavailable,
        0,
        0,
    )
    .await;
}
