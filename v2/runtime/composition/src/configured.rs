use std::{fmt, path::Path, time::Duration};

use agent_host_contract::{OpenSessionRequest, SessionReference};
use boxology_contract::{CallContext, CallError, Caller, CancelToken, TraceContext};
use native_channel_contract::{
    BindChannelRequest, BindingReference, ChannelBinding, ChannelLifecycle, LocateBindingRequest,
    ReplaceSessionRequest,
};
use tokio::{sync::watch, task::JoinSet};
use turn_router_contract::{DrainLaneRequest, PutRouteRequest, RouteReference};

use crate::{
    ChannelConfig, DraftRuntime, LaneConfig, RuntimeConfig, RuntimeStartError,
    start_runtime_with_state_directory,
};

/// A configured, restored graph with one continuously draining worker per trigger lane.
pub struct ConfiguredRuntime {
    runtime: DraftRuntime,
    sessions: Vec<String>,
    shutdown: watch::Sender<bool>,
    workers: JoinSet<Result<(), RuntimeRunError>>,
}

/// Failures after a configured runtime has started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRunError {
    /// The durable turn router stopped accepting drain calls.
    RouterUnavailable,
    /// A lane worker stopped before runtime shutdown.
    WorkerStopped,
    /// The operating-system shutdown signal could not be installed.
    SignalUnavailable,
    /// A live ACP session could not be closed cleanly.
    AgentHostUnavailable,
}

impl fmt::Display for RuntimeRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RouterUnavailable => formatter.write_str("turn-router worker failed"),
            Self::WorkerStopped => formatter.write_str("runtime worker stopped unexpectedly"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
            Self::AgentHostUnavailable => formatter.write_str("ACP session shutdown failed"),
        }
    }
}

impl std::error::Error for RuntimeRunError {}

impl ConfiguredRuntime {
    /// Load strict JSON configuration, restore topology and start the configured lane workers.
    pub async fn start_from_path(
        config_path: impl AsRef<Path>,
        state_directory: impl AsRef<Path>,
    ) -> Result<Self, RuntimeStartError> {
        let config = RuntimeConfig::load(config_path)?;
        Self::start(config, state_directory).await
    }

    /// Validate resolved configuration, restore topology and start the configured lane workers.
    pub async fn start(
        config: RuntimeConfig,
        state_directory: impl AsRef<Path>,
    ) -> Result<Self, RuntimeStartError> {
        config.validate()?;
        let agents = config.configured_agents()?;
        let runtime = start_runtime_with_state_directory(state_directory.as_ref(), agents)?;
        let sessions = initialize_topology(&runtime, &config).await?;
        let (shutdown, receiver) = watch::channel(false);
        let mut workers = JoinSet::new();
        for lane in config.lanes {
            spawn_lane_worker(
                &mut workers,
                runtime.turn_router().clone(),
                lane,
                receiver.clone(),
            );
        }
        Ok(Self {
            runtime,
            sessions,
            shutdown,
            workers,
        })
    }

    /// Return the live Boxology graph for adapters and operator inspection.
    pub fn graph(&self) -> &DraftRuntime {
        &self.runtime
    }

    /// Run until SIGINT/SIGTERM or an unexpected worker exit, then close every ACP session.
    pub async fn run_until_signal(mut self) -> Result<(), RuntimeRunError> {
        let outcome = tokio::select! {
            signal = wait_for_shutdown_signal() => signal,
            worker = self.workers.join_next() => match worker {
                Some(Ok(Err(error))) => Err(error),
                Some(Ok(Ok(()))) | Some(Err(_)) | None => Err(RuntimeRunError::WorkerStopped),
            },
        };
        let cleanup = self.finish().await;
        outcome.and(cleanup)
    }

    /// Request graceful worker shutdown and close every opened ACP session.
    pub async fn shutdown(mut self) -> Result<(), RuntimeRunError> {
        self.finish().await
    }

    async fn finish(&mut self) -> Result<(), RuntimeRunError> {
        self.shutdown.send_replace(true);
        let mut outcome = Ok(());
        while let Some(worker) = self.workers.join_next().await {
            match worker {
                Ok(Ok(())) => {}
                Ok(Err(error)) => outcome = outcome.and(Err(error)),
                Err(_) => outcome = outcome.and(Err(RuntimeRunError::WorkerStopped)),
            }
        }
        if !close_sessions(self.runtime.agent_host(), &self.sessions).await {
            outcome = outcome.and(Err(RuntimeRunError::AgentHostUnavailable));
        }
        outcome
    }
}

#[derive(Clone, Copy)]
struct StartupHandles<'a> {
    agent_host: &'a agent_host_contract::AgentHostHandle,
    native_channel: &'a native_channel_contract::NativeChannelHandle,
    turn_router: &'a turn_router_contract::TurnRouterHandle,
}

async fn initialize_topology(
    runtime: &DraftRuntime,
    config: &RuntimeConfig,
) -> Result<Vec<String>, RuntimeStartError> {
    let handles = StartupHandles {
        agent_host: runtime.agent_host(),
        native_channel: runtime.native_channel(),
        turn_router: runtime.turn_router(),
    };
    initialize_topology_with_handles(handles, config).await
}

async fn initialize_topology_with_handles(
    handles: StartupHandles<'_>,
    config: &RuntimeConfig,
) -> Result<Vec<String>, RuntimeStartError> {
    let mut sessions = Vec::with_capacity(config.channels.len());
    for channel in &config.channels {
        let result = initialize_channel(handles, config, channel).await;
        match result {
            Ok(session_id) => sessions.push(session_id),
            Err(error) => {
                close_sessions(handles.agent_host, &sessions).await;
                return Err(error);
            }
        }
    }
    Ok(sessions)
}

async fn initialize_channel(
    handles: StartupHandles<'_>,
    config: &RuntimeConfig,
    channel: &ChannelConfig,
) -> Result<String, RuntimeStartError> {
    let metadata_json = session_metadata(channel)?;
    let session = handles
        .agent_host
        .open_session(
            call_context(),
            OpenSessionRequest {
                agent_id: channel.agent_id.clone(),
                working_directory: channel.working_directory.to_string_lossy().into_owned(),
                bootstrap_prompt: config.bootstrap_prompt(channel)?,
                metadata_json,
            },
        )
        .await
        .map_err(RuntimeStartError::OpenSession)?;
    let session_id = session.session_id;
    let result = attach_channel(handles, channel, &session_id).await;
    if result.is_err() {
        close_sessions(handles.agent_host, std::slice::from_ref(&session_id)).await;
    }
    result.map(|()| session_id)
}

async fn attach_channel(
    handles: StartupHandles<'_>,
    channel: &ChannelConfig,
    session_id: &str,
) -> Result<(), RuntimeStartError> {
    let channel_json = serde_json::to_string(&channel.native_channel)
        .map_err(RuntimeStartError::SessionMetadata)?;
    let route = resolve_route(handles, channel).await?;
    let existing = locate_binding(handles, channel, route.as_ref()).await?;
    let binding = restore_binding(handles, channel, &channel_json, session_id, existing).await?;
    handles
        .turn_router
        .put_route(
            call_context(),
            PutRouteRequest {
                target_channel_id: channel.channel_id.clone(),
                lane: channel.lane.clone(),
                binding_id: binding.binding_id,
                expected_generation: route.map(|route| route.generation),
            },
        )
        .await
        .map_err(RuntimeStartError::PutRoute)?;
    Ok(())
}

fn session_metadata(channel: &ChannelConfig) -> Result<String, RuntimeStartError> {
    serde_json::to_string(&serde_json::json!({
        "configured": channel.session_metadata,
        "crabRuntime": {
            "adapterId": channel.adapter_id,
            "channelId": channel.channel_id,
            "lane": channel.lane,
        }
    }))
    .map_err(RuntimeStartError::SessionMetadata)
}

async fn resolve_route(
    handles: StartupHandles<'_>,
    channel: &ChannelConfig,
) -> Result<Option<turn_router_contract::ChannelRoute>, RuntimeStartError> {
    match handles
        .turn_router
        .resolve_route(
            call_context(),
            RouteReference {
                target_channel_id: channel.channel_id.clone(),
            },
        )
        .await
    {
        Ok(route) => Ok(Some(route)),
        Err(CallError::Domain(turn_router_contract::TurnRouterError::UnknownRoute)) => Ok(None),
        Err(error) => Err(RuntimeStartError::ResolveRoute(error)),
    }
}

async fn locate_binding(
    handles: StartupHandles<'_>,
    channel: &ChannelConfig,
    route: Option<&turn_router_contract::ChannelRoute>,
) -> Result<Option<ChannelBinding>, RuntimeStartError> {
    if let Some(route) = route {
        match handles
            .native_channel
            .inspect_binding(
                call_context(),
                BindingReference {
                    binding_id: route.binding_id.clone(),
                },
            )
            .await
        {
            Ok(binding) if matches!(binding.lifecycle, ChannelLifecycle::Detached) => {}
            Ok(binding)
                if binding.channel_id == channel.channel_id
                    && binding.adapter_id == channel.adapter_id =>
            {
                return Ok(Some(binding));
            }
            Ok(binding) if binding.channel_id == channel.channel_id => {
                handles
                    .native_channel
                    .unbind_channel(
                        call_context(),
                        BindingReference {
                            binding_id: binding.binding_id,
                        },
                    )
                    .await
                    .map_err(RuntimeStartError::UnbindChannel)?;
            }
            Ok(_)
            | Err(CallError::Domain(native_channel_contract::NativeChannelError::UnknownBinding)) =>
                {}
            Err(error) => return Err(RuntimeStartError::InspectBinding(error)),
        }
    }
    match handles
        .native_channel
        .find_binding(
            call_context(),
            LocateBindingRequest {
                channel_id: channel.channel_id.clone(),
                adapter_id: channel.adapter_id.clone(),
            },
        )
        .await
    {
        Ok(binding) => Ok(Some(binding)),
        Err(CallError::Domain(native_channel_contract::NativeChannelError::UnknownBinding)) => {
            Ok(None)
        }
        Err(error) => Err(RuntimeStartError::FindBinding(error)),
    }
}

async fn restore_binding(
    handles: StartupHandles<'_>,
    channel: &ChannelConfig,
    channel_json: &str,
    session_id: &str,
    existing: Option<ChannelBinding>,
) -> Result<ChannelBinding, RuntimeStartError> {
    if let Some(binding) = existing {
        if binding.channel_id == channel.channel_id
            && binding.adapter_id == channel.adapter_id
            && binding.native_channel_json == channel_json
            && !matches!(binding.lifecycle, ChannelLifecycle::Detached)
        {
            return handles
                .native_channel
                .replace_session(
                    call_context(),
                    ReplaceSessionRequest {
                        binding_id: binding.binding_id,
                        expected_session_id: binding.session_id,
                        fresh_session_id: session_id.to_owned(),
                        reason: "configured runtime startup".into(),
                    },
                )
                .await
                .map_err(RuntimeStartError::ReplaceSession);
        }
        handles
            .native_channel
            .unbind_channel(
                call_context(),
                BindingReference {
                    binding_id: binding.binding_id,
                },
            )
            .await
            .map_err(RuntimeStartError::UnbindChannel)?;
    }
    handles
        .native_channel
        .bind_channel(
            call_context(),
            BindChannelRequest {
                channel_id: channel.channel_id.clone(),
                adapter_id: channel.adapter_id.clone(),
                session_id: session_id.to_owned(),
                native_channel_json: channel_json.to_owned(),
            },
        )
        .await
        .map_err(RuntimeStartError::BindChannel)
}

fn spawn_lane_worker(
    workers: &mut JoinSet<Result<(), RuntimeRunError>>,
    router: turn_router_contract::TurnRouterHandle,
    lane: LaneConfig,
    mut shutdown: watch::Receiver<bool>,
) {
    workers.spawn(async move {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let report = router
                .drain_lane(
                    call_context(),
                    DrainLaneRequest {
                        worker_id: lane.worker_id.clone(),
                        lane: lane.lane.clone(),
                        limit: lane.batch_limit,
                        lease_duration_ms: lane.lease_duration_ms,
                        retry_delay_ms: lane.retry_delay_ms,
                        max_attempts: lane.max_attempts,
                    },
                )
                .await
                .map_err(|_| RuntimeRunError::RouterUnavailable)?;
            if report.claimed < lane.batch_limit {
                tokio::select! {
                    _ = shutdown.changed() => return Ok(()),
                    () = tokio::time::sleep(Duration::from_millis(lane.poll_interval_ms)) => {}
                }
            }
        }
    });
}

async fn close_sessions(
    agent_host: &agent_host_contract::AgentHostHandle,
    session_ids: &[String],
) -> bool {
    let mut clean = true;
    for session_id in session_ids {
        if agent_host
            .close_session(
                call_context(),
                SessionReference {
                    session_id: session_id.clone(),
                },
            )
            .await
            .is_err()
        {
            clean = false;
        }
    }
    clean
}

fn call_context() -> CallContext {
    CallContext::new(
        Caller::System("crab-v2-runtime"),
        None,
        CancelToken::new(),
        TraceContext::empty(),
        None,
    )
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() -> Result<(), RuntimeRunError> {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate =
        signal(SignalKind::terminate()).map_err(|_| RuntimeRunError::SignalUnavailable)?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|_| RuntimeRunError::SignalUnavailable),
        _ = terminate.recv() => Ok(()),
    }
}

#[cfg(not(unix))]
async fn wait_for_shutdown_signal() -> Result<(), RuntimeRunError> {
    tokio::signal::ctrl_c()
        .await
        .map_err(|_| RuntimeRunError::SignalUnavailable)
}

impl fmt::Display for RuntimeStartError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::Configuration(error) => return write!(formatter, "{error}"),
            Self::AgentHost(_) => "agent-host startup failed",
            Self::BridgeHost(_) => "bridge-host startup failed",
            Self::CredentialStore(_) => "credential-store startup failed",
            Self::NativeChannel(_) => "native-channel startup failed",
            Self::SubAgentHost(_) => "sub-agent-host startup failed",
            Self::TurnRouter(_) => "turn-router startup failed",
            Self::TriggerInbox(_) => "trigger-inbox startup failed",
            Self::SessionMetadata(_) => "session metadata encoding failed",
            Self::OpenSession(_) => "ACP session startup failed",
            Self::ResolveRoute(_) => "route recovery failed",
            Self::InspectBinding(_) | Self::FindBinding(_) => "binding recovery failed",
            Self::BindChannel(_) => "channel binding failed",
            Self::ReplaceSession(_) => "channel session replacement failed",
            Self::UnbindChannel(_) => "stale binding cleanup failed",
            Self::PutRoute(_) => "route registration failed",
            Self::StateDirectory(_) => "state-directory startup failed",
            Self::Assembly(_) => "Boxology assembly failed",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for RuntimeStartError {}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        path::PathBuf,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use agent_host_implementation::{
        AcpNegotiation, AcpProtocolProfile, AgentCatalog, AgentHostError, AgentLifecycle,
        AgentSession, AuthorityAttestation, CompactionReporting, DiscoverAgentsRequest, EventPage,
        FilesystemAuthority, NetworkAuthority, OpenSessionRequest, OperationReceipt,
        PermissionAuthority, PermissionRequest, PermissionResolution, PreflightReport,
        PreflightRequest, PromptAccepted, PromptDisposition, PromptRequest, ReadEventsRequest,
        RootAuthority, RunReference, SandboxAuthority, SessionReference, SessionStatus,
        SteeringSupport, generated as agent_host,
    };
    use boxology_contract::CallContext;
    use boxology_runtime::{Composition, CompositionBuilder};
    use native_channel_contract::{BindingReference, ChannelLifecycle};
    use native_channel_implementation::{NativeChannelState, generated as native_channel};
    use serde_json::json;
    use tokio::{sync::watch, task::JoinSet};
    use trigger_inbox_contract::{
        EnqueueTrigger, TriggerMode, TriggerReference, TriggerSource, TriggerState,
    };
    use trigger_inbox_implementation::{TriggerInbox, generated as trigger_inbox};
    use turn_router_contract::RouteReference;
    use turn_router_implementation::{TurnRouterState, generated as turn_router};

    use super::{
        StartupHandles, call_context, close_sessions, initialize_topology_with_handles,
        spawn_lane_worker,
    };
    use crate::{
        AgentConfig, ChannelConfig, CommandConfig, LaneConfig, ProtocolConfig, RuntimeConfig,
    };

    #[derive(Default)]
    struct FakeState {
        next_session: u64,
        live_sessions: HashSet<String>,
        prompts: Vec<PromptRequest>,
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
            state.next_session += 1;
            let session_id = format!("session-{}", state.next_session);
            state.live_sessions.insert(session_id.clone());
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

        async fn prompt(
            &self,
            context: CallContext,
            request: PromptRequest,
        ) -> Result<PromptAccepted, AgentHostError> {
            let _ = context;
            let mut state = self.state.lock().expect("fake state lock");
            if !state.live_sessions.contains(&request.session_id) {
                return Err(AgentHostError::UnknownSession);
            }
            state.prompts.push(request.clone());
            Ok(PromptAccepted {
                session_id: request.session_id,
                run_id: format!("run-{}", state.prompts.len()),
                accepted_at_ms: 1,
                disposition: PromptDisposition::StartedForegroundWork,
            })
        }

        async fn read_events(
            &self,
            context: CallContext,
            request: ReadEventsRequest,
        ) -> Result<EventPage, AgentHostError> {
            let _ = (context, request);
            Err(AgentHostError::InvalidCursor)
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
            if !state.live_sessions.contains(&request.session_id) {
                return Err(AgentHostError::UnknownSession);
            }
            Ok(SessionStatus {
                session_id: request.session_id,
                lifecycle: AgentLifecycle::Ready,
                last_sequence: 0,
                active_run_id: None,
            })
        }

        async fn cancel_run(
            &self,
            context: CallContext,
            request: RunReference,
        ) -> Result<OperationReceipt, AgentHostError> {
            let _ = (context, request);
            Err(AgentHostError::UnknownRun)
        }

        async fn close_session(
            &self,
            context: CallContext,
            request: SessionReference,
        ) -> Result<OperationReceipt, AgentHostError> {
            let _ = context;
            if !self
                .state
                .lock()
                .expect("fake state lock")
                .live_sessions
                .remove(&request.session_id)
            {
                return Err(AgentHostError::UnknownSession);
            }
            Ok(OperationReceipt {
                accepted: true,
                recorded_at_ms: 1,
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

    struct TestGraph {
        _composition: Composition,
        agent_host: agent_host_contract::AgentHostHandle,
        native_channel: native_channel_contract::NativeChannelHandle,
        turn_router: turn_router_contract::TurnRouterHandle,
        trigger_inbox: trigger_inbox_contract::TriggerInboxHandle,
    }

    impl TestGraph {
        fn handles(&self) -> StartupHandles<'_> {
            StartupHandles {
                agent_host: &self.agent_host,
                native_channel: &self.native_channel,
                turn_router: &self.turn_router,
            }
        }
    }

    fn graph(path: &std::path::Path, state: Arc<Mutex<FakeState>>) -> TestGraph {
        let mut builder = CompositionBuilder::new();
        let agent = agent_host::register(&mut builder, FakeAgentHost { state });
        let agent_host = builder.handle::<agent_host_contract::AgentHostHandle>(&agent);

        let channel_state =
            NativeChannelState::open(path.join("native.sqlite")).expect("channel store opens");
        let channel = native_channel::register(&mut builder, move |imports| {
            channel_state.connect(imports.agent_host)
        });
        builder.connect(&channel, &agent);
        let native_channel =
            builder.handle::<native_channel_contract::NativeChannelHandle>(&channel);

        let inbox_store =
            TriggerInbox::open(path.join("inbox.sqlite")).expect("trigger store opens");
        let inbox = trigger_inbox::register(&mut builder, inbox_store);
        let trigger_inbox = builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&inbox);
        let router_state =
            TurnRouterState::open(path.join("router.sqlite")).expect("route store opens");
        let router = turn_router::register(&mut builder, move |imports| {
            router_state.connect(imports.trigger_inbox, imports.native_channel)
        });
        builder.connect(&router, &inbox);
        builder.connect(&router, &channel);
        let turn_router = builder.handle::<turn_router_contract::TurnRouterHandle>(&router);
        let composition = builder.start().expect("test graph starts");
        TestGraph {
            _composition: composition,
            agent_host,
            native_channel,
            turn_router,
            trigger_inbox,
        }
    }

    fn config(working_directory: PathBuf) -> RuntimeConfig {
        RuntimeConfig {
            schema: 1,
            agents: vec![AgentConfig {
                agent_id: "fake".into(),
                display_name: "Fake".into(),
                executable: "unused".into(),
                arguments: Vec::new(),
                environment_from: Vec::new(),
                protocol: ProtocolConfig::V2,
                authority_probe: CommandConfig {
                    executable: "unused".into(),
                    arguments: Vec::new(),
                    environment_from: Vec::new(),
                },
            }],
            channels: vec![ChannelConfig {
                channel_id: "primary".into(),
                adapter_id: "native-ui".into(),
                agent_id: "fake".into(),
                working_directory,
                bootstrap_prompt_file: None,
                native_channel: json!({"title": "Jim"}),
                session_metadata: json!({}),
                lane: "primary".into(),
            }],
            lanes: vec![LaneConfig {
                lane: "primary".into(),
                worker_id: "test-worker".into(),
                batch_limit: 16,
                lease_duration_ms: 30_000,
                retry_delay_ms: 1_000,
                max_attempts: 3,
                poll_interval_ms: 10,
            }],
        }
    }

    #[tokio::test]
    async fn restart_reuses_binding_and_replaces_only_the_acp_session() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let config = config(directory.path().to_path_buf());
        let first = graph(directory.path(), state.clone());
        let first_sessions = initialize_topology_with_handles(first.handles(), &config)
            .await
            .expect("first topology starts");
        let first_route = first
            .turn_router
            .resolve_route(
                call_context(),
                RouteReference {
                    target_channel_id: "primary".into(),
                },
            )
            .await
            .expect("first route exists");
        assert!(close_sessions(&first.agent_host, &first_sessions).await);
        drop(first);

        let restarted = graph(directory.path(), state);
        let restarted_sessions = initialize_topology_with_handles(restarted.handles(), &config)
            .await
            .expect("restarted topology recovers");
        let restarted_route = restarted
            .turn_router
            .resolve_route(
                call_context(),
                RouteReference {
                    target_channel_id: "primary".into(),
                },
            )
            .await
            .expect("restarted route exists");
        let binding = restarted
            .native_channel
            .inspect_binding(
                call_context(),
                BindingReference {
                    binding_id: restarted_route.binding_id.clone(),
                },
            )
            .await
            .expect("recovered binding is inspectable");

        assert_eq!(restarted_route.binding_id, first_route.binding_id);
        assert_eq!(restarted_route.generation, first_route.generation);
        assert_eq!(binding.lifecycle, ChannelLifecycle::Attached);
        assert_eq!(binding.session_id, restarted_sessions[0]);
        assert_ne!(binding.session_id, first_sessions[0]);
        assert!(close_sessions(&restarted.agent_host, &restarted_sessions).await);
    }

    #[tokio::test]
    async fn lane_worker_continuously_routes_and_settles_triggers() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let config = config(directory.path().to_path_buf());
        let graph = graph(directory.path(), state.clone());
        let sessions = initialize_topology_with_handles(graph.handles(), &config)
            .await
            .expect("topology starts");
        let receipt = graph
            .trigger_inbox
            .enqueue(
                call_context(),
                EnqueueTrigger {
                    source: TriggerSource::Operator,
                    source_id: "runtime-test".into(),
                    deduplication_key: "worker-turn-1".into(),
                    target_channel_id: "primary".into(),
                    lane: "primary".into(),
                    mode: TriggerMode::Queue,
                    not_before_ms: 0,
                    message_json: r#"{"text":"hello"}"#.into(),
                    attachments: Vec::new(),
                },
            )
            .await
            .expect("trigger enqueues");
        let (shutdown, receiver) = watch::channel(false);
        let mut workers = JoinSet::new();
        spawn_lane_worker(
            &mut workers,
            graph.turn_router.clone(),
            config.lanes[0].clone(),
            receiver,
        );

        let mut completed = false;
        for _ in 0..100 {
            let record = graph
                .trigger_inbox
                .inspect(
                    call_context(),
                    TriggerReference {
                        trigger_id: receipt.trigger_id.clone(),
                    },
                )
                .await
                .expect("trigger remains inspectable");
            if record.state == TriggerState::Completed {
                completed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        shutdown.send_replace(true);
        assert_eq!(
            workers
                .join_next()
                .await
                .expect("worker exits")
                .expect("worker does not panic"),
            Ok(())
        );
        assert!(completed, "worker did not settle the trigger");
        {
            let state = state.lock().expect("fake state lock");
            assert_eq!(state.prompts.len(), 1);
            assert!(state.prompts[0].native_prompt_json.contains("hello"));
        }
        assert!(close_sessions(&graph.agent_host, &sessions).await);
    }
}
