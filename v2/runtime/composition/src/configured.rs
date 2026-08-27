use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::Path,
    time::Duration,
};

use agent_host_contract::{DetachSessionsRequest, SessionReference};
use boxology_contract::{CallContext, CallError, Caller, CancelToken, TraceContext};
use bridge_host_contract::{
    BridgeHostError, BridgeReference, BridgeSpec, ListBridgesRequest, ReplaceBridgeRequest,
};
use channel_gateway_contract::{AttachChannelRequest, ChannelAttachmentDisposition};
use native_channel_contract::{BindingReference, ChannelLifecycle};
use sub_agent_host_contract::{RecoverSubAgentsRequest, SubAgentRecoveryDisposition};
use tokio::{sync::watch, task::JoinSet};
use turn_router_contract::{DrainLaneRequest, PutRouteRequest, RouteReference};

use crate::{
    ChannelConfig, ChannelIpcPaths, DraftRuntime, LaneConfig, RuntimeConfig, RuntimeStartError,
    channel_ipc::ChannelIpcServer, start_runtime_with_state_directory,
};

/// A configured, restored graph with one continuously draining worker per trigger lane.
pub struct ConfiguredRuntime {
    runtime: DraftRuntime,
    bridges: Vec<String>,
    channel_ipc: ChannelIpcServer,
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
    /// A live ACP session could not be detached cleanly.
    AgentHostUnavailable,
    /// A live bridge package could not be suspended cleanly.
    BridgeHostUnavailable,
    /// The local capability endpoint failed or could not shut down cleanly.
    ChannelIpcUnavailable,
}

impl fmt::Display for RuntimeRunError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RouterUnavailable => formatter.write_str("turn-router worker failed"),
            Self::WorkerStopped => formatter.write_str("runtime worker stopped unexpectedly"),
            Self::SignalUnavailable => formatter.write_str("shutdown signal is unavailable"),
            Self::AgentHostUnavailable => formatter.write_str("ACP session detach failed"),
            Self::BridgeHostUnavailable => formatter.write_str("bridge shutdown failed"),
            Self::ChannelIpcUnavailable => formatter.write_str("local IPC failed"),
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
        let state_directory = state_directory.as_ref();
        config.validate()?;
        let agents = config.configured_agents()?;
        let runtime = start_runtime_with_state_directory(state_directory, agents)?;
        let paths = ChannelIpcPaths::for_state_directory(state_directory)
            .map_err(|error| RuntimeStartError::ChannelIpc(error.into()))?;
        let bridges = initialize_bridges(runtime.bridge_host(), &config).await?;
        let mut sessions = match initialize_topology(&runtime, &config).await {
            Ok(sessions) => sessions,
            Err(error) => {
                suspend_bridges(runtime.bridge_host(), &bridges).await;
                return Err(error);
            }
        };
        match recover_sub_agents(runtime.sub_agent_host()).await {
            Ok(recovered) => sessions.extend(recovered),
            Err(error) => {
                suspend_bridges(runtime.bridge_host(), &bridges).await;
                close_sessions(runtime.agent_host(), &sessions).await;
                return Err(error);
            }
        }
        let channel_ipc = match ChannelIpcServer::start(
            paths,
            runtime.agent_host().clone(),
            runtime.channel_gateway().clone(),
            runtime.native_channel().clone(),
            runtime.bridge_host().clone(),
            runtime.trigger_inbox().clone(),
            runtime.sub_agent_host().clone(),
        )
        .await
        {
            Ok(server) => server,
            Err(error) => {
                suspend_bridges(runtime.bridge_host(), &bridges).await;
                close_sessions(runtime.agent_host(), &sessions).await;
                return Err(RuntimeStartError::ChannelIpc(error));
            }
        };
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
            bridges,
            channel_ipc,
            shutdown,
            workers,
        })
    }

    /// Return the live Boxology graph for adapters and operator inspection.
    pub fn graph(&self) -> &DraftRuntime {
        &self.runtime
    }

    /// Run until SIGINT/SIGTERM or an unexpected worker exit, then detach every ACP session.
    pub async fn run_until_signal(mut self) -> Result<(), RuntimeRunError> {
        let outcome = tokio::select! {
            signal = wait_for_shutdown_signal() => signal,
            worker = self.workers.join_next() => match worker {
                Some(Ok(Err(error))) => Err(error),
                Some(Ok(Ok(()))) | Some(Err(_)) | None => Err(RuntimeRunError::WorkerStopped),
            },
            () = self.channel_ipc.wait_for_failure() => Err(RuntimeRunError::ChannelIpcUnavailable),
        };
        let cleanup = self.finish().await;
        outcome.and(cleanup)
    }

    /// Request graceful worker shutdown and detach every live ACP session.
    pub async fn shutdown(mut self) -> Result<(), RuntimeRunError> {
        self.finish().await
    }

    async fn finish(&mut self) -> Result<(), RuntimeRunError> {
        let mut outcome = if self.channel_ipc.shutdown().await.is_ok() {
            Ok(())
        } else {
            Err(RuntimeRunError::ChannelIpcUnavailable)
        };
        self.shutdown.send_replace(true);
        while let Some(worker) = self.workers.join_next().await {
            match worker {
                Ok(Ok(())) => {}
                Ok(Err(error)) => outcome = outcome.and(Err(error)),
                Err(_) => outcome = outcome.and(Err(RuntimeRunError::WorkerStopped)),
            }
        }
        if !suspend_bridges(self.runtime.bridge_host(), &self.bridges).await {
            outcome = outcome.and(Err(RuntimeRunError::BridgeHostUnavailable));
        }
        if !detach_sessions(self.runtime.agent_host()).await {
            outcome = outcome.and(Err(RuntimeRunError::AgentHostUnavailable));
        }
        outcome
    }
}

async fn initialize_bridges(
    bridge_host: &bridge_host_contract::BridgeHostHandle,
    config: &RuntimeConfig,
) -> Result<Vec<String>, RuntimeStartError> {
    let specs = config.bridge_specs()?;
    let catalog = bridge_host
        .list_bridges(call_context(), ListBridgesRequest {})
        .await
        .map_err(RuntimeStartError::ListBridges)?;
    let persisted = catalog
        .bridges
        .into_iter()
        .map(|record| (record.bridge_id.clone(), record))
        .collect::<HashMap<_, _>>();
    let configured = specs
        .iter()
        .map(|spec| spec.bridge_id.clone())
        .collect::<HashSet<_>>();
    let mut initialized = Vec::with_capacity(specs.len());
    for spec in specs {
        let result = reconcile_bridge_spec(bridge_host, &persisted, spec).await;
        match result {
            Ok(bridge_id) => initialized.push(bridge_id),
            Err(error) => {
                suspend_bridges(bridge_host, &initialized).await;
                return Err(error);
            }
        }
    }
    for (bridge_id, record) in persisted {
        if !configured.contains(&bridge_id)
            && record.desired_running
            && let Err(error) = bridge_host
                .stop_bridge(call_context(), BridgeReference { bridge_id })
                .await
        {
            suspend_bridges(bridge_host, &initialized).await;
            return Err(RuntimeStartError::StopBridge(error));
        }
    }
    Ok(initialized)
}

async fn reconcile_bridge_spec(
    bridge_host: &bridge_host_contract::BridgeHostHandle,
    persisted: &HashMap<String, bridge_host_contract::BridgeRecord>,
    spec: BridgeSpec,
) -> Result<String, RuntimeStartError> {
    let bridge_id = spec.bridge_id.clone();
    match bridge_host
        .register_bridge(call_context(), spec.clone())
        .await
    {
        Ok(_) => Ok(bridge_id),
        Err(CallError::Domain(BridgeHostError::DuplicateBridgeConflict)) => {
            let generation = persisted
                .get(&bridge_id)
                .ok_or({
                    RuntimeStartError::RegisterBridge(CallError::Domain(
                        BridgeHostError::DuplicateBridgeConflict,
                    ))
                })?
                .generation;
            bridge_host
                .replace_bridge(
                    call_context(),
                    ReplaceBridgeRequest {
                        expected_generation: generation,
                        spec,
                    },
                )
                .await
                .map_err(RuntimeStartError::ReplaceBridge)?;
            Ok(bridge_id)
        }
        Err(error) => Err(RuntimeStartError::RegisterBridge(error)),
    }
}

async fn suspend_bridges(
    bridge_host: &bridge_host_contract::BridgeHostHandle,
    bridge_ids: &[String],
) -> bool {
    let mut clean = true;
    for bridge_id in bridge_ids {
        if bridge_host
            .suspend_bridge(
                call_context(),
                BridgeReference {
                    bridge_id: bridge_id.clone(),
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

async fn recover_sub_agents(
    sub_agent_host: &sub_agent_host_contract::SubAgentHostHandle,
) -> Result<Vec<String>, RuntimeStartError> {
    let report = sub_agent_host
        .recover(call_context(), RecoverSubAgentsRequest {})
        .await
        .map_err(RuntimeStartError::RecoverSubAgents)?;
    Ok(report
        .recoveries
        .into_iter()
        .filter(|recovery| matches!(recovery.disposition, SubAgentRecoveryDisposition::Resumed))
        .map(|recovery| recovery.child_session_id)
        .collect())
}

#[derive(Clone, Copy)]
struct StartupHandles<'a> {
    agent_host: &'a agent_host_contract::AgentHostHandle,
    channel_gateway: &'a channel_gateway_contract::ChannelGatewayHandle,
    native_channel: &'a native_channel_contract::NativeChannelHandle,
    turn_router: &'a turn_router_contract::TurnRouterHandle,
}

async fn initialize_topology(
    runtime: &DraftRuntime,
    config: &RuntimeConfig,
) -> Result<Vec<String>, RuntimeStartError> {
    let handles = StartupHandles {
        agent_host: runtime.agent_host(),
        channel_gateway: runtime.channel_gateway(),
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
    let route = resolve_route(handles, channel).await?;
    cleanup_stale_route_binding(handles, channel, route.as_ref()).await?;
    let native_channel_json = serde_json::to_string(&channel.native_channel)
        .map_err(RuntimeStartError::SessionMetadata)?;
    let attachment = handles
        .channel_gateway
        .attach_channel(
            call_context(),
            AttachChannelRequest {
                channel_id: channel.channel_id.clone(),
                adapter_id: channel.adapter_id.clone(),
                agent_id: channel.agent_id.clone(),
                working_directory: channel.working_directory.to_string_lossy().into_owned(),
                bootstrap_prompt: config.bootstrap_prompt(channel)?,
                session_metadata_json: session_metadata(channel)?,
                native_channel_json,
            },
        )
        .await
        .map_err(RuntimeStartError::AttachChannel)?;
    let route_result = handles
        .turn_router
        .put_route(
            call_context(),
            PutRouteRequest {
                target_channel_id: channel.channel_id.clone(),
                lane: channel.lane.clone(),
                binding_id: attachment.binding_id.clone(),
                expected_generation: route.map(|route| route.generation),
            },
        )
        .await;
    if let Err(error) = route_result {
        if matches!(
            attachment.disposition,
            ChannelAttachmentDisposition::Created
        ) {
            let _ = handles
                .native_channel
                .unbind_channel(
                    call_context(),
                    BindingReference {
                        binding_id: attachment.binding_id,
                    },
                )
                .await;
        }
        if !matches!(
            attachment.disposition,
            ChannelAttachmentDisposition::ReusedLiveSession
        ) {
            close_sessions(
                handles.agent_host,
                std::slice::from_ref(&attachment.session_id),
            )
            .await;
        }
        return Err(RuntimeStartError::PutRoute(error));
    }
    Ok(attachment.session_id)
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

async fn cleanup_stale_route_binding(
    handles: StartupHandles<'_>,
    channel: &ChannelConfig,
    route: Option<&turn_router_contract::ChannelRoute>,
) -> Result<(), RuntimeStartError> {
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
                return Ok(());
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
    Ok(())
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

async fn detach_sessions(agent_host: &agent_host_contract::AgentHostHandle) -> bool {
    match agent_host
        .detach_sessions(call_context(), DetachSessionsRequest {})
        .await
    {
        Ok(report) => report.failed_session_ids.is_empty(),
        Err(_) => false,
    }
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
            Self::AttachChannel(_) => "native channel attachment failed",
            Self::ResolveRoute(_) => "route recovery failed",
            Self::InspectBinding(_) => "binding recovery failed",
            Self::UnbindChannel(_) => "stale binding cleanup failed",
            Self::PutRoute(_) => "route registration failed",
            Self::ListBridges(_) => "bridge catalog recovery failed",
            Self::RegisterBridge(_) => "bridge registration failed",
            Self::ReplaceBridge(_) => "bridge replacement failed",
            Self::StopBridge(_) => "stale bridge cleanup failed",
            Self::RecoverSubAgents(_) => "sub-agent recovery failed",
            Self::ChannelIpc(_) => "local IPC startup failed",
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
        collections::{HashMap, HashSet},
        os::unix::fs::PermissionsExt as _,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use agent_client_protocol::{
        Client, UntypedMessage,
        schema::{
            ProtocolVersion,
            v1::{
                AuthenticateRequest, CancelNotification, ContentBlock, InitializeRequest,
                LoadSessionRequest, NewSessionRequest, PromptRequest as AcpPromptRequest,
                StopReason, TextContent,
            },
        },
    };
    use agent_host_implementation::{
        AcpEvent, AcpEventDirection, AcpEventKind, AcpNegotiation, AcpProtocolProfile,
        AgentCatalog, AgentHostError, AgentLifecycle, AgentSession, AuthorityAttestation,
        CompactionReporting, DetachSessionsReport, DetachSessionsRequest, DiscoverAgentsRequest,
        EventPage, FilesystemAuthority, NetworkAuthority, OpenSessionRequest, OperationReceipt,
        PermissionAuthority, PermissionRequest, PermissionResolution, PreflightReport,
        PreflightRequest, PromptAccepted, PromptDisposition, PromptRequest, ReadEventsRequest,
        ResumeSessionRequest, RootAuthority, RunReference, SandboxAuthority, SessionReference,
        SessionStatus, SteeringSupport, generated as agent_host,
    };
    use boxology_contract::{CallContext, CallError};
    use boxology_runtime::{Composition, CompositionBuilder};
    use bridge_host_contract::{
        AuthenticationMethod as ContractAuthenticationMethod, BeginAuthenticationRequest,
        BridgeLifecycle, BridgeReference, CredentialLifecycle, DeliveryLifecycle,
        DeliveryReference, ListBridgesRequest, ReconcileBridgeRequest, ReplaceBridgeRequest,
        SubmitAuthenticationRequest,
    };
    use bridge_host_implementation::{
        AuthenticationMethod, BridgeCredentialSink, BridgeHostState, BridgeInboundSink,
        BridgeOutbound, BridgePackage, BridgePackageError, BridgePackageFactory, BridgeSpec,
        InMemoryCredentialStore, PackageChallenge, PackageCredential, PackageCredentialValidation,
        PackageDelivery, PackageHealth, generated as bridge_host,
    };
    use channel_gateway_contract::{
        AttachChannelRequest, ChannelAttachmentDisposition, ChannelGatewayError,
    };
    use channel_gateway_implementation::{ChannelGateway, generated as channel_gateway};
    use native_channel_contract::{BindingReference, ChannelLifecycle};
    use native_channel_implementation::{NativeChannelState, generated as native_channel};
    use serde_json::json;
    use sub_agent_host_contract::{
        ContextRealization, InputDisposition, ReadSubAgentEventsRequest, SendToChildRequest,
        SendToParentRequest, SpawnSubAgentRequest, StopSubAgentRequest, SubAgentContextMode,
        SubAgentEventKind, SubAgentInputMode, SubAgentLifecycle, SubAgentReference,
    };
    use sub_agent_host_implementation::{SubAgentHostState, generated as sub_agent_host};
    use tokio::{sync::watch, task::JoinSet};
    use trigger_inbox_contract::{
        EnqueueTrigger, TriggerMode, TriggerReference, TriggerSource, TriggerState,
    };
    use trigger_inbox_implementation::{TriggerInbox, generated as trigger_inbox};
    use turn_router_contract::RouteReference;
    use turn_router_implementation::{TurnRouterState, generated as turn_router};

    use super::{
        StartupHandles, call_context, close_sessions, detach_sessions, initialize_bridges,
        initialize_topology_with_handles, recover_sub_agents, session_metadata, spawn_lane_worker,
        suspend_bridges,
    };
    use crate::{
        AcpChannelOptions, AgentConfig, BridgeAuthenticationConfig, BridgeConfig,
        BridgeIngressConfig, ChannelConfig, ChannelIpcClient, ChannelIpcClientError,
        ChannelIpcPaths, ChannelIpcStartupError, CommandConfig, LaneConfig, ProtocolConfig,
        RuntimeConfig, acp_channel::AcpChannelFacade, channel_ipc::ChannelIpcServer,
    };

    #[derive(Clone, Copy, Default)]
    enum FakeResumeMode {
        Success,
        #[default]
        Unavailable,
        AuthorityFailure,
    }

    #[derive(Default)]
    struct FakeState {
        next_session: u64,
        live_sessions: HashSet<String>,
        resume_mode: FakeResumeMode,
        resume_attempts: Vec<String>,
        prompts: Vec<PromptRequest>,
        events: Vec<AcpEvent>,
        active_runs: HashMap<String, String>,
    }

    struct FakeAgentHost {
        state: Arc<Mutex<FakeState>>,
    }

    #[derive(Default)]
    struct FakeBridgeProcesses {
        launches: AtomicUsize,
        stops: AtomicUsize,
    }

    struct FakeBridgePackage {
        processes: Arc<FakeBridgeProcesses>,
    }

    #[async_trait::async_trait]
    impl BridgePackage for FakeBridgePackage {
        async fn health(
            &self,
            credential_json: Option<&str>,
        ) -> Result<PackageHealth, BridgePackageError> {
            Ok(PackageHealth {
                process_alive: true,
                service_connected: true,
                can_receive: true,
                can_send: true,
                credential_valid: credential_json.is_some(),
                detail_json: "{}".into(),
            })
        }

        async fn begin_authentication(
            &self,
            method: Option<&AuthenticationMethod>,
            context_json: &str,
        ) -> Result<PackageChallenge, BridgePackageError> {
            assert!(
                serde_json::from_str::<serde_json::Value>(context_json)
                    .is_ok_and(|value| value.is_object())
            );
            Ok(PackageChallenge {
                method: method.cloned().unwrap_or(AuthenticationMethod::QrCode),
                expires_at_ms: None,
                presentation_json: r#"{"kind":"fixture","code":"1234-5678"}"#.into(),
            })
        }

        async fn submit_authentication(
            &self,
            challenge_id: &str,
            response_json: &str,
        ) -> Result<PackageCredential, BridgePackageError> {
            assert!(!challenge_id.is_empty());
            assert!(
                serde_json::from_str::<serde_json::Value>(response_json)
                    .is_ok_and(|value| value.is_object())
            );
            Ok(PackageCredential {
                secret_json: r#"{"fixtureSecret":"never-cross-ipc"}"#.into(),
                expires_at_ms: None,
                account_hint: Some("fixture-account".into()),
                detail_json: r#"{"paired":true}"#.into(),
            })
        }

        async fn validate_credentials(
            &self,
            credential_json: &str,
        ) -> Result<PackageCredentialValidation, BridgePackageError> {
            Ok(PackageCredentialValidation {
                valid: credential_json.contains("never-cross-ipc"),
                expires_at_ms: None,
                account_hint: Some("fixture-account".into()),
                detail_json: r#"{"validated":true}"#.into(),
            })
        }

        async fn credential_committed(
            &self,
            credential_json: &str,
        ) -> Result<(), BridgePackageError> {
            assert!(credential_json.contains("never-cross-ipc"));
            Ok(())
        }

        async fn invalidate_credentials(
            &self,
            credential_json: &str,
        ) -> Result<(), BridgePackageError> {
            assert!(credential_json.contains("never-cross-ipc"));
            Ok(())
        }

        async fn deliver(
            &self,
            request: &BridgeOutbound,
            _credential_json: Option<&str>,
        ) -> Result<PackageDelivery, BridgePackageError> {
            Ok(PackageDelivery {
                external_delivery_id: format!("fixture:{}", request.message_id),
                detail_json: r#"{"fixture":true}"#.into(),
            })
        }

        async fn stop(&self) -> Result<(), BridgePackageError> {
            self.processes.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct FakeBridgeFactory {
        processes: Arc<FakeBridgeProcesses>,
    }

    #[async_trait::async_trait]
    impl BridgePackageFactory for FakeBridgeFactory {
        async fn launch(
            &self,
            _spec: &BridgeSpec,
            _inbound: Arc<dyn BridgeInboundSink>,
            _credentials: Arc<dyn BridgeCredentialSink>,
        ) -> Result<Arc<dyn BridgePackage>, BridgePackageError> {
            self.processes.launches.fetch_add(1, Ordering::SeqCst);
            Ok(Arc::new(FakeBridgePackage {
                processes: self.processes.clone(),
            }))
        }
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

        async fn resume_session(
            &self,
            context: CallContext,
            request: ResumeSessionRequest,
        ) -> Result<AgentSession, AgentHostError> {
            let _ = context;
            let mut state = self.state.lock().expect("fake state lock");
            state.resume_attempts.push(request.session_id.clone());
            match state.resume_mode {
                FakeResumeMode::Unavailable => Err(AgentHostError::SessionResumeUnavailable),
                FakeResumeMode::AuthorityFailure => Err(AgentHostError::AuthorityUnavailable),
                FakeResumeMode::Success => {
                    state.live_sessions.insert(request.session_id.clone());
                    Ok(AgentSession {
                        session_id: request.session_id.clone(),
                        native_session_id: format!("native-{}", request.session_id),
                        agent_id: "fake".into(),
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
            if !state.live_sessions.contains(&request.session_id) {
                return Err(AgentHostError::UnknownSession);
            }
            state.prompts.push(request.clone());
            let run_id = format!("run-{}", state.prompts.len());
            let sequence = state
                .events
                .iter()
                .filter(|event| event.session_id == request.session_id)
                .count() as u64
                + 1;
            state.events.push(AcpEvent {
                session_id: request.session_id.clone(),
                run_id: Some(run_id.clone()),
                sequence,
                observed_at_ms: sequence,
                kind: AcpEventKind::Message,
                direction: AcpEventDirection::AgentToClient,
                native_event_json: json!({
                    "jsonrpc": "2.0",
                    "method": "session/update",
                    "params": {
                        "sessionId": format!("native-{}", request.session_id),
                        "update": {
                            "sessionUpdate": "agent_message_chunk",
                            "content": {"type": "text", "text": "fixture reply"}
                        }
                    }
                })
                .to_string(),
            });
            if request.native_prompt_json.contains("hold") {
                state
                    .active_runs
                    .insert(request.session_id.clone(), run_id.clone());
            } else {
                state.events.push(AcpEvent {
                    session_id: request.session_id.clone(),
                    run_id: Some(run_id.clone()),
                    sequence: sequence + 1,
                    observed_at_ms: sequence + 1,
                    kind: AcpEventKind::RunFinished,
                    direction: AcpEventDirection::AgentToClient,
                    native_event_json: json!({
                        "jsonrpc": "2.0",
                        "id": format!("native-{run_id}"),
                        "result": {"stopReason": "end_turn"}
                    })
                    .to_string(),
                });
            }
            Ok(PromptAccepted {
                session_id: request.session_id,
                run_id,
                accepted_at_ms: 1,
                disposition: PromptDisposition::StartedForegroundWork,
            })
        }

        async fn read_events(
            &self,
            context: CallContext,
            request: ReadEventsRequest,
        ) -> Result<EventPage, AgentHostError> {
            let _ = context;
            let state = self.state.lock().expect("fake state lock");
            if !state.live_sessions.contains(&request.session_id) {
                return Err(AgentHostError::UnknownSession);
            }
            let last_sequence = state
                .events
                .iter()
                .filter(|event| event.session_id == request.session_id)
                .count() as u64;
            let events = state
                .events
                .iter()
                .filter(|event| {
                    event.session_id == request.session_id
                        && event.sequence > request.after_sequence
                })
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
            let state = self.state.lock().expect("fake state lock");
            if !state.live_sessions.contains(&request.session_id) {
                return Err(AgentHostError::UnknownSession);
            }
            let active_run_id = state.active_runs.get(&request.session_id).cloned();
            Ok(SessionStatus {
                session_id: request.session_id.clone(),
                lifecycle: if active_run_id.is_some() {
                    AgentLifecycle::Busy
                } else {
                    AgentLifecycle::Ready
                },
                last_sequence: state
                    .events
                    .iter()
                    .filter(|event| event.session_id == request.session_id)
                    .count() as u64,
                active_run_id,
            })
        }

        async fn cancel_run(
            &self,
            context: CallContext,
            request: RunReference,
        ) -> Result<OperationReceipt, AgentHostError> {
            let _ = context;
            let mut state = self.state.lock().expect("fake state lock");
            if state.active_runs.get(&request.session_id) != Some(&request.run_id) {
                return Err(AgentHostError::UnknownRun);
            }
            state.active_runs.remove(&request.session_id);
            let sequence = state
                .events
                .iter()
                .filter(|event| event.session_id == request.session_id)
                .count() as u64
                + 1;
            state.events.push(AcpEvent {
                session_id: request.session_id,
                run_id: Some(request.run_id.clone()),
                sequence,
                observed_at_ms: sequence,
                kind: AcpEventKind::RunFinished,
                direction: AcpEventDirection::AgentToClient,
                native_event_json: json!({
                    "jsonrpc": "2.0",
                    "id": format!("native-{}", request.run_id),
                    "result": {"stopReason": "cancelled"}
                })
                .to_string(),
            });
            Ok(OperationReceipt {
                accepted: true,
                recorded_at_ms: sequence,
            })
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
            self.state
                .lock()
                .expect("fake state lock")
                .active_runs
                .remove(&request.session_id);
            Ok(OperationReceipt {
                accepted: true,
                recorded_at_ms: 1,
            })
        }

        async fn detach_sessions(
            &self,
            context: CallContext,
            request: DetachSessionsRequest,
        ) -> Result<DetachSessionsReport, AgentHostError> {
            let _ = (context, request);
            let mut state = self.state.lock().expect("fake state lock");
            let mut detached_session_ids = state.live_sessions.drain().collect::<Vec<_>>();
            detached_session_ids.sort();
            state.active_runs.clear();
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

    struct TestGraph {
        _composition: Composition,
        agent_host: agent_host_contract::AgentHostHandle,
        bridge_host: bridge_host_contract::BridgeHostHandle,
        bridge_processes: Arc<FakeBridgeProcesses>,
        channel_gateway: channel_gateway_contract::ChannelGatewayHandle,
        native_channel: native_channel_contract::NativeChannelHandle,
        sub_agent_host: sub_agent_host_contract::SubAgentHostHandle,
        turn_router: turn_router_contract::TurnRouterHandle,
        trigger_inbox: trigger_inbox_contract::TriggerInboxHandle,
    }

    impl TestGraph {
        fn handles(&self) -> StartupHandles<'_> {
            StartupHandles {
                agent_host: &self.agent_host,
                channel_gateway: &self.channel_gateway,
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

        let gateway = channel_gateway::register(&mut builder, move |imports| {
            ChannelGateway::connect(imports.agent_host, imports.native_channel)
        });
        builder.connect(&gateway, &agent);
        builder.connect(&gateway, &channel);
        let channel_gateway =
            builder.handle::<channel_gateway_contract::ChannelGatewayHandle>(&gateway);

        let sub_agent_state =
            SubAgentHostState::open(path.join("sub-agent.sqlite")).expect("sub-agent store opens");
        let sub_agent = sub_agent_host::register(&mut builder, move |imports| {
            sub_agent_state.connect(imports.agent_host)
        });
        builder.connect(&sub_agent, &agent);
        let sub_agent_host =
            builder.handle::<sub_agent_host_contract::SubAgentHostHandle>(&sub_agent);

        let inbox_store =
            TriggerInbox::open(path.join("inbox.sqlite")).expect("trigger store opens");
        let inbox = trigger_inbox::register(&mut builder, inbox_store);
        let trigger_inbox = builder.handle::<trigger_inbox_contract::TriggerInboxHandle>(&inbox);
        let bridge_processes = Arc::new(FakeBridgeProcesses::default());
        let processes_for_factory = bridge_processes.clone();
        let bridge_state =
            BridgeHostState::open(path.join("bridge.sqlite")).expect("bridge store opens");
        let bridge = bridge_host::register(&mut builder, move |imports| {
            bridge_state.connect(
                imports.trigger_inbox,
                Arc::new(FakeBridgeFactory {
                    processes: processes_for_factory.clone(),
                }),
                Arc::new(InMemoryCredentialStore::default()),
            )
        });
        builder.connect(&bridge, &inbox);
        let bridge_host = builder.handle::<bridge_host_contract::BridgeHostHandle>(&bridge);
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
            bridge_host,
            bridge_processes,
            channel_gateway,
            native_channel,
            sub_agent_host,
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
                session_options: std::collections::BTreeMap::new(),
                session_mcp_servers: Vec::new(),
                protocol: ProtocolConfig::V2,
                steering_extension: None,
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
            bridges: Vec::new(),
        }
    }

    fn bridge_config(working_directory: PathBuf) -> BridgeConfig {
        BridgeConfig {
            bridge_id: "whatsapp".into(),
            package_id: "crab.whatsapp".into(),
            display_name: "WhatsApp".into(),
            executable: "/fixture/whatsapp".into(),
            arguments: Vec::new(),
            environment_from: Vec::new(),
            working_directory,
            configuration: json!({"targetChannelId":"primary"}),
            authentication_methods: Vec::new(),
            ingress_mode: BridgeIngressConfig::Queue,
            alert_target: None,
            desired_running: true,
            health_interval_ms: 60_000,
            credential_validation_interval_ms: 60_000,
            restart_limit: 3,
            restart_window_ms: 60_000,
        }
    }

    fn primary_attachment(config: &RuntimeConfig) -> AttachChannelRequest {
        let channel = &config.channels[0];
        AttachChannelRequest {
            channel_id: channel.channel_id.clone(),
            adapter_id: channel.adapter_id.clone(),
            agent_id: channel.agent_id.clone(),
            working_directory: channel.working_directory.to_string_lossy().into_owned(),
            bootstrap_prompt: config
                .bootstrap_prompt(channel)
                .expect("bootstrap resolves"),
            session_metadata_json: session_metadata(channel).expect("metadata encodes"),
            native_channel_json: serde_json::to_string(&channel.native_channel)
                .expect("native channel encodes"),
        }
    }

    #[tokio::test]
    async fn restart_reuses_binding_and_replaces_only_the_acp_session() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let mut config = config(directory.path().to_path_buf());
        let first = graph(directory.path(), state.clone());
        let first_sessions = initialize_topology_with_handles(first.handles(), &config)
            .await
            .expect("first topology starts");
        let duplicate_sessions = initialize_topology_with_handles(first.handles(), &config)
            .await
            .expect("duplicate attach is idempotent");
        assert_eq!(duplicate_sessions, first_sessions);
        assert_eq!(
            state.lock().expect("fake state lock").next_session,
            1,
            "duplicate attach must not start a second agent session"
        );
        config.channels[0].native_channel = json!({"title": "Jim v2"});
        assert!(matches!(
            initialize_topology_with_handles(first.handles(), &config).await,
            Err(crate::RuntimeStartError::AttachChannel(CallError::Domain(
                channel_gateway_contract::ChannelGatewayError::AttachmentConflict
            )))
        ));
        assert_eq!(
            state.lock().expect("fake state lock").next_session,
            1,
            "changed intent must not replace a live agent session"
        );
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
        assert!(detach_sessions(&first.agent_host).await);
        drop(first);

        let restarted = graph(directory.path(), state.clone());
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
        assert!(
            state
                .lock()
                .expect("fake state lock")
                .resume_attempts
                .is_empty(),
            "changed intent must replace without resuming the old session"
        );
        let native: serde_json::Value =
            serde_json::from_str(&binding.native_channel_json).expect("gateway envelope decodes");
        assert_eq!(native["adapter"]["title"], "Jim v2");
        assert!(close_sessions(&restarted.agent_host, &restarted_sessions).await);
    }

    #[tokio::test]
    async fn matching_restart_resumes_the_existing_binding_and_session_identity() {
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
        assert!(detach_sessions(&first.agent_host).await);
        drop(first);
        state.lock().expect("fake state lock").resume_mode = FakeResumeMode::Success;

        let restarted = graph(directory.path(), state.clone());
        let resumed = restarted
            .channel_gateway
            .attach_channel(call_context(), primary_attachment(&config))
            .await
            .expect("matching unavailable attachment resumes");
        assert_eq!(
            resumed.disposition,
            ChannelAttachmentDisposition::ResumedUnavailableSession
        );
        assert_eq!(resumed.binding_id, first_route.binding_id);
        assert_eq!(resumed.session_id, first_sessions[0]);
        restarted
            .agent_host
            .session_status(
                call_context(),
                SessionReference {
                    session_id: resumed.session_id.clone(),
                },
            )
            .await
            .expect("resumed fake session is ready");
        restarted
            .agent_host
            .read_events(
                call_context(),
                ReadEventsRequest {
                    session_id: resumed.session_id.clone(),
                    after_sequence: 0,
                    limit: 1_000,
                },
            )
            .await
            .expect("resumed fake events remain readable");
        let stored_binding = restarted
            .native_channel
            .inspect_binding(
                call_context(),
                BindingReference {
                    binding_id: resumed.binding_id.clone(),
                },
            )
            .await
            .expect("resumed binding remains stored");
        assert_eq!(stored_binding.lifecycle, ChannelLifecycle::Attached);
        assert_eq!(stored_binding.session_id, resumed.session_id);
        assert_eq!(
            state.lock().expect("fake state lock").resume_attempts,
            first_sessions
        );
        restarted
            .native_channel
            .channel_status(
                call_context(),
                BindingReference {
                    binding_id: resumed.binding_id.clone(),
                },
            )
            .await
            .expect("resumed binding is immediately live");
        let restarted_sessions = initialize_topology_with_handles(restarted.handles(), &config)
            .await
            .expect("resumed topology initializes idempotently");
        assert_eq!(restarted_sessions, first_sessions);
        {
            let state = state.lock().expect("fake state lock");
            assert_eq!(state.next_session, 1, "resume must not open a replacement");
            assert_eq!(state.resume_attempts, first_sessions);
        }
        assert!(close_sessions(&restarted.agent_host, &restarted_sessions).await);
    }

    #[tokio::test]
    async fn runtime_recovers_parent_before_its_durable_sub_agent() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let config = config(directory.path().to_path_buf());
        let first = graph(directory.path(), state.clone());
        let parent_sessions = initialize_topology_with_handles(first.handles(), &config)
            .await
            .expect("parent topology starts");
        let child = first
            .sub_agent_host
            .spawn(
                call_context(),
                SpawnSubAgentRequest {
                    client_sub_agent_id: "restart-child".into(),
                    parent_session_id: parent_sessions[0].clone(),
                    agent_id: "fake".into(),
                    working_directory: directory.path().to_string_lossy().into_owned(),
                    context_mode: SubAgentContextMode::Fresh,
                    parent_context_through_sequence: None,
                    allow_portable_snapshot: false,
                    native_task_prompt_json: json!([{
                        "type": "text",
                        "text": "continue independently after restart"
                    }])
                    .to_string(),
                    metadata_json: "{}".into(),
                    crash_restart_limit: 1,
                },
            )
            .await
            .expect("durable child starts");
        let mut expected_detached =
            vec![parent_sessions[0].clone(), child.child_session_id.clone()];
        expected_detached.sort();
        let detached = first
            .agent_host
            .detach_sessions(call_context(), DetachSessionsRequest {})
            .await
            .expect("graceful shutdown detaches every host-owned session");
        assert_eq!(detached.detached_session_ids, expected_detached);
        assert!(detached.failed_session_ids.is_empty());
        drop(first);
        state.lock().expect("fake state lock").resume_mode = FakeResumeMode::Success;

        let restarted = graph(directory.path(), state.clone());
        let recovered_parents = initialize_topology_with_handles(restarted.handles(), &config)
            .await
            .expect("parent resumes first");
        let recovered_children = recover_sub_agents(&restarted.sub_agent_host)
            .await
            .expect("child reconciliation completes");
        assert_eq!(recovered_parents, parent_sessions);
        assert_eq!(
            recovered_children.as_slice(),
            std::slice::from_ref(&child.child_session_id)
        );
        let status = restarted
            .sub_agent_host
            .status(
                call_context(),
                SubAgentReference {
                    sub_agent_id: child.sub_agent_id,
                },
            )
            .await
            .expect("recovered child is inspectable");
        assert_eq!(status.record.child_session_id, child.child_session_id);
        assert_eq!(
            status.record.native_child_session_id,
            child.native_child_session_id
        );
        assert_eq!(status.restart_count, 1);
        assert!(matches!(status.record.lifecycle, SubAgentLifecycle::Idle));
        assert_eq!(
            state.lock().expect("fake state lock").resume_attempts,
            [parent_sessions[0].clone(), child.child_session_id.clone()],
            "startup must recover the parent before its child"
        );
        let mut all_sessions = recovered_parents;
        all_sessions.extend(recovered_children);
        assert!(close_sessions(&restarted.agent_host, &all_sessions).await);
    }

    #[tokio::test]
    async fn matching_restart_falls_back_only_for_explicit_resume_unavailability() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let config = config(directory.path().to_path_buf());
        let first = graph(directory.path(), state.clone());
        let first_sessions = initialize_topology_with_handles(first.handles(), &config)
            .await
            .expect("first topology starts");
        assert!(close_sessions(&first.agent_host, &first_sessions).await);
        drop(first);

        let restarted = graph(directory.path(), state.clone());
        let replaced = restarted
            .channel_gateway
            .attach_channel(call_context(), primary_attachment(&config))
            .await
            .expect("unsupported resume opens a replacement");
        assert_eq!(
            replaced.disposition,
            ChannelAttachmentDisposition::ReplacedUnavailableSession
        );
        assert_ne!(replaced.session_id, first_sessions[0]);
        {
            let state = state.lock().expect("fake state lock");
            assert_eq!(state.next_session, 2);
            assert_eq!(state.resume_attempts, first_sessions);
        }
        assert!(close_sessions(&restarted.agent_host, &[replaced.session_id]).await);
    }

    #[tokio::test]
    async fn matching_restart_does_not_replace_after_authority_failure() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let config = config(directory.path().to_path_buf());
        let first = graph(directory.path(), state.clone());
        let first_sessions = initialize_topology_with_handles(first.handles(), &config)
            .await
            .expect("first topology starts");
        assert!(close_sessions(&first.agent_host, &first_sessions).await);
        drop(first);
        state.lock().expect("fake state lock").resume_mode = FakeResumeMode::AuthorityFailure;

        let restarted = graph(directory.path(), state.clone());
        assert!(matches!(
            restarted
                .channel_gateway
                .attach_channel(call_context(), primary_attachment(&config))
                .await,
            Err(CallError::Domain(ChannelGatewayError::AgentUnavailable))
        ));
        let state = state.lock().expect("fake state lock");
        assert_eq!(
            state.next_session, 1,
            "hard failure must not open a replacement"
        );
        assert_eq!(state.resume_attempts, first_sessions);
    }

    #[tokio::test]
    async fn authenticated_ipc_reuses_live_attachment_across_client_disconnects() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let config = config(directory.path().to_path_buf());
        let graph = graph(directory.path(), state.clone());
        let sessions = initialize_topology_with_handles(graph.handles(), &config)
            .await
            .expect("topology starts");
        let paths = ChannelIpcPaths::for_state_directory(directory.path()).expect("paths resolve");
        let mut server = ChannelIpcServer::start(
            paths.clone(),
            graph.agent_host.clone(),
            graph.channel_gateway.clone(),
            graph.native_channel.clone(),
            graph.bridge_host.clone(),
            graph.trigger_inbox.clone(),
            graph.sub_agent_host.clone(),
        )
        .await
        .expect("IPC starts");
        assert!(matches!(
            ChannelIpcServer::start(
                paths.clone(),
                graph.agent_host.clone(),
                graph.channel_gateway.clone(),
                graph.native_channel.clone(),
                graph.bridge_host.clone(),
                graph.trigger_inbox.clone(),
                graph.sub_agent_host.clone(),
            )
            .await,
            Err(ChannelIpcStartupError::AlreadyRunning)
        ));
        let request = AttachChannelRequest {
            channel_id: "primary".into(),
            adapter_id: "native-ui".into(),
            agent_id: "fake".into(),
            working_directory: directory.path().to_string_lossy().into_owned(),
            bootstrap_prompt: config
                .bootstrap_prompt(&config.channels[0])
                .expect("bootstrap"),
            session_metadata_json: session_metadata(&config.channels[0]).expect("metadata"),
            native_channel_json: serde_json::to_string(&config.channels[0].native_channel)
                .expect("native metadata"),
        };

        let client =
            ChannelIpcClient::from_state_directory(directory.path()).expect("client opens");
        let first = client
            .attach_channel(request.clone())
            .await
            .expect("first client attaches");
        assert_eq!(
            first.disposition,
            ChannelAttachmentDisposition::ReusedLiveSession
        );
        drop(client);
        let second = ChannelIpcClient::from_state_directory(directory.path())
            .expect("second client opens")
            .attach_channel(request.clone())
            .await
            .expect("second client attaches");
        assert_eq!(second.binding_id, first.binding_id);
        assert_eq!(second.session_id, sessions[0]);
        assert_eq!(state.lock().expect("fake state lock").next_session, 1);

        let valid_token = std::fs::read_to_string(paths.token()).expect("token reads");
        std::fs::write(paths.token(), "0".repeat(64)).expect("test token changes");
        let unauthorized = ChannelIpcClient::from_state_directory(directory.path())
            .expect("wrong-token client opens")
            .channel_status(BindingReference {
                binding_id: first.binding_id,
            })
            .await;
        assert!(matches!(
            unauthorized,
            Err(ChannelIpcClientError::Remote { kind, code })
                if kind == "authentication" && code == "Unauthorized"
        ));
        std::fs::write(paths.token(), &valid_token).expect("test token restores");
        assert_eq!(
            std::fs::metadata(paths.socket())
                .expect("socket metadata")
                .permissions()
                .mode()
                & 0o077,
            0
        );
        assert_eq!(
            std::fs::metadata(paths.token())
                .expect("token metadata")
                .permissions()
                .mode()
                & 0o077,
            0
        );

        server.shutdown().await.expect("IPC shuts down");
        assert!(!paths.socket().exists());
        assert!(paths.token().exists());
        let mut restarted = ChannelIpcServer::start(
            paths.clone(),
            graph.agent_host.clone(),
            graph.channel_gateway.clone(),
            graph.native_channel.clone(),
            graph.bridge_host.clone(),
            graph.trigger_inbox.clone(),
            graph.sub_agent_host.clone(),
        )
        .await
        .expect("IPC restarts");
        let after_restart = ChannelIpcClient::from_state_directory(directory.path())
            .expect("restart client opens")
            .attach_channel(request)
            .await
            .expect("restart client reuses attachment");
        assert_eq!(after_restart.session_id, sessions[0]);
        assert_eq!(
            std::fs::read_to_string(paths.token()).expect("persisted token reads"),
            valid_token
        );
        restarted
            .shutdown()
            .await
            .expect("restarted IPC shuts down");
        assert!(close_sessions(&graph.agent_host, &sessions).await);
    }

    #[tokio::test]
    async fn authenticated_ipc_operates_bridge_auth_health_and_recovery_without_state_access() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let graph = graph(directory.path(), state);
        let mut topology_config = config(directory.path().to_path_buf());
        let mut bridge = bridge_config(directory.path().to_path_buf());
        bridge.authentication_methods = vec![BridgeAuthenticationConfig::PhoneCode];
        topology_config.bridges.push(bridge);
        initialize_bridges(&graph.bridge_host, &topology_config)
            .await
            .expect("configured bridge starts");
        let paths = ChannelIpcPaths::for_state_directory(directory.path()).expect("paths resolve");
        let mut server = ChannelIpcServer::start(
            paths,
            graph.agent_host.clone(),
            graph.channel_gateway.clone(),
            graph.native_channel.clone(),
            graph.bridge_host.clone(),
            graph.trigger_inbox.clone(),
            graph.sub_agent_host.clone(),
        )
        .await
        .expect("IPC starts");
        let client =
            ChannelIpcClient::from_state_directory(directory.path()).expect("client opens");

        let catalog = client.list_bridges().await.expect("catalog crosses IPC");
        assert_eq!(catalog.bridges.len(), 1);
        assert_eq!(catalog.bridges[0].bridge_id, "whatsapp");
        let mut dynamic_config = config(directory.path().to_path_buf());
        let mut dynamic_bridge = bridge_config(directory.path().to_path_buf());
        dynamic_bridge.bridge_id = "signal".into();
        dynamic_bridge.package_id = "agent.signal".into();
        dynamic_bridge.display_name = "Signal".into();
        dynamic_bridge.desired_running = false;
        dynamic_config.bridges.push(dynamic_bridge);
        let mut dynamic_spec = dynamic_config
            .bridge_specs()
            .expect("dynamic bridge spec encodes")
            .remove(0);
        let registered = client
            .register_bridge(dynamic_spec.clone())
            .await
            .expect("agent-installed bridge registration crosses IPC");
        assert_eq!(registered.bridge_id, "signal");
        assert_eq!(registered.generation, 1);
        dynamic_spec.display_name = "Signal bridge".into();
        let replaced = client
            .replace_bridge(ReplaceBridgeRequest {
                expected_generation: 1,
                spec: dynamic_spec,
            })
            .await
            .expect("agent-installed bridge replacement crosses IPC");
        assert_eq!(replaced.generation, 2);
        let status = client
            .bridge_status(BridgeReference {
                bridge_id: "whatsapp".into(),
            })
            .await
            .expect("status crosses IPC");
        assert_eq!(status.generation, 1);
        let challenge = client
            .begin_bridge_authentication(BeginAuthenticationRequest {
                bridge_id: "whatsapp".into(),
                preferred_method: Some(ContractAuthenticationMethod::PhoneCode),
                context_json: r#"{"phoneNumber":"+33600000000"}"#.into(),
            })
            .await
            .expect("authentication challenge crosses IPC");
        assert_eq!(challenge.method, ContractAuthenticationMethod::PhoneCode);
        assert!(challenge.presentation_json.contains("1234-5678"));
        let credential = client
            .submit_bridge_authentication(SubmitAuthenticationRequest {
                bridge_id: "whatsapp".into(),
                challenge_id: challenge.challenge_id,
                response_json: "{}".into(),
            })
            .await
            .expect("authentication submission crosses IPC");
        assert_eq!(credential.lifecycle, CredentialLifecycle::Valid);
        assert!(credential.credential_handle.is_some());
        assert!(!format!("{credential:?}").contains("never-cross-ipc"));
        let validated = client
            .validate_bridge_credentials(BridgeReference {
                bridge_id: "whatsapp".into(),
            })
            .await
            .expect("credential validation crosses IPC");
        assert_eq!(validated.lifecycle, CredentialLifecycle::Valid);
        let reconciled = client
            .reconcile_bridge(ReconcileBridgeRequest {
                bridge_id: "whatsapp".into(),
                expected_generation: 1,
                desired_running: true,
            })
            .await
            .expect("reconcile crosses IPC");
        assert_eq!(reconciled.lifecycle, BridgeLifecycle::Healthy);
        let outbound = BridgeOutbound {
            bridge_id: "whatsapp".into(),
            message_id: "selected-1".into(),
            destination_json: r#"{"chatId":"fixture"}"#.into(),
            message_json: r#"{"type":"text","text":"selected output"}"#.into(),
            attachments: Vec::new(),
            idempotency_key: "selected-1".into(),
        };
        let delivered = client
            .deliver_bridge_message(outbound.clone())
            .await
            .expect("selected message delivery crosses IPC");
        assert_eq!(delivered.lifecycle, DeliveryLifecycle::Delivered);
        let delivery_status = client
            .bridge_delivery_status(DeliveryReference {
                bridge_id: "whatsapp".into(),
                message_id: "selected-1".into(),
            })
            .await
            .expect("delivery status crosses IPC");
        assert_eq!(
            delivery_status.external_delivery_id,
            delivered.external_delivery_id
        );
        assert_eq!(
            client
                .deliver_bridge_message(outbound)
                .await
                .expect("delivery retry deduplicates")
                .attempt,
            delivered.attempt
        );
        assert!(
            client
                .invalidate_bridge_credentials(BridgeReference {
                    bridge_id: "whatsapp".into(),
                })
                .await
                .expect("credential invalidation crosses IPC")
                .accepted
        );
        assert!(matches!(
            client
                .bridge_status(BridgeReference {
                    bridge_id: "missing".into(),
                })
                .await,
            Err(ChannelIpcClientError::Remote { kind, code })
                if kind == "domain" && code == "UnknownBridge"
        ));
        let suspended = client
            .suspend_bridge(BridgeReference {
                bridge_id: "whatsapp".into(),
            })
            .await
            .expect("bridge suspension crosses IPC");
        assert_eq!(suspended.lifecycle, BridgeLifecycle::Stopped);
        assert!(
            client
                .stop_bridge(BridgeReference {
                    bridge_id: "whatsapp".into(),
                })
                .await
                .expect("durable stop crosses IPC")
                .accepted
        );

        server.shutdown().await.expect("IPC shuts down");
    }

    #[tokio::test]
    async fn authenticated_ipc_operates_complete_realtime_sub_agent_lifecycle() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let graph = graph(directory.path(), state);
        let config = config(directory.path().to_path_buf());
        let sessions = initialize_topology_with_handles(graph.handles(), &config)
            .await
            .expect("parent topology starts");
        graph
            .agent_host
            .prompt(
                call_context(),
                PromptRequest {
                    session_id: sessions[0].clone(),
                    client_turn_id: "parent-context".into(),
                    mode: agent_host_contract::AgentInputMode::Queue,
                    native_prompt_json: r#"[{"type":"text","text":"parent context"}]"#.into(),
                },
            )
            .await
            .expect("parent context is durable");

        let paths = ChannelIpcPaths::for_state_directory(directory.path()).expect("paths resolve");
        let mut server = ChannelIpcServer::start(
            paths,
            graph.agent_host.clone(),
            graph.channel_gateway.clone(),
            graph.native_channel.clone(),
            graph.bridge_host.clone(),
            graph.trigger_inbox.clone(),
            graph.sub_agent_host.clone(),
        )
        .await
        .expect("IPC starts");
        let client =
            ChannelIpcClient::from_state_directory(directory.path()).expect("client opens");

        let fresh_request = SpawnSubAgentRequest {
            client_sub_agent_id: "ipc-fresh".into(),
            parent_session_id: sessions[0].clone(),
            agent_id: "fake".into(),
            working_directory: directory.path().to_string_lossy().into_owned(),
            context_mode: SubAgentContextMode::Fresh,
            parent_context_through_sequence: None,
            allow_portable_snapshot: false,
            native_task_prompt_json: r#"[{"type":"text","text":"fresh task"}]"#.into(),
            metadata_json: r#"{"purpose":"ipc-fresh"}"#.into(),
            crash_restart_limit: 0,
        };
        let fresh = client
            .spawn_sub_agent(fresh_request.clone())
            .await
            .expect("fresh child spawns through IPC");
        assert_eq!(fresh.context_realization, ContextRealization::FreshSession);
        let fresh_retry = client
            .spawn_sub_agent(fresh_request)
            .await
            .expect("spawn retry deduplicates through IPC");
        assert_eq!(fresh_retry.sub_agent_id, fresh.sub_agent_id);
        assert_eq!(fresh_retry.child_session_id, fresh.child_session_id);
        assert_eq!(fresh_retry.process_identity, fresh.process_identity);
        assert!(
            client
                .stop_sub_agent(StopSubAgentRequest {
                    sub_agent_id: fresh.sub_agent_id,
                    reason: "fresh coverage complete".into(),
                })
                .await
                .expect("fresh child stops")
                .accepted
        );

        let inherited = client
            .spawn_sub_agent(SpawnSubAgentRequest {
                client_sub_agent_id: "ipc-inherited".into(),
                parent_session_id: sessions[0].clone(),
                agent_id: "fake".into(),
                working_directory: directory.path().to_string_lossy().into_owned(),
                context_mode: SubAgentContextMode::InheritParent,
                parent_context_through_sequence: Some(2),
                allow_portable_snapshot: true,
                native_task_prompt_json: r#"[{"type":"text","text":"hold child work"}]"#.into(),
                metadata_json: r#"{"purpose":"ipc-inherited"}"#.into(),
                crash_restart_limit: 0,
            })
            .await
            .expect("inherited child spawns through IPC");
        assert_eq!(
            inherited.context_realization,
            ContextRealization::PortableSnapshot
        );
        let steered = client
            .send_to_child(SendToChildRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                client_message_id: "parent-steer".into(),
                mode: SubAgentInputMode::Steer,
                native_prompt_json: r#"[{"type":"text","text":"steer now"}]"#.into(),
            })
            .await
            .expect("parent steers child through IPC");
        assert_eq!(steered.client_message_id, "parent-steer");
        let progress = client
            .send_to_parent(SendToParentRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                client_message_id: "child-progress".into(),
                mode: SubAgentInputMode::Queue,
                message_json: r#"{"progress":"halfway"}"#.into(),
            })
            .await
            .expect("child sends progress through IPC");
        assert_eq!(progress.client_message_id, "child-progress");
        let interrupted = client
            .send_to_child(SendToChildRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                client_message_id: "parent-interrupt".into(),
                mode: SubAgentInputMode::InterruptAndSteer,
                native_prompt_json: r#"[{"type":"text","text":"replace work"}]"#.into(),
            })
            .await
            .expect("parent interrupts child through IPC");
        assert_eq!(
            interrupted.disposition,
            InputDisposition::CancelRequestedThenQueued
        );

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = client
                    .read_sub_agent_events(ReadSubAgentEventsRequest {
                        sub_agent_id: inherited.sub_agent_id.clone(),
                        after_sequence: 0,
                        limit: 100,
                    })
                    .await
                    .expect("ordered events cross IPC");
                if page
                    .events
                    .iter()
                    .any(|event| event.kind == SubAgentEventKind::NativeAcp)
                    && page
                        .events
                        .iter()
                        .any(|event| event.kind == SubAgentEventKind::ChildToParent)
                {
                    assert!(page.next_sequence > 0);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("event cursor catches up");
        let status = client
            .sub_agent_status(SubAgentReference {
                sub_agent_id: inherited.sub_agent_id.clone(),
            })
            .await
            .expect("status crosses IPC");
        assert!(status.last_sequence > 0);
        let stopped = client
            .stop_sub_agent(StopSubAgentRequest {
                sub_agent_id: inherited.sub_agent_id.clone(),
                reason: "integration complete".into(),
            })
            .await
            .expect("child stops through IPC");
        assert!(stopped.accepted);
        assert_eq!(
            client
                .sub_agent_status(SubAgentReference {
                    sub_agent_id: inherited.sub_agent_id.clone(),
                })
                .await
                .expect("terminal status crosses IPC")
                .record
                .lifecycle,
            SubAgentLifecycle::Completed
        );
        assert!(
            !client
                .stop_sub_agent(StopSubAgentRequest {
                    sub_agent_id: inherited.sub_agent_id,
                    reason: "idempotent retry".into(),
                })
                .await
                .expect("terminal stop retry is safe")
                .accepted
        );

        server.shutdown().await.expect("IPC shuts down");
        assert!(close_sessions(&graph.agent_host, &sessions).await);
    }

    #[tokio::test]
    async fn acp_facade_streams_cancels_and_reloads_through_real_local_ipc() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let graph = graph(directory.path(), state.clone());
        let paths = ChannelIpcPaths::for_state_directory(directory.path()).expect("paths resolve");
        let mut server = ChannelIpcServer::start(
            paths,
            graph.agent_host.clone(),
            graph.channel_gateway.clone(),
            graph.native_channel.clone(),
            graph.bridge_host.clone(),
            graph.trigger_inbox.clone(),
            graph.sub_agent_host.clone(),
        )
        .await
        .expect("IPC starts");
        let options = AcpChannelOptions::new(directory.path(), "fake");
        let first_updates = Arc::new(Mutex::new(Vec::new()));
        let received_updates = first_updates.clone();
        let first_session = Arc::new(Mutex::new(None::<String>));
        let recorded_session = first_session.clone();
        let first_agent = AcpChannelFacade::new(
            ChannelIpcClient::from_state_directory(directory.path()).expect("client opens"),
            options.clone(),
        )
        .agent();
        let first_client = Client.builder().on_receive_notification(
            async move |notification: UntypedMessage, _connection| {
                received_updates
                    .lock()
                    .expect("updates lock")
                    .push(notification.params);
                Ok(())
            },
            agent_client_protocol::on_receive_notification!(),
        );
        let first_working_directory = directory.path().to_path_buf();
        tokio::time::timeout(
            Duration::from_secs(5),
            first_client.connect_with(first_agent, async move |connection| {
                let initialized = connection
                    .send_request(InitializeRequest::new(ProtocolVersion::V1))
                    .block_task()
                    .await?;
                assert!(initialized.agent_capabilities.load_session);
                assert_eq!(initialized.auth_methods.len(), 1);
                connection
                    .send_request(AuthenticateRequest::new("crab-local"))
                    .block_task()
                    .await?;
                let created = connection
                    .send_request(NewSessionRequest::new(first_working_directory))
                    .block_task()
                    .await?;
                let session_id = created.session_id.to_string();
                *recorded_session.lock().expect("session lock") = Some(session_id.clone());
                let completed = connection
                    .send_request(AcpPromptRequest::new(
                        session_id.clone(),
                        vec![ContentBlock::Text(TextContent::new("hello"))],
                    ))
                    .block_task()
                    .await?;
                assert_eq!(completed.stop_reason, StopReason::EndTurn);

                let held = connection.send_request(AcpPromptRequest::new(
                    session_id.clone(),
                    vec![ContentBlock::Text(TextContent::new("hold"))],
                ));
                connection.send_notification(CancelNotification::new(session_id))?;
                let cancelled = held.block_task().await?;
                assert_eq!(cancelled.stop_reason, StopReason::Cancelled);
                Ok(())
            }),
        )
        .await
        .expect("first ACP connection completes")
        .expect("first ACP connection succeeds");

        let facade_session_id = first_session
            .lock()
            .expect("session lock")
            .clone()
            .expect("session was created");
        assert!(
            first_updates
                .lock()
                .expect("updates lock")
                .iter()
                .all(
                    |params| params.get("sessionId").and_then(serde_json::Value::as_str)
                        == Some(facade_session_id.as_str())
                )
        );
        let sessions_after_disconnect = state.lock().expect("fake state lock").next_session;
        assert_eq!(sessions_after_disconnect, 1);
        assert_eq!(
            state.lock().expect("fake state lock").live_sessions.len(),
            1
        );

        let second_agent = AcpChannelFacade::new(
            ChannelIpcClient::from_state_directory(directory.path()).expect("client reopens"),
            options,
        )
        .agent();
        let second_working_directory = directory.path().to_path_buf();
        tokio::time::timeout(
            Duration::from_secs(5),
            Client
                .builder()
                .connect_with(second_agent, async move |connection| {
                    connection
                        .send_request(InitializeRequest::new(ProtocolVersion::V1))
                        .block_task()
                        .await?;
                    connection
                        .send_request(AuthenticateRequest::new("crab-local"))
                        .block_task()
                        .await?;
                    connection
                        .send_request(LoadSessionRequest::new(
                            facade_session_id.clone(),
                            second_working_directory,
                        ))
                        .block_task()
                        .await?;
                    let completed = connection
                        .send_request(AcpPromptRequest::new(
                            facade_session_id,
                            vec![ContentBlock::Text(TextContent::new("after reload"))],
                        ))
                        .block_task()
                        .await?;
                    assert_eq!(completed.stop_reason, StopReason::EndTurn);
                    Ok(())
                }),
        )
        .await
        .expect("second ACP connection completes")
        .expect("second ACP connection succeeds");
        assert_eq!(state.lock().expect("fake state lock").next_session, 1);

        server.shutdown().await.expect("IPC shuts down");
        assert!(detach_sessions(&graph.agent_host).await);
    }

    #[tokio::test]
    async fn configured_bridges_restore_replace_suspend_and_stop_removed_registrations() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let state = Arc::new(Mutex::new(FakeState::default()));
        let graph = graph(directory.path(), state);
        let mut config = config(directory.path().to_path_buf());
        config
            .bridges
            .push(bridge_config(directory.path().to_path_buf()));

        assert_eq!(
            initialize_bridges(&graph.bridge_host, &config)
                .await
                .expect("bridge registers"),
            ["whatsapp"]
        );
        initialize_bridges(&graph.bridge_host, &config)
            .await
            .expect("same bridge restores idempotently");
        assert_eq!(graph.bridge_processes.launches.load(Ordering::SeqCst), 1);

        config.bridges[0].ingress_mode = BridgeIngressConfig::Steer;
        initialize_bridges(&graph.bridge_host, &config)
            .await
            .expect("changed bridge replaces");
        let replaced = graph
            .bridge_host
            .list_bridges(call_context(), ListBridgesRequest {})
            .await
            .expect("catalog lists")
            .bridges
            .remove(0);
        assert_eq!(replaced.generation, 2);
        assert_eq!(graph.bridge_processes.launches.load(Ordering::SeqCst), 2);
        assert_eq!(graph.bridge_processes.stops.load(Ordering::SeqCst), 1);

        assert!(suspend_bridges(&graph.bridge_host, &["whatsapp".into()]).await);
        let suspended = graph
            .bridge_host
            .list_bridges(call_context(), ListBridgesRequest {})
            .await
            .expect("suspended catalog lists")
            .bridges
            .remove(0);
        assert!(suspended.desired_running);
        assert_eq!(suspended.generation, 2);
        assert_eq!(graph.bridge_processes.stops.load(Ordering::SeqCst), 2);

        initialize_bridges(&graph.bridge_host, &config)
            .await
            .expect("suspended bridge restarts without generation churn");
        assert_eq!(graph.bridge_processes.launches.load(Ordering::SeqCst), 3);
        config.bridges.clear();
        initialize_bridges(&graph.bridge_host, &config)
            .await
            .expect("removed bridge stops durably");
        let removed = graph
            .bridge_host
            .list_bridges(call_context(), ListBridgesRequest {})
            .await
            .expect("removed catalog lists")
            .bridges
            .remove(0);
        assert!(!removed.desired_running);
        assert_eq!(removed.generation, 3);
        assert_eq!(graph.bridge_processes.stops.load(Ordering::SeqCst), 3);
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
        let paths = ChannelIpcPaths::for_state_directory(directory.path()).expect("paths resolve");
        let mut server = ChannelIpcServer::start(
            paths,
            graph.agent_host.clone(),
            graph.channel_gateway.clone(),
            graph.native_channel.clone(),
            graph.bridge_host.clone(),
            graph.trigger_inbox.clone(),
            graph.sub_agent_host.clone(),
        )
        .await
        .expect("IPC starts");
        let client =
            ChannelIpcClient::from_state_directory(directory.path()).expect("client opens");
        let trigger = EnqueueTrigger {
            source: TriggerSource::Operator,
            source_id: "runtime-test".into(),
            deduplication_key: "worker-turn-1".into(),
            target_channel_id: "primary".into(),
            lane: "primary".into(),
            mode: TriggerMode::Queue,
            not_before_ms: 0,
            message_json: r#"{"text":"hello"}"#.into(),
            attachments: Vec::new(),
        };
        let receipt = client
            .enqueue_trigger(trigger.clone())
            .await
            .expect("trigger enqueues through authenticated IPC");
        let duplicate = client
            .enqueue_trigger(trigger)
            .await
            .expect("retry returns the durable trigger");
        assert_eq!(duplicate.trigger_id, receipt.trigger_id);
        assert!(duplicate.deduplicated);
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
        server.shutdown().await.expect("IPC shuts down");
        assert!(close_sessions(&graph.agent_host, &sessions).await);
    }
}
