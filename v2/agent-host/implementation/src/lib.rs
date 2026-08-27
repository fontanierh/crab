mod actor;
mod authority;
mod config;
mod contract;
mod store;

pub use authority::{AuthorityVerifier, SystemAuthorityVerifier};
pub use config::{
    AgentProtocol, AgentSteeringExtension, AuthorityProbeConfig, CRAB_AGENT_ID_ENV,
    CRAB_PARENT_SESSION_ID_ENV, CRAB_SESSION_ID_ENV, CRAB_STATE_DIRECTORY_ENV,
    CRAB_SUB_AGENT_ID_ENV, CRAB_WORKING_DIRECTORY_ENV, ConfiguredAgent, ConfiguredMcpServer,
};
pub use contract::*;

use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use actor::{SessionCommand, SessionHandle, SessionLaunch, spawn_session};
use authority::SharedAuthorityVerifier;
use serde_json::{Map, Value};
use store::AgentStore;
use tokio::{
    sync::{RwLock, oneshot},
    task::JoinSet,
};
use uuid::Uuid;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(30);
const AUTHORITY_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) type Clock = Arc<dyn Fn() -> Result<u64, AgentHostError> + Send + Sync>;

/// Durable ACP subprocess host. Every live session owns one actor and one child process.
pub struct AgentHost {
    agents: Arc<BTreeMap<String, Arc<ConfiguredAgent>>>,
    store: Arc<AgentStore>,
    authority: SharedAuthorityVerifier,
    sessions: Arc<RwLock<HashMap<String, SessionHandle>>>,
    state_directory: Option<PathBuf>,
    clock: Clock,
    authority_timeout: Duration,
    control_timeout: Duration,
}

impl AgentHost {
    /// Open a file-backed host using the production authority verifier.
    pub fn open(
        path: impl AsRef<Path>,
        agents: Vec<ConfiguredAgent>,
    ) -> Result<Self, AgentHostError> {
        let state_directory = store_state_directory(path.as_ref())?;
        Self::with_store(
            AgentStore::open(path)?,
            agents,
            Arc::new(SystemAuthorityVerifier),
            Some(state_directory),
        )
    }

    /// Open an in-memory host using the production authority verifier.
    pub fn open_in_memory(agents: Vec<ConfiguredAgent>) -> Result<Self, AgentHostError> {
        Self::with_store(
            AgentStore::open_in_memory()?,
            agents,
            Arc::new(SystemAuthorityVerifier),
            None,
        )
    }

    /// Open a file-backed host with an injected external authority boundary.
    ///
    /// Production callers should use [`Self::open`]. This seam exists for deterministic tests and
    /// deployments whose authority checks are owned by a separate privileged service.
    pub fn open_with_authority_verifier(
        path: impl AsRef<Path>,
        agents: Vec<ConfiguredAgent>,
        authority: Arc<dyn AuthorityVerifier>,
    ) -> Result<Self, AgentHostError> {
        let state_directory = store_state_directory(path.as_ref())?;
        Self::with_store(
            AgentStore::open(path)?,
            agents,
            authority,
            Some(state_directory),
        )
    }

    /// Open an in-memory host with an injected external authority boundary.
    pub fn open_in_memory_with_authority_verifier(
        agents: Vec<ConfiguredAgent>,
        authority: Arc<dyn AuthorityVerifier>,
    ) -> Result<Self, AgentHostError> {
        Self::with_store(AgentStore::open_in_memory()?, agents, authority, None)
    }

    fn with_store(
        store: AgentStore,
        agents: Vec<ConfiguredAgent>,
        authority: SharedAuthorityVerifier,
        state_directory: Option<PathBuf>,
    ) -> Result<Self, AgentHostError> {
        let mut configured = BTreeMap::new();
        for agent in agents {
            agent.validate()?;
            if !agent.session_mcp_servers.is_empty() && state_directory.is_none() {
                return Err(AgentHostError::InvalidConfiguration);
            }
            let agent_id = agent.agent_id.clone();
            if configured.insert(agent_id, Arc::new(agent)).is_some() {
                return Err(AgentHostError::InvalidConfiguration);
            }
        }
        Ok(Self {
            agents: Arc::new(configured),
            store: Arc::new(store),
            authority,
            sessions: Arc::new(RwLock::new(HashMap::new())),
            state_directory,
            clock: Arc::new(system_time_ms),
            authority_timeout: AUTHORITY_TIMEOUT,
            control_timeout: CONTROL_TIMEOUT,
        })
    }

    /// Cooperatively detach every live session without destroying its native ACP identity.
    /// Dropping the returned future is safe because dropping the host also detaches transports.
    pub async fn shutdown(&self) {
        let _ = self.detach_live_sessions().await;
    }

    async fn detach_live_sessions(&self) -> DetachSessionsReport {
        let handles = {
            let mut sessions = self.sessions.write().await;
            sessions.drain().collect::<Vec<_>>()
        };
        let mut pending = handles
            .iter()
            .map(|(session_id, _)| session_id.clone())
            .collect::<BTreeSet<_>>();
        let mut detachments = JoinSet::new();
        for (session_id, handle) in handles {
            let timeout = self.control_timeout;
            detachments.spawn(async move {
                let result =
                    session_control(&handle, timeout, |reply| SessionCommand::Detach { reply })
                        .await;
                (session_id, result)
            });
        }
        let mut report = DetachSessionsReport {
            detached_session_ids: Vec::new(),
            failed_session_ids: Vec::new(),
        };
        while let Some(completion) = detachments.join_next().await {
            if let Ok((session_id, result)) = completion {
                pending.remove(&session_id);
                if result.is_ok() {
                    report.detached_session_ids.push(session_id);
                } else {
                    report.failed_session_ids.push(session_id);
                }
            }
        }
        report.failed_session_ids.extend(pending);
        report.detached_session_ids.sort();
        report.failed_session_ids.sort();
        report
    }

    async fn run_preflight(
        &self,
        agent_id: &str,
        working_directory: &str,
    ) -> Result<PreflightReport, AgentHostError> {
        let agent = self
            .agents
            .get(agent_id)
            .ok_or(AgentHostError::UnknownAgent)?;
        let canonical = tokio::fs::canonicalize(working_directory)
            .await
            .map_err(|_| AgentHostError::PreflightFailed)?;
        if !canonical.is_absolute() {
            return Err(AgentHostError::PreflightFailed);
        }
        let now_ms = (self.clock)()?;
        let authority = tokio::time::timeout(
            self.authority_timeout,
            self.authority.verify(agent, &canonical, now_ms),
        )
        .await
        .map_err(|_| AgentHostError::AuthorityUnavailable)??;
        Ok(PreflightReport {
            agent_id: agent_id.to_owned(),
            working_directory: canonical.to_string_lossy().into_owned(),
            authority,
        })
    }

    async fn live_session(&self, session_id: &str) -> Result<SessionHandle, AgentHostError> {
        self.sessions
            .read()
            .await
            .get(session_id)
            .cloned()
            .ok_or_else(|| match self.store.status(session_id) {
                Ok(_) => AgentHostError::SessionClosed,
                Err(error) => error,
            })
    }

    fn session_can_fork(session: &AgentSession) -> bool {
        let Ok(capabilities) =
            serde_json::from_str::<Value>(&session.negotiation.agent_capabilities_json)
        else {
            return false;
        };
        let pointer = match session.negotiation.protocol_profile {
            AcpProtocolProfile::V1Stable => "/sessionCapabilities/fork",
            AcpProtocolProfile::V2Draft => "/session/fork",
            AcpProtocolProfile::Unknown { .. } => return false,
        };
        capabilities
            .pointer(pointer)
            .is_some_and(|capability| !capability.is_null())
    }
}

#[boxology::implementation]
impl AgentHost {
    pub async fn discover_agents(
        &self,
        context: boxology::CallContext,
        request: DiscoverAgentsRequest,
    ) -> Result<AgentCatalog, AgentHostError> {
        let _ = (context, request);
        let mut agents = Vec::with_capacity(self.agents.len());
        for configured in self.agents.values() {
            let lifecycle = self.store.lifecycle_for_agent(&configured.agent_id)?;
            agents.push(configured.descriptor(lifecycle));
        }
        Ok(AgentCatalog { agents })
    }

    pub async fn preflight(
        &self,
        context: boxology::CallContext,
        request: PreflightRequest,
    ) -> Result<PreflightReport, AgentHostError> {
        let _ = context;
        self.run_preflight(&request.agent_id, &request.working_directory)
            .await
    }

    pub async fn open_session(
        &self,
        context: boxology::CallContext,
        request: OpenSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = context;
        let metadata = serde_json::from_str::<Map<String, Value>>(&request.metadata_json)
            .map_err(|_| AgentHostError::InvalidNativePayload)?;
        let agent = self
            .agents
            .get(&request.agent_id)
            .cloned()
            .ok_or(AgentHostError::UnknownAgent)?;
        let preflight = self
            .run_preflight(&request.agent_id, &request.working_directory)
            .await?;
        let session_id = format!("session_{}", Uuid::new_v4());
        let now_ms = (self.clock)()?;
        self.store.create_starting_session(
            &session_id,
            &request.agent_id,
            &preflight.working_directory,
            &request.metadata_json,
            &agent.protocol.profile(),
            &preflight.authority,
            now_ms,
        )?;
        let state_directory = self.state_directory.clone();
        let (handle, opened) = spawn_session(
            agent,
            self.store.clone(),
            self.clock.clone(),
            session_id.clone(),
            preflight.working_directory.into(),
            SessionLaunch::New {
                bootstrap_prompt: request.bootstrap_prompt,
            },
            metadata,
            state_directory,
        );
        let opened = tokio::time::timeout(CONTROL_TIMEOUT, opened).await;
        let session = match opened {
            Ok(Ok(Ok(session))) => session,
            Ok(Ok(Err(error))) => {
                self.store
                    .set_lifecycle(&session_id, &AgentLifecycle::Failed, (self.clock)()?)?;
                return Err(error);
            }
            Ok(Err(_)) | Err(_) => {
                let (reply, _) = oneshot::channel();
                let _ = handle.commands.try_send(SessionCommand::Close { reply });
                self.store
                    .set_lifecycle(&session_id, &AgentLifecycle::Failed, (self.clock)()?)?;
                return Err(AgentHostError::TransportFailed);
            }
        };
        self.sessions.write().await.insert(session_id, handle);
        Ok(session)
    }

    pub async fn resume_session(
        &self,
        context: boxology::CallContext,
        request: ResumeSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = context;
        let recoverable = self.store.recoverable_session(&request.session_id)?;
        let metadata = serde_json::from_str::<Map<String, Value>>(&recoverable.metadata_json)
            .map_err(|_| AgentHostError::InvalidNativePayload)?;
        let agent = self
            .agents
            .get(&recoverable.agent_id)
            .cloned()
            .ok_or(AgentHostError::UnknownAgent)?;
        if agent.protocol.profile() != recoverable.protocol_profile {
            return Err(AgentHostError::SessionResumeUnavailable);
        }
        let preflight = self
            .run_preflight(&recoverable.agent_id, &recoverable.working_directory)
            .await?;
        self.store
            .prepare_resume(&request.session_id, &preflight.authority, (self.clock)()?)?;
        let state_directory = self.state_directory.clone();
        let session_id = request.session_id;
        let (handle, opened) = spawn_session(
            agent,
            self.store.clone(),
            self.clock.clone(),
            session_id.clone(),
            preflight.working_directory.into(),
            SessionLaunch::Resume {
                native_session_id: recoverable.native_session_id,
            },
            metadata,
            state_directory,
        );
        let opened = tokio::time::timeout(CONTROL_TIMEOUT, opened).await;
        let session = match opened {
            Ok(Ok(Ok(session))) => session,
            Ok(Ok(Err(error))) => {
                self.store
                    .set_lifecycle(&session_id, &AgentLifecycle::Failed, (self.clock)()?)?;
                return Err(error);
            }
            Ok(Err(_)) | Err(_) => {
                let (reply, _) = oneshot::channel();
                let _ = handle.commands.try_send(SessionCommand::Close { reply });
                self.store
                    .set_lifecycle(&session_id, &AgentLifecycle::Failed, (self.clock)()?)?;
                return Err(AgentHostError::TransportFailed);
            }
        };
        self.sessions.write().await.insert(session_id, handle);
        Ok(session)
    }

    pub async fn fork_session(
        &self,
        context: boxology::CallContext,
        request: ForkSessionRequest,
    ) -> Result<AgentSession, AgentHostError> {
        let _ = context;
        let metadata = serde_json::from_str::<Map<String, Value>>(&request.metadata_json)
            .map_err(|_| AgentHostError::InvalidNativePayload)?;
        let parent_handle = self.live_session(&request.parent_session_id).await?;
        let _parent_control = parent_handle.control.lock().await;
        let parent_status = self.store.status(&request.parent_session_id)?;
        if !matches!(parent_status.lifecycle, AgentLifecycle::Ready)
            || parent_status.active_run_id.is_some()
            || parent_status.last_sequence != request.expected_parent_sequence
        {
            return Err(AgentHostError::SessionForkConflict);
        }
        let parent = self.store.session(&request.parent_session_id)?;
        if parent.agent_id != request.agent_id || !Self::session_can_fork(&parent) {
            return Err(AgentHostError::SessionForkUnavailable);
        }
        let agent = self
            .agents
            .get(&parent.agent_id)
            .cloned()
            .ok_or(AgentHostError::UnknownAgent)?;
        if agent.protocol.profile() != parent.negotiation.protocol_profile {
            return Err(AgentHostError::SessionForkUnavailable);
        }
        let preflight = self
            .run_preflight(&parent.agent_id, &request.working_directory)
            .await?;
        let session_id = format!("session_{}", Uuid::new_v4());
        let now_ms = (self.clock)()?;
        self.store.create_starting_session(
            &session_id,
            &parent.agent_id,
            &preflight.working_directory,
            &request.metadata_json,
            &agent.protocol.profile(),
            &preflight.authority,
            now_ms,
        )?;
        let (handle, opened) = spawn_session(
            agent,
            self.store.clone(),
            self.clock.clone(),
            session_id.clone(),
            preflight.working_directory.into(),
            SessionLaunch::Fork {
                native_parent_session_id: parent.native_session_id,
            },
            metadata,
            self.state_directory.clone(),
        );
        let opened = tokio::time::timeout(CONTROL_TIMEOUT, opened).await;
        let session = match opened {
            Ok(Ok(Ok(session))) => session,
            Ok(Ok(Err(error))) => {
                self.store
                    .set_lifecycle(&session_id, &AgentLifecycle::Failed, (self.clock)()?)?;
                return Err(error);
            }
            Ok(Err(_)) | Err(_) => {
                let (reply, _) = oneshot::channel();
                let _ = handle.commands.try_send(SessionCommand::Close { reply });
                self.store
                    .set_lifecycle(&session_id, &AgentLifecycle::Failed, (self.clock)()?)?;
                return Err(AgentHostError::TransportFailed);
            }
        };
        self.sessions.write().await.insert(session_id, handle);
        Ok(session)
    }

    pub async fn prompt(
        &self,
        context: boxology::CallContext,
        request: PromptRequest,
    ) -> Result<PromptAccepted, AgentHostError> {
        let _ = context;
        let handle = self.live_session(&request.session_id).await?;
        session_control(&handle, self.control_timeout, |reply| {
            SessionCommand::Prompt { request, reply }
        })
        .await
    }

    pub async fn read_events(
        &self,
        context: boxology::CallContext,
        request: ReadEventsRequest,
    ) -> Result<EventPage, AgentHostError> {
        let _ = context;
        self.store
            .read_events(&request.session_id, request.after_sequence, request.limit)
    }

    pub async fn resolve_permission(
        &self,
        context: boxology::CallContext,
        request: PermissionRequest,
    ) -> Result<PermissionResolution, AgentHostError> {
        let _ = context;
        self.store.permission_resolution(
            &request.session_id,
            &request.request_id,
            &request.native_request_json,
        )
    }

    pub async fn session_status(
        &self,
        context: boxology::CallContext,
        request: SessionReference,
    ) -> Result<SessionStatus, AgentHostError> {
        let _ = context;
        self.store.status(&request.session_id)
    }

    pub async fn list_sessions(
        &self,
        context: boxology::CallContext,
        request: ListAgentSessionsRequest,
    ) -> Result<AgentSessionCatalog, AgentHostError> {
        let _ = context;
        self.store.list_sessions(request.limit)
    }

    pub async fn read_diagnostics(
        &self,
        context: boxology::CallContext,
        request: ReadAgentDiagnosticsRequest,
    ) -> Result<AgentDiagnosticPage, AgentHostError> {
        let _ = context;
        self.store
            .read_diagnostics(&request.session_id, request.after_sequence, request.limit)
    }

    pub async fn cancel_run(
        &self,
        context: boxology::CallContext,
        request: RunReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = context;
        let handle = self.live_session(&request.session_id).await?;
        session_control(&handle, self.control_timeout, |reply| {
            SessionCommand::Cancel {
                run_id: request.run_id,
                reply,
            }
        })
        .await
    }

    pub async fn detach_sessions(
        &self,
        context: boxology::CallContext,
        request: DetachSessionsRequest,
    ) -> Result<DetachSessionsReport, AgentHostError> {
        let _ = (context, request);
        Ok(self.detach_live_sessions().await)
    }

    pub async fn close_session(
        &self,
        context: boxology::CallContext,
        request: SessionReference,
    ) -> Result<OperationReceipt, AgentHostError> {
        let _ = context;
        let handle = self.live_session(&request.session_id).await?;
        let result = session_control(&handle, self.control_timeout, |reply| {
            SessionCommand::Close { reply }
        })
        .await?;
        self.sessions.write().await.remove(&request.session_id);
        Ok(result)
    }
}

async fn session_control<T>(
    handle: &SessionHandle,
    timeout: Duration,
    command: impl FnOnce(oneshot::Sender<Result<T, AgentHostError>>) -> SessionCommand,
) -> Result<T, AgentHostError> {
    tokio::time::timeout(timeout, async {
        let _control = handle.control.lock().await;
        let (reply, response) = oneshot::channel();
        handle
            .commands
            .send(command(reply))
            .await
            .map_err(|_| AgentHostError::SessionClosed)?;
        response.await.map_err(|_| AgentHostError::SessionClosed)?
    })
    .await
    .map_err(|_| AgentHostError::TransportFailed)?
}

impl Drop for AgentHost {
    fn drop(&mut self) {
        if let Ok(sessions) = self.sessions.try_read() {
            for handle in sessions.values() {
                let (reply, _) = oneshot::channel();
                let _ = handle.commands.try_send(SessionCommand::Detach { reply });
            }
        }
    }
}

fn system_time_ms() -> Result<u64, AgentHostError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| AgentHostError::StorageUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| AgentHostError::StorageUnavailable)
}

fn store_state_directory(path: &Path) -> Result<PathBuf, AgentHostError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::canonicalize(parent).map_err(|_| AgentHostError::StorageUnavailable)
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use std::{future::pending, path::Path, sync::Arc, time::Duration};

    use async_trait::async_trait;
    use boxology_contract::{CallContext, Caller, CancelToken, CapabilityId, TraceContext};

    use super::{
        AgentHost, AgentHostError, AgentProtocol, AuthorityAttestation, AuthorityProbeConfig,
        AuthorityVerifier, ConfiguredAgent, ListAgentSessionsRequest, OpenSessionRequest,
        SessionCommand, SessionHandle, generated, session_control,
    };
    use tokio::sync::{Mutex, mpsc, oneshot};

    struct HangingAuthority;

    #[async_trait]
    impl AuthorityVerifier for HangingAuthority {
        async fn verify(
            &self,
            _agent: &ConfiguredAgent,
            _working_directory: &Path,
            _now_ms: u64,
        ) -> Result<AuthorityAttestation, AgentHostError> {
            pending().await
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

    #[test]
    fn live_host_declares_the_complete_surface_and_rejects_duplicate_configuration() {
        let capabilities = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().clone())
            .collect::<Vec<CapabilityId>>();
        let names = capabilities
            .iter()
            .map(|capability| capability.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            names,
            [
                "discover_agents",
                "preflight",
                "open_session",
                "resume_session",
                "fork_session",
                "prompt",
                "read_events",
                "resolve_permission",
                "session_status",
                "list_sessions",
                "read_diagnostics",
                "cancel_run",
                "detach_sessions",
                "close_session",
            ]
        );
        let host = AgentHost::open_in_memory(Vec::new()).expect("empty catalog is valid");
        drop(host);
        assert_eq!(AgentHostError::UnknownAgent, AgentHostError::UnknownAgent);
    }

    #[tokio::test]
    async fn authority_timeout_fails_closed_before_session_state_exists() {
        let directory = tempfile::tempdir().expect("temporary working directory");
        let executable = std::env::current_exe().expect("test executable path");
        let agent = ConfiguredAgent::new(
            "hanging",
            "Hanging authority fixture",
            &executable,
            AgentProtocol::V1,
            AuthorityProbeConfig::new(executable.clone()),
        );
        let mut host = AgentHost::open_in_memory_with_authority_verifier(
            vec![agent],
            Arc::new(HangingAuthority),
        )
        .expect("host opens");
        host.authority_timeout = Duration::from_millis(20);

        let result = host
            .open_session(
                context(),
                OpenSessionRequest {
                    agent_id: "hanging".into(),
                    working_directory: directory.path().to_string_lossy().into_owned(),
                    bootstrap_prompt: None,
                    metadata_json: "{}".into(),
                },
            )
            .await;

        assert_eq!(result, Err(AgentHostError::AuthorityUnavailable));
        let catalog = host
            .list_sessions(context(), ListAgentSessionsRequest { limit: 1 })
            .await
            .expect("session catalog remains readable");
        assert_eq!(catalog.total_sessions, 0);
        assert!(catalog.sessions.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn control_deadline_includes_serialization_lock() {
        let (commands, _receiver) = mpsc::channel(1);
        let handle = SessionHandle {
            commands,
            control: Arc::new(Mutex::new(())),
        };
        let _held = handle.control.clone().lock_owned().await;
        let timeout = Duration::from_secs(30);
        let started = tokio::time::Instant::now();

        let result =
            session_control(&handle, timeout, |reply| SessionCommand::Close { reply }).await;

        assert_eq!(result, Err(AgentHostError::TransportFailed));
        assert_eq!(started.elapsed(), timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn control_deadline_includes_saturated_queue_admission() {
        let (commands, _receiver) = mpsc::channel(1);
        let (blocking_reply, _) = oneshot::channel();
        assert!(
            commands
                .try_send(SessionCommand::Close {
                    reply: blocking_reply,
                })
                .is_ok()
        );
        let handle = SessionHandle {
            commands,
            control: Arc::new(Mutex::new(())),
        };
        let timeout = Duration::from_secs(30);
        let started = tokio::time::Instant::now();

        let result =
            session_control(&handle, timeout, |reply| SessionCommand::Close { reply }).await;

        assert_eq!(result, Err(AgentHostError::TransportFailed));
        assert_eq!(started.elapsed(), timeout);
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_times_out_stalled_sessions_concurrently() {
        let mut host = AgentHost::open_in_memory(Vec::new()).expect("empty host opens");
        host.control_timeout = Duration::from_secs(30);
        let mut held = Vec::new();
        for session_id in ["session-c", "session-a", "session-b"] {
            let (commands, _receiver) = mpsc::channel(1);
            let handle = SessionHandle {
                commands,
                control: Arc::new(Mutex::new(())),
            };
            held.push(handle.control.clone().lock_owned().await);
            host.sessions
                .write()
                .await
                .insert(session_id.into(), handle);
        }
        let started = tokio::time::Instant::now();

        let report = host.detach_live_sessions().await;

        assert_eq!(started.elapsed(), host.control_timeout);
        assert!(report.detached_session_ids.is_empty());
        assert_eq!(
            report.failed_session_ids,
            ["session-a", "session-b", "session-c"]
        );
    }
}
