mod contract;
mod store;

pub use contract::*;

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use boxology_contract::{CallContext, Caller, CancelToken, ErasedCallError, TraceContext};
use boxology_import_agent_host::{
    AcpEventDirection, AcpEventKind, AgentInputMode, AgentLifecycle, OpenSessionRequest,
    PromptDisposition, PromptRequest, ReadEventsRequest, ResumeSessionRequest, RunReference,
    SessionReference,
};
use generated::AgentHostImport;
use serde_json::{Value, json};
use store::{InteractionDirection, RecoveryCandidate, SubAgentStore, spawn_fingerprint};
use tokio::{sync::Mutex, task::JoinHandle};

const EVENT_PAGE_LIMIT: u64 = 1_000;
const MAX_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
const PUMP_INTERVAL: Duration = Duration::from_millis(25);
const PUMP_FAILURE_LIMIT: u8 = 3;
const INITIAL_TASK_MESSAGE_ID: &str = "__initial_task__";

type Clock = Arc<dyn Fn() -> Result<u64, SubAgentHostError> + Send + Sync>;

/// Opened durable state waiting for composition-owned `agent-host` import injection.
pub struct SubAgentHostState {
    store: SubAgentStore,
}

impl SubAgentHostState {
    /// Open file-backed sub-agent state before assembling the Boxology graph.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SubAgentHostError> {
        Ok(Self {
            store: SubAgentStore::open(path)?,
        })
    }

    /// Open ephemeral sub-agent state before assembling the Boxology graph.
    pub fn open_in_memory() -> Result<Self, SubAgentHostError> {
        Ok(Self {
            store: SubAgentStore::open_in_memory()?,
        })
    }

    /// Attach the composition-selected agent host.
    #[must_use]
    pub fn connect(self, agent_host: AgentHostImport) -> SubAgentHost {
        SubAgentHost {
            agent_host: Arc::new(agent_host),
            store: Arc::new(self.store),
            spawn_operations: Arc::new(Mutex::new(())),
            pumps: Arc::new(StdMutex::new(HashMap::new())),
            clock: Arc::new(system_time_ms),
        }
    }
}

/// Crab-owned orchestration for independently supervised ACP child sessions.
pub struct SubAgentHost {
    agent_host: Arc<AgentHostImport>,
    store: Arc<SubAgentStore>,
    spawn_operations: Arc<Mutex<()>>,
    pumps: Arc<StdMutex<HashMap<String, JoinHandle<()>>>>,
    clock: Clock,
}

impl SubAgentHost {
    /// Open a file-backed host and attach a generated agent-host import.
    pub fn open(
        path: impl AsRef<Path>,
        agent_host: AgentHostImport,
    ) -> Result<Self, SubAgentHostError> {
        Ok(SubAgentHostState::open(path)?.connect(agent_host))
    }

    /// Open an in-memory host while retaining real agent-host dispatch.
    pub fn open_in_memory(agent_host: AgentHostImport) -> Result<Self, SubAgentHostError> {
        Ok(SubAgentHostState::open_in_memory()?.connect(agent_host))
    }

    async fn parent_status(
        &self,
        context: CallContext,
        parent_session_id: &str,
    ) -> Result<boxology_import_agent_host::SessionStatus, SubAgentHostError> {
        let status = self
            .agent_host
            .session_status(
                context,
                SessionReference {
                    session_id: parent_session_id.to_owned(),
                },
            )
            .await
            .map_err(map_parent_call)?;
        match status.lifecycle {
            AgentLifecycle::Ready | AgentLifecycle::Busy => Ok(status),
            _ => Err(SubAgentHostError::UnknownParentSession),
        }
    }

    async fn portable_snapshot(
        &self,
        context: CallContext,
        parent_session_id: &str,
        through_sequence: u64,
    ) -> Result<String, SubAgentHostError> {
        let mut cursor = 0;
        let mut visible = Vec::new();
        while cursor < through_sequence {
            let remaining = through_sequence - cursor;
            let page = self
                .agent_host
                .read_events(
                    context.clone(),
                    ReadEventsRequest {
                        session_id: parent_session_id.to_owned(),
                        after_sequence: cursor,
                        limit: remaining.min(EVENT_PAGE_LIMIT),
                    },
                )
                .await
                .map_err(map_parent_call)?;
            if page.events.is_empty() || page.next_sequence <= cursor {
                return Err(SubAgentHostError::InvalidContextBoundary);
            }
            for event in page
                .events
                .into_iter()
                .filter(|event| event.sequence <= through_sequence)
                .filter(|event| matches!(event.kind, AcpEventKind::Message))
            {
                visible.push(json!({
                    "sequence": event.sequence,
                    "direction": match event.direction {
                        AcpEventDirection::ClientToAgent => "client_to_agent",
                        AcpEventDirection::AgentToClient => "agent_to_client",
                        AcpEventDirection::Unknown { .. } => {
                            return Err(SubAgentHostError::InvalidNativePayload);
                        }
                    },
                    "nativeEvent": parse_json(&event.native_event_json)?,
                }));
            }
            cursor = page.next_sequence.min(through_sequence);
        }
        let snapshot =
            serde_json::to_string(&visible).map_err(|_| SubAgentHostError::InvalidNativePayload)?;
        if snapshot.len() > MAX_SNAPSHOT_BYTES {
            return Err(SubAgentHostError::InvalidContextBoundary);
        }
        let escaped = snapshot.replace("]]>", "]]]]><![CDATA[>");
        Ok(format!(
            "<crab_parent_context realization=\"portable_snapshot\" through_sequence=\"{through_sequence}\"><![CDATA[{escaped}]]></crab_parent_context>"
        ))
    }

    fn start_pump(&self, sub_agent_id: &str, child_session_id: &str) {
        let agent_host = self.agent_host.clone();
        let store = self.store.clone();
        let clock = self.clock.clone();
        let sub_agent_id = sub_agent_id.to_owned();
        let child_session_id = child_session_id.to_owned();
        let pump_key = sub_agent_id.clone();
        let handle = tokio::spawn(async move {
            let mut consecutive_failures = 0_u8;
            while let Ok(cursor) = store.child_cursor(&sub_agent_id) {
                let page = agent_host
                    .read_events(
                        background_context(),
                        ReadEventsRequest {
                            session_id: child_session_id.clone(),
                            after_sequence: cursor,
                            limit: EVENT_PAGE_LIMIT,
                        },
                    )
                    .await;
                let page = match page {
                    Ok(page) => {
                        consecutive_failures = 0;
                        page
                    }
                    Err(error) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures >= PUMP_FAILURE_LIMIT {
                            let reason =
                                format!("agent-host event pump failed: {}", error_label(&error));
                            if let Ok(now_ms) = clock() {
                                let _ = store.set_lifecycle(
                                    &sub_agent_id,
                                    &SubAgentLifecycle::Failed,
                                    Some(&reason),
                                    now_ms,
                                );
                            }
                            break;
                        }
                        tokio::time::sleep(PUMP_INTERVAL).await;
                        continue;
                    }
                };
                let mut failed = false;
                for event in &page.events {
                    if store.record_native_event(&sub_agent_id, event).is_err() {
                        failed = true;
                        break;
                    }
                }
                if failed {
                    if let Ok(now_ms) = clock() {
                        let _ = store.set_lifecycle(
                            &sub_agent_id,
                            &SubAgentLifecycle::Failed,
                            Some("native ACP event sequence became inconsistent"),
                            now_ms,
                        );
                    }
                    break;
                }
                let status = agent_host
                    .session_status(
                        background_context(),
                        SessionReference {
                            session_id: child_session_id.clone(),
                        },
                    )
                    .await;
                let lifecycle = match status {
                    Ok(status) => match status.lifecycle {
                        AgentLifecycle::Discovered | AgentLifecycle::Starting => {
                            SubAgentLifecycle::Starting
                        }
                        AgentLifecycle::Ready => SubAgentLifecycle::Idle,
                        AgentLifecycle::Busy => SubAgentLifecycle::Running,
                        AgentLifecycle::Stopping => SubAgentLifecycle::Stopping,
                        AgentLifecycle::Stopped => SubAgentLifecycle::Completed,
                        AgentLifecycle::Failed | AgentLifecycle::Unknown { .. } => {
                            SubAgentLifecycle::Failed
                        }
                    },
                    Err(error) => {
                        consecutive_failures = consecutive_failures.saturating_add(1);
                        if consecutive_failures < PUMP_FAILURE_LIMIT {
                            tokio::time::sleep(PUMP_INTERVAL).await;
                            continue;
                        }
                        let reason =
                            format!("agent-host status pump failed: {}", error_label(&error));
                        if let Ok(now_ms) = clock() {
                            let _ = store.set_lifecycle(
                                &sub_agent_id,
                                &SubAgentLifecycle::Failed,
                                Some(&reason),
                                now_ms,
                            );
                        }
                        break;
                    }
                };
                let terminal = matches!(
                    lifecycle,
                    SubAgentLifecycle::Completed | SubAgentLifecycle::Failed
                );
                if let Ok(now_ms) = clock() {
                    let _ = store.set_lifecycle(&sub_agent_id, &lifecycle, None, now_ms);
                }
                if terminal {
                    break;
                }
                if page.caught_up {
                    tokio::time::sleep(PUMP_INTERVAL).await;
                }
            }
        });
        let mut pumps = self.pumps.lock().expect("sub-agent pump lock poisoned");
        if let Some(previous) = pumps.insert(pump_key, handle) {
            previous.abort();
        }
    }

    async fn dispatch_input(
        &self,
        context: CallContext,
        session_id: &str,
        client_turn_id: &str,
        mode: &SubAgentInputMode,
        native_prompt_json: &str,
        unknown_session: SubAgentHostError,
    ) -> Result<(InputDisposition, u64), SubAgentHostError> {
        let (agent_mode, interrupted) = match mode {
            SubAgentInputMode::Queue => (AgentInputMode::Queue, false),
            SubAgentInputMode::Steer => (AgentInputMode::Steer, false),
            SubAgentInputMode::InterruptAndSteer => {
                let status = self
                    .agent_host
                    .session_status(
                        context.clone(),
                        SessionReference {
                            session_id: session_id.to_owned(),
                        },
                    )
                    .await
                    .map_err(|error| map_session_call(error, unknown_session.clone()))?;
                let mut interrupted = false;
                if let Some(run_id) = status.active_run_id {
                    let receipt = self
                        .agent_host
                        .cancel_run(
                            context.clone(),
                            RunReference {
                                session_id: session_id.to_owned(),
                                run_id,
                            },
                        )
                        .await
                        .map_err(|error| map_session_call(error, unknown_session.clone()))?;
                    interrupted = receipt.accepted;
                }
                (AgentInputMode::Queue, interrupted)
            }
            SubAgentInputMode::Unknown { .. } => {
                return Err(SubAgentHostError::InvalidNativePayload);
            }
        };
        let accepted = self
            .agent_host
            .prompt(
                context,
                PromptRequest {
                    session_id: session_id.to_owned(),
                    client_turn_id: client_turn_id.to_owned(),
                    mode: agent_mode,
                    native_prompt_json: native_prompt_json.to_owned(),
                },
            )
            .await
            .map_err(|error| map_session_call(error, unknown_session))?;
        let disposition = if interrupted {
            InputDisposition::CancelRequestedThenQueued
        } else {
            map_disposition(accepted.disposition)?
        };
        Ok((disposition, accepted.accepted_at_ms))
    }

    fn fail_recovery(
        &self,
        candidate: &RecoveryCandidate,
        disposition: SubAgentRecoveryDisposition,
        reason: &str,
        attempted: bool,
    ) -> Result<SubAgentRecovery, SubAgentHostError> {
        self.store.fail_recovery(
            &candidate.record.sub_agent_id,
            reason,
            attempted,
            (self.clock)()?,
        )?;
        Ok(SubAgentRecovery {
            sub_agent_id: candidate.record.sub_agent_id.clone(),
            child_session_id: candidate.record.child_session_id.clone(),
            disposition,
        })
    }

    async fn recover_candidate(
        &self,
        context: CallContext,
        candidate: &RecoveryCandidate,
    ) -> Result<SubAgentRecovery, SubAgentHostError> {
        if candidate.crash_restart_limit == 0 {
            return self.fail_recovery(
                candidate,
                SubAgentRecoveryDisposition::RecoveryDisabled,
                "runtime restarted; child recovery disabled",
                false,
            );
        }
        if candidate.restart_count >= candidate.crash_restart_limit {
            return self.fail_recovery(
                candidate,
                SubAgentRecoveryDisposition::RestartBudgetExhausted,
                "runtime restarted; child recovery budget exhausted",
                false,
            );
        }
        if candidate.record.child_session_id.trim().is_empty()
            || candidate.record.native_child_session_id.trim().is_empty()
        {
            return self.fail_recovery(
                candidate,
                SubAgentRecoveryDisposition::IdentityMismatch,
                "runtime restarted; durable child identity is incomplete",
                false,
            );
        }
        match self
            .agent_host
            .session_status(
                context.clone(),
                SessionReference {
                    session_id: candidate.record.parent_session_id.clone(),
                },
            )
            .await
        {
            Ok(status)
                if status.session_id == candidate.record.parent_session_id
                    && matches!(
                        status.lifecycle,
                        AgentLifecycle::Ready | AgentLifecycle::Busy
                    ) => {}
            _ => {
                return self.fail_recovery(
                    candidate,
                    SubAgentRecoveryDisposition::ParentUnavailable,
                    "runtime restarted; parent session is unavailable",
                    false,
                );
            }
        }
        let resumed = match self
            .agent_host
            .resume_session(
                context.clone(),
                ResumeSessionRequest {
                    session_id: candidate.record.child_session_id.clone(),
                },
            )
            .await
        {
            Ok(resumed) => resumed,
            Err(error)
                if has_domain_tag(&error, "SessionResumeUnavailable")
                    || has_domain_tag(&error, "UnknownSession") =>
            {
                return self.fail_recovery(
                    candidate,
                    SubAgentRecoveryDisposition::SessionUnavailable,
                    "runtime restarted; native child session cannot be resumed",
                    true,
                );
            }
            Err(error) => {
                return self.fail_recovery(
                    candidate,
                    SubAgentRecoveryDisposition::Failed,
                    hard_recovery_reason(&error),
                    true,
                );
            }
        };
        if resumed.session_id != candidate.record.child_session_id
            || resumed.native_session_id != candidate.record.native_child_session_id
            || resumed.agent_id != candidate.record.agent_id
        {
            if resumed.session_id == candidate.record.child_session_id {
                let _ = self
                    .agent_host
                    .close_session(
                        context,
                        SessionReference {
                            session_id: candidate.record.child_session_id.clone(),
                        },
                    )
                    .await;
            }
            return self.fail_recovery(
                candidate,
                SubAgentRecoveryDisposition::IdentityMismatch,
                "runtime restarted; resumed child identity changed",
                true,
            );
        }
        let lifecycle = match self
            .agent_host
            .session_status(
                context.clone(),
                SessionReference {
                    session_id: candidate.record.child_session_id.clone(),
                },
            )
            .await
        {
            Ok(status)
                if status.session_id == candidate.record.child_session_id
                    && matches!(status.lifecycle, AgentLifecycle::Ready) =>
            {
                SubAgentLifecycle::Idle
            }
            Ok(status)
                if status.session_id == candidate.record.child_session_id
                    && matches!(status.lifecycle, AgentLifecycle::Busy) =>
            {
                SubAgentLifecycle::Running
            }
            _ => {
                let _ = self
                    .agent_host
                    .close_session(
                        context,
                        SessionReference {
                            session_id: candidate.record.child_session_id.clone(),
                        },
                    )
                    .await;
                return self.fail_recovery(
                    candidate,
                    SubAgentRecoveryDisposition::Failed,
                    "runtime restarted; resumed child did not become available",
                    true,
                );
            }
        };
        let record = match self.store.complete_recovery(
            &candidate.record.sub_agent_id,
            &candidate.record.child_session_id,
            &candidate.record.native_child_session_id,
            &lifecycle,
            (self.clock)()?,
        ) {
            Ok(record) => record,
            Err(error) => {
                let _ = self
                    .agent_host
                    .close_session(
                        context,
                        SessionReference {
                            session_id: candidate.record.child_session_id.clone(),
                        },
                    )
                    .await;
                return Err(error);
            }
        };
        self.start_pump(&record.sub_agent_id, &record.child_session_id);
        Ok(SubAgentRecovery {
            sub_agent_id: record.sub_agent_id,
            child_session_id: record.child_session_id,
            disposition: SubAgentRecoveryDisposition::Resumed,
        })
    }
}

impl Drop for SubAgentHost {
    fn drop(&mut self) {
        if let Ok(mut pumps) = self.pumps.lock() {
            for (_, handle) in pumps.drain() {
                handle.abort();
            }
        }
    }
}

#[boxology::implementation]
impl SubAgentHost {
    pub async fn spawn(
        &self,
        context: CallContext,
        request: SpawnSubAgentRequest,
    ) -> Result<SubAgentRecord, SubAgentHostError> {
        let _spawn = self.spawn_operations.lock().await;
        validate_spawn(&request)?;
        let fingerprint = spawn_fingerprint(&request)?;
        if let Some(existing) = self
            .store
            .existing_spawn(&request.client_sub_agent_id, &fingerprint)?
        {
            return Ok(existing);
        }
        let parent_status = self
            .parent_status(context.clone(), &request.parent_session_id)
            .await?;
        let (realization, bootstrap_prompt) = match request.context_mode {
            SubAgentContextMode::Fresh => (ContextRealization::FreshSession, None),
            SubAgentContextMode::InheritParent => {
                if !request.allow_portable_snapshot {
                    return Err(SubAgentHostError::PortableSnapshotForbidden);
                }
                let boundary = request
                    .parent_context_through_sequence
                    .ok_or(SubAgentHostError::InvalidContextBoundary)?;
                if boundary > parent_status.last_sequence {
                    return Err(SubAgentHostError::InvalidContextBoundary);
                }
                (
                    ContextRealization::PortableSnapshot,
                    Some(
                        self.portable_snapshot(
                            context.clone(),
                            &request.parent_session_id,
                            boundary,
                        )
                        .await?,
                    ),
                )
            }
            SubAgentContextMode::Unknown { .. } => {
                return Err(SubAgentHostError::InvalidNativePayload);
            }
        };
        let now_ms = (self.clock)()?;
        let start = self
            .store
            .begin_spawn(&request, &realization, &fingerprint, now_ms)?;
        if !start.inserted {
            return Ok(start.record);
        }
        let metadata_json = sub_agent_metadata(&request, &start.record.sub_agent_id)?;
        let child = match self
            .agent_host
            .open_session(
                context.clone(),
                OpenSessionRequest {
                    agent_id: request.agent_id.clone(),
                    working_directory: request.working_directory.clone(),
                    bootstrap_prompt,
                    metadata_json,
                },
            )
            .await
        {
            Ok(child) => child,
            Err(error) => {
                let mapped = map_child_call(error);
                self.store.fail_spawn(
                    &start.record.sub_agent_id,
                    &format!("child session open failed: {mapped:?}"),
                    (self.clock)()?,
                )?;
                return Err(mapped);
            }
        };
        let record = self.store.complete_spawn(
            &start.record.sub_agent_id,
            &child.session_id,
            &child.native_session_id,
            (self.clock)()?,
        )?;
        self.store.begin_interaction(
            &record.sub_agent_id,
            INITIAL_TASK_MESSAGE_ID,
            &InteractionDirection::ParentToChild,
            &SubAgentInputMode::Queue,
            &request.native_task_prompt_json,
            (self.clock)()?,
        )?;
        let initial_turn_id = format!("subagent:{}:initial", record.sub_agent_id);
        let accepted = self
            .agent_host
            .prompt(
                context.clone(),
                PromptRequest {
                    session_id: child.session_id.clone(),
                    client_turn_id: initial_turn_id,
                    mode: AgentInputMode::Queue,
                    native_prompt_json: request.native_task_prompt_json.clone(),
                },
            )
            .await;
        let accepted = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                let mapped = map_child_call(error);
                let _ = self
                    .agent_host
                    .close_session(
                        context,
                        SessionReference {
                            session_id: child.session_id,
                        },
                    )
                    .await;
                self.store.fail_spawn(
                    &record.sub_agent_id,
                    &format!("initial child task failed: {mapped:?}"),
                    (self.clock)()?,
                )?;
                return Err(mapped);
            }
        };
        self.store.complete_interaction(
            &record.sub_agent_id,
            INITIAL_TASK_MESSAGE_ID,
            &InteractionDirection::ParentToChild,
            &map_disposition(accepted.disposition)?,
            accepted.accepted_at_ms,
        )?;
        self.start_pump(&record.sub_agent_id, &record.child_session_id);
        Ok(record)
    }

    pub async fn send_to_child(
        &self,
        context: CallContext,
        request: SendToChildRequest,
    ) -> Result<InteractionReceipt, SubAgentHostError> {
        validate_identifier(&request.client_message_id)?;
        validate_prompt(&request.native_prompt_json)?;
        let record = self.store.record(&request.sub_agent_id)?;
        let now_ms = (self.clock)()?;
        let start = self.store.begin_interaction(
            &request.sub_agent_id,
            &request.client_message_id,
            &InteractionDirection::ParentToChild,
            &request.mode,
            &request.native_prompt_json,
            now_ms,
        )?;
        if let Some(receipt) = start.completed {
            return Ok(receipt);
        }
        debug_assert!(start.should_dispatch);
        let client_turn_id = format!(
            "subagent:{}:parent:{}",
            request.sub_agent_id, request.client_message_id
        );
        let (disposition, accepted_at_ms) = self
            .dispatch_input(
                context,
                &record.child_session_id,
                &client_turn_id,
                &request.mode,
                &request.native_prompt_json,
                SubAgentHostError::UnknownSubAgent,
            )
            .await?;
        self.store.complete_interaction(
            &request.sub_agent_id,
            &request.client_message_id,
            &InteractionDirection::ParentToChild,
            &disposition,
            accepted_at_ms,
        )
    }

    pub async fn send_to_parent(
        &self,
        context: CallContext,
        request: SendToParentRequest,
    ) -> Result<InteractionReceipt, SubAgentHostError> {
        validate_identifier(&request.client_message_id)?;
        let message = parse_json(&request.message_json)?;
        let record = self.store.record(&request.sub_agent_id)?;
        let now_ms = (self.clock)()?;
        let start = self.store.begin_interaction(
            &request.sub_agent_id,
            &request.client_message_id,
            &InteractionDirection::ChildToParent,
            &request.mode,
            &request.message_json,
            now_ms,
        )?;
        if let Some(receipt) = start.completed {
            return Ok(receipt);
        }
        debug_assert!(start.should_dispatch);
        let native_prompt_json = serde_json::to_string(&vec![json!({
            "type": "text",
            "text": format!(
                "[Crab sub-agent {}]\n{}",
                request.sub_agent_id,
                serde_json::to_string(&message)
                    .map_err(|_| SubAgentHostError::InvalidNativePayload)?
            ),
        })])
        .map_err(|_| SubAgentHostError::InvalidNativePayload)?;
        let client_turn_id = format!(
            "subagent:{}:child:{}",
            request.sub_agent_id, request.client_message_id
        );
        let (disposition, accepted_at_ms) = self
            .dispatch_input(
                context,
                &record.parent_session_id,
                &client_turn_id,
                &request.mode,
                &native_prompt_json,
                SubAgentHostError::UnknownParentSession,
            )
            .await?;
        self.store.complete_interaction(
            &request.sub_agent_id,
            &request.client_message_id,
            &InteractionDirection::ChildToParent,
            &disposition,
            accepted_at_ms,
        )
    }

    pub async fn read_events(
        &self,
        context: CallContext,
        request: ReadSubAgentEventsRequest,
    ) -> Result<SubAgentEventPage, SubAgentHostError> {
        let _ = context;
        self.store.read_events(&request)
    }

    pub async fn status(
        &self,
        context: CallContext,
        request: SubAgentReference,
    ) -> Result<SubAgentStatus, SubAgentHostError> {
        let _ = context;
        self.store.status(&request.sub_agent_id)
    }

    pub async fn recover(
        &self,
        context: CallContext,
        request: RecoverSubAgentsRequest,
    ) -> Result<SubAgentRecoveryReport, SubAgentHostError> {
        let _ = request;
        let _spawn = self.spawn_operations.lock().await;
        let candidates = self.store.recovery_candidates()?;
        let mut recoveries = Vec::with_capacity(candidates.len());
        for candidate in &candidates {
            match self.recover_candidate(context.clone(), candidate).await {
                Ok(recovery) => recoveries.push(recovery),
                Err(error) => {
                    for recovery in &recoveries {
                        if !matches!(recovery.disposition, SubAgentRecoveryDisposition::Resumed) {
                            continue;
                        }
                        if let Ok(mut pumps) = self.pumps.lock()
                            && let Some(handle) = pumps.remove(&recovery.sub_agent_id)
                        {
                            handle.abort();
                        }
                        let _ = self
                            .agent_host
                            .close_session(
                                context.clone(),
                                SessionReference {
                                    session_id: recovery.child_session_id.clone(),
                                },
                            )
                            .await;
                        if let Ok(now_ms) = (self.clock)() {
                            let _ = self.store.set_lifecycle(
                                &recovery.sub_agent_id,
                                &SubAgentLifecycle::Failed,
                                Some("sub-agent recovery batch aborted"),
                                now_ms,
                            );
                        }
                    }
                    return Err(error);
                }
            }
        }
        Ok(SubAgentRecoveryReport { recoveries })
    }

    pub async fn stop(
        &self,
        context: CallContext,
        request: StopSubAgentRequest,
    ) -> Result<SubAgentReceipt, SubAgentHostError> {
        if request.reason.trim().is_empty() {
            return Err(SubAgentHostError::InvalidNativePayload);
        }
        let record = self.store.record(&request.sub_agent_id)?;
        let now_ms = (self.clock)()?;
        if matches!(
            record.lifecycle,
            SubAgentLifecycle::Completed | SubAgentLifecycle::Failed
        ) {
            return Ok(SubAgentReceipt {
                accepted: false,
                recorded_at_ms: now_ms,
            });
        }
        self.store.set_lifecycle(
            &request.sub_agent_id,
            &SubAgentLifecycle::Stopping,
            None,
            now_ms,
        )?;
        self.agent_host
            .close_session(
                context,
                SessionReference {
                    session_id: record.child_session_id,
                },
            )
            .await
            .map_err(map_child_call)?;
        if let Some(handle) = self
            .pumps
            .lock()
            .map_err(|_| SubAgentHostError::StorageUnavailable)?
            .remove(&request.sub_agent_id)
        {
            handle.abort();
        }
        let recorded_at_ms = (self.clock)()?;
        self.store.stop(&request.sub_agent_id, recorded_at_ms)?;
        Ok(SubAgentReceipt {
            accepted: true,
            recorded_at_ms,
        })
    }
}

fn validate_spawn(request: &SpawnSubAgentRequest) -> Result<(), SubAgentHostError> {
    validate_identifier(&request.client_sub_agent_id)?;
    validate_identifier(&request.parent_session_id)?;
    validate_identifier(&request.agent_id)?;
    if request.working_directory.trim().is_empty() {
        return Err(SubAgentHostError::InvalidNativePayload);
    }
    validate_prompt(&request.native_task_prompt_json)?;
    let metadata = parse_json(&request.metadata_json)?;
    if !metadata.is_object() {
        return Err(SubAgentHostError::InvalidNativePayload);
    }
    match request.context_mode {
        SubAgentContextMode::Fresh if request.parent_context_through_sequence.is_some() => {
            Err(SubAgentHostError::InvalidContextBoundary)
        }
        SubAgentContextMode::Fresh | SubAgentContextMode::InheritParent => Ok(()),
        SubAgentContextMode::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn sub_agent_metadata(
    request: &SpawnSubAgentRequest,
    sub_agent_id: &str,
) -> Result<String, SubAgentHostError> {
    let Value::Object(mut metadata) = parse_json(&request.metadata_json)? else {
        return Err(SubAgentHostError::InvalidNativePayload);
    };
    metadata.insert(
        "crabSubAgent".into(),
        json!({
            "subAgentId": sub_agent_id,
            "parentSessionId": request.parent_session_id,
            "contextMode": match request.context_mode {
                SubAgentContextMode::Fresh => "fresh",
                SubAgentContextMode::InheritParent => "inherit_parent",
                SubAgentContextMode::Unknown { .. } => {
                    return Err(SubAgentHostError::InvalidNativePayload);
                }
            },
        }),
    );
    serde_json::to_string(&Value::Object(metadata))
        .map_err(|_| SubAgentHostError::InvalidNativePayload)
}

fn validate_identifier(value: &str) -> Result<(), SubAgentHostError> {
    if value.trim().is_empty() {
        Err(SubAgentHostError::InvalidNativePayload)
    } else {
        Ok(())
    }
}

fn validate_prompt(value: &str) -> Result<(), SubAgentHostError> {
    if parse_json(value)?.is_array() {
        Ok(())
    } else {
        Err(SubAgentHostError::InvalidNativePayload)
    }
}

fn parse_json(value: &str) -> Result<Value, SubAgentHostError> {
    serde_json::from_str(value).map_err(|_| SubAgentHostError::InvalidNativePayload)
}

fn map_disposition(disposition: PromptDisposition) -> Result<InputDisposition, SubAgentHostError> {
    match disposition {
        PromptDisposition::StartedForegroundWork => Ok(InputDisposition::StartedForegroundWork),
        PromptDisposition::ContributedToActiveWork => Ok(InputDisposition::ContributedToActiveWork),
        PromptDisposition::QueuedForTurnBoundary => Ok(InputDisposition::QueuedForTurnBoundary),
        PromptDisposition::Unknown { .. } => Err(SubAgentHostError::InvalidNativePayload),
    }
}

fn map_parent_call(error: ErasedCallError) -> SubAgentHostError {
    map_session_call(error, SubAgentHostError::UnknownParentSession)
}

fn map_child_call(error: ErasedCallError) -> SubAgentHostError {
    map_session_call(error, SubAgentHostError::UnknownSubAgent)
}

fn map_session_call(
    error: ErasedCallError,
    unknown_session: SubAgentHostError,
) -> SubAgentHostError {
    match error {
        ErasedCallError::Domain { error_tag, .. }
            if matches!(error_tag.as_str(), "UnknownSession" | "SessionClosed") =>
        {
            unknown_session
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(error_tag.as_str(), "UnknownRun" | "SteeringUnavailable") =>
        {
            SubAgentHostError::SteeringUnavailable
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "PreflightFailed" | "AuthorityUnavailable" | "UnknownAgent"
            ) =>
        {
            SubAgentHostError::AuthorityUnavailable
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "ProtocolNegotiationFailed" | "UnsupportedProtocolProfile"
            ) =>
        {
            SubAgentHostError::ProtocolNegotiationFailed
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "InvalidNativePayload" | "DuplicateTurnConflict" | "InvalidCursor"
            ) =>
        {
            SubAgentHostError::InvalidNativePayload
        }
        _ => SubAgentHostError::TransportFailed,
    }
}

fn error_label(error: &ErasedCallError) -> &'static str {
    match error {
        ErasedCallError::Domain { .. } => "domain",
        ErasedCallError::Deadline => "deadline",
        ErasedCallError::Cancelled => "cancelled",
        ErasedCallError::Unavailable(_) => "unavailable",
        ErasedCallError::ContractViolation(_) => "contract_violation",
        ErasedCallError::InvalidResponse(_) => "invalid_response",
        ErasedCallError::Internal(_) => "internal",
        _ => "unknown",
    }
}

fn has_domain_tag(error: &ErasedCallError, expected: &str) -> bool {
    matches!(error, ErasedCallError::Domain { error_tag, .. } if error_tag == expected)
}

fn hard_recovery_reason(error: &ErasedCallError) -> &'static str {
    match error {
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "PreflightFailed" | "AuthorityUnavailable" | "UnknownAgent"
            ) =>
        {
            "runtime restarted; child authority recovery failed"
        }
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "ProtocolNegotiationFailed" | "UnsupportedProtocolProfile"
            ) =>
        {
            "runtime restarted; child protocol recovery failed"
        }
        ErasedCallError::Domain { .. } => "runtime restarted; child session recovery was rejected",
        _ => "runtime restarted; child recovery transport failed",
    }
}

fn background_context() -> CallContext {
    CallContext::new(
        Caller::System("sub-agent-host"),
        None,
        CancelToken::new(),
        TraceContext::empty(),
        None,
    )
}

fn system_time_ms() -> Result<u64, SubAgentHostError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SubAgentHostError::StorageUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| SubAgentHostError::StorageUnavailable)
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use super::{SubAgentContextMode, SubAgentInputMode, generated};

    #[test]
    fn contract_is_non_blocking_bidirectional_and_supervised() {
        let names = generated::implementation_descriptor()
            .contract()
            .capabilities()
            .iter()
            .map(|capability| capability.id().name().as_str())
            .collect::<Vec<_>>();

        assert_eq!(
            names,
            [
                "spawn",
                "send_to_child",
                "send_to_parent",
                "read_events",
                "status",
                "recover",
                "stop",
            ]
        );
        assert_ne!(
            SubAgentContextMode::Fresh,
            SubAgentContextMode::InheritParent
        );
        assert_ne!(SubAgentInputMode::Queue, SubAgentInputMode::Steer);
        assert_ne!(
            SubAgentInputMode::Steer,
            SubAgentInputMode::InterruptAndSteer
        );
        assert_eq!(generated::implementation_descriptor().imports().len(), 1);
    }
}
