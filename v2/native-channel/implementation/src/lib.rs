mod contract;
mod store;

pub use contract::*;

use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex, PoisonError, Weak},
    time::{SystemTime, UNIX_EPOCH},
};

use boxology_contract::{CallContext, ErasedCallError};
use boxology_import_agent_host::{
    AcpEvent, AcpEventDirection, AcpEventKind, AgentInputMode, AgentLifecycle, PromptDisposition,
    PromptRequest, ReadEventsRequest, RunReference, SessionReference,
};
use generated::AgentHostImport;
use store::ChannelStore;
use tokio::sync::{Mutex, OwnedMutexGuard};

const EVENT_PAGE_LIMIT: u64 = 1_000;
const MAX_CHANNEL_IDENTIFIER_BYTES: usize = 256;
const MAX_NATIVE_PROMPT_BYTES: usize = 2 * 1024 * 1024;

type Clock = Arc<dyn Fn() -> Result<u64, NativeChannelError> + Send + Sync>;

/// Opened durable state waiting for composition-owned `agent-host` import injection.
pub struct NativeChannelState {
    store: ChannelStore,
}

impl NativeChannelState {
    /// Open file-backed channel state before assembling the Boxology graph.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, NativeChannelError> {
        Ok(Self {
            store: ChannelStore::open(path)?,
        })
    }

    /// Open ephemeral channel state before assembling the Boxology graph.
    pub fn open_in_memory() -> Result<Self, NativeChannelError> {
        Ok(Self {
            store: ChannelStore::open_in_memory()?,
        })
    }

    /// Attach the composition-selected import without another fallible state transition.
    #[must_use]
    pub fn connect(self, agent_host: AgentHostImport) -> NativeChannel {
        NativeChannel {
            agent_host,
            store: Arc::new(self.store),
            operations: OperationLocks::default(),
            clock: Arc::new(system_time_ms),
        }
    }
}

/// Durable native-channel router over the generated `agent-host` capability handle.
pub struct NativeChannel {
    agent_host: AgentHostImport,
    store: Arc<ChannelStore>,
    operations: OperationLocks,
    clock: Clock,
}

impl NativeChannel {
    /// Open file-backed channel state.
    pub fn open(
        path: impl AsRef<Path>,
        agent_host: AgentHostImport,
    ) -> Result<Self, NativeChannelError> {
        Ok(NativeChannelState::open(path)?.connect(agent_host))
    }

    /// Open ephemeral channel state while retaining real agent-host dispatch.
    pub fn open_in_memory(agent_host: AgentHostImport) -> Result<Self, NativeChannelError> {
        Ok(NativeChannelState::open_in_memory()?.connect(agent_host))
    }

    async fn require_available_session(
        &self,
        context: CallContext,
        session_id: &str,
    ) -> Result<boxology_import_agent_host::SessionStatus, NativeChannelError> {
        let status = self
            .agent_host
            .session_status(
                context,
                SessionReference {
                    session_id: session_id.to_owned(),
                },
            )
            .await
            .map_err(map_host_call)?;
        match status.lifecycle {
            AgentLifecycle::Ready | AgentLifecycle::Busy => Ok(status),
            _ => Err(NativeChannelError::SessionUnavailable),
        }
    }

    async fn authoritative_page(
        &self,
        context: CallContext,
        binding: &ChannelBinding,
        after_sequence: u64,
        limit: u64,
    ) -> Result<PublishedEventPage, NativeChannelError> {
        let page = self
            .agent_host
            .read_events(
                context,
                ReadEventsRequest {
                    session_id: binding.session_id.clone(),
                    after_sequence,
                    limit,
                },
            )
            .await
            .map_err(map_host_call)?;
        let events = page
            .events
            .into_iter()
            .map(|event| map_event(&binding.binding_id, event))
            .collect::<Vec<_>>();
        Ok(PublishedEventPage {
            events,
            next_sequence: page.next_sequence,
            caught_up: page.caught_up,
        })
    }

    async fn refresh_turn_state(
        &self,
        context: CallContext,
        binding: &ChannelBinding,
    ) -> Result<boxology_import_agent_host::SessionStatus, NativeChannelError> {
        let mut cursor = self
            .store
            .reconciled_sequence(&binding.binding_id, &binding.session_id)?;
        loop {
            let page = self
                .agent_host
                .read_events(
                    context.clone(),
                    ReadEventsRequest {
                        session_id: binding.session_id.clone(),
                        after_sequence: cursor,
                        limit: EVENT_PAGE_LIMIT,
                    },
                )
                .await
                .map_err(map_host_call)?;
            let finished_runs = page
                .events
                .iter()
                .filter(|event| matches!(event.kind, AcpEventKind::RunFinished))
                .filter_map(|event| event.run_id.clone())
                .collect::<Vec<_>>();
            cursor = page.next_sequence;
            self.store.reconcile(
                &binding.binding_id,
                &binding.session_id,
                cursor,
                &finished_runs,
                None,
            )?;
            if page.caught_up {
                break;
            }
        }
        let status = self
            .require_available_session(context, &binding.session_id)
            .await?;
        self.store.reconcile(
            &binding.binding_id,
            &binding.session_id,
            cursor,
            &[],
            status.active_run_id.as_deref(),
        )?;
        Ok(status)
    }
}

#[boxology::implementation]
impl NativeChannel {
    pub async fn bind_channel(
        &self,
        context: CallContext,
        request: BindChannelRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _operation = self
            .operations
            .lock_attachment(&request.adapter_id, &request.channel_id)
            .await?;
        self.require_available_session(context, &request.session_id)
            .await?;
        self.store.bind(&request, (self.clock)()?)
    }

    pub async fn accept_turn(
        &self,
        context: CallContext,
        request: ChannelTurn,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        validate_turn(&request)?;
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        require_attached(&binding)?;
        if let Some(existing) = self
            .store
            .existing_turn(&request, &binding.session_id, None)?
        {
            return Ok(existing);
        }
        let accepted = self
            .agent_host
            .prompt(
                context,
                PromptRequest {
                    session_id: binding.session_id.clone(),
                    client_turn_id: request.client_turn_id.clone(),
                    mode: map_input_mode(&request.mode)?,
                    native_prompt_json: request.native_prompt_json.clone(),
                },
            )
            .await
            .map_err(map_prompt_call)?;
        let accepted = AcceptedTurn {
            binding_id: binding.binding_id,
            session_id: binding.session_id,
            client_turn_id: request.client_turn_id.clone(),
            accepted_at_ms: accepted.accepted_at_ms,
            mode: request.mode.clone(),
            run_id: accepted.run_id,
            disposition: map_disposition(accepted.disposition)?,
            interrupted_run_id: accepted.interrupted_run_id,
            cancel_requested_at_ms: accepted.cancel_requested_at_ms,
        };
        self.store.record_turn(&request, &accepted)
    }

    pub async fn accept_interrupting_turn(
        &self,
        context: CallContext,
        request: InterruptingTurnRequest,
    ) -> Result<AcceptedTurn, NativeChannelError> {
        validate_turn(&request.turn)?;
        if !matches!(request.turn.mode, ChannelInputMode::Queue) || request.reason.trim().is_empty()
        {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        let _operation = self
            .operations
            .lock_binding(&request.turn.binding_id)
            .await?;
        let binding = self.store.binding(&request.turn.binding_id)?;
        require_attached(&binding)?;
        if let Some(existing) =
            self.store
                .existing_turn(&request.turn, &binding.session_id, Some(&request.reason))?
        {
            return Ok(existing);
        }
        let accepted = self
            .agent_host
            .prompt(
                context,
                PromptRequest {
                    session_id: binding.session_id.clone(),
                    client_turn_id: request.turn.client_turn_id.clone(),
                    mode: AgentInputMode::InterruptAndQueue,
                    native_prompt_json: request.turn.native_prompt_json.clone(),
                },
            )
            .await
            .map_err(map_prompt_call)?;
        let accepted = AcceptedTurn {
            binding_id: binding.binding_id,
            session_id: binding.session_id,
            client_turn_id: request.turn.client_turn_id.clone(),
            accepted_at_ms: accepted.accepted_at_ms,
            mode: request.turn.mode.clone(),
            run_id: accepted.run_id,
            disposition: map_disposition(accepted.disposition)?,
            interrupted_run_id: accepted.interrupted_run_id,
            cancel_requested_at_ms: accepted.cancel_requested_at_ms,
        };
        self.store.record_interrupting_turn(
            &request.turn,
            &accepted,
            request.turn.received_at_ms,
            &request.reason,
        )
    }

    pub async fn interrupt_and_drain(
        &self,
        context: CallContext,
        request: InterruptRequest,
    ) -> Result<InterruptReceipt, NativeChannelError> {
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        require_attached(&binding)?;
        if binding.session_id != request.expected_session_id {
            return Err(NativeChannelError::SessionMismatch);
        }
        if request.reason.trim().is_empty() {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        let status = self.refresh_turn_state(context.clone(), &binding).await?;
        let run_id = status
            .active_run_id
            .ok_or(NativeChannelError::NothingToInterrupt)?;
        let pending_input_count = self
            .store
            .pending_count(&binding.binding_id, &binding.session_id)?;
        let cancelled_run_id = run_id.clone();
        let receipt = self
            .agent_host
            .cancel_run(
                context,
                RunReference {
                    session_id: binding.session_id.clone(),
                    run_id,
                },
            )
            .await
            .map_err(map_cancel_call)?;
        if !receipt.accepted {
            return Err(NativeChannelError::NothingToInterrupt);
        }
        self.store.record_interrupt(
            &binding.binding_id,
            &binding.session_id,
            &cancelled_run_id,
            request.requested_at_ms,
            &request.reason,
            receipt.recorded_at_ms,
            pending_input_count,
        )?;
        Ok(InterruptReceipt {
            binding_id: binding.binding_id,
            session_id: binding.session_id,
            cancel_requested_at_ms: receipt.recorded_at_ms,
            pending_input_count,
        })
    }

    pub async fn publish_native_event(
        &self,
        context: CallContext,
        request: NativeChannelEvent,
    ) -> Result<PublishReceipt, NativeChannelError> {
        if request.sequence == 0 {
            return Err(NativeChannelError::SequenceGap);
        }
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        require_attached(&binding)?;
        if binding.session_id != request.session_id {
            return Err(NativeChannelError::SessionMismatch);
        }
        let page = self
            .authoritative_page(context, &binding, request.sequence - 1, 1)
            .await?;
        if !page
            .events
            .first()
            .is_some_and(|event| event_matches(event, &request))
        {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        self.store.record_publication(&request, (self.clock)()?)
    }

    pub async fn replay_native_events(
        &self,
        context: CallContext,
        request: ReplayRequest,
    ) -> Result<PublishedEventPage, NativeChannelError> {
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        self.authoritative_page(context, &binding, request.after_sequence, request.limit)
            .await
    }

    pub async fn replace_session(
        &self,
        context: CallContext,
        request: ReplaceSessionRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        if binding.session_id != request.expected_session_id {
            return Err(NativeChannelError::SessionMismatch);
        }
        if request.reason.trim().is_empty() {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        self.require_available_session(context, &request.fresh_session_id)
            .await?;
        self.store.replace_session(
            &request.binding_id,
            &request.expected_session_id,
            &request.fresh_session_id,
            request.fresh_native_channel_json.as_deref(),
            &request.reason,
            (self.clock)()?,
        )
    }

    pub async fn recover_session(
        &self,
        context: CallContext,
        request: RecoverSessionRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        if binding.session_id != request.expected_session_id {
            return Err(NativeChannelError::SessionMismatch);
        }
        self.require_available_session(context, &request.expected_session_id)
            .await?;
        self.store.recover_session(
            &request.binding_id,
            &request.expected_session_id,
            (self.clock)()?,
        )
    }

    pub async fn channel_status(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelStatus, NativeChannelError> {
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        let binding = self.store.binding(&request.binding_id)?;
        require_attached(&binding)?;
        let status = self.refresh_turn_state(context, &binding).await?;
        Ok(ChannelStatus {
            pending_input_count: self
                .store
                .pending_count(&binding.binding_id, &binding.session_id)?,
            last_error: self.store.last_error(&binding.binding_id)?,
            binding,
            available_sequence: status.last_sequence,
            updated_at_ms: (self.clock)()?,
        })
    }

    pub async fn list_bindings(
        &self,
        context: CallContext,
        request: ListChannelBindingsRequest,
    ) -> Result<ChannelBindingCatalog, NativeChannelError> {
        let _ = context;
        self.store.list_bindings(request.limit)
    }

    pub async fn binding_summary(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelBindingSummary, NativeChannelError> {
        let _ = context;
        self.store.binding_summary(&request.binding_id)
    }

    pub async fn inspect_binding(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = context;
        self.store.binding(&request.binding_id)
    }

    pub async fn find_binding(
        &self,
        context: CallContext,
        request: LocateBindingRequest,
    ) -> Result<ChannelBinding, NativeChannelError> {
        let _ = context;
        if request.channel_id.trim().is_empty() || request.adapter_id.trim().is_empty() {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        self.store.find_binding(&request)
    }

    pub async fn unbind_channel(
        &self,
        context: CallContext,
        request: BindingReference,
    ) -> Result<ChannelReceipt, NativeChannelError> {
        let _ = context;
        let _operation = self.operations.lock_binding(&request.binding_id).await?;
        self.store.detach(&request.binding_id, (self.clock)()?)
    }
}

#[derive(Eq, Hash, PartialEq)]
enum OperationIdentity {
    Attachment {
        adapter_id: String,
        channel_id: String,
    },
    Binding(String),
}

#[derive(Default)]
struct OperationLocks {
    locks: StdMutex<HashMap<OperationIdentity, WeakOperationLock>>,
}

type WeakOperationLock = Weak<Mutex<()>>;

impl OperationLocks {
    async fn lock_attachment(
        &self,
        adapter_id: &str,
        channel_id: &str,
    ) -> Result<OwnedMutexGuard<()>, NativeChannelError> {
        if !valid_identifier(adapter_id) || !valid_identifier(channel_id) {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        Ok(self
            .lock(OperationIdentity::Attachment {
                adapter_id: adapter_id.to_owned(),
                channel_id: channel_id.to_owned(),
            })
            .await)
    }

    async fn lock_binding(
        &self,
        binding_id: &str,
    ) -> Result<OwnedMutexGuard<()>, NativeChannelError> {
        if !valid_identifier(binding_id) {
            return Err(NativeChannelError::InvalidNativePayload);
        }
        Ok(self
            .lock(OperationIdentity::Binding(binding_id.to_owned()))
            .await)
    }

    async fn lock(&self, identity: OperationIdentity) -> OwnedMutexGuard<()> {
        let lock = {
            let mut locks = self.locks.lock().unwrap_or_else(PoisonError::into_inner);
            locks.retain(|_, lock| lock.strong_count() > 0);
            if let Some(lock) = locks.get(&identity).and_then(Weak::upgrade) {
                lock
            } else {
                let lock = Arc::new(Mutex::new(()));
                locks.insert(identity, Arc::downgrade(&lock));
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

fn validate_turn(request: &ChannelTurn) -> Result<(), NativeChannelError> {
    if !valid_identifier(&request.binding_id)
        || !valid_identifier(&request.client_turn_id)
        || request.native_prompt_json.len() > MAX_NATIVE_PROMPT_BYTES
    {
        return Err(NativeChannelError::InvalidNativePayload);
    }
    let prompt: serde_json::Value = serde_json::from_str(&request.native_prompt_json)
        .map_err(|_| NativeChannelError::InvalidNativePayload)?;
    if !prompt.is_array() {
        return Err(NativeChannelError::InvalidNativePayload);
    }
    Ok(())
}

fn valid_identifier(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_CHANNEL_IDENTIFIER_BYTES
}

fn require_attached(binding: &ChannelBinding) -> Result<(), NativeChannelError> {
    if matches!(binding.lifecycle, ChannelLifecycle::Attached) {
        Ok(())
    } else {
        Err(NativeChannelError::SessionUnavailable)
    }
}

fn map_input_mode(mode: &ChannelInputMode) -> Result<AgentInputMode, NativeChannelError> {
    match mode {
        ChannelInputMode::Queue => Ok(AgentInputMode::Queue),
        ChannelInputMode::Steer => Ok(AgentInputMode::Steer),
        ChannelInputMode::Unknown { .. } => Err(NativeChannelError::InvalidNativePayload),
    }
}

fn map_disposition(
    disposition: PromptDisposition,
) -> Result<ChannelTurnDisposition, NativeChannelError> {
    match disposition {
        PromptDisposition::StartedForegroundWork => {
            Ok(ChannelTurnDisposition::StartedForegroundWork)
        }
        PromptDisposition::ContributedToActiveWork => {
            Ok(ChannelTurnDisposition::ContributedToActiveWork)
        }
        PromptDisposition::QueuedForTurnBoundary => {
            Ok(ChannelTurnDisposition::QueuedForTurnBoundary)
        }
        PromptDisposition::CancelRequestedThenQueued => {
            Ok(ChannelTurnDisposition::CancelRequestedThenQueued)
        }
        PromptDisposition::Unknown { .. } => Err(NativeChannelError::AdapterUnavailable),
    }
}

fn map_event(binding_id: &str, event: AcpEvent) -> NativeChannelEvent {
    NativeChannelEvent {
        binding_id: binding_id.to_owned(),
        session_id: event.session_id,
        run_id: event.run_id,
        sequence: event.sequence,
        observed_at_ms: event.observed_at_ms,
        kind: match event.kind {
            AcpEventKind::Message => NativeEventKind::Message,
            AcpEventKind::Thought => NativeEventKind::Thought,
            AcpEventKind::Plan => NativeEventKind::Plan,
            AcpEventKind::ToolCall => NativeEventKind::ToolCall,
            AcpEventKind::ToolResult => NativeEventKind::ToolResult,
            AcpEventKind::Terminal => NativeEventKind::Terminal,
            AcpEventKind::FileDiff => NativeEventKind::FileDiff,
            AcpEventKind::PermissionRequest => NativeEventKind::PermissionRequest,
            AcpEventKind::Usage => NativeEventKind::Usage,
            AcpEventKind::Compaction => NativeEventKind::Compaction,
            AcpEventKind::SessionState => NativeEventKind::SessionState,
            AcpEventKind::RunFinished => NativeEventKind::RunFinished,
            AcpEventKind::Other | AcpEventKind::Unknown { .. } => NativeEventKind::Other,
        },
        direction: Some(match event.direction {
            AcpEventDirection::ClientToAgent => NativeEventDirection::ClientToAgent,
            AcpEventDirection::AgentToClient => NativeEventDirection::AgentToClient,
            AcpEventDirection::Unknown { .. } => NativeEventDirection::Other,
        }),
        native_event_json: event.native_event_json,
    }
}

fn event_matches(authoritative: &NativeChannelEvent, submitted: &NativeChannelEvent) -> bool {
    authoritative.binding_id == submitted.binding_id
        && authoritative.session_id == submitted.session_id
        && authoritative.run_id == submitted.run_id
        && authoritative.sequence == submitted.sequence
        && authoritative.observed_at_ms == submitted.observed_at_ms
        && authoritative.kind == submitted.kind
        && submitted
            .direction
            .as_ref()
            .is_none_or(|direction| authoritative.direction.as_ref() == Some(direction))
        && authoritative.native_event_json == submitted.native_event_json
}

fn map_host_call(error: ErasedCallError) -> NativeChannelError {
    match error {
        ErasedCallError::Domain { error_tag, .. }
            if matches!(
                error_tag.as_str(),
                "UnknownSession" | "SessionClosed" | "UnknownRun"
            ) =>
        {
            NativeChannelError::SessionUnavailable
        }
        ErasedCallError::Domain { error_tag, .. } if error_tag == "InvalidCursor" => {
            NativeChannelError::SequenceGap
        }
        ErasedCallError::Domain { error_tag, .. } if error_tag == "InvalidNativePayload" => {
            NativeChannelError::InvalidNativePayload
        }
        _ => NativeChannelError::AdapterUnavailable,
    }
}

fn map_prompt_call(error: ErasedCallError) -> NativeChannelError {
    match error {
        ErasedCallError::Domain { error_tag, .. } if error_tag == "DuplicateTurnConflict" => {
            NativeChannelError::DuplicateTurnConflict
        }
        ErasedCallError::Domain { error_tag, .. } if error_tag == "SteeringUnavailable" => {
            NativeChannelError::SteeringUnavailable
        }
        other => map_host_call(other),
    }
}

fn map_cancel_call(error: ErasedCallError) -> NativeChannelError {
    match error {
        ErasedCallError::Domain { error_tag, .. } if error_tag == "UnknownRun" => {
            NativeChannelError::NothingToInterrupt
        }
        other => map_host_call(other),
    }
}

fn system_time_ms() -> Result<u64, NativeChannelError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| NativeChannelError::StorageUnavailable)?;
    u64::try_from(duration.as_millis()).map_err(|_| NativeChannelError::StorageUnavailable)
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use boxology_contract::CapabilityId;
    use tokio::time::timeout;

    use super::{
        ChannelInputMode, ChannelTurn, MAX_CHANNEL_IDENTIFIER_BYTES, MAX_NATIVE_PROMPT_BYTES,
        NativeChannelError, OperationLocks, generated, validate_turn,
    };

    #[test]
    fn contract_keeps_native_publication_separate_from_bridge_delivery() {
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
                "bind_channel",
                "accept_turn",
                "accept_interrupting_turn",
                "interrupt_and_drain",
                "publish_native_event",
                "replay_native_events",
                "replace_session",
                "recover_session",
                "channel_status",
                "list_bindings",
                "binding_summary",
                "inspect_binding",
                "find_binding",
                "unbind_channel",
            ]
        );
        assert!(names.iter().all(|name| !name.contains("bridge")));
        assert_ne!(ChannelInputMode::Queue, ChannelInputMode::Steer);
    }

    #[test]
    fn native_turn_is_rejected_before_parsing_when_over_limit() {
        let turn = ChannelTurn {
            binding_id: "binding-1".into(),
            client_turn_id: "turn-1".into(),
            received_at_ms: 1,
            mode: ChannelInputMode::Queue,
            native_prompt_json: "x".repeat(MAX_NATIVE_PROMPT_BYTES + 1),
        };

        assert_eq!(
            validate_turn(&turn),
            Err(NativeChannelError::InvalidNativePayload)
        );
    }

    #[test]
    fn native_turn_rejects_oversized_lane_identifiers() {
        let turn = ChannelTurn {
            binding_id: "b".repeat(MAX_CHANNEL_IDENTIFIER_BYTES + 1),
            client_turn_id: "turn-1".into(),
            received_at_ms: 1,
            mode: ChannelInputMode::Queue,
            native_prompt_json: "[]".into(),
        };

        assert_eq!(
            validate_turn(&turn),
            Err(NativeChannelError::InvalidNativePayload)
        );
    }

    #[tokio::test]
    async fn operation_locks_isolate_identities_and_prune() {
        let locks = OperationLocks::default();
        let first_binding = locks
            .lock_binding("binding-a")
            .await
            .expect("valid binding");
        let first_attachment = locks
            .lock_attachment("t3code", "channel-a")
            .await
            .expect("valid attachment");

        let other_binding = timeout(Duration::from_millis(100), locks.lock_binding("binding-b"))
            .await
            .expect("unrelated binding does not wait")
            .expect("valid binding");
        let other_attachment = timeout(
            Duration::from_millis(100),
            locks.lock_attachment("t3code", "channel-b"),
        )
        .await
        .expect("unrelated attachment does not wait")
        .expect("valid attachment");
        assert!(
            timeout(Duration::from_millis(20), locks.lock_binding("binding-a"))
                .await
                .is_err()
        );
        assert!(
            timeout(
                Duration::from_millis(20),
                locks.lock_attachment("t3code", "channel-a")
            )
            .await
            .is_err()
        );

        drop(first_binding);
        drop(first_attachment);
        let matching_binding = locks
            .lock_binding("binding-a")
            .await
            .expect("valid binding");
        let matching_attachment = locks
            .lock_attachment("t3code", "channel-a")
            .await
            .expect("valid attachment");
        drop(matching_binding);
        drop(matching_attachment);
        drop(other_binding);
        drop(other_attachment);

        let fresh = locks.lock_binding("fresh").await.expect("valid binding");
        assert_eq!(locks.len(), 1);
        drop(fresh);
    }
}
