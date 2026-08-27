// A channel is a native client view over one ACP session. It is deliberately not the reusable
// external-integration abstraction: that is `bridge-host`.
boxology::contract! {
    pub enum ChannelLifecycle {
        Binding,
        Attached,
        Replaying,
        Detached,
        Failed,
    }

    /// Native channels select delivery per turn. Interrupt is a separate explicit action so a
    /// normal message can never accidentally cancel thinking or a tool call.
    pub enum ChannelInputMode {
        Queue,
        Steer,
    }

    /// Creates the invariant `channel_id -> session_id` for the lifetime of this binding.
    pub struct BindChannelRequest {
        pub channel_id: String,
        pub adapter_id: String,
        pub session_id: String,
        /// Adapter-specific destination metadata, retained losslessly as JSON.
        pub native_channel_json: String,
    }

    pub struct ChannelBinding {
        pub binding_id: String,
        pub channel_id: String,
        pub adapter_id: String,
        pub session_id: String,
        pub lifecycle: ChannelLifecycle,
        /// Adapter-specific destination metadata, retained losslessly as JSON.
        pub native_channel_json: String,
        /// Last ACP sequence durably published to the adapter.
        pub published_sequence: u64,
    }

    /// A user turn from the native interface, destined for exactly the bound ACP session.
    pub struct ChannelTurn {
        pub binding_id: String,
        pub client_turn_id: String,
        pub received_at_ms: u64,
        pub mode: ChannelInputMode,
        /// Exact ACP-compatible prompt JSON, including attachments and other rich content.
        pub native_prompt_json: String,
    }

    pub enum ChannelTurnDisposition {
        StartedForegroundWork,
        ContributedToActiveWork,
        QueuedForTurnBoundary,
        CancelRequestedThenQueued,
    }

    pub struct AcceptedTurn {
        pub binding_id: String,
        pub session_id: String,
        pub client_turn_id: String,
        pub accepted_at_ms: u64,
        pub mode: ChannelInputMode,
        /// The durable run selected by `agent-host`.
        pub run_id: String,
        pub disposition: ChannelTurnDisposition,
        pub interrupted_run_id: Option<String>,
        pub cancel_requested_at_ms: Option<u64>,
    }

    /// Automatic interrupting ingress is distinct from the native UI's explicit interrupt action.
    /// The turn is accepted durably in the same session operation that requests cancellation.
    pub struct InterruptingTurnRequest {
        pub turn: ChannelTurn,
        pub reason: String,
    }

    /// The hint is useful for rendering; no event may be dropped because its hint is unknown.
    pub enum NativeEventKind {
        Message,
        Thought,
        Plan,
        ToolCall,
        ToolResult,
        Terminal,
        FileDiff,
        PermissionRequest,
        Usage,
        Compaction,
        SessionState,
        RunFinished,
        Other,
    }

    pub enum NativeEventDirection {
        ClientToAgent,
        AgentToClient,
        Other,
    }

    /// Channels receive the complete ordered ACP stream rather than assistant-text projections.
    pub struct NativeChannelEvent {
        pub binding_id: String,
        pub session_id: String,
        pub run_id: Option<String>,
        pub sequence: u64,
        pub observed_at_ms: u64,
        pub kind: NativeEventKind,
        pub direction: Option<NativeEventDirection>,
        /// Exact native ACP JSON-RPC message, including intermediate and tool events.
        pub native_event_json: String,
    }

    pub struct PublishReceipt {
        pub binding_id: String,
        pub sequence: u64,
        /// Stable adapter receipt used to make crash retries idempotent.
        pub delivery_id: String,
        pub published_at_ms: u64,
    }

    pub struct ReplayRequest {
        pub binding_id: String,
        pub after_sequence: u64,
        pub limit: u64,
    }

    pub struct PublishedEventPage {
        pub events: Vec<NativeChannelEvent>,
        pub next_sequence: u64,
        pub caught_up: bool,
    }

    /// A fresh session is an explicit user/operator operation, not Crab-owned compaction.
    pub struct ReplaceSessionRequest {
        pub binding_id: String,
        pub expected_session_id: String,
        pub fresh_session_id: String,
        /// Fresh adapter destination metadata, atomically installed with the new session. Omit to
        /// retain the current metadata when only the physical session changes.
        pub fresh_native_channel_json: Option<String>,
        pub reason: String,
    }

    /// Reattach a failed binding after its exact ACP session was resumed. Unlike replacement this
    /// preserves session identity plus publication and reconciliation cursors.
    pub struct RecoverSessionRequest {
        pub binding_id: String,
        pub expected_session_id: String,
    }

    pub struct BindingReference {
        pub binding_id: String,
    }

    pub struct LocateBindingRequest {
        pub channel_id: String,
        pub adapter_id: String,
    }

    /// Explicit cooperative interruption. The router cancels current ACP work, retains every
    /// already accepted queue/steer input, then drains those inputs immediately in stable order.
    pub struct InterruptRequest {
        pub binding_id: String,
        pub expected_session_id: String,
        pub requested_at_ms: u64,
        pub reason: String,
    }

    pub struct InterruptReceipt {
        pub binding_id: String,
        pub session_id: String,
        pub cancel_requested_at_ms: u64,
        pub pending_input_count: u64,
    }

    pub struct ChannelStatus {
        pub binding: ChannelBinding,
        /// Latest sequence currently readable from `agent-host`.
        pub available_sequence: u64,
        /// Accepted turn-boundary inputs that have not started yet.
        pub pending_input_count: u64,
        pub last_error: Option<String>,
        pub updated_at_ms: u64,
    }

    pub struct ChannelReceipt {
        pub accepted: bool,
        pub recorded_at_ms: u64,
    }

    #[error]
    pub enum NativeChannelError {
        DraftOnly,
        AlreadyBound,
        UnknownBinding,
        SessionMismatch,
        SequenceGap,
        DuplicateTurnConflict,
        SteeringUnavailable,
        NothingToInterrupt,
        InvalidNativePayload,
        SessionUnavailable,
        AdapterUnavailable,
        StorageUnavailable,
    }

    /// Bind one native interface to one already-open ACP session.
    #[capability]
    pub async fn bind_channel(request: BindChannelRequest) -> Result<ChannelBinding, NativeChannelError>;

    /// Route a native user turn without translating it into bridge semantics.
    #[capability]
    pub async fn accept_turn(request: ChannelTurn) -> Result<AcceptedTurn, NativeChannelError>;

    /// Accept an input and cooperatively cancel active work as one ordered session operation. An
    /// idle session simply starts the accepted turn. This is the safe primitive for automatic
    /// bridge and sub-system `InterruptAndSteer` policy; native UI interrupt stays explicit.
    #[capability]
    pub async fn accept_interrupting_turn(request: InterruptingTurnRequest) -> Result<AcceptedTurn, NativeChannelError>;

    /// Cancel current thinking/tool execution cooperatively and immediately drain accepted input.
    #[capability]
    pub async fn interrupt_and_drain(request: InterruptRequest) -> Result<InterruptReceipt, NativeChannelError>;

    /// Confirm adapter publication of the next authoritative ACP event. The supplied event must
    /// exactly match `agent-host`; arbitrary or out-of-order events are rejected.
    #[capability]
    pub async fn publish_native_event(request: NativeChannelEvent) -> Result<PublishReceipt, NativeChannelError>;

    /// Replay the durable native view after an adapter disconnect or client reconnect.
    #[capability]
    pub async fn replay_native_events(request: ReplayRequest) -> Result<PublishedEventPage, NativeChannelError>;

    /// Atomically bind a fresh ACP session after explicit close/reopen by the caller.
    #[capability]
    pub async fn replace_session(request: ReplaceSessionRequest) -> Result<ChannelBinding, NativeChannelError>;

    /// Mark one failed binding attached after proving its unchanged ACP session is available.
    #[capability]
    pub async fn recover_session(request: RecoverSessionRequest) -> Result<ChannelBinding, NativeChannelError>;

    #[capability]
    pub async fn channel_status(request: BindingReference) -> Result<ChannelStatus, NativeChannelError>;

    /// Read persisted binding identity even when its previous ACP session is unavailable. Runtime
    /// startup uses this to attach a fresh session after a crash without inventing a second route.
    #[capability]
    pub async fn inspect_binding(request: BindingReference) -> Result<ChannelBinding, NativeChannelError>;

    /// Recover the one non-detached binding for an adapter/channel pair after a crash between
    /// binding creation and route registration.
    #[capability]
    pub async fn find_binding(request: LocateBindingRequest) -> Result<ChannelBinding, NativeChannelError>;

    #[capability]
    pub async fn unbind_channel(request: BindingReference) -> Result<ChannelReceipt, NativeChannelError>;
}
