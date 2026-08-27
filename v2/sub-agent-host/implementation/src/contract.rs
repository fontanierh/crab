// Crab owns sub-agent orchestration instead of inheriting incompatible agent-specific concepts.
// Each child is a separately supervised ACP harness subprocess with durable, cursor-addressed
// communication. Boxology V0 is unary request/response, so the realtime stream is represented by
// non-blocking sends plus ordered reads rather than a fake blocking stream capability.
boxology::contract! {
    pub enum SubAgentLifecycle {
        Starting,
        Running,
        Idle,
        Stopping,
        Completed,
        Failed,
    }

    /// The two portable context contracts Crab promises to callers.
    pub enum SubAgentContextMode {
        /// Start an independent ACP session with only the explicit child bootstrap/task input.
        Fresh,
        /// Seed the child from a stable point in the parent conversation.
        InheritParent,
    }

    /// Records how `InheritParent` was actually honored; callers never have to guess fidelity.
    pub enum ContextRealization {
        FreshSession,
        /// The agent negotiated draft ACP `session/fork`, preserving opaque native context.
        NativeAcpFork,
        /// Crab replayed its durable user-visible snapshot; hidden provider state is not claimed.
        PortableSnapshot,
    }

    /// Message delivery is realtime and non-blocking even when model consumption must wait.
    pub enum SubAgentInputMode {
        Queue,
        Steer,
        InterruptAndSteer,
    }

    pub enum InputDisposition {
        StartedForegroundWork,
        ContributedToActiveWork,
        QueuedForTurnBoundary,
        CancelRequestedThenQueued,
    }

    /// Starts exactly one separately supervised ACP harness subprocess.
    pub struct SpawnSubAgentRequest {
        /// Stable idempotency key chosen by the parent.
        pub client_sub_agent_id: String,
        pub parent_session_id: String,
        pub agent_id: String,
        pub working_directory: String,
        pub context_mode: SubAgentContextMode,
        /// Immutable inclusive parent event boundary for inherited context.
        pub parent_context_through_sequence: Option<u64>,
        /// Permit a visible-history snapshot when native ACP session fork is unavailable.
        pub allow_portable_snapshot: bool,
        /// Exact ACP-compatible initial task content, capped at 2 MiB. Larger content travels by
        /// reference.
        pub native_task_prompt_json: String,
        pub metadata_json: String,
        /// Zero disables crash restart. Resume is allowed only when the ACP session can be restored.
        pub crash_restart_limit: u64,
    }

    pub struct SubAgentRecord {
        pub sub_agent_id: String,
        pub parent_session_id: String,
        pub child_session_id: String,
        pub native_child_session_id: String,
        pub agent_id: String,
        pub lifecycle: SubAgentLifecycle,
        pub context_mode: SubAgentContextMode,
        pub context_realization: ContextRealization,
        pub context_through_sequence: Option<u64>,
        /// Opaque identity of the separately supervised agent-host session/process boundary.
        pub process_identity: String,
        pub started_at_ms: u64,
    }

    pub struct SendToChildRequest {
        pub sub_agent_id: String,
        pub client_message_id: String,
        pub mode: SubAgentInputMode,
        /// Exact ACP-compatible prompt content, capped at 2 MiB.
        pub native_prompt_json: String,
    }

    pub struct SendToParentRequest {
        pub sub_agent_id: String,
        pub client_message_id: String,
        pub mode: SubAgentInputMode,
        /// Structured child result/progress, converted to an ACP-compatible parent input.
        pub message_json: String,
    }

    pub struct InteractionReceipt {
        pub sub_agent_id: String,
        pub client_message_id: String,
        pub disposition: InputDisposition,
        pub accepted_at_ms: u64,
    }

    pub enum SubAgentEventKind {
        Lifecycle,
        NativeAcp,
        ParentToChild,
        ChildToParent,
        Compaction,
        Failed,
    }

    /// One durable event. Native ACP payloads are preserved so parent tooling can render tool
    /// calls, thoughts, plans and intermediate output in realtime rather than waiting for a result.
    pub struct SubAgentEvent {
        pub sub_agent_id: String,
        pub sequence: u64,
        pub observed_at_ms: u64,
        pub kind: SubAgentEventKind,
        pub payload_json: String,
    }

    pub struct ReadSubAgentEventsRequest {
        pub sub_agent_id: String,
        pub after_sequence: u64,
        pub limit: u64,
    }

    pub struct SubAgentEventPage {
        pub events: Vec<SubAgentEvent>,
        pub next_sequence: u64,
        pub caught_up: bool,
    }

    pub struct SubAgentReference {
        pub sub_agent_id: String,
    }

    pub struct StopSubAgentRequest {
        pub sub_agent_id: String,
        pub reason: String,
    }

    pub struct SubAgentStatus {
        pub record: SubAgentRecord,
        pub last_sequence: u64,
        pub pending_parent_to_child: u64,
        pub pending_child_to_parent: u64,
        pub restart_count: u64,
        pub last_error: Option<String>,
    }

    pub struct SubAgentReceipt {
        pub accepted: bool,
        pub recorded_at_ms: u64,
    }

    pub struct RecoverSubAgentsRequest {}

    /// Truthful startup outcome for one durable child. Recovery never opens a replacement session.
    pub enum SubAgentRecoveryDisposition {
        Resumed,
        RecoveryDisabled,
        RestartBudgetExhausted,
        ParentUnavailable,
        SessionUnavailable,
        IdentityMismatch,
        Failed,
    }

    pub struct SubAgentRecovery {
        pub sub_agent_id: String,
        pub child_session_id: String,
        pub disposition: SubAgentRecoveryDisposition,
    }

    pub struct SubAgentRecoveryReport {
        pub recoveries: Vec<SubAgentRecovery>,
    }

    #[error]
    pub enum SubAgentHostError {
        DraftOnly,
        DuplicateIdConflict,
        UnknownParentSession,
        UnknownSubAgent,
        InvalidContextBoundary,
        NativeForkUnavailable,
        PortableSnapshotForbidden,
        /// Retained for older consumers; bounded native resume is now implemented.
        CrashRestartUnavailable,
        SteeringUnavailable,
        AuthorityUnavailable,
        ProtocolNegotiationFailed,
        InvalidNativePayload,
        TransportFailed,
        StorageUnavailable,
    }

    /// Return after the subprocess and child session are accepted; child work continues in the
    /// background and is observed through `read_events`.
    #[capability]
    pub async fn spawn(request: SpawnSubAgentRequest) -> Result<SubAgentRecord, SubAgentHostError>;

    /// Durable parent-to-child delivery. `Steer` never silently degrades to `Queue`.
    #[capability]
    pub async fn send_to_child(request: SendToChildRequest) -> Result<InteractionReceipt, SubAgentHostError>;

    /// Durable child-to-parent delivery, suitable for progress or final results while both run.
    #[capability]
    pub async fn send_to_parent(request: SendToParentRequest) -> Result<InteractionReceipt, SubAgentHostError>;

    #[capability]
    pub async fn read_events(request: ReadSubAgentEventsRequest) -> Result<SubAgentEventPage, SubAgentHostError>;

    #[capability]
    pub async fn status(request: SubAgentReference) -> Result<SubAgentStatus, SubAgentHostError>;

    /// Reconcile children interrupted by a runtime crash. Exact native ACP resume is the only
    /// successful path; bootstrap context and initial task input are never replayed.
    #[capability]
    pub async fn recover(request: RecoverSubAgentsRequest) -> Result<SubAgentRecoveryReport, SubAgentHostError>;

    /// Cooperatively cancel the child session, then terminate its harness after graceful timeout.
    #[capability]
    pub async fn stop(request: StopSubAgentRequest) -> Result<SubAgentReceipt, SubAgentHostError>;
}
