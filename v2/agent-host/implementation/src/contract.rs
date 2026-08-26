// This is the authored source of truth. `boxology generate` derives the Rust contract crate,
// dispatch adapter and language-neutral schema from it. Keep policy here, not in generated files.
boxology::contract! {
    /// Coarse lifecycle information for operators. ACP remains the source of detailed state.
    pub enum AgentLifecycle {
        Discovered,
        Starting,
        Ready,
        Busy,
        Stopping,
        Stopped,
        Failed,
    }

    /// Crab v2 has exactly one acceptable sandbox policy: none.
    pub enum SandboxAuthority {
        DisabledAndVerified,
    }

    /// Permission prompts must be bypassed or automatically granted at the strongest level.
    pub enum PermissionAuthority {
        YoloAndVerified,
    }

    /// The agent receives ordinary host filesystem access, not a projected workspace.
    pub enum FilesystemAuthority {
        UnrestrictedAndVerified,
    }

    /// The agent receives host networking without an allowlist or proxy sandbox.
    pub enum NetworkAuthority {
        UnrestrictedAndVerified,
    }

    /// The process may stay unprivileged, but every session must prove non-interactive root access.
    /// This accommodates agents such as Claude Code that reject yolo mode when launched as EUID 0.
    pub enum RootAuthority {
        PasswordlessSudoAndVerified,
    }

    /// Evidence that launch may proceed. Missing evidence is a hard failure, never a downgrade.
    pub struct AuthorityAttestation {
        pub sandbox: SandboxAuthority,
        pub permissions: PermissionAuthority,
        pub filesystem: FilesystemAuthority,
        pub network: NetworkAuthority,
        pub root: RootAuthority,
        /// Unix time in milliseconds at which the host performed the checks.
        pub verified_at_ms: u64,
        /// Opaque, non-secret audit data such as probe versions and exit codes.
        pub evidence_json: String,
    }

    /// An ACP-capable executable known to Crab. Arguments are explicit because yolo flags differ.
    pub struct AgentDescriptor {
        pub agent_id: String,
        pub display_name: String,
        pub executable: String,
        pub arguments: Vec<String>,
        /// Names only. Secret values remain in the host's credential provider.
        pub environment_names: Vec<String>,
        /// Configured stdio MCP servers attached by Crab to every session.
        pub mcp_server_names: Vec<String>,
        pub lifecycle: AgentLifecycle,
    }

    pub struct AgentCatalog {
        pub agents: Vec<AgentDescriptor>,
    }

    /// Explicit unit request keeps the unary Boxology capability evolvable.
    pub struct DiscoverAgentsRequest {}

    pub struct PreflightRequest {
        pub agent_id: String,
        pub working_directory: String,
    }

    pub struct PreflightReport {
        pub agent_id: String,
        pub working_directory: String,
        pub authority: AuthorityAttestation,
    }

    /// ACP v1 is stable. ACP v2 is deliberately usable behind negotiation because its prompt
    /// lifecycle is the portable basis for non-blocking steering, but it remains a draft today.
    pub enum AcpProtocolProfile {
        V1Stable,
        V2Draft,
    }

    /// What an input submitted while the session is already working can actually do.
    pub enum SteeringSupport {
        /// ACP v1 has no portable mid-work prompt contract. Crab accepts immediately and queues
        /// the input for the next turn boundary instead of claiming it steered the active model.
        TurnBoundaryQueue,
        /// ACP v2 acknowledges `session/prompt` immediately and lets it contribute to active work.
        AcpV2ConcurrentPrompt,
        /// An agent-specific ACP extension provides equivalent semantics on another profile.
        AgentExtension,
    }

    /// Compaction remains agent-owned. This only describes what the client can observe.
    pub enum CompactionReporting {
        /// Usage may change, but the agent exposes no portable compaction lifecycle event.
        OpaqueAgentManaged,
        /// The draft ACP compaction updates and optional displayable summary are preserved.
        DraftLifecycleUpdates,
    }

    /// ACP version and optional features negotiated during `initialize`.
    pub struct AcpNegotiation {
        pub protocol_version: u64,
        pub protocol_profile: AcpProtocolProfile,
        pub steering: SteeringSupport,
        pub compaction_reporting: CompactionReporting,
        /// Preserve capability evolution without forcing Crab to mirror every ACP revision.
        pub agent_capabilities_json: String,
    }

    pub struct OpenSessionRequest {
        pub agent_id: String,
        pub working_directory: String,
        /// Crab may bootstrap identity/memory, but it does not own compaction or token arithmetic.
        pub bootstrap_prompt: Option<String>,
        pub metadata_json: String,
    }

    /// Reconnect one failed durable Crab session to the same native ACP session. Agent identity,
    /// working directory, metadata and native session identity come only from durable host state.
    pub struct ResumeSessionRequest {
        pub session_id: String,
    }

    pub struct AgentSession {
        /// Crab's durable identifier, independent from any agent-specific identifier format.
        pub session_id: String,
        /// The exact ACP session identifier returned by the agent.
        pub native_session_id: String,
        pub agent_id: String,
        pub negotiation: AcpNegotiation,
        pub authority: AuthorityAttestation,
    }

    pub enum AgentInputMode {
        /// Accept durably and send only when the session is idle.
        Queue,
        /// Contribute to active work. Fail if ACP v2 or an equivalent extension was not negotiated.
        Steer,
    }

    pub struct PromptRequest {
        pub session_id: String,
        /// Stable caller key used to deduplicate a retried turn.
        pub client_turn_id: String,
        pub mode: AgentInputMode,
        /// Exact JSON array carried in ACP's `prompt` field. Crab must not narrow multimodal ACP
        /// content blocks.
        pub native_prompt_json: String,
    }

    /// Submission is always non-blocking; this says when the agent can consume it.
    pub enum PromptDisposition {
        StartedForegroundWork,
        ContributedToActiveWork,
        QueuedForTurnBoundary,
    }

    pub struct PromptAccepted {
        pub session_id: String,
        pub run_id: String,
        pub accepted_at_ms: u64,
        pub disposition: PromptDisposition,
    }

    /// A hint for indexing and UI filtering. `native_event_json` is always authoritative.
    pub enum AcpEventKind {
        Message,
        Thought,
        Plan,
        ToolCall,
        ToolResult,
        Terminal,
        FileDiff,
        PermissionRequest,
        Usage,
        /// Draft ACP `compaction_update` and `compaction_summary_chunk`, when advertised.
        Compaction,
        SessionState,
        RunFinished,
        Other,
    }

    /// Which side emitted the preserved JSON-RPC message.
    pub enum AcpEventDirection {
        ClientToAgent,
        AgentToClient,
    }

    /// One lossless event from the ACP connection, ordered within a Crab session.
    pub struct AcpEvent {
        pub session_id: String,
        pub run_id: Option<String>,
        pub sequence: u64,
        pub observed_at_ms: u64,
        pub kind: AcpEventKind,
        pub direction: AcpEventDirection,
        /// The complete ACP JSON-RPC message. `crab/*` lifecycle notifications are the only
        /// Crab-authored extension; every native ACP message remains byte-for-byte intact.
        pub native_event_json: String,
    }

    pub struct ReadEventsRequest {
        pub session_id: String,
        /// Exclusive cursor; zero reads from the first retained event.
        pub after_sequence: u64,
        pub limit: u64,
    }

    pub struct EventPage {
        pub events: Vec<AcpEvent>,
        pub next_sequence: u64,
        pub caught_up: bool,
    }

    /// ACP permission requests are recorded for visibility, then resolved without human gating.
    pub struct PermissionRequest {
        pub session_id: String,
        pub request_id: String,
        pub native_request_json: String,
    }

    pub enum PermissionDecision {
        AllowUnrestricted,
    }

    pub struct PermissionResolution {
        pub request_id: String,
        pub decision: PermissionDecision,
        pub native_response_json: String,
    }

    pub struct SessionReference {
        pub session_id: String,
    }

    pub struct RunReference {
        pub session_id: String,
        pub run_id: String,
    }

    pub struct SessionStatus {
        pub session_id: String,
        pub lifecycle: AgentLifecycle,
        pub last_sequence: u64,
        pub active_run_id: Option<String>,
    }

    pub struct OperationReceipt {
        pub accepted: bool,
        pub recorded_at_ms: u64,
    }

    #[error]
    pub enum AgentHostError {
        /// Retained so older draft consumers decode cleanly; the live host never returns it.
        DraftOnly,
        InvalidConfiguration,
        UnknownAgent,
        PreflightFailed,
        AuthorityUnavailable,
        ProtocolNegotiationFailed,
        UnsupportedProtocolProfile,
        UnknownSession,
        SessionClosed,
        SteeringUnavailable,
        DuplicateTurnConflict,
        UnknownRun,
        UnknownPermission,
        InvalidCursor,
        InvalidNativePayload,
        SessionResumeUnavailable,
        TransportFailed,
        StorageUnavailable,
    }

    /// Discover configured ACP agents; no Claude-specific type is permitted at this boundary.
    #[capability]
    pub async fn discover_agents(request: DiscoverAgentsRequest) -> Result<AgentCatalog, AgentHostError>;

    /// Fail closed unless every mandatory authority probe succeeds in this working directory.
    #[capability]
    pub async fn preflight(request: PreflightRequest) -> Result<PreflightReport, AgentHostError>;

    /// Spawn an ACP process, negotiate the protocol, and open a new native ACP session.
    #[capability]
    pub async fn open_session(request: OpenSessionRequest) -> Result<AgentSession, AgentHostError>;

    /// Resume one failed durable session through native ACP. This re-runs authority checks,
    /// reconnects configured MCP servers and verifies required session policy. It never sends the
    /// original bootstrap prompt or retries prompts that were active when the process failed.
    #[capability]
    pub async fn resume_session(request: ResumeSessionRequest) -> Result<AgentSession, AgentHostError>;

    /// Submit input without waiting for work to finish. `Queue` is portable. `Steer` uses the ACP
    /// v2 prompt lifecycle or a negotiated extension and fails rather than silently becoming queue.
    #[capability]
    pub async fn prompt(request: PromptRequest) -> Result<PromptAccepted, AgentHostError>;

    /// Pull a lossless ordered page from Crab's durable copy of the native ACP event stream.
    #[capability]
    pub async fn read_events(request: ReadEventsRequest) -> Result<EventPage, AgentHostError>;

    /// Return the automatic unrestricted resolution already sent by the host. The supplied ID and
    /// native request must match the durable record; callers never gate the agent on human input.
    #[capability]
    pub async fn resolve_permission(request: PermissionRequest) -> Result<PermissionResolution, AgentHostError>;

    #[capability]
    pub async fn session_status(request: SessionReference) -> Result<SessionStatus, AgentHostError>;

    #[capability]
    pub async fn cancel_run(request: RunReference) -> Result<OperationReceipt, AgentHostError>;

    /// Close the native session. There is intentionally no `compact` capability: ACP's current
    /// compaction proposal reports agent-owned compaction; it does not transfer control to Crab.
    #[capability]
    pub async fn close_session(request: SessionReference) -> Result<OperationReceipt, AgentHostError>;
}
