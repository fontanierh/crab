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

    /// ACP version and optional features negotiated during `initialize`.
    pub struct AcpNegotiation {
        pub protocol_version: u64,
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

    pub struct AgentSession {
        /// Crab's durable identifier, independent from any agent-specific identifier format.
        pub session_id: String,
        /// The exact ACP session identifier returned by the agent.
        pub native_session_id: String,
        pub agent_id: String,
        pub negotiation: AcpNegotiation,
        pub authority: AuthorityAttestation,
    }

    pub struct PromptRequest {
        pub session_id: String,
        /// Stable caller key used to deduplicate a retried turn.
        pub client_turn_id: String,
        /// Exact ACP prompt payload encoded as JSON. Crab must not narrow multimodal ACP content.
        pub native_prompt_json: String,
    }

    pub struct PromptAccepted {
        pub session_id: String,
        pub run_id: String,
        pub accepted_at_ms: u64,
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
        SessionState,
        RunFinished,
        Other,
    }

    /// One lossless event from the ACP connection, ordered within a Crab session.
    pub struct AcpEvent {
        pub session_id: String,
        pub run_id: Option<String>,
        pub sequence: u64,
        pub observed_at_ms: u64,
        pub kind: AcpEventKind,
        /// The complete ACP JSON-RPC message. Channels render this, including tool activity.
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
        /// This contract draft intentionally has no live backend yet.
        DraftOnly,
        UnknownAgent,
        PreflightFailed,
        AuthorityUnavailable,
        ProtocolNegotiationFailed,
        UnknownSession,
        SessionBusy,
        InvalidCursor,
        InvalidNativePayload,
        TransportFailed,
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

    /// Send a prompt to the existing ACP session. Context management stays inside the agent.
    #[capability]
    pub async fn prompt(request: PromptRequest) -> Result<PromptAccepted, AgentHostError>;

    /// Pull a lossless ordered page from Crab's durable copy of the native ACP event stream.
    #[capability]
    pub async fn read_events(request: ReadEventsRequest) -> Result<EventPage, AgentHostError>;

    /// Resolve an ACP permission request with the mandatory unrestricted decision.
    #[capability]
    pub async fn resolve_permission(request: PermissionRequest) -> Result<PermissionResolution, AgentHostError>;

    #[capability]
    pub async fn session_status(request: SessionReference) -> Result<SessionStatus, AgentHostError>;

    #[capability]
    pub async fn cancel_run(request: RunReference) -> Result<OperationReceipt, AgentHostError>;

    /// Close the native session. Opening a fresh one is explicit; there is no compaction API.
    #[capability]
    pub async fn close_session(request: SessionReference) -> Result<OperationReceipt, AgentHostError>;
}
