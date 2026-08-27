// Bridges are durable external communication integrations. They can wake Crab and receive
// deliberately selected messages; they never receive the complete native ACP event stream.
boxology::contract! {
    pub enum BridgeLifecycle {
        Registered,
        Starting,
        AwaitingAuthentication,
        Healthy,
        Degraded,
        BackingOff,
        Stopped,
        Failed,
    }

    /// Authentication transports that a generic bridge host can present to an operator.
    pub enum AuthenticationMethod {
        QrCode,
        PhoneCode,
        OAuth,
        Browser,
        Terminal,
        Manual,
    }

    pub enum CredentialLifecycle {
        Missing,
        Challenged,
        Validating,
        Valid,
        Expiring,
        Rejected,
        Revoked,
    }

    pub enum DeliveryLifecycle {
        Queued,
        Sending,
        Delivered,
        Retrying,
        Rejected,
        Failed,
    }

    /// Fixed when a bridge is registered so an external surface cannot silently change how much
    /// it disrupts the agent. Changing this policy requires a new bridge generation.
    pub enum BridgeIngressMode {
        /// Preserve FIFO order and wait until the target session is idle.
        Queue,
        /// Contribute to active work when negotiated ACP support exists; never silently interrupt.
        Steer,
        /// Cooperatively cancel active work, then process pending ingress immediately.
        InterruptAndSteer,
    }

    /// Agent channel that receives actionable supervisor incidents in queue mode.
    pub struct BridgeAlertTarget {
        pub channel_id: String,
        pub lane: String,
    }

    /// Installation metadata for a bridge package. The agent may add new packages at runtime.
    pub struct BridgeSpec {
        pub bridge_id: String,
        /// Stable package identifier, for example a future first-party `whatsapp` package.
        pub package_id: String,
        pub display_name: String,
        /// Executable/entrypoint metadata without embedded credential values.
        pub launch_json: String,
        /// Service-specific configuration. Secret fields must be credential-provider handles.
        pub configuration_json: String,
        pub authentication_methods: Vec<AuthenticationMethod>,
        pub ingress_mode: BridgeIngressMode,
        /// Optional and generation-fixed. When absent, supervision remains silent.
        pub alert_target: Option<BridgeAlertTarget>,
        pub desired_running: bool,
        pub health_interval_ms: u64,
        pub credential_validation_interval_ms: u64,
        /// Bounded restart policy prevents a broken bridge from becoming an infinite crash loop.
        pub restart_limit: u64,
        pub restart_window_ms: u64,
    }

    pub struct BridgeRecord {
        pub bridge_id: String,
        pub package_id: String,
        pub display_name: String,
        pub lifecycle: BridgeLifecycle,
        pub ingress_mode: BridgeIngressMode,
        pub alert_target: Option<BridgeAlertTarget>,
        pub desired_running: bool,
        pub generation: u64,
        pub registered_at_ms: u64,
    }

    /// Non-secret durable registrations ordered by bridge identity.
    pub struct BridgeCatalog {
        pub bridges: Vec<BridgeRecord>,
    }

    pub struct ListBridgesRequest {}

    pub struct BridgeReference {
        pub bridge_id: String,
    }

    pub struct ReconcileBridgeRequest {
        pub bridge_id: String,
        pub expected_generation: u64,
        pub desired_running: bool,
    }

    /// Replace package/configuration/policy with compare-and-swap generation control.
    pub struct ReplaceBridgeRequest {
        pub expected_generation: u64,
        pub spec: BridgeSpec,
    }

    /// Truthful observed health from the bridge process, not merely supervisor process liveness.
    pub struct HealthObservation {
        pub bridge_id: String,
        pub observed_at_ms: u64,
        pub process_alive: bool,
        pub service_connected: bool,
        pub can_receive: bool,
        pub can_send: bool,
        pub credential_lifecycle: CredentialLifecycle,
        pub detail_json: String,
    }

    pub struct BridgeStatus {
        pub bridge_id: String,
        pub lifecycle: BridgeLifecycle,
        pub generation: u64,
        pub consecutive_failures: u64,
        pub restart_count_in_window: u64,
        pub next_restart_at_ms: Option<u64>,
        pub last_health: Option<HealthObservation>,
        pub last_error: Option<String>,
    }

    pub struct BeginAuthenticationRequest {
        pub bridge_id: String,
        pub preferred_method: Option<AuthenticationMethod>,
        /// Operator locale/device hints; never raw credentials.
        pub context_json: String,
    }

    /// A challenge can contain renderable QR, URL or prompt data, but never stored credentials.
    pub struct AuthenticationChallenge {
        pub bridge_id: String,
        pub challenge_id: String,
        pub method: AuthenticationMethod,
        pub expires_at_ms: Option<u64>,
        pub presentation_json: String,
    }

    pub struct SubmitAuthenticationRequest {
        pub bridge_id: String,
        pub challenge_id: String,
        /// Ephemeral response passed directly to the package's authentication handler.
        pub response_json: String,
    }

    /// The bridge host stores only a reference to credential material owned by a credential store.
    pub struct CredentialStatus {
        pub bridge_id: String,
        pub lifecycle: CredentialLifecycle,
        pub credential_handle: Option<String>,
        pub validated_at_ms: Option<u64>,
        pub expires_at_ms: Option<u64>,
        pub account_hint: Option<String>,
        pub detail_json: String,
    }

    pub struct BridgeAttachment {
        pub media_type: String,
        pub name: Option<String>,
        /// Reference to durable content; large or private bytes do not cross this contract.
        pub content_handle: String,
    }

    /// Stage one agent-readable local file into Crab-owned content before external delivery.
    pub struct ImportBridgeContentRequest {
        pub bridge_id: String,
        /// Stable caller identity. Repeating the same import with the same bytes is idempotent.
        pub import_id: String,
        /// Absolute source path. Crab copies it; packages never receive this path.
        pub source_path: String,
        pub media_type: String,
        pub name: Option<String>,
    }

    pub struct ImportedBridgeContent {
        pub attachment: BridgeAttachment,
        pub size_bytes: u64,
        pub sha256: String,
    }

    /// One external event normalized just enough to create a durable Crab trigger.
    pub struct BridgeInbound {
        pub bridge_id: String,
        pub external_event_id: String,
        pub received_at_ms: u64,
        pub target_channel_id: String,
        pub sender_json: String,
        pub message_json: String,
        pub attachments: Vec<BridgeAttachment>,
    }

    /// Output of bridge ingestion; `trigger-inbox` owns the durable enqueue/claim protocol.
    pub struct TriggerIntent {
        pub source_id: String,
        pub deduplication_key: String,
        pub target_channel_id: String,
        /// Copied from the registered bridge generation, never selected by an inbound event.
        pub ingress_mode: BridgeIngressMode,
        pub message_json: String,
        pub attachment_handles: Vec<String>,
        /// Durable trigger-inbox identity proving enqueue completed before acknowledgement.
        pub trigger_id: String,
        pub deduplicated: bool,
        pub recorded_at_ms: u64,
    }

    /// A selected message for an external system, not a copied ACP event.
    pub struct BridgeOutbound {
        pub bridge_id: String,
        pub message_id: String,
        pub destination_json: String,
        pub message_json: String,
        pub attachments: Vec<BridgeAttachment>,
        /// Stable across retries so the bridge package can provide idempotent delivery.
        pub idempotency_key: String,
    }

    pub struct DeliveryReference {
        pub bridge_id: String,
        pub message_id: String,
    }

    pub struct DeliveryReceipt {
        pub bridge_id: String,
        pub message_id: String,
        pub lifecycle: DeliveryLifecycle,
        pub external_delivery_id: Option<String>,
        pub attempt: u64,
        pub updated_at_ms: u64,
        pub detail_json: String,
    }

    pub struct BridgeReceipt {
        pub accepted: bool,
        pub recorded_at_ms: u64,
    }

    #[error]
    pub enum BridgeHostError {
        DraftOnly,
        InvalidSpec,
        UnknownBridge,
        DuplicateBridgeConflict,
        GenerationConflict,
        RestartBudgetExhausted,
        AuthenticationUnavailable,
        ChallengeExpired,
        CredentialRejected,
        BridgeUnhealthy,
        DuplicateMessageConflict,
        UnknownDelivery,
        DeliveryFailed,
        PackageProtocolFailed,
        StorageUnavailable,
    }

    /// Register a package-defined bridge under generic Crab supervision.
    #[capability]
    pub async fn register_bridge(request: BridgeSpec) -> Result<BridgeRecord, BridgeHostError>;

    /// List durable registrations without package configuration or credential material.
    #[capability]
    pub async fn list_bridges(request: ListBridgesRequest) -> Result<BridgeCatalog, BridgeHostError>;

    /// Install a new immutable generation; ingress mode changes only through this operation.
    #[capability]
    pub async fn replace_bridge(request: ReplaceBridgeRequest) -> Result<BridgeRecord, BridgeHostError>;

    /// Converge observed lifecycle toward desired lifecycle using bounded recovery.
    #[capability]
    pub async fn reconcile_bridge(request: ReconcileBridgeRequest) -> Result<BridgeStatus, BridgeHostError>;

    #[capability]
    pub async fn report_health(request: HealthObservation) -> Result<BridgeStatus, BridgeHostError>;

    /// Begin QR, phone-code, OAuth or other package-supported authentication.
    #[capability]
    pub async fn begin_authentication(request: BeginAuthenticationRequest) -> Result<AuthenticationChallenge, BridgeHostError>;

    #[capability]
    pub async fn submit_authentication(request: SubmitAuthenticationRequest) -> Result<CredentialStatus, BridgeHostError>;

    /// Actively prove credentials still work; process liveness alone cannot return `Valid`.
    #[capability]
    pub async fn validate_credentials(request: BridgeReference) -> Result<CredentialStatus, BridgeHostError>;

    /// Explicitly invalidate credentials so reset/re-pair is auditable rather than ad hoc deletion.
    #[capability]
    pub async fn invalidate_credentials(request: BridgeReference) -> Result<BridgeReceipt, BridgeHostError>;

    #[capability]
    pub async fn accept_inbound(request: BridgeInbound) -> Result<TriggerIntent, BridgeHostError>;

    /// Copy bounded local content into the private bridge store before selected delivery.
    #[capability]
    pub async fn import_content(request: ImportBridgeContentRequest) -> Result<ImportedBridgeContent, BridgeHostError>;

    #[capability]
    pub async fn deliver_message(request: BridgeOutbound) -> Result<DeliveryReceipt, BridgeHostError>;

    #[capability]
    pub async fn delivery_status(request: DeliveryReference) -> Result<DeliveryReceipt, BridgeHostError>;

    #[capability]
    pub async fn bridge_status(request: BridgeReference) -> Result<BridgeStatus, BridgeHostError>;

    #[capability]
    pub async fn stop_bridge(request: BridgeReference) -> Result<BridgeReceipt, BridgeHostError>;

    /// Gracefully stop the live package while preserving durable desired state for restart.
    #[capability]
    pub async fn suspend_bridge(request: BridgeReference) -> Result<BridgeStatus, BridgeHostError>;
}
