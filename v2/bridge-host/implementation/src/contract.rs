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
        pub lifecycle: BridgeLifecycle,
        pub generation: u64,
        pub registered_at_ms: u64,
    }

    pub struct BridgeReference {
        pub bridge_id: String,
    }

    pub struct ReconcileBridgeRequest {
        pub bridge_id: String,
        pub expected_generation: u64,
        pub desired_running: bool,
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
        pub message_json: String,
        pub attachment_handles: Vec<String>,
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
        GenerationConflict,
        RestartBudgetExhausted,
        AuthenticationUnavailable,
        ChallengeExpired,
        CredentialRejected,
        BridgeUnhealthy,
        DuplicateMessageConflict,
        DeliveryFailed,
        PackageProtocolFailed,
    }

    /// Register a package-defined bridge under generic Crab supervision.
    #[capability]
    pub async fn register_bridge(request: BridgeSpec) -> Result<BridgeRecord, BridgeHostError>;

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

    #[capability]
    pub async fn deliver_message(request: BridgeOutbound) -> Result<DeliveryReceipt, BridgeHostError>;

    #[capability]
    pub async fn delivery_status(request: DeliveryReference) -> Result<DeliveryReceipt, BridgeHostError>;

    #[capability]
    pub async fn bridge_status(request: BridgeReference) -> Result<BridgeStatus, BridgeHostError>;

    #[capability]
    pub async fn stop_bridge(request: BridgeReference) -> Result<BridgeReceipt, BridgeHostError>;
}
