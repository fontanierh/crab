// `crab-trigger` survives as this small durable primitive. Bridges, schedules, the agent itself and
// operators all use the same idempotent enqueue/lease/settle protocol.
boxology::contract! {
    pub enum TriggerSource {
        Bridge,
        Scheduler,
        SelfWork,
        Operator,
    }

    pub enum TriggerMode {
        /// Wait for idle and preserve lane FIFO order.
        Queue,
        /// Contribute to active work when the target negotiated support; never interrupt.
        Steer,
        /// Cooperatively cancel current work, then drain accepted lane input immediately.
        InterruptAndSteer,
    }

    pub enum TriggerState {
        Pending,
        Leased,
        Completed,
        RetryScheduled,
        DeadLettered,
    }

    pub enum SettlementOutcome {
        Completed,
        Retry,
        DeadLetter,
    }

    pub struct TriggerAttachment {
        pub media_type: String,
        pub name: Option<String>,
        pub content_handle: String,
    }

    /// Caller-supplied envelope. The source ID plus deduplication key identifies one logical event.
    pub struct EnqueueTrigger {
        pub source: TriggerSource,
        /// Bridge ID, schedule ID, agent ID or operator identity.
        pub source_id: String,
        pub deduplication_key: String,
        /// Channel/session router key. It does not imply the source itself is a channel.
        pub target_channel_id: String,
        pub lane: String,
        pub mode: TriggerMode,
        pub not_before_ms: u64,
        pub message_json: String,
        pub attachments: Vec<TriggerAttachment>,
    }

    pub struct TriggerRecord {
        pub trigger_id: String,
        pub source: TriggerSource,
        pub source_id: String,
        pub deduplication_key: String,
        pub target_channel_id: String,
        pub lane: String,
        pub mode: TriggerMode,
        pub state: TriggerState,
        pub enqueued_at_ms: u64,
        pub not_before_ms: u64,
        pub message_json: String,
        pub attachments: Vec<TriggerAttachment>,
        pub attempt: u64,
    }

    pub struct ClaimTriggers {
        pub worker_id: String,
        pub lane: String,
        pub limit: u64,
        pub lease_duration_ms: u64,
        pub now_ms: u64,
    }

    /// A lease makes delivery at-least-once. Expiry returns unfinished work to the pending queue.
    pub struct TriggerLease {
        pub trigger: TriggerRecord,
        pub lease_token: String,
        pub worker_id: String,
        pub leased_at_ms: u64,
        pub expires_at_ms: u64,
    }

    pub struct TriggerBatch {
        /// Records are ordered by lane sequence; callers must process each lane serially.
        pub leases: Vec<TriggerLease>,
    }

    pub struct ExtendLease {
        pub trigger_id: String,
        pub lease_token: String,
        pub extend_by_ms: u64,
        pub now_ms: u64,
    }

    pub struct SettleTrigger {
        pub trigger_id: String,
        pub lease_token: String,
        pub outcome: SettlementOutcome,
        /// Populated for retry/dead-letter diagnostics; never interpreted as agent input.
        pub detail: Option<String>,
        pub retry_not_before_ms: Option<u64>,
        pub settled_at_ms: u64,
    }

    pub struct TriggerReference {
        pub trigger_id: String,
    }

    pub struct TriggerReceipt {
        pub trigger_id: String,
        pub state: TriggerState,
        /// True when this enqueue returned an existing record for the same idempotency key.
        pub deduplicated: bool,
        pub recorded_at_ms: u64,
    }

    #[error]
    pub enum TriggerInboxError {
        DraftOnly,
        InvalidSource,
        InvalidTarget,
        InvalidLane,
        InvalidPayload,
        InvalidClaim,
        InvalidLease,
        DuplicateKeyConflict,
        UnknownTrigger,
        LeaseMismatch,
        LeaseExpired,
        InvalidSettlement,
        StorageUnavailable,
    }

    /// Durably enqueue before acknowledging the caller. Retries return the original record.
    #[capability]
    pub async fn enqueue(request: EnqueueTrigger) -> Result<TriggerReceipt, TriggerInboxError>;

    /// Lease ready records in stable lane order; crashed leases become claimable again.
    #[capability]
    pub async fn claim(request: ClaimTriggers) -> Result<TriggerBatch, TriggerInboxError>;

    #[capability]
    pub async fn extend_lease(request: ExtendLease) -> Result<TriggerLease, TriggerInboxError>;

    /// Settle only after the downstream turn is durably accepted or definitively rejected.
    #[capability]
    pub async fn settle(request: SettleTrigger) -> Result<TriggerReceipt, TriggerInboxError>;

    #[capability]
    pub async fn inspect(request: TriggerReference) -> Result<TriggerRecord, TriggerInboxError>;
}
