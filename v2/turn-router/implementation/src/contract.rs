// A bridge is not a channel. The turn router is the explicit policy boundary that resolves a
// bridge/scheduler/self-work target into a native channel and settles the durable trigger only
// after ACP input acceptance.
boxology::contract! {
    pub struct PutRouteRequest {
        pub target_channel_id: String,
        pub lane: String,
        pub binding_id: String,
        /// Required only to replace an existing route. An exact retry is always idempotent.
        pub expected_generation: Option<u64>,
    }

    pub struct RouteReference {
        pub target_channel_id: String,
    }

    pub struct ChannelRoute {
        pub target_channel_id: String,
        pub lane: String,
        pub binding_id: String,
        pub generation: u64,
        pub updated_at_ms: u64,
    }

    pub struct DrainLaneRequest {
        pub worker_id: String,
        pub lane: String,
        pub limit: u64,
        pub lease_duration_ms: u64,
        pub retry_delay_ms: u64,
        /// A transient failure on this attempt is dead-lettered at or above this value.
        pub max_attempts: u64,
    }

    pub enum RoutedTriggerOutcome {
        Completed,
        RetryScheduled,
        DeadLettered,
    }

    pub struct RoutedTrigger {
        pub trigger_id: String,
        pub binding_id: Option<String>,
        pub outcome: RoutedTriggerOutcome,
        /// Stable diagnostic tag only; trigger payloads and credentials never appear here.
        pub detail: Option<String>,
    }

    pub struct DrainLaneReport {
        pub lane: String,
        pub claimed: u64,
        pub completed: u64,
        pub retry_scheduled: u64,
        pub dead_lettered: u64,
        pub results: Vec<RoutedTrigger>,
    }

    #[error]
    pub enum TurnRouterError {
        InvalidRoute,
        RouteConflict,
        UnknownRoute,
        InvalidDrain,
        TriggerInboxUnavailable,
        NativeChannelUnavailable,
        InvalidTriggerPayload,
        StorageUnavailable,
    }

    /// Register idempotently or replace with compare-and-swap generation control.
    #[capability]
    pub async fn put_route(request: PutRouteRequest) -> Result<ChannelRoute, TurnRouterError>;

    #[capability]
    pub async fn resolve_route(request: RouteReference) -> Result<ChannelRoute, TurnRouterError>;

    /// Claim and process one lane serially. Returns after ACP accepts each turn, never after model
    /// completion. On a successful return, every claimed lease is completed, retried or
    /// dead-lettered.
    #[capability]
    pub async fn drain_lane(request: DrainLaneRequest) -> Result<DrainLaneReport, TurnRouterError>;
}
