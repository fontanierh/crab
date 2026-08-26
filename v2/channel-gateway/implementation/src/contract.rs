// This authored contract is the source for Boxology-generated Rust and language-neutral schemas.
boxology::contract! {
    /// Whether attach created a binding, reused its live session or recovered an unavailable one.
    pub enum ChannelAttachmentDisposition {
        Created,
        ReusedLiveSession,
        ResumedUnavailableSession,
        ReplacedUnavailableSession,
    }

    /// Complete non-secret intent required to attach one native UI identity.
    pub struct AttachChannelRequest {
        pub channel_id: String,
        pub adapter_id: String,
        pub agent_id: String,
        pub working_directory: String,
        pub bootstrap_prompt: Option<String>,
        /// Exact non-secret agent session metadata as a JSON object.
        pub session_metadata_json: String,
        /// Exact adapter-owned destination metadata as a JSON object.
        pub native_channel_json: String,
    }

    /// Stable native binding and the one live Crab-owned ACP session behind it.
    pub struct ChannelAttachment {
        pub binding_id: String,
        pub channel_id: String,
        pub adapter_id: String,
        pub session_id: String,
        pub disposition: ChannelAttachmentDisposition,
    }

    #[error]
    pub enum ChannelGatewayError {
        DraftOnly,
        InvalidRequest,
        UnknownAgent,
        AgentUnavailable,
        AttachmentConflict,
        ChannelUnavailable,
        StorageUnavailable,
    }

    /// Attach idempotently. A matching live binding is reused. An unavailable session is resumed
    /// before explicit unsupported/missing recovery falls back to replacement. A changed request
    /// never replaces a live session implicitly.
    #[capability]
    pub async fn attach_channel(request: AttachChannelRequest) -> Result<ChannelAttachment, ChannelGatewayError>;
}
