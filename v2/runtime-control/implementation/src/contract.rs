// This authored contract is the source for Boxology-generated Rust and language-neutral schemas.
boxology::contract! {
    pub struct RuntimeStatusRequest {}

    /// Immutable, non-secret evidence captured by the running process before local IPC starts.
    pub struct RuntimeAttestation {
        /// SHA-256 of the resolved semantic topology and referenced bootstrap prompt contents.
        /// An unconfigured draft graph has no fingerprint.
        pub configuration_fingerprint: Option<String>,
        pub started_at_ms: u64,
        pub process_id: u64,
    }

    #[error]
    pub enum RuntimeControlError {
        DraftOnly,
        InvalidAttestation,
        ClockUnavailable,
    }

    #[capability]
    pub async fn runtime_status(request: RuntimeStatusRequest) -> Result<RuntimeAttestation, RuntimeControlError>;
}
