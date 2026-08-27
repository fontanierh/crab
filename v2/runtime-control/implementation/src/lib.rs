mod contract;

pub use contract::*;

use std::time::{SystemTime, UNIX_EPOCH};

/// Immutable runtime identity supplied to owner-only health and deployment surfaces.
pub struct RuntimeControl {
    attestation: RuntimeAttestation,
}

impl RuntimeControl {
    /// Capture current process identity and start time with an optional semantic config digest.
    pub fn for_current_process(
        configuration_fingerprint: Option<String>,
    ) -> Result<Self, RuntimeControlError> {
        let duration = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| RuntimeControlError::ClockUnavailable)?;
        let started_at_ms = u64::try_from(duration.as_millis())
            .map_err(|_| RuntimeControlError::ClockUnavailable)?;
        Self::new(
            configuration_fingerprint,
            started_at_ms,
            u64::from(std::process::id()),
        )
    }

    /// Construct deterministic attestation for composition and contract tests.
    pub fn new(
        configuration_fingerprint: Option<String>,
        started_at_ms: u64,
        process_id: u64,
    ) -> Result<Self, RuntimeControlError> {
        if process_id == 0
            || configuration_fingerprint
                .as_deref()
                .is_some_and(|fingerprint| !valid_fingerprint(fingerprint))
        {
            return Err(RuntimeControlError::InvalidAttestation);
        }
        Ok(Self {
            attestation: RuntimeAttestation {
                configuration_fingerprint,
                started_at_ms,
                process_id,
            },
        })
    }
}

#[boxology::implementation]
impl RuntimeControl {
    pub async fn runtime_status(
        &self,
        context: boxology::CallContext,
        request: RuntimeStatusRequest,
    ) -> Result<RuntimeAttestation, RuntimeControlError> {
        let _ = (context, request);
        Ok(self.attestation.clone())
    }
}

fn valid_fingerprint(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub mod generated {
    include!("../../generated/adapter/adapter.rs");
}

#[cfg(test)]
mod tests {
    use boxology_contract::{CallContext, Caller, CancelToken, TraceContext};

    use super::{RuntimeControl, RuntimeControlError, RuntimeStatusRequest};

    #[tokio::test]
    async fn attestation_is_immutable_and_rejects_ambiguous_identity() {
        let control = RuntimeControl::new(Some("a".repeat(64)), 42, 7).expect("attestation starts");
        let status = control
            .runtime_status(
                CallContext::new(
                    Caller::Anonymous,
                    None,
                    CancelToken::new(),
                    TraceContext::empty(),
                    None,
                ),
                RuntimeStatusRequest {},
            )
            .await
            .expect("status is available");
        assert_eq!(status.configuration_fingerprint, Some("a".repeat(64)));
        assert_eq!(status.started_at_ms, 42);
        assert_eq!(status.process_id, 7);
        assert!(matches!(
            RuntimeControl::new(Some("not-sha256".into()), 42, 7),
            Err(RuntimeControlError::InvalidAttestation)
        ));
        assert!(matches!(
            RuntimeControl::new(None, 42, 0),
            Err(RuntimeControlError::InvalidAttestation)
        ));
    }
}
