#![deny(warnings, dead_code, unused_imports, unused_variables)]

pub mod config;
pub mod domain;
pub mod error;
pub mod profile;

pub use config::RuntimeConfig;
pub use domain::{
    BackendKind, Checkpoint, EventEnvelope, EventKind, EventSource, InferenceProfile, LaneState,
    LogicalSession, OutboundRecord, PhysicalSession, ReasoningLevel, Run, RunStatus,
    TokenAccounting,
};
pub use error::{CrabError, CrabResult};
pub use profile::{
    resolve_inference_profile, BackendInferenceDefault, BackendInferenceDefaults,
    InferenceProfileOverride, InferenceProfileResolutionInput, ProfileValueSource,
    ResolvedInferenceProfile,
};

#[cfg(test)]
mod tests {
    use super::{CrabError, CrabResult};

    #[test]
    fn result_alias_is_usable() {
        let ok_value: CrabResult<usize> = Ok(42);
        assert_eq!(ok_value, Ok(42));

        let err_value: CrabResult<usize> = Err(CrabError::MissingConfig {
            key: "CRAB_DISCORD_TOKEN",
        });
        assert!(matches!(
            err_value,
            Err(CrabError::MissingConfig {
                key: "CRAB_DISCORD_TOKEN"
            })
        ));
    }
}
