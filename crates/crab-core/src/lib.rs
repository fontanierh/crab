#![deny(warnings, dead_code, unused_imports, unused_variables)]

pub mod compatibility;
pub mod config;
pub mod domain;
pub mod error;
pub mod fallback;
pub mod profile;
pub mod rotation;
mod validation;

pub use compatibility::{
    evaluate_profile_compatibility, BackendCompatibilityCatalog, BackendCompatibilityRules,
    CompatibilityIssue, CompatibilityReport, AUTO_MODEL_ALIAS,
};
pub use config::RuntimeConfig;
pub use domain::{
    BackendKind, Checkpoint, EventEnvelope, EventKind, EventSource, InferenceProfile, LaneState,
    LogicalSession, OutboundRecord, PhysicalSession, ReasoningLevel, Run, RunProfileTelemetry,
    RunStatus, TokenAccounting,
};
pub use error::{CrabError, CrabResult};
pub use fallback::{apply_fallback_policy, FallbackDecision, FallbackPolicyMode};
pub use profile::{
    resolve_inference_profile, BackendInferenceDefault, BackendInferenceDefaults,
    InferenceProfileOverride, InferenceProfileResolutionInput, ProfileValueSource,
    ResolvedInferenceProfile,
};
pub use rotation::{
    evaluate_rotation_triggers, ManualRotationRequest, RotationTrigger, RotationTriggerDecision,
    RotationTriggerInput,
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
