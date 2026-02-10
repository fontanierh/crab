#![deny(warnings, dead_code, unused_imports, unused_variables)]

pub mod checkpoint_fallback;
pub mod checkpoint_turn;
pub mod compatibility;
pub mod config;
pub mod diagnostics;
pub mod domain;
pub mod error;
pub mod fallback;
pub mod heartbeat;
pub mod memory_flush;
pub mod operator_commands;
pub mod profile;
pub mod rotation;
pub mod rotation_sequence;
pub mod sender_identity;
pub mod startup_reconciliation;
mod validation;
pub mod workspace;

pub use checkpoint_fallback::{
    build_fallback_checkpoint_document, TranscriptEntry, TranscriptEntryRole,
    DEFAULT_FALLBACK_TRANSCRIPT_TAIL_LIMIT,
};
pub use checkpoint_turn::{
    build_checkpoint_prompt, parse_checkpoint_turn_document, resolve_checkpoint_turn_output,
    CheckpointTurnArtifact, CheckpointTurnDocument, CheckpointTurnResolution,
    DEFAULT_CHECKPOINT_MAX_ATTEMPTS,
};
pub use compatibility::{
    evaluate_profile_compatibility, BackendCompatibilityCatalog, BackendCompatibilityRules,
    CompatibilityIssue, CompatibilityReport, AUTO_MODEL_ALIAS,
};
pub use config::{OwnerConfig, OwnerProfileDefaults, RuntimeConfig};
pub use diagnostics::{
    build_diagnostic_record, parse_diagnostic_record_json, parse_diagnostics_fixture,
    render_diagnostic_record_json, render_diagnostics_fixture, DiagnosticCategory, DiagnosticEvent,
    DiagnosticRecord, DiagnosticSeverity,
};
pub use domain::{
    BackendKind, Checkpoint, EventEnvelope, EventKind, EventSource, InferenceProfile, LaneState,
    LogicalSession, OutboundRecord, PhysicalSession, ReasoningLevel, Run, RunProfileTelemetry,
    RunStatus, TokenAccounting,
};
pub use error::{CrabError, CrabResult};
pub use fallback::{apply_fallback_policy, FallbackDecision, FallbackPolicyMode};
pub use heartbeat::{
    execute_heartbeat_cycle, ActiveRunHeartbeat, BackendHeartbeat, DispatcherHeartbeat,
    HeartbeatComponent, HeartbeatEvent, HeartbeatOutcome, HeartbeatPolicy, HeartbeatRunAction,
    HeartbeatRuntime,
};
pub use memory_flush::{
    build_memory_flush_prompt, finalize_hidden_memory_flush, should_run_memory_flush_cycle,
    HiddenMemoryFlushOutcome, MemoryFlushAck, MEMORY_FLUSH_DONE_TOKEN, MEMORY_FLUSH_NO_REPLY_TOKEN,
};
pub use operator_commands::{
    apply_operator_command, parse_operator_command, render_active_profile_summary,
    render_resolved_profile_summary, OperatorCommand, OperatorCommandOutcome, OperatorSessionState,
};
pub use profile::{
    resolve_inference_profile, BackendInferenceDefault, BackendInferenceDefaults,
    InferenceProfileOverride, InferenceProfileResolutionInput, ProfileValueSource,
    ResolvedInferenceProfile,
};
pub use rotation::{
    evaluate_rotation_triggers, ManualRotationRequest, RotationTrigger, RotationTriggerDecision,
    RotationTriggerInput,
};
pub use rotation_sequence::{
    execute_rotation_sequence, RotationSequenceOutcome, RotationSequenceRuntime,
};
pub use sender_identity::{
    resolve_sender_identity, ResolvedSenderIdentity, SenderConversationKind, SenderIdentityInput,
    OWNER_CANONICAL_USER_KEY,
};
pub use startup_reconciliation::{
    execute_startup_reconciliation, StartupReconciliationOutcome,
    StartupReconciliationRecoveredRun, StartupReconciliationRuntime,
};
pub use workspace::{
    default_workspace_templates, detect_workspace_bootstrap_state, ensure_user_memory_scope,
    ensure_workspace_layout, WorkspaceBootstrapState, WorkspaceEnsureOutcome, WorkspaceTemplate,
    AGENTS_FILE_NAME, BOOTSTRAP_FILE_NAME, CLAUDE_LINK_NAME, IDENTITY_FILE_NAME, MEMORY_FILE_NAME,
    SOUL_FILE_NAME, USER_FILE_NAME,
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
