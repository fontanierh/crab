#![deny(warnings, dead_code, unused_imports, unused_variables)]

pub mod checkpoint_fallback;
pub mod checkpoint_turn;
pub mod compatibility;
pub mod config;
pub mod context_assembly;
pub mod context_budget;
pub mod context_diagnostics;
pub mod diagnostics;
pub mod domain;
pub mod error;
pub mod fallback;
pub mod heartbeat;
pub mod memory_flush;
pub mod memory_search;
pub mod memory_snippets;
pub mod onboarding;
pub mod onboarding_completion;
pub mod onboarding_profile_files;
pub mod onboarding_state;
pub mod operator_commands;
pub mod profile;
pub mod prompt_contract;
pub mod rotation;
pub mod rotation_sequence;
pub mod sender_identity;
pub mod startup_reconciliation;
pub mod trust;
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
pub use context_assembly::{
    assemble_turn_context, ContextAssemblyInput, ContextMemorySnippet, CONTEXT_INJECTION_ORDER,
};
pub use context_budget::{
    render_budgeted_turn_context, BudgetedContextOutput, ContextBudgetPolicy, ContextBudgetReport,
    DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_CHARS, DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_COUNT,
    DEFAULT_CONTEXT_MAX_SECTION_CHARS, DEFAULT_CONTEXT_MAX_TOTAL_CHARS,
    DEFAULT_CONTEXT_MAX_TURN_INPUT_CHARS, MEMORY_SNIPPET_DROP_MARKER_PATH,
    TOTAL_CONTEXT_TRUNCATION_MARKER,
};
pub use context_diagnostics::{
    build_context_diagnostics_report, render_context_diagnostics_fixture,
    ContextDiagnosticsFileEntry, ContextDiagnosticsReport, ContextDiagnosticsSnippetEntry,
    CONTEXT_DIAGNOSTICS_FIXTURE_HEADER,
};
pub use diagnostics::{
    build_diagnostic_record, parse_diagnostic_record_json, parse_diagnostics_fixture,
    render_diagnostic_record_json, render_diagnostics_fixture, DiagnosticCategory, DiagnosticEvent,
    DiagnosticRecord, DiagnosticSeverity,
};
pub use domain::{
    BackendKind, Checkpoint, EventEnvelope, EventKind, EventSource, InferenceProfile, LaneState,
    LogicalSession, OutboundRecord, OwnerProfileMetadata, PhysicalSession, ReasoningLevel, Run,
    RunProfileTelemetry, RunStatus, TokenAccounting,
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
pub use memory_search::{
    search_memory, MemorySearchInput, MemorySearchResult, DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT,
    DEFAULT_MEMORY_SEARCH_RESULT_LIMIT, DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS,
};
pub use memory_snippets::{
    resolve_scoped_memory_snippets, ScopedMemorySnippetResolverInput,
    DEFAULT_MEMORY_GLOBAL_FILE_LIMIT, DEFAULT_MEMORY_GLOBAL_RECENCY_DAYS,
    DEFAULT_MEMORY_USER_FILE_LIMIT,
};
pub use onboarding::{
    build_onboarding_prompt, default_onboarding_questions, parse_onboarding_capture_document,
    OnboardingCaptureDocument, OnboardingField, OnboardingQuestion, ONBOARDING_SCHEMA_VERSION,
};
pub use onboarding_completion::{
    execute_onboarding_completion_protocol, OnboardingCompletionEventRuntime,
    OnboardingCompletionInput, OnboardingCompletionOutcome, ONBOARDING_MEMORY_BASELINE_END_MARKER,
    ONBOARDING_MEMORY_BASELINE_START_MARKER,
};
pub use onboarding_profile_files::{
    build_onboarding_profile_documents, persist_onboarding_profile_files,
    OnboardingProfileDocuments, OnboardingProfileWriteOutcome, ONBOARDING_MANAGED_END_MARKER,
    ONBOARDING_MANAGED_START_MARKER, ONBOARDING_NOTES_END_MARKER, ONBOARDING_NOTES_START_MARKER,
};
pub use onboarding_state::{
    apply_onboarding_transition, OnboardingLifecycle, OnboardingState, OnboardingTransition,
};
pub use operator_commands::{
    apply_onboarding_operator_command, apply_operator_command, authorize_operator_command,
    parse_operator_command, render_active_profile_summary, render_resolved_profile_summary,
    OnboardingOperatorCommandOutcome, OperatorActorContext, OperatorCommand,
    OperatorCommandOutcome, OperatorSessionState,
};
pub use profile::{
    resolve_inference_profile, BackendInferenceDefault, BackendInferenceDefaults,
    InferenceProfileOverride, InferenceProfileResolutionInput, ProfileValueSource,
    ResolvedInferenceProfile,
};
pub use prompt_contract::{compile_prompt_contract, PromptContractInput};
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
pub use trust::{
    resolve_sender_trust_context, MemoryScopeMode, MemoryScopeResolution, SenderTrustContext,
    TrustSurface, OWNER_MEMORY_SCOPE_DIRECTORY,
};
pub use workspace::{
    default_workspace_templates, detect_workspace_bootstrap_state, ensure_named_memory_scope,
    ensure_user_memory_scope, ensure_workspace_layout, WorkspaceBootstrapState,
    WorkspaceEnsureOutcome, WorkspaceTemplate, AGENTS_FILE_NAME, BOOTSTRAP_FILE_NAME,
    CLAUDE_LINK_NAME, IDENTITY_FILE_NAME, MEMORY_FILE_NAME, SOUL_FILE_NAME, USER_FILE_NAME,
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
