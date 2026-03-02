#![deny(warnings, dead_code, unused_imports, unused_variables)]

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
pub(crate) mod file_signal;
pub mod heartbeat;
pub mod memory_citation;
pub mod memory_get;
pub mod memory_search;
pub mod memory_snippets;
pub mod onboarding;
pub mod onboarding_completion;
pub mod onboarding_profile_files;
pub mod onboarding_state;
pub mod operator_commands;
pub mod pending_rotation;
pub mod profile;
pub mod prompt_contract;
pub mod rotation;
pub mod rotation_sequence;
pub mod self_trigger;
pub mod sender_identity;
pub mod startup_reconciliation;
pub mod state_schema;
#[cfg(test)]
pub(crate) mod test_support;
pub mod trust;
mod validation;
pub mod workspace;

pub use checkpoint_turn::{
    parse_checkpoint_turn_document, CheckpointTurnArtifact, CheckpointTurnDocument,
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
    estimate_token_count, render_budgeted_turn_context, BudgetedContextOutput, ContextBudgetPolicy,
    ContextBudgetReport, ContextSectionTokenUsage, ContextSnippetTokenUsage,
    DEFAULT_CONTEXT_MAX_IDENTITY_TOKENS, DEFAULT_CONTEXT_MAX_LATEST_CHECKPOINT_TOKENS,
    DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_COUNT, DEFAULT_CONTEXT_MAX_MEMORY_SNIPPET_TOKENS,
    DEFAULT_CONTEXT_MAX_MEMORY_TOKENS, DEFAULT_CONTEXT_MAX_PROMPT_CONTRACT_TOKENS,
    DEFAULT_CONTEXT_MAX_SOUL_TOKENS, DEFAULT_CONTEXT_MAX_TURN_INPUT_TOKENS,
    DEFAULT_CONTEXT_MAX_USER_TOKENS,
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
pub use memory_citation::{
    disclosure_text_for_source, evaluate_memory_citation_policy, format_memory_citation,
    MemoryCitationMode, MemoryCitationPolicyDecision, MemoryCitationPolicyInput,
    MemoryRecallSource, SHARED_CONTEXT_DISCLOSURE_TEXT,
};
pub use memory_get::{
    get_memory, MemoryGetInput, MemoryGetResult, DEFAULT_MEMORY_GET_MAX_CHARS,
    DEFAULT_MEMORY_GET_MAX_LINES,
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
    build_onboarding_extraction_prompt, build_onboarding_prompt, default_onboarding_questions,
    parse_onboarding_capture_document, OnboardingCaptureDocument, OnboardingField,
    OnboardingQuestion, ONBOARDING_CAPTURE_INCOMPLETE_TOKEN, ONBOARDING_SCHEMA_VERSION,
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
pub use pending_rotation::{
    consume_pending_rotation, read_pending_rotations, validate_pending_rotation,
    write_pending_rotation, PendingRotation, PENDING_ROTATIONS_DIR_NAME,
};
pub use profile::{
    resolve_inference_profile, BackendInferenceDefault, BackendInferenceDefaults,
    InferenceProfileOverride, InferenceProfileResolutionInput, ProfileValueSource,
    ResolvedInferenceProfile,
};
pub use prompt_contract::{compile_prompt_contract, PromptContractInput};
pub use rotation::RotationTrigger;
pub use rotation_sequence::{
    execute_rotation_sequence, RotationSequenceOutcome, RotationSequenceRuntime,
};
pub use self_trigger::{
    consume_pending_trigger, read_pending_triggers, validate_pending_trigger,
    write_pending_trigger, PendingTrigger, PENDING_TRIGGERS_DIR_NAME,
};
pub use sender_identity::{
    resolve_sender_identity, ResolvedSenderIdentity, SenderConversationKind, SenderIdentityInput,
    OWNER_CANONICAL_USER_KEY,
};
pub use startup_reconciliation::{
    execute_startup_reconciliation, StartupReconciliationOutcome,
    StartupReconciliationRecoveredRun, StartupReconciliationRepairedPhysicalSession,
    StartupReconciliationRuntime,
};
pub use state_schema::{
    ensure_state_schema_version, evaluate_state_schema_compatibility, state_schema_marker_path,
    StateSchemaCompatibilityReport, StateSchemaCompatibilityStatus, StateSchemaMigrationEvent,
    StateSchemaMigrationEventKind, StateSchemaMigrationOutcome, StateSchemaVersionMarker,
    CURRENT_STATE_SCHEMA_VERSION, MIN_SUPPORTED_STATE_SCHEMA_VERSION,
    STATE_SCHEMA_MARKER_FILE_NAME,
};
pub use trust::{
    resolve_sender_trust_context, MemoryScopeMode, MemoryScopeResolution, SenderTrustContext,
    TrustSurface, OWNER_MEMORY_SCOPE_DIRECTORY,
};
pub use workspace::{
    default_workspace_templates, detect_workspace_bootstrap_state, ensure_named_memory_scope,
    ensure_user_memory_scope, ensure_workspace_layout, WorkspaceBootstrapState,
    WorkspaceEnsureOutcome, WorkspaceTemplate, AGENTS_FILE_NAME, AGENTS_SKILLS_ROOT_RELATIVE_PATH,
    BOOTSTRAP_FILE_NAME, CLAUDE_LINK_NAME, CLAUDE_SKILLS_LINK_RELATIVE_PATH, IDENTITY_FILE_NAME,
    MEMORY_FILE_NAME, ROTATE_SESSION_SKILL_FILE_RELATIVE_PATH, ROTATE_SESSION_SKILL_NAME,
    SKILL_AUTHORING_POLICY_FILE_RELATIVE_PATH, SKILL_AUTHORING_POLICY_SKILL_NAME, SOUL_FILE_NAME,
    USER_FILE_NAME,
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
