use crate::{
    evaluate_profile_compatibility, BackendCompatibilityCatalog, CompatibilityIssue, CrabResult,
    InferenceProfile, ReasoningLevel,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FallbackPolicyMode {
    Strict,
    Compatible,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FallbackDecision {
    pub mode: FallbackPolicyMode,
    pub requested_profile: InferenceProfile,
    pub effective_profile: InferenceProfile,
    pub issues: Vec<CompatibilityIssue>,
    pub rejected: bool,
    pub fallback_applied: bool,
    pub notes: Vec<String>,
}

impl FallbackDecision {
    pub fn is_accepted(&self) -> bool {
        !self.rejected
    }
}

pub fn apply_fallback_policy(
    profile: &InferenceProfile,
    catalog: &BackendCompatibilityCatalog,
    mode: FallbackPolicyMode,
) -> CrabResult<FallbackDecision> {
    let report = evaluate_profile_compatibility(profile, catalog)?;
    let issues = report.issues;

    if issues.is_empty() {
        return Ok(FallbackDecision {
            mode,
            requested_profile: profile.clone(),
            effective_profile: profile.clone(),
            issues,
            rejected: false,
            fallback_applied: false,
            notes: Vec::new(),
        });
    }

    if mode == FallbackPolicyMode::Strict {
        return Ok(FallbackDecision {
            mode,
            requested_profile: profile.clone(),
            effective_profile: profile.clone(),
            issues,
            rejected: true,
            fallback_applied: false,
            notes: Vec::new(),
        });
    }

    let mut effective_profile = profile.clone();
    let rules = catalog.rules_for_backend(profile.backend);
    let mut notes = Vec::new();

    for issue in &issues {
        match issue {
            CompatibilityIssue::UnsupportedModel { .. } => {
                let replacement_model = rules
                    .supported_models
                    .first()
                    .cloned()
                    .unwrap_or(effective_profile.model.clone());

                notes.push(format!(
                    "fallback model for {:?}: {} -> {}",
                    profile.backend, effective_profile.model, replacement_model
                ));
                effective_profile.model = replacement_model;
            }
            CompatibilityIssue::UnsupportedReasoningLevel {
                reasoning_level, ..
            } => {
                let replacement_level =
                    nearest_reasoning_level(*reasoning_level, &rules.supported_reasoning_levels);
                notes.push(format!(
                    "fallback reasoning for {:?}: {:?} -> {:?}",
                    profile.backend, effective_profile.reasoning_level, replacement_level
                ));
                effective_profile.reasoning_level = replacement_level;
            }
        }
    }

    Ok(FallbackDecision {
        mode,
        requested_profile: profile.clone(),
        fallback_applied: profile != &effective_profile,
        rejected: false,
        effective_profile,
        issues,
        notes,
    })
}

fn reasoning_level_rank(level: ReasoningLevel) -> i8 {
    match level {
        ReasoningLevel::None => 0,
        ReasoningLevel::Minimal => 1,
        ReasoningLevel::Low => 2,
        ReasoningLevel::Medium => 3,
        ReasoningLevel::High => 4,
        ReasoningLevel::XHigh => 5,
    }
}

fn nearest_reasoning_level(
    requested: ReasoningLevel,
    supported: &[ReasoningLevel],
) -> ReasoningLevel {
    let requested_rank = reasoning_level_rank(requested);

    supported
        .iter()
        .copied()
        .min_by_key(|candidate| {
            let candidate_rank = reasoning_level_rank(*candidate);
            ((candidate_rank - requested_rank).abs(), candidate_rank)
        })
        .unwrap_or(requested)
}

#[cfg(test)]
mod tests {
    use crate::{BackendCompatibilityCatalog, BackendCompatibilityRules, BackendKind, CrabError};

    use super::{apply_fallback_policy, FallbackPolicyMode};
    use crate::{InferenceProfile, ReasoningLevel, AUTO_MODEL_ALIAS};

    fn claude_profile(model: &str, reasoning_level: ReasoningLevel) -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Claude,
            model: model.to_string(),
            reasoning_level,
        }
    }

    fn policy_catalog() -> BackendCompatibilityCatalog {
        BackendCompatibilityCatalog {
            claude: BackendCompatibilityRules {
                supported_models: vec!["claude-sonnet".to_string()],
                supported_reasoning_levels: vec![
                    ReasoningLevel::Low,
                    ReasoningLevel::Medium,
                    ReasoningLevel::High,
                ],
            },
        }
    }

    #[test]
    fn strict_mode_rejects_incompatible_profiles() {
        let requested = claude_profile("unsupported-model", ReasoningLevel::XHigh);
        let decision =
            apply_fallback_policy(&requested, &policy_catalog(), FallbackPolicyMode::Strict)
                .expect("policy evaluation should succeed");

        assert!(!decision.is_accepted());
        assert!(decision.rejected);
        assert!(!decision.fallback_applied);
        assert_eq!(decision.requested_profile, requested);
        assert_eq!(decision.effective_profile, requested);
        assert_eq!(decision.issues.len(), 2);
        assert!(decision.notes.is_empty());
    }

    #[test]
    fn strict_mode_accepts_compatible_profiles() {
        let requested = claude_profile("claude-sonnet", ReasoningLevel::Medium);
        let decision =
            apply_fallback_policy(&requested, &policy_catalog(), FallbackPolicyMode::Strict)
                .expect("policy evaluation should succeed");

        assert!(decision.is_accepted());
        assert!(!decision.rejected);
        assert!(!decision.fallback_applied);
        assert!(decision.issues.is_empty());
        assert!(decision.notes.is_empty());
        assert_eq!(decision.effective_profile, requested);
    }

    #[test]
    fn compatible_mode_applies_model_and_reasoning_fallbacks() {
        let requested = claude_profile("legacy-model", ReasoningLevel::XHigh);
        let decision = apply_fallback_policy(
            &requested,
            &policy_catalog(),
            FallbackPolicyMode::Compatible,
        )
        .expect("policy evaluation should succeed");

        assert!(decision.is_accepted());
        assert!(!decision.rejected);
        assert!(decision.fallback_applied);
        assert_eq!(decision.issues.len(), 2);
        assert_eq!(decision.effective_profile.model, "claude-sonnet");
        assert_eq!(
            decision.effective_profile.reasoning_level,
            ReasoningLevel::High
        );
        assert_eq!(decision.notes.len(), 2);
    }

    #[test]
    fn compatible_mode_breaks_nearest_reasoning_ties_toward_lower_level() {
        let mut catalog = policy_catalog();
        catalog.claude.supported_reasoning_levels = vec![ReasoningLevel::Low, ReasoningLevel::High];

        let requested = claude_profile("claude-sonnet", ReasoningLevel::Medium);
        let decision = apply_fallback_policy(&requested, &catalog, FallbackPolicyMode::Compatible)
            .expect("policy evaluation should succeed");

        assert!(decision.fallback_applied);
        assert_eq!(
            decision.effective_profile.reasoning_level,
            ReasoningLevel::Low
        );
        assert_eq!(decision.issues.len(), 1);
    }

    #[test]
    fn compatible_mode_can_shift_from_none_to_minimal() {
        let mut catalog = policy_catalog();
        catalog.claude.supported_reasoning_levels = vec![ReasoningLevel::Minimal];

        let requested = claude_profile("claude-sonnet", ReasoningLevel::None);
        let decision = apply_fallback_policy(&requested, &catalog, FallbackPolicyMode::Compatible)
            .expect("policy evaluation should succeed");

        assert!(decision.fallback_applied);
        assert_eq!(
            decision.effective_profile.reasoning_level,
            ReasoningLevel::Minimal
        );
        assert_eq!(decision.issues.len(), 1);
    }

    #[test]
    fn compatible_mode_treats_auto_model_as_compatible() {
        let requested = claude_profile(AUTO_MODEL_ALIAS, ReasoningLevel::High);
        let decision = apply_fallback_policy(
            &requested,
            &policy_catalog(),
            FallbackPolicyMode::Compatible,
        )
        .expect("policy evaluation should succeed");

        assert!(decision.is_accepted());
        assert!(!decision.fallback_applied);
        assert!(decision.issues.is_empty());
        assert!(decision.notes.is_empty());
        assert_eq!(decision.effective_profile, requested);
    }

    #[test]
    fn invalid_profile_input_propagates_validation_errors() {
        let requested = claude_profile(" ", ReasoningLevel::Low);
        let error =
            apply_fallback_policy(&requested, &policy_catalog(), FallbackPolicyMode::Strict)
                .expect_err("blank model should fail");

        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "compatibility_validator",
                message: "profile.model must not be empty".to_string(),
            }
        );
    }
}
