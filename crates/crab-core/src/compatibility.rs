use crate::{
    validation::validate_non_empty_text, BackendKind, CrabResult, InferenceProfile, ReasoningLevel,
};

pub const AUTO_MODEL_ALIAS: &str = "auto";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendCompatibilityRules {
    pub supported_models: Vec<String>,
    pub supported_reasoning_levels: Vec<ReasoningLevel>,
}

impl BackendCompatibilityRules {
    pub fn supports_model(&self, model: &str) -> bool {
        if model == AUTO_MODEL_ALIAS {
            return true;
        }
        if self.supported_models.is_empty() {
            return true;
        }
        self.supported_models.iter().any(|value| value == model)
    }

    pub fn supports_reasoning_level(&self, reasoning_level: ReasoningLevel) -> bool {
        if self.supported_reasoning_levels.is_empty() {
            return true;
        }
        self.supported_reasoning_levels
            .iter()
            .any(|value| *value == reasoning_level)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendCompatibilityCatalog {
    pub claude: BackendCompatibilityRules,
    pub codex: BackendCompatibilityRules,
    pub opencode: BackendCompatibilityRules,
}

impl BackendCompatibilityCatalog {
    pub fn rules_for_backend(&self, backend: BackendKind) -> &BackendCompatibilityRules {
        match backend {
            BackendKind::Claude => &self.claude,
            BackendKind::Codex => &self.codex,
            BackendKind::OpenCode => &self.opencode,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompatibilityIssue {
    UnsupportedModel {
        backend: BackendKind,
        model: String,
    },
    UnsupportedReasoningLevel {
        backend: BackendKind,
        reasoning_level: ReasoningLevel,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityReport {
    pub issues: Vec<CompatibilityIssue>,
}

impl CompatibilityReport {
    pub fn is_compatible(&self) -> bool {
        self.issues.is_empty()
    }
}

pub fn evaluate_profile_compatibility(
    profile: &InferenceProfile,
    catalog: &BackendCompatibilityCatalog,
) -> CrabResult<CompatibilityReport> {
    validate_compatibility_inputs(profile, catalog)?;

    let rules = catalog.rules_for_backend(profile.backend);
    let mut issues = Vec::new();

    if !rules.supports_model(&profile.model) {
        issues.push(CompatibilityIssue::UnsupportedModel {
            backend: profile.backend,
            model: profile.model.clone(),
        });
    }

    if !rules.supports_reasoning_level(profile.reasoning_level) {
        issues.push(CompatibilityIssue::UnsupportedReasoningLevel {
            backend: profile.backend,
            reasoning_level: profile.reasoning_level,
        });
    }

    Ok(CompatibilityReport { issues })
}

fn validate_compatibility_inputs(
    profile: &InferenceProfile,
    catalog: &BackendCompatibilityCatalog,
) -> CrabResult<()> {
    let context = "compatibility_validator";
    validate_non_empty_text(context, "profile.model", &profile.model)?;

    validate_supported_models(context, "catalog.claude.supported_models", &catalog.claude)?;
    validate_supported_models(context, "catalog.codex.supported_models", &catalog.codex)?;
    validate_supported_models(
        context,
        "catalog.opencode.supported_models",
        &catalog.opencode,
    )?;

    Ok(())
}

fn validate_supported_models(
    context: &'static str,
    field: &str,
    rules: &BackendCompatibilityRules,
) -> CrabResult<()> {
    for model in &rules.supported_models {
        validate_non_empty_text(context, field, model)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{BackendKind, CrabError, InferenceProfile, ReasoningLevel};

    use super::{
        evaluate_profile_compatibility, BackendCompatibilityCatalog, BackendCompatibilityRules,
        CompatibilityIssue, AUTO_MODEL_ALIAS,
    };

    fn profile(
        backend: BackendKind,
        model: &str,
        reasoning_level: ReasoningLevel,
    ) -> InferenceProfile {
        InferenceProfile {
            backend,
            model: model.to_string(),
            reasoning_level,
        }
    }

    fn catalog() -> BackendCompatibilityCatalog {
        BackendCompatibilityCatalog {
            claude: BackendCompatibilityRules {
                supported_models: vec!["claude-sonnet".to_string(), "claude-opus".to_string()],
                supported_reasoning_levels: vec![
                    ReasoningLevel::None,
                    ReasoningLevel::Low,
                    ReasoningLevel::Medium,
                    ReasoningLevel::High,
                ],
            },
            codex: BackendCompatibilityRules {
                supported_models: Vec::new(),
                supported_reasoning_levels: vec![
                    ReasoningLevel::None,
                    ReasoningLevel::Minimal,
                    ReasoningLevel::Low,
                    ReasoningLevel::Medium,
                    ReasoningLevel::High,
                    ReasoningLevel::XHigh,
                ],
            },
            opencode: BackendCompatibilityRules {
                supported_models: vec!["o4-mini".to_string()],
                supported_reasoning_levels: vec![ReasoningLevel::Low, ReasoningLevel::Medium],
            },
        }
    }

    fn assert_input_error(
        mut profile: InferenceProfile,
        mut catalog: BackendCompatibilityCatalog,
        profile_model: Option<&str>,
        field_backend: Option<BackendKind>,
        expected_message: &str,
    ) {
        if let Some(model) = profile_model {
            profile.model = model.to_string();
        }
        if let Some(field_backend) = field_backend {
            match field_backend {
                BackendKind::Claude => catalog.claude.supported_models = vec![" ".to_string()],
                BackendKind::Codex => catalog.codex.supported_models = vec![" ".to_string()],
                BackendKind::OpenCode => catalog.opencode.supported_models = vec![" ".to_string()],
            }
        }

        let error = evaluate_profile_compatibility(&profile, &catalog)
            .expect_err("invalid inputs should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "compatibility_validator",
                message: expected_message.to_string(),
            }
        );
    }

    #[test]
    fn rules_lookup_and_wildcards_work_for_all_backends() {
        let source = catalog();
        let cases = [
            (
                BackendKind::Claude,
                "claude-sonnet",
                ReasoningLevel::Medium,
                true,
                true,
            ),
            (
                BackendKind::Codex,
                "gpt-5-codex",
                ReasoningLevel::XHigh,
                true,
                true,
            ),
            (
                BackendKind::OpenCode,
                "o4-mini",
                ReasoningLevel::Low,
                true,
                true,
            ),
        ];
        for (backend, model, reasoning_level, model_ok, reasoning_ok) in cases {
            let rules = source.rules_for_backend(backend);
            assert_eq!(rules.supports_model(model), model_ok);
            assert_eq!(
                rules.supports_reasoning_level(reasoning_level),
                reasoning_ok
            );
        }
    }

    #[test]
    fn auto_model_alias_is_always_compatible() {
        let input = profile(
            BackendKind::OpenCode,
            AUTO_MODEL_ALIAS,
            ReasoningLevel::Medium,
        );
        let report = evaluate_profile_compatibility(&input, &catalog())
            .expect("compatibility check should succeed");
        assert!(report.is_compatible());
    }

    #[test]
    fn empty_reasoning_catalog_is_a_wildcard() {
        let mut rules = catalog();
        rules.opencode.supported_reasoning_levels = Vec::new();

        let input = profile(BackendKind::OpenCode, "o4-mini", ReasoningLevel::XHigh);
        let report = evaluate_profile_compatibility(&input, &rules)
            .expect("compatibility check should succeed");
        assert!(report.is_compatible());
    }

    #[test]
    fn reports_model_and_reasoning_incompatibilities() {
        let cases = [
            (
                profile(BackendKind::Claude, "unsupported", ReasoningLevel::Medium),
                vec![CompatibilityIssue::UnsupportedModel {
                    backend: BackendKind::Claude,
                    model: "unsupported".to_string(),
                }],
            ),
            (
                profile(BackendKind::OpenCode, "o4-mini", ReasoningLevel::XHigh),
                vec![CompatibilityIssue::UnsupportedReasoningLevel {
                    backend: BackendKind::OpenCode,
                    reasoning_level: ReasoningLevel::XHigh,
                }],
            ),
            (
                profile(BackendKind::OpenCode, "bad", ReasoningLevel::XHigh),
                vec![
                    CompatibilityIssue::UnsupportedModel {
                        backend: BackendKind::OpenCode,
                        model: "bad".to_string(),
                    },
                    CompatibilityIssue::UnsupportedReasoningLevel {
                        backend: BackendKind::OpenCode,
                        reasoning_level: ReasoningLevel::XHigh,
                    },
                ],
            ),
        ];

        for (input, expected_issues) in cases {
            let report = evaluate_profile_compatibility(&input, &catalog())
                .expect("compatibility check should succeed");
            assert_eq!(report.issues, expected_issues);
            assert_eq!(report.is_compatible(), expected_issues.is_empty());
        }
    }

    #[test]
    fn accepts_fully_compatible_profiles() {
        let cases = [
            profile(BackendKind::Claude, "claude-sonnet", ReasoningLevel::Low),
            profile(BackendKind::Codex, "any-model", ReasoningLevel::XHigh),
            profile(BackendKind::OpenCode, "o4-mini", ReasoningLevel::Medium),
        ];
        for input in cases {
            let report = evaluate_profile_compatibility(&input, &catalog())
                .expect("compatibility check should succeed");
            assert!(report.issues.is_empty());
            assert!(report.is_compatible());
        }
    }

    #[test]
    fn rejects_invalid_models_in_profile_or_catalog() {
        assert_input_error(
            profile(BackendKind::Claude, "claude-sonnet", ReasoningLevel::Medium),
            catalog(),
            Some(" "),
            None,
            "profile.model must not be empty",
        );
        assert_input_error(
            profile(BackendKind::Claude, "claude-sonnet", ReasoningLevel::Medium),
            catalog(),
            None,
            Some(BackendKind::Claude),
            "catalog.claude.supported_models must not be empty",
        );
        assert_input_error(
            profile(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::High),
            catalog(),
            None,
            Some(BackendKind::Codex),
            "catalog.codex.supported_models must not be empty",
        );
        assert_input_error(
            profile(BackendKind::OpenCode, "o4-mini", ReasoningLevel::Medium),
            catalog(),
            None,
            Some(BackendKind::OpenCode),
            "catalog.opencode.supported_models must not be empty",
        );
    }
}
