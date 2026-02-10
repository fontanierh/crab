use crate::{BackendKind, CrabError, CrabResult, InferenceProfile, ReasoningLevel};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProfileValueSource {
    TurnOverride,
    SessionProfile,
    ChannelOverride,
    BackendDefault,
    GlobalDefault,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InferenceProfileOverride {
    pub backend: Option<BackendKind>,
    pub model: Option<String>,
    pub reasoning_level: Option<ReasoningLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendInferenceDefault {
    pub model: Option<String>,
    pub reasoning_level: Option<ReasoningLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct BackendInferenceDefaults {
    pub claude: BackendInferenceDefault,
    pub codex: BackendInferenceDefault,
    pub opencode: BackendInferenceDefault,
}

impl BackendInferenceDefaults {
    pub fn for_backend(&self, backend: BackendKind) -> &BackendInferenceDefault {
        match backend {
            BackendKind::Claude => &self.claude,
            BackendKind::Codex => &self.codex,
            BackendKind::OpenCode => &self.opencode,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InferenceProfileResolutionInput {
    pub turn_override: Option<InferenceProfileOverride>,
    pub session_profile: Option<InferenceProfile>,
    pub channel_override: Option<InferenceProfileOverride>,
    pub backend_defaults: BackendInferenceDefaults,
    pub global_default: InferenceProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedInferenceProfile {
    pub profile: InferenceProfile,
    pub backend_source: ProfileValueSource,
    pub model_source: ProfileValueSource,
    pub reasoning_level_source: ProfileValueSource,
}

pub fn resolve_inference_profile(
    input: &InferenceProfileResolutionInput,
) -> CrabResult<ResolvedInferenceProfile> {
    validate_resolution_input(input)?;

    let (backend, backend_source) =
        if let Some(backend) = input.turn_override.as_ref().and_then(|value| value.backend) {
            (backend, ProfileValueSource::TurnOverride)
        } else if let Some(session_profile) = input.session_profile.as_ref() {
            (session_profile.backend, ProfileValueSource::SessionProfile)
        } else if let Some(backend) = input
            .channel_override
            .as_ref()
            .and_then(|value| value.backend)
        {
            (backend, ProfileValueSource::ChannelOverride)
        } else {
            (
                input.global_default.backend,
                ProfileValueSource::GlobalDefault,
            )
        };

    let backend_default = input.backend_defaults.for_backend(backend);

    let (model, model_source) = if let Some(model) = input
        .turn_override
        .as_ref()
        .and_then(|value| value.model.as_ref())
    {
        (model.clone(), ProfileValueSource::TurnOverride)
    } else if let Some(session_profile) = input.session_profile.as_ref() {
        (
            session_profile.model.clone(),
            ProfileValueSource::SessionProfile,
        )
    } else if let Some(model) = input
        .channel_override
        .as_ref()
        .and_then(|value| value.model.as_ref())
    {
        (model.clone(), ProfileValueSource::ChannelOverride)
    } else if let Some(model) = backend_default.model.as_ref() {
        (model.clone(), ProfileValueSource::BackendDefault)
    } else {
        (
            input.global_default.model.clone(),
            ProfileValueSource::GlobalDefault,
        )
    };

    let (reasoning_level, reasoning_level_source) = if let Some(reasoning_level) = input
        .turn_override
        .as_ref()
        .and_then(|value| value.reasoning_level)
    {
        (reasoning_level, ProfileValueSource::TurnOverride)
    } else if let Some(session_profile) = input.session_profile.as_ref() {
        (
            session_profile.reasoning_level,
            ProfileValueSource::SessionProfile,
        )
    } else if let Some(reasoning_level) = input
        .channel_override
        .as_ref()
        .and_then(|value| value.reasoning_level)
    {
        (reasoning_level, ProfileValueSource::ChannelOverride)
    } else if let Some(reasoning_level) = backend_default.reasoning_level {
        (reasoning_level, ProfileValueSource::BackendDefault)
    } else {
        (
            input.global_default.reasoning_level,
            ProfileValueSource::GlobalDefault,
        )
    };

    Ok(ResolvedInferenceProfile {
        profile: InferenceProfile {
            backend,
            model,
            reasoning_level,
        },
        backend_source,
        model_source,
        reasoning_level_source,
    })
}

fn validate_resolution_input(input: &InferenceProfileResolutionInput) -> CrabResult<()> {
    let context = "inference_profile_resolver";

    validate_non_empty_text(context, "global_default.model", &input.global_default.model)?;

    if let Some(model) = input.backend_defaults.claude.model.as_deref() {
        validate_non_empty_text(context, "backend_defaults.claude.model", model)?;
    }
    if let Some(model) = input.backend_defaults.codex.model.as_deref() {
        validate_non_empty_text(context, "backend_defaults.codex.model", model)?;
    }
    if let Some(model) = input.backend_defaults.opencode.model.as_deref() {
        validate_non_empty_text(context, "backend_defaults.opencode.model", model)?;
    }

    if let Some(session_profile) = input.session_profile.as_ref() {
        validate_non_empty_text(context, "session_profile.model", &session_profile.model)?;
    }
    if let Some(model) = input
        .turn_override
        .as_ref()
        .and_then(|value| value.model.as_deref())
    {
        validate_non_empty_text(context, "turn_override.model", model)?;
    }
    if let Some(model) = input
        .channel_override
        .as_ref()
        .and_then(|value| value.model.as_deref())
    {
        validate_non_empty_text(context, "channel_override.model", model)?;
    }

    Ok(())
}

fn validate_non_empty_text(context: &'static str, field: &str, value: &str) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{BackendKind, CrabError, InferenceProfile, ReasoningLevel};

    use super::{
        resolve_inference_profile, BackendInferenceDefault, BackendInferenceDefaults,
        InferenceProfileOverride, InferenceProfileResolutionInput, ProfileValueSource,
    };

    fn global_default() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Codex,
            model: "global-model".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        }
    }

    fn backend_defaults() -> BackendInferenceDefaults {
        BackendInferenceDefaults {
            claude: BackendInferenceDefault {
                model: Some("claude-default".to_string()),
                reasoning_level: Some(ReasoningLevel::Low),
            },
            codex: BackendInferenceDefault {
                model: Some("codex-default".to_string()),
                reasoning_level: Some(ReasoningLevel::High),
            },
            opencode: BackendInferenceDefault {
                model: Some("opencode-default".to_string()),
                reasoning_level: Some(ReasoningLevel::Minimal),
            },
        }
    }

    fn base_input() -> InferenceProfileResolutionInput {
        InferenceProfileResolutionInput {
            turn_override: None,
            session_profile: None,
            channel_override: None,
            backend_defaults: backend_defaults(),
            global_default: global_default(),
        }
    }

    fn session_profile() -> InferenceProfile {
        InferenceProfile {
            backend: BackendKind::Claude,
            model: "session-model".to_string(),
            reasoning_level: ReasoningLevel::XHigh,
        }
    }

    fn profile_override(
        backend: Option<BackendKind>,
        model: Option<&str>,
        reasoning_level: Option<ReasoningLevel>,
    ) -> InferenceProfileOverride {
        InferenceProfileOverride {
            backend,
            model: model.map(str::to_string),
            reasoning_level,
        }
    }

    fn assert_blank_model_error(input: InferenceProfileResolutionInput, field: &str) {
        let error = resolve_inference_profile(&input).expect_err("blank model should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "inference_profile_resolver",
                message: format!("{field} must not be empty"),
            }
        );
    }

    #[test]
    fn backend_precedence_matrix() {
        let cases = [
            (
                Some(BackendKind::OpenCode),
                true,
                Some(BackendKind::Claude),
                BackendKind::OpenCode,
                ProfileValueSource::TurnOverride,
            ),
            (
                None,
                true,
                Some(BackendKind::OpenCode),
                BackendKind::Claude,
                ProfileValueSource::SessionProfile,
            ),
            (
                None,
                false,
                Some(BackendKind::OpenCode),
                BackendKind::OpenCode,
                ProfileValueSource::ChannelOverride,
            ),
            (
                None,
                false,
                None,
                BackendKind::Codex,
                ProfileValueSource::GlobalDefault,
            ),
        ];

        for (turn_backend, has_session, channel_backend, expected_backend, expected_source) in cases
        {
            let mut input = base_input();
            input.turn_override =
                turn_backend.map(|backend| profile_override(Some(backend), None, None));
            input.channel_override =
                channel_backend.map(|backend| profile_override(Some(backend), None, None));
            input.session_profile = has_session.then(session_profile);

            let resolved = resolve_inference_profile(&input).expect("resolution should succeed");
            assert_eq!(resolved.profile.backend, expected_backend);
            assert_eq!(resolved.backend_source, expected_source);
        }
    }

    #[test]
    fn model_precedence_matrix() {
        let cases = [
            (
                Some("turn-model"),
                true,
                Some("channel-model"),
                Some("codex-default"),
                "turn-model",
                ProfileValueSource::TurnOverride,
            ),
            (
                None,
                true,
                Some("channel-model"),
                Some("codex-default"),
                "session-model",
                ProfileValueSource::SessionProfile,
            ),
            (
                None,
                false,
                Some("channel-model"),
                Some("codex-default"),
                "channel-model",
                ProfileValueSource::ChannelOverride,
            ),
            (
                None,
                false,
                None,
                Some("codex-default"),
                "codex-default",
                ProfileValueSource::BackendDefault,
            ),
            (
                None,
                false,
                None,
                None,
                "global-model",
                ProfileValueSource::GlobalDefault,
            ),
        ];

        for (
            turn_model,
            has_session,
            channel_model,
            backend_default_model,
            expected_model,
            expected_source,
        ) in cases
        {
            let mut input = base_input();
            input.turn_override = turn_model.map(|model| profile_override(None, Some(model), None));
            input.channel_override =
                channel_model.map(|model| profile_override(None, Some(model), None));
            input.session_profile = has_session.then(session_profile);
            input.backend_defaults.codex.model = backend_default_model.map(str::to_string);

            let resolved = resolve_inference_profile(&input).expect("resolution should succeed");
            assert_eq!(resolved.profile.model, expected_model);
            assert_eq!(resolved.model_source, expected_source);
        }
    }

    #[test]
    fn reasoning_precedence_matrix() {
        let cases = [
            (
                Some(ReasoningLevel::None),
                true,
                Some(ReasoningLevel::Low),
                Some(ReasoningLevel::High),
                ReasoningLevel::None,
                ProfileValueSource::TurnOverride,
            ),
            (
                None,
                true,
                Some(ReasoningLevel::Minimal),
                Some(ReasoningLevel::High),
                ReasoningLevel::XHigh,
                ProfileValueSource::SessionProfile,
            ),
            (
                None,
                false,
                Some(ReasoningLevel::High),
                Some(ReasoningLevel::High),
                ReasoningLevel::High,
                ProfileValueSource::ChannelOverride,
            ),
            (
                None,
                false,
                None,
                Some(ReasoningLevel::High),
                ReasoningLevel::High,
                ProfileValueSource::BackendDefault,
            ),
            (
                None,
                false,
                None,
                None,
                ReasoningLevel::Medium,
                ProfileValueSource::GlobalDefault,
            ),
        ];

        for (
            turn_reasoning,
            has_session,
            channel_reasoning,
            backend_default_reasoning,
            expected_reasoning,
            expected_source,
        ) in cases
        {
            let mut input = base_input();
            input.turn_override =
                turn_reasoning.map(|value| profile_override(None, None, Some(value)));
            input.channel_override =
                channel_reasoning.map(|value| profile_override(None, None, Some(value)));
            input.session_profile = has_session.then(session_profile);
            input.backend_defaults.codex.reasoning_level = backend_default_reasoning;

            let resolved = resolve_inference_profile(&input).expect("resolution should succeed");
            assert_eq!(resolved.profile.reasoning_level, expected_reasoning);
            assert_eq!(resolved.reasoning_level_source, expected_source);
        }
    }

    #[test]
    fn backend_defaults_follow_resolved_backend() {
        let cases = [
            (BackendKind::Claude, "claude-default", ReasoningLevel::Low),
            (BackendKind::Codex, "codex-default", ReasoningLevel::High),
            (
                BackendKind::OpenCode,
                "opencode-default",
                ReasoningLevel::Minimal,
            ),
        ];

        for (backend, expected_model, expected_reasoning) in cases {
            let mut input = base_input();
            input.global_default.backend = backend;

            let resolved = resolve_inference_profile(&input).expect("resolution should succeed");
            assert_eq!(resolved.profile.model, expected_model);
            assert_eq!(resolved.profile.reasoning_level, expected_reasoning);
        }
    }

    #[test]
    fn rejects_blank_models_with_field_specific_errors() {
        let mut global_input = base_input();
        global_input.global_default.model = " ".to_string();
        assert_blank_model_error(global_input, "global_default.model");

        let mut session_input = base_input();
        session_input.session_profile = Some(InferenceProfile {
            backend: BackendKind::Claude,
            model: " ".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        });
        assert_blank_model_error(session_input, "session_profile.model");

        let mut turn_input = base_input();
        turn_input.turn_override = Some(profile_override(None, Some(" "), None));
        assert_blank_model_error(turn_input, "turn_override.model");

        let mut channel_input = base_input();
        channel_input.channel_override = Some(profile_override(None, Some(" "), None));
        assert_blank_model_error(channel_input, "channel_override.model");

        let backend_fields = [
            ("backend_defaults.claude.model", BackendKind::Claude),
            ("backend_defaults.codex.model", BackendKind::Codex),
            ("backend_defaults.opencode.model", BackendKind::OpenCode),
        ];
        for (field, backend) in backend_fields {
            let mut input = base_input();
            match backend {
                BackendKind::Claude => input.backend_defaults.claude.model = Some(" ".to_string()),
                BackendKind::Codex => input.backend_defaults.codex.model = Some(" ".to_string()),
                BackendKind::OpenCode => {
                    input.backend_defaults.opencode.model = Some(" ".to_string())
                }
            }
            assert_blank_model_error(input, field);
        }
    }

    #[test]
    fn allows_missing_optional_backend_default_models() {
        let mut input = base_input();
        input.backend_defaults.claude.model = None;
        input.backend_defaults.opencode.model = None;

        let resolved = resolve_inference_profile(&input).expect("resolution should succeed");
        assert_eq!(resolved.profile.backend, BackendKind::Codex);
        assert_eq!(resolved.profile.model, "codex-default");
        assert_eq!(resolved.model_source, ProfileValueSource::BackendDefault);
    }
}
