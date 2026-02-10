use crate::{
    validation::validate_non_empty_text, BackendKind, CrabError, CrabResult, InferenceProfile,
    ProfileValueSource, ReasoningLevel, ResolvedInferenceProfile,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperatorCommand {
    SetBackend { backend: BackendKind },
    SetModel { model: String },
    SetReasoning { reasoning_level: ReasoningLevel },
    ShowProfile,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorSessionState {
    pub active_backend: BackendKind,
    pub active_profile: InferenceProfile,
    pub active_physical_session_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorCommandOutcome {
    pub requires_rotation: bool,
    pub user_message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperatorActorContext {
    pub sender_id: String,
    pub sender_is_owner: bool,
}

pub fn parse_operator_command(input: &str) -> CrabResult<Option<OperatorCommand>> {
    let trimmed_input = input.trim();
    if !trimmed_input.starts_with('/') {
        return Ok(None);
    }

    let mut parts = trimmed_input.split_whitespace();
    let command_name = parts
        .next()
        .expect("slash-prefixed commands should always contain one token");
    let args: Vec<&str> = parts.collect();
    let command_key = command_name.to_ascii_lowercase();

    match command_key.as_str() {
        "/backend" => parse_backend_command(&args).map(Some),
        "/model" => parse_model_command(&args).map(Some),
        "/reasoning" => parse_reasoning_command(&args).map(Some),
        "/profile" => parse_profile_command(&args).map(Some),
        _ => Ok(None),
    }
}

pub fn apply_operator_command(
    state: &mut OperatorSessionState,
    command: &OperatorCommand,
    actor: &OperatorActorContext,
) -> CrabResult<OperatorCommandOutcome> {
    authorize_operator_command(actor)?;
    match command {
        OperatorCommand::SetBackend { backend } => {
            let backend_changed = state.active_backend != *backend;
            state.active_backend = *backend;
            state.active_profile.backend = *backend;

            if backend_changed {
                state.active_physical_session_id = None;
                return Ok(OperatorCommandOutcome {
                    requires_rotation: true,
                    user_message: format!(
                        "backend set to {}; active physical session cleared",
                        backend_label(*backend)
                    ),
                });
            }

            Ok(OperatorCommandOutcome {
                requires_rotation: false,
                user_message: format!("backend already {}", backend_label(*backend)),
            })
        }
        OperatorCommand::SetModel { model } => {
            validate_non_empty_text("operator_command_apply", "model", model)?;
            state.active_profile.model = model.clone();
            Ok(OperatorCommandOutcome {
                requires_rotation: false,
                user_message: format!("model set to {}", state.active_profile.model),
            })
        }
        OperatorCommand::SetReasoning { reasoning_level } => {
            state.active_profile.reasoning_level = *reasoning_level;
            Ok(OperatorCommandOutcome {
                requires_rotation: false,
                user_message: format!(
                    "reasoning set to {}",
                    reasoning_level_label(*reasoning_level)
                ),
            })
        }
        OperatorCommand::ShowProfile => Ok(OperatorCommandOutcome {
            requires_rotation: false,
            user_message: render_active_profile_summary(state),
        }),
    }
}

pub fn authorize_operator_command(actor: &OperatorActorContext) -> CrabResult<()> {
    validate_non_empty_text("operator_command_authorize", "sender_id", &actor.sender_id)?;
    if actor.sender_is_owner {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: "operator_command_authorize",
        message: format!(
            "sender {} is not authorized to run operator commands",
            actor.sender_id
        ),
    })
}

#[must_use]
pub fn render_active_profile_summary(state: &OperatorSessionState) -> String {
    format!(
        "backend={}, model={}, reasoning={}",
        backend_label(state.active_profile.backend),
        state.active_profile.model,
        reasoning_level_label(state.active_profile.reasoning_level)
    )
}

#[must_use]
pub fn render_resolved_profile_summary(resolved: &ResolvedInferenceProfile) -> String {
    format!(
        "backend={} (source={}), model={} (source={}), reasoning={} (source={})",
        backend_label(resolved.profile.backend),
        profile_source_label(resolved.backend_source),
        resolved.profile.model,
        profile_source_label(resolved.model_source),
        reasoning_level_label(resolved.profile.reasoning_level),
        profile_source_label(resolved.reasoning_level_source)
    )
}

fn parse_backend_command(args: &[&str]) -> CrabResult<OperatorCommand> {
    let value = single_argument("operator_command_parse", "/backend", args)?;
    let Some(backend) = parse_backend(value) else {
        return Err(CrabError::InvariantViolation {
            context: "operator_command_parse",
            message: format!("invalid backend {value:?}; expected claude|codex|opencode"),
        });
    };
    Ok(OperatorCommand::SetBackend { backend })
}

fn parse_model_command(args: &[&str]) -> CrabResult<OperatorCommand> {
    let value = single_argument("operator_command_parse", "/model", args)?;
    Ok(OperatorCommand::SetModel {
        model: value.to_string(),
    })
}

fn parse_reasoning_command(args: &[&str]) -> CrabResult<OperatorCommand> {
    let value = single_argument("operator_command_parse", "/reasoning", args)?;
    let normalized = value.to_ascii_lowercase();
    let Some(reasoning_level) = ReasoningLevel::parse_token(normalized.as_str()) else {
        return Err(CrabError::InvariantViolation {
            context: "operator_command_parse",
            message: format!(
                "invalid reasoning level {value:?}; expected none|minimal|low|medium|high|xhigh"
            ),
        });
    };
    Ok(OperatorCommand::SetReasoning { reasoning_level })
}

fn parse_profile_command(args: &[&str]) -> CrabResult<OperatorCommand> {
    if args.is_empty() {
        return Ok(OperatorCommand::ShowProfile);
    }
    Err(CrabError::InvariantViolation {
        context: "operator_command_parse",
        message: "/profile does not accept arguments".to_string(),
    })
}

fn single_argument<'a>(
    context: &'static str,
    command_name: &'static str,
    args: &'a [&str],
) -> CrabResult<&'a str> {
    match args {
        [value] => Ok(*value),
        [] => Err(CrabError::InvariantViolation {
            context,
            message: format!("{command_name} requires exactly one argument"),
        }),
        _ => Err(CrabError::InvariantViolation {
            context,
            message: format!("{command_name} requires exactly one argument"),
        }),
    }
}

fn parse_backend(value: &str) -> Option<BackendKind> {
    match value.to_ascii_lowercase().as_str() {
        "claude" => Some(BackendKind::Claude),
        "codex" => Some(BackendKind::Codex),
        "opencode" => Some(BackendKind::OpenCode),
        _ => None,
    }
}

fn backend_label(backend: BackendKind) -> &'static str {
    match backend {
        BackendKind::Claude => "claude",
        BackendKind::Codex => "codex",
        BackendKind::OpenCode => "opencode",
    }
}

fn reasoning_level_label(level: ReasoningLevel) -> &'static str {
    level.as_token()
}

fn profile_source_label(source: ProfileValueSource) -> &'static str {
    match source {
        ProfileValueSource::TurnOverride => "turn_override",
        ProfileValueSource::SessionProfile => "session_profile",
        ProfileValueSource::ChannelOverride => "channel_override",
        ProfileValueSource::BackendDefault => "backend_default",
        ProfileValueSource::GlobalDefault => "global_default",
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        BackendKind, CrabError, InferenceProfile, ProfileValueSource, ReasoningLevel,
        ResolvedInferenceProfile,
    };

    use super::{
        apply_operator_command, authorize_operator_command, parse_operator_command,
        render_active_profile_summary, render_resolved_profile_summary, OperatorActorContext,
        OperatorCommand, OperatorSessionState,
    };

    fn session_state() -> OperatorSessionState {
        OperatorSessionState {
            active_backend: BackendKind::Codex,
            active_profile: InferenceProfile {
                backend: BackendKind::Codex,
                model: "gpt-5-codex".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
            active_physical_session_id: Some("thread-1".to_string()),
        }
    }

    fn owner_actor() -> OperatorActorContext {
        OperatorActorContext {
            sender_id: "1234567890".to_string(),
            sender_is_owner: true,
        }
    }

    fn non_owner_actor() -> OperatorActorContext {
        OperatorActorContext {
            sender_id: "0987654321".to_string(),
            sender_is_owner: false,
        }
    }

    #[test]
    fn parses_supported_backend_commands() {
        let cases = [
            ("/backend claude", BackendKind::Claude),
            ("/backend codex", BackendKind::Codex),
            ("/backend opencode", BackendKind::OpenCode),
            ("/BACKEND CoDeX", BackendKind::Codex),
        ];

        for (input, expected_backend) in cases {
            let command = parse_operator_command(input)
                .expect("backend command should parse")
                .expect("backend command should be recognized");
            assert_eq!(
                command,
                OperatorCommand::SetBackend {
                    backend: expected_backend
                }
            );
        }
    }

    #[test]
    fn parses_model_reasoning_and_profile_commands() {
        let model_command = parse_operator_command("/model auto")
            .expect("model command should parse")
            .expect("model command should be recognized");
        assert_eq!(
            model_command,
            OperatorCommand::SetModel {
                model: "auto".to_string(),
            }
        );

        let reasoning_cases = [
            ("/reasoning none", ReasoningLevel::None),
            ("/reasoning minimal", ReasoningLevel::Minimal),
            ("/reasoning low", ReasoningLevel::Low),
            ("/reasoning medium", ReasoningLevel::Medium),
            ("/reasoning high", ReasoningLevel::High),
            ("/reasoning xhigh", ReasoningLevel::XHigh),
        ];
        for (input, expected_reasoning_level) in reasoning_cases {
            let command = parse_operator_command(input)
                .expect("reasoning command should parse")
                .expect("reasoning command should be recognized");
            assert_eq!(
                command,
                OperatorCommand::SetReasoning {
                    reasoning_level: expected_reasoning_level
                }
            );
        }

        let profile_command = parse_operator_command("/profile")
            .expect("profile command should parse")
            .expect("profile command should be recognized");
        assert_eq!(profile_command, OperatorCommand::ShowProfile);
    }

    #[test]
    fn non_operator_or_unknown_commands_return_none() {
        let text = parse_operator_command("hello world").expect("plain text should parse");
        assert!(text.is_none());

        let unknown = parse_operator_command("/help").expect("unknown command should parse");
        assert!(unknown.is_none());
    }

    #[test]
    fn rejects_invalid_command_shapes() {
        let missing_backend = parse_operator_command("/backend")
            .expect_err("backend command without argument should fail");
        assert_eq!(
            missing_backend.to_string(),
            "operator_command_parse invariant violation: /backend requires exactly one argument"
        );

        let extra_model_arg = parse_operator_command("/model auto extra")
            .expect_err("model command with extra args should fail");
        assert_eq!(
            extra_model_arg.to_string(),
            "operator_command_parse invariant violation: /model requires exactly one argument"
        );

        let invalid_backend =
            parse_operator_command("/backend pi").expect_err("invalid backend should fail");
        assert_eq!(
            invalid_backend.to_string(),
            "operator_command_parse invariant violation: invalid backend \"pi\"; expected claude|codex|opencode"
        );

        let invalid_reasoning =
            parse_operator_command("/reasoning turbo").expect_err("invalid reasoning should fail");
        assert_eq!(
            invalid_reasoning.to_string(),
            "operator_command_parse invariant violation: invalid reasoning level \"turbo\"; expected none|minimal|low|medium|high|xhigh"
        );

        let missing_reasoning = parse_operator_command("/reasoning")
            .expect_err("reasoning command without argument should fail");
        assert_eq!(
            missing_reasoning.to_string(),
            "operator_command_parse invariant violation: /reasoning requires exactly one argument"
        );

        let profile_args = parse_operator_command("/profile now")
            .expect_err("profile command with args should fail");
        assert_eq!(
            profile_args.to_string(),
            "operator_command_parse invariant violation: /profile does not accept arguments"
        );
    }

    #[test]
    fn applies_backend_command_with_rotation_semantics() {
        let mut state = session_state();
        let owner = owner_actor();
        let outcome = apply_operator_command(
            &mut state,
            &OperatorCommand::SetBackend {
                backend: BackendKind::Claude,
            },
            &owner,
        )
        .expect("backend update should apply");

        assert_eq!(state.active_backend, BackendKind::Claude);
        assert_eq!(state.active_profile.backend, BackendKind::Claude);
        assert_eq!(state.active_physical_session_id, None);
        assert!(outcome.requires_rotation);
        assert_eq!(
            outcome.user_message,
            "backend set to claude; active physical session cleared"
        );
    }

    #[test]
    fn backend_command_is_idempotent_when_value_matches() {
        let mut state = session_state();
        let owner = owner_actor();
        let outcome = apply_operator_command(
            &mut state,
            &OperatorCommand::SetBackend {
                backend: BackendKind::Codex,
            },
            &owner,
        )
        .expect("backend update should apply");

        assert_eq!(
            state.active_physical_session_id,
            Some("thread-1".to_string())
        );
        assert!(!outcome.requires_rotation);
        assert_eq!(outcome.user_message, "backend already codex");
    }

    #[test]
    fn applies_model_and_reasoning_commands() {
        let mut state = session_state();
        let owner = owner_actor();
        let model_outcome = apply_operator_command(
            &mut state,
            &OperatorCommand::SetModel {
                model: "auto".to_string(),
            },
            &owner,
        )
        .expect("model update should apply");
        assert_eq!(state.active_profile.model, "auto");
        assert_eq!(model_outcome.user_message, "model set to auto");

        let reasoning_outcome = apply_operator_command(
            &mut state,
            &OperatorCommand::SetReasoning {
                reasoning_level: ReasoningLevel::XHigh,
            },
            &owner,
        )
        .expect("reasoning update should apply");
        assert_eq!(state.active_profile.reasoning_level, ReasoningLevel::XHigh);
        assert_eq!(reasoning_outcome.user_message, "reasoning set to xhigh");
    }

    #[test]
    fn rejects_blank_model_update() {
        let mut state = session_state();
        let owner = owner_actor();
        let error = apply_operator_command(
            &mut state,
            &OperatorCommand::SetModel {
                model: " ".to_string(),
            },
            &owner,
        )
        .expect_err("blank model should fail");
        assert_eq!(
            error.to_string(),
            "operator_command_apply invariant violation: model must not be empty"
        );
    }

    #[test]
    fn show_profile_command_renders_current_active_profile() {
        let mut state = session_state();
        let owner = owner_actor();
        let outcome = apply_operator_command(&mut state, &OperatorCommand::ShowProfile, &owner)
            .expect("show profile should apply");
        assert!(!outcome.requires_rotation);
        assert_eq!(
            outcome.user_message,
            "backend=codex, model=gpt-5-codex, reasoning=medium"
        );
        assert_eq!(render_active_profile_summary(&state), outcome.user_message);
    }

    #[test]
    fn authorization_gate_requires_owner_actor() {
        let commands = [
            OperatorCommand::SetBackend {
                backend: BackendKind::Claude,
            },
            OperatorCommand::SetModel {
                model: "gpt-5".to_string(),
            },
            OperatorCommand::SetReasoning {
                reasoning_level: ReasoningLevel::Low,
            },
            OperatorCommand::ShowProfile,
        ];

        for command in commands {
            let mut state = session_state();
            let error = apply_operator_command(&mut state, &command, &non_owner_actor())
                .expect_err("non-owner should be denied");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "operator_command_authorize",
                    message: "sender 0987654321 is not authorized to run operator commands"
                        .to_string(),
                }
            );
        }
    }

    #[test]
    fn authorization_rejects_blank_sender_id() {
        let error = authorize_operator_command(&OperatorActorContext {
            sender_id: "  ".to_string(),
            sender_is_owner: true,
        })
        .expect_err("blank sender_id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "operator_command_authorize",
                message: "sender_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn resolved_profile_summary_renders_sources() {
        let resolved = ResolvedInferenceProfile {
            profile: InferenceProfile {
                backend: BackendKind::OpenCode,
                model: "auto".to_string(),
                reasoning_level: ReasoningLevel::Low,
            },
            backend_source: ProfileValueSource::TurnOverride,
            model_source: ProfileValueSource::ChannelOverride,
            reasoning_level_source: ProfileValueSource::BackendDefault,
        };

        let summary = render_resolved_profile_summary(&resolved);
        assert_eq!(
            summary,
            "backend=opencode (source=turn_override), model=auto (source=channel_override), reasoning=low (source=backend_default)"
        );

        let source_labels = [
            ProfileValueSource::TurnOverride,
            ProfileValueSource::SessionProfile,
            ProfileValueSource::ChannelOverride,
            ProfileValueSource::BackendDefault,
            ProfileValueSource::GlobalDefault,
        ];
        for source in source_labels {
            let resolved = ResolvedInferenceProfile {
                profile: InferenceProfile {
                    backend: BackendKind::Codex,
                    model: "auto".to_string(),
                    reasoning_level: ReasoningLevel::Medium,
                },
                backend_source: source,
                model_source: source,
                reasoning_level_source: source,
            };
            let rendered = render_resolved_profile_summary(&resolved);
            assert!(rendered.contains("source="));
        }
    }

    #[test]
    fn active_profile_summary_covers_all_reasoning_labels() {
        let cases = [
            (ReasoningLevel::None, "reasoning=none"),
            (ReasoningLevel::Minimal, "reasoning=minimal"),
            (ReasoningLevel::Low, "reasoning=low"),
            (ReasoningLevel::Medium, "reasoning=medium"),
            (ReasoningLevel::High, "reasoning=high"),
            (ReasoningLevel::XHigh, "reasoning=xhigh"),
        ];

        for (level, expected_fragment) in cases {
            let mut state = session_state();
            state.active_profile.reasoning_level = level;
            let rendered = render_active_profile_summary(&state);
            assert!(rendered.contains(expected_fragment));
        }
    }
}
