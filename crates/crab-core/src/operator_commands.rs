use std::fs;
use std::path::Path;

use crate::workspace::{bootstrap_template_contents, BOOTSTRAP_FILE_NAME};
use crate::{apply_onboarding_transition, OnboardingLifecycle, OnboardingTransition};
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
    OnboardingRerun,
    OnboardingResetBootstrap,
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
pub struct OnboardingOperatorCommandOutcome {
    pub next_lifecycle: OnboardingLifecycle,
    pub bootstrap_path: String,
    pub audit_action: String,
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
        "/onboarding" => parse_onboarding_command(&args).map(Some),
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
            state.active_backend = *backend;
            state.active_profile.backend = *backend;

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
        OperatorCommand::OnboardingRerun | OperatorCommand::OnboardingResetBootstrap => {
            Err(CrabError::InvariantViolation {
                context: "operator_command_apply",
                message: "onboarding commands require apply_onboarding_operator_command"
                    .to_string(),
            })
        }
    }
}

pub fn apply_onboarding_operator_command(
    current_lifecycle: &OnboardingLifecycle,
    command: &OperatorCommand,
    actor: &OperatorActorContext,
    workspace_root: &Path,
) -> CrabResult<OnboardingOperatorCommandOutcome> {
    authorize_operator_command(actor)?;
    validate_workspace_root(workspace_root)?;

    let (audit_action, user_message) = match command {
        OperatorCommand::OnboardingRerun => (
            "onboarding.rerun",
            "onboarding rerun scheduled; lifecycle reset to pending and BOOTSTRAP.md rewritten",
        ),
        OperatorCommand::OnboardingResetBootstrap => (
            "onboarding.reset_bootstrap",
            "bootstrap state reset; lifecycle reset to pending and BOOTSTRAP.md rewritten",
        ),
        _ => {
            return Err(CrabError::InvariantViolation {
                context: "operator_onboarding_command_apply",
                message: "command is not an onboarding command".to_string(),
            });
        }
    };

    let next_lifecycle =
        apply_onboarding_transition(current_lifecycle, OnboardingTransition::Reset)?;
    let bootstrap_path = reset_bootstrap_marker(workspace_root)?;

    Ok(OnboardingOperatorCommandOutcome {
        next_lifecycle,
        bootstrap_path: display_path(&bootstrap_path),
        audit_action: audit_action.to_string(),
        user_message: user_message.to_string(),
    })
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
            message: format!("invalid backend {value:?}; expected claude"),
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

fn parse_onboarding_command(args: &[&str]) -> CrabResult<OperatorCommand> {
    let [action, confirmation_token] = args else {
        return Err(CrabError::InvariantViolation {
            context: "operator_command_parse",
            message: "/onboarding requires exactly two arguments: rerun|reset-bootstrap confirm"
                .to_string(),
        });
    };

    validate_confirmation_token(confirmation_token)?;
    match action.to_ascii_lowercase().as_str() {
        "rerun" => Ok(OperatorCommand::OnboardingRerun),
        "reset-bootstrap" | "reset_bootstrap" => Ok(OperatorCommand::OnboardingResetBootstrap),
        _ => Err(CrabError::InvariantViolation {
            context: "operator_command_parse",
            message: format!(
                "invalid onboarding action {action:?}; expected rerun|reset-bootstrap"
            ),
        }),
    }
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

fn validate_confirmation_token(token: &str) -> CrabResult<()> {
    if token.eq_ignore_ascii_case("confirm") {
        return Ok(());
    }
    Err(CrabError::InvariantViolation {
        context: "operator_command_parse",
        message: "onboarding command requires confirmation token \"confirm\"".to_string(),
    })
}

fn validate_workspace_root(workspace_root: &Path) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "operator_onboarding_command_apply",
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let metadata = fs::metadata(workspace_root).map_err(|error| CrabError::Io {
        context: "operator_onboarding_command_apply",
        path: Some(display_path(workspace_root)),
        message: error.to_string(),
    })?;
    if !metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: "operator_onboarding_command_apply",
            message: format!("{} must be a directory", display_path(workspace_root)),
        });
    }

    Ok(())
}

fn reset_bootstrap_marker(workspace_root: &Path) -> CrabResult<std::path::PathBuf> {
    let bootstrap_path = workspace_root.join(BOOTSTRAP_FILE_NAME);
    if bootstrap_path.exists() && !bootstrap_path.is_file() {
        return Err(CrabError::InvariantViolation {
            context: "operator_onboarding_command_apply",
            message: format!("{} must be a regular file", display_path(&bootstrap_path)),
        });
    }

    fs::write(&bootstrap_path, bootstrap_template_contents()).map_err(|error| CrabError::Io {
        context: "operator_onboarding_command_apply",
        path: Some(display_path(&bootstrap_path)),
        message: error.to_string(),
    })?;

    Ok(bootstrap_path)
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn parse_backend(value: &str) -> Option<BackendKind> {
    match value.to_ascii_lowercase().as_str() {
        "claude" => Some(BackendKind::Claude),
        _ => None,
    }
}

fn backend_label(backend: BackendKind) -> &'static str {
    match backend {
        BackendKind::Claude => "claude",
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
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use crate::{
        apply_onboarding_transition, BackendKind, CrabError, InferenceProfile, OnboardingLifecycle,
        OnboardingState, OnboardingTransition, ProfileValueSource, ReasoningLevel,
        ResolvedInferenceProfile,
    };

    use super::{
        apply_onboarding_operator_command, apply_operator_command, authorize_operator_command,
        parse_operator_command, render_active_profile_summary, render_resolved_profile_summary,
        OperatorActorContext, OperatorCommand, OperatorSessionState,
    };

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn session_state() -> OperatorSessionState {
        OperatorSessionState {
            active_backend: BackendKind::Claude,
            active_profile: InferenceProfile {
                backend: BackendKind::Claude,
                model: "claude-sonnet".to_string(),
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

    fn pending_onboarding() -> OnboardingLifecycle {
        OnboardingLifecycle::pending()
    }

    fn in_progress_onboarding() -> OnboardingLifecycle {
        apply_onboarding_transition(
            &OnboardingLifecycle::pending(),
            OnboardingTransition::Start {
                onboarding_session_id: "bootstrap-42".to_string(),
            },
        )
        .expect("start transition should succeed")
    }

    fn with_temp_workspace<T>(label: &str, test_fn: impl FnOnce(&Path) -> T) -> T {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let workspace_path = std::env::temp_dir().join(format!(
            "crab-operator-commands-{label}-{suffix}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&workspace_path);
        let result = test_fn(&workspace_path);
        let _ = fs::remove_dir_all(&workspace_path);
        result
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path)
            .expect("metadata should be readable")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("permissions should be writable");
    }

    #[test]
    fn parses_supported_backend_commands() {
        let cases = [
            ("/backend claude", BackendKind::Claude),
            ("/BACKEND CLAUDE", BackendKind::Claude),
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

        let compact =
            parse_operator_command("/compact confirm").expect("compact command should parse");
        assert!(compact.is_none());

        let reset = parse_operator_command("/reset confirm").expect("reset command should parse");
        assert!(reset.is_none());
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
            "operator_command_parse invariant violation: invalid backend \"pi\"; expected claude"
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
    fn backend_command_is_idempotent_when_value_matches() {
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

        assert_eq!(
            state.active_physical_session_id,
            Some("thread-1".to_string())
        );
        assert!(!outcome.requires_rotation);

        assert_eq!(outcome.user_message, "backend already claude");
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
            "backend=claude, model=claude-sonnet, reasoning=medium"
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
            OperatorCommand::OnboardingRerun,
            OperatorCommand::OnboardingResetBootstrap,
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
                backend: BackendKind::Claude,
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
            "backend=claude (source=turn_override), model=auto (source=channel_override), reasoning=low (source=backend_default)"
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
                    backend: BackendKind::Claude,
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

    #[test]
    fn parses_onboarding_commands_with_confirmation_token() {
        let rerun = parse_operator_command("/onboarding rerun confirm")
            .expect("rerun command should parse")
            .expect("rerun command should be recognized");
        assert_eq!(rerun, OperatorCommand::OnboardingRerun);

        let reset_hyphen = parse_operator_command("/onboarding reset-bootstrap confirm")
            .expect("reset command should parse")
            .expect("reset command should be recognized");
        assert_eq!(reset_hyphen, OperatorCommand::OnboardingResetBootstrap);

        let reset_underscore = parse_operator_command("/onboarding reset_bootstrap CONFIRM")
            .expect("underscore reset command should parse")
            .expect("underscore reset command should be recognized");
        assert_eq!(reset_underscore, OperatorCommand::OnboardingResetBootstrap);
    }

    #[test]
    fn rejects_invalid_onboarding_command_shapes() {
        let missing_parts = parse_operator_command("/onboarding")
            .expect_err("onboarding command without args should fail");
        assert_eq!(
            missing_parts.to_string(),
            "operator_command_parse invariant violation: /onboarding requires exactly two arguments: rerun|reset-bootstrap confirm"
        );

        let invalid_action = parse_operator_command("/onboarding rotate confirm")
            .expect_err("invalid onboarding action should fail");
        assert_eq!(
            invalid_action.to_string(),
            "operator_command_parse invariant violation: invalid onboarding action \"rotate\"; expected rerun|reset-bootstrap"
        );

        let invalid_token = parse_operator_command("/onboarding rerun now")
            .expect_err("invalid confirmation token should fail");
        assert_eq!(
            invalid_token.to_string(),
            "operator_command_parse invariant violation: onboarding command requires confirmation token \"confirm\""
        );

        let extra_arg = parse_operator_command("/onboarding rerun confirm extra")
            .expect_err("onboarding command with extra args should fail");
        assert_eq!(
            extra_arg.to_string(),
            "operator_command_parse invariant violation: /onboarding requires exactly two arguments: rerun|reset-bootstrap confirm"
        );
    }

    #[test]
    fn apply_operator_command_rejects_onboarding_commands_without_context() {
        let mut state = session_state();
        let owner = owner_actor();
        let error = apply_operator_command(&mut state, &OperatorCommand::OnboardingRerun, &owner)
            .expect_err("onboarding command should require dedicated onboarding apply path");
        assert_eq!(
            error.to_string(),
            "operator_command_apply invariant violation: onboarding commands require apply_onboarding_operator_command"
        );
    }

    #[test]
    fn apply_onboarding_command_resets_lifecycle_and_writes_bootstrap_marker() {
        with_temp_workspace("onboarding-rerun", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            let outcome = apply_onboarding_operator_command(
                &in_progress_onboarding(),
                &OperatorCommand::OnboardingRerun,
                &owner_actor(),
                workspace,
            )
            .expect("rerun command should apply");

            assert_eq!(outcome.next_lifecycle.state, OnboardingState::Pending);
            assert_eq!(outcome.next_lifecycle.onboarding_session_id, None);
            assert_eq!(outcome.audit_action, "onboarding.rerun");
            assert_eq!(
                outcome.user_message,
                "onboarding rerun scheduled; lifecycle reset to pending and BOOTSTRAP.md rewritten"
            );

            let bootstrap_path = workspace.join("BOOTSTRAP.md");
            assert_eq!(outcome.bootstrap_path, bootstrap_path.to_string_lossy());
            let bootstrap_contents =
                fs::read_to_string(bootstrap_path).expect("bootstrap marker should be readable");
            assert!(bootstrap_contents.contains("Bootstrap is pending."));
        });
    }

    #[test]
    fn apply_onboarding_reset_bootstrap_command_is_owner_only() {
        with_temp_workspace("onboarding-owner-gate", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            let error = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::OnboardingResetBootstrap,
                &non_owner_actor(),
                workspace,
            )
            .expect_err("non-owner should be rejected");
            assert_eq!(
                error.to_string(),
                "operator_command_authorize invariant violation: sender 0987654321 is not authorized to run operator commands"
            );
        });
    }

    #[test]
    fn apply_onboarding_command_rejects_non_onboarding_variants() {
        with_temp_workspace("onboarding-wrong-command", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            let error = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::ShowProfile,
                &owner_actor(),
                workspace,
            )
            .expect_err("non-onboarding command should fail");
            assert_eq!(
                error.to_string(),
                "operator_onboarding_command_apply invariant violation: command is not an onboarding command"
            );
        });
    }

    #[test]
    fn apply_onboarding_command_validates_workspace_root() {
        let empty_path_error = apply_onboarding_operator_command(
            &pending_onboarding(),
            &OperatorCommand::OnboardingResetBootstrap,
            &owner_actor(),
            Path::new(""),
        )
        .expect_err("empty workspace path should fail");
        assert_eq!(
            empty_path_error.to_string(),
            "operator_onboarding_command_apply invariant violation: workspace_root must not be empty"
        );

        with_temp_workspace("onboarding-workspace-invalid", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            let workspace_file = workspace.join("workspace.txt");
            fs::write(&workspace_file, "not-a-directory")
                .expect("workspace file should be writable");

            let not_dir_error = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::OnboardingResetBootstrap,
                &owner_actor(),
                &workspace_file,
            )
            .expect_err("workspace root file should fail");
            assert_eq!(
                not_dir_error.to_string(),
                format!(
                    "operator_onboarding_command_apply invariant violation: {} must be a directory",
                    workspace_file.to_string_lossy()
                )
            );

            let missing_workspace = workspace.join("missing");
            let missing_error = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::OnboardingResetBootstrap,
                &owner_actor(),
                &missing_workspace,
            )
            .expect_err("missing workspace should fail");
            assert!(matches!(
                missing_error,
                CrabError::Io {
                    context: "operator_onboarding_command_apply",
                    path: Some(path),
                    ..
                } if path == missing_workspace.to_string_lossy()
            ));
        });
    }

    #[test]
    fn apply_onboarding_command_rejects_bootstrap_directory_marker() {
        with_temp_workspace("onboarding-bootstrap-dir", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            fs::create_dir_all(workspace.join("BOOTSTRAP.md"))
                .expect("bootstrap marker directory should be creatable");

            let error = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::OnboardingResetBootstrap,
                &owner_actor(),
                workspace,
            )
            .expect_err("bootstrap directory marker should fail");

            assert_eq!(
                error.to_string(),
                format!(
                    "operator_onboarding_command_apply invariant violation: {} must be a regular file",
                    workspace.join("BOOTSTRAP.md").to_string_lossy()
                )
            );
        });
    }

    #[cfg(unix)]
    #[test]
    fn apply_onboarding_command_surfaces_bootstrap_write_errors() {
        with_temp_workspace("onboarding-write-error", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            set_mode(workspace, 0o555);

            let error = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::OnboardingResetBootstrap,
                &owner_actor(),
                workspace,
            )
            .expect_err("read-only workspace should fail writes");

            assert!(matches!(
                error,
                CrabError::Io {
                    context: "operator_onboarding_command_apply",
                    path: Some(path),
                    ..
                } if path == workspace.join("BOOTSTRAP.md").to_string_lossy()
            ));

            set_mode(workspace, 0o755);
        });
    }

    #[test]
    fn apply_onboarding_command_propagates_invalid_lifecycle_shape() {
        with_temp_workspace("onboarding-invalid-lifecycle", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            let invalid_current = OnboardingLifecycle {
                state: OnboardingState::InProgress,
                onboarding_session_id: None,
            };
            let error = apply_onboarding_operator_command(
                &invalid_current,
                &OperatorCommand::OnboardingResetBootstrap,
                &owner_actor(),
                workspace,
            )
            .expect_err("invalid onboarding lifecycle should fail");

            assert_eq!(
                error.to_string(),
                "onboarding_state_machine invariant violation: state in_progress requires onboarding_session_id"
            );
        });
    }

    #[test]
    fn apply_onboarding_reset_bootstrap_command_has_expected_audit_output() {
        with_temp_workspace("onboarding-reset-audit", |workspace| {
            fs::create_dir_all(workspace).expect("workspace root should be creatable");
            let outcome = apply_onboarding_operator_command(
                &pending_onboarding(),
                &OperatorCommand::OnboardingResetBootstrap,
                &owner_actor(),
                workspace,
            )
            .expect("reset command should apply");

            assert_eq!(outcome.next_lifecycle, OnboardingLifecycle::pending());
            assert_eq!(outcome.audit_action, "onboarding.reset_bootstrap");
            assert_eq!(
                outcome.user_message,
                "bootstrap state reset; lifecycle reset to pending and BOOTSTRAP.md rewritten"
            );
        });
    }
}
