use crate::validation::validate_non_empty_text;
use crate::{BackendKind, CrabResult, OwnerProfileMetadata, ReasoningLevel};

const PROMPT_CONTRACT_CONTEXT: &str = "prompt_contract_compile";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptContractInput {
    pub backend: BackendKind,
    pub model: String,
    pub reasoning_level: ReasoningLevel,
    pub sender_id: String,
    pub sender_is_owner: bool,
    pub owner_profile: Option<OwnerProfileMetadata>,
    pub memory_tools_enabled: bool,
}

pub fn compile_prompt_contract(input: &PromptContractInput) -> CrabResult<String> {
    validate_prompt_contract_input(input)?;

    let sections = [
        render_runtime_profile_section(input),
        render_memory_search_first_section(input.memory_tools_enabled),
        render_owner_context_section(input),
        render_runtime_notes_section(),
        render_messaging_semantics_section(),
    ];

    Ok(sections.join("\n\n"))
}

fn validate_prompt_contract_input(input: &PromptContractInput) -> CrabResult<()> {
    validate_non_empty_text(PROMPT_CONTRACT_CONTEXT, "model", &input.model)?;
    validate_non_empty_text(PROMPT_CONTRACT_CONTEXT, "sender_id", &input.sender_id)?;

    if let Some(owner_profile) = &input.owner_profile {
        if let Some(machine_location) = owner_profile.machine_location.as_deref() {
            validate_non_empty_text(
                PROMPT_CONTRACT_CONTEXT,
                "owner_profile.machine_location",
                machine_location,
            )?;
        }
        if let Some(machine_timezone) = owner_profile.machine_timezone.as_deref() {
            validate_non_empty_text(
                PROMPT_CONTRACT_CONTEXT,
                "owner_profile.machine_timezone",
                machine_timezone,
            )?;
        }
        if let Some(default_model) = owner_profile.default_model.as_deref() {
            validate_non_empty_text(
                PROMPT_CONTRACT_CONTEXT,
                "owner_profile.default_model",
                default_model,
            )?;
        }
    }

    Ok(())
}

fn render_runtime_profile_section(input: &PromptContractInput) -> String {
    format!(
        "## RUNTIME_PROFILE\n- backend: {}\n- model: {}\n- reasoning_level: {}",
        backend_token(input.backend),
        input.model.trim(),
        input.reasoning_level.as_token()
    )
}

fn render_memory_search_first_section(memory_tools_enabled: bool) -> String {
    let mut lines = vec![
        "## MEMORY_SEARCH_FIRST".to_string(),
        "- Before claiming information is unknown, run `memory_search` for relevant prior context."
            .to_string(),
    ];

    if memory_tools_enabled {
        lines.push(
            "- When citing stored context, call `memory_get` for exact line ranges before quoting."
                .to_string(),
        );
    } else {
        lines.push(
            "- Memory tools are disabled for this run; explicitly note lookup limitations when relevant."
                .to_string(),
        );
    }

    lines.join("\n")
}

fn render_owner_context_section(input: &PromptContractInput) -> String {
    let owner_profile = input.owner_profile.as_ref();
    let machine_location = owner_profile
        .and_then(|profile| profile.machine_location.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("(none)");
    let machine_timezone = owner_profile
        .and_then(|profile| profile.machine_timezone.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("(none)");
    let default_backend = owner_profile
        .and_then(|profile| profile.default_backend)
        .map(backend_token)
        .unwrap_or("(none)");
    let default_model = owner_profile
        .and_then(|profile| profile.default_model.as_deref())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("(none)");
    let default_reasoning_level = owner_profile
        .and_then(|profile| profile.default_reasoning_level)
        .map(ReasoningLevel::as_token)
        .unwrap_or("(none)");

    format!(
        "## OWNER_CONTEXT\n- sender_id: {}\n- sender_is_owner: {}\n- owner.machine_location: {}\n- owner.machine_timezone: {}\n- owner.default_backend: {}\n- owner.default_model: {}\n- owner.default_reasoning_level: {}",
        input.sender_id.trim(),
        input.sender_is_owner,
        machine_location,
        machine_timezone,
        default_backend,
        default_model,
        default_reasoning_level,
    )
}

fn render_runtime_notes_section() -> String {
    "## RUNTIME_NOTES\n- Crab runs autonomously; approval routing is handled outside this prompt.\n- Keep behavior deterministic and avoid contradictory state updates."
        .to_string()
}

fn render_messaging_semantics_section() -> String {
    "## MESSAGING_SEMANTICS\n- Normal assistant text is streamed by Crab to Discord as the channel reply.\n- Use Discord messaging tools only for explicit Discord actions (send/edit/delete/react/moderation/proactive operations).\n- Do not emit duplicate plain-text confirmations for actions already completed via Discord tools."
        .to_string()
}

fn backend_token(backend: BackendKind) -> &'static str {
    match backend {
        BackendKind::Claude => "claude",
        BackendKind::Codex => "codex",
        BackendKind::OpenCode => "opencode",
    }
}

#[cfg(test)]
mod tests {
    use crate::{BackendKind, CrabError, OwnerProfileMetadata, ReasoningLevel};

    use super::{compile_prompt_contract, PromptContractInput};

    fn input_for(
        backend: BackendKind,
        model: &str,
        reasoning_level: ReasoningLevel,
    ) -> PromptContractInput {
        PromptContractInput {
            backend,
            model: model.to_string(),
            reasoning_level,
            sender_id: "1234567890".to_string(),
            sender_is_owner: false,
            owner_profile: None,
            memory_tools_enabled: true,
        }
    }

    fn expected_for(backend: &str, model: &str, reasoning: &str) -> String {
        format!(
            "## RUNTIME_PROFILE\n- backend: {backend}\n- model: {model}\n- reasoning_level: {reasoning}\n\n## MEMORY_SEARCH_FIRST\n- Before claiming information is unknown, run `memory_search` for relevant prior context.\n- When citing stored context, call `memory_get` for exact line ranges before quoting.\n\n## OWNER_CONTEXT\n- sender_id: 1234567890\n- sender_is_owner: false\n- owner.machine_location: (none)\n- owner.machine_timezone: (none)\n- owner.default_backend: (none)\n- owner.default_model: (none)\n- owner.default_reasoning_level: (none)\n\n## RUNTIME_NOTES\n- Crab runs autonomously; approval routing is handled outside this prompt.\n- Keep behavior deterministic and avoid contradictory state updates.\n\n## MESSAGING_SEMANTICS\n- Normal assistant text is streamed by Crab to Discord as the channel reply.\n- Use Discord messaging tools only for explicit Discord actions (send/edit/delete/react/moderation/proactive operations).\n- Do not emit duplicate plain-text confirmations for actions already completed via Discord tools."
        )
    }

    #[test]
    fn prompt_snapshot_matrix_covers_backend_model_reasoning_combinations() {
        let cases = [
            (
                input_for(BackendKind::Claude, "claude-sonnet-4", ReasoningLevel::High),
                expected_for("claude", "claude-sonnet-4", "high"),
            ),
            (
                input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::Medium),
                expected_for("codex", "gpt-5-codex", "medium"),
            ),
            (
                input_for(BackendKind::OpenCode, "qwen2.5-coder", ReasoningLevel::Low),
                expected_for("opencode", "qwen2.5-coder", "low"),
            ),
        ];

        for (input, expected) in cases {
            let rendered = compile_prompt_contract(&input).expect("prompt contract should compile");
            assert_eq!(rendered, expected);
        }
    }

    #[test]
    fn owner_profile_section_includes_owner_metadata_when_present() {
        let mut input = input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::High);
        input.sender_is_owner = true;
        let mut owner_profile = OwnerProfileMetadata {
            machine_location: None,
            machine_timezone: None,
            default_backend: None,
            default_model: None,
            default_reasoning_level: None,
        };
        owner_profile.machine_location = Some("Paris, France".to_string());
        owner_profile.machine_timezone = Some("Europe/Paris".to_string());
        owner_profile.default_backend = Some(BackendKind::Codex);
        owner_profile.default_model = Some("gpt-5-codex".to_string());
        owner_profile.default_reasoning_level = Some(ReasoningLevel::High);
        input.owner_profile = Some(owner_profile);

        let rendered = compile_prompt_contract(&input).expect("prompt contract should compile");
        assert!(rendered.contains("- sender_is_owner: true"));
        assert!(rendered.contains("- owner.machine_location: Paris, France"));
        assert!(rendered.contains("- owner.machine_timezone: Europe/Paris"));
        assert!(rendered.contains("- owner.default_backend: codex"));
        assert!(rendered.contains("- owner.default_model: gpt-5-codex"));
        assert!(rendered.contains("- owner.default_reasoning_level: high"));
    }

    #[test]
    fn memory_tools_disabled_mode_changes_memory_search_first_section() {
        let mut input = input_for(
            BackendKind::Claude,
            "claude-sonnet-4",
            ReasoningLevel::Medium,
        );
        input.memory_tools_enabled = false;

        let rendered = compile_prompt_contract(&input).expect("prompt contract should compile");
        assert!(rendered.contains(
            "Memory tools are disabled for this run; explicitly note lookup limitations when relevant."
        ));
        assert!(!rendered.contains(
            "When citing stored context, call `memory_get` for exact line ranges before quoting."
        ));
    }

    #[test]
    fn compiler_rejects_blank_required_inputs() {
        let mut blank_model = input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::Medium);
        blank_model.model = " ".to_string();
        let model_error =
            compile_prompt_contract(&blank_model).expect_err("blank model should fail");
        assert_eq!(
            model_error,
            CrabError::InvariantViolation {
                context: "prompt_contract_compile",
                message: "model must not be empty".to_string(),
            }
        );

        let mut blank_sender = input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::Medium);
        blank_sender.sender_id = " ".to_string();
        let sender_error =
            compile_prompt_contract(&blank_sender).expect_err("blank sender id should fail");
        assert_eq!(
            sender_error,
            CrabError::InvariantViolation {
                context: "prompt_contract_compile",
                message: "sender_id must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn compiler_rejects_blank_owner_profile_fields() {
        let mut blank_location =
            input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::Medium);
        blank_location.owner_profile = Some(OwnerProfileMetadata {
            machine_location: Some(" ".to_string()),
            machine_timezone: None,
            default_backend: None,
            default_model: None,
            default_reasoning_level: None,
        });
        let location_error = compile_prompt_contract(&blank_location)
            .expect_err("blank owner machine location should fail");
        assert_eq!(
            location_error,
            CrabError::InvariantViolation {
                context: "prompt_contract_compile",
                message: "owner_profile.machine_location must not be empty".to_string(),
            }
        );

        let mut blank_timezone =
            input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::Medium);
        blank_timezone.owner_profile = Some(OwnerProfileMetadata {
            machine_location: None,
            machine_timezone: Some(" ".to_string()),
            default_backend: None,
            default_model: None,
            default_reasoning_level: None,
        });
        let timezone_error = compile_prompt_contract(&blank_timezone)
            .expect_err("blank owner machine timezone should fail");
        assert_eq!(
            timezone_error,
            CrabError::InvariantViolation {
                context: "prompt_contract_compile",
                message: "owner_profile.machine_timezone must not be empty".to_string(),
            }
        );

        let mut blank_model = input_for(BackendKind::Codex, "gpt-5-codex", ReasoningLevel::Medium);
        blank_model.owner_profile = Some(OwnerProfileMetadata {
            machine_location: None,
            machine_timezone: None,
            default_backend: None,
            default_model: Some(" ".to_string()),
            default_reasoning_level: None,
        });
        let model_error = compile_prompt_contract(&blank_model)
            .expect_err("blank owner default model should fail");
        assert_eq!(
            model_error,
            CrabError::InvariantViolation {
                context: "prompt_contract_compile",
                message: "owner_profile.default_model must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn owner_profile_optional_fields_render_none_tokens_when_missing() {
        let mut input = input_for(BackendKind::OpenCode, "qwen2.5-coder", ReasoningLevel::Low);
        input.sender_is_owner = true;
        input.owner_profile = Some(OwnerProfileMetadata {
            machine_location: None,
            machine_timezone: None,
            default_backend: None,
            default_model: None,
            default_reasoning_level: None,
        });

        let rendered = compile_prompt_contract(&input).expect("prompt contract should compile");
        assert!(rendered.contains("- owner.machine_location: (none)"));
        assert!(rendered.contains("- owner.machine_timezone: (none)"));
        assert!(rendered.contains("- owner.default_backend: (none)"));
        assert!(rendered.contains("- owner.default_model: (none)"));
        assert!(rendered.contains("- owner.default_reasoning_level: (none)"));
    }
}
