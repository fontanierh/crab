use crab_core::{InferenceProfile, ReasoningLevel, AUTO_MODEL_ALIAS};

use crate::{CodexTurnConfig, OpenCodeSessionConfig, OpenCodeTurnConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaudeThinkingMode {
    Off,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeInferenceConfig {
    pub model: Option<String>,
    pub thinking_mode: ClaudeThinkingMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenCodeReasoningMode {
    Native,
    BestEffort,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenCodeInferenceMapping {
    pub session_config: OpenCodeSessionConfig,
    pub turn_config: OpenCodeTurnConfig,
    pub guidance_note: Option<String>,
}

pub fn map_claude_inference_profile(profile: &InferenceProfile) -> ClaudeInferenceConfig {
    let thinking_mode = match profile.reasoning_level {
        ReasoningLevel::None => ClaudeThinkingMode::Off,
        ReasoningLevel::Minimal | ReasoningLevel::Low => ClaudeThinkingMode::Low,
        ReasoningLevel::Medium => ClaudeThinkingMode::Medium,
        ReasoningLevel::High | ReasoningLevel::XHigh => ClaudeThinkingMode::High,
    };

    ClaudeInferenceConfig {
        model: model_override(profile),
        thinking_mode,
    }
}

pub fn map_codex_turn_config(profile: &InferenceProfile) -> CodexTurnConfig {
    CodexTurnConfig {
        model: model_override(profile),
        effort: Some(reasoning_level_token(profile.reasoning_level).to_string()),
    }
}

pub fn map_opencode_inference_profile(
    profile: &InferenceProfile,
    reasoning_mode: OpenCodeReasoningMode,
) -> OpenCodeInferenceMapping {
    let model = model_override(profile);
    let reasoning_token = reasoning_level_token(profile.reasoning_level);

    let (reasoning_level, guidance_note) = match reasoning_mode {
        OpenCodeReasoningMode::Native => (Some(reasoning_token.to_string()), None),
        OpenCodeReasoningMode::BestEffort => (
            None,
            Some(format!(
                "Prefer {} reasoning level for this turn.",
                reasoning_token
            )),
        ),
    };

    OpenCodeInferenceMapping {
        session_config: OpenCodeSessionConfig {
            model: model.clone(),
            reasoning_level: reasoning_level.clone(),
        },
        turn_config: OpenCodeTurnConfig {
            model,
            reasoning_level,
        },
        guidance_note,
    }
}

fn model_override(profile: &InferenceProfile) -> Option<String> {
    if profile.model == AUTO_MODEL_ALIAS {
        return None;
    }
    Some(profile.model.clone())
}

fn reasoning_level_token(level: ReasoningLevel) -> &'static str {
    match level {
        ReasoningLevel::None => "none",
        ReasoningLevel::Minimal => "minimal",
        ReasoningLevel::Low => "low",
        ReasoningLevel::Medium => "medium",
        ReasoningLevel::High => "high",
        ReasoningLevel::XHigh => "xhigh",
    }
}

#[cfg(test)]
mod tests {
    use crab_core::{BackendKind, InferenceProfile, ReasoningLevel, AUTO_MODEL_ALIAS};

    use super::{
        map_claude_inference_profile, map_codex_turn_config, map_opencode_inference_profile,
        ClaudeThinkingMode, OpenCodeReasoningMode,
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

    #[test]
    fn codex_mapping_uses_direct_effort_token_for_all_reasoning_levels() {
        let cases = [
            (ReasoningLevel::None, "none"),
            (ReasoningLevel::Minimal, "minimal"),
            (ReasoningLevel::Low, "low"),
            (ReasoningLevel::Medium, "medium"),
            (ReasoningLevel::High, "high"),
            (ReasoningLevel::XHigh, "xhigh"),
        ];
        for (reasoning_level, expected_effort) in cases {
            let input = profile(BackendKind::Codex, "gpt-5-codex", reasoning_level);
            let mapped = map_codex_turn_config(&input);
            assert_eq!(mapped.model, Some("gpt-5-codex".to_string()));
            assert_eq!(mapped.effort, Some(expected_effort.to_string()));
        }
    }

    #[test]
    fn codex_mapping_omits_auto_model_override() {
        let input = profile(BackendKind::Codex, AUTO_MODEL_ALIAS, ReasoningLevel::High);
        let mapped = map_codex_turn_config(&input);
        assert_eq!(mapped.model, None);
        assert_eq!(mapped.effort, Some("high".to_string()));
    }

    #[test]
    fn claude_mapping_clamps_reasoning_bands() {
        let cases = [
            (ReasoningLevel::None, ClaudeThinkingMode::Off),
            (ReasoningLevel::Minimal, ClaudeThinkingMode::Low),
            (ReasoningLevel::Low, ClaudeThinkingMode::Low),
            (ReasoningLevel::Medium, ClaudeThinkingMode::Medium),
            (ReasoningLevel::High, ClaudeThinkingMode::High),
            (ReasoningLevel::XHigh, ClaudeThinkingMode::High),
        ];
        for (reasoning_level, expected_mode) in cases {
            let input = profile(BackendKind::Claude, "claude-sonnet", reasoning_level);
            let mapped = map_claude_inference_profile(&input);
            assert_eq!(mapped.thinking_mode, expected_mode);
            assert_eq!(mapped.model, Some("claude-sonnet".to_string()));
        }
    }

    #[test]
    fn opencode_native_mapping_sets_reasoning_level_directly() {
        let input = profile(BackendKind::OpenCode, "o4-mini", ReasoningLevel::Medium);
        let mapped = map_opencode_inference_profile(&input, OpenCodeReasoningMode::Native);
        assert_eq!(mapped.session_config.model, Some("o4-mini".to_string()));
        assert_eq!(
            mapped.session_config.reasoning_level,
            Some("medium".to_string())
        );
        assert_eq!(mapped.turn_config.model, Some("o4-mini".to_string()));
        assert_eq!(
            mapped.turn_config.reasoning_level,
            Some("medium".to_string())
        );
        assert_eq!(mapped.guidance_note, None);
    }

    #[test]
    fn opencode_best_effort_mapping_emits_guidance_note() {
        let input = profile(
            BackendKind::OpenCode,
            AUTO_MODEL_ALIAS,
            ReasoningLevel::XHigh,
        );
        let mapped = map_opencode_inference_profile(&input, OpenCodeReasoningMode::BestEffort);
        assert_eq!(mapped.session_config.model, None);
        assert_eq!(mapped.session_config.reasoning_level, None);
        assert_eq!(mapped.turn_config.model, None);
        assert_eq!(mapped.turn_config.reasoning_level, None);
        assert_eq!(
            mapped.guidance_note,
            Some("Prefer xhigh reasoning level for this turn.".to_string())
        );
    }
}
