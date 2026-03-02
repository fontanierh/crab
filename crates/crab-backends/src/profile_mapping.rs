use crab_core::{InferenceProfile, ReasoningLevel, AUTO_MODEL_ALIAS};

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

fn model_override(profile: &InferenceProfile) -> Option<String> {
    if profile.model == AUTO_MODEL_ALIAS {
        return None;
    }
    Some(profile.model.clone())
}

#[cfg(test)]
mod tests {
    use crab_core::{BackendKind, InferenceProfile, ReasoningLevel, AUTO_MODEL_ALIAS};

    use super::{map_claude_inference_profile, ClaudeThinkingMode};

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
    fn claude_mapping_omits_auto_model_override() {
        let input = profile(BackendKind::Claude, AUTO_MODEL_ALIAS, ReasoningLevel::High);
        let mapped = map_claude_inference_profile(&input);
        assert_eq!(mapped.model, None);
        assert_eq!(mapped.thinking_mode, ClaudeThinkingMode::High);
    }
}
