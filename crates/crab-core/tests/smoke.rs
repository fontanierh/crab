use crab_core::{
    apply_fallback_policy, BackendCompatibilityCatalog, BackendCompatibilityRules, BackendKind,
    CrabError, CrabResult, FallbackPolicyMode, InferenceProfile, ReasoningLevel, RuntimeConfig,
};
use std::collections::HashMap;

#[test]
fn public_api_is_stable() {
    let mut values = HashMap::new();
    values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());

    let parsed: CrabResult<RuntimeConfig> = RuntimeConfig::from_map(&values);
    assert!(parsed.is_ok());

    let display = CrabError::MissingConfig {
        key: "CRAB_DISCORD_TOKEN",
    }
    .to_string();
    assert_eq!(display, "missing required config: CRAB_DISCORD_TOKEN");
}

#[test]
fn fallback_policy_api_is_stable() {
    let profile = InferenceProfile {
        backend: BackendKind::Claude,
        model: "legacy-model".to_string(),
        reasoning_level: ReasoningLevel::XHigh,
    };

    let catalog = BackendCompatibilityCatalog {
        claude: BackendCompatibilityRules {
            supported_models: vec!["claude-sonnet".to_string()],
            supported_reasoning_levels: vec![ReasoningLevel::High],
        },
    };

    let decision = apply_fallback_policy(&profile, &catalog, FallbackPolicyMode::Compatible)
        .expect("fallback policy should produce a decision");
    assert!(decision.is_accepted());
    assert!(decision.fallback_applied);
    assert_eq!(decision.effective_profile.model, "claude-sonnet");
    assert_eq!(
        decision.effective_profile.reasoning_level,
        ReasoningLevel::High
    );
}
