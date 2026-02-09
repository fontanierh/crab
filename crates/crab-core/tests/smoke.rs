use crab_core::{CrabError, CrabResult, RuntimeConfig};
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
