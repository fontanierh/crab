use std::collections::HashMap;

use crate::error::{CrabError, CrabResult};

pub const DEFAULT_WORKSPACE_ROOT: &str = "~/.crab/workspace";
pub const DEFAULT_MAX_CONCURRENT_LANES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub discord_token: String,
    pub workspace_root: String,
    pub max_concurrent_lanes: usize,
}

impl RuntimeConfig {
    pub fn from_map(values: &HashMap<String, String>) -> CrabResult<Self> {
        let discord_token =
            values
                .get("CRAB_DISCORD_TOKEN")
                .cloned()
                .ok_or(CrabError::MissingConfig {
                    key: "CRAB_DISCORD_TOKEN",
                })?;

        let workspace_root = values
            .get("CRAB_WORKSPACE_ROOT")
            .cloned()
            .unwrap_or_else(|| DEFAULT_WORKSPACE_ROOT.to_string());

        let max_concurrent_lanes = match values.get("CRAB_MAX_CONCURRENT_LANES") {
            Some(raw_value) => parse_positive_usize("CRAB_MAX_CONCURRENT_LANES", raw_value)?,
            None => DEFAULT_MAX_CONCURRENT_LANES,
        };

        Ok(Self {
            discord_token,
            workspace_root,
            max_concurrent_lanes,
        })
    }
}

fn parse_positive_usize(key: &'static str, raw_value: &str) -> CrabResult<usize> {
    let parsed = raw_value
        .parse::<usize>()
        .map_err(|_| CrabError::InvalidConfig {
            key,
            value: raw_value.to_string(),
            reason: "must be a positive integer",
        })?;

    if parsed == 0 {
        return Err(CrabError::InvalidConfig {
            key,
            value: raw_value.to_string(),
            reason: "must be greater than 0",
        });
    }

    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{RuntimeConfig, DEFAULT_MAX_CONCURRENT_LANES, DEFAULT_WORKSPACE_ROOT};
    use crate::error::CrabError;

    fn vars(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn parses_explicit_values() {
        let input = vars(&[
            ("CRAB_DISCORD_TOKEN", "test-token"),
            ("CRAB_WORKSPACE_ROOT", "/tmp/crab"),
            ("CRAB_MAX_CONCURRENT_LANES", "8"),
        ]);

        let parsed = RuntimeConfig::from_map(&input).expect("config should parse");
        assert_eq!(parsed.discord_token, "test-token");
        assert_eq!(parsed.workspace_root, "/tmp/crab");
        assert_eq!(parsed.max_concurrent_lanes, 8);
    }

    #[test]
    fn applies_defaults_for_optional_settings() {
        let input = vars(&[("CRAB_DISCORD_TOKEN", "token")]);
        let parsed = RuntimeConfig::from_map(&input).expect("config should parse");
        assert_eq!(parsed.workspace_root, DEFAULT_WORKSPACE_ROOT);
        assert_eq!(parsed.max_concurrent_lanes, DEFAULT_MAX_CONCURRENT_LANES);
    }

    #[test]
    fn requires_discord_token() {
        let input = vars(&[]);
        let err = RuntimeConfig::from_map(&input).expect_err("token should be required");
        assert_eq!(
            err,
            CrabError::MissingConfig {
                key: "CRAB_DISCORD_TOKEN"
            }
        );
    }

    #[test]
    fn rejects_non_numeric_lane_count() {
        let input = vars(&[
            ("CRAB_DISCORD_TOKEN", "token"),
            ("CRAB_MAX_CONCURRENT_LANES", "many"),
        ]);

        let err = RuntimeConfig::from_map(&input).expect_err("invalid lane count should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_MAX_CONCURRENT_LANES",
                value: "many".to_string(),
                reason: "must be a positive integer",
            }
        );
    }

    #[test]
    fn rejects_zero_lane_count() {
        let input = vars(&[
            ("CRAB_DISCORD_TOKEN", "token"),
            ("CRAB_MAX_CONCURRENT_LANES", "0"),
        ]);

        let err = RuntimeConfig::from_map(&input).expect_err("zero lane count should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_MAX_CONCURRENT_LANES",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }
}
