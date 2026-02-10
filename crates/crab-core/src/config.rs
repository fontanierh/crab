use std::collections::{HashMap, HashSet};

use crate::domain::{BackendKind, ReasoningLevel};
use crate::error::{CrabError, CrabResult};

pub const DEFAULT_WORKSPACE_ROOT: &str = "~/.crab/workspace";
pub const DEFAULT_MAX_CONCURRENT_LANES: usize = 4;
const OWNER_DISCORD_USER_IDS_KEY: &str = "CRAB_OWNER_DISCORD_USER_IDS";
const OWNER_ALIASES_KEY: &str = "CRAB_OWNER_ALIASES";
const OWNER_DEFAULT_BACKEND_KEY: &str = "CRAB_OWNER_DEFAULT_BACKEND";
const OWNER_DEFAULT_MODEL_KEY: &str = "CRAB_OWNER_DEFAULT_MODEL";
const OWNER_DEFAULT_REASONING_LEVEL_KEY: &str = "CRAB_OWNER_DEFAULT_REASONING_LEVEL";
const OWNER_MACHINE_LOCATION_KEY: &str = "CRAB_OWNER_MACHINE_LOCATION";
const OWNER_MACHINE_TIMEZONE_KEY: &str = "CRAB_OWNER_MACHINE_TIMEZONE";

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnerProfileDefaults {
    pub backend: Option<BackendKind>,
    pub model: Option<String>,
    pub reasoning_level: Option<ReasoningLevel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct OwnerConfig {
    pub discord_user_ids: Vec<String>,
    pub aliases: Vec<String>,
    pub profile_defaults: OwnerProfileDefaults,
    pub machine_location: Option<String>,
    pub machine_timezone: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub discord_token: String,
    pub workspace_root: String,
    pub max_concurrent_lanes: usize,
    pub owner: OwnerConfig,
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

        let owner = parse_owner_config(values)?;

        Ok(Self {
            discord_token,
            workspace_root,
            max_concurrent_lanes,
            owner,
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

fn parse_owner_config(values: &HashMap<String, String>) -> CrabResult<OwnerConfig> {
    let raw_owner_ids = values.get(OWNER_DISCORD_USER_IDS_KEY).map(String::as_str);
    let raw_aliases = values.get(OWNER_ALIASES_KEY).map(String::as_str);
    let raw_backend = values.get(OWNER_DEFAULT_BACKEND_KEY).map(String::as_str);
    let raw_model = values.get(OWNER_DEFAULT_MODEL_KEY).map(String::as_str);
    let raw_reasoning = values
        .get(OWNER_DEFAULT_REASONING_LEVEL_KEY)
        .map(String::as_str);
    let raw_machine_location = values.get(OWNER_MACHINE_LOCATION_KEY).map(String::as_str);
    let raw_machine_timezone = values.get(OWNER_MACHINE_TIMEZONE_KEY).map(String::as_str);

    let discord_user_ids = parse_owner_discord_user_ids(raw_owner_ids)?;
    let aliases = parse_owner_aliases(raw_aliases)?;

    let profile_defaults = OwnerProfileDefaults {
        backend: parse_owner_default_backend(raw_backend)?,
        model: parse_non_empty_optional_string(OWNER_DEFAULT_MODEL_KEY, raw_model)?,
        reasoning_level: parse_owner_default_reasoning_level(raw_reasoning)?,
    };

    Ok(OwnerConfig {
        discord_user_ids,
        aliases,
        profile_defaults,
        machine_location: parse_non_empty_optional_string(
            OWNER_MACHINE_LOCATION_KEY,
            raw_machine_location,
        )?,
        machine_timezone: parse_owner_machine_timezone(raw_machine_timezone)?,
    })
}

fn parse_owner_discord_user_ids(raw_value: Option<&str>) -> CrabResult<Vec<String>> {
    let owner_ids = parse_csv_list(OWNER_DISCORD_USER_IDS_KEY, raw_value)?;
    let mut seen = HashSet::new();
    for owner_id in &owner_ids {
        if !is_discord_user_id(owner_id) {
            return Err(CrabError::InvalidConfig {
                key: OWNER_DISCORD_USER_IDS_KEY,
                value: owner_ids.join(","),
                reason: "must contain only Discord snowflake ids (digits comma-separated)",
            });
        }
        if !seen.insert(owner_id.clone()) {
            return Err(CrabError::InvalidConfig {
                key: OWNER_DISCORD_USER_IDS_KEY,
                value: owner_ids.join(","),
                reason: "must not contain duplicate Discord user ids",
            });
        }
    }
    Ok(owner_ids)
}

fn parse_owner_aliases(raw_value: Option<&str>) -> CrabResult<Vec<String>> {
    let aliases = parse_csv_list(OWNER_ALIASES_KEY, raw_value)?;
    let mut seen = HashSet::new();
    for alias in &aliases {
        if !seen.insert(alias.to_ascii_lowercase()) {
            return Err(CrabError::InvalidConfig {
                key: OWNER_ALIASES_KEY,
                value: aliases.join(","),
                reason: "must not contain duplicate aliases",
            });
        }
    }
    Ok(aliases)
}

fn parse_owner_default_backend(raw_value: Option<&str>) -> CrabResult<Option<BackendKind>> {
    let Some(raw_backend) = raw_value else {
        return Ok(None);
    };
    let normalized = raw_backend.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(CrabError::InvalidConfig {
            key: OWNER_DEFAULT_BACKEND_KEY,
            value: raw_backend.to_string(),
            reason: "must not be empty",
        });
    }

    let backend = match normalized.as_str() {
        "claude" => BackendKind::Claude,
        "codex" => BackendKind::Codex,
        "opencode" => BackendKind::OpenCode,
        _ => {
            return Err(CrabError::InvalidConfig {
                key: OWNER_DEFAULT_BACKEND_KEY,
                value: raw_backend.to_string(),
                reason: "must be one of: claude, codex, opencode",
            });
        }
    };

    Ok(Some(backend))
}

fn parse_owner_default_reasoning_level(
    raw_value: Option<&str>,
) -> CrabResult<Option<ReasoningLevel>> {
    let Some(raw_reasoning_level) = raw_value else {
        return Ok(None);
    };
    let normalized = raw_reasoning_level.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(CrabError::InvalidConfig {
            key: OWNER_DEFAULT_REASONING_LEVEL_KEY,
            value: raw_reasoning_level.to_string(),
            reason: "must not be empty",
        });
    }

    let parsed = ReasoningLevel::parse_token(&normalized).ok_or(CrabError::InvalidConfig {
        key: OWNER_DEFAULT_REASONING_LEVEL_KEY,
        value: raw_reasoning_level.to_string(),
        reason: "must be one of: none, minimal, low, medium, high, xhigh",
    })?;

    Ok(Some(parsed))
}

fn parse_non_empty_optional_string(
    key: &'static str,
    raw_value: Option<&str>,
) -> CrabResult<Option<String>> {
    let Some(raw_value) = raw_value else {
        return Ok(None);
    };
    let normalized = raw_value.trim();
    if normalized.is_empty() {
        return Err(CrabError::InvalidConfig {
            key,
            value: raw_value.to_string(),
            reason: "must not be empty",
        });
    }
    Ok(Some(normalized.to_string()))
}

fn parse_owner_machine_timezone(raw_value: Option<&str>) -> CrabResult<Option<String>> {
    let Some(raw_timezone) = raw_value else {
        return Ok(None);
    };
    let normalized = raw_timezone.trim();
    if normalized.is_empty() {
        return Err(CrabError::InvalidConfig {
            key: OWNER_MACHINE_TIMEZONE_KEY,
            value: raw_timezone.to_string(),
            reason: "must not be empty",
        });
    }
    if !is_valid_timezone(normalized) {
        return Err(CrabError::InvalidConfig {
            key: OWNER_MACHINE_TIMEZONE_KEY,
            value: raw_timezone.to_string(),
            reason: "must be a valid timezone token (for example America/Los_Angeles or UTC)",
        });
    }
    Ok(Some(normalized.to_string()))
}

fn parse_csv_list(key: &'static str, raw_value: Option<&str>) -> CrabResult<Vec<String>> {
    let Some(raw_list) = raw_value else {
        return Ok(Vec::new());
    };

    if raw_list.trim().is_empty() {
        return Err(CrabError::InvalidConfig {
            key,
            value: raw_list.to_string(),
            reason: "must not be empty",
        });
    }

    let mut values = Vec::new();
    for raw_entry in raw_list.split(',') {
        let entry = raw_entry.trim();
        if entry.is_empty() {
            return Err(CrabError::InvalidConfig {
                key,
                value: raw_list.to_string(),
                reason: "must not contain empty comma-separated values",
            });
        }
        values.push(entry.to_string());
    }

    Ok(values)
}

fn is_discord_user_id(value: &str) -> bool {
    !value.is_empty() && value.chars().all(|character| character.is_ascii_digit())
}

fn is_valid_timezone(value: &str) -> bool {
    if value == "UTC" {
        return true;
    }
    if !value.contains('/') || value.starts_with('/') || value.ends_with('/') {
        return false;
    }

    let mut previous_was_slash = false;
    for character in value.chars() {
        if !(character.is_ascii_alphanumeric()
            || character == '/'
            || character == '_'
            || character == '-'
            || character == '+')
        {
            return false;
        }
        if character == '/' {
            if previous_was_slash {
                return false;
            }
            previous_was_slash = true;
        } else {
            previous_was_slash = false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{BackendKind, ReasoningLevel};

    use super::{
        is_discord_user_id, OwnerConfig, OwnerProfileDefaults, RuntimeConfig,
        DEFAULT_MAX_CONCURRENT_LANES, DEFAULT_WORKSPACE_ROOT,
    };
    use crate::error::CrabError;

    fn vars(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    fn parse(entries: &[(&str, &str)]) -> Result<RuntimeConfig, CrabError> {
        RuntimeConfig::from_map(&vars(entries))
    }

    fn parse_with_token(entries: &[(&str, &str)]) -> Result<RuntimeConfig, CrabError> {
        let mut values = vars(entries);
        values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());
        RuntimeConfig::from_map(&values)
    }

    #[test]
    fn parses_explicit_values() {
        let input = vars(&[
            ("CRAB_DISCORD_TOKEN", "test-token"),
            ("CRAB_WORKSPACE_ROOT", "/tmp/crab"),
            ("CRAB_MAX_CONCURRENT_LANES", "8"),
            ("CRAB_OWNER_DISCORD_USER_IDS", "12345,67890"),
            ("CRAB_OWNER_ALIASES", "Henry,Ops"),
            ("CRAB_OWNER_DEFAULT_BACKEND", "codex"),
            ("CRAB_OWNER_DEFAULT_MODEL", "gpt-5-codex"),
            ("CRAB_OWNER_DEFAULT_REASONING_LEVEL", "high"),
            ("CRAB_OWNER_MACHINE_LOCATION", "Paris, France"),
            ("CRAB_OWNER_MACHINE_TIMEZONE", "Europe/Paris"),
        ]);

        let parsed = RuntimeConfig::from_map(&input).expect("config should parse");
        assert_eq!(parsed.discord_token, "test-token");
        assert_eq!(parsed.workspace_root, "/tmp/crab");
        assert_eq!(parsed.max_concurrent_lanes, 8);
        assert_eq!(
            parsed.owner,
            OwnerConfig {
                discord_user_ids: vec!["12345".to_string(), "67890".to_string()],
                aliases: vec!["Henry".to_string(), "Ops".to_string()],
                profile_defaults: OwnerProfileDefaults {
                    backend: Some(BackendKind::Codex),
                    model: Some("gpt-5-codex".to_string()),
                    reasoning_level: Some(ReasoningLevel::High),
                },
                machine_location: Some("Paris, France".to_string()),
                machine_timezone: Some("Europe/Paris".to_string()),
            }
        );
    }

    #[test]
    fn applies_defaults_for_optional_settings() {
        let input = vars(&[("CRAB_DISCORD_TOKEN", "token")]);
        let parsed = RuntimeConfig::from_map(&input).expect("config should parse");
        assert_eq!(parsed.workspace_root, DEFAULT_WORKSPACE_ROOT);
        assert_eq!(parsed.max_concurrent_lanes, DEFAULT_MAX_CONCURRENT_LANES);
        assert_eq!(parsed.owner, OwnerConfig::default());
    }

    #[test]
    fn requires_discord_token() {
        let err = parse(&[]).expect_err("token should be required");
        assert_eq!(
            err,
            CrabError::MissingConfig {
                key: "CRAB_DISCORD_TOKEN"
            }
        );
    }

    #[test]
    fn rejects_non_numeric_lane_count() {
        let err = parse_with_token(&[("CRAB_MAX_CONCURRENT_LANES", "many")])
            .expect_err("invalid lane count should fail");
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
        let err = parse_with_token(&[("CRAB_MAX_CONCURRENT_LANES", "0")])
            .expect_err("zero lane count should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_MAX_CONCURRENT_LANES",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn rejects_owner_discord_ids_when_blank() {
        let err = parse_with_token(&[("CRAB_OWNER_DISCORD_USER_IDS", "   ")])
            .expect_err("blank owner ids should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DISCORD_USER_IDS",
                value: "   ".to_string(),
                reason: "must not be empty",
            }
        );
    }

    #[test]
    fn rejects_owner_discord_ids_with_non_digit_characters() {
        let err = parse_with_token(&[("CRAB_OWNER_DISCORD_USER_IDS", "1234,abc")])
            .expect_err("owner ids must be numeric");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DISCORD_USER_IDS",
                value: "1234,abc".to_string(),
                reason: "must contain only Discord snowflake ids (digits comma-separated)",
            }
        );
    }

    #[test]
    fn discord_user_id_validator_rejects_empty_and_accepts_digits() {
        assert!(!is_discord_user_id(""));
        assert!(is_discord_user_id("1234567890"));
    }

    #[test]
    fn rejects_duplicate_owner_discord_ids() {
        let err = parse_with_token(&[("CRAB_OWNER_DISCORD_USER_IDS", "1234,1234")])
            .expect_err("duplicate owner ids should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DISCORD_USER_IDS",
                value: "1234,1234".to_string(),
                reason: "must not contain duplicate Discord user ids",
            }
        );
    }

    #[test]
    fn rejects_owner_aliases_with_empty_entries() {
        let err = parse_with_token(&[("CRAB_OWNER_ALIASES", "henry, ,ops")])
            .expect_err("owner aliases must not contain empty values");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_ALIASES",
                value: "henry, ,ops".to_string(),
                reason: "must not contain empty comma-separated values",
            }
        );
    }

    #[test]
    fn rejects_duplicate_owner_aliases() {
        let err = parse_with_token(&[("CRAB_OWNER_ALIASES", "henry,Henry")])
            .expect_err("duplicate owner aliases should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_ALIASES",
                value: "henry,Henry".to_string(),
                reason: "must not contain duplicate aliases",
            }
        );
    }

    #[test]
    fn rejects_invalid_owner_default_backend() {
        let err = parse_with_token(&[("CRAB_OWNER_DEFAULT_BACKEND", "pi")])
            .expect_err("unsupported owner backend should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DEFAULT_BACKEND",
                value: "pi".to_string(),
                reason: "must be one of: claude, codex, opencode",
            }
        );
    }

    #[test]
    fn parses_owner_default_backend_variants() {
        let claude = parse_with_token(&[("CRAB_OWNER_DEFAULT_BACKEND", "claude")])
            .expect("claude backend should parse");
        assert_eq!(
            claude.owner.profile_defaults.backend,
            Some(BackendKind::Claude)
        );

        let opencode = parse_with_token(&[("CRAB_OWNER_DEFAULT_BACKEND", "opencode")])
            .expect("opencode backend should parse");
        assert_eq!(
            opencode.owner.profile_defaults.backend,
            Some(BackendKind::OpenCode)
        );
    }

    #[test]
    fn rejects_blank_owner_default_backend() {
        let err = parse_with_token(&[("CRAB_OWNER_DEFAULT_BACKEND", "  ")])
            .expect_err("blank backend should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DEFAULT_BACKEND",
                value: "  ".to_string(),
                reason: "must not be empty",
            }
        );
    }

    #[test]
    fn rejects_invalid_owner_default_reasoning_level() {
        let err = parse_with_token(&[("CRAB_OWNER_DEFAULT_REASONING_LEVEL", "ultra")])
            .expect_err("unsupported reasoning level should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DEFAULT_REASONING_LEVEL",
                value: "ultra".to_string(),
                reason: "must be one of: none, minimal, low, medium, high, xhigh",
            }
        );
    }

    #[test]
    fn rejects_blank_owner_default_reasoning_level() {
        let err = parse_with_token(&[("CRAB_OWNER_DEFAULT_REASONING_LEVEL", " ")])
            .expect_err("blank reasoning should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DEFAULT_REASONING_LEVEL",
                value: " ".to_string(),
                reason: "must not be empty",
            }
        );
    }

    #[test]
    fn rejects_blank_owner_default_model() {
        let err = parse_with_token(&[("CRAB_OWNER_DEFAULT_MODEL", "  ")])
            .expect_err("blank model should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_DEFAULT_MODEL",
                value: "  ".to_string(),
                reason: "must not be empty",
            }
        );
    }

    #[test]
    fn rejects_blank_owner_machine_location() {
        let err = parse_with_token(&[("CRAB_OWNER_MACHINE_LOCATION", "   ")])
            .expect_err("blank machine location should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_MACHINE_LOCATION",
                value: "   ".to_string(),
                reason: "must not be empty",
            }
        );
    }

    #[test]
    fn rejects_invalid_owner_machine_timezone() {
        let err = parse_with_token(&[("CRAB_OWNER_MACHINE_TIMEZONE", "Europe//Paris")])
            .expect_err("invalid timezone should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_MACHINE_TIMEZONE",
                value: "Europe//Paris".to_string(),
                reason: "must be a valid timezone token (for example America/Los_Angeles or UTC)",
            }
        );
    }

    #[test]
    fn rejects_blank_owner_machine_timezone() {
        let err = parse_with_token(&[("CRAB_OWNER_MACHINE_TIMEZONE", "   ")])
            .expect_err("blank timezone should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_MACHINE_TIMEZONE",
                value: "   ".to_string(),
                reason: "must not be empty",
            }
        );
    }

    #[test]
    fn rejects_owner_machine_timezone_without_slash() {
        let err = parse_with_token(&[("CRAB_OWNER_MACHINE_TIMEZONE", "PST")])
            .expect_err("timezone without slash should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_MACHINE_TIMEZONE",
                value: "PST".to_string(),
                reason: "must be a valid timezone token (for example America/Los_Angeles or UTC)",
            }
        );
    }

    #[test]
    fn rejects_owner_machine_timezone_with_invalid_character() {
        let err = parse_with_token(&[("CRAB_OWNER_MACHINE_TIMEZONE", "Europe/Paris!")])
            .expect_err("timezone with invalid character should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_MACHINE_TIMEZONE",
                value: "Europe/Paris!".to_string(),
                reason: "must be a valid timezone token (for example America/Los_Angeles or UTC)",
            }
        );
    }

    #[test]
    fn accepts_utc_owner_machine_timezone() {
        let parsed = parse_with_token(&[("CRAB_OWNER_MACHINE_TIMEZONE", "UTC")])
            .expect("UTC should be accepted");
        assert_eq!(parsed.owner.machine_timezone, Some("UTC".to_string()));
    }

    #[test]
    fn accepts_owner_machine_timezone_with_allowed_special_characters() {
        let parsed = parse_with_token(&[("CRAB_OWNER_MACHINE_TIMEZONE", "Area/Sub_A-B+C")])
            .expect("timezone should allow underscore, hyphen, and plus");
        assert_eq!(
            parsed.owner.machine_timezone,
            Some("Area/Sub_A-B+C".to_string())
        );
    }
}
