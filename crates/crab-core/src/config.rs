use std::collections::{HashMap, HashSet};

use crate::domain::{BackendKind, ReasoningLevel};
use crate::error::{CrabError, CrabResult};

pub const DEFAULT_WORKSPACE_ROOT: &str = "~/.crab/workspace";
pub const DEFAULT_MAX_CONCURRENT_LANES: usize = 4;
pub const DEFAULT_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS: u64 = 90;
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 10;
pub const DEFAULT_RUN_STALL_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_BACKEND_STALL_TIMEOUT_SECS: u64 = 600;
pub const DEFAULT_DISPATCHER_STALL_TIMEOUT_SECS: u64 = 20;
pub const DEFAULT_SELF_WORK_IDLE_DELAY_MS: u64 = 180_000;
pub const MIN_SELF_WORK_IDLE_DELAY_MS: u64 = 120_000;
pub const MAX_SELF_WORK_IDLE_DELAY_MS: u64 = 300_000;

const STARTUP_RECONCILIATION_GRACE_PERIOD_SECS_KEY: &str =
    "CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS";
const HEARTBEAT_INTERVAL_SECS_KEY: &str = "CRAB_HEARTBEAT_INTERVAL_SECS";
const RUN_STALL_TIMEOUT_SECS_KEY: &str = "CRAB_RUN_STALL_TIMEOUT_SECS";
const BACKEND_STALL_TIMEOUT_SECS_KEY: &str = "CRAB_BACKEND_STALL_TIMEOUT_SECS";
const DISPATCHER_STALL_TIMEOUT_SECS_KEY: &str = "CRAB_DISPATCHER_STALL_TIMEOUT_SECS";
const SELF_WORK_IDLE_DELAY_MS_KEY: &str = "CRAB_SELF_WORK_IDLE_DELAY_MS";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StartupReconciliationConfig {
    pub grace_period_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeartbeatConfig {
    pub interval_secs: u64,
    pub run_stall_timeout_secs: u64,
    pub backend_stall_timeout_secs: u64,
    pub dispatcher_stall_timeout_secs: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelfWorkConfig {
    pub idle_delay_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub discord_token: String,
    pub workspace_root: String,
    pub max_concurrent_lanes: usize,
    pub startup_reconciliation: StartupReconciliationConfig,
    pub heartbeat: HeartbeatConfig,
    pub self_work: SelfWorkConfig,
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

        let startup_reconciliation = parse_startup_reconciliation_config(values)?;
        let heartbeat = parse_heartbeat_config(values)?;
        let self_work = parse_self_work_config(values)?;
        let owner = parse_owner_config(values)?;

        Ok(Self {
            discord_token,
            workspace_root,
            max_concurrent_lanes,
            startup_reconciliation,
            heartbeat,
            self_work,
            owner,
        })
    }
}

fn parse_positive_usize(key: &'static str, raw_value: &str) -> CrabResult<usize> {
    let parsed = parse_positive_u64(key, raw_value)?;
    #[cfg(target_pointer_width = "64")]
    {
        Ok(parsed as usize)
    }
    #[cfg(not(target_pointer_width = "64"))]
    {
        usize::try_from(parsed).map_err(|_| CrabError::InvalidConfig {
            key,
            value: raw_value.to_string(),
            reason: "must fit in usize",
        })
    }
}

fn parse_positive_u64(key: &'static str, raw_value: &str) -> CrabResult<u64> {
    let parsed = raw_value
        .parse::<u64>()
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

fn parse_startup_reconciliation_config(
    values: &HashMap<String, String>,
) -> CrabResult<StartupReconciliationConfig> {
    let grace_period_secs = match values.get(STARTUP_RECONCILIATION_GRACE_PERIOD_SECS_KEY) {
        Some(raw_value) => {
            parse_positive_u64(STARTUP_RECONCILIATION_GRACE_PERIOD_SECS_KEY, raw_value)?
        }
        None => DEFAULT_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS,
    };

    Ok(StartupReconciliationConfig { grace_period_secs })
}

fn parse_heartbeat_config(values: &HashMap<String, String>) -> CrabResult<HeartbeatConfig> {
    let interval_secs = match values.get(HEARTBEAT_INTERVAL_SECS_KEY) {
        Some(raw_value) => parse_positive_u64(HEARTBEAT_INTERVAL_SECS_KEY, raw_value)?,
        None => DEFAULT_HEARTBEAT_INTERVAL_SECS,
    };
    let run_stall_timeout_secs = match values.get(RUN_STALL_TIMEOUT_SECS_KEY) {
        Some(raw_value) => parse_positive_u64(RUN_STALL_TIMEOUT_SECS_KEY, raw_value)?,
        None => DEFAULT_RUN_STALL_TIMEOUT_SECS,
    };
    let backend_stall_timeout_secs = match values.get(BACKEND_STALL_TIMEOUT_SECS_KEY) {
        Some(raw_value) => parse_positive_u64(BACKEND_STALL_TIMEOUT_SECS_KEY, raw_value)?,
        None => DEFAULT_BACKEND_STALL_TIMEOUT_SECS,
    };
    let dispatcher_stall_timeout_secs = match values.get(DISPATCHER_STALL_TIMEOUT_SECS_KEY) {
        Some(raw_value) => parse_positive_u64(DISPATCHER_STALL_TIMEOUT_SECS_KEY, raw_value)?,
        None => DEFAULT_DISPATCHER_STALL_TIMEOUT_SECS,
    };

    Ok(HeartbeatConfig {
        interval_secs,
        run_stall_timeout_secs,
        backend_stall_timeout_secs,
        dispatcher_stall_timeout_secs,
    })
}

fn parse_self_work_config(values: &HashMap<String, String>) -> CrabResult<SelfWorkConfig> {
    let idle_delay_ms = match values.get(SELF_WORK_IDLE_DELAY_MS_KEY) {
        Some(raw_value) => parse_positive_u64(SELF_WORK_IDLE_DELAY_MS_KEY, raw_value)?,
        None => DEFAULT_SELF_WORK_IDLE_DELAY_MS,
    };

    if !(MIN_SELF_WORK_IDLE_DELAY_MS..=MAX_SELF_WORK_IDLE_DELAY_MS).contains(&idle_delay_ms) {
        return Err(CrabError::InvalidConfig {
            key: SELF_WORK_IDLE_DELAY_MS_KEY,
            value: idle_delay_ms.to_string(),
            reason: "must be between 120000 and 300000 inclusive",
        });
    }

    Ok(SelfWorkConfig { idle_delay_ms })
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
        _ => {
            return Err(CrabError::InvalidConfig {
                key: OWNER_DEFAULT_BACKEND_KEY,
                value: raw_backend.to_string(),
                reason: "must be one of: claude",
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{BackendKind, ReasoningLevel};

    use super::{
        is_discord_user_id, HeartbeatConfig, OwnerConfig, OwnerProfileDefaults, RuntimeConfig,
        SelfWorkConfig, StartupReconciliationConfig, DEFAULT_BACKEND_STALL_TIMEOUT_SECS,
        DEFAULT_DISPATCHER_STALL_TIMEOUT_SECS, DEFAULT_HEARTBEAT_INTERVAL_SECS,
        DEFAULT_MAX_CONCURRENT_LANES, DEFAULT_RUN_STALL_TIMEOUT_SECS,
        DEFAULT_SELF_WORK_IDLE_DELAY_MS, DEFAULT_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS,
        DEFAULT_WORKSPACE_ROOT,
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
            ("CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS", "120"),
            ("CRAB_HEARTBEAT_INTERVAL_SECS", "15"),
            ("CRAB_RUN_STALL_TIMEOUT_SECS", "91"),
            ("CRAB_BACKEND_STALL_TIMEOUT_SECS", "31"),
            ("CRAB_DISPATCHER_STALL_TIMEOUT_SECS", "21"),
            ("CRAB_SELF_WORK_IDLE_DELAY_MS", "240000"),
            ("CRAB_OWNER_DISCORD_USER_IDS", "12345,67890"),
            ("CRAB_OWNER_ALIASES", "Alice,Ops"),
            ("CRAB_OWNER_DEFAULT_BACKEND", "claude"),
            ("CRAB_OWNER_DEFAULT_MODEL", "claude-opus-4-5"),
            ("CRAB_OWNER_DEFAULT_REASONING_LEVEL", "high"),
            ("CRAB_OWNER_MACHINE_LOCATION", "Berlin, Germany"),
            ("CRAB_OWNER_MACHINE_TIMEZONE", "Europe/Paris"),
        ]);

        let parsed = RuntimeConfig::from_map(&input).expect("config should parse");
        assert_eq!(parsed.discord_token, "test-token");
        assert_eq!(parsed.workspace_root, "/tmp/crab");
        assert_eq!(parsed.max_concurrent_lanes, 8);
        assert_eq!(
            parsed.startup_reconciliation,
            StartupReconciliationConfig {
                grace_period_secs: 120,
            }
        );
        assert_eq!(
            parsed.heartbeat,
            HeartbeatConfig {
                interval_secs: 15,
                run_stall_timeout_secs: 91,
                backend_stall_timeout_secs: 31,
                dispatcher_stall_timeout_secs: 21,
            }
        );
        assert_eq!(
            parsed.self_work,
            SelfWorkConfig {
                idle_delay_ms: 240_000,
            }
        );
        assert_eq!(
            parsed.owner,
            OwnerConfig {
                discord_user_ids: vec!["12345".to_string(), "67890".to_string()],
                aliases: vec!["Alice".to_string(), "Ops".to_string()],
                profile_defaults: OwnerProfileDefaults {
                    backend: Some(BackendKind::Claude),
                    model: Some("claude-opus-4-5".to_string()),
                    reasoning_level: Some(ReasoningLevel::High),
                },
                machine_location: Some("Berlin, Germany".to_string()),
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
        assert_eq!(
            parsed.startup_reconciliation,
            StartupReconciliationConfig {
                grace_period_secs: DEFAULT_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS,
            }
        );
        assert_eq!(
            parsed.heartbeat,
            HeartbeatConfig {
                interval_secs: DEFAULT_HEARTBEAT_INTERVAL_SECS,
                run_stall_timeout_secs: DEFAULT_RUN_STALL_TIMEOUT_SECS,
                backend_stall_timeout_secs: DEFAULT_BACKEND_STALL_TIMEOUT_SECS,
                dispatcher_stall_timeout_secs: DEFAULT_DISPATCHER_STALL_TIMEOUT_SECS,
            }
        );
        assert_eq!(
            parsed.self_work,
            SelfWorkConfig {
                idle_delay_ms: DEFAULT_SELF_WORK_IDLE_DELAY_MS,
            }
        );
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
    fn rejects_self_work_idle_delay_below_minimum() {
        let error = parse_with_token(&[("CRAB_SELF_WORK_IDLE_DELAY_MS", "119999")])
            .expect_err("idle delay below minimum should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_SELF_WORK_IDLE_DELAY_MS",
                value: "119999".to_string(),
                reason: "must be between 120000 and 300000 inclusive",
            }
        );
    }

    #[test]
    fn rejects_self_work_idle_delay_above_maximum() {
        let error = parse_with_token(&[("CRAB_SELF_WORK_IDLE_DELAY_MS", "300001")])
            .expect_err("idle delay above maximum should fail");
        assert_eq!(
            error,
            CrabError::InvalidConfig {
                key: "CRAB_SELF_WORK_IDLE_DELAY_MS",
                value: "300001".to_string(),
                reason: "must be between 120000 and 300000 inclusive",
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
    fn rejects_non_numeric_startup_reconciliation_grace_period() {
        let err = parse_with_token(&[("CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS", "soon")])
            .expect_err("grace period must be numeric");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS",
                value: "soon".to_string(),
                reason: "must be a positive integer",
            }
        );
    }

    #[test]
    fn rejects_zero_startup_reconciliation_grace_period() {
        let err = parse_with_token(&[("CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS", "0")])
            .expect_err("grace period must be positive");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn rejects_zero_heartbeat_interval() {
        let err = parse_with_token(&[("CRAB_HEARTBEAT_INTERVAL_SECS", "0")])
            .expect_err("heartbeat interval must be positive");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_HEARTBEAT_INTERVAL_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn rejects_non_numeric_run_stall_timeout() {
        let err = parse_with_token(&[("CRAB_RUN_STALL_TIMEOUT_SECS", "never")])
            .expect_err("run stall timeout must be numeric");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_RUN_STALL_TIMEOUT_SECS",
                value: "never".to_string(),
                reason: "must be a positive integer",
            }
        );
    }

    #[test]
    fn rejects_zero_backend_stall_timeout() {
        let err = parse_with_token(&[("CRAB_BACKEND_STALL_TIMEOUT_SECS", "0")])
            .expect_err("backend stall timeout must be positive");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_BACKEND_STALL_TIMEOUT_SECS",
                value: "0".to_string(),
                reason: "must be greater than 0",
            }
        );
    }

    #[test]
    fn rejects_zero_dispatcher_stall_timeout() {
        let err = parse_with_token(&[("CRAB_DISPATCHER_STALL_TIMEOUT_SECS", "0")])
            .expect_err("dispatcher stall timeout must be positive");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_DISPATCHER_STALL_TIMEOUT_SECS",
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
        let err = parse_with_token(&[("CRAB_OWNER_ALIASES", "alice,Alice")])
            .expect_err("duplicate owner aliases should fail");
        assert_eq!(
            err,
            CrabError::InvalidConfig {
                key: "CRAB_OWNER_ALIASES",
                value: "alice,Alice".to_string(),
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
                reason: "must be one of: claude",
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
