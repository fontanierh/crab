use std::collections::BTreeMap;
use std::env;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::{io_result, result_context, sha256_hex, FactoryError, FactoryResult};

pub(crate) const CLAUDE_MODEL: &str = "claude-fable-5";
pub(crate) const CODEX_MODEL: &str = "gpt-5.6-sol";
pub(crate) const REASONING_EFFORT: &str = "max";
pub(crate) const CODEX_PERMISSION_MODE: &str = "dangerously-bypass-approvals-and-sandbox";
pub(crate) const CLAUDE_PERMISSION_MODE: &str = "dangerously-skip-permissions";
pub(crate) const WORKER_HOST_PERMISSIONS: &str = "unrestricted";
pub(crate) const WORKER_SANDBOX: &str = "disabled";
pub(crate) const WORKER_NETWORK_ACCESS: bool = true;
pub(crate) const NESTED_AGENTS_ENABLED: bool = false;
pub(crate) const DEFAULT_TIMEOUT_SECONDS: u64 = 14_400;
pub(crate) const MIN_TIMEOUT_SECONDS: u64 = 60;
pub(crate) const MAX_TIMEOUT_SECONDS: u64 = 86_400;
pub(crate) const MAX_ADDITIONAL_ROUNDS: u32 = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LaunchOptions {
    pub(crate) prompt_file: PathBuf,
    pub(crate) repo: PathBuf,
    pub(crate) base_ref: String,
    pub(crate) run_id: Option<String>,
    pub(crate) additional_review_rounds: u32,
    pub(crate) artifact_root: Option<PathBuf>,
    pub(crate) worktree_root: Option<PathBuf>,
    pub(crate) agent_timeout_seconds: u64,
    pub(crate) allow_dirty_source: bool,
    pub(crate) launcher: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolPaths {
    pub(crate) git: PathBuf,
    pub(crate) claude: PathBuf,
    pub(crate) codex: PathBuf,
    pub(crate) make: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ToolVersions {
    pub(crate) git: String,
    pub(crate) claude: String,
    pub(crate) codex: String,
    #[serde(rename = "make")]
    pub(crate) make_tool: String,
}

#[derive(Debug, Clone)]
pub(crate) struct ResolvedRoots {
    pub(crate) artifact_root: PathBuf,
    pub(crate) worktree_root: PathBuf,
}

pub(crate) fn validate_counts(additional_rounds: u32, timeout_seconds: u64) -> FactoryResult<u32> {
    if additional_rounds > MAX_ADDITIONAL_ROUNDS {
        return Err(FactoryError::new(format!(
            "--additional-review-rounds must be between 0 and {MAX_ADDITIONAL_ROUNDS}"
        )));
    }
    if !(MIN_TIMEOUT_SECONDS..=MAX_TIMEOUT_SECONDS).contains(&timeout_seconds) {
        return Err(FactoryError::new(format!(
            "--agent-timeout-seconds must be between {MIN_TIMEOUT_SECONDS} and {MAX_TIMEOUT_SECONDS}"
        )));
    }
    checked_review_rounds(additional_rounds)
}

fn checked_review_rounds(additional_rounds: u32) -> FactoryResult<u32> {
    Ok(require_some!(
        additional_rounds.checked_add(1),
        FactoryError::new("review-round count overflow")
    ))
}

pub(crate) fn sanitize_run_id(value: &str) -> FactoryResult<String> {
    let mut normalized = String::with_capacity(value.len());
    let mut replacing = false;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || matches!(character, '_' | '-') {
            normalized.push(character.to_ascii_lowercase());
            replacing = false;
        } else if !replacing {
            normalized.push('-');
            replacing = true;
        }
    }
    let normalized = normalized.trim_matches('-').to_string();
    if normalized.is_empty() {
        return Err(FactoryError::new(
            "run ID must contain an ASCII letter, number, or underscore",
        ));
    }
    if normalized.len() > 64 {
        return Err(FactoryError::new("run ID must be 64 characters or fewer"));
    }
    Ok(normalized)
}

pub(crate) fn default_run_id(request: &[u8]) -> FactoryResult<String> {
    default_run_id_at(request, OffsetDateTime::now_utc())
}

fn default_run_id_at(request: &[u8], now: OffsetDateTime) -> FactoryResult<String> {
    default_run_id_with_description(request, now, "[year][month][day]-[hour][minute][second]")
}

fn default_run_id_with_description(
    request: &[u8],
    now: OffsetDateTime,
    description: &str,
) -> FactoryResult<String> {
    let format = result_context(
        time::format_description::parse(description),
        "could not build run ID format",
    )?;
    let stamp = result_context(now.format(&format), "could not format default run ID")?;
    Ok(format!("{stamp}-{}", &sha256_hex(request)[..8]))
}

pub(crate) fn proc_name_for(run_id: &str) -> String {
    const PREFIX: &str = "code-factory-";
    let candidate = format!("{PREFIX}{run_id}");
    if candidate.len() <= 64 {
        return candidate;
    }
    let digest = &sha256_hex(run_id.as_bytes())[..8];
    let available = 64 - PREFIX.len() - digest.len() - 1;
    format!("{PREFIX}{}-{digest}", &run_id[..available])
}

pub(crate) fn resolve_roots(
    artifact_root: Option<&Path>,
    worktree_root: Option<&Path>,
    cwd: &Path,
) -> FactoryResult<ResolvedRoots> {
    Ok(ResolvedRoots {
        artifact_root: resolve_root(artifact_root, cwd, ".crab/code-factory/runs")?,
        worktree_root: resolve_root(worktree_root, cwd, ".crab/code-factory/worktrees")?,
    })
}

fn resolve_root(
    explicit: Option<&Path>,
    cwd: &Path,
    default_suffix: &str,
) -> FactoryResult<PathBuf> {
    let path = match explicit {
        Some(path) => absolutize(path, cwd),
        None => PathBuf::from(require_some!(
            env::var_os("HOME"),
            FactoryError::new(
                "HOME is not set; pass --artifact-root and --worktree-root explicitly"
            )
        ))
        .join(default_suffix),
    };
    canonicalize_intended(&path)
}

pub(crate) fn canonicalize_intended(path: &Path) -> FactoryResult<PathBuf> {
    canonicalize_intended_with_current_dir(path, env::current_dir())
}

fn canonicalize_intended_with_current_dir(
    path: &Path,
    current_dir: std::io::Result<PathBuf>,
) -> FactoryResult<PathBuf> {
    let absolute = if path.is_absolute() {
        normalize_path(path)
    } else {
        let cwd = io_result(current_dir, "read current directory at", Path::new("."))?;
        normalize_path(&cwd.join(path))
    };
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        #[rustfmt::skip]
        let name = require_some!(ancestor.file_name(), FactoryError::new(format!("no existing ancestor for {}", absolute.display())));
        suffix.push(name.to_os_string());
        #[rustfmt::skip]
        let parent = require_some!(ancestor.parent(), FactoryError::new(format!("no existing ancestor for {}", absolute.display())));
        ancestor = parent;
    }
    let mut resolved = io_result(fs::canonicalize(ancestor), "canonicalize", ancestor)?;
    for component in suffix.iter().rev() {
        resolved.push(component);
    }
    Ok(resolved)
}

fn absolutize(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        normalize_path(path)
    } else {
        normalize_path(&cwd.join(path))
    }
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

pub(crate) fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

pub(crate) fn resolve_executable(name_or_path: &OsStr) -> FactoryResult<PathBuf> {
    let candidate = Path::new(name_or_path);
    let paths = if candidate.components().count() > 1 {
        vec![candidate.to_path_buf()]
    } else {
        let path = require_some!(env::var_os("PATH"), FactoryError::new("PATH is not set"));
        env::split_paths(&path)
            .map(|directory| directory.join(candidate))
            .collect()
    };
    for path in paths {
        let Ok(metadata) = fs::metadata(&path) else {
            continue;
        };
        if metadata.is_file() && metadata.permissions().mode() & 0o111 != 0 {
            let parent = path.parent().unwrap_or(Path::new("."));
            let name = path.file_name().unwrap_or(name_or_path);
            #[rustfmt::skip]
            let parent = io_result(fs::canonicalize(parent), "canonicalize executable parent", parent)?;
            return Ok(parent.join(name));
        }
    }
    Err(FactoryError::new(format!(
        "required executable is not installed or executable: {}",
        candidate.display()
    )))
}

pub(crate) fn allowlisted_environment() -> BTreeMap<OsString, OsString> {
    allowlisted_environment_from(env::vars_os())
}

fn allowlisted_environment_from<I>(values: I) -> BTreeMap<OsString, OsString>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    values
        .into_iter()
        .filter(|(name, _)| environment_name_allowed(name))
        .collect()
}

fn environment_name_allowed(name: &OsStr) -> bool {
    let Some(name) = name.to_str() else {
        return false;
    };
    if environment_name_disables_network(name) || environment_name_selects_sandbox(name) {
        return false;
    }
    const EXACT: &[&str] = &[
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "TMPDIR",
        "TERM",
        "LANG",
        "LC_ALL",
        "LC_CTYPE",
        "TZ",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "no_proxy",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
        "CARGO_HOME",
        "RUSTUP_HOME",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
    ];
    EXACT.contains(&name)
        || ["ANTHROPIC_", "CLAUDE_", "OPENAI_", "CODEX_"]
            .iter()
            .any(|prefix| name.starts_with(prefix))
}

fn environment_name_selects_sandbox(name: &str) -> bool {
    name.to_ascii_uppercase().contains("SANDBOX")
}

fn environment_name_disables_network(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "NETWORK_DISABLED",
        "DISABLE_NETWORK",
        "NO_NETWORK",
        "OFFLINE",
    ]
    .iter()
    .any(|marker| upper.contains(marker))
}

#[cfg(test)]
#[path = "config/tests/mod.rs"]
mod tests;
