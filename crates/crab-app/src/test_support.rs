use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crab_core::RuntimeConfig;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) struct TempWorkspace {
    pub(crate) path: PathBuf,
}

impl TempWorkspace {
    pub(crate) fn new(scope: &str, label: &str) -> Self {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "crab-app-{scope}-{label}-{}-{suffix}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        Self { path }
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(crate) fn runtime_config_for_workspace(root: &Path) -> RuntimeConfig {
    runtime_config_for_workspace_root(&root.to_string_lossy())
}

pub(crate) fn runtime_config_for_workspace_with_lanes(
    root: &Path,
    max_concurrent_lanes: usize,
) -> RuntimeConfig {
    let mut values = default_runtime_config_values();
    values.insert(
        "CRAB_WORKSPACE_ROOT".to_string(),
        root.to_string_lossy().to_string(),
    );
    values.insert(
        "CRAB_MAX_CONCURRENT_LANES".to_string(),
        max_concurrent_lanes.to_string(),
    );
    RuntimeConfig::from_map(&values).expect("runtime config should parse")
}

pub(crate) fn runtime_config_for_workspace_root(raw_workspace_root: &str) -> RuntimeConfig {
    let mut values = default_runtime_config_values();
    values.insert(
        "CRAB_WORKSPACE_ROOT".to_string(),
        raw_workspace_root.to_string(),
    );
    RuntimeConfig::from_map(&values).expect("runtime config should parse")
}

fn default_runtime_config_values() -> HashMap<String, String> {
    let mut values = HashMap::new();
    values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());
    values
}

/// Hex-encode a byte slice (used to derive on-disk store paths).
pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let upper = usize::from(byte >> 4);
        let lower = usize::from(byte & 0x0f);
        output.push(HEX[upper]);
        output.push(HEX[lower]);
    }
    output
}

pub(crate) fn state_root(workspace: &TempWorkspace) -> PathBuf {
    workspace.path.join("state")
}

pub(crate) fn session_file_path(state_root: &Path, logical_session_id: &str) -> PathBuf {
    state_root.join("sessions").join(format!(
        "{}.json",
        hex_encode(logical_session_id.as_bytes())
    ))
}

pub(crate) fn run_file_path(
    state_root: &Path,
    logical_session_id: &str,
    run_id: &str,
) -> PathBuf {
    state_root
        .join("runs")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(format!("{}.json", hex_encode(run_id.as_bytes())))
}

pub(crate) fn event_log_path(
    state_root: &Path,
    logical_session_id: &str,
    run_id: &str,
) -> PathBuf {
    state_root
        .join("events")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(format!("{}.jsonl", hex_encode(run_id.as_bytes())))
}

pub(crate) fn outbound_log_path(
    state_root: &Path,
    logical_session_id: &str,
    run_id: &str,
) -> PathBuf {
    state_root
        .join("outbound")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(format!("{}.jsonl", hex_encode(run_id.as_bytes())))
}

/// Replace an existing path (file or directory) with an empty directory.
pub(crate) fn replace_path_with_directory(path: &Path) {
    let _ = fs::remove_file(path);
    let _ = fs::remove_dir_all(path);
    fs::create_dir_all(path).expect("directory fixture should be creatable");
}
