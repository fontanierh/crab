use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crab_backends::{CodexAppServerProcess, CodexProcessHandle, OpenCodeServerHandle};
use crab_core::{CrabResult, RuntimeConfig};

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

#[derive(Debug, Clone, Default)]
pub(crate) struct FakeCodexProcess;

impl CodexAppServerProcess for FakeCodexProcess {
    fn spawn_app_server(&self) -> CrabResult<CodexProcessHandle> {
        Ok(CodexProcessHandle {
            process_id: 101,
            started_at_epoch_ms: 1_739_173_200_000,
        })
    }

    fn is_healthy(&self, _handle: &CodexProcessHandle) -> bool {
        true
    }

    fn terminate_app_server(&self, _handle: &CodexProcessHandle) -> CrabResult<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct FakeOpenCodeProcess;

impl crab_backends::OpenCodeServerProcess for FakeOpenCodeProcess {
    fn spawn_server(&self) -> CrabResult<OpenCodeServerHandle> {
        Ok(OpenCodeServerHandle {
            process_id: 202,
            started_at_epoch_ms: 1_739_173_200_001,
            server_base_url: "http://127.0.0.1:4210".to_string(),
        })
    }

    fn is_server_healthy(&self, _handle: &OpenCodeServerHandle) -> bool {
        true
    }

    fn terminate_server(&self, _handle: &OpenCodeServerHandle) -> CrabResult<()> {
        Ok(())
    }
}
