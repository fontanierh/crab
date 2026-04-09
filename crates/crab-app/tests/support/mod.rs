#![allow(dead_code)]

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
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
        fs::create_dir_all(&path).expect("workspace directory should be creatable");
        Self { path }
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub(crate) fn runtime_config_for_workspace(root: &Path) -> RuntimeConfig {
    let mut values = HashMap::new();
    values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());
    values.insert(
        "CRAB_WORKSPACE_ROOT".to_string(),
        root.to_string_lossy().to_string(),
    );
    RuntimeConfig::from_map(&values).expect("runtime config should parse")
}

pub(crate) fn seed_ready_workspace(root: &Path) {
    for file_name in [
        "AGENTS.md",
        "SOUL.md",
        "IDENTITY.md",
        "USER.md",
        "MEMORY.md",
    ] {
        fs::write(root.join(file_name), "seed\n").expect("seed file should be writable");
    }
    let _ = fs::remove_file(root.join("BOOTSTRAP.md"));
}

pub(crate) fn child_env(command: &mut Command, workspace_root: &Path) {
    command
        .env("CRAB_DISCORD_TOKEN", "test-token")
        .env("CRAB_WORKSPACE_ROOT", workspace_root)
        .env("CRAB_BOT_USER_ID", "999")
        .env("CRAB_DAEMON_TICK_INTERVAL_MS", "1")
        .env("CRAB_DAEMON_MAX_ITERATIONS", "3")
        .env("CRAB_OUTBOUND_RECEIPT_TIMEOUT_MS", "100")
        .env("CRAB_DAEMON_FORCE_DETERMINISTIC_CLAUDE_PROCESS", "1");
}
