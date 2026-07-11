use std::fs;
use std::path::{Path, PathBuf};

use crate::config::{ToolPaths, ToolVersions};
use crate::manifest::{LaunchRecord, Manifest};
use crate::utc_now_rfc3339;

use super::*;

fn launch(root: &Path) -> LaunchRecord {
    LaunchRecord {
        run_id: "terminal-test".to_string(),
        mode: "run".to_string(),
        queued_at: utc_now_rfc3339().unwrap(),
        source_prompt: root.join("source-request"),
        request_sha256: "a".repeat(64),
        repo: root.join("repo"),
        base_ref: "HEAD".to_string(),
        base_sha: "b".repeat(40),
        source_was_dirty: false,
        allow_dirty_source: false,
        additional_review_rounds: 0,
        agent_timeout_seconds: 60,
        artifact_root: root.to_path_buf(),
        worktree_root: root.join("worktrees"),
        worktree: root.join("worktrees/terminal-test"),
        branch: "factory/terminal-test".to_string(),
        launch_mode: None,
        launched_pid: None,
        proc_name: "code-factory-terminal-test".to_string(),
        launcher: None,
    }
}

fn manifest(root: &Path, launch: &LaunchRecord) -> Manifest {
    Manifest::initial(
        launch,
        1,
        root.join("00-request.md"),
        ToolPaths {
            git: PathBuf::from("/git"),
            claude: PathBuf::from("/claude"),
            codex: PathBuf::from("/codex"),
            make: PathBuf::from("/make"),
        },
        ToolVersions {
            git: "git".to_string(),
            claude: "claude".to_string(),
            codex: "codex".to_string(),
            make_tool: "make".to_string(),
        },
    )
    .unwrap()
}

#[test]
fn fallback_status_survives_manifest_recovery_write_failures() {
    let root = std::env::temp_dir().join(format!("crab-factory-terminal-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let launch = launch(&root);
    launch.write(&root.join("launch.json")).unwrap();
    fs::create_dir(root.join("manifest.json")).unwrap();
    finalize_established_path(&root, "established recovery failed");
    assert!(fs::read_to_string(root.join("final-status.md"))
        .unwrap()
        .contains("established recovery failed"));

    fs::remove_file(root.join("final-status.md")).unwrap();
    finalize_initialization_failure(
        &root,
        &launch,
        manifest(&root, &launch),
        "initial recovery failed",
    );
    assert!(fs::read_to_string(root.join("final-status.md"))
        .unwrap()
        .contains("initial recovery failed"));

    fs::remove_file(root.join("launch.json")).unwrap();
    fs::remove_file(root.join("final-status.md")).unwrap();
    finalize_established_path(&root, "no launch metadata");
    assert!(fs::read_to_string(root.join("final-status.md"))
        .unwrap()
        .contains("no launch metadata"));
    fs::remove_dir_all(root).unwrap();
}
