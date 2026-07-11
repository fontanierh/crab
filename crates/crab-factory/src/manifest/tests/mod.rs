use std::fs;

use serde_json::json;

use super::*;

fn launch(root: &Path) -> LaunchRecord {
    LaunchRecord {
        run_id: "test-run".to_string(),
        mode: "run".to_string(),
        queued_at: "2026-07-11T00:00:00Z".to_string(),
        source_prompt: root.join("request"),
        request_sha256: "abc".to_string(),
        repo: root.join("repo"),
        base_ref: "HEAD".to_string(),
        base_sha: "def".to_string(),
        source_was_dirty: false,
        allow_dirty_source: false,
        additional_review_rounds: 0,
        agent_timeout_seconds: 60,
        artifact_root: root.join("runs"),
        worktree_root: root.join("worktrees"),
        worktree: root.join("worktrees/test-run"),
        branch: "factory/test-run".to_string(),
        launch_mode: None,
        launched_pid: None,
        proc_name: "code-factory-test-run".to_string(),
        launcher: None,
    }
}

fn tools() -> (ToolPaths, ToolVersions) {
    (
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
}

#[test]
fn journal_persists_lifecycle_and_launch_round_trip() {
    let root = std::env::temp_dir().join(format!("crab-factory-journal-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let launch = launch(&root);
    let launch_path = root.join("launch.json");
    launch.write(&launch_path).unwrap();
    assert_eq!(LaunchRecord::read(&launch_path).unwrap(), launch);
    let (paths, versions) = tools();
    let manifest =
        Manifest::initial(&launch, 1, root.join("00-request.md"), paths, versions).unwrap();
    let journal = Journal::create(root.join("manifest.json"), manifest).unwrap();
    journal.set_running().unwrap();
    journal.event("started", json!({"value": 1})).unwrap();
    journal.event("non-object", Value::Null).unwrap();
    journal
        .register_cohort(CohortRecord {
            name: "cohort".to_string(),
            members: vec!["agent".to_string()],
            prompt: root.join("prompt"),
            prompt_sha256: "hash".to_string(),
        })
        .unwrap();
    journal
        .agent_started(
            "agent".to_string(),
            AgentRecord {
                provider: "codex".to_string(),
                command: vec!["codex".to_string()],
                sandbox: "disabled".to_string(),
                permission_mode: "dangerously-bypass-approvals-and-sandbox".to_string(),
                network_access: true,
                prompt_sha256: "hash".to_string(),
                status: "running".to_string(),
                started_at: utc_now_rfc3339().unwrap(),
                finished_at: None,
                output: root.join("output"),
                log: root.join("log"),
                returncode: None,
            },
        )
        .unwrap();
    journal
        .agent_finished("agent", "complete", Some(0))
        .unwrap();
    journal.agent_finished("missing", "failed", None).unwrap();
    journal.checkpoint_review(1, Some("clean")).unwrap();
    journal.checkpoint_thermo("clean", Some(false)).unwrap();
    journal.complete("clean").unwrap();
    let loaded = Journal::load(root.join("manifest.json")).unwrap();
    let snapshot = loaded.snapshot().unwrap();
    assert_eq!(snapshot.status, "complete");
    assert_eq!(snapshot.outcome.as_deref(), Some("clean"));
    assert_eq!(snapshot.worker_policy.host_permissions, "unrestricted");
    assert_eq!(snapshot.worker_policy.sandbox, "disabled");
    assert!(snapshot.worker_policy.network_access);
    assert!(!snapshot.worker_policy.nested_agents_enabled);
    assert_eq!(snapshot.events[0]["event"], "started");
    loaded.fail("later failure").unwrap();
    assert_eq!(loaded.snapshot().unwrap().status, "failed");
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = loaded.data.lock().unwrap();
        panic!("poison journal lock");
    }));
    assert!(poisoned.is_err());
    assert!(loaded.snapshot().is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn invalid_json_is_rejected() {
    let root =
        std::env::temp_dir().join(format!("crab-factory-journal-bad-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    fs::write(root.join("bad.json"), b"not json").unwrap();
    assert!(Journal::load(root.join("bad.json")).is_err());
    assert!(LaunchRecord::read(&root.join("bad.json")).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn recovered_manifest_round_count_is_checked() {
    assert_eq!(checked_maximum_review_rounds(0).unwrap(), 1);
    assert!(checked_maximum_review_rounds(u32::MAX).is_err());
}

#[test]
fn failed_atomic_update_does_not_mutate_the_in_memory_manifest() {
    let root =
        std::env::temp_dir().join(format!("crab-factory-journal-txn-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let (tool_paths, tool_versions) = tools();
    let manifest = Manifest::initial(
        &launch(&root),
        1,
        root.join("request"),
        tool_paths,
        tool_versions,
    )
    .unwrap();
    let path = root.join("manifest.json");
    let journal = Journal::create(path.clone(), manifest).unwrap();
    fs::remove_file(&path).unwrap();
    fs::create_dir(&path).unwrap();
    assert!(journal.complete("clean").is_err());
    let snapshot = journal.snapshot().unwrap();
    assert_eq!(snapshot.status, "initializing");
    assert_eq!(snapshot.outcome, None);
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn json_serialization_errors_are_reported() {
    struct FailingSerialize;

    impl serde::Serialize for FailingSerialize {
        fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(
                "intentional serialization failure",
            ))
        }
    }

    assert!(json_bytes(&FailingSerialize).is_err());
}
