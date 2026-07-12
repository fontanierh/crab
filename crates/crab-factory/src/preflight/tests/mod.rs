use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::sync::atomic::AtomicBool;

use crate::factory_test_support::Fixture;

use super::*;

fn options(fixture: &Fixture) -> LaunchOptions {
    LaunchOptions {
        prompt_file: fixture.prompt.clone(),
        repo: fixture.repo.clone(),
        base_ref: "HEAD".to_string(),
        run_id: Some(fixture.run_id.clone()),
        additional_review_rounds: 0,
        artifact_root: Some(fixture.runs.clone()),
        worktree_root: Some(fixture.worktrees.clone()),
        agent_timeout_seconds: 60,
        allow_dirty_source: false,
        launcher: None,
        effort: None,
        plan_critics: None,
        codex_reviewers: None,
    }
}

fn reject_journal_creation(_: PathBuf, _: Manifest) -> FactoryResult<Journal> {
    Err(FactoryError::new("injected manifest creation failure"))
}

#[test]
fn destination_validation_rejects_containment_and_overlap() {
    let repo = Path::new("/repo");
    assert!(validate_destinations(repo, Path::new("/repo/runs/id"), Path::new("/wt/id")).is_err());
    assert!(validate_destinations(repo, Path::new("/runs/id"), Path::new("/repo/wt/id")).is_err());
    assert!(validate_destinations(repo, Path::new("/runs/id"), Path::new("/runs/id/wt")).is_err());
    validate_destinations(repo, Path::new("/runs/id"), Path::new("/wt/id")).unwrap();
    assert_eq!(anchored(Path::new("relative"), repo), repo.join("relative"));
    assert_eq!(
        anchored(Path::new("/absolute"), repo),
        Path::new("/absolute")
    );
}

#[test]
fn quality_preflight_enumerates_every_external_gate_command() {
    assert_eq!(
        QUALITY_GATE_EXECUTABLES,
        [
            "cargo", "rg", "npx", "bash", "python3", "find", "sort", "grep", "sed", "dirname",
            "mkdir", "pwd",
        ]
    );
}

#[test]
fn validation_derives_defaults_and_tool_probe_failures_are_actionable() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let original_path = std::env::var_os("PATH");
    let fixture = Fixture::new("preflight-branches", "clean");
    std::env::set_var("PATH", fixture.environment_path());

    let mut value = options(&fixture);
    value.run_id = None;
    let validated = validate_without_writes(value, RequestedMode::Start).unwrap();
    assert!(validated.launch.run_id.starts_with("20"));
    assert_eq!(validated.launch.mode, "start");

    let cancellation = Arc::new(AtomicBool::new(false));
    assert_eq!(
        probe_version(
            Path::new("/bin/sh"),
            &["-c", "printf stderr-version >&2"],
            Arc::clone(&cancellation),
        )
        .unwrap(),
        "stderr-version"
    );
    assert!(probe_version(
        Path::new("/bin/sh"),
        &["-c", "exit 9"],
        Arc::clone(&cancellation),
    )
    .is_err());
    assert!(probe_version(
        Path::new("/bin/sh"),
        &["-c", "exit 0"],
        Arc::clone(&cancellation),
    )
    .is_err());
    assert!(probe_version(
        Path::new("/definitely/missing/factory-tool"),
        &[],
        cancellation,
    )
    .is_err());

    match original_path {
        Some(path) => std::env::set_var("PATH", path),
        None => std::env::remove_var("PATH"),
    }
}

#[test]
fn multicall_symlink_keeps_the_requested_executable_leaf_name() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let original_path = std::env::var_os("PATH");
    let root = std::env::temp_dir().join(format!("crab-factory-multicall-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let multicall = root.join("fake-multicall");
    fs::write(
        &multicall,
        "#!/bin/sh\ncase \"$0\" in *cargo) echo cargo-leaf;; *) exit 7;; esac\n",
    )
    .unwrap();
    fs::set_permissions(&multicall, fs::Permissions::from_mode(0o755)).unwrap();
    std::os::unix::fs::symlink(&multicall, root.join("cargo")).unwrap();
    std::env::set_var("PATH", &root);
    let cargo = resolve_executable(OsStr::new("cargo")).unwrap();
    assert_eq!(cargo, fs::canonicalize(&root).unwrap().join("cargo"));
    assert_eq!(
        probe_version(&cargo, &["--version"], Arc::new(AtomicBool::new(false)),).unwrap(),
        "cargo-leaf"
    );
    match original_path {
        Some(path) => std::env::set_var("PATH", path),
        None => std::env::remove_var("PATH"),
    }
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn reservation_guard_recovers_manifest_and_status_when_journal_creation_fails() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let original_path = std::env::var_os("PATH");
    let fixture = Fixture::new("reservation-journal-failure", "clean");
    std::env::set_var("PATH", fixture.environment_path());
    let validated = validate_without_writes(options(&fixture), RequestedMode::Run).unwrap();
    let launch = validated.launch.clone();
    let fallback = Manifest::initial(
        &launch,
        validated.maximum_review_rounds,
        fixture.run_dir().join("00-request.md"),
        validated.tool_paths.clone(),
        validated.tool_versions.clone(),
    )
    .unwrap();
    let error = reserve_with_journal_creator(validated, reject_journal_creation).unwrap_err();
    assert!(error
        .to_string()
        .contains("injected manifest creation failure"));
    let manifest = Journal::load(fixture.run_dir().join("manifest.json"))
        .unwrap()
        .snapshot()
        .unwrap();
    assert_eq!(manifest.status, "failed");
    assert!(fixture.run_dir().join("final-status.md").is_file());
    {
        let _armed_guard = ReservationTerminalGuard::new(fixture.run_dir(), launch, fallback);
    }
    assert!(
        fs::read_to_string(fixture.run_dir().join("final-status.md"))
            .unwrap()
            .contains("run reservation ended before initialization completed")
    );
    match original_path {
        Some(path) => std::env::set_var("PATH", path),
        None => std::env::remove_var("PATH"),
    }
}
