use super::*;
use crate::config::{resolve_executable, ToolPaths, ToolVersions};
use crate::factory_test_support::Fixture;
use crate::launch::*;
use crate::manifest::{Journal, Manifest};
use crate::pipeline::*;
use crate::preflight::ReservedRun;
use crate::run_lock::RunLock;
use crate::{
    atomic_write, create_exclusive_dir, set_secure_dir_permissions, utc_now_rfc3339, write_new_file,
};
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI32, Ordering};

static POST_SPAWN_PID: AtomicI32 = AtomicI32::new(0);

struct FailWriter;

impl Write for FailWriter {
    fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
        Err(std::io::Error::other("intentional output failure"))
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Err(std::io::Error::other("intentional flush failure"))
    }
}

fn prepared(fixture: &Fixture, launcher: Option<PathBuf>) -> ReservedRun {
    create_secure_dir(&fixture.runs).unwrap();
    create_secure_dir(&fixture.worktrees).unwrap();
    create_exclusive_dir(&fixture.run_dir()).unwrap();
    set_secure_dir_permissions(&fixture.run_dir()).unwrap();
    let artifact_root = fs::canonicalize(&fixture.runs).unwrap();
    let worktree_root = fs::canonicalize(&fixture.worktrees).unwrap();
    let run_dir = artifact_root.join(&fixture.run_id);
    let worktree = worktree_root.join(&fixture.run_id);
    create_secure_dir(&run_dir.join("prompts")).unwrap();
    create_secure_dir(&run_dir.join("logs")).unwrap();
    let request = fs::read(&fixture.prompt).unwrap();
    let request_sha256 = sha256_hex(&request);
    let tools = ToolPaths {
        git: resolve_executable(OsStr::new("git")).unwrap(),
        claude: fixture.fake_bin.join("claude"),
        codex: fixture.fake_bin.join("codex"),
        make: fixture.fake_bin.join("make"),
    };
    let launch = LaunchRecord {
        run_id: fixture.run_id.clone(),
        mode: "start".to_string(),
        queued_at: utc_now_rfc3339().unwrap(),
        source_prompt: fixture.prompt.clone(),
        request_sha256: request_sha256.clone(),
        repo: fs::canonicalize(&fixture.repo).unwrap(),
        base_ref: "HEAD".to_string(),
        base_sha: fixture.source_head(),
        source_was_dirty: false,
        allow_dirty_source: false,
        additional_review_rounds: 0,
        agent_timeout_seconds: 60,
        artifact_root,
        worktree_root,
        worktree,
        branch: format!("factory/{}", fixture.run_id),
        launch_mode: None,
        launched_pid: None,
        proc_name: crate::config::proc_name_for(&fixture.run_id),
        launcher: launcher.clone(),
        effort: Some(crate::config::DEFAULT_EFFORT),
        plan_critics: Some(2),
        codex_reviewers: Some(2),
        tool_paths: Some(tools.clone()),
    };
    let manifest = Manifest::initial(
        &launch,
        1,
        run_dir.join("00-request.md"),
        tools,
        ToolVersions {
            git: "git fake".to_string(),
            claude: "claude fake".to_string(),
            codex: "codex fake".to_string(),
            make_tool: "make fake".to_string(),
        },
    )
    .unwrap();
    Journal::create(run_dir.join("manifest.json"), manifest).unwrap();
    RunLock::initialize(&run_dir, &fixture.run_id, &request_sha256).unwrap();
    crate::controls::initialize(&run_dir, &fixture.run_id, &request_sha256).unwrap();
    write_new_file(&run_dir.join("00-request.md"), &request, 0o400).unwrap();
    launch.write(&run_dir.join("launch.json")).unwrap();
    ReservedRun {
        run_dir,
        request_sha256,
        launcher,
    }
}

fn reject_pid_record(_: &Path, pid: u32) -> FactoryResult<()> {
    POST_SPAWN_PID.store(pid as i32, Ordering::SeqCst);
    Err(FactoryError::new("injected PID-record failure"))
}

fn reject_lock(_: &Path) -> Result<RunLock, RunLockError> {
    Err(RunLockError::Other(FactoryError::new(
        "injected lock acquisition failure",
    )))
}

#[test]
fn verdict_parser_accepts_one_exact_line_and_rejects_ambiguous_reports() {
    assert_eq!(parse_verdict("\nVERDICT: CLEAN\ntext").unwrap(), "clean");
    assert_eq!(
        parse_verdict("VERDICT: CHANGES_REQUIRED\r\ntext").unwrap(),
        "changes_required"
    );
    assert_eq!(
        parse_verdict("preface\nVERDICT: CLEAN\nreport").unwrap(),
        "clean"
    );
    assert_eq!(
        parse_verdict("preface\n  VERDICT: CLEAN\ntrailing prose").unwrap(),
        "clean"
    );
    assert_eq!(
        parse_verdict("text without a verdict").unwrap_err().to_string(),
        "review has no exact verdict line; expected exactly one VERDICT: CLEAN or VERDICT: CHANGES_REQUIRED"
    );
    assert_eq!(
        parse_verdict("VERDICT: CLEAN\nVERDICT: CLEAN").unwrap_err().to_string(),
        "review has multiple verdict lines; expected exactly one VERDICT: CLEAN or VERDICT: CHANGES_REQUIRED"
    );
    assert!(parse_verdict("VERDICT: CLEAN\nVERDICT: CHANGES_REQUIRED").is_err());
    assert_eq!(require_normal_outcome(Some("clean")).unwrap(), "clean");
    assert!(require_normal_outcome(None).is_err());
}

#[test]
fn signal_install_seam_propagates_errors_and_shares_flag() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    fn success(flag: Arc<AtomicBool>) -> Result<(), String> {
        flag.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn failure(_: Arc<AtomicBool>) -> Result<(), String> {
        Err("signal failure".to_string())
    }
    assert!(install_signal_handlers_with(success)
        .unwrap()
        .load(Ordering::SeqCst));
    assert!(install_signal_handlers_with(failure)
        .unwrap_err()
        .to_string()
        .contains("signal failure"));
    assert!(register_signal(Arc::new(AtomicBool::new(false)), -1, "invalid").is_err());

    let fixture = Fixture::new("start-signal-failure", "clean");
    let reserved = prepared(&fixture, None);
    assert!(start_background_with_signal_result(
        reserved,
        Path::new("/usr/bin/true"),
        &mut Vec::new(),
        &mut Vec::new(),
        record_launched_pid,
        Err(FactoryError::new("injected start signal failure")),
        LAUNCHER_TIMEOUT,
    )
    .is_err());
    assert_eq!(fixture.manifest()["status"], "failed");
}

#[test]
fn signal_install_manifest_load_and_final_success_write_failures_terminalize_consistently() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    fn signal_failure(_: Arc<AtomicBool>) -> Result<(), String> {
        Err("injected signal install failure".to_string())
    }

    let mut fixture = Fixture::new("terminal-failures", "clean");
    fixture.run_id = "exec-signal-failure".to_string();
    let reserved = prepared(&fixture, None);
    assert!(execute_run_with_installer(
        &reserved.run_dir,
        &reserved.request_sha256,
        &mut Vec::new(),
        signal_failure,
    )
    .is_err());
    assert_eq!(fixture.manifest()["status"], "failed");
    assert!(fixture.run_dir().join("final-status.md").is_file());

    fixture.run_id = "exec-corrupt-manifest".to_string();
    let reserved = prepared(&fixture, None);
    fs::write(reserved.run_dir.join("manifest.json"), "not json").unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new(),).is_err());
    assert_eq!(fixture.manifest()["status"], "failed");
    assert!(fixture.run_dir().join("final-status.md").is_file());

    fixture.run_id = "completion-output-failure".to_string();
    let reserved = prepared(&fixture, None);
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    assert!(finish_successful_run(&journal, &reserved.run_dir, &mut FailWriter, "clean").is_err());
    assert_eq!(journal.snapshot().unwrap().status, "initializing");
    finalize_failure(&journal, &reserved.run_dir, "completion output failed");
    assert_eq!(fixture.manifest()["status"], "failed");

    fixture.run_id = "completion-status-failure".to_string();
    let reserved = prepared(&fixture, None);
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    let status_path = reserved.run_dir.join("final-status.md");
    fs::create_dir(&status_path).unwrap();
    assert!(finish_successful_run(&journal, &reserved.run_dir, &mut Vec::new(), "clean").is_err());
    assert_eq!(journal.snapshot().unwrap().status, "initializing");
    fs::remove_dir(&status_path).unwrap();
    finalize_failure(&journal, &reserved.run_dir, "final status failed");
    assert_eq!(fixture.manifest()["status"], "failed");
    assert!(status_path.is_file());

    fixture.run_id = "completion-control-sweep-failure".to_string();
    let reserved = prepared(&fixture, None);
    fs::write(reserved.run_dir.join("controls/state.json"), "not json").unwrap();
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    finish_successful_run(&journal, &reserved.run_dir, &mut Vec::new(), "clean").unwrap();
    assert_eq!(journal.snapshot().unwrap().status, "complete");

    fixture.run_id = "execution-interrupted".to_string();
    let reserved = prepared(&fixture, None);
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    journal.set_running().unwrap();
    assert!(finalize_execution_result(
        &journal,
        &reserved.run_dir,
        Err(FactoryError::new("factory interrupted by signal")),
    )
    .is_err());
    assert!(fixture.manifest()["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event"] == "interrupted"));

    fixture.run_id = "execution-snapshot-failure".to_string();
    let reserved = prepared(&fixture, None);
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    finalize_execution_error(
        &journal,
        &reserved.run_dir,
        &FactoryError::new("execution failed"),
        Err(FactoryError::new("snapshot failed")),
    );
    assert_eq!(fixture.manifest()["status"], "failed");
}

#[test]
fn launcher_pid_parser_and_output_combiner_are_deterministic() {
    let root = std::env::temp_dir().join(format!("crab-factory-pid-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let receipt = root.join("receipt");
    fs::write(&receipt, "PID=123\n").unwrap();
    assert_eq!(read_launcher_pid(&receipt).unwrap(), Some(123));
    fs::write(&receipt, "\n").unwrap();
    assert_eq!(read_launcher_pid(&receipt).unwrap(), None);
    fs::write(&receipt, "invalid\n").unwrap();
    assert!(read_launcher_pid(&receipt).is_err());
    fs::remove_dir_all(root).unwrap();
    assert_eq!(combined_output(b"out", b"err"), b"out\nerr");
    assert_eq!(combined_output(b"out\n", b"err"), b"out\nerr");
    let before = crate::gitops::WorktreeFingerprint::synthetic("one", &[("file", "hash")]);
    assert_quality_unchanged(&before, &before).unwrap();
    let status_only = crate::gitops::WorktreeFingerprint::synthetic("two", &[("file", "hash")]);
    assert!(assert_quality_unchanged(&before, &status_only)
        .unwrap_err()
        .to_string()
        .contains("git status changed"));
    let content = crate::gitops::WorktreeFingerprint::synthetic("three", &[("file", "changed")]);
    assert!(assert_quality_unchanged(&before, &content)
        .unwrap_err()
        .to_string()
        .contains("file"));
    assert!(finish_launcher_recording(
        Path::new("/missing-run"),
        i32::MAX as u32,
        Err(FactoryError::new("launcher log failed")),
        record_launched_pid,
    )
    .is_err());
    assert!(finish_launcher_recording(
        Path::new("/missing-run"),
        i32::MAX as u32,
        Ok(()),
        reject_pid_record,
    )
    .is_err());
}

#[test]
fn a_post_spawn_record_failure_kills_the_executor_before_terminalization() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let fixture = Fixture::new("post-spawn-failure", "clean");
    let executable = fixture.root.join("sleeping-executor");
    fs::write(&executable, "#!/bin/sh\nsleep 120\n").unwrap();
    fs::set_permissions(&executable, fs::Permissions::from_mode(0o755)).unwrap();
    let result = start_background_with_pid_recorder(
        prepared(&fixture, None),
        &executable,
        &mut Vec::new(),
        &mut Vec::new(),
        reject_pid_record,
    );
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("injected PID-record failure"));
    let pid = POST_SPAWN_PID.load(Ordering::SeqCst);
    assert!(pid > 0);
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(fixture.manifest()["status"], "failed");
    assert!(fixture.run_dir().join("final-status.md").is_file());
    assert!(print_launch(&mut FailWriter, &fixture.run_dir(), pid as u32, "detached",).is_err());
}

#[test]
fn established_run_metadata_and_launch_failures_are_terminalized() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let mut fixture = Fixture::new("launch-failures", "clean");
    fixture.run_id = "foreground-launch-missing".to_string();
    let reserved = prepared(&fixture, None);
    fs::remove_file(reserved.run_dir.join("launch.json")).unwrap();
    assert!(run_foreground(reserved, &mut Vec::new()).is_err());
    assert!(fixture.run_dir().join("final-status.md").is_file());

    fixture.run_id = "detached-spawn-missing".to_string();
    assert!(start_background(
        prepared(&fixture, None),
        Path::new("/definitely/missing/factory-executable"),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .is_err());

    fixture.run_id = "detached-update-missing".to_string();
    let reserved = prepared(&fixture, None);
    fs::remove_file(reserved.run_dir.join("launch.json")).unwrap();
    assert!(start_background(
        reserved,
        Path::new("/usr/bin/true"),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .is_err());

    let mut warning = Vec::new();
    report_launch(
        Err(FactoryError::new("intentional presentation failure")),
        &mut warning,
    );
    assert!(String::from_utf8(warning).unwrap().contains("warning:"));

    fixture.run_id = "launcher-spawn-missing".to_string();
    let launcher = fixture.launcher("noop");
    let reserved = prepared(&fixture, Some(launcher.clone()));
    fs::remove_file(launcher).unwrap();
    assert!(start_background(
        reserved,
        Path::new("/usr/bin/true"),
        &mut Vec::new(),
        &mut Vec::new(),
    )
    .is_err());
    assert!(fixture.run_dir().join("launch-error.txt").is_file());

    fixture.run_id = "launcher-timeout".to_string();
    let launcher = fixture.launcher("spawn-timeout");
    let sleeping_executor = fixture.root.join("launcher-sleeping-executor");
    fs::write(&sleeping_executor, "#!/bin/sh\nsleep 120\n").unwrap();
    fs::set_permissions(&sleeping_executor, fs::Permissions::from_mode(0o755)).unwrap();
    let result = start_background_with_signal_result(
        prepared(&fixture, Some(launcher)),
        &sleeping_executor,
        &mut Vec::new(),
        &mut Vec::new(),
        record_launched_pid,
        Ok(Arc::new(AtomicBool::new(false))),
        Duration::from_secs(2),
    );
    assert!(result.unwrap_err().to_string().contains("launcher failed"));
    let receipt = fs::read_to_string(fixture.run_dir().join("launcher-pid")).unwrap();
    let pid: i32 = receipt.trim().trim_start_matches("PID=").parse().unwrap();
    assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
    assert_eq!(fixture.manifest()["status"], "failed");
}

#[test]
fn executor_rejects_each_tampered_prepared_run_boundary() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    assert!(execute_run(
        Path::new("/definitely/missing/factory-run"),
        &"0".repeat(64),
        &mut Vec::new(),
    )
    .is_err());

    let mut fixture = Fixture::new("tampered-boundaries", "clean");
    fixture.run_id = "marker-directory-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    let renamed = reserved.run_dir.with_file_name("renamed-run-directory");
    fs::rename(&reserved.run_dir, &renamed).unwrap();
    assert!(
        execute_run(&renamed, &reserved.request_sha256, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("marker does not match")
    );
    assert!(renamed.join("final-status.md").is_file());

    fixture.run_id = "run-id-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    let launch_path = reserved.run_dir.join("launch.json");
    let mut launch = LaunchRecord::read(&launch_path).unwrap();
    launch.run_id = "different-run-id".to_string();
    launch.write(&launch_path).unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());

    fixture.run_id = "status-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    Journal::load(reserved.run_dir.join("manifest.json"))
        .unwrap()
        .set_running()
        .unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());

    fixture.run_id = "sha-metadata-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    assert!(execute_run(&reserved.run_dir, &"f".repeat(64), &mut Vec::new()).is_err());

    fixture.run_id = "configuration-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    let launch_path = reserved.run_dir.join("launch.json");
    let mut launch = LaunchRecord::read(&launch_path).unwrap();
    launch.effort = Some(crate::config::Effort::Max);
    launch.write(&launch_path).unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());

    fixture.run_id = "tool-path-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    let manifest_path = reserved.run_dir.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["tool_paths"]["git"] = serde_json::Value::String("/usr/bin/false".to_string());
    atomic_write(
        &manifest_path,
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());

    fixture.run_id = "configuration-out-of-bounds".to_string();
    let reserved = prepared(&fixture, None);
    let launch_path = reserved.run_dir.join("launch.json");
    let mut launch: serde_json::Value =
        serde_json::from_slice(&fs::read(&launch_path).unwrap()).unwrap();
    launch["plan_critics"] = serde_json::json!(9);
    atomic_write(&launch_path, &serde_json::to_vec_pretty(&launch).unwrap()).unwrap();
    let manifest_path = reserved.run_dir.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["prepared_configuration"]["plan_critics"] = serde_json::json!(9);
    manifest["effective_configuration"]["plan_critics"] = serde_json::json!(9);
    atomic_write(
        &manifest_path,
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());

    fixture.run_id = "request-path-mismatch".to_string();
    let reserved = prepared(&fixture, None);
    let manifest_path = reserved.run_dir.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["request"] = serde_json::Value::String("wrong-request".to_string());
    atomic_write(
        &manifest_path,
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());

    fixture.run_id = "worktree-appeared".to_string();
    let reserved = prepared(&fixture, None);
    fs::create_dir_all(fixture.worktree()).unwrap();
    let error = execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new())
        .unwrap_err()
        .to_string();
    assert!(error.contains("worktree destination appeared"), "{error}");

    fixture.run_id = "worktree-parent-missing".to_string();
    let reserved = prepared(&fixture, None);
    let manifest_path = reserved.run_dir.join("manifest.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["worktree"] = serde_json::Value::String(String::new());
    atomic_write(
        &manifest_path,
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let launch_path = reserved.run_dir.join("launch.json");
    let mut launch = LaunchRecord::read(&launch_path).unwrap();
    launch.worktree = PathBuf::new();
    launch.write(&launch_path).unwrap();
    assert!(
        execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new())
            .unwrap_err()
            .to_string()
            .contains("no parent directory")
    );

    fixture.run_id = "lock-open-failure".to_string();
    let reserved = prepared(&fixture, None);
    fs::remove_file(reserved.run_dir.join(".lock")).unwrap();
    fs::create_dir(reserved.run_dir.join(".lock")).unwrap();
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());
    assert_eq!(fixture.manifest()["status"], "initializing");
    assert!(!fixture.run_dir().join("final-status.md").exists());

    fixture.run_id = "lock-acquire-failure".to_string();
    let reserved = prepared(&fixture, None);
    assert!(execute_run_with_dependencies(
        &reserved.run_dir,
        &reserved.request_sha256,
        &mut Vec::new(),
        default_signal_installer,
        reject_lock,
    )
    .unwrap_err()
    .to_string()
    .contains("injected lock acquisition failure"));
    assert_eq!(fixture.manifest()["status"], "failed");

    fixture.run_id = "worktree-add-failure".to_string();
    let reserved = prepared(&fixture, None);
    assert!(std::process::Command::new("git")
        .args(["branch", &format!("factory/{}", fixture.run_id)])
        .current_dir(&fixture.repo)
        .status()
        .unwrap()
        .success());
    assert!(execute_run(&reserved.run_dir, &reserved.request_sha256, &mut Vec::new()).is_err());
    assert_eq!(fixture.manifest()["status"], "failed");
}

#[test]
fn quality_supervision_reports_timeout_spawn_failure_cancellation_and_output_failure() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let fixture = Fixture::new("quality-supervision", "quality-sleep");
    let reserved = prepared(&fixture, None);
    let journal = Arc::new(Journal::load(reserved.run_dir.join("manifest.json")).unwrap());
    let snapshot = journal.snapshot().unwrap();
    assert!(std::process::Command::new("git")
        .args([
            "-C",
            fixture.repo.to_str().unwrap(),
            "worktree",
            "add",
            "-b",
            &snapshot.branch,
            fixture.worktree().to_str().unwrap(),
            &snapshot.base_sha,
        ])
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .status()
        .unwrap()
        .success());
    let cancellation = Arc::new(AtomicBool::new(false));
    let git = GitRunner::new(
        snapshot.tool_paths.git.clone(),
        Arc::clone(&cancellation),
        Duration::from_secs(2),
    );
    let mut output = Vec::new();
    let mut pipeline = Pipeline {
        run_dir: reserved.run_dir.clone(),
        request: "request".to_string(),
        base_sha: snapshot.base_sha.clone(),
        branch: snapshot.branch.clone(),
        worktree: snapshot.worktree.clone(),
        tools: snapshot.tool_paths.clone(),
        timeout: Duration::from_millis(50),
        maximum_review_rounds: 1,
        effort: crate::config::DEFAULT_EFFORT,
        plan_critics: 2,
        codex_reviewers: 2,
        journal: Arc::clone(&journal),
        git,
        cancellation: Arc::clone(&cancellation),
        stdout: &mut output,
        controls: crate::controls::ControlPlane::new(
            reserved.run_dir.clone(),
            Arc::clone(&journal),
        ),
    };
    assert!(pipeline
        .run_quality()
        .unwrap_err()
        .to_string()
        .contains("timeout"));
    pipeline.tools.make = PathBuf::from("/definitely/missing/factory-make");
    pipeline.timeout = Duration::from_secs(1);
    assert!(pipeline
        .run_quality()
        .unwrap_err()
        .to_string()
        .contains("supervision"));
    cancellation.store(true, Ordering::SeqCst);
    assert!(pipeline
        .run_quality()
        .unwrap_err()
        .to_string()
        .contains("interrupted"));

    let combined = pipeline
        .finish_stage::<()>(
            "combined stage failure",
            Path::new("output"),
            Err(FactoryError::new("worker failed")),
            Err(FactoryError::new("invariant failed")),
        )
        .unwrap_err();
    assert!(combined.to_string().contains("stage invariant also failed"));

    let mut failed_output = FailWriter;
    assert!(failed_output.flush().is_err());
    pipeline.stdout = &mut failed_output;
    cancellation.store(false, Ordering::SeqCst);
    assert!(pipeline.log("message").is_err());
}
