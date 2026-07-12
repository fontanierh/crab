use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::time::{Duration, Instant};

mod support;

use support::{assert_failure, assert_success, read_json, Fixture};

#[test]
fn invalid_verdict_is_terminal() {
    assert_pipeline_failure("invalid", "invalid-verdict", "no exact verdict line");
}

#[test]
fn empty_output_is_terminal() {
    assert_pipeline_failure(
        "empty",
        "empty-output",
        "output from 02-critique-codex-01 is empty",
    );
}

#[test]
fn cohort_worker_failure_cancels_a_sleeping_peer() {
    let fixture = assert_pipeline_failure_with_limit(
        "worker",
        "worker-fail",
        "agent 02-critique-codex-01 exited 7",
        Duration::from_secs(60),
    );
    let manifest = fixture.manifest();
    assert_eq!(
        manifest["agents"]["02-critique-codex-01"]["status"],
        "failed"
    );
    assert_eq!(manifest["agents"]["02-critique-codex-01"]["returncode"], 7);
    assert_eq!(
        manifest["agents"]["02-critique-codex-02"]["status"],
        "cancelled"
    );
    assert_failed_stage_event(&fixture, "plan-critiques");
}

#[test]
fn persistent_cohort_prompt_mutation_is_terminal() {
    assert_pipeline_failure(
        "prompt",
        "prompt-mutate",
        "cohort prompt changed during execution",
    );
}

#[test]
fn readonly_worker_mutation_is_rejected() {
    assert_pipeline_failure(
        "readonly",
        "readonly-write",
        "read-only stage review-round-01 modified",
    );
}

#[test]
fn worker_commit_is_rejected() {
    assert_pipeline_failure("commit", "commit", "changed worktree HEAD");
}

#[test]
fn quality_mutation_is_rejected_after_checkpointing_reviews() {
    let fixture = assert_quality_failure(
        "quality-mutate",
        "quality-mutate",
        "make quality modified the worktree",
    );
    let events = fixture.manifest()["events"].as_array().unwrap().clone();
    let finished = event_index(&events, "quality_finished");
    let violation = event_index(&events, "quality_invariant_violation");
    assert!(finished < violation);
}

#[test]
fn quality_nonzero_is_rejected_after_checkpointing_reviews() {
    let _fixture = assert_quality_failure(
        "quality-fail",
        "quality-fail",
        "make quality failed with exit 6",
    );
}

#[test]
fn exec_refuses_an_arbitrary_directory_without_mutating_it() {
    let fixture = Fixture::new("arbitrary-exec", "clean");
    let mut before = fs::read_dir(&fixture.repo)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    before.sort();
    let mut command = Command::new(env!("CARGO_BIN_EXE_crab-factory"));
    command.args([
        "exec",
        "--run-dir",
        fixture.repo.to_str().unwrap(),
        "--request-sha256",
        &"0".repeat(64),
    ]);
    fixture.configure(&mut command);
    let output = command.output().unwrap();
    assert_failure(&output, "prepared-run marker");
    let mut after = fs::read_dir(&fixture.repo)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    after.sort();
    assert_eq!(after, before);
    assert!(!fixture.repo.join(".lock").exists());
    assert!(!fixture.repo.join("final-status.md").exists());
    assert!(git_status(&fixture.repo).is_empty());
}

#[test]
fn dirty_source_is_rejected_without_artifacts_and_explicit_override_is_isolated() {
    let fixture = Fixture::new("dirty", "clean");
    fs::write(fixture.repo.join("dirty.txt"), "caller change\n").unwrap();
    let output = fixture.run(0);
    assert_failure(&output, "source checkout is dirty");
    assert!(!fixture.run_dir().exists());
    assert!(!fixture.worktree().exists());

    let mut command = fixture.command(0);
    command.arg("--allow-dirty-source");
    let output = command.output().unwrap();
    assert_success(&output);
    let manifest = fixture.manifest();
    assert_eq!(manifest["source_was_dirty"], true);
    assert!(!fixture.worktree().join("dirty.txt").exists());
    assert_eq!(
        fs::read_to_string(fixture.repo.join("dirty.txt")).unwrap(),
        "caller change\n"
    );
}

#[test]
fn cli_and_root_refusals_happen_before_run_reservation() {
    let timeout = Fixture::new("timeout-refusal", "clean");
    let mut command = timeout.command(0);
    replace_arg_value(&mut command, "--agent-timeout-seconds", "59");
    let output = command.output().unwrap();
    assert_failure(&output, "must be between 60 and 86400");
    assert!(!timeout.run_dir().exists());

    let leading = Fixture::new("base-refusal", "clean");
    let mut command = leading.command(0);
    command.args(["--base", "-bad"]);
    let output = command.output().unwrap();
    assert_failure(&output, "--base must not begin with '-'");
    assert!(!leading.run_dir().exists());

    let roots = Fixture::new("root-refusal", "clean");
    let in_repo = roots.repo.join("artifacts");
    let mut command = roots.command(0);
    replace_arg_value(&mut command, "--artifact-root", in_repo.to_str().unwrap());
    let output = command.output().unwrap();
    assert_failure(&output, "artifact destination must be outside");
    assert!(!in_repo.exists());
    assert!(git_status(&roots.repo).is_empty());
}

#[test]
fn missing_tool_home_and_destination_collisions_are_pre_run_rejections() {
    let missing = Fixture::new("missing-codex", "clean");
    fs::remove_file(missing.fake_bin.join("codex")).unwrap();
    let mut command = missing.command(0);
    command.env(
        "PATH",
        format!("{}:/usr/bin:/bin", missing.fake_bin.display()),
    );
    let output = command.output().unwrap();
    assert_failure(
        &output,
        "required executable is not installed or executable: codex",
    );
    assert!(!missing.run_dir().exists());

    let no_home = Fixture::new("no-home", "clean");
    let mut command = Command::new(env!("CARGO_BIN_EXE_crab-factory"));
    command.args([
        "run",
        "--prompt-file",
        no_home.prompt.to_str().unwrap(),
        "--repo",
        no_home.repo.to_str().unwrap(),
        "--run-id",
        &no_home.run_id,
        "--agent-timeout-seconds",
        "60",
    ]);
    no_home.configure(&mut command);
    command.env_remove("HOME");
    let output = command.output().unwrap();
    assert_failure(&output, "HOME is not set");

    let run_collision = Fixture::new("run-collision", "clean");
    fs::create_dir_all(run_collision.run_dir()).unwrap();
    fs::write(run_collision.run_dir().join("marker"), "keep").unwrap();
    let output = run_collision.run(0);
    assert_failure(&output, "run artifact directory already exists");
    assert_eq!(
        fs::read_to_string(run_collision.run_dir().join("marker")).unwrap(),
        "keep"
    );

    let worktree_collision = Fixture::new("worktree-collision", "clean");
    fs::create_dir_all(worktree_collision.worktree()).unwrap();
    let output = worktree_collision.run(0);
    assert_failure(&output, "worktree destination already exists");
    assert!(!worktree_collision.run_dir().exists());

    let branch_collision = Fixture::new("branch-collision", "clean");
    assert!(Command::new("git")
        .args(["branch", &format!("factory/{}", branch_collision.run_id)])
        .current_dir(&branch_collision.repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .status()
        .unwrap()
        .success());
    let output = branch_collision.run(0);
    assert_failure(&output, "factory branch already exists");
    assert!(!branch_collision.run_dir().exists());
}

#[test]
fn managed_snapshot_tampering_and_launcher_failure_finalize_the_run() {
    let fixture = Fixture::new("tamper", "clean");
    let launcher = fixture.launcher("noop");
    let mut start = fixture.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    assert_success(&start.output().unwrap());
    let launch = read_json(&fixture.run_dir().join("launch.json"));
    let snapshot = fixture.run_dir().join("00-request.md");
    fs::set_permissions(&snapshot, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&snapshot, "tampered").unwrap();
    fs::set_permissions(&snapshot, fs::Permissions::from_mode(0o400)).unwrap();
    let mut exec = Command::new(env!("CARGO_BIN_EXE_crab-factory"));
    exec.args([
        "exec",
        "--run-dir",
        fixture.run_dir().to_str().unwrap(),
        "--request-sha256",
        launch["request_sha256"].as_str().unwrap(),
    ]);
    fixture.configure(&mut exec);
    let output = exec.output().unwrap();
    assert_failure(&output, "managed request snapshot hash mismatch");
    assert_eq!(fixture.manifest()["status"], "failed");
    assert!(fixture.run_dir().join("final-status.md").is_file());

    let failed = Fixture::new("launcher-fail", "clean");
    let launcher = failed.launcher("fail");
    let mut start = failed.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    let output = start.output().unwrap();
    assert_failure(&output, "launcher exited 7");
    assert!(failed.run_dir().join("launch-error.txt").is_file());
    assert_eq!(failed.manifest()["status"], "failed");
    assert!(failed.run_dir().join("final-status.md").is_file());

    let missing_pid = Fixture::new("launcher-no-pid", "clean");
    let launcher = missing_pid.launcher("no-pid");
    let mut start = missing_pid.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    let output = start.output().unwrap();
    assert_failure(&output, "without writing the required PID receipt");
    assert!(missing_pid.run_dir().join("launch-error.txt").is_file());
    assert_eq!(missing_pid.manifest()["status"], "failed");

    let invalid_pid = Fixture::new("launcher-invalid-pid", "clean");
    let launcher = invalid_pid.launcher("invalid-pid");
    let mut start = invalid_pid.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    let output = start.output().unwrap();
    assert_failure(&output, "invalid PID receipt");
    assert_eq!(invalid_pid.manifest()["status"], "failed");

    let spawned = Fixture::new("launcher-spawn-fail", "slow-plan");
    let launcher = spawned.launcher("spawn-fail");
    let mut start = spawned.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    let output = start.output().unwrap();
    assert_failure(&output, "launcher exited 7");
    let receipt = fs::read_to_string(spawned.run_dir().join("launcher-pid")).unwrap();
    let pid = receipt.trim().trim_start_matches("PID=").parse().unwrap();
    support::wait_for_pid_exit(pid, Duration::from_secs(10));
    assert_eq!(spawned.manifest()["status"], "failed");
    assert!(spawned.run_dir().join("final-status.md").is_file());
}

fn assert_pipeline_failure(label: &str, scenario: &str, expected: &str) -> Fixture {
    assert_pipeline_failure_with_limit(label, scenario, expected, Duration::from_secs(90))
}

fn assert_pipeline_failure_with_limit(
    label: &str,
    scenario: &str,
    expected: &str,
    limit: Duration,
) -> Fixture {
    let fixture = Fixture::new(label, scenario);
    let started = Instant::now();
    let output = fixture.run(0);
    assert_failure(&output, expected);
    assert!(
        started.elapsed() < limit,
        "{scenario} exceeded the bounded integration-test runtime"
    );
    let manifest = fixture.manifest();
    assert_eq!(manifest["status"], "failed");
    assert!(manifest["error"].as_str().unwrap().contains(expected));
    assert!(fixture.run_dir().join("final-status.md").is_file());
    assert!(fixture.worktree().is_dir());
    fixture
}

fn assert_quality_failure(label: &str, scenario: &str, expected: &str) -> Fixture {
    let fixture = assert_pipeline_failure(label, scenario, expected);
    let manifest = fixture.manifest();
    assert_eq!(manifest["completed_review_rounds"], 1);
    assert_eq!(manifest["normal_review_outcome"], "clean");
    assert_eq!(manifest["thermonuclear_verdict"], "clean");
    let status = fs::read_to_string(fixture.run_dir().join("final-status.md")).unwrap();
    assert!(status.contains("Normal review outcome: `clean`"));
    fixture
}

fn assert_failed_stage_event(fixture: &Fixture, stage: &str) {
    let manifest = fixture.manifest();
    let events = manifest["events"].as_array().unwrap();
    assert!(events
        .iter()
        .any(|event| event["event"] == "stage_failed" && event["stage"] == stage));
    assert!(!events
        .iter()
        .any(|event| event["event"] == "stage_completed" && event["stage"] == stage));
}

fn event_index(events: &[serde_json::Value], event: &str) -> usize {
    events
        .iter()
        .position(|entry| entry["event"] == event)
        .unwrap()
}

fn replace_arg_value(command: &mut Command, flag: &str, value: &str) {
    let program = command.get_program().to_os_string();
    let mut args = command
        .get_args()
        .map(|argument| argument.to_os_string())
        .collect::<Vec<_>>();
    let index = args.iter().position(|argument| argument == flag).unwrap();
    args[index + 1] = value.into();
    let mut replacement = Command::new(program);
    replacement.args(args);
    for (key, value) in command.get_envs() {
        match value {
            Some(value) => {
                replacement.env(key, value);
            }
            None => {
                replacement.env_remove(key);
            }
        }
    }
    *command = replacement;
}

fn git_status(repo: &std::path::Path) -> String {
    let output = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .unwrap();
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap()
}
