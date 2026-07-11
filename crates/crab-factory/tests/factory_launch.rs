use std::fs;
use std::process::Command;
use std::time::{Duration, Instant};

mod support;

use support::{assert_failure, assert_success, read_json, wait_for, wait_for_pid_exit, Fixture};

#[test]
fn detached_start_records_provenance_finishes_and_leaves_no_executor() {
    let fixture = Fixture::new("background", "clean");
    let started = Instant::now();
    let output = fixture.start_command().output().unwrap();
    assert_success(&output);
    assert!(started.elapsed() < Duration::from_secs(15));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("Stop: kill -TERM"));
    let launch = read_json(&fixture.run_dir().join("launch.json"));
    assert_eq!(launch["launch_mode"], "detached");
    let pid = launch["launched_pid"].as_i64().unwrap() as i32;
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(45),
    );
    wait_for_pid_exit(pid, Duration::from_secs(15));
    assert_eq!(fixture.manifest()["status"], "complete");
    assert!(!fs::read(fixture.run_dir().join("factory.log"))
        .unwrap()
        .is_empty());

    let mut duplicate = Command::new(env!("CARGO_BIN_EXE_crab-factory"));
    duplicate.args([
        "exec",
        "--run-dir",
        fixture.run_dir().to_str().unwrap(),
        "--request-sha256",
        launch["request_sha256"].as_str().unwrap(),
    ]);
    fixture.configure(&mut duplicate);
    let output = duplicate.output().unwrap();
    assert_failure(&output, "manifest status complete");
    assert_eq!(fixture.manifest()["status"], "complete");
}

#[test]
fn launcher_receives_an_argument_vector_and_can_report_the_detached_pid() {
    let mut fixture = Fixture::new("launcher", "clean");
    fixture.run_id = "a".repeat(64);
    let launcher = fixture.launcher("success");
    let mut command = fixture.start_command();
    command.args(["--launcher", launcher.to_str().unwrap()]);
    let output = command.output().unwrap();
    assert_success(&output);
    let launch = read_json(&fixture.run_dir().join("launch.json"));
    assert_eq!(launch["launch_mode"], "launcher");
    let pid = launch["launched_pid"].as_i64().unwrap() as i32;
    assert_eq!(
        fs::read_to_string(fixture.run_dir().join("launcher-pid"))
            .unwrap()
            .trim(),
        format!("PID={pid}")
    );
    let proc_name = launch["proc_name"].as_str().unwrap();
    assert!(proc_name.starts_with("code-factory-"));
    assert_eq!(proc_name.len(), 64);
    let receipt = read_json(&fixture.receipts.join("launcher-success.json"));
    let argv = receipt["argv"].as_array().unwrap();
    assert_eq!(argv[0], proc_name);
    assert!(argv[1].as_str().unwrap().ends_with("crab-factory"));
    assert_eq!(argv[2], "exec");
    assert_eq!(argv[3], "--run-dir");
    assert_eq!(
        argv[4],
        fs::canonicalize(fixture.run_dir())
            .unwrap()
            .to_str()
            .unwrap()
    );
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(45),
    );
    wait_for_pid_exit(pid, Duration::from_secs(15));
    assert_eq!(fixture.manifest()["status"], "complete");
}

#[test]
fn active_executor_rejects_duplicate_exec_and_signal_cancellation_finalizes() {
    let fixture = Fixture::new("active-lock", "slow-plan");
    let output = fixture.start_command().output().unwrap();
    assert_success(&output);
    let launch = read_json(&fixture.run_dir().join("launch.json"));
    let pid = launch["launched_pid"].as_i64().unwrap() as i32;
    wait_for(&fixture.worktree(), Duration::from_secs(10));

    let mut duplicate = Command::new(env!("CARGO_BIN_EXE_crab-factory"));
    duplicate.args([
        "exec",
        "--run-dir",
        fixture.run_dir().to_str().unwrap(),
        "--request-sha256",
        launch["request_sha256"].as_str().unwrap(),
    ]);
    fixture.configure(&mut duplicate);
    let duplicate = duplicate.output().unwrap();
    assert_failure(&duplicate, "already executing or has an active executor");

    assert_eq!(unsafe { libc::kill(pid, libc::SIGTERM) }, 0);
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(10),
    );
    wait_for_pid_exit(pid, Duration::from_secs(10));
    let manifest = fixture.manifest();
    assert_eq!(manifest["status"], "failed");
    assert!(manifest["error"].as_str().unwrap().contains("interrupted"));
    assert!(manifest["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event"] == "interrupted"));
}
