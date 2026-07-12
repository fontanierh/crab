use std::fs;
use std::os::unix::fs::symlink;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::thread;
use std::time::Duration;

mod support;

use support::{assert_success, read_json, wait_for, wait_for_pid_exit, Fixture};

fn control(fixture: &Fixture, arguments: &[&str]) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_crab-factory"));
    command.args(arguments);
    fixture.configure(&mut command);
    command.output().unwrap()
}

#[test]
fn live_controls_apply_at_the_next_boundary_and_remain_auditable() {
    let fixture = Fixture::new("controls", "hold-plan");
    let original_request = fs::read(&fixture.prompt).unwrap();
    let started = fixture.start_command().output().unwrap();
    assert_success(&started);
    let launch = read_json(&fixture.run_dir().join("launch.json"));
    let pid = launch["launched_pid"].as_i64().unwrap() as i32;
    wait_for(&fixture.worktree(), Duration::from_secs(10));

    let run_dir = fixture.run_dir().to_string_lossy().into_owned();
    let status = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_success(&status);
    let status: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["lifecycle"], "running");
    assert_eq!(status["effective_configuration"]["effort"], "high");

    let steer = control(
        &fixture,
        &[
            "steer",
            "--run-dir",
            &run_dir,
            "--message",
            "Prefer the smaller safe design.",
        ],
    );
    assert_success(&steer);
    let configure = control(
        &fixture,
        &[
            "configure",
            "--run-dir",
            &run_dir,
            "--effort",
            "max",
            "--plan-critics",
            "1",
        ],
    );
    assert_success(&configure);

    let pending = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_success(&pending);
    let pending: serde_json::Value = serde_json::from_slice(&pending.stdout).unwrap();
    assert_eq!(pending["controls"].as_array().unwrap().len(), 2);
    assert_eq!(pending["effective_configuration"]["effort"], "high");

    fs::write(fixture.receipts.join("release-plan"), b"release\n").unwrap();
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(45),
    );
    wait_for_pid_exit(pid, Duration::from_secs(15));

    let manifest = fixture.manifest();
    assert_eq!(manifest["status"], "complete");
    assert_eq!(manifest["prepared_configuration"]["effort"], "high");
    assert_eq!(manifest["effective_configuration"]["effort"], "max");
    assert_eq!(manifest["effective_configuration"]["plan_critics"], 1);
    let prompt =
        fs::read_to_string(fixture.run_dir().join("prompts/02-plan-critiques.md")).unwrap();
    assert!(prompt.contains("--- BEGIN OPERATOR STEERING ---"));
    assert!(prompt.contains("Prefer the smaller safe design."));
    assert!(prompt.contains("sole independent plan critic"));
    assert_eq!(
        fs::read(fixture.run_dir().join("00-request.md")).unwrap(),
        original_request
    );
    assert_eq!(
        fixture
            .receipts("codex")
            .iter()
            .filter(|receipt| {
                receipt["output"]
                    .as_str()
                    .is_some_and(|value| value.ends_with("02-plan-critiques/codex-01.md"))
            })
            .count(),
        1
    );
    assert!(manifest["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| { event["event"] == "control_applied" && event["knob"] == "steering" }));

    let rejected = control(
        &fixture,
        &["steer", "--run-dir", &run_dir, "--message", "too late"],
    );
    assert_eq!(rejected.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("run is terminal"));

    let terminal = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_success(&terminal);
    let terminal: serde_json::Value = serde_json::from_slice(&terminal.stdout).unwrap();
    assert_eq!(terminal["current_stage"], "terminal");
}

#[test]
fn configuration_changes_affect_only_future_workers_and_late_counts_are_rejected() {
    let fixture = Fixture::new("controls-next-worker", "hold-implementation");
    let started = fixture.start_command().output().unwrap();
    assert_success(&started);
    wait_for(
        &fixture
            .receipts
            .join("codex-04-address-critiques-implementation-report.md.receipt"),
        Duration::from_secs(20),
    );
    let run_dir = fixture.run_dir().to_string_lossy().into_owned();
    let effort = control(
        &fixture,
        &["configure", "--run-dir", &run_dir, "--effort", "max"],
    );
    assert_success(&effort);
    let late_count = control(
        &fixture,
        &["configure", "--run-dir", &run_dir, "--plan-critics", "4"],
    );
    assert_success(&late_count);
    assert!(String::from_utf8_lossy(&late_count.stdout).contains("already launched"));

    let pending = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_success(&pending);
    let pending: serde_json::Value = serde_json::from_slice(&pending.stdout).unwrap();
    assert_eq!(pending["current_stage"], "addressing compiled critiques");
    assert!(pending["controls"][0]["dispositions"][0]["earliest_stage"]
        .as_str()
        .unwrap()
        .contains("next prompt"));

    fs::write(
        fixture.receipts.join("release-implementation"),
        b"release\n",
    )
    .unwrap();
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(45),
    );
    let manifest = fixture.manifest();
    assert_eq!(manifest["prepared_configuration"]["effort"], "high");
    assert_eq!(manifest["effective_configuration"]["effort"], "max");
    assert_eq!(manifest["effective_configuration"]["plan_critics"], 2);
    assert!(manifest["events"].as_array().unwrap().iter().any(|event| {
        event["event"] == "control_rejected"
            && event["knob"] == "plan_critics"
            && event["reason"] == "plan-critique cohort already launched"
    }));
    let receipts = fixture.receipts("codex");
    let implementation = receipts
        .iter()
        .find(|receipt| {
            receipt["output"]
                .as_str()
                .is_some_and(|path| path.ends_with("04-address-critiques/implementation-report.md"))
        })
        .unwrap();
    assert!(implementation["argv"]
        .as_array()
        .unwrap()
        .iter()
        .any(|arg| arg.as_str().is_some_and(|arg| arg.contains("=\"high\""))));
    assert!(receipts
        .iter()
        .filter(|receipt| receipt["output"]
            .as_str()
            .is_some_and(|output| output.contains("05-review") || output.contains("06-thermo")))
        .all(|receipt| receipt["argv"]
            .as_array()
            .unwrap()
            .iter()
            .any(|arg| arg.as_str().is_some_and(|arg| arg.contains("=\"max\"")))));
}

#[test]
fn concurrent_writers_publish_unique_ordered_records() {
    let fixture = Fixture::new("controls-concurrent", "clean");
    let launcher = fixture.launcher("noop");
    let mut start = fixture.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    assert_success(&start.output().unwrap());
    let run_dir = fixture.run_dir().to_string_lossy().into_owned();
    let initializing = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_success(&initializing);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&initializing.stdout).unwrap()["current_stage"],
        "initializing"
    );

    thread::scope(|scope| {
        let mut handles = Vec::new();
        for index in 0..16 {
            let fixture = &fixture;
            let run_dir = &run_dir;
            handles.push(scope.spawn(move || {
                control(
                    fixture,
                    &[
                        "steer",
                        "--run-dir",
                        run_dir,
                        "--message",
                        &format!("message {index}"),
                    ],
                )
            }));
        }
        for handle in handles {
            assert_success(&handle.join().unwrap());
        }
    });
    let mut names = fs::read_dir(fixture.run_dir().join("controls"))
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with("-steer.json"))
        .collect::<Vec<_>>();
    names.sort();
    assert_eq!(names.len(), 16);
    assert_eq!(names.first().unwrap(), "000001-steer.json");
    assert_eq!(names.last().unwrap(), "000016-steer.json");
    for name in names {
        assert_eq!(
            fs::metadata(fixture.run_dir().join("controls").join(name))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o400
        );
    }

    let manifest_path = fixture.run_dir().join("manifest.json");
    let mut manifest = read_json(&manifest_path);
    manifest
        .as_object_mut()
        .unwrap()
        .remove("prepared_configuration");
    manifest
        .as_object_mut()
        .unwrap()
        .remove("effective_configuration");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
    let legacy = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_success(&legacy);
    assert!(
        serde_json::from_slice::<serde_json::Value>(&legacy.stdout).unwrap()["configuration_note"]
            .as_str()
            .unwrap()
            .contains("predates live controls")
    );
    let rejected = control(
        &fixture,
        &["steer", "--run-dir", &run_dir, "--message", "legacy"],
    );
    assert_eq!(rejected.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("predates live-control support"));
}

#[test]
fn authenticated_ledger_and_record_bytes_fail_closed_after_tampering() {
    let fixture = Fixture::new("controls-tamper", "hold-plan");
    assert_success(&fixture.start_command().output().unwrap());
    wait_for(&fixture.worktree(), Duration::from_secs(10));
    let run_dir = fixture.run_dir().to_string_lossy().into_owned();
    assert_success(&control(
        &fixture,
        &["steer", "--run-dir", &run_dir, "--message", "audit me"],
    ));
    fs::write(fixture.receipts.join("release-plan"), b"release\n").unwrap();
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(45),
    );

    let record = fixture.run_dir().join("controls/000001-steer.json");
    let mut bytes = fs::read(&record).unwrap();
    bytes.push(b' ');
    fs::set_permissions(&record, fs::Permissions::from_mode(0o600)).unwrap();
    fs::write(&record, bytes).unwrap();
    fs::set_permissions(&record, fs::Permissions::from_mode(0o400)).unwrap();
    let rejected = control(&fixture, &["status", "--run-dir", &run_dir, "--json"]);
    assert_eq!(rejected.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&rejected.stderr).contains("ledger"));
}

#[test]
fn malformed_controls_are_audited_and_metadata_symlinks_are_rejected_without_writes() {
    let fixture = Fixture::new("controls-invalid", "hold-plan");
    assert_success(&fixture.start_command().output().unwrap());
    wait_for(&fixture.worktree(), Duration::from_secs(10));
    let malformed = fixture.run_dir().join("controls/000001-steer.json");
    fs::write(&malformed, b"not json\n").unwrap();
    fs::set_permissions(&malformed, fs::Permissions::from_mode(0o400)).unwrap();
    fs::write(fixture.receipts.join("release-plan"), b"release\n").unwrap();
    wait_for(
        &fixture.run_dir().join("final-status.md"),
        Duration::from_secs(45),
    );
    let manifest = fixture.manifest();
    assert_eq!(manifest["status"], "failed");
    assert!(manifest["events"]
        .as_array()
        .unwrap()
        .iter()
        .any(|event| event["event"] == "control_invalid"));

    let symlinked = Fixture::new("controls-symlink", "clean");
    let launcher = symlinked.launcher("noop");
    let mut start = symlinked.start_command();
    start.args(["--launcher", launcher.to_str().unwrap()]);
    assert_success(&start.output().unwrap());
    let launch = symlinked.run_dir().join("launch.json");
    let sentinel = symlinked.root.join("launch-sentinel.json");
    fs::rename(&launch, &sentinel).unwrap();
    symlink(&sentinel, &launch).unwrap();
    let before = fs::read(&sentinel).unwrap();
    let run_dir = symlinked.run_dir().to_string_lossy().into_owned();
    let rejected = control(
        &symlinked,
        &["steer", "--run-dir", &run_dir, "--message", "blocked"],
    );
    assert_eq!(rejected.status.code(), Some(2));
    assert_eq!(fs::read(&sentinel).unwrap(), before);
    assert!(fs::read_dir(symlinked.run_dir().join("controls"))
        .unwrap()
        .all(|entry| !entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .ends_with("-steer.json")));
}
