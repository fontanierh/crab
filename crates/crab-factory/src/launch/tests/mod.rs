use std::fs;
use std::os::unix::process::CommandExt;
use std::process::Command;

use super::*;

#[test]
fn failed_launcher_preserves_cleanup_and_artifact_errors() {
    let root =
        std::env::temp_dir().join(format!("crab-factory-launch-errors-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let receipt = root.join("receipt");
    fs::write(&receipt, "invalid").unwrap();
    fs::create_dir(root.join("launch-error.txt")).unwrap();
    let error = failed_launcher(&root, &receipt, b"detail", "launcher failed".to_string());
    assert!(error.to_string().contains("executor cleanup failed"));
    assert!(error
        .to_string()
        .contains("could not preserve launcher failure output"));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn managed_process_cleanup_escalates_and_deadlines_are_checked() {
    let mut child = Command::new("/bin/sh")
        .args(["-c", "trap '' TERM; sleep 120"])
        .process_group(0)
        .spawn()
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    assert!(stop_managed_process(child.id(), Duration::from_secs(2))
        .unwrap_err()
        .to_string()
        .contains("did not exit"));
    child.wait().unwrap();

    let mut child = Command::new("/bin/sleep")
        .arg("120")
        .process_group(0)
        .spawn()
        .unwrap();
    assert!(stop_managed_process(child.id(), Duration::MAX)
        .unwrap_err()
        .to_string()
        .contains("deadline overflow"));
    let _ = child.kill();
    let _ = child.wait();
}

#[test]
fn managed_process_cleanup_confirms_reparented_sigkill() {
    let root = std::env::temp_dir().join(format!(
        "crab-factory-launch-reparented-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let ready = root.join("pid");
    let child = r#"import os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
path = pathlib.Path(sys.argv[1])
temporary = path.with_suffix(".tmp")
temporary.write_text(str(os.getpid()), encoding="utf-8")
temporary.replace(path)
while True:
    time.sleep(60)
"#;
    let parent = r#"import subprocess, sys
subprocess.Popen([sys.executable, "-c", sys.argv[2], sys.argv[1]], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, start_new_session=True)
"#;
    let output = Command::new("python3")
        .args(["-c", parent, ready.to_str().unwrap(), child])
        .output()
        .unwrap();
    assert!(output.status.success());

    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(ready.exists());
    let pid = fs::read_to_string(&ready).unwrap().parse().unwrap();
    stop_managed_process(pid, Duration::from_secs(5)).unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn process_signal_and_probe_errors_are_classified() {
    classify_signal_result(0, std::io::Error::from_raw_os_error(libc::EPERM), 1).unwrap();
    classify_signal_result(-1, std::io::Error::from_raw_os_error(libc::ESRCH), 1).unwrap();
    assert!(classify_signal_result(-1, std::io::Error::from_raw_os_error(libc::EPERM), 1).is_err());
    assert!(classify_process_probe(0, std::io::Error::from_raw_os_error(libc::EPERM), 1).unwrap());
    assert!(classify_process_probe(-1, std::io::Error::from_raw_os_error(libc::EPERM), 1).unwrap());
    assert!(classify_process_probe(-1, std::io::Error::from_raw_os_error(libc::EIO), 1).is_err());
}
