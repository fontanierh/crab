use std::fs;
use std::io;
use std::io::ErrorKind;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::ExitStatusExt;
use std::sync::atomic::AtomicU64;

use crate::config::LaunchOptions;
use crate::factory_test_support::Fixture;
use crate::manifest::Journal;
use crate::preflight::{prepare_run, RequestedMode};

use super::*;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn root(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "crab-factory-workers-{label}-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path
}

fn shell_spec(script: &str, timeout: Duration, cancel: CancelFlags) -> CommandSpec {
    CommandSpec {
        program: PathBuf::from("/bin/sh"),
        args: vec!["-c".into(), script.into()],
        cwd: None,
        input: Some(Arc::new(b"input".to_vec())),
        timeout,
        cancellation: cancel,
        inherit_environment: false,
        environment_overrides: std::collections::BTreeMap::new(),
        spawn_observer: None,
    }
}

fn no_cancel() -> CancelFlags {
    CancelFlags::global_only(Arc::new(AtomicBool::new(false)))
}

fn process_is_gone(pid: i32) -> bool {
    for _ in 0..50 {
        let result = unsafe { libc::kill(pid, 0) };
        if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}

#[test]
fn capture_feeds_stdin_and_collects_both_streams() {
    let result = supervise(
        shell_spec(
            "read value; printf '%s' \"$value\"; printf err >&2",
            Duration::from_secs(2),
            no_cancel(),
        ),
        OutputPlan::Capture,
    )
    .unwrap();
    assert_eq!(result.returncode, 0);
    assert_eq!(result.stdout, b"input");
    assert_eq!(result.stderr, b"err");
    assert!(result.elapsed < Duration::from_secs(2));

    let mut closes_stdin = shell_spec("exec 0<&-; sleep 0.05", Duration::from_secs(2), no_cancel());
    closes_stdin.input = Some(Arc::new(vec![b'x'; 1_000_000]));
    assert_eq!(
        supervise(closes_stdin, OutputPlan::Capture)
            .unwrap()
            .returncode,
        0
    );
}

#[test]
fn capture_is_bounded_and_timeout_errors_keep_partial_diagnostics() {
    let overflow = supervise(
        shell_spec(
            "exec /usr/bin/yes noisy",
            Duration::from_secs(2),
            no_cancel(),
        ),
        OutputPlan::Capture,
    )
    .unwrap_err();
    assert_eq!(overflow.kind, SupervisorErrorKind::Other);
    assert!(overflow
        .detail()
        .contains("exceeded the 1048576-byte limit"));
    assert!(overflow.stdout.len() <= MAX_CAPTURE_BYTES);
    assert!(overflow.detail().len() < ERROR_EXCERPT_BYTES + 200);

    let timeout = supervise(
        shell_spec(
            "printf before-timeout; printf stderr-note >&2; sleep 30",
            // Instrumented process startup can exceed a few dozen milliseconds under load.
            Duration::from_secs(1),
            no_cancel(),
        ),
        OutputPlan::Capture,
    )
    .unwrap_err();
    assert_eq!(timeout.kind, SupervisorErrorKind::TimedOut);
    let captured = String::from_utf8(timeout.captured_output()).unwrap();
    assert!(captured.contains("before-timeout"));
    assert!(captured.contains("stderr-note"));
    assert_eq!(combined_excerpt(b" \n", b"\t"), "");
}

#[test]
fn timeout_and_post_exit_sweep_kill_grandchildren() {
    let root = root("groups");
    let child_path = root.join("child.pid");
    let script = format!("sleep 30 & echo $! > '{}'; sleep 30", child_path.display());
    let error = supervise(
        shell_spec(&script, Duration::from_millis(120), no_cancel()),
        OutputPlan::Capture,
    )
    .unwrap_err();
    assert_eq!(error.kind, SupervisorErrorKind::TimedOut);
    assert!(process_is_gone(-error.process_group.unwrap()));

    let child_path = root.join("orphan.pid");
    let script = format!("sleep 30 & echo $! > '{}'; exit 0", child_path.display());
    supervise(
        shell_spec(&script, Duration::from_secs(2), no_cancel()),
        OutputPlan::Capture,
    )
    .unwrap();
    let child_pid: i32 = fs::read_to_string(child_path)
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    assert!(process_is_gone(child_pid));
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn cancellation_kills_the_process_group() {
    let flag = Arc::new(AtomicBool::new(false));
    let setter = Arc::clone(&flag);
    let thread = thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(60));
        setter.store(true, Ordering::SeqCst);
    });
    let error = supervise(
        shell_spec(
            "sleep 30",
            Duration::from_secs(2),
            CancelFlags::global_only(flag),
        ),
        OutputPlan::Capture,
    )
    .unwrap_err();
    thread.join().unwrap();
    assert_eq!(error.kind, SupervisorErrorKind::Cancelled);
    assert!(process_is_gone(-error.process_group.unwrap()));
}

#[test]
fn deadline_overflow_and_pre_cancel_are_reported() {
    assert_eq!(poll_interval(0), Duration::from_millis(1));
    assert_eq!(poll_interval(STARTUP_POLL_LIMIT - 1), STARTUP_POLL_INTERVAL);
    assert_eq!(poll_interval(STARTUP_POLL_LIMIT), MAX_POLL_INTERVAL);

    let mut spec = shell_spec("exit 0", Duration::MAX, no_cancel());
    let overflow = supervise(spec, OutputPlan::Capture).unwrap_err();
    assert_eq!(overflow.kind, SupervisorErrorKind::Other);
    assert_eq!(overflow.detail(), "process deadline overflow");
    let cancelled = Arc::new(AtomicBool::new(true));
    spec = shell_spec(
        "exit 0",
        Duration::from_secs(1),
        CancelFlags::global_only(cancelled),
    );
    let cancelled = supervise(spec, OutputPlan::Capture).unwrap_err();
    assert_eq!(cancelled.kind, SupervisorErrorKind::Cancelled);
    assert!(cancelled
        .detail()
        .contains("process cancelled before spawn"));
    assert!(
        SupervisorError::new(SupervisorErrorKind::Other, "with group", Some(42))
            .detail()
            .contains("process group 42")
    );
    assert!(supervise(
        CommandSpec {
            program: PathBuf::from("/definitely/missing/factory-worker"),
            args: Vec::new(),
            cwd: None,
            input: None,
            timeout: Duration::from_secs(1),
            cancellation: no_cancel(),
            inherit_environment: true,
            environment_overrides: std::collections::BTreeMap::new(),
            spawn_observer: None,
        },
        OutputPlan::Capture,
    )
    .is_err());

    struct FakeChild {
        poll_error: bool,
        wait_error: bool,
    }
    impl ChildWait for FakeChild {
        fn try_wait_status(&mut self) -> io::Result<Option<ExitStatus>> {
            if self.poll_error {
                Err(io::Error::other("poll failure"))
            } else {
                Ok(None)
            }
        }

        fn wait_status(&mut self) -> io::Result<ExitStatus> {
            if self.wait_error {
                Err(io::Error::other("wait failure"))
            } else {
                Ok(ExitStatus::from_raw(0))
            }
        }
    }
    let deadline = Instant::now() + Duration::from_secs(1);
    let (status, termination) = wait_for_child(
        &mut FakeChild {
            poll_error: true,
            wait_error: false,
        },
        Path::new("fake"),
        i32::MAX,
        deadline,
        Duration::from_secs(1),
        &no_cancel(),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(status.success());
    assert!(termination.is_some());
    assert!(wait_for_child(
        &mut FakeChild {
            poll_error: true,
            wait_error: true,
        },
        Path::new("fake"),
        i32::MAX,
        deadline,
        Duration::from_secs(1),
        &no_cancel(),
        &AtomicBool::new(false),
    )
    .is_err());
    assert!(wait_for_child(
        &mut FakeChild {
            poll_error: false,
            wait_error: true,
        },
        Path::new("fake"),
        i32::MAX,
        deadline,
        Duration::from_secs(1),
        &CancelFlags::global_only(Arc::new(AtomicBool::new(true))),
        &AtomicBool::new(false),
    )
    .is_err());
}

#[test]
fn thread_join_helpers_report_io_errors_and_panics() {
    assert!(join_writer(Some(thread::spawn(|| {
        Err(io::Error::other("writer failure"))
    })))
    .is_some());
    assert!(join_writer(Some(thread::spawn(|| -> io::Result<()> {
        panic!("writer panic")
    })))
    .is_some());
    assert!(join_reader(
        Some(thread::spawn(|| Err(io::Error::other("reader failure")))),
        "stdout",
        7,
    )
    .is_err());
    assert!(join_reader(
        Some(thread::spawn(|| -> io::Result<Vec<u8>> {
            panic!("reader panic")
        })),
        "stderr",
        7,
    )
    .is_err());

    struct FailingWriter(ErrorKind);
    impl Write for FailingWriter {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(self.0, "write failure"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    assert!(write_process_input(&mut FailingWriter(ErrorKind::BrokenPipe), b"x").is_ok());
    assert!(write_process_input(&mut FailingWriter(ErrorKind::Other), b"x").is_err());

    let mut termination = None;
    record_late_capture_overflow(&mut termination, true, 7);
    assert!(termination.is_some());
    record_late_capture_overflow(&mut termination, true, 7);
    assert!(finish_supervision(
        termination,
        None,
        None,
        0,
        b"out".to_vec(),
        b"err".to_vec(),
        Duration::ZERO,
        7,
    )
    .is_err());
    assert!(finish_supervision(
        None,
        Some(io::Error::other("sweep")),
        None,
        0,
        Vec::new(),
        Vec::new(),
        Duration::ZERO,
        7,
    )
    .is_err());
    assert!(finish_supervision(
        None,
        None,
        Some(io::Error::other("writer")),
        0,
        Vec::new(),
        Vec::new(),
        Duration::ZERO,
        7,
    )
    .is_err());
    assert!(classify_group_kill(0, io::Error::other("unused")).is_ok());
    assert!(classify_group_kill(-1, io::Error::from_raw_os_error(libc::ESRCH),).is_ok());
    assert!(classify_group_kill(-1, io::Error::from_raw_os_error(libc::EPERM)).is_err());

    let mut cohort_error = None;
    record_cohort_join(&mut cohort_error, Err(Box::new("panic")));
    assert!(cohort_error.is_some());
    record_cohort_join(&mut cohort_error, Err(Box::new("second panic")));
    assert!(order_cohort_outputs(&["missing".to_string()], BTreeMap::new()).is_err());
}

#[path = "agent_tests.rs"]
mod agent_tests;

#[test]
fn worker_arguments_pin_models_effort_full_permissions_and_nested_agent_boundaries() {
    let codex = codex_arguments(Path::new("/tmp/out"), DEFAULT_EFFORT);
    assert_eq!(
        codex,
        [
            "exec",
            "--model",
            CODEX_MODEL,
            "--config",
            "model_reasoning_effort=\"high\"",
            "--dangerously-bypass-approvals-and-sandbox",
            "--disable",
            "multi_agent",
            "--ephemeral",
            "--color",
            "never",
            "--output-last-message",
            "/tmp/out",
            "-",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>()
    );
    assert!(codex_arguments(Path::new("/tmp/out"), Effort::Max)
        .contains(&OsString::from("model_reasoning_effort=\"max\"")));

    let claude_strings: Vec<String> = claude_arguments(DEFAULT_EFFORT)
        .iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect();
    assert_eq!(
        claude_strings,
        [
            "--print",
            "--model",
            CLAUDE_MODEL,
            "--effort",
            "high",
            "--no-session-persistence",
            "--disable-slash-commands",
            "--dangerously-skip-permissions",
            "--tools",
            "default",
            "--disallowedTools",
            "Agent",
            "--output-format",
            "text",
        ]
    );
    assert!(claude_arguments(Effort::Max)
        .windows(2)
        .any(|pair| pair == [OsString::from("--effort"), OsString::from("max")]));
}

#[test]
fn file_output_plan_uses_private_artifacts() {
    let root = root("files");
    let stdout_path = root.join("stdout");
    let stderr_path = root.join("stderr");
    let result = supervise(
        shell_spec(
            "printf out; printf err >&2",
            Duration::from_secs(2),
            no_cancel(),
        ),
        OutputPlan::Files {
            stdout: open_private_file(&stdout_path, false).unwrap(),
            stderr: open_private_file(&stderr_path, false).unwrap(),
        },
    )
    .unwrap();
    assert_eq!(result.returncode, 0);
    assert!(result.stdout.is_empty());
    assert_eq!(fs::read(stdout_path).unwrap(), b"out");
    assert_eq!(
        fs::metadata(stderr_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    fs::remove_dir_all(root).unwrap();
}
