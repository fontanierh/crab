use std::{
    env,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::mpsc::{self, TryRecvError},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use nix::{
    sys::signal::{Signal, kill},
    unistd::Pid,
};
use serde_json::json;
use uuid::Uuid;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const PROBE_COMMAND_TIMEOUT: Duration = Duration::from_secs(8);
const PROBE_COMMAND_TERMINATION_GRACE: Duration = Duration::from_millis(500);
const PROBE_COMMAND_POLL_INTERVAL: Duration = Duration::from_millis(10);
const MAX_PROBE_COMMAND_OUTPUT_BYTES: usize = 64 * 1024;

pub(crate) struct ProbeDefinition {
    pub(crate) probe_name: &'static str,
    pub(crate) adapter_name: &'static str,
    pub(crate) adapter_package: &'static str,
    pub(crate) adapter_version: &'static str,
    pub(crate) adapter_version_output: &'static str,
    pub(crate) network_endpoint: &'static str,
    pub(crate) require_non_root: bool,
}

pub(crate) struct AdapterInvocation {
    pub(crate) executable: PathBuf,
    pub(crate) arguments: Vec<OsString>,
    pub(crate) source: &'static str,
}

pub(crate) fn run(
    definition: &ProbeDefinition,
    arguments: impl Iterator<Item = OsString>,
) -> Result<serde_json::Value, String> {
    let adapter = adapter_invocation(definition, arguments)?;
    let uid = verify_uid(definition)?;
    verify_adapter_version(definition, &adapter)?;
    verify_macos_sandbox()?;
    verify_write_scope(&env::temp_dir())?;
    let home = env::var_os("HOME").ok_or_else(|| "HOME is unavailable".to_owned())?;
    verify_write_scope(Path::new(&home))?;
    verify_network(definition)?;

    Ok(json!({
        "sandboxDisabled": true,
        "permissionBypass": true,
        "unrestrictedFilesystem": true,
        "unrestrictedNetwork": true,
        "evidence": {
            "probe": definition.probe_name,
            "probeVersion": env!("CARGO_PKG_VERSION"),
            "adapterPackage": definition.adapter_package,
            "adapterVersion": definition.adapter_version,
            "adapterSource": adapter.source,
            "uid": uid,
            "sandbox": "launchctl:sandboxed=no",
            "filesystemScopes": ["home", "temporary-directory"],
            "networkEndpoint": definition.network_endpoint
        }
    }))
}

fn verify_uid(definition: &ProbeDefinition) -> Result<u32, String> {
    let mut command = Command::new("id");
    command.arg("-u");
    let output = command_output(&mut command, PROBE_COMMAND_TIMEOUT)
        .map_err(|_| "id could not run".to_owned())?;
    if !output.status.success() {
        return Err("id failed".into());
    }
    let uid = std::str::from_utf8(&output.stdout)
        .map_err(|_| "id returned non-UTF-8".to_owned())?
        .trim()
        .parse::<u32>()
        .map_err(|_| "id returned an invalid uid".to_owned())?;
    if definition.require_non_root && uid == 0 {
        return Err(format!(
            "{} unrestricted mode is unavailable at EUID 0",
            definition.adapter_name
        ));
    }
    Ok(uid)
}

pub(crate) fn adapter_invocation(
    definition: &ProbeDefinition,
    mut arguments: impl Iterator<Item = OsString>,
) -> Result<AdapterInvocation, String> {
    let Some(first) = arguments.next() else {
        return Ok(AdapterInvocation {
            executable: "npx".into(),
            arguments: ["--yes", definition.adapter_package, "--version"]
                .into_iter()
                .map(OsString::from)
                .collect(),
            source: "pinned-npx",
        });
    };
    if first != "--adapter-relative-to-probe" {
        return Err("authority probe arguments are invalid".into());
    }
    let relative = PathBuf::from(
        arguments
            .next()
            .ok_or_else(|| "authority probe adapter path is missing".to_owned())?,
    );
    if relative.is_absolute() || arguments.next().is_some() {
        return Err("authority probe adapter path must be one relative path".into());
    }
    let executable = env::current_exe()
        .map_err(|_| "authority probe executable path is unavailable".to_owned())?
        .parent()
        .ok_or_else(|| "authority probe executable directory is unavailable".to_owned())?
        .join(relative);
    Ok(AdapterInvocation {
        executable,
        arguments: vec![OsString::from("--version")],
        source: "bundle-relative",
    })
}

fn verify_adapter_version(
    definition: &ProbeDefinition,
    adapter: &AdapterInvocation,
) -> Result<(), String> {
    let mut command = Command::new(&adapter.executable);
    command.args(&adapter.arguments);
    let output = command_output(&mut command, PROBE_COMMAND_TIMEOUT).map_err(|_| {
        format!(
            "pinned {} ACP adapter could not run",
            definition.adapter_name
        )
    })?;
    if !output.status.success() {
        return Err(format!(
            "pinned {} ACP adapter failed",
            definition.adapter_name
        ));
    }
    let version = std::str::from_utf8(&output.stdout)
        .map_err(|_| format!("{} ACP adapter returned non-UTF-8", definition.adapter_name))?
        .trim();
    if version != definition.adapter_version_output {
        return Err(format!(
            "{} ACP adapter version did not match the pin",
            definition.adapter_name
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn verify_macos_sandbox() -> Result<(), String> {
    let pid = std::process::id().to_string();
    let mut command = Command::new("sudo");
    command.args(["-n", "launchctl", "procinfo", &pid]);
    let output = command_output(&mut command, PROBE_COMMAND_TIMEOUT)
        .map_err(|_| "macOS process policy could not be inspected".to_owned())?;
    if !output.status.success() {
        return Err("macOS process policy inspection failed".into());
    }
    let stdout = std::str::from_utf8(&output.stdout)
        .map_err(|_| "macOS process policy returned non-UTF-8".to_owned())?;
    if !sandbox_is_disabled(stdout) {
        return Err("macOS reports that the probe is sandboxed".into());
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn verify_macos_sandbox() -> Result<(), String> {
    Err("the first-party authority probes currently require macOS".into())
}

fn command_output(command: &mut Command, timeout: Duration) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Keep nested commands in the probe's process group so the outer host supervisor can still
    // reach every descendant if the probe itself is cancelled.
    let mut child = command.spawn()?;
    let Some(stdout) = child.stdout.take() else {
        terminate_and_reap(&mut child);
        return Err(io::Error::other("probe command stdout unavailable"));
    };
    let Some(stderr) = child.stderr.take() else {
        terminate_and_reap(&mut child);
        return Err(io::Error::other("probe command stderr unavailable"));
    };
    let stdout = spawn_output_reader(stdout);
    let stderr = spawn_output_reader(stderr);
    let deadline = Instant::now() + timeout;
    let mut status = None;
    let mut stdout_bytes = None;
    let mut stderr_bytes = None;

    loop {
        if status.is_none() {
            match child.try_wait() {
                Ok(current) => status = current,
                Err(error) => {
                    terminate_and_reap(&mut child);
                    return Err(error);
                }
            }
        }
        if let Err(error) = receive_output(&stdout, &mut stdout_bytes)
            .and_then(|()| receive_output(&stderr, &mut stderr_bytes))
        {
            terminate_and_reap(&mut child);
            return Err(error);
        }
        if status.is_some() && stdout_bytes.is_some() && stderr_bytes.is_some() {
            return Ok(Output {
                status: status.take().expect("status checked above"),
                stdout: stdout_bytes.take().expect("stdout checked above"),
                stderr: stderr_bytes.take().expect("stderr checked above"),
            });
        }
        if Instant::now() >= deadline {
            terminate_and_reap(&mut child);
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "probe command timed out",
            ));
        }
        thread::sleep(
            PROBE_COMMAND_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn spawn_output_reader<R>(reader: R) -> mpsc::Receiver<io::Result<Vec<u8>>>
where
    R: Read + Send + 'static,
{
    let (sender, receiver) = mpsc::channel();
    thread::spawn(move || {
        let _ = sender.send(read_output(reader));
    });
    receiver
}

fn receive_output(
    receiver: &mpsc::Receiver<io::Result<Vec<u8>>>,
    output: &mut Option<Vec<u8>>,
) -> io::Result<()> {
    if output.is_some() {
        return Ok(());
    }
    match receiver.try_recv() {
        Ok(result) => *output = Some(result?),
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            return Err(io::Error::other("probe output reader stopped"));
        }
    }
    Ok(())
}

fn read_output<R: Read>(reader: R) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(MAX_PROBE_COMMAND_OUTPUT_BYTES.min(8 * 1024));
    reader
        .take((MAX_PROBE_COMMAND_OUTPUT_BYTES + 1) as u64)
        .read_to_end(&mut output)?;
    if output.len() > MAX_PROBE_COMMAND_OUTPUT_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "probe command output exceeded its limit",
        ));
    }
    Ok(output)
}

fn terminate_and_reap(child: &mut Child) {
    #[cfg(unix)]
    if let Ok(pid) = i32::try_from(child.id()) {
        let _ = kill(Pid::from_raw(pid), Signal::SIGTERM);
    }
    let deadline = Instant::now() + PROBE_COMMAND_TERMINATION_GRACE;
    while matches!(child.try_wait(), Ok(None)) && Instant::now() < deadline {
        thread::sleep(PROBE_COMMAND_POLL_INTERVAL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

fn verify_write_scope(directory: &Path) -> Result<(), String> {
    let path = directory.join(format!(".crab-v2-authority-{}", Uuid::new_v4()));
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .map_err(|_| "authority file could not be created".to_owned())?;
    let result = file
        .write_all(b"crab-v2-authority\n")
        .map_err(|_| "authority file could not be written".to_owned());
    drop(file);
    let removed =
        fs::remove_file(path).map_err(|_| "authority file could not be removed".to_owned());
    result.and(removed)
}

fn verify_network(definition: &ProbeDefinition) -> Result<(), String> {
    let addresses = definition
        .network_endpoint
        .to_socket_addrs()
        .map_err(|_| format!("{} endpoint could not be resolved", definition.adapter_name))?;
    for address in addresses {
        if TcpStream::connect_timeout(&address, CONNECT_TIMEOUT).is_ok() {
            return Ok(());
        }
    }
    Err(format!(
        "{} endpoint could not be reached",
        definition.adapter_name
    ))
}

pub(crate) fn sandbox_is_disabled(output: &str) -> bool {
    output.lines().any(|line| line.trim() == "sandboxed = no")
}

#[cfg(all(test, unix))]
mod command_tests {
    use std::{fs, io, path::Path, process::Command, thread, time::Duration};

    use nix::{errno::Errno, sys::signal::kill, unistd::Pid};

    use super::command_output;

    fn assert_process_terminated(child_pid_path: &Path) {
        let child_pid = fs::read_to_string(child_pid_path)
            .expect("fixture recorded descendant pid")
            .parse::<i32>()
            .expect("descendant pid is numeric");
        let child_pid = Pid::from_raw(child_pid);
        for _ in 0..50 {
            if matches!(kill(child_pid, None), Err(Errno::ESRCH)) {
                return;
            }
            thread::sleep(Duration::from_millis(20));
        }
        panic!("probe command descendant survived cleanup");
    }

    #[test]
    fn timeout_terminates_the_nested_command() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let child_pid_path = directory.path().join("timeout.pid");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "printf '%s' \"$$\" > \"$1\"; exec /bin/sleep 60",
            "probe-timeout-test",
            &child_pid_path.to_string_lossy(),
        ]);

        let error = command_output(&mut command, Duration::from_millis(300))
            .expect_err("silent command must time out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_process_terminated(&child_pid_path);
    }

    #[test]
    fn output_overflow_terminates_stdout_and_stderr_commands() {
        let directory = tempfile::tempdir().expect("temporary directory");
        for (stream, redirect) in [("stdout", ""), ("stderr", ">&2")] {
            let child_pid_path = directory.path().join(format!("{stream}.pid"));
            let script = format!(
                "printf '%s' \"$$\" > \"$1\"; \
                 exec /bin/sh -c '/usr/bin/head -c 70000 /dev/zero {redirect}'"
            );
            let mut command = Command::new("/bin/sh");
            command.args([
                "-c",
                &script,
                "probe-output-test",
                &child_pid_path.to_string_lossy(),
            ]);

            let error = command_output(&mut command, Duration::from_secs(5))
                .expect_err("oversized output must fail closed");
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{stream}");
            assert_process_terminated(&child_pid_path);
        }
    }

    #[test]
    fn successful_command_captures_bounded_output() {
        let mut command = Command::new("/bin/sh");
        command.args(["-c", "printf ok; printf warning >&2"]);

        let output =
            command_output(&mut command, Duration::from_secs(5)).expect("bounded command succeeds");
        assert!(output.status.success());
        assert_eq!(output.stdout, b"ok");
        assert_eq!(output.stderr, b"warning");
    }
}
