use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::config::allowlisted_environment;
use crate::detached;
use crate::manifest::LaunchRecord;
use crate::orchestrator::{execute_run, install_signal_handlers};
use crate::preflight::ReservedRun;
use crate::terminal::finalize_established_path;
use crate::workers::{supervise, CancelFlags, CommandSpec, OutputPlan};
use crate::{
    atomic_write, io_result, open_private_file, read_bytes, write_new_file, FactoryError,
    FactoryResult,
};

pub(crate) const LAUNCHER_TIMEOUT: Duration = Duration::from_secs(120);
const LAUNCH_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
const LAUNCH_GRACE_TIMEOUT: Duration = Duration::from_secs(1);
const LAUNCH_PID_RECEIPT_ENV: &str = "CRAB_FACTORY_LAUNCH_PID_RECEIPT";

pub(crate) fn run_foreground(reserved: ReservedRun, stdout: &mut dyn Write) -> FactoryResult<()> {
    let result = update_launch(&reserved.run_dir, |launch| {
        launch.launch_mode = Some("foreground".to_string());
        launch.launched_pid = Some(std::process::id());
    });
    if let Err(error) = result {
        finalize_established_path(&reserved.run_dir, &error.to_string());
        return Err(error);
    }
    execute_run(&reserved.run_dir, &reserved.request_sha256, stdout)
}

pub(crate) fn start_background(
    reserved: ReservedRun,
    executable: &Path,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> FactoryResult<()> {
    start_background_with_pid_recorder(reserved, executable, stdout, stderr, record_launched_pid)
}

type PidRecorder = fn(&Path, u32) -> FactoryResult<()>;

pub(crate) fn start_background_with_pid_recorder(
    reserved: ReservedRun,
    executable: &Path,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    pid_recorder: PidRecorder,
) -> FactoryResult<()> {
    start_background_with_signal_result(
        reserved,
        executable,
        stdout,
        stderr,
        pid_recorder,
        install_signal_handlers(),
        LAUNCHER_TIMEOUT,
    )
}

pub(crate) fn start_background_with_signal_result(
    reserved: ReservedRun,
    executable: &Path,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    pid_recorder: PidRecorder,
    cancellation: FactoryResult<Arc<AtomicBool>>,
    launcher_timeout: Duration,
) -> FactoryResult<()> {
    let cancellation = match cancellation {
        Ok(cancellation) => cancellation,
        Err(error) => {
            finalize_established_path(&reserved.run_dir, &error.to_string());
            return Err(error);
        }
    };
    let result = if let Some(launcher) = &reserved.launcher {
        start_with_launcher(
            &reserved,
            executable,
            launcher,
            stdout,
            stderr,
            cancellation,
            launcher_timeout,
        )
    } else {
        start_detached(&reserved, executable, stdout, stderr, pid_recorder)
    };
    if let Err(error) = &result {
        finalize_established_path(&reserved.run_dir, &error.to_string());
    }
    result
}

fn start_detached(
    reserved: &ReservedRun,
    executable: &Path,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    pid_recorder: PidRecorder,
) -> FactoryResult<()> {
    let log_path = reserved.run_dir.join("factory.log");
    let log = open_private_file(&log_path, true)?;
    update_launch(&reserved.run_dir, |launch| {
        launch.launch_mode = Some("detached".to_string());
        launch.launched_pid = None;
    })?;
    let arguments = exec_arguments(&reserved.run_dir, &reserved.request_sha256);
    let mut child = match detached::spawn(
        executable,
        &arguments,
        &allowlisted_environment(),
        &reserved.run_dir,
        &log,
    ) {
        Ok(child) => child,
        Err(error) => {
            let failure = FactoryError::new(format!("could not launch detached factory: {error}"));
            finalize_established_path(&reserved.run_dir, &failure.to_string());
            return Err(failure);
        }
    };
    let pid = child.id();
    if let Err(error) = pid_recorder(&reserved.run_dir, pid) {
        detached::kill_and_reap(&mut child);
        return Err(error);
    }
    drop(child);
    report_launch(
        print_launch(stdout, &reserved.run_dir, pid, "detached"),
        stderr,
    );
    Ok(())
}

pub(crate) fn record_launched_pid(run_dir: &Path, pid: u32) -> FactoryResult<()> {
    update_launch(run_dir, |launch| launch.launched_pid = Some(pid))
}

fn start_with_launcher(
    reserved: &ReservedRun,
    executable: &Path,
    launcher: &Path,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    cancellation: Arc<AtomicBool>,
    launcher_timeout: Duration,
) -> FactoryResult<()> {
    let launch = LaunchRecord::read(&reserved.run_dir.join("launch.json"))?;
    let factory_log = reserved.run_dir.join("factory.log");
    let mut log = open_private_file(&factory_log, true)?;
    update_launch(&reserved.run_dir, |record| {
        record.launch_mode = Some("launcher".to_string());
        record.launched_pid = None;
    })?;
    let receipt_path = reserved.run_dir.join("launcher-pid");
    write_new_file(&receipt_path, b"", 0o600)?;
    let args = std::iter::once(OsString::from(&launch.proc_name))
        .chain(std::iter::once(executable.as_os_str().to_os_string()))
        .chain(exec_arguments(&reserved.run_dir, &reserved.request_sha256))
        .collect();
    let mut environment_overrides = BTreeMap::new();
    environment_overrides.insert(
        OsString::from(LAUNCH_PID_RECEIPT_ENV),
        receipt_path.as_os_str().to_os_string(),
    );
    let result = supervise(
        CommandSpec::inherited(
            launcher.to_path_buf(),
            args,
            launcher_timeout,
            CancelFlags::global_only(cancellation),
            environment_overrides,
        ),
        OutputPlan::Capture,
    );
    let result = match result {
        Ok(result) if result.returncode == 0 => result,
        Ok(result) => {
            let detail = combined_output(&result.stdout, &result.stderr);
            let message = format!("launcher exited {}", result.returncode);
            return Err(failed_launcher(
                &reserved.run_dir,
                &receipt_path,
                &detail,
                message,
            ));
        }
        Err(error) => {
            let detail = error.detail();
            let captured = error.captured_output();
            let artifact = if captured.is_empty() {
                detail.as_bytes()
            } else {
                captured.as_slice()
            };
            return Err(failed_launcher(
                &reserved.run_dir,
                &receipt_path,
                artifact,
                format!("launcher failed: {detail}"),
            ));
        }
    };
    let launched_pid = match read_launcher_pid(&receipt_path) {
        Ok(Some(pid)) => pid,
        Ok(None) => {
            let detail = combined_output(&result.stdout, &result.stderr);
            let message = "launcher succeeded without writing the required PID receipt";
            return Err(failed_launcher(
                &reserved.run_dir,
                &receipt_path,
                &detail,
                message.to_string(),
            ));
        }
        Err(error) => {
            let detail = combined_output(&result.stdout, &result.stderr);
            return Err(failed_launcher(
                &reserved.run_dir,
                &receipt_path,
                &detail,
                error.to_string(),
            ));
        }
    };
    let log_result = io_result(
        log.write_all(&combined_output(&result.stdout, &result.stderr)),
        "write launcher output to",
        &factory_log,
    );
    #[rustfmt::skip]
    finish_launcher_recording(&reserved.run_dir, launched_pid, log_result, record_launched_pid)?;
    report_launch(
        print_launch(stdout, &reserved.run_dir, launched_pid, "launcher"),
        stderr,
    );
    Ok(())
}

pub(crate) fn finish_launcher_recording(
    run_dir: &Path,
    launched_pid: u32,
    log_result: FactoryResult<()>,
    pid_recorder: PidRecorder,
) -> FactoryResult<()> {
    if let Err(error) = log_result {
        stop_managed_process(launched_pid, LAUNCH_CLEANUP_TIMEOUT)?;
        return Err(error);
    }
    if let Err(error) = pid_recorder(run_dir, launched_pid) {
        stop_managed_process(launched_pid, LAUNCH_CLEANUP_TIMEOUT)?;
        return Err(error);
    }
    Ok(())
}

fn exec_arguments(run_dir: &Path, request_sha256: &str) -> Vec<OsString> {
    vec![
        OsString::from("exec"),
        OsString::from("--run-dir"),
        run_dir.as_os_str().to_os_string(),
        OsString::from("--request-sha256"),
        OsString::from(request_sha256),
    ]
}

pub(crate) fn combined_output(stdout: &[u8], stderr: &[u8]) -> Vec<u8> {
    let mut bytes = stdout.to_vec();
    if !bytes.is_empty() && !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(stderr);
    bytes
}

fn record_launcher_failure(run_dir: &Path, detail: &[u8]) -> FactoryResult<()> {
    atomic_write(&run_dir.join("launch-error.txt"), detail)
}

fn failed_launcher(
    run_dir: &Path,
    receipt_path: &Path,
    detail: &[u8],
    mut message: String,
) -> FactoryError {
    if let Err(error) = cleanup_receipted_process(receipt_path) {
        message.push_str(&format!("; executor cleanup failed: {error}"));
    }
    if let Err(error) = record_launcher_failure(run_dir, detail) {
        message.push_str(&format!(
            "; could not preserve launcher failure output: {error}"
        ));
    }
    FactoryError::new(message)
}

fn cleanup_receipted_process(receipt_path: &Path) -> FactoryResult<()> {
    if let Some(pid) = read_launcher_pid(receipt_path)? {
        stop_managed_process(pid, LAUNCH_CLEANUP_TIMEOUT)?;
    }
    Ok(())
}

pub(crate) fn read_launcher_pid(receipt_path: &Path) -> FactoryResult<Option<u32>> {
    let bytes = read_bytes(receipt_path, "launcher PID receipt")?;
    let value = String::from_utf8_lossy(&bytes);
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let value = value.strip_prefix("PID=").unwrap_or(value);
    match value.parse::<u32>() {
        Ok(pid) if pid > 0 => Ok(Some(pid)),
        _ => Err(FactoryError::new(format!(
            "launcher wrote an invalid PID receipt at {}",
            receipt_path.display()
        ))),
    }
}

fn stop_managed_process(pid: u32, timeout: Duration) -> FactoryResult<()> {
    let pid = pid as i32;
    if !process_exists(pid)? {
        return Ok(());
    }
    let deadline = require_some!(
        Instant::now().checked_add(timeout),
        FactoryError::new("launcher cleanup deadline overflow")
    );
    signal_managed_process(pid, libc::SIGTERM)?;
    let graceful_deadline = Instant::now()
        .checked_add(timeout.min(LAUNCH_GRACE_TIMEOUT))
        .unwrap_or(deadline)
        .min(deadline);
    if wait_for_process_exit(pid, graceful_deadline)? {
        return Ok(());
    }
    signal_managed_process(pid, libc::SIGKILL)?;
    if wait_for_process_exit(pid, deadline)? {
        Ok(())
    } else {
        Err(FactoryError::new(format!(
            "receipted executor PID {pid} did not exit within {} seconds",
            timeout.as_secs_f64()
        )))
    }
}

fn signal_managed_process(pid: i32, signal: i32) -> FactoryResult<()> {
    let _ = unsafe { libc::kill(-pid, signal) };
    let direct = unsafe { libc::kill(pid, signal) };
    classify_signal_result(direct, std::io::Error::last_os_error(), pid)
}

fn classify_signal_result(result: i32, error: std::io::Error, pid: i32) -> FactoryResult<()> {
    if result == 0 || error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(FactoryError::new(format!(
            "could not signal receipted executor PID {pid}: {error}"
        )))
    }
}

fn wait_for_process_exit(pid: i32, deadline: Instant) -> FactoryResult<bool> {
    while process_exists(pid)? {
        if Instant::now() >= deadline {
            return Ok(false);
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(true)
}

fn process_exists(pid: i32) -> FactoryResult<bool> {
    let result = unsafe { libc::kill(pid, 0) };
    classify_process_probe(result, std::io::Error::last_os_error(), pid)
}

fn classify_process_probe(result: i32, error: std::io::Error, pid: i32) -> FactoryResult<bool> {
    if result == 0 {
        return Ok(true);
    }
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(FactoryError::new(format!(
            "could not inspect receipted executor PID {pid}: {error}"
        ))),
    }
}

pub(crate) fn report_launch(result: FactoryResult<()>, stderr: &mut dyn Write) {
    if let Err(error) = result {
        let _ = writeln!(
            stderr,
            "warning: launch succeeded but status output failed: {error}"
        );
    }
}

pub(crate) fn print_launch(
    stdout: &mut dyn Write,
    run_dir: &Path,
    pid: u32,
    mode: &str,
) -> FactoryResult<()> {
    let launch = LaunchRecord::read(&run_dir.join("launch.json"))?;
    try_mapped!(writeln!(
        stdout,
        "Factory {} started ({mode})\nArtifacts: {}\nWorktree: {}\nPID: {pid}\nStop: kill -TERM {pid}",
        launch.run_id,
        run_dir.display(),
        launch.worktree.display()
    ), error => FactoryError::new(format!("could not write launch status: {error}")));
    Ok(())
}

fn update_launch(run_dir: &Path, update: impl FnOnce(&mut LaunchRecord)) -> FactoryResult<()> {
    let path = run_dir.join("launch.json");
    let mut launch = LaunchRecord::read(&path)?;
    update(&mut launch);
    launch.write(&path)
}

#[cfg(test)]
#[path = "launch/tests/mod.rs"]
mod tests;
