use std::{io, path::Path, process::Output, process::Stdio, sync::Arc, time::Duration};

use async_trait::async_trait;
#[cfg(unix)]
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{fs, io::AsyncReadExt, process::Command};
use uuid::Uuid;

use crate::AgentHostError;
use crate::{
    AuthorityAttestation, ConfiguredAgent, FilesystemAuthority, NetworkAuthority,
    PermissionAuthority, RootAuthority, SandboxAuthority,
};

const MAX_PROBE_OUTPUT_BYTES: usize = 64 * 1024;
const AUTHORITY_COMMAND_TIMEOUT: Duration = Duration::from_secs(10);
const COMMAND_TERMINATION_GRACE: Duration = Duration::from_secs(1);

/// Injectable boundary for mandatory host-authority verification.
#[async_trait]
pub trait AuthorityVerifier: Send + Sync {
    /// Verify the complete authority contract for one launch directory.
    async fn verify(
        &self,
        agent: &ConfiguredAgent,
        working_directory: &Path,
        now_ms: u64,
    ) -> Result<AuthorityAttestation, AgentHostError>;
}

/// Production verifier: real directory access, real `sudo -n`, and an agent-specific policy probe.
#[derive(Debug, Default)]
pub struct SystemAuthorityVerifier;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AgentProbeOutput {
    sandbox_disabled: bool,
    permission_bypass: bool,
    unrestricted_filesystem: bool,
    unrestricted_network: bool,
    evidence: Value,
}

#[async_trait]
impl AuthorityVerifier for SystemAuthorityVerifier {
    async fn verify(
        &self,
        agent: &ConfiguredAgent,
        working_directory: &Path,
        now_ms: u64,
    ) -> Result<AuthorityAttestation, AgentHostError> {
        let canonical = fs::canonicalize(working_directory)
            .await
            .map_err(|_| AgentHostError::PreflightFailed)?;
        let metadata = fs::metadata(&canonical)
            .await
            .map_err(|_| AgentHostError::PreflightFailed)?;
        if !canonical.is_absolute() || !metadata.is_dir() {
            return Err(AgentHostError::PreflightFailed);
        }

        let write_probe = canonical.join(format!(".crab-authority-{}", Uuid::new_v4()));
        let writable = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&write_probe)
            .await
            .map_err(|_| AgentHostError::AuthorityUnavailable)?;
        drop(writable);
        fs::remove_file(&write_probe)
            .await
            .map_err(|_| AgentHostError::AuthorityUnavailable)?;

        let mut sudo_command = Command::new("sudo");
        sudo_command
            .args(["-n", "id", "-u"])
            .current_dir(&canonical);
        let sudo = command_output(sudo_command, AUTHORITY_COMMAND_TIMEOUT)
            .await
            .map_err(|_| AgentHostError::AuthorityUnavailable)?;
        if !sudo.status.success() || String::from_utf8_lossy(&sudo.stdout).trim() != "0" {
            return Err(AgentHostError::AuthorityUnavailable);
        }

        let mut probe_command = Command::new(&agent.authority_probe.executable);
        probe_command
            .args(&agent.authority_probe.arguments)
            .envs(&agent.authority_probe.environment)
            .current_dir(&canonical);
        let probe = command_output(probe_command, AUTHORITY_COMMAND_TIMEOUT)
            .await
            .map_err(|error| {
                if error.kind() == io::ErrorKind::TimedOut {
                    AgentHostError::AuthorityUnavailable
                } else {
                    AgentHostError::PreflightFailed
                }
            })?;
        if !probe.status.success() || probe.stdout.len() > MAX_PROBE_OUTPUT_BYTES {
            return Err(AgentHostError::PreflightFailed);
        }
        let report: AgentProbeOutput =
            serde_json::from_slice(&probe.stdout).map_err(|_| AgentHostError::PreflightFailed)?;
        if !(report.sandbox_disabled
            && report.permission_bypass
            && report.unrestricted_filesystem
            && report.unrestricted_network)
        {
            return Err(AgentHostError::AuthorityUnavailable);
        }

        let evidence_json = serde_json::to_string(&json!({
            "agentProbe": report.evidence,
            "agentProbeExitCode": probe.status.code(),
            "passwordlessSudo": { "exitCode": sudo.status.code(), "uid": 0 },
            "workingDirectory": canonical,
        }))
        .map_err(|_| AgentHostError::PreflightFailed)?;

        Ok(AuthorityAttestation {
            sandbox: SandboxAuthority::DisabledAndVerified,
            permissions: PermissionAuthority::YoloAndVerified,
            filesystem: FilesystemAuthority::UnrestrictedAndVerified,
            network: NetworkAuthority::UnrestrictedAndVerified,
            root: RootAuthority::PasswordlessSudoAndVerified,
            verified_at_ms: now_ms,
            evidence_json,
        })
    }
}

async fn command_output(mut command: Command, timeout: Duration) -> io::Result<Output> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command.spawn()?;
    #[cfg(unix)]
    let mut process_group = ProcessGroupGuard::new(child.id());
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| io::Error::other("authority command stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| io::Error::other("authority command stderr unavailable"))?;
    let result = tokio::time::timeout(timeout, async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read_output(stdout), read_output(stderr))?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;

    let error = match result {
        Ok(Ok(output)) => {
            #[cfg(unix)]
            process_group.disarm();
            return Ok(output);
        }
        Ok(Err(error)) => error,
        Err(_) => io::Error::new(io::ErrorKind::TimedOut, "authority command timed out"),
    };

    #[cfg(unix)]
    process_group.terminate(Signal::SIGTERM);
    let reaped = tokio::time::timeout(COMMAND_TERMINATION_GRACE, child.wait()).await;
    #[cfg(unix)]
    process_group.terminate(Signal::SIGKILL);
    if reaped.is_err() {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
    #[cfg(unix)]
    process_group.disarm();
    Err(error)
}

async fn read_output<R>(mut reader: R) -> io::Result<Vec<u8>>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut output = Vec::new();
    reader.read_to_end(&mut output).await?;
    Ok(output)
}

#[cfg(unix)]
struct ProcessGroupGuard {
    process_group: Option<Pid>,
}

#[cfg(unix)]
impl ProcessGroupGuard {
    fn new(process_id: Option<u32>) -> Self {
        Self {
            process_group: process_id
                .and_then(|process_id| i32::try_from(process_id).ok())
                .map(Pid::from_raw),
        }
    }

    fn terminate(&self, signal: Signal) {
        if let Some(process_group) = self.process_group {
            let _ = killpg(process_group, signal);
        }
    }

    fn disarm(&mut self) {
        self.process_group = None;
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupGuard {
    fn drop(&mut self) {
        self.terminate(Signal::SIGKILL);
    }
}

#[cfg(all(test, unix))]
mod tests {
    use std::{io, time::Duration};

    use nix::{errno::Errno, sys::signal::kill, unistd::Pid};
    use tokio::process::Command;

    use super::command_output;

    #[tokio::test]
    async fn timed_out_command_terminates_its_descendant_process_group() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let child_pid_path = directory.path().join("child.pid");
        let mut command = Command::new("/bin/sh");
        command.args([
            "-c",
            "/bin/sleep 60 & child=$!; printf '%s' \"$child\" > \"$1\"; exit 0",
            "authority-timeout-test",
            &child_pid_path.to_string_lossy(),
        ]);

        let error = command_output(command, Duration::from_millis(300))
            .await
            .expect_err("silent command must time out");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);

        let child_pid = tokio::fs::read_to_string(&child_pid_path)
            .await
            .expect("fixture recorded descendant pid")
            .parse::<i32>()
            .expect("descendant pid is numeric");
        let child_pid = Pid::from_raw(child_pid);
        let mut terminated = false;
        for _ in 0..50 {
            if matches!(kill(child_pid, None), Err(Errno::ESRCH)) {
                terminated = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(terminated, "timed-out descendant process survived cleanup");
    }
}

pub(crate) type SharedAuthorityVerifier = Arc<dyn AuthorityVerifier>;
