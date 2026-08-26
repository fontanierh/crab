use std::{path::Path, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{fs, process::Command};
use uuid::Uuid;

use crate::AgentHostError;
use crate::{
    AuthorityAttestation, ConfiguredAgent, FilesystemAuthority, NetworkAuthority,
    PermissionAuthority, RootAuthority, SandboxAuthority,
};

const MAX_PROBE_OUTPUT_BYTES: usize = 64 * 1024;

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

        let sudo = Command::new("sudo")
            .args(["-n", "id", "-u"])
            .current_dir(&canonical)
            .output()
            .await
            .map_err(|_| AgentHostError::AuthorityUnavailable)?;
        if !sudo.status.success() || String::from_utf8_lossy(&sudo.stdout).trim() != "0" {
            return Err(AgentHostError::AuthorityUnavailable);
        }

        let probe = Command::new(&agent.authority_probe.executable)
            .args(&agent.authority_probe.arguments)
            .envs(&agent.authority_probe.environment)
            .current_dir(&canonical)
            .output()
            .await
            .map_err(|_| AgentHostError::PreflightFailed)?;
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

pub(crate) type SharedAuthorityVerifier = Arc<dyn AuthorityVerifier>;
