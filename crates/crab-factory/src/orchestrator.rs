use std::ffi::OsStr;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use crate::gitops::{assert_identity, GitRunner};
use crate::manifest::{Journal, LaunchRecord};
use crate::pipeline::{write_progress, Pipeline};
use crate::rubric;
use crate::run_lock::{RunLock, RunLockError, RunMarker};
use crate::terminal::{finalize_established_path, finalize_failure, write_success_status};
use crate::{
    create_secure_dir, io_result, read_managed_bytes, required_utf8, sha256_hex, FactoryError,
    FactoryResult,
};

pub(crate) fn execute_run(
    run_dir: &Path,
    request_sha256: &str,
    stdout: &mut dyn Write,
) -> FactoryResult<()> {
    execute_run_with_installer(run_dir, request_sha256, stdout, default_signal_installer)
}

fn execute_run_with_installer(
    run_dir: &Path,
    request_sha256: &str,
    stdout: &mut dyn Write,
    installer: SignalInstaller,
) -> FactoryResult<()> {
    execute_run_with_dependencies(run_dir, request_sha256, stdout, installer, RunLock::acquire)
}

type LockAcquirer = fn(&Path) -> Result<RunLock, RunLockError>;

fn execute_run_with_dependencies(
    run_dir: &Path,
    request_sha256: &str,
    stdout: &mut dyn Write,
    installer: SignalInstaller,
    acquire_lock: LockAcquirer,
) -> FactoryResult<()> {
    let run_dir = crate::controls::canonical_run_dir(run_dir)?;
    // Loading the reservation-time marker is the non-mutating trust boundary.
    // Nothing in an arbitrary directory is created or terminalized unless this
    // typed marker proves that crab-factory reserved it.
    let marker = RunLock::marker(&run_dir)?;
    if run_dir.file_name().and_then(OsStr::to_str) != Some(marker.run_id.as_str()) {
        let error = FactoryError::new("prepared-run marker does not match the run-directory name");
        finalize_established_path(&run_dir, &error.to_string());
        return Err(error);
    }
    let _lock = match acquire_lock(&run_dir) {
        Ok(lock) => lock,
        Err(RunLockError::Busy) => {
            return Err(FactoryError::new(
                "run is already executing or has an active executor",
            ));
        }
        Err(RunLockError::Other(error)) => {
            finalize_established_path(&run_dir, &error.to_string());
            return Err(error);
        }
    };
    let journal = match Journal::load(run_dir.join("manifest.json")) {
        Ok(journal) => Arc::new(journal),
        Err(error) => {
            finalize_established_path(&run_dir, &error.to_string());
            return Err(error);
        }
    };
    let result = install_signal_handlers_with(installer).and_then(|cancellation| {
        execute_locked(
            &run_dir,
            request_sha256,
            stdout,
            &journal,
            cancellation,
            &marker,
        )
    });
    finalize_execution_result(&journal, &run_dir, result)
}

fn finalize_execution_result(
    journal: &Arc<Journal>,
    run_dir: &Path,
    result: FactoryResult<()>,
) -> FactoryResult<()> {
    if let Err(error) = &result {
        finalize_execution_error(journal, run_dir, error, journal.snapshot());
    }
    result
}

fn finalize_execution_error(
    journal: &Arc<Journal>,
    run_dir: &Path,
    error: &FactoryError,
    snapshot: FactoryResult<crate::manifest::Manifest>,
) {
    if let Ok(snapshot) = snapshot {
        if !matches!(snapshot.status.as_str(), "initializing" | "running") {
            return;
        }
        if snapshot.status == "running" {
            let cancel = error.to_string().contains("interrupted")
                || error.to_string().contains("cancelled");
            if cancel {
                let _ = journal.event("interrupted", json!({"error": error.to_string()}));
            }
        }
        finalize_failure(journal, run_dir, &error.to_string());
    } else {
        finalize_established_path(run_dir, &error.to_string());
    }
}

fn execute_locked(
    run_dir: &Path,
    request_sha256: &str,
    stdout: &mut dyn Write,
    journal: &Arc<Journal>,
    cancellation: Arc<AtomicBool>,
    marker: &RunMarker,
) -> FactoryResult<()> {
    let launch = LaunchRecord::read(&run_dir.join("launch.json"))?;
    let snapshot = journal.snapshot()?;
    validate_prepared_metadata(run_dir, &launch, &snapshot, marker)?;
    if snapshot.status != "initializing" {
        return Err(FactoryError::new(format!(
            "run cannot execute from manifest status {}",
            snapshot.status
        )));
    }
    if request_sha256 != launch.request_sha256 {
        return Err(FactoryError::new(
            "request SHA-256 does not match prepared run metadata",
        ));
    }
    if let Err(error) = crate::controls::authenticate_prepared_controls(run_dir) {
        let _ = journal.event_once("control_invalid", json!({"error": error.to_string()}));
        return Err(error);
    }
    let request_path = run_dir.join("00-request.md");
    let request_bytes = read_managed_bytes(&request_path, "managed request snapshot", 0o400)?;
    let request = required_utf8(&request_bytes, "managed request snapshot")?;
    journal.set_running()?;
    rubric::verify()?;
    let git = GitRunner::new(
        snapshot.tool_paths.git.clone(),
        Arc::clone(&cancellation),
        Duration::from_secs(snapshot.agent_timeout_seconds),
    );
    git.resolve_commit(&snapshot.repo, &snapshot.base_sha)?;
    if snapshot.worktree.exists() {
        return Err(FactoryError::new(format!(
            "worktree destination appeared after preparation: {}",
            snapshot.worktree.display()
        )));
    }
    let worktree_parent = require_some!(
        snapshot.worktree.parent(),
        FactoryError::new("prepared worktree has no parent directory")
    );
    create_secure_dir(worktree_parent)?;
    #[rustfmt::skip]
    git.add_worktree(&snapshot.repo, &snapshot.branch, &snapshot.worktree, &snapshot.base_sha)?;
    journal.event(
        "worktree_created",
        json!({"worktree": snapshot.worktree, "branch": snapshot.branch}),
    )?;
    #[rustfmt::skip]
    assert_identity(&git, &snapshot.worktree, &snapshot.base_sha, &snapshot.branch, "worktree creation")?;
    let configuration = require_some!(
        snapshot.effective_configuration.as_ref(),
        FactoryError::new("run predates live-control support")
    );
    let mut pipeline = Pipeline::new(
        run_dir.to_path_buf(),
        request,
        snapshot.base_sha,
        snapshot.branch,
        snapshot.worktree,
        snapshot.tool_paths,
        Duration::from_secs(snapshot.agent_timeout_seconds),
        snapshot.maximum_review_rounds,
        configuration.effort,
        configuration.plan_critics,
        configuration.codex_reviewers,
        Arc::clone(journal),
        git,
        cancellation,
        stdout,
        crate::controls::ControlPlane::new(run_dir.to_path_buf(), Arc::clone(journal)),
    );
    let outcome = pipeline.execute()?;
    drop(pipeline);
    finish_successful_run(journal, run_dir, stdout, &outcome)
}

pub(crate) fn validate_prepared_metadata(
    run_dir: &Path,
    launch: &LaunchRecord,
    manifest: &crate::manifest::Manifest,
    marker: &RunMarker,
) -> FactoryResult<()> {
    let prepared = require_some!(
        manifest.prepared_configuration.as_ref(),
        FactoryError::new("run predates live-control support")
    );
    let launch_configuration = (
        require_some!(
            launch.effort,
            FactoryError::new("run predates live-control support")
        ),
        require_some!(
            launch.plan_critics,
            FactoryError::new("run predates live-control support")
        ),
        require_some!(
            launch.codex_reviewers,
            FactoryError::new("run predates live-control support")
        ),
    );
    let maximum_review_rounds = launch.additional_review_rounds.checked_add(1);
    let artifact_root = io_result(
        fs::canonicalize(&launch.artifact_root),
        "canonicalize prepared artifact root",
        &launch.artifact_root,
    )?;
    let worktree_root = io_result(
        fs::canonicalize(&launch.worktree_root),
        "canonicalize prepared worktree root",
        &launch.worktree_root,
    )?;
    let request_path = run_dir.join("00-request.md");
    let request_bytes = read_managed_bytes(&request_path, "managed request snapshot", 0o400)?;
    let actual_request_sha256 = sha256_hex(&request_bytes);
    if actual_request_sha256 != marker.request_sha256 {
        return Err(FactoryError::new(format!(
            "managed request snapshot hash mismatch: expected {}, found {actual_request_sha256}",
            marker.request_sha256
        )));
    }
    let effective = require_some!(
        manifest.effective_configuration.as_ref(),
        FactoryError::new("run predates live-control support")
    );
    let launch_tools = require_some!(
        launch.tool_paths.as_ref(),
        FactoryError::new("run predates live-control support")
    );
    let consistent = manifest.schema_version == 1
        && launch.run_id == marker.run_id
        && run_dir.file_name().and_then(|name| name.to_str()) == Some(marker.run_id.as_str())
        && run_dir == artifact_root.join(&marker.run_id)
        && run_dir.parent() == Some(artifact_root.as_path())
        && launch.worktree == worktree_root.join(&marker.run_id)
        && launch.branch == format!("factory/{}", marker.run_id)
        && launch.proc_name == crate::config::proc_name_for(&marker.run_id)
        && manifest.run_id == marker.run_id
        && launch.request_sha256 == marker.request_sha256
        && manifest.request_sha256 == marker.request_sha256
        && launch.repo == manifest.repo
        && launch.base_ref == manifest.base_ref
        && launch.base_sha == manifest.base_sha
        && launch.source_was_dirty == manifest.source_was_dirty
        && launch.branch == manifest.branch
        && launch.worktree == manifest.worktree
        && launch.additional_review_rounds == manifest.additional_review_rounds
        && launch.agent_timeout_seconds == manifest.agent_timeout_seconds
        && manifest.request == request_path
        && manifest.worker_policy.host_permissions == crate::config::WORKER_HOST_PERMISSIONS
        && manifest.worker_policy.sandbox == crate::config::WORKER_SANDBOX
        && manifest.worker_policy.network_access == crate::config::WORKER_NETWORK_ACCESS
        && manifest.worker_policy.nested_agents_enabled == crate::config::NESTED_AGENTS_ENABLED
        && manifest.thermonuclear_skill.path == crate::rubric::THERMO_SKILL_MANIFEST_PATH
        && manifest.thermonuclear_skill.sha256 == crate::rubric::THERMO_SKILL_SHA256
        && manifest.thermonuclear_skill.source == crate::rubric::THERMO_SKILL_SOURCE
        && manifest.thermonuclear_skill.source_commit == crate::rubric::THERMO_SKILL_COMMIT
        && launch_tools == &manifest.tool_paths
        && !manifest.tool_versions.git.is_empty()
        && !manifest.tool_versions.claude.is_empty()
        && !manifest.tool_versions.codex.is_empty()
        && !manifest.tool_versions.make_tool.is_empty();
    if !consistent
        || launch_configuration
            != (
                prepared.effort,
                prepared.plan_critics,
                prepared.codex_reviewers,
            )
        || maximum_review_rounds != Some(manifest.maximum_review_rounds)
        || manifest.models.claude.model != crate::config::CLAUDE_MODEL
        || manifest.models.codex.model != crate::config::CODEX_MODEL
        || manifest.models.claude.effort != manifest.models.codex.reasoning_effort
        || manifest.models.claude.effort != effective.effort.as_str()
        || !manifest.tool_paths.git.is_absolute()
        || !manifest.tool_paths.claude.is_absolute()
        || !manifest.tool_paths.codex.is_absolute()
        || !manifest.tool_paths.make.is_absolute()
    {
        return Err(FactoryError::new(
            "prepared run metadata is inconsistent across marker, launch record, and manifest",
        ));
    }
    if crate::config::validate_cohort_size("--plan-critics", prepared.plan_critics).is_err()
        || crate::config::validate_cohort_size("--codex-reviewers", prepared.codex_reviewers)
            .is_err()
    {
        return Err(FactoryError::new(
            "prepared run configuration is out of bounds",
        ));
    }
    if crate::config::validate_cohort_size("--plan-critics", effective.plan_critics).is_err()
        || crate::config::validate_cohort_size("--codex-reviewers", effective.codex_reviewers)
            .is_err()
    {
        return Err(FactoryError::new(
            "effective run configuration is out of bounds",
        ));
    }
    Ok(())
}

fn finish_successful_run(
    journal: &Journal,
    run_dir: &Path,
    stdout: &mut dyn Write,
    outcome: &str,
) -> FactoryResult<()> {
    write_progress(stdout, &format!("Factory complete with outcome {outcome}"))?;
    crate::controls::terminalize(run_dir, journal, || {
        write_success_status(journal, run_dir, outcome)?;
        journal.complete(outcome)
    })
}

type SignalInstaller = fn(Arc<AtomicBool>) -> Result<(), String>;

pub(crate) fn install_signal_handlers() -> FactoryResult<Arc<AtomicBool>> {
    install_signal_handlers_with(default_signal_installer)
}

fn default_signal_installer(flag: Arc<AtomicBool>) -> Result<(), String> {
    register_signal(Arc::clone(&flag), signal_hook::consts::SIGINT, "SIGINT")?;
    register_signal(flag, signal_hook::consts::SIGTERM, "SIGTERM")?;
    Ok(())
}

fn register_signal(flag: Arc<AtomicBool>, signal: i32, label: &str) -> Result<(), String> {
    match signal_hook::flag::register(signal, flag) {
        Ok(_) => Ok(()),
        Err(error) => Err(format!("could not register {label} handler: {error}")),
    }
}

fn install_signal_handlers_with(installer: SignalInstaller) -> FactoryResult<Arc<AtomicBool>> {
    let flag = Arc::new(AtomicBool::new(false));
    try_mapped!(installer(Arc::clone(&flag)), error => FactoryError::new(error));
    Ok(flag)
}

#[cfg(test)]
#[path = "orchestrator/tests/mod.rs"]
mod tests;
