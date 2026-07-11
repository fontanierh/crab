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
    create_secure_dir, io_result, read_bytes, required_utf8, sha256_hex, FactoryError,
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
    let run_dir = io_result(
        fs::canonicalize(run_dir),
        "resolve prepared run directory",
        run_dir,
    )?;
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
    validate_prepared_metadata(&launch, &snapshot, marker)?;
    if snapshot.status != "initializing" {
        return Err(FactoryError::new(format!(
            "run cannot execute from manifest status {}",
            snapshot.status
        )));
    }
    if request_sha256 != launch.request_sha256 || request_sha256 != snapshot.request_sha256 {
        return Err(FactoryError::new(
            "request SHA-256 does not match prepared run metadata",
        ));
    }
    let request_path = run_dir.join("00-request.md");
    if snapshot.request != request_path {
        return Err(FactoryError::new(
            "manifest request path is not the managed run snapshot",
        ));
    }
    let request_bytes = read_bytes(&request_path, "managed request snapshot")?;
    let actual_hash = sha256_hex(&request_bytes);
    if actual_hash != request_sha256 {
        return Err(FactoryError::new(format!(
            "managed request snapshot hash mismatch: expected {request_sha256}, found {actual_hash}"
        )));
    }
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
    let mut pipeline = Pipeline::new(
        run_dir.to_path_buf(),
        request,
        snapshot.base_sha,
        snapshot.branch,
        snapshot.worktree,
        snapshot.tool_paths,
        Duration::from_secs(snapshot.agent_timeout_seconds),
        snapshot.maximum_review_rounds,
        Arc::clone(journal),
        git,
        cancellation,
        stdout,
    );
    let outcome = pipeline.execute()?;
    drop(pipeline);
    finish_successful_run(journal, run_dir, stdout, &outcome)
}

fn validate_prepared_metadata(
    launch: &LaunchRecord,
    manifest: &crate::manifest::Manifest,
    marker: &RunMarker,
) -> FactoryResult<()> {
    let consistent = launch.run_id == marker.run_id
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
        && launch.agent_timeout_seconds == manifest.agent_timeout_seconds;
    if consistent {
        Ok(())
    } else {
        Err(FactoryError::new(
            "prepared run metadata is inconsistent across marker, launch record, and manifest",
        ))
    }
}

fn finish_successful_run(
    journal: &Journal,
    run_dir: &Path,
    stdout: &mut dyn Write,
    outcome: &str,
) -> FactoryResult<()> {
    write_progress(stdout, &format!("Factory complete with outcome {outcome}"))?;
    write_success_status(journal, run_dir, outcome)?;
    journal.complete(outcome)
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
