use std::ffi::OsStr;
use std::path::Path;
use std::sync::Arc;

use crate::manifest::{Journal, LaunchRecord, Manifest};
use crate::{atomic_write, FactoryResult};

pub(crate) fn finalize_established_path(run_dir: &Path, error: &str) {
    if let Ok(journal) = Journal::load(run_dir.join("manifest.json")) {
        finalize_failure(&Arc::new(journal), run_dir, error);
        return;
    }
    let launch = LaunchRecord::read(&run_dir.join("launch.json")).ok();
    if let Some(launch) = &launch {
        let request = run_dir.join("00-request.md");
        let recovered = Manifest::failure_skeleton(launch, request, error)
            .and_then(|manifest| Journal::create(run_dir.join("manifest.json"), manifest));
        if let Ok(journal) = recovered {
            let journal = Arc::new(journal);
            let _ = write_failure_status(&journal, run_dir, error);
            return;
        }
    }
    let _ = write_fallback_failure_status(run_dir, launch.as_ref(), error);
}

pub(crate) fn finalize_initialization_failure(
    run_dir: &Path,
    launch: &LaunchRecord,
    mut fallback: Manifest,
    error: &str,
) {
    if let Ok(journal) = Journal::load(run_dir.join("manifest.json")) {
        finalize_failure(&Arc::new(journal), run_dir, error);
        return;
    }
    let recovered = fallback
        .mark_failed(error)
        .and_then(|()| Journal::create(run_dir.join("manifest.json"), fallback));
    if let Ok(journal) = recovered {
        let journal = Arc::new(journal);
        let _ = write_failure_status(&journal, run_dir, error);
        return;
    }
    let _ = write_fallback_failure_status(run_dir, Some(launch), error);
}

pub(crate) fn finalize_failure(journal: &Arc<Journal>, run_dir: &Path, error: &str) {
    let _ = crate::controls::terminalize(run_dir, journal, || {
        let _ = journal.fail(error);
        write_failure_status(journal, run_dir, error)
    });
}

pub(crate) fn write_success_status(
    journal: &Journal,
    run_dir: &Path,
    outcome: &str,
) -> FactoryResult<()> {
    let mut manifest = journal.snapshot()?;
    manifest.status = "complete".to_string();
    manifest.outcome = Some(outcome.to_string());
    manifest.error = None;
    write_final_status(&manifest, run_dir, None)
}

fn write_failure_status(journal: &Journal, run_dir: &Path, error: &str) -> FactoryResult<()> {
    let mut manifest = journal.snapshot()?;
    manifest.status = "failed".to_string();
    manifest.outcome = None;
    manifest.error = Some(error.to_string());
    write_final_status(&manifest, run_dir, Some(error))
}

fn write_final_status(
    manifest: &Manifest,
    run_dir: &Path,
    failure: Option<&str>,
) -> FactoryResult<()> {
    let quality_log = run_dir.join("quality/make-quality.log");
    let display_status = manifest
        .outcome
        .as_deref()
        .unwrap_or(manifest.status.as_str());
    let quality = if quality_log.exists() {
        quality_log.display().to_string()
    } else {
        "not completed".to_string()
    };
    let detail = if let Some(error) = failure.or(manifest.error.as_deref()) {
        format!("Failure: {error}")
    } else {
        "The canonical quality gate passed after the final remediation stage.".to_string()
    };
    let completed_rounds = match manifest.completed_review_rounds {
        Some(value) => value.to_string(),
        None => "not completed".to_string(),
    };
    let thermo_addressed = match manifest.thermonuclear_addressed {
        Some(value) => value.to_string(),
        None => "not completed".to_string(),
    };
    let content = format!(
        "# Code factory result\n\n- Run: `{}`\n- Status: `{display_status}`\n- Repository: `{}`\n- Base: `{}`\n- Branch: `{}`\n- Worktree: `{}`\n- Artifacts: `{}`\n- Quality log: `{quality}`\n\nCompleted review rounds: {} of {}. Normal review outcome: `{}`. Thermonuclear verdict: `{}`. Thermonuclear findings addressed without re-review: `{}`.\n\n{detail}\n",
        manifest.run_id,
        manifest.repo.display(),
        manifest.base_sha,
        manifest.branch,
        manifest.worktree.display(),
        run_dir.display(),
        completed_rounds,
        manifest.maximum_review_rounds,
        manifest.normal_review_outcome.as_deref().unwrap_or("not completed"),
        manifest.thermonuclear_verdict.as_deref().unwrap_or("not completed"),
        thermo_addressed,
    );
    atomic_write(&run_dir.join("final-status.md"), content.as_bytes())
}

fn write_fallback_failure_status(
    run_dir: &Path,
    launch: Option<&LaunchRecord>,
    error: &str,
) -> FactoryResult<()> {
    let run_id = launch
        .map(|value| value.run_id.as_str())
        .or_else(|| run_dir.file_name().and_then(OsStr::to_str))
        .unwrap_or("unknown");
    let content = format!(
        "# Code factory result\n\n- Run: `{run_id}`\n- Status: `failed`\n- Artifacts: `{}`\n- Quality log: `not completed`\n\nFailure: {error}\n",
        run_dir.display()
    );
    atomic_write(&run_dir.join("final-status.md"), content.as_bytes())
}

#[cfg(test)]
#[path = "terminal/tests/mod.rs"]
mod tests;
