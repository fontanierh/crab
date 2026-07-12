use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use crate::config::{
    canonicalize_intended, default_run_id, paths_overlap, proc_name_for, resolve_executable,
    resolve_roots, sanitize_run_id, validate_cohort_size, validate_counts, LaunchOptions,
    ToolPaths, ToolVersions, DEFAULT_CODEX_REVIEWERS, DEFAULT_EFFORT, DEFAULT_PLAN_CRITICS,
};
use crate::gitops::GitRunner;
use crate::manifest::{Journal, LaunchRecord, Manifest};
use crate::run_lock::RunLock;
use crate::terminal::finalize_initialization_failure;
use crate::workers::{supervise, CancelFlags, CommandSpec, OutputPlan};
use crate::{
    create_exclusive_dir, create_secure_dir, io_result, read_bytes, required_utf8,
    set_secure_dir_permissions, sha256_hex, utc_now_rfc3339, write_new_file, FactoryError,
    FactoryResult,
};

const PREFLIGHT_TIMEOUT: Duration = Duration::from_secs(60);
// Executables used directly by Makefile quality targets and their shell scripts;
// `git` and `make` are already resolved and version-probed as primary tools.
// Shell builtins such as `cd`, `echo`, `printf`, and `command` need no PATH probe.
const QUALITY_GATE_EXECUTABLES: &[&str] = &[
    "cargo", "rg", "npx", "bash", "python3", "find", "sort", "grep", "sed", "dirname", "mkdir",
    "pwd",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestedMode {
    Run,
    Start,
}

impl RequestedMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Run => "run",
            Self::Start => "start",
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReservedRun {
    pub(crate) run_dir: PathBuf,
    pub(crate) request_sha256: String,
    pub(crate) launcher: Option<PathBuf>,
}

#[derive(Debug)]
struct ValidatedRun {
    launch: LaunchRecord,
    request: Vec<u8>,
    tool_paths: ToolPaths,
    tool_versions: ToolVersions,
    maximum_review_rounds: u32,
    run_dir: PathBuf,
}

pub(crate) fn prepare_run(
    options: LaunchOptions,
    mode: RequestedMode,
) -> FactoryResult<ReservedRun> {
    reserve(validate_without_writes(options, mode)?)
}

fn validate_without_writes(
    options: LaunchOptions,
    mode: RequestedMode,
) -> FactoryResult<ValidatedRun> {
    #[rustfmt::skip]
    let maximum_review_rounds = validate_counts(options.additional_review_rounds, options.agent_timeout_seconds)?;
    let effort = options.effort.unwrap_or(DEFAULT_EFFORT);
    let plan_critics = validate_cohort_size(
        "--plan-critics",
        options.plan_critics.unwrap_or(DEFAULT_PLAN_CRITICS),
    )?;
    let codex_reviewers = validate_cohort_size(
        "--codex-reviewers",
        options.codex_reviewers.unwrap_or(DEFAULT_CODEX_REVIEWERS),
    )?;
    if options.base_ref.starts_with('-') {
        return Err(FactoryError::new("--base must not begin with '-'"));
    }
    #[rustfmt::skip]
    let cwd = io_result(std::env::current_dir(), "read current directory at", Path::new("."))?;
    let source_prompt = canonicalize_intended(&anchored(&options.prompt_file, &cwd))?;
    let request = read_bytes(&source_prompt, "prompt file")?;
    required_utf8(&request, "prompt file")?;
    let request_sha256 = sha256_hex(&request);
    let raw_run_id = match options.run_id.as_deref() {
        Some(run_id) => run_id.to_string(),
        None => default_run_id(&request)?,
    };
    let run_id = sanitize_run_id(&raw_run_id)?;

    let global_cancel = Arc::new(AtomicBool::new(false));
    let git_path = resolve_executable(OsStr::new("git"))?;
    let git = GitRunner::new(
        git_path.clone(),
        Arc::clone(&global_cancel),
        PREFLIGHT_TIMEOUT,
    );
    let repo_input = anchored(&options.repo, &cwd);
    #[rustfmt::skip]
    let repo = try_mapped!(git.discover_toplevel(&repo_input), _error => FactoryError::new("--repo is not inside a git work tree"));
    let tool_paths = ToolPaths {
        git: git_path,
        claude: resolve_executable(OsStr::new("claude"))?,
        codex: resolve_executable(OsStr::new("codex"))?,
        make: resolve_executable(OsStr::new("make"))?,
    };
    let tool_versions = probe_primary_tools(&tool_paths, Arc::clone(&global_cancel))?;
    probe_quality_tools(Arc::clone(&global_cancel))?;

    let base_sha = git.resolve_commit(&repo, &options.base_ref)?;
    let source_was_dirty = !git.source_status(&repo)?.is_empty();
    if source_was_dirty && !options.allow_dirty_source {
        return Err(FactoryError::new(
            "source checkout is dirty; commit or stash it, or pass --allow-dirty-source",
        ));
    }
    #[rustfmt::skip]
    let roots = resolve_roots(options.artifact_root.as_deref(), options.worktree_root.as_deref(), &cwd)?;
    let run_dir = roots.artifact_root.join(&run_id);
    let worktree = roots.worktree_root.join(&run_id);
    validate_destinations(&repo, &run_dir, &worktree)?;
    if run_dir.exists() {
        return Err(FactoryError::new(format!(
            "run artifact directory already exists: {}",
            run_dir.display()
        )));
    }
    if worktree.exists() {
        return Err(FactoryError::new(format!(
            "worktree destination already exists: {}",
            worktree.display()
        )));
    }
    let branch = format!("factory/{run_id}");
    if git.branch_exists(&repo, &branch)? {
        return Err(FactoryError::new(format!(
            "factory branch already exists: {branch}"
        )));
    }
    let launcher = options
        .launcher
        .as_deref()
        .map(|path| resolve_executable(path.as_os_str()))
        .transpose()?;
    let launch = LaunchRecord {
        run_id: run_id.clone(),
        mode: mode.as_str().to_string(),
        queued_at: utc_now_rfc3339()?,
        source_prompt,
        request_sha256,
        repo,
        base_ref: options.base_ref,
        base_sha,
        source_was_dirty,
        allow_dirty_source: options.allow_dirty_source,
        additional_review_rounds: options.additional_review_rounds,
        agent_timeout_seconds: options.agent_timeout_seconds,
        artifact_root: roots.artifact_root,
        worktree_root: roots.worktree_root,
        worktree,
        branch,
        launch_mode: None,
        launched_pid: None,
        proc_name: proc_name_for(&run_id),
        launcher,
        effort: Some(effort),
        plan_critics: Some(plan_critics),
        codex_reviewers: Some(codex_reviewers),
        tool_paths: Some(tool_paths.clone()),
    };
    Ok(ValidatedRun {
        launch,
        request,
        tool_paths,
        tool_versions,
        maximum_review_rounds,
        run_dir,
    })
}

fn anchored(path: &Path, cwd: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        cwd.join(path)
    }
}

fn validate_destinations(repo: &Path, run_dir: &Path, worktree: &Path) -> FactoryResult<()> {
    if run_dir.starts_with(repo) {
        return Err(FactoryError::new(format!(
            "artifact destination must be outside the source repository: {}",
            run_dir.display()
        )));
    }
    if worktree.starts_with(repo) {
        return Err(FactoryError::new(format!(
            "worktree destination must be outside the source repository: {}",
            worktree.display()
        )));
    }
    if paths_overlap(run_dir, worktree) {
        return Err(FactoryError::new(
            "run artifact and worktree destinations must not be equal or nested",
        ));
    }
    Ok(())
}

fn probe_primary_tools(
    tools: &ToolPaths,
    cancellation: Arc<AtomicBool>,
) -> FactoryResult<ToolVersions> {
    Ok(ToolVersions {
        git: probe_version(&tools.git, &["--version"], Arc::clone(&cancellation))?,
        claude: probe_version(&tools.claude, &["--version"], Arc::clone(&cancellation))?,
        codex: probe_version(&tools.codex, &["--version"], Arc::clone(&cancellation))?,
        make_tool: probe_version(&tools.make, &["--version"], cancellation)?,
    })
}

fn probe_quality_tools(cancellation: Arc<AtomicBool>) -> FactoryResult<()> {
    for tool in QUALITY_GATE_EXECUTABLES {
        #[rustfmt::skip]
        try_mapped!(resolve_executable(OsStr::new(tool)), error => FactoryError::new(format!("quality-gate prerequisite {tool} is unavailable: {error}")));
    }
    let cargo = resolve_executable(OsStr::new("cargo"))?;
    probe_version(&cargo, &["llvm-cov", "--version"], cancellation).map(|_| ())
}

fn probe_version(
    program: &Path,
    arguments: &[&str],
    cancellation: Arc<AtomicBool>,
) -> FactoryResult<String> {
    let spec = CommandSpec::isolated(
        program.to_path_buf(),
        arguments.iter().map(OsString::from).collect(),
        None,
        None,
        PREFLIGHT_TIMEOUT,
        CancelFlags::global_only(cancellation),
    );
    #[rustfmt::skip]
    let result = try_mapped!(supervise(spec, OutputPlan::Capture), error => FactoryError::new(format!("tool probe failed for {}: {}", program.display(), error.detail())));
    if result.returncode != 0 {
        return Err(FactoryError::new(format!(
            "tool probe failed for {} with exit {}",
            program.display(),
            result.returncode
        )));
    }
    let bytes = if result.stdout.is_empty() {
        result.stderr
    } else {
        result.stdout
    };
    let version = String::from_utf8_lossy(&bytes).trim().to_string();
    if version.is_empty() {
        return Err(FactoryError::new(format!(
            "tool probe returned no version for {}",
            program.display()
        )));
    }
    Ok(version)
}

fn reserve(validated: ValidatedRun) -> FactoryResult<ReservedRun> {
    reserve_with_journal_creator(validated, Journal::create)
}

type JournalCreator = fn(PathBuf, Manifest) -> FactoryResult<Journal>;

fn reserve_with_journal_creator(
    validated: ValidatedRun,
    create_journal: JournalCreator,
) -> FactoryResult<ReservedRun> {
    let snapshot_path = validated.run_dir.join("00-request.md");
    #[rustfmt::skip]
    let manifest = Manifest::initial(&validated.launch, validated.maximum_review_rounds, snapshot_path.clone(), validated.tool_paths, validated.tool_versions)?;
    create_secure_dir(&validated.launch.artifact_root)?;
    create_secure_dir(&validated.launch.worktree_root)?;
    create_exclusive_dir(&validated.run_dir)?;
    let mut guard = ReservationTerminalGuard::new(
        validated.run_dir.clone(),
        validated.launch.clone(),
        manifest.clone(),
    );
    let result = (|| {
        set_secure_dir_permissions(&validated.run_dir)?;
        #[rustfmt::skip]
        let _journal = create_journal(validated.run_dir.join("manifest.json"), manifest).map_err(|error| error.context("could not establish run manifest"))?;
        #[rustfmt::skip]
        RunLock::initialize(&validated.run_dir, &validated.launch.run_id, &validated.launch.request_sha256)?;
        #[rustfmt::skip]
        crate::controls::initialize(&validated.run_dir, &validated.launch.run_id, &validated.launch.request_sha256)?;
        create_secure_dir(&validated.run_dir.join("prompts"))?;
        create_secure_dir(&validated.run_dir.join("logs"))?;
        write_new_file(&snapshot_path, &validated.request, 0o400)?;
        validated
            .launch
            .write(&validated.run_dir.join("launch.json"))
    })();
    if let Err(error) = result {
        guard.finalize(&error.to_string());
        return Err(error);
    }
    guard.disarm();
    Ok(ReservedRun {
        run_dir: validated.run_dir,
        request_sha256: validated.launch.request_sha256,
        launcher: validated.launch.launcher,
    })
}

struct ReservationTerminalGuard {
    run_dir: PathBuf,
    launch: LaunchRecord,
    manifest: Manifest,
    armed: bool,
}

impl ReservationTerminalGuard {
    fn new(run_dir: PathBuf, launch: LaunchRecord, manifest: Manifest) -> Self {
        Self {
            run_dir,
            launch,
            manifest,
            armed: true,
        }
    }

    fn finalize(&mut self, error: &str) {
        finalize_initialization_failure(&self.run_dir, &self.launch, self.manifest.clone(), error);
        self.armed = false;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for ReservationTerminalGuard {
    fn drop(&mut self) {
        if self.armed {
            finalize_initialization_failure(
                &self.run_dir,
                &self.launch,
                self.manifest.clone(),
                "run reservation ended before initialization completed",
            );
        }
    }
}

#[cfg(test)]
#[path = "preflight/tests/mod.rs"]
mod tests;
