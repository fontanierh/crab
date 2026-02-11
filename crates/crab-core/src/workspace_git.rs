use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};

use crate::config::WorkspaceGitConfig;
use crate::{CrabError, CrabResult};

const WORKSPACE_GIT_CONTEXT: &str = "workspace_git_bootstrap";
const WORKSPACE_GIT_COMMIT_CONTEXT: &str = "workspace_git_commit";
const WORKSPACE_GIT_PUSH_QUEUE_CONTEXT: &str = "workspace_git_push_queue";
const WORKSPACE_GIT_COMMIT_VERSION: &str = "1";
const WORKSPACE_GIT_STAGING_POLICY_VERSION: &str = "1";
const WORKSPACE_GIT_COMMIT_KEY_TRAILER: &str = "Crab-Commit-Key";
const WORKSPACE_GIT_PUSH_QUEUE_FILE_NAME: &str = "workspace_git_push_queue.json";
const WORKSPACE_GIT_PUSH_QUEUE_VERSION: u8 = 1;
const WORKSPACE_GIT_PUSH_MAX_ATTEMPTS: u32 = 8;
const WORKSPACE_GIT_PUSH_BACKOFF_BASE_MS: u64 = 1_000;
const WORKSPACE_GIT_PUSH_BACKOFF_MAX_MS: u64 = 300_000;
const GIT_BINARY: &str = "git";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceGitEnsureOutcome {
    pub enabled: bool,
    pub repository_initialized: bool,
    pub branch_bootstrapped: bool,
    pub remote_bound: bool,
    pub repository_root: Option<String>,
}

impl WorkspaceGitEnsureOutcome {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            repository_initialized: false,
            branch_bootstrapped: false,
            remote_bound: false,
            repository_root: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceGitCommitTrigger {
    RunFinalized,
    RotationCheckpoint,
}

impl WorkspaceGitCommitTrigger {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::RunFinalized => "run_finalized",
            Self::RotationCheckpoint => "rotation_checkpoint",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceGitCommitRequest {
    pub logical_session_id: String,
    pub run_id: String,
    pub trigger: WorkspaceGitCommitTrigger,
    pub checkpoint_id: Option<String>,
    pub run_status: Option<String>,
    pub emitted_at_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceGitCommitOutcome {
    pub enabled: bool,
    pub trigger: Option<WorkspaceGitCommitTrigger>,
    pub committed: bool,
    pub commit_key: Option<String>,
    pub commit_id: Option<String>,
    pub skipped_reason: Option<String>,
    pub staging_skipped_paths: Vec<String>,
}

impl WorkspaceGitCommitOutcome {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            trigger: None,
            committed: false,
            commit_key: None,
            commit_id: None,
            skipped_reason: None,
            staging_skipped_paths: Vec::new(),
        }
    }

    fn skipped(
        trigger: WorkspaceGitCommitTrigger,
        commit_key: String,
        skipped_reason: &'static str,
        commit_id: Option<String>,
        staging_skipped_paths: Vec<String>,
    ) -> Self {
        Self {
            enabled: true,
            trigger: Some(trigger),
            committed: false,
            commit_key: Some(commit_key),
            commit_id,
            skipped_reason: Some(skipped_reason.to_string()),
            staging_skipped_paths,
        }
    }

    fn committed(
        trigger: WorkspaceGitCommitTrigger,
        commit_key: String,
        commit_id: String,
        staging_skipped_paths: Vec<String>,
    ) -> Self {
        Self {
            enabled: true,
            trigger: Some(trigger),
            committed: true,
            commit_key: Some(commit_key),
            commit_id: Some(commit_id),
            skipped_reason: None,
            staging_skipped_paths,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct StagingPolicyAudit {
    skipped_paths: Vec<String>,
    skipped_rules: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceGitPushRequest {
    pub commit_key: String,
    pub commit_id: String,
    pub enqueued_at_epoch_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceGitPushEnqueueOutcome {
    pub enabled: bool,
    pub enqueued: bool,
    pub queue_depth: usize,
    pub skipped_reason: Option<String>,
}

impl WorkspaceGitPushEnqueueOutcome {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            enqueued: false,
            queue_depth: 0,
            skipped_reason: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceGitPushTickOutcome {
    pub enabled: bool,
    pub attempted: bool,
    pub pushed: bool,
    pub exhausted: bool,
    pub commit_key: Option<String>,
    pub queue_depth: usize,
    pub next_due_at_epoch_ms: Option<u64>,
    pub next_backoff_ms: Option<u64>,
    pub failure: Option<String>,
    pub failure_kind: Option<String>,
    pub recovery_commands: Vec<String>,
    pub skipped_reason: Option<String>,
}

impl WorkspaceGitPushTickOutcome {
    #[must_use]
    pub const fn disabled() -> Self {
        Self {
            enabled: false,
            attempted: false,
            pushed: false,
            exhausted: false,
            commit_key: None,
            queue_depth: 0,
            next_due_at_epoch_ms: None,
            next_backoff_ms: None,
            failure: None,
            failure_kind: None,
            recovery_commands: Vec::new(),
            skipped_reason: None,
        }
    }

    fn skipped(
        skipped_reason: &'static str,
        queue_depth: usize,
        next_due_at_epoch_ms: Option<u64>,
    ) -> Self {
        Self {
            enabled: true,
            attempted: false,
            pushed: false,
            exhausted: false,
            commit_key: None,
            queue_depth,
            next_due_at_epoch_ms,
            next_backoff_ms: None,
            failure: None,
            failure_kind: None,
            recovery_commands: Vec::new(),
            skipped_reason: Some(skipped_reason.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkspaceGitPushQueueState {
    version: u8,
    entries: Vec<WorkspaceGitPushQueueEntry>,
}

impl Default for WorkspaceGitPushQueueState {
    fn default() -> Self {
        Self {
            version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
            entries: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct WorkspaceGitPushQueueEntry {
    commit_key: String,
    commit_id: String,
    enqueued_at_epoch_ms: u64,
    next_attempt_at_epoch_ms: u64,
    attempt_count: u32,
    exhausted: bool,
    last_error: Option<String>,
}

pub fn ensure_workspace_git_repository(
    workspace_root: &Path,
    config: &WorkspaceGitConfig,
) -> CrabResult<WorkspaceGitEnsureOutcome> {
    validate_workspace_root(workspace_root)?;
    if !config.enabled {
        return Ok(WorkspaceGitEnsureOutcome::disabled());
    }

    let expected_root = canonicalize_path(workspace_root)?;
    if probe_bare_repository(workspace_root) {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_CONTEXT,
            message: "workspace git persistence requires a non-bare repository".to_string(),
        });
    }
    if let Some(existing_root) = detect_git_repository_root(workspace_root)? {
        ensure_same_repository_root(&expected_root, &existing_root)?;
    }

    let mut outcome = WorkspaceGitEnsureOutcome {
        enabled: true,
        repository_initialized: false,
        branch_bootstrapped: false,
        remote_bound: false,
        repository_root: None,
    };

    if !workspace_root.join(".git").exists() {
        run_git_checked(workspace_root, &["init"])?;
        outcome.repository_initialized = true;
    }

    let repository_root = require_git_repository_root(workspace_root)?;
    ensure_same_repository_root(&expected_root, &repository_root)?;

    let current_branch = resolve_head_branch(workspace_root)?;
    if current_branch != config.branch {
        if has_commits(workspace_root) {
            return Err(CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                message: format!(
                    "existing repository branch is {current_branch:?} but CRAB_WORKSPACE_GIT_BRANCH requires {:?}",
                    config.branch
                ),
            });
        }
        let target_ref = format!("refs/heads/{}", config.branch);
        run_git_checked(workspace_root, &["symbolic-ref", "HEAD", &target_ref])?;
        outcome.branch_bootstrapped = true;
    }

    if let Some(expected_remote) = config.remote.as_deref() {
        match read_origin_remote(workspace_root)? {
            Some(existing_remote) if existing_remote == expected_remote => {}
            Some(existing_remote) => {
                return Err(CrabError::InvariantViolation {
                    context: WORKSPACE_GIT_CONTEXT,
                    message: format!(
                        "existing origin remote is {existing_remote:?} but CRAB_WORKSPACE_GIT_REMOTE requires {:?}",
                        expected_remote
                    ),
                });
            }
            None => {
                let args = ["remote", "add", "origin", expected_remote];
                run_git_checked(workspace_root, &args)?;
                outcome.remote_bound = true;
            }
        }
    }

    outcome.repository_root = Some(display_path(&repository_root));
    Ok(outcome)
}

pub fn maybe_commit_workspace_snapshot(
    workspace_root: &Path,
    config: &WorkspaceGitConfig,
    request: &WorkspaceGitCommitRequest,
) -> CrabResult<WorkspaceGitCommitOutcome> {
    validate_workspace_root(workspace_root)?;
    validate_commit_request(request)?;
    if !config.enabled {
        return Ok(WorkspaceGitCommitOutcome::disabled());
    }

    let expected_root = canonicalize_path(workspace_root)?;
    let repository_root = require_git_repository_root(workspace_root)?;
    ensure_same_repository_root(&expected_root, &repository_root)?;

    let commit_key = workspace_commit_key(request);
    let head_has_commit_key = head_commit_contains_key(workspace_root, &commit_key)?;

    let staging_audit = stage_workspace_changes(workspace_root)?;
    let has_changes = workspace_has_changes(workspace_root)?;
    if !has_changes {
        if head_has_commit_key {
            return Ok(WorkspaceGitCommitOutcome::skipped(
                request.trigger,
                commit_key,
                "already_committed",
                Some(current_head_commit_id(workspace_root)?),
                staging_audit.skipped_paths,
            ));
        }

        return Ok(WorkspaceGitCommitOutcome::skipped(
            request.trigger,
            commit_key,
            "no_changes",
            None,
            staging_audit.skipped_paths,
        ));
    }

    if head_has_commit_key {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_COMMIT_CONTEXT,
            message: format!(
                "HEAD already contains commit key {commit_key:?} but workspace has staged changes; refusing duplicate-key commit"
            ),
        });
    }

    commit_workspace_changes(workspace_root, config, request, &commit_key, &staging_audit)?;
    let commit_id = current_head_commit_id(workspace_root)?;

    Ok(WorkspaceGitCommitOutcome::committed(
        request.trigger,
        commit_key,
        commit_id,
        staging_audit.skipped_paths,
    ))
}

pub fn enqueue_workspace_git_push_request(
    state_root: &Path,
    config: &WorkspaceGitConfig,
    request: &WorkspaceGitPushRequest,
) -> CrabResult<WorkspaceGitPushEnqueueOutcome> {
    if !config.enabled || config.push_policy != crate::WorkspaceGitPushPolicy::OnCommit {
        return Ok(WorkspaceGitPushEnqueueOutcome::disabled());
    }

    validate_push_request(request)?;
    ensure_push_queue_layout(state_root)?;
    let queue_path = workspace_git_push_queue_path(state_root);
    let mut queue = read_workspace_git_push_queue(&queue_path)?;

    if let Some(existing) = queue
        .entries
        .iter()
        .find(|entry| entry.commit_key == request.commit_key)
    {
        if existing.commit_id != request.commit_id {
            return Err(CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: format!(
                    "commit key {:?} already queued with commit id {:?}, received {:?}",
                    request.commit_key, existing.commit_id, request.commit_id
                ),
            });
        }

        return Ok(WorkspaceGitPushEnqueueOutcome {
            enabled: true,
            enqueued: false,
            queue_depth: queue.entries.len(),
            skipped_reason: Some("already_queued".to_string()),
        });
    }

    queue.entries.push(WorkspaceGitPushQueueEntry {
        commit_key: request.commit_key.clone(),
        commit_id: request.commit_id.clone(),
        enqueued_at_epoch_ms: request.enqueued_at_epoch_ms,
        next_attempt_at_epoch_ms: request.enqueued_at_epoch_ms,
        attempt_count: 0,
        exhausted: false,
        last_error: None,
    });
    persist_workspace_git_push_queue(&queue_path, &queue)?;

    Ok(WorkspaceGitPushEnqueueOutcome {
        enabled: true,
        enqueued: true,
        queue_depth: queue.entries.len(),
        skipped_reason: None,
    })
}

pub fn process_workspace_git_push_queue(
    workspace_root: &Path,
    state_root: &Path,
    config: &WorkspaceGitConfig,
    now_epoch_ms: u64,
) -> CrabResult<WorkspaceGitPushTickOutcome> {
    validate_workspace_root(workspace_root)?;
    if !config.enabled || config.push_policy != crate::WorkspaceGitPushPolicy::OnCommit {
        return Ok(WorkspaceGitPushTickOutcome::disabled());
    }
    if now_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "now_epoch_ms must be greater than 0".to_string(),
        });
    }

    ensure_push_queue_layout(state_root)?;
    let queue_path = workspace_git_push_queue_path(state_root);
    let mut queue = read_workspace_git_push_queue(&queue_path)?;
    if queue.entries.is_empty() {
        return Ok(WorkspaceGitPushTickOutcome::skipped("queue_empty", 0, None));
    }

    let mut due_index: Option<usize> = None;
    let mut next_due_at: Option<u64> = None;
    for (index, entry) in queue.entries.iter().enumerate() {
        if entry.exhausted {
            continue;
        }
        if entry.next_attempt_at_epoch_ms <= now_epoch_ms {
            due_index = Some(index);
            break;
        }
        next_due_at = Some(
            next_due_at.map_or(entry.next_attempt_at_epoch_ms, |current| {
                current.min(entry.next_attempt_at_epoch_ms)
            }),
        );
    }

    let Some(index) = due_index else {
        return Ok(WorkspaceGitPushTickOutcome::skipped(
            "no_due_entries",
            queue.entries.len(),
            next_due_at,
        ));
    };

    let commit_id = queue
        .entries
        .get(index)
        .expect("due index should be in bounds")
        .commit_id
        .clone();
    let push_error = push_workspace_commit(workspace_root, config, &commit_id).err();

    if let Some(error) = push_error {
        let queue_depth = queue.entries.len();
        let entry = queue
            .entries
            .get_mut(index)
            .expect("due index should remain in bounds");
        let attempts = entry.attempt_count.saturating_add(1);
        entry.attempt_count = attempts;
        entry.last_error = Some(error.clone());
        let commit_key = entry.commit_key.clone();
        let failure_class = classify_push_failure(&error);
        let requires_manual_recovery = failure_class.requires_manual_recovery();
        let failure_kind = failure_class.as_token().to_string();
        let recovery_commands = if requires_manual_recovery {
            workspace_git_recovery_commands(config)
        } else {
            Vec::new()
        };

        let mut outcome = WorkspaceGitPushTickOutcome {
            enabled: true,
            attempted: true,
            pushed: false,
            exhausted: false,
            commit_key: Some(commit_key),
            queue_depth,
            next_due_at_epoch_ms: None,
            next_backoff_ms: None,
            failure: Some(error),
            failure_kind: Some(failure_kind),
            recovery_commands,
            skipped_reason: None,
        };

        if requires_manual_recovery {
            entry.exhausted = true;
            entry.next_attempt_at_epoch_ms = u64::MAX;
            let _ = entry;
            outcome.exhausted = true;
            outcome.next_due_at_epoch_ms = next_non_exhausted_due_at(&queue.entries);
            outcome.skipped_reason = Some("manual_recovery_required".to_string());
        } else if attempts >= WORKSPACE_GIT_PUSH_MAX_ATTEMPTS {
            entry.exhausted = true;
            entry.next_attempt_at_epoch_ms = u64::MAX;
            let _ = entry;
            outcome.exhausted = true;
            outcome.next_due_at_epoch_ms = next_non_exhausted_due_at(&queue.entries);
            outcome.skipped_reason = Some("retry_exhausted".to_string());
        } else {
            let backoff_ms = compute_push_backoff_ms(attempts);
            entry.next_attempt_at_epoch_ms = now_epoch_ms.saturating_add(backoff_ms);
            let next_attempt_at_epoch_ms = entry.next_attempt_at_epoch_ms;
            let _ = entry;
            outcome.next_backoff_ms = Some(backoff_ms);
            outcome.next_due_at_epoch_ms = Some(next_attempt_at_epoch_ms);
            outcome.skipped_reason = Some("retry_scheduled".to_string());
        }

        persist_workspace_git_push_queue(&queue_path, &queue)?;
        return Ok(outcome);
    }

    let commit_key = queue
        .entries
        .get(index)
        .expect("due index should remain in bounds")
        .commit_key
        .clone();
    queue.entries.remove(index);
    let next_due = next_non_exhausted_due_at(&queue.entries);
    persist_workspace_git_push_queue(&queue_path, &queue)?;

    Ok(WorkspaceGitPushTickOutcome {
        enabled: true,
        attempted: true,
        pushed: true,
        exhausted: false,
        commit_key: Some(commit_key),
        queue_depth: queue.entries.len(),
        next_due_at_epoch_ms: next_due,
        next_backoff_ms: None,
        failure: None,
        failure_kind: None,
        recovery_commands: Vec::new(),
        skipped_reason: None,
    })
}

fn validate_workspace_root(workspace_root: &Path) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_CONTEXT,
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let metadata = fs::metadata(workspace_root).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_CONTEXT,
        path: Some(display_path(workspace_root)),
        message: error.to_string(),
    })?;
    if !metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_CONTEXT,
            message: format!("{} must be a directory", display_path(workspace_root)),
        });
    }

    Ok(())
}

fn validate_commit_request(request: &WorkspaceGitCommitRequest) -> CrabResult<()> {
    ensure_non_empty_commit_field("logical_session_id", &request.logical_session_id)?;
    ensure_non_empty_commit_field("run_id", &request.run_id)?;
    if let Some(checkpoint_id) = request.checkpoint_id.as_deref() {
        ensure_non_empty_commit_field("checkpoint_id", checkpoint_id)?;
    }
    if let Some(run_status) = request.run_status.as_deref() {
        ensure_non_empty_commit_field("run_status", run_status)?;
    }
    if request.emitted_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_COMMIT_CONTEXT,
            message: "emitted_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    Ok(())
}

fn ensure_non_empty_commit_field(field: &str, value: &str) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_COMMIT_CONTEXT,
            message: format!("{field} must not be empty"),
        });
    }
    Ok(())
}

fn validate_push_request(request: &WorkspaceGitPushRequest) -> CrabResult<()> {
    if request.commit_key.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "commit_key must not be empty".to_string(),
        });
    }
    if request.commit_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "commit_id must not be empty".to_string(),
        });
    }
    if request.enqueued_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "enqueued_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    Ok(())
}

fn validate_push_queue_entry(entry: &WorkspaceGitPushQueueEntry) -> CrabResult<()> {
    if entry.commit_key.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "queue entry commit_key must not be empty".to_string(),
        });
    }
    if entry.commit_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "queue entry commit_id must not be empty".to_string(),
        });
    }
    if entry.enqueued_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "queue entry enqueued_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    if !entry.exhausted && entry.next_attempt_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "queue entry next_attempt_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    Ok(())
}

fn ensure_push_queue_layout(state_root: &Path) -> CrabResult<()> {
    if state_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: "state_root must not be empty".to_string(),
        });
    }

    fs::create_dir_all(state_root).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
        path: Some(display_path(state_root)),
        message: error.to_string(),
    })
}

fn workspace_git_push_queue_path(state_root: &Path) -> PathBuf {
    state_root.join(WORKSPACE_GIT_PUSH_QUEUE_FILE_NAME)
}

fn read_workspace_git_push_queue(path: &Path) -> CrabResult<WorkspaceGitPushQueueState> {
    if !path.exists() {
        return Ok(WorkspaceGitPushQueueState::default());
    }

    let raw = fs::read_to_string(path).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })?;
    let parsed: WorkspaceGitPushQueueState =
        serde_json::from_str(&raw).map_err(|_error| CrabError::CorruptData {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            path: display_path(path),
        })?;
    if parsed.version != WORKSPACE_GIT_PUSH_QUEUE_VERSION {
        return Err(CrabError::InvariantViolation {
            context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
            message: format!(
                "unsupported queue state version {}; expected {}",
                parsed.version, WORKSPACE_GIT_PUSH_QUEUE_VERSION
            ),
        });
    }

    for entry in &parsed.entries {
        validate_push_queue_entry(entry)?;
    }
    Ok(parsed)
}

fn persist_workspace_git_push_queue(
    path: &Path,
    queue: &WorkspaceGitPushQueueState,
) -> CrabResult<()> {
    for entry in &queue.entries {
        validate_push_queue_entry(entry)?;
    }

    let encoded = serde_json::to_vec_pretty(queue).expect("queue serialization should not fail");
    let temp_path = PathBuf::from(format!("{}.tmp", path.to_string_lossy()));
    fs::write(&temp_path, encoded).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
        path: Some(display_path(&temp_path)),
        message: error.to_string(),
    })?;
    fs::rename(&temp_path, path).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn push_workspace_commit(
    workspace_root: &Path,
    config: &WorkspaceGitConfig,
    commit_id: &str,
) -> Result<(), String> {
    let remote = match config.remote.as_deref() {
        Some(remote) => remote,
        None => {
            return Err("workspace git remote is missing for on-commit push policy".to_string());
        }
    };

    let target_ref = format!("{commit_id}:refs/heads/{}", config.branch);
    let args = ["push", "--porcelain", remote, &target_ref];
    let output = run_git(workspace_root, &args);
    if output.success {
        return Ok(());
    }

    let stderr = output.stderr.trim();
    let stdout = output.stdout.trim();
    Err(format!(
        "git push failed: stderr={stderr:?}; stdout={stdout:?}"
    ))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushFailureClass {
    Retryable,
    NonFastForward,
    DivergedHistory,
}

impl PushFailureClass {
    const fn requires_manual_recovery(self) -> bool {
        !matches!(self, Self::Retryable)
    }

    const fn as_token(self) -> &'static str {
        match self {
            Self::Retryable => "retryable",
            Self::NonFastForward => "non_fast_forward",
            Self::DivergedHistory => "diverged_history",
        }
    }
}

fn classify_push_failure(failure: &str) -> PushFailureClass {
    let lower = failure.to_ascii_lowercase();
    if lower.contains("non-fast-forward")
        || lower.contains("[rejected]")
        || lower.contains("fetch first")
        || lower.contains("failed to push some refs")
    {
        return PushFailureClass::NonFastForward;
    }
    if lower.contains("would clobber existing tag")
        || lower.contains("stale info")
        || lower.contains("remote rejected")
    {
        return PushFailureClass::DivergedHistory;
    }
    PushFailureClass::Retryable
}

#[must_use]
pub fn workspace_git_recovery_commands(config: &WorkspaceGitConfig) -> Vec<String> {
    let remote = config.remote.as_deref().unwrap_or("origin");
    let branch = config.branch.as_str();
    vec![
        format!("git fetch --prune {remote}"),
        format!("git log --oneline --graph --decorate --left-right HEAD...{remote}/{branch}"),
        format!("git rebase {remote}/{branch}"),
        format!("git push --porcelain {remote} HEAD:refs/heads/{branch}"),
    ]
}

fn compute_push_backoff_ms(attempt_count: u32) -> u64 {
    let shift = attempt_count.saturating_sub(1).min(16);
    let factor = 1_u64 << shift;
    WORKSPACE_GIT_PUSH_BACKOFF_BASE_MS
        .saturating_mul(factor)
        .min(WORKSPACE_GIT_PUSH_BACKOFF_MAX_MS)
}

fn next_non_exhausted_due_at(entries: &[WorkspaceGitPushQueueEntry]) -> Option<u64> {
    entries
        .iter()
        .filter(|entry| !entry.exhausted)
        .map(|entry| entry.next_attempt_at_epoch_ms)
        .min()
}

fn canonicalize_path(path: &Path) -> CrabResult<PathBuf> {
    fs::canonicalize(path).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn detect_git_repository_root(workspace_root: &Path) -> CrabResult<Option<PathBuf>> {
    let inside = run_git(workspace_root, &["rev-parse", "--is-inside-work-tree"]);
    if !inside.success {
        return Ok(None);
    }
    if inside.stdout.trim() != "true" {
        return Ok(None);
    }

    require_git_repository_root(workspace_root).map(Some)
}

fn require_git_repository_root(workspace_root: &Path) -> CrabResult<PathBuf> {
    let output = run_git_checked(workspace_root, &["rev-parse", "--show-toplevel"])?;
    canonicalize_path(Path::new(output.trim()))
}

fn ensure_same_repository_root(expected_root: &Path, repository_root: &Path) -> CrabResult<()> {
    if repository_root == expected_root {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: WORKSPACE_GIT_CONTEXT,
        message: format!(
            "workspace root {} is nested inside repository {}; refusing to mutate external repository",
            display_path(expected_root),
            display_path(repository_root)
        ),
    })
}

fn probe_bare_repository(workspace_root: &Path) -> bool {
    let raw = run_git(workspace_root, &["rev-parse", "--is-bare-repository"]);
    if !raw.success {
        return false;
    }
    raw.stdout.trim() == "true"
}

fn resolve_head_branch(workspace_root: &Path) -> CrabResult<String> {
    let args = ["symbolic-ref", "--quiet", "--short", "HEAD"];
    let output = run_git_checked(workspace_root, &args)?;
    Ok(output.trim().to_string())
}

fn has_commits(workspace_root: &Path) -> bool {
    let output = run_git(workspace_root, &["rev-parse", "--verify", "HEAD"]);
    output.success
}

fn workspace_commit_key(request: &WorkspaceGitCommitRequest) -> String {
    let checkpoint_id = request
        .checkpoint_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("none");
    format!(
        "{}|{}|{}|{}",
        request.logical_session_id,
        request.run_id,
        request.trigger.as_token(),
        checkpoint_id
    )
}

fn head_commit_contains_key(workspace_root: &Path, commit_key: &str) -> CrabResult<bool> {
    let has_head = has_commits(workspace_root);
    if !has_head {
        return Ok(false);
    }

    let args = ["log", "-1", "--format=%B"];
    let body = run_git_checked_in_context(WORKSPACE_GIT_COMMIT_CONTEXT, workspace_root, &args)?;
    let expected = format!("{WORKSPACE_GIT_COMMIT_KEY_TRAILER}: {commit_key}");
    Ok(body.lines().map(str::trim).any(|line| line == expected))
}

fn stage_workspace_changes(workspace_root: &Path) -> CrabResult<StagingPolicyAudit> {
    let mut audit = StagingPolicyAudit::default();
    let changed_paths = changed_workspace_paths(workspace_root)?;
    for path in changed_paths {
        match evaluate_staging_policy(&path) {
            Some(rule_id) => {
                audit.skipped_paths.push(path.clone());
                audit.skipped_rules.push(rule_id.to_string());
            }
            None => {
                let args = ["add", "-A", "--", path.as_str()];
                run_git_checked_in_context(WORKSPACE_GIT_COMMIT_CONTEXT, workspace_root, &args)?;
            }
        }
    }
    audit.skipped_paths.sort();
    audit.skipped_paths.dedup();
    audit.skipped_rules.sort();
    audit.skipped_rules.dedup();
    Ok(audit)
}

fn changed_workspace_paths(workspace_root: &Path) -> CrabResult<Vec<String>> {
    let args = ["status", "--porcelain", "-z", "--untracked-files=all"];
    let status = run_git_checked_in_context(WORKSPACE_GIT_COMMIT_CONTEXT, workspace_root, &args)?;
    parse_porcelain_paths(&status)
}

fn parse_porcelain_paths(status: &str) -> CrabResult<Vec<String>> {
    let mut paths = Vec::new();
    let mut records = status.split_terminator('\0');
    while let Some(record) = records.next() {
        if record.is_empty() {
            continue;
        }
        let mut chars = record.chars();
        let index_status = chars.next().expect("record is non-empty by guard");
        let Some(_worktree_status) = chars.next() else {
            return Err(CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: format!("invalid porcelain status entry {record:?}"),
            });
        };
        if chars.next() != Some(' ') {
            return Err(CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: format!("invalid porcelain status entry {record:?}"),
            });
        }
        let first_path = normalize_workspace_relative_path(chars.as_str());
        if first_path.is_empty() {
            return Err(CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "porcelain status entry path must not be empty".to_string(),
            });
        }
        paths.push(first_path);
        if index_status == 'R' || index_status == 'C' {
            let Some(second_path_raw) = records.next() else {
                return Err(CrabError::InvariantViolation {
                    context: WORKSPACE_GIT_COMMIT_CONTEXT,
                    message: format!("missing rename/copy destination for {record:?}"),
                });
            };
            let second_path = normalize_workspace_relative_path(second_path_raw);
            if second_path.is_empty() {
                return Err(CrabError::InvariantViolation {
                    context: WORKSPACE_GIT_COMMIT_CONTEXT,
                    message: "rename/copy destination path must not be empty".to_string(),
                });
            }
            paths.push(second_path);
        }
    }
    paths.sort();
    paths.dedup();
    Ok(paths)
}

fn normalize_workspace_relative_path(path: &str) -> String {
    path.trim_start_matches("./").replace('\\', "/")
}

fn evaluate_staging_policy(path: &str) -> Option<&'static str> {
    let file_name = path.rsplit('/').next().unwrap_or(path);

    if matches!(file_name, ".env.example" | ".env.sample" | ".env.template") {
        return None;
    }

    if file_name == ".env"
        || matches!(
            file_name,
            ".env.local" | ".env.production" | ".env.development" | ".env.test"
        )
        || file_name.starts_with(".env.")
    {
        return Some("deny_dotenv");
    }
    if has_path_segment(path, "secrets") || has_path_segment(path, "secret") {
        return Some("deny_secret_directory");
    }
    if has_path_segment(path, ".aws") || has_path_segment(path, ".ssh") {
        return Some("deny_credentials_directory");
    }
    if matches!(file_name, "id_rsa" | "id_ed25519") {
        return Some("deny_private_key_name");
    }
    if path.ends_with(".pem")
        || path.ends_with(".key")
        || path.ends_with(".p12")
        || path.ends_with(".pfx")
    {
        return Some("deny_private_key_extension");
    }
    if has_path_segment(path, "node_modules") {
        return Some("deny_transient_node_modules");
    }
    if has_path_segment(path, "target") {
        return Some("deny_transient_target");
    }
    if path.ends_with(".log") || file_name == ".DS_Store" {
        return Some("deny_transient_misc");
    }

    None
}

fn has_path_segment(path: &str, segment: &str) -> bool {
    path.split('/').any(|part| part == segment)
}

fn workspace_has_changes(workspace_root: &Path) -> CrabResult<bool> {
    let args = ["diff", "--cached", "--name-only"];
    let staged = run_git_checked_in_context(WORKSPACE_GIT_COMMIT_CONTEXT, workspace_root, &args)?;
    Ok(staged.lines().any(|line| !line.trim().is_empty()))
}

fn commit_workspace_changes(
    workspace_root: &Path,
    config: &WorkspaceGitConfig,
    request: &WorkspaceGitCommitRequest,
    commit_key: &str,
    staging_audit: &StagingPolicyAudit,
) -> CrabResult<()> {
    let subject = format!(
        "crab(workspace): {} {}",
        request.trigger.as_token(),
        request.run_id
    );
    let checkpoint_id = request.checkpoint_id.as_deref().unwrap_or("none");
    let run_status = request.run_status.as_deref().unwrap_or("none");
    let skipped_rule_tokens = if staging_audit.skipped_rules.is_empty() {
        "none".to_string()
    } else {
        staging_audit.skipped_rules.join(",")
    };
    let body = [
        format!("Crab-Commit-Version: {WORKSPACE_GIT_COMMIT_VERSION}"),
        format!("Crab-Staging-Policy-Version: {WORKSPACE_GIT_STAGING_POLICY_VERSION}"),
        format!(
            "Crab-Staging-Skipped-Count: {}",
            staging_audit.skipped_paths.len()
        ),
        format!("Crab-Staging-Skipped-Rules: {skipped_rule_tokens}"),
        format!("Crab-Trigger: {}", request.trigger.as_token()),
        format!("Crab-Logical-Session-Id: {}", request.logical_session_id),
        format!("Crab-Run-Id: {}", request.run_id),
        format!("Crab-Checkpoint-Id: {checkpoint_id}"),
        format!("Crab-Run-Status: {run_status}"),
        format!("Crab-Emitted-At-Epoch-Ms: {}", request.emitted_at_epoch_ms),
        format!("{WORKSPACE_GIT_COMMIT_KEY_TRAILER}: {commit_key}"),
    ]
    .join("\n");

    let args = vec![
        "-c".to_string(),
        format!("user.name={}", config.commit_name),
        "-c".to_string(),
        format!("user.email={}", config.commit_email),
        "commit".to_string(),
        "--no-gpg-sign".to_string(),
        "-m".to_string(),
        subject,
        "-m".to_string(),
        body,
    ];
    run_git_checked_owned(WORKSPACE_GIT_COMMIT_CONTEXT, workspace_root, &args)?;
    Ok(())
}

fn current_head_commit_id(workspace_root: &Path) -> CrabResult<String> {
    let args = ["rev-parse", "HEAD"];
    let head = run_git_checked_in_context(WORKSPACE_GIT_COMMIT_CONTEXT, workspace_root, &args)?;
    Ok(head.trim().to_string())
}

fn read_origin_remote(workspace_root: &Path) -> CrabResult<Option<String>> {
    let remotes_output = run_git_checked(workspace_root, &["remote"])?;
    let remotes = remotes_output
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if !remotes.contains(&"origin") {
        return Ok(None);
    }

    let origin_url = run_git_checked(workspace_root, &["remote", "get-url", "origin"])?;
    Ok(Some(origin_url.trim().to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommandOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

fn run_git(workspace_root: &Path, args: &[&str]) -> CommandOutput {
    run_command(GIT_BINARY, workspace_root, args).unwrap_or_else(command_spawn_failure_output)
}

fn command_spawn_failure_output(error: CrabError) -> CommandOutput {
    CommandOutput {
        success: false,
        stdout: String::new(),
        stderr: error.to_string(),
    }
}

fn run_git_checked(workspace_root: &Path, args: &[&str]) -> CrabResult<String> {
    run_git_checked_in_context(WORKSPACE_GIT_CONTEXT, workspace_root, args)
}

fn run_git_checked_in_context(
    context: &'static str,
    workspace_root: &Path,
    args: &[&str],
) -> CrabResult<String> {
    let output = run_git(workspace_root, args);
    if output.success {
        return Ok(output.stdout);
    }

    Err(CrabError::InvariantViolation {
        context,
        message: format!("git {} failed: {}", args.join(" "), output.stderr.trim()),
    })
}

fn run_git_checked_owned(
    context: &'static str,
    workspace_root: &Path,
    args: &[String],
) -> CrabResult<String> {
    let refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_git_checked_in_context(context, workspace_root, &refs)
}

fn run_command(binary: &str, workspace_root: &Path, args: &[&str]) -> CrabResult<CommandOutput> {
    let output = Command::new(binary)
        .arg("-C")
        .arg(workspace_root)
        .args(args)
        .output()
        .map_err(|error| CrabError::Io {
            context: WORKSPACE_GIT_CONTEXT,
            path: Some(display_path(workspace_root)),
            message: format!("failed to spawn {binary}: {error}"),
        })?;

    Ok(CommandOutput {
        success: output.status.success(),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    })
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{RuntimeConfig, WorkspaceGitPushPolicy};

    use super::{
        canonicalize_path, classify_push_failure, command_spawn_failure_output,
        detect_git_repository_root, enqueue_workspace_git_push_request, ensure_push_queue_layout,
        ensure_workspace_git_repository, evaluate_staging_policy, has_commits,
        maybe_commit_workspace_snapshot, parse_porcelain_paths, persist_workspace_git_push_queue,
        process_workspace_git_push_queue, push_workspace_commit, read_origin_remote,
        resolve_head_branch, run_command, run_git_checked, validate_workspace_root,
        workspace_git_push_queue_path, workspace_git_recovery_commands, PushFailureClass,
        WorkspaceGitCommitOutcome, WorkspaceGitCommitRequest, WorkspaceGitCommitTrigger,
        WorkspaceGitEnsureOutcome, WorkspaceGitPushEnqueueOutcome, WorkspaceGitPushQueueEntry,
        WorkspaceGitPushQueueState, WorkspaceGitPushRequest, WorkspaceGitPushTickOutcome,
        WORKSPACE_GIT_COMMIT_CONTEXT, WORKSPACE_GIT_COMMIT_KEY_TRAILER, WORKSPACE_GIT_CONTEXT,
        WORKSPACE_GIT_PUSH_MAX_ATTEMPTS, WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
        WORKSPACE_GIT_PUSH_QUEUE_VERSION,
    };
    use crate::CrabError;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "crab-workspace-git-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            Self { path }
        }

        fn create(&self) {
            fs::create_dir_all(&self.path).expect("temp directory should be creatable");
        }

        fn child(&self, relative: &str) -> PathBuf {
            self.path.join(relative)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn parse_workspace_git(entries: &[(&str, &str)]) -> crate::WorkspaceGitConfig {
        let mut values = HashMap::new();
        values.insert("CRAB_DISCORD_TOKEN".to_string(), "token".to_string());
        for (key, value) in entries {
            values.insert((*key).to_string(), (*value).to_string());
        }
        RuntimeConfig::from_map(&values)
            .expect("runtime config should parse")
            .workspace_git
    }

    fn run_git_in(path: &Path, args: &[&str]) -> String {
        run_git_checked(path, args).expect("git command should succeed")
    }

    fn write_fixture(path: &Path, relative: &str, contents: &str) {
        let full_path = path.join(relative);
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent).expect("fixture parent should be creatable");
        }
        fs::write(full_path, contents).expect("fixture file should be writable");
    }

    fn initialize_repo(path: &Path, branch: &str) {
        run_git_in(path, &["init"]);
        run_git_in(
            path,
            &["symbolic-ref", "HEAD", &format!("refs/heads/{branch}")],
        );
    }

    fn commit_file(path: &Path, file_name: &str) {
        fs::write(path.join(file_name), "payload\n").expect("fixture file should be writable");
        run_git_in(path, &["add", file_name]);
        run_git_in(path, &["config", "user.name", "Crab Test"]);
        run_git_in(path, &["config", "user.email", "crab-test@example.com"]);
        run_git_in(path, &["commit", "-m", "fixture commit"]);
    }

    fn initialize_bare_remote(path: &Path) {
        fs::create_dir_all(path).expect("remote directory should be creatable");
        run_git_in(path, &["init", "--bare"]);
    }

    fn state_root(workspace: &TempDir) -> PathBuf {
        workspace.child("state")
    }

    fn read_push_queue_entries(state_root: &Path) -> Vec<WorkspaceGitPushQueueEntry> {
        let path = workspace_git_push_queue_path(state_root);
        if !path.exists() {
            return Vec::new();
        }
        let raw = fs::read_to_string(path).expect("queue file should be readable");
        let parsed: WorkspaceGitPushQueueState =
            serde_json::from_str(&raw).expect("queue file should parse");
        parsed.entries
    }

    fn write_push_queue_state(state_root: &Path, queue: &WorkspaceGitPushQueueState) {
        fs::create_dir_all(state_root).expect("state root should be creatable");
        let path = workspace_git_push_queue_path(state_root);
        let encoded = serde_json::to_string_pretty(queue).expect("queue state should encode");
        fs::write(path, encoded).expect("queue file should be writable");
    }

    #[test]
    fn disabled_configuration_is_noop() {
        let workspace = TempDir::new("disabled");
        workspace.create();
        let config = parse_workspace_git(&[]);

        let outcome = ensure_workspace_git_repository(&workspace.path, &config)
            .expect("disabled workspace git should be noop");
        assert_eq!(outcome, WorkspaceGitEnsureOutcome::disabled());
        assert!(!workspace.child(".git").exists());
    }

    #[test]
    fn enabled_configuration_bootstraps_repo_branch_and_remote() {
        let workspace = TempDir::new("bootstrap");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private-crab-workspace.git",
            ),
            ("CRAB_WORKSPACE_GIT_BRANCH", "crab/runtime"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
        ]);

        let outcome = ensure_workspace_git_repository(&workspace.path, &config)
            .expect("workspace git bootstrap should succeed");
        assert!(outcome.enabled);
        assert!(outcome.repository_initialized);
        assert!(outcome.branch_bootstrapped);
        assert!(outcome.remote_bound);
        assert_eq!(
            resolve_head_branch(&workspace.path).expect("branch should be readable"),
            "crab/runtime"
        );
        assert_eq!(
            read_origin_remote(&workspace.path).expect("origin should be readable"),
            Some("git@github.com:fontanierh/private-crab-workspace.git".to_string())
        );
        assert!(workspace.child(".git").exists());
        assert!(outcome.repository_root.is_some());
    }

    #[test]
    fn existing_repo_with_matching_branch_and_remote_is_noop() {
        let workspace = TempDir::new("existing-noop");
        workspace.create();
        initialize_repo(&workspace.path, "main");
        run_git_in(
            &workspace.path,
            &[
                "remote",
                "add",
                "origin",
                "git@github.com:fontanierh/workspace.git",
            ],
        );

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/workspace.git",
            ),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
        ]);

        let outcome = ensure_workspace_git_repository(&workspace.path, &config)
            .expect("existing aligned repository should validate");
        assert!(outcome.enabled);
        assert!(!outcome.repository_initialized);
        assert!(!outcome.branch_bootstrapped);
        assert!(!outcome.remote_bound);
    }

    #[test]
    fn binds_origin_remote_when_missing() {
        let workspace = TempDir::new("bind-origin");
        workspace.create();
        initialize_repo(&workspace.path, "main");

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/workspace-private.git",
            ),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
        ]);

        let outcome = ensure_workspace_git_repository(&workspace.path, &config)
            .expect("missing origin should be bound");
        assert!(outcome.remote_bound);
        assert_eq!(
            read_origin_remote(&workspace.path).expect("origin should exist"),
            Some("git@github.com:fontanierh/workspace-private.git".to_string())
        );
    }

    #[test]
    fn rejects_remote_mismatch() {
        let workspace = TempDir::new("remote-mismatch");
        workspace.create();
        initialize_repo(&workspace.path, "main");
        run_git_in(
            &workspace.path,
            &[
                "remote",
                "add",
                "origin",
                "git@github.com:fontanierh/old.git",
            ],
        );

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/new.git",
            ),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
        ]);

        let error = ensure_workspace_git_repository(&workspace.path, &config)
            .expect_err("remote mismatch should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));
        assert!(error
            .to_string()
            .contains("existing origin remote is \"git@github.com:fontanierh/old.git\""));
    }

    #[test]
    fn rejects_branch_mismatch_when_repository_has_commits() {
        let workspace = TempDir::new("branch-mismatch");
        workspace.create();
        initialize_repo(&workspace.path, "main");
        commit_file(&workspace.path, "state.txt");

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "release"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);

        let error = ensure_workspace_git_repository(&workspace.path, &config)
            .expect_err("branch mismatch with commits should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));
        assert!(error
            .to_string()
            .contains("existing repository branch is \"main\""));
    }

    #[test]
    fn rebinds_branch_without_commits() {
        let workspace = TempDir::new("branch-rebind");
        workspace.create();
        initialize_repo(&workspace.path, "main");

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "crab/alt"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);

        let outcome = ensure_workspace_git_repository(&workspace.path, &config)
            .expect("branch should be re-bound on empty repo");
        assert!(outcome.branch_bootstrapped);
        assert_eq!(
            resolve_head_branch(&workspace.path).expect("branch should be updated"),
            "crab/alt"
        );
    }

    #[test]
    fn rejects_when_workspace_is_nested_inside_external_repository() {
        let root = TempDir::new("nested");
        root.create();
        initialize_repo(&root.path, "main");
        let nested_workspace = root.child("workspace");
        fs::create_dir_all(&nested_workspace).expect("nested workspace should be creatable");

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);

        let error = ensure_workspace_git_repository(&nested_workspace, &config)
            .expect_err("nested workspace should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));
        assert!(error
            .to_string()
            .contains("refusing to mutate external repository"));
    }

    #[test]
    fn rejects_bare_repository_workspace() {
        let workspace = TempDir::new("bare");
        workspace.create();
        run_git_in(&workspace.path, &["init", "--bare"]);

        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);

        let error = ensure_workspace_git_repository(&workspace.path, &config)
            .expect_err("bare repository should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                message: "workspace git persistence requires a non-bare repository".to_string(),
            }
        );
    }

    #[test]
    fn helper_validation_and_command_paths_are_actionable() {
        let blank =
            validate_workspace_root(Path::new("")).expect_err("blank path should fail validation");
        assert_eq!(
            blank,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                message: "workspace_root must not be empty".to_string(),
            }
        );

        let missing =
            validate_workspace_root(Path::new("/tmp/crab-workspace-git-missing-does-not-exist"))
                .expect_err("missing workspace should fail validation");
        assert!(matches!(
            missing,
            CrabError::Io {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));

        let root_as_file = TempDir::new("root-as-file");
        fs::write(&root_as_file.path, "payload").expect("fixture file should be writable");
        let root_as_file_error =
            validate_workspace_root(&root_as_file.path).expect_err("file workspace should fail");
        assert_eq!(
            root_as_file_error,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                message: format!(
                    "{} must be a directory",
                    root_as_file.path.to_string_lossy()
                ),
            }
        );

        let canonicalize_error =
            canonicalize_path(Path::new("/tmp/crab-workspace-git-canonicalize-missing"))
                .expect_err("missing path canonicalization should fail");
        assert!(matches!(
            canonicalize_error,
            CrabError::Io {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));

        let workspace = TempDir::new("command-errors");
        workspace.create();
        let spawn_error = run_command("definitely-not-a-real-binary", &workspace.path, &["status"])
            .expect_err("missing binary should fail spawn");
        assert!(matches!(
            spawn_error,
            CrabError::Io {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));

        let git_failure = run_git_checked(&workspace.path, &["rev-parse", "--show-toplevel"])
            .expect_err("git failure should surface as invariant");
        assert!(matches!(
            git_failure,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_CONTEXT,
                ..
            }
        ));
    }

    #[test]
    fn command_spawn_failure_output_maps_to_failed_command_output() {
        let output = command_spawn_failure_output(CrabError::Io {
            context: WORKSPACE_GIT_CONTEXT,
            path: None,
            message: "spawn failed".to_string(),
        });
        assert!(!output.success);
        assert!(output.stdout.is_empty());
        assert!(output.stderr.contains("spawn failed"));
    }

    #[test]
    fn push_failure_classification_and_recovery_commands_cover_manual_paths() {
        let non_fast_forward = classify_push_failure(
            "git push failed: stderr=\"[rejected] non-fast-forward\"; stdout=\"\"",
        );
        assert_eq!(non_fast_forward, PushFailureClass::NonFastForward);
        assert_eq!(non_fast_forward.as_token(), "non_fast_forward");
        assert!(non_fast_forward.requires_manual_recovery());

        let diverged = classify_push_failure(
            "git push failed: stderr=\"remote rejected, stale info\"; stdout=\"\"",
        );
        assert_eq!(diverged, PushFailureClass::DivergedHistory);
        assert_eq!(diverged.as_token(), "diverged_history");
        assert!(diverged.requires_manual_recovery());

        let retryable = classify_push_failure("git push failed: stderr=\"timeout\"; stdout=\"\"");
        assert_eq!(retryable, PushFailureClass::Retryable);
        assert_eq!(retryable.as_token(), "retryable");
        assert!(!retryable.requires_manual_recovery());

        let config = crate::WorkspaceGitConfig {
            enabled: true,
            remote: None,
            branch: "main".to_string(),
            commit_name: "Crab Runtime".to_string(),
            commit_email: "crab-runtime@example.com".to_string(),
            push_policy: WorkspaceGitPushPolicy::Manual,
        };
        let commands = workspace_git_recovery_commands(&config);
        assert_eq!(
            commands,
            vec![
                "git fetch --prune origin".to_string(),
                "git log --oneline --graph --decorate --left-right HEAD...origin/main".to_string(),
                "git rebase origin/main".to_string(),
                "git push --porcelain origin HEAD:refs/heads/main".to_string(),
            ]
        );
    }

    #[test]
    fn push_workspace_commit_failure_message_includes_trimmed_streams() {
        let workspace = TempDir::new("push-command-failure-message");
        workspace.create();
        initialize_repo(&workspace.path, "main");
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "/tmp/crab-workspace-git-remote-missing",
            ),
        ]);

        let error = push_workspace_commit(&workspace.path, &config, "deadbeef")
            .expect_err("invalid push target should fail");
        assert!(error.contains("git push failed: stderr="));
        assert!(error.contains("stdout="));
    }

    #[test]
    fn parse_porcelain_paths_validation_is_strict() {
        let empty_ok = parse_porcelain_paths("\0").expect("empty records should be ignored");
        assert!(empty_ok.is_empty());

        let missing_worktree_status =
            parse_porcelain_paths("A\0").expect_err("single-char status must fail");
        assert!(matches!(
            missing_worktree_status,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                ..
            }
        ));

        let missing_separator =
            parse_porcelain_paths("A?\0").expect_err("missing separator must fail");
        assert!(matches!(
            missing_separator,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                ..
            }
        ));

        let empty_path = parse_porcelain_paths("A  \0").expect_err("empty path entry must fail");
        assert_eq!(
            empty_path,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "porcelain status entry path must not be empty".to_string(),
            }
        );

        let missing_rename_destination =
            parse_porcelain_paths("R  old.txt\0").expect_err("rename entries require destination");
        assert!(matches!(
            missing_rename_destination,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                ..
            }
        ));

        let empty_rename_destination = parse_porcelain_paths("R  old.txt\0\0")
            .expect_err("rename destination must not be empty");
        assert_eq!(
            empty_rename_destination,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "rename/copy destination path must not be empty".to_string(),
            }
        );

        let renamed = parse_porcelain_paths("R  old name.txt\0new-name.txt\0")
            .expect("valid rename entries should parse");
        assert_eq!(
            renamed,
            vec!["new-name.txt".to_string(), "old name.txt".to_string()]
        );
    }

    #[test]
    fn evaluate_staging_policy_covers_sensitive_and_transient_rules() {
        assert_eq!(evaluate_staging_policy(".env.local"), Some("deny_dotenv"));
        assert_eq!(
            evaluate_staging_policy("nested/.env.custom"),
            Some("deny_dotenv")
        );
        assert_eq!(
            evaluate_staging_policy("memory/users/secrets/2026-02-11.md"),
            Some("deny_secret_directory")
        );
        assert_eq!(
            evaluate_staging_policy(".aws/credentials"),
            Some("deny_credentials_directory")
        );
        assert_eq!(
            evaluate_staging_policy("ops/.ssh/config"),
            Some("deny_credentials_directory")
        );
        assert_eq!(
            evaluate_staging_policy("keys/id_rsa"),
            Some("deny_private_key_name")
        );
        assert_eq!(
            evaluate_staging_policy("certs/service.pem"),
            Some("deny_private_key_extension")
        );
        assert_eq!(
            evaluate_staging_policy("logs/runtime.log"),
            Some("deny_transient_misc")
        );
        assert_eq!(
            evaluate_staging_policy("node_modules/pkg/index.js"),
            Some("deny_transient_node_modules")
        );
        assert_eq!(
            evaluate_staging_policy("target/debug/crabd"),
            Some("deny_transient_target")
        );
        assert_eq!(evaluate_staging_policy(".env.example"), None);
    }

    #[test]
    fn detects_bare_repository_as_non_work_tree_for_nested_guard() {
        let workspace = TempDir::new("detect-bare");
        workspace.create();
        run_git_in(&workspace.path, &["init", "--bare"]);

        let repository_root =
            detect_git_repository_root(&workspace.path).expect("probe should succeed");
        assert_eq!(repository_root, None);
    }

    #[test]
    fn helper_status_probes_report_expected_states() {
        let workspace = TempDir::new("status-probes");
        workspace.create();
        initialize_repo(&workspace.path, "main");

        assert!(!has_commits(&workspace.path));
        commit_file(&workspace.path, "data.txt");
        assert!(has_commits(&workspace.path));

        run_git_in(
            &workspace.path,
            &["remote", "add", "origin", "git@github.com:foo/bar.git"],
        );
        assert_eq!(
            read_origin_remote(&workspace.path).expect("origin should be present"),
            Some("git@github.com:foo/bar.git".to_string())
        );
    }

    #[test]
    fn runtime_config_manual_push_without_remote_stays_valid_for_git_bootstrap() {
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);
        assert!(config.enabled);
        assert_eq!(config.remote, None);
        assert_eq!(config.push_policy, WorkspaceGitPushPolicy::Manual);
    }

    fn sample_commit_request(trigger: WorkspaceGitCommitTrigger) -> WorkspaceGitCommitRequest {
        WorkspaceGitCommitRequest {
            logical_session_id: "discord:channel:777".to_string(),
            run_id: "run:discord:channel:777:message-1".to_string(),
            trigger,
            checkpoint_id: None,
            run_status: Some("succeeded".to_string()),
            emitted_at_epoch_ms: 123,
        }
    }

    #[test]
    fn workspace_commit_is_disabled_when_persistence_is_off() {
        let workspace = TempDir::new("commit-disabled");
        workspace.create();
        initialize_repo(&workspace.path, "main");
        fs::write(workspace.child("state.json"), "{}\n").expect("fixture state should be writable");

        let config = parse_workspace_git(&[]);
        let outcome = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("disabled commit should be noop");
        assert_eq!(outcome, WorkspaceGitCommitOutcome::disabled());
    }

    #[test]
    fn workspace_commit_writes_metadata_and_returns_commit_id() {
        let workspace = TempDir::new("commit-metadata");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
            ("CRAB_WORKSPACE_GIT_COMMIT_NAME", "Crab Runtime"),
            (
                "CRAB_WORKSPACE_GIT_COMMIT_EMAIL",
                "crab-runtime@example.com",
            ),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("repository bootstrap should succeed");
        write_fixture(&workspace.path, "state/session.json", "{ \"run\": 1 }\n");

        let outcome = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                checkpoint_id: Some("ckpt:run:777:42".to_string()),
                ..sample_commit_request(WorkspaceGitCommitTrigger::RotationCheckpoint)
            },
        )
        .expect("commit should succeed");
        assert!(outcome.enabled);
        assert!(outcome.committed);
        assert_eq!(
            outcome.trigger,
            Some(WorkspaceGitCommitTrigger::RotationCheckpoint)
        );
        assert!(outcome.skipped_reason.is_none());
        let commit_id = outcome
            .commit_id
            .as_deref()
            .expect("commit id should be returned");
        assert!(!commit_id.trim().is_empty());

        let message = run_git_in(&workspace.path, &["log", "-1", "--pretty=%B"]);
        assert!(message.contains("Crab-Commit-Version: 1"));
        assert!(message.contains("Crab-Staging-Policy-Version: 1"));
        assert!(message.contains("Crab-Staging-Skipped-Count: 0"));
        assert!(message.contains("Crab-Staging-Skipped-Rules: none"));
        assert!(message.contains("Crab-Trigger: rotation_checkpoint"));
        assert!(message.contains("Crab-Logical-Session-Id: discord:channel:777"));
        assert!(message.contains("Crab-Run-Id: run:discord:channel:777:message-1"));
        assert!(message.contains("Crab-Checkpoint-Id: ckpt:run:777:42"));
        assert!(message.contains("Crab-Run-Status: succeeded"));
        assert!(message.contains("Crab-Emitted-At-Epoch-Ms: 123"));
        let expected_key = outcome
            .commit_key
            .as_deref()
            .expect("commit key should be present");
        assert!(message.contains(&format!(
            "{WORKSPACE_GIT_COMMIT_KEY_TRAILER}: {expected_key}"
        )));
    }

    #[test]
    fn workspace_commit_staging_policy_excludes_sensitive_paths_and_audits_skips() {
        let workspace = TempDir::new("commit-staging-policy");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("repository bootstrap should succeed");
        write_fixture(&workspace.path, ".env", "SUPER_SECRET=1\n");
        write_fixture(&workspace.path, ".env.example", "EXAMPLE=1\n");
        write_fixture(&workspace.path, "secrets/api-token.txt", "token\n");
        write_fixture(&workspace.path, "notes/plan.md", "plan\n");

        let outcome = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("commit should succeed");
        assert!(outcome.committed);
        assert_eq!(
            outcome.staging_skipped_paths,
            vec![".env".to_string(), "secrets/api-token.txt".to_string()]
        );

        let committed_files =
            run_git_in(&workspace.path, &["ls-tree", "-r", "--name-only", "HEAD"]);
        assert!(committed_files.lines().any(|line| line == ".env.example"));
        assert!(committed_files.lines().any(|line| line == "notes/plan.md"));
        assert!(!committed_files.lines().any(|line| line == ".env"));
        assert!(!committed_files
            .lines()
            .any(|line| line == "secrets/api-token.txt"));

        let message = run_git_in(&workspace.path, &["log", "-1", "--pretty=%B"]);
        assert!(message.contains("Crab-Staging-Policy-Version: 1"));
        assert!(message.contains("Crab-Staging-Skipped-Count: 2"));
        assert!(message.contains("Crab-Staging-Skipped-Rules: deny_dotenv,deny_secret_directory"));
    }

    #[test]
    fn workspace_commit_reports_no_changes_when_only_denied_paths_are_dirty() {
        let workspace = TempDir::new("commit-staging-only-denied");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("repository bootstrap should succeed");
        write_fixture(&workspace.path, ".env", "SUPER_SECRET=1\n");

        let outcome = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("commit should return no_changes");
        assert!(!outcome.committed);
        assert_eq!(outcome.skipped_reason, Some("no_changes".to_string()));
        assert_eq!(outcome.commit_id, None);
        assert_eq!(outcome.staging_skipped_paths, vec![".env".to_string()]);
    }

    #[test]
    fn workspace_commit_skips_replayed_head_commit_without_changes() {
        let workspace = TempDir::new("commit-replay");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("repository bootstrap should succeed");
        write_fixture(
            &workspace.path,
            "state/run.json",
            "{ \"status\": \"ok\" }\n",
        );
        let request = sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized);

        let first = maybe_commit_workspace_snapshot(&workspace.path, &config, &request)
            .expect("first commit should succeed");
        assert!(first.committed);

        let second = maybe_commit_workspace_snapshot(&workspace.path, &config, &request)
            .expect("replayed commit should be detected");
        assert!(!second.committed);
        assert_eq!(second.skipped_reason, Some("already_committed".to_string()));
        assert_eq!(second.commit_key, first.commit_key);
        assert_eq!(second.commit_id, first.commit_id);
    }

    #[test]
    fn workspace_commit_rejects_duplicate_key_when_new_changes_exist() {
        let workspace = TempDir::new("commit-duplicate-key-dirty");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("repository bootstrap should succeed");
        write_fixture(&workspace.path, "state/first.json", "one\n");
        let request = sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized);
        maybe_commit_workspace_snapshot(&workspace.path, &config, &request)
            .expect("first commit should succeed");

        write_fixture(&workspace.path, "state/second.json", "two\n");
        let error = maybe_commit_workspace_snapshot(&workspace.path, &config, &request)
            .expect_err("duplicate key with pending changes should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "HEAD already contains commit key \"discord:channel:777|run:discord:channel:777:message-1|run_finalized|none\" but workspace has staged changes; refusing duplicate-key commit".to_string(),
            }
        );
    }

    #[test]
    fn enqueue_workspace_git_push_request_is_idempotent_by_commit_key() {
        let workspace = TempDir::new("push-enqueue-idempotent");
        workspace.create();
        let state_root = state_root(&workspace);
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);
        let request = WorkspaceGitPushRequest {
            commit_key: "k1".to_string(),
            commit_id: "c1".to_string(),
            enqueued_at_epoch_ms: 100,
        };

        let first = enqueue_workspace_git_push_request(&state_root, &config, &request)
            .expect("first enqueue should succeed");
        assert!(first.enabled);
        assert!(first.enqueued);
        assert_eq!(first.queue_depth, 1);

        let second = enqueue_workspace_git_push_request(&state_root, &config, &request)
            .expect("second enqueue should dedupe");
        assert!(second.enabled);
        assert!(!second.enqueued);
        assert_eq!(second.queue_depth, 1);
        assert_eq!(second.skipped_reason, Some("already_queued".to_string()));

        let entries = read_push_queue_entries(&state_root);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].commit_key, "k1");
    }

    #[test]
    fn enqueue_workspace_git_push_request_rejects_conflicting_commit_ids() {
        let workspace = TempDir::new("push-enqueue-conflict");
        workspace.create();
        let state_root = state_root(&workspace);
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);

        enqueue_workspace_git_push_request(
            &state_root,
            &config,
            &WorkspaceGitPushRequest {
                commit_key: "k1".to_string(),
                commit_id: "c1".to_string(),
                enqueued_at_epoch_ms: 100,
            },
        )
        .expect("first enqueue should succeed");

        let conflict = enqueue_workspace_git_push_request(
            &state_root,
            &config,
            &WorkspaceGitPushRequest {
                commit_key: "k1".to_string(),
                commit_id: "c2".to_string(),
                enqueued_at_epoch_ms: 101,
            },
        )
        .expect_err("conflicting commit ids should be rejected");
        assert!(matches!(
            conflict,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));
    }

    #[test]
    fn process_workspace_git_push_queue_retries_and_recovers() {
        let workspace = TempDir::new("push-retry-recover-workspace");
        workspace.create();
        let remote = TempDir::new("push-retry-recover-remote");
        let state_root = state_root(&workspace);
        let remote_url = remote.path.to_string_lossy().to_string();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            ("CRAB_WORKSPACE_GIT_REMOTE", &remote_url),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("workspace git bootstrap should succeed");
        write_fixture(&workspace.path, "state/a.txt", "hello\n");
        let commit = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("commit should succeed");
        let commit_key = commit
            .commit_key
            .clone()
            .expect("commit key should be present");
        let commit_id = commit
            .commit_id
            .clone()
            .expect("commit id should be present");
        enqueue_workspace_git_push_request(
            &state_root,
            &config,
            &WorkspaceGitPushRequest {
                commit_key: commit_key.clone(),
                commit_id: commit_id.clone(),
                enqueued_at_epoch_ms: 1_000,
            },
        )
        .expect("enqueue should succeed");

        let failed = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1_000)
            .expect("failed push should still return outcome");
        assert!(failed.attempted);
        assert!(!failed.pushed);
        assert_eq!(failed.commit_key, Some(commit_key.clone()));
        assert_eq!(failed.next_backoff_ms, Some(1_000));
        assert_eq!(failed.skipped_reason, Some("retry_scheduled".to_string()));

        initialize_bare_remote(&remote.path);

        let early = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1_500)
            .expect("early retry should be deferred");
        assert!(!early.attempted);
        assert_eq!(early.skipped_reason, Some("no_due_entries".to_string()));

        let recovered =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 2_000)
                .expect("retry after backoff should succeed");
        assert!(recovered.attempted);
        assert!(recovered.pushed);
        assert_eq!(recovered.commit_key, Some(commit_key));
        assert_eq!(read_push_queue_entries(&state_root).len(), 0);

        let remote_head = run_git_in(&remote.path, &["rev-parse", "refs/heads/main"]);
        assert_eq!(remote_head.trim(), commit_id);
    }

    #[test]
    fn process_workspace_git_push_queue_requires_manual_recovery_on_non_fast_forward() {
        let workspace = TempDir::new("push-non-fast-forward-workspace");
        workspace.create();
        let remote = TempDir::new("push-non-fast-forward-remote");
        let state_root = state_root(&workspace);
        let remote_url = remote.path.to_string_lossy().to_string();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            ("CRAB_WORKSPACE_GIT_REMOTE", &remote_url),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("workspace git bootstrap should succeed");
        initialize_bare_remote(&remote.path);

        write_fixture(&workspace.path, "state/one.txt", "one\n");
        let first_commit = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("first commit should succeed");
        enqueue_workspace_git_push_request(
            &state_root,
            &config,
            &WorkspaceGitPushRequest {
                commit_key: first_commit
                    .commit_key
                    .clone()
                    .expect("first commit key should exist"),
                commit_id: first_commit
                    .commit_id
                    .clone()
                    .expect("first commit id should exist"),
                enqueued_at_epoch_ms: 1_000,
            },
        )
        .expect("first enqueue should succeed");
        let first_push =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1_000)
                .expect("first push should succeed");
        assert!(first_push.pushed);

        let other = TempDir::new("push-non-fast-forward-other");
        other.create();
        run_git_in(&other.path, &["init"]);
        run_git_in(&other.path, &["remote", "add", "origin", &remote_url]);
        run_git_in(&other.path, &["fetch", "origin", "main"]);
        run_git_in(&other.path, &["checkout", "-B", "main", "FETCH_HEAD"]);
        commit_file(&other.path, "state/from-other.txt");
        run_git_in(&other.path, &["push", "origin", "HEAD:refs/heads/main"]);

        write_fixture(&workspace.path, "state/two.txt", "two\n");
        let second_commit = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                run_id: "run:discord:channel:777:message-2".to_string(),
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect("second commit should succeed");
        enqueue_workspace_git_push_request(
            &state_root,
            &config,
            &WorkspaceGitPushRequest {
                commit_key: second_commit
                    .commit_key
                    .clone()
                    .expect("second commit key should exist"),
                commit_id: second_commit
                    .commit_id
                    .clone()
                    .expect("second commit id should exist"),
                enqueued_at_epoch_ms: 2_000,
            },
        )
        .expect("second enqueue should succeed");
        let second_push =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 2_000)
                .expect("non-fast-forward should still return outcome");
        assert!(second_push.attempted);
        assert!(!second_push.pushed);
        assert!(second_push.exhausted);
        assert_eq!(
            second_push.skipped_reason,
            Some("manual_recovery_required".to_string())
        );
        assert_eq!(
            second_push.failure_kind,
            Some("non_fast_forward".to_string())
        );
        assert_eq!(
            second_push.recovery_commands,
            workspace_git_recovery_commands(&config)
        );

        let entries = read_push_queue_entries(&state_root);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].exhausted);
        assert_eq!(entries[0].attempt_count, 1);
        assert_eq!(entries[0].next_attempt_at_epoch_ms, u64::MAX);
    }

    #[test]
    fn process_workspace_git_push_queue_marks_entries_exhausted_after_bound() {
        let workspace = TempDir::new("push-exhaust-workspace");
        workspace.create();
        let state_root = state_root(&workspace);
        let remote_path = workspace.child("missing-remote.git");
        let remote_url = remote_path.to_string_lossy().to_string();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            ("CRAB_WORKSPACE_GIT_REMOTE", &remote_url),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("workspace git bootstrap should succeed");
        write_fixture(&workspace.path, "state/b.txt", "hello\n");
        let commit = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("commit should succeed");
        enqueue_workspace_git_push_request(
            &state_root,
            &config,
            &WorkspaceGitPushRequest {
                commit_key: commit
                    .commit_key
                    .clone()
                    .expect("commit key should be present"),
                commit_id: commit.commit_id.expect("commit id should be present"),
                enqueued_at_epoch_ms: 1_000,
            },
        )
        .expect("enqueue should succeed");

        let mut now_epoch_ms = 1_000;
        let mut last_outcome = None;
        for _ in 0..WORKSPACE_GIT_PUSH_MAX_ATTEMPTS {
            let outcome = process_workspace_git_push_queue(
                &workspace.path,
                &state_root,
                &config,
                now_epoch_ms,
            )
            .expect("tick should succeed");
            now_epoch_ms = now_epoch_ms.saturating_add(1_000_000);
            last_outcome = Some(outcome);
        }

        let exhausted = last_outcome.expect("final outcome should exist");
        assert!(exhausted.attempted);
        assert!(!exhausted.pushed);
        assert!(exhausted.exhausted);
        assert_eq!(
            exhausted.skipped_reason,
            Some("retry_exhausted".to_string())
        );

        let entries = read_push_queue_entries(&state_root);
        assert_eq!(entries.len(), 1);
        assert!(entries[0].exhausted);
        assert_eq!(entries[0].attempt_count, WORKSPACE_GIT_PUSH_MAX_ATTEMPTS);
        assert_eq!(entries[0].next_attempt_at_epoch_ms, u64::MAX);
    }

    #[test]
    fn workspace_git_push_disabled_and_validation_paths_are_strict() {
        let workspace = TempDir::new("push-disabled-validation");
        workspace.create();
        let state_root = state_root(&workspace);
        let disabled_config = parse_workspace_git(&[]);
        let on_commit_config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);

        let disabled_enqueue = enqueue_workspace_git_push_request(
            &state_root,
            &disabled_config,
            &WorkspaceGitPushRequest {
                commit_key: "k".to_string(),
                commit_id: "c".to_string(),
                enqueued_at_epoch_ms: 1,
            },
        )
        .expect("disabled enqueue should succeed");
        assert_eq!(disabled_enqueue, WorkspaceGitPushEnqueueOutcome::disabled());

        let disabled_tick =
            process_workspace_git_push_queue(&workspace.path, &state_root, &disabled_config, 1)
                .expect("disabled tick should succeed");
        assert_eq!(disabled_tick, WorkspaceGitPushTickOutcome::disabled());

        let blank_key = enqueue_workspace_git_push_request(
            &state_root,
            &on_commit_config,
            &WorkspaceGitPushRequest {
                commit_key: "  ".to_string(),
                commit_id: "c".to_string(),
                enqueued_at_epoch_ms: 1,
            },
        )
        .expect_err("blank key should fail");
        assert_eq!(
            blank_key,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "commit_key must not be empty".to_string(),
            }
        );

        let blank_id = enqueue_workspace_git_push_request(
            &state_root,
            &on_commit_config,
            &WorkspaceGitPushRequest {
                commit_key: "k".to_string(),
                commit_id: " ".to_string(),
                enqueued_at_epoch_ms: 1,
            },
        )
        .expect_err("blank id should fail");
        assert_eq!(
            blank_id,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "commit_id must not be empty".to_string(),
            }
        );

        let zero_epoch = enqueue_workspace_git_push_request(
            &state_root,
            &on_commit_config,
            &WorkspaceGitPushRequest {
                commit_key: "k".to_string(),
                commit_id: "c".to_string(),
                enqueued_at_epoch_ms: 0,
            },
        )
        .expect_err("zero epoch should fail");
        assert_eq!(
            zero_epoch,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "enqueued_at_epoch_ms must be greater than 0".to_string(),
            }
        );

        let zero_now =
            process_workspace_git_push_queue(&workspace.path, &state_root, &on_commit_config, 0)
                .expect_err("zero now should fail");
        assert_eq!(
            zero_now,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn workspace_git_push_queue_handles_empty_no_due_and_exhausted_entries() {
        let workspace = TempDir::new("push-queue-scheduling");
        workspace.create();
        let state_root = state_root(&workspace);
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);

        let empty = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1_000)
            .expect("empty queue should be handled");
        assert_eq!(empty.skipped_reason, Some("queue_empty".to_string()));

        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![
                    WorkspaceGitPushQueueEntry {
                        commit_key: "exhausted".to_string(),
                        commit_id: "c1".to_string(),
                        enqueued_at_epoch_ms: 10,
                        next_attempt_at_epoch_ms: u64::MAX,
                        attempt_count: WORKSPACE_GIT_PUSH_MAX_ATTEMPTS,
                        exhausted: true,
                        last_error: Some("final".to_string()),
                    },
                    WorkspaceGitPushQueueEntry {
                        commit_key: "future-1".to_string(),
                        commit_id: "c2".to_string(),
                        enqueued_at_epoch_ms: 10,
                        next_attempt_at_epoch_ms: 5_000,
                        attempt_count: 1,
                        exhausted: false,
                        last_error: None,
                    },
                    WorkspaceGitPushQueueEntry {
                        commit_key: "future-2".to_string(),
                        commit_id: "c3".to_string(),
                        enqueued_at_epoch_ms: 10,
                        next_attempt_at_epoch_ms: 3_000,
                        attempt_count: 1,
                        exhausted: false,
                        last_error: None,
                    },
                ],
            },
        );

        let no_due = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 2_000)
            .expect("future queue should be deferred");
        assert_eq!(no_due.skipped_reason, Some("no_due_entries".to_string()));
        assert_eq!(no_due.next_due_at_epoch_ms, Some(3_000));
    }

    #[test]
    fn workspace_git_push_queue_rejects_malformed_or_inconsistent_queue_files() {
        let workspace = TempDir::new("push-queue-corruption");
        workspace.create();
        let state_root = state_root(&workspace);
        let queue_path = workspace_git_push_queue_path(&state_root);
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);

        fs::create_dir_all(&state_root).expect("state root should exist");
        fs::write(&queue_path, "{ not-json").expect("corrupt queue fixture should be written");
        let corrupt = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
            .expect_err("corrupt queue should fail");
        assert_eq!(
            corrupt,
            CrabError::CorruptData {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                path: queue_path.to_string_lossy().to_string(),
            }
        );

        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: 2,
                entries: Vec::new(),
            },
        );
        let wrong_version =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
                .expect_err("unsupported queue version should fail");
        assert_eq!(
            wrong_version,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "unsupported queue state version 2; expected 1".to_string(),
            }
        );

        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: " ".to_string(),
                    commit_id: "c".to_string(),
                    enqueued_at_epoch_ms: 1,
                    next_attempt_at_epoch_ms: 1,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        );
        let invalid_entry =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
                .expect_err("invalid queue entry should fail");
        assert_eq!(
            invalid_entry,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "queue entry commit_key must not be empty".to_string(),
            }
        );

        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: "k".to_string(),
                    commit_id: "  ".to_string(),
                    enqueued_at_epoch_ms: 1,
                    next_attempt_at_epoch_ms: 1,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        );
        let invalid_commit_id =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
                .expect_err("blank queue commit id should fail");
        assert_eq!(
            invalid_commit_id,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "queue entry commit_id must not be empty".to_string(),
            }
        );

        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: "k".to_string(),
                    commit_id: "c".to_string(),
                    enqueued_at_epoch_ms: 0,
                    next_attempt_at_epoch_ms: 1,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        );
        let invalid_enqueued_at =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
                .expect_err("zero enqueued timestamp should fail");
        assert_eq!(
            invalid_enqueued_at,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "queue entry enqueued_at_epoch_ms must be greater than 0".to_string(),
            }
        );

        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: "k".to_string(),
                    commit_id: "c".to_string(),
                    enqueued_at_epoch_ms: 1,
                    next_attempt_at_epoch_ms: 0,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        );
        let invalid_next_attempt =
            process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
                .expect_err("zero next-attempt timestamp should fail");
        assert_eq!(
            invalid_next_attempt,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "queue entry next_attempt_at_epoch_ms must be greater than 0".to_string(),
            }
        );

        let _ = fs::remove_file(&queue_path);
        fs::create_dir_all(&queue_path).expect("queue path directory fixture should be created");
        let read_io = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
            .expect_err("queue path directory should fail read");
        assert!(matches!(
            read_io,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));
    }

    #[test]
    fn workspace_git_push_persist_reports_temp_write_and_rename_failures() {
        let workspace = TempDir::new("push-persist-io");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);
        let request = WorkspaceGitPushRequest {
            commit_key: "k1".to_string(),
            commit_id: "c1".to_string(),
            enqueued_at_epoch_ms: 1,
        };

        let write_fail_root = workspace.child("state-write-fail");
        fs::create_dir_all(&write_fail_root).expect("state root should exist");
        let write_fail_queue = workspace_git_push_queue_path(&write_fail_root);
        fs::create_dir_all(format!("{}.tmp", write_fail_queue.to_string_lossy()))
            .expect("tmp path directory should exist");
        let write_error = enqueue_workspace_git_push_request(&write_fail_root, &config, &request)
            .expect_err("temp write should fail");
        assert!(matches!(
            write_error,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));

        let rename_fail_root = workspace.child("state-rename-fail");
        fs::create_dir_all(&rename_fail_root).expect("state root should exist");
        let rename_fail_queue = workspace_git_push_queue_path(&rename_fail_root);
        fs::create_dir_all(&rename_fail_queue).expect("queue path directory should exist");
        let rename_error = enqueue_workspace_git_push_request(&rename_fail_root, &config, &request)
            .expect_err("rename should fail");
        assert!(matches!(
            rename_error,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));

        let direct_rename_path = workspace.child("state-direct-rename-fail/queue.json");
        fs::create_dir_all(&direct_rename_path).expect("target directory should exist");
        let direct_rename = persist_workspace_git_push_queue(
            &direct_rename_path,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: "k2".to_string(),
                    commit_id: "c2".to_string(),
                    enqueued_at_epoch_ms: 1,
                    next_attempt_at_epoch_ms: 1,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        )
        .expect_err("rename into directory path should fail");
        assert!(matches!(
            direct_rename,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));
    }

    #[test]
    fn workspace_git_push_missing_remote_configuration_surfaces_actionable_failure() {
        let workspace = TempDir::new("push-missing-remote");
        workspace.create();
        initialize_repo(&workspace.path, "main");
        let state_root = state_root(&workspace);
        let config = crate::WorkspaceGitConfig {
            enabled: true,
            remote: None,
            branch: "main".to_string(),
            commit_name: "Crab Runtime".to_string(),
            commit_email: "crab-runtime@example.com".to_string(),
            push_policy: WorkspaceGitPushPolicy::OnCommit,
        };
        write_push_queue_state(
            &state_root,
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: "k1".to_string(),
                    commit_id: "deadbeef".to_string(),
                    enqueued_at_epoch_ms: 1,
                    next_attempt_at_epoch_ms: 1,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        );

        let outcome = process_workspace_git_push_queue(&workspace.path, &state_root, &config, 1)
            .expect("missing remote should be returned as retryable failure");
        assert!(outcome.attempted);
        assert!(!outcome.pushed);
        assert_eq!(outcome.skipped_reason, Some("retry_scheduled".to_string()));
        assert_eq!(
            outcome.failure,
            Some("workspace git remote is missing for on-commit push policy".to_string())
        );
    }

    #[test]
    fn workspace_commit_request_validation_is_strict() {
        let workspace = TempDir::new("commit-validate");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "manual"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &config)
            .expect("repository bootstrap should succeed");

        let blank_session = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                logical_session_id: "   ".to_string(),
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect_err("blank logical session id should fail");
        assert_eq!(
            blank_session,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "logical_session_id must not be empty".to_string(),
            }
        );

        let zero_epoch = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                emitted_at_epoch_ms: 0,
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect_err("zero emitted timestamp should fail");
        assert_eq!(
            zero_epoch,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "emitted_at_epoch_ms must be greater than 0".to_string(),
            }
        );

        let blank_run_id = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                run_id: " ".to_string(),
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect_err("blank run id should fail");
        assert_eq!(
            blank_run_id,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "run_id must not be empty".to_string(),
            }
        );

        let blank_checkpoint_id = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                checkpoint_id: Some(" ".to_string()),
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect_err("blank checkpoint id should fail");
        assert_eq!(
            blank_checkpoint_id,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "checkpoint_id must not be empty".to_string(),
            }
        );

        let blank_run_status = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                run_status: Some(" ".to_string()),
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect_err("blank run status should fail");
        assert_eq!(
            blank_run_status,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_COMMIT_CONTEXT,
                message: "run_status must not be empty".to_string(),
            }
        );

        let run_status_none = maybe_commit_workspace_snapshot(
            &workspace.path,
            &config,
            &WorkspaceGitCommitRequest {
                run_status: None,
                ..sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized)
            },
        )
        .expect("missing run_status should be accepted");
        assert_eq!(
            run_status_none.skipped_reason,
            Some("no_changes".to_string())
        );
    }

    #[test]
    fn push_queue_layout_validation_is_strict() {
        let workspace = TempDir::new("push-layout-validation");
        workspace.create();
        let config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            (
                "CRAB_WORKSPACE_GIT_REMOTE",
                "git@github.com:fontanierh/private.git",
            ),
        ]);
        let request = WorkspaceGitPushRequest {
            commit_key: "k".to_string(),
            commit_id: "c".to_string(),
            enqueued_at_epoch_ms: 1,
        };

        let blank_layout = enqueue_workspace_git_push_request(Path::new(""), &config, &request)
            .expect_err("blank state root should fail");
        assert_eq!(
            blank_layout,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "state_root must not be empty".to_string(),
            }
        );

        let state_root_file = workspace.child("state-root-file");
        fs::write(&state_root_file, "payload").expect("state root file fixture should be writable");
        let file_layout = enqueue_workspace_git_push_request(&state_root_file, &config, &request)
            .expect_err("file state root should fail");
        assert!(matches!(
            file_layout,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));

        let blank_runtime_layout =
            process_workspace_git_push_queue(&workspace.path, Path::new(""), &config, 1)
                .expect_err("blank state root should fail at runtime");
        assert_eq!(
            blank_runtime_layout,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "state_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn process_workspace_git_push_queue_surfaces_persist_failures_for_retry_and_success_paths() {
        let workspace = TempDir::new("push-process-persist-failures");
        workspace.create();

        let remote_retry_path = workspace.child("missing-remote.git");
        let remote_retry_url = remote_retry_path.to_string_lossy().to_string();
        let retry_config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            ("CRAB_WORKSPACE_GIT_REMOTE", &remote_retry_url),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
        ]);
        ensure_workspace_git_repository(&workspace.path, &retry_config)
            .expect("workspace git bootstrap should succeed");
        write_fixture(&workspace.path, "state/retry.txt", "hello\n");
        let retry_commit = maybe_commit_workspace_snapshot(
            &workspace.path,
            &retry_config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("retry commit should succeed");
        let retry_state_root = workspace.child("state-retry");
        enqueue_workspace_git_push_request(
            &retry_state_root,
            &retry_config,
            &WorkspaceGitPushRequest {
                commit_key: retry_commit
                    .commit_key
                    .expect("retry commit key should be present"),
                commit_id: retry_commit
                    .commit_id
                    .expect("retry commit id should be present"),
                enqueued_at_epoch_ms: 1,
            },
        )
        .expect("retry enqueue should succeed");
        let retry_queue_path = workspace_git_push_queue_path(&retry_state_root);
        fs::create_dir_all(format!("{}.tmp", retry_queue_path.to_string_lossy()))
            .expect("retry tmp path directory should exist");
        let retry_persist_error =
            process_workspace_git_push_queue(&workspace.path, &retry_state_root, &retry_config, 1)
                .expect_err("retry persist failure should surface");
        assert!(matches!(
            retry_persist_error,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));

        let remote_success = TempDir::new("push-process-persist-success-remote");
        initialize_bare_remote(&remote_success.path);
        let remote_success_url = remote_success.path.to_string_lossy().to_string();
        let success_config = parse_workspace_git(&[
            ("CRAB_WORKSPACE_GIT_PERSISTENCE_ENABLED", "true"),
            ("CRAB_WORKSPACE_GIT_PUSH_POLICY", "on-commit"),
            ("CRAB_WORKSPACE_GIT_REMOTE", &remote_success_url),
            ("CRAB_WORKSPACE_GIT_BRANCH", "main"),
        ]);
        let success_workspace = TempDir::new("push-process-persist-success-workspace");
        success_workspace.create();
        ensure_workspace_git_repository(&success_workspace.path, &success_config)
            .expect("success workspace bootstrap should succeed");
        write_fixture(&success_workspace.path, "state/success.txt", "hello\n");
        let success_commit = maybe_commit_workspace_snapshot(
            &success_workspace.path,
            &success_config,
            &sample_commit_request(WorkspaceGitCommitTrigger::RunFinalized),
        )
        .expect("success commit should succeed");
        let success_state_root = state_root(&success_workspace);
        enqueue_workspace_git_push_request(
            &success_state_root,
            &success_config,
            &WorkspaceGitPushRequest {
                commit_key: success_commit
                    .commit_key
                    .expect("success commit key should be present"),
                commit_id: success_commit
                    .commit_id
                    .expect("success commit id should be present"),
                enqueued_at_epoch_ms: 1,
            },
        )
        .expect("success enqueue should succeed");
        let success_queue_path = workspace_git_push_queue_path(&success_state_root);
        fs::create_dir_all(format!("{}.tmp", success_queue_path.to_string_lossy()))
            .expect("success tmp path directory should exist");
        let success_persist_error = process_workspace_git_push_queue(
            &success_workspace.path,
            &success_state_root,
            &success_config,
            1,
        )
        .expect_err("success-path persist failure should surface");
        assert!(matches!(
            success_persist_error,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));
    }

    #[test]
    fn push_queue_helpers_cover_missing_file_and_validation_error_paths() {
        let workspace = TempDir::new("push-queue-helper-paths");
        workspace.create();
        let state_root = state_root(&workspace);

        let missing_entries = read_push_queue_entries(&state_root);
        assert!(missing_entries.is_empty());

        let invalid_persist = persist_workspace_git_push_queue(
            &workspace.child("queue.json"),
            &WorkspaceGitPushQueueState {
                version: WORKSPACE_GIT_PUSH_QUEUE_VERSION,
                entries: vec![WorkspaceGitPushQueueEntry {
                    commit_key: "k".to_string(),
                    commit_id: "c".to_string(),
                    enqueued_at_epoch_ms: 1,
                    next_attempt_at_epoch_ms: 0,
                    attempt_count: 0,
                    exhausted: false,
                    last_error: None,
                }],
            },
        )
        .expect_err("invalid queue state should be rejected before write");
        assert_eq!(
            invalid_persist,
            CrabError::InvariantViolation {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                message: "queue entry next_attempt_at_epoch_ms must be greater than 0".to_string(),
            }
        );

        let layout_file = workspace.child("layout-file");
        fs::write(&layout_file, "payload").expect("layout file should be writable");
        let layout_error =
            ensure_push_queue_layout(&layout_file).expect_err("layout file should be rejected");
        assert!(matches!(
            layout_error,
            CrabError::Io {
                context: WORKSPACE_GIT_PUSH_QUEUE_CONTEXT,
                ..
            }
        ));
    }
}
