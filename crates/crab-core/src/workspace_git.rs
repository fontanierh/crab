use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use crate::config::WorkspaceGitConfig;
use crate::{CrabError, CrabResult};

const WORKSPACE_GIT_CONTEXT: &str = "workspace_git_bootstrap";
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

pub fn ensure_workspace_git_repository(
    workspace_root: &Path,
    config: &WorkspaceGitConfig,
) -> CrabResult<WorkspaceGitEnsureOutcome> {
    validate_workspace_root(workspace_root)?;
    if !config.enabled {
        return Ok(WorkspaceGitEnsureOutcome::disabled());
    }

    let expected_root = canonicalize_path(workspace_root)?;
    if probe_bare_repository(workspace_root)? {
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
        if has_commits(workspace_root)? {
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

fn canonicalize_path(path: &Path) -> CrabResult<PathBuf> {
    fs::canonicalize(path).map_err(|error| CrabError::Io {
        context: WORKSPACE_GIT_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn detect_git_repository_root(workspace_root: &Path) -> CrabResult<Option<PathBuf>> {
    let inside = run_git(workspace_root, &["rev-parse", "--is-inside-work-tree"])?;
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

fn probe_bare_repository(workspace_root: &Path) -> CrabResult<bool> {
    let raw = run_git(workspace_root, &["rev-parse", "--is-bare-repository"])?;
    if !raw.success {
        return Ok(false);
    }
    Ok(raw.stdout.trim() == "true")
}

fn resolve_head_branch(workspace_root: &Path) -> CrabResult<String> {
    let args = ["symbolic-ref", "--quiet", "--short", "HEAD"];
    let output = run_git_checked(workspace_root, &args)?;
    Ok(output.trim().to_string())
}

fn has_commits(workspace_root: &Path) -> CrabResult<bool> {
    let output = run_git(workspace_root, &["rev-parse", "--verify", "HEAD"])?;
    Ok(output.success)
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

fn run_git(workspace_root: &Path, args: &[&str]) -> CrabResult<CommandOutput> {
    run_command(GIT_BINARY, workspace_root, args)
}

fn run_git_checked(workspace_root: &Path, args: &[&str]) -> CrabResult<String> {
    let output = run_git(workspace_root, args)?;
    if output.success {
        return Ok(output.stdout);
    }

    Err(CrabError::InvariantViolation {
        context: WORKSPACE_GIT_CONTEXT,
        message: format!("git {} failed: {}", args.join(" "), output.stderr.trim()),
    })
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
        canonicalize_path, detect_git_repository_root, ensure_workspace_git_repository,
        has_commits, read_origin_remote, resolve_head_branch, run_command, run_git_checked,
        validate_workspace_root, WorkspaceGitEnsureOutcome, WORKSPACE_GIT_CONTEXT,
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

        assert!(!has_commits(&workspace.path).expect("empty repo should report no commits"));
        commit_file(&workspace.path, "data.txt");
        assert!(has_commits(&workspace.path).expect("commit should be detected"));

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
}
