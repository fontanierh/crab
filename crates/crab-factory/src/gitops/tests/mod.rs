use std::ffi::OsStr;
use std::os::unix::fs::PermissionsExt;
use std::process::Command;
use std::sync::atomic::AtomicU64;

use crate::config::resolve_executable;

use super::*;

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn repo() -> (PathBuf, GitRunner, String) {
    let root = std::env::temp_dir().join(format!(
        "crab-factory-git-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("sub")).unwrap();
    let git = resolve_executable(OsStr::new("git")).unwrap();
    for args in [
        vec!["init", "-q"],
        vec!["config", "user.name", "Factory Test"],
        vec!["config", "user.email", "factory@example.invalid"],
    ] {
        assert!(Command::new(&git)
            .args(args)
            .current_dir(&root)
            .status()
            .unwrap()
            .success());
    }
    fs::write(root.join("tracked"), "one\n").unwrap();
    fs::write(root.join(".gitignore"), "ignored\n").unwrap();
    assert!(Command::new(&git)
        .args(["add", "."])
        .current_dir(&root)
        .status()
        .unwrap()
        .success());
    assert!(Command::new(&git)
        .args(["commit", "-qm", "base"])
        .current_dir(&root)
        .status()
        .unwrap()
        .success());
    let runner = GitRunner::new(
        git,
        Arc::new(AtomicBool::new(false)),
        Duration::from_secs(5),
    );
    let sha = runner.resolve_commit(&root, "HEAD").unwrap();
    (root, runner, sha)
}

#[test]
fn discovers_subdirectory_and_resolves_repository_state() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let (root, git, sha) = repo();
    assert_eq!(
        git.discover_toplevel(&root.join("sub")).unwrap(),
        fs::canonicalize(&root).unwrap()
    );
    assert_eq!(git.head_sha(&root).unwrap(), sha);
    assert!(git.source_status(&root).unwrap().is_empty());
    assert!(!git.branch_exists(&root, "factory/missing").unwrap());
    assert!(Command::new("git")
        .args(["branch", "factory/existing"])
        .current_dir(&root)
        .status()
        .unwrap()
        .success());
    assert!(git.branch_exists(&root, "factory/existing").unwrap());
    let failing_git = root.join("failing-git");
    fs::write(&failing_git, "#!/bin/sh\nexit 2\n").unwrap();
    fs::set_permissions(&failing_git, fs::Permissions::from_mode(0o755)).unwrap();
    let failing = GitRunner::new(
        failing_git,
        Arc::new(AtomicBool::new(false)),
        Duration::from_secs(1),
    );
    assert!(failing.branch_exists(&root, "factory/existing").is_err());
    assert!(git.resolve_commit(&root, "missing").is_err());
    assert!(git.checked([OsString::from("not-a-git-command")]).is_err());
    assert!(git.discover_toplevel(&std::env::temp_dir()).is_err());
    assert!(required_stdout(b"", "empty").is_err());
    assert!(required_stdout(&[0xff], "invalid").is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn fingerprint_tracks_untracked_content_and_ignores_ignored_files() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let (root, git, sha) = repo();
    let clean = git.fingerprint(&root).unwrap();
    fs::write(root.join("untracked"), "one").unwrap();
    let one = git.fingerprint(&root).unwrap();
    assert!(assert_unchanged(&clean, &one, "test").is_err());
    fs::write(root.join("untracked"), "two").unwrap();
    let two = git.fingerprint(&root).unwrap();
    assert_eq!(one.changed_paths(&two), vec!["untracked"]);
    fs::write(root.join("ignored"), "ignored").unwrap();
    assert_eq!(two, git.fingerprint(&root).unwrap());
    std::os::unix::fs::symlink("tracked", root.join("untracked-link")).unwrap();
    let linked = git.fingerprint(&root).unwrap();
    assert!(linked.files.contains_key("untracked-link"));
    fs::remove_file(root.join("untracked-link")).unwrap();
    fs::remove_file(root.join("untracked")).unwrap();
    assert!(assert_unchanged(&two, &git.fingerprint(&root).unwrap(), "test").is_err());
    fs::remove_file(root.join("tracked")).unwrap();
    assert!(git
        .fingerprint(&root)
        .unwrap()
        .files
        .contains_key("tracked"));
    assert_identity(
        &git,
        &root,
        &sha,
        git.symbolic_branch(&root)
            .unwrap()
            .trim_start_matches("refs/heads/"),
        "test",
    )
    .unwrap();
    assert!(assert_identity(&git, &root, "wrong", "wrong", "test").is_err());
    assert!(assert_identity(&git, &root, &sha, "wrong", "test").is_err());
    assert_eq!(
        status_paths(b"R  old\0new\0C  copy\0target\0malformed\0"),
        vec![
            b"copy".to_vec(),
            b"new".to_vec(),
            b"old".to_vec(),
            b"target".to_vec(),
        ]
    );
    let synthetic = GitOutput {
        code: 2,
        stdout: b"stdout diagnostic".to_vec(),
        stderr: Vec::new(),
    };
    assert!(command_failure("git test", 2, &synthetic)
        .to_string()
        .contains("stdout diagnostic"));
    let same_files = WorktreeFingerprint {
        digest: "different".to_string(),
        files: two.files.clone(),
    };
    assert!(assert_unchanged(&two, &same_files, "test")
        .unwrap_err()
        .to_string()
        .contains("git status changed"));
    let cancelled = GitRunner::new(
        resolve_executable(OsStr::new("git")).unwrap(),
        Arc::new(AtomicBool::new(true)),
        Duration::from_secs(1),
    );
    assert!(cancelled.head_sha(&root).is_err());
    let missing = GitRunner::new(
        root.join("missing-git"),
        Arc::new(AtomicBool::new(false)),
        Duration::from_secs(1),
    );
    assert!(missing.head_sha(&root).is_err());
    fs::remove_dir_all(root).unwrap();
}
