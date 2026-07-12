use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::OsStringExt;

use super::*;

#[test]
fn validates_bounds_and_checked_round_count() {
    assert_eq!(validate_counts(0, 60).unwrap(), 1);
    assert_eq!(validate_counts(100, 86_400).unwrap(), 101);
    assert!(validate_counts(101, 60).is_err());
    assert!(validate_counts(0, 59).is_err());
    assert!(validate_counts(0, 86_401).is_err());
    assert!(checked_review_rounds(u32::MAX).is_err());
    assert_eq!(Effort::parse("high").unwrap(), Effort::High);
    assert_eq!(Effort::parse("max").unwrap(), Effort::Max);
    assert!(Effort::parse("medium").is_err());
    assert_eq!(validate_cohort_size("--plan-critics", 1).unwrap(), 1);
    assert_eq!(validate_cohort_size("--plan-critics", 8).unwrap(), 8);
    assert!(validate_cohort_size("--plan-critics", 0).is_err());
    assert!(validate_cohort_size("--plan-critics", 9).is_err());
}

#[test]
fn sanitizes_and_derives_bounded_names() {
    assert_eq!(sanitize_run_id("A useful run!").unwrap(), "a-useful-run");
    assert_eq!(sanitize_run_id("__A__").unwrap(), "__a__");
    assert!(sanitize_run_id("!!!").is_err());
    assert!(sanitize_run_id(&"a".repeat(65)).is_err());
    assert!(default_run_id(b"request").unwrap().ends_with("-1f58b914"));
    assert_eq!(
        default_run_id_at(
            b"request",
            OffsetDateTime::parse(
                "2026-07-11T12:34:56Z",
                &time::format_description::well_known::Rfc3339,
            )
            .unwrap(),
        )
        .unwrap(),
        "20260711-123456-1f58b914"
    );
    assert!(
        default_run_id_with_description(b"request", OffsetDateTime::UNIX_EPOCH, "[invalid",)
            .is_err()
    );
    assert_eq!(proc_name_for("short"), "code-factory-short");
    let long = proc_name_for(&"a".repeat(64));
    assert_eq!(long.len(), 64);
    assert!(long.starts_with("code-factory-"));
}

#[test]
fn environment_filter_allows_only_documented_names() {
    let filtered = allowlisted_environment_from([
        (OsString::from("PATH"), OsString::from("/bin")),
        (OsString::from("OPENAI_TOKEN"), OsString::from("yes")),
        (
            OsString::from("HTTP_PROXY"),
            OsString::from("http://proxy.invalid"),
        ),
        (
            OsString::from("CODEX_SANDBOX_NETWORK_DISABLED"),
            OsString::from("1"),
        ),
        (OsString::from("CODEX_SANDBOX"), OsString::from("read-only")),
        (
            OsString::from("CLAUDE_DISABLE_NETWORK"),
            OsString::from("1"),
        ),
        (OsString::from("OPENAI_OFFLINE"), OsString::from("1")),
        (OsString::from("ANTHROPIC_NO_NETWORK"), OsString::from("1")),
        (OsString::from("DISCORD_TOKEN"), OsString::from("no")),
        (OsString::from("GIT_DIR"), OsString::from("no")),
        (OsString::from("MAKEFLAGS"), OsString::from("no")),
        (OsString::from("CARGO_TARGET_DIR"), OsString::from("no")),
    ]);
    assert_eq!(filtered.len(), 3);
    assert_eq!(filtered[OsStr::new("OPENAI_TOKEN")], "yes");
    assert_eq!(filtered[OsStr::new("HTTP_PROXY")], "http://proxy.invalid");
    assert!(!environment_name_allowed(OsStr::new("ENV")));
    assert!(environment_name_disables_network(
        "CODEX_SANDBOX_NETWORK_DISABLED"
    ));
    assert!(!environment_name_disables_network("OPENAI_API_KEY"));
    assert!(environment_name_selects_sandbox("CODEX_SANDBOX"));
    assert!(!environment_name_selects_sandbox("CODEX_API_KEY"));
}

#[test]
fn intended_paths_resolve_missing_suffixes_and_symlinks() {
    let root = std::env::temp_dir().join(format!("crab-factory-config-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("real")).unwrap();
    std::os::unix::fs::symlink(root.join("real"), root.join("link")).unwrap();
    let resolved = canonicalize_intended(&root.join("link/a/../b/c")).unwrap();
    assert_eq!(
        resolved,
        fs::canonicalize(root.join("real")).unwrap().join("b/c")
    );
    assert!(paths_overlap(&root.join("x"), &root.join("x/y")));
    assert!(!paths_overlap(&root.join("x"), &root.join("y")));
    assert_eq!(normalize_path(Path::new("./a/../b")), PathBuf::from("b"));
    assert!(canonicalize_intended_with_current_dir(
        Path::new("relative"),
        Err(std::io::Error::other("current directory failure")),
    )
    .is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn roots_and_executable_resolution_cover_defaults_and_missing_environment() {
    let _environment = match crate::FACTORY_ENV_LOCK.lock() {
        Ok(lock) => lock,
        Err(error) => error.into_inner(),
    };
    let root = std::env::temp_dir().join(format!("crab-factory-config-env-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let old_home = std::env::var_os("HOME");
    let old_path = std::env::var_os("PATH");
    std::env::set_var("HOME", &root);
    let defaults = resolve_roots(None, None, &root).unwrap();
    assert!(defaults.artifact_root.ends_with(".crab/code-factory/runs"));
    let relatives =
        resolve_roots(Some(Path::new("runs")), Some(Path::new("trees")), &root).unwrap();
    assert_eq!(
        relatives.artifact_root,
        fs::canonicalize(&root).unwrap().join("runs")
    );
    assert_eq!(
        canonicalize_intended(Path::new(".")).unwrap(),
        std::env::current_dir().unwrap()
    );
    assert!(resolve_executable(OsStr::new("definitely-missing-factory-tool")).is_err());
    let directory_tool = root.join("directory-tool");
    fs::create_dir(&directory_tool).unwrap();
    assert!(resolve_executable(directory_tool.as_os_str()).is_err());
    let non_executable = root.join("non-executable");
    fs::write(&non_executable, "tool").unwrap();
    assert!(resolve_executable(non_executable.as_os_str()).is_err());
    let executable = resolve_executable(OsStr::new("/usr/bin/true")).unwrap();
    assert!(executable.is_absolute());
    assert!(executable.is_file());
    std::env::remove_var("HOME");
    assert!(resolve_roots(None, None, &root).is_err());
    std::env::remove_var("PATH");
    assert!(resolve_executable(OsStr::new("tool")).is_err());
    match old_home {
        Some(value) => std::env::set_var("HOME", value),
        None => std::env::remove_var("HOME"),
    }
    match old_path {
        Some(value) => std::env::set_var("PATH", value),
        None => std::env::remove_var("PATH"),
    }
    assert!(!environment_name_allowed(&OsString::from_vec(vec![0xff])));
    fs::remove_dir_all(root).unwrap();
}
