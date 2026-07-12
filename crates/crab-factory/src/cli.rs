use std::io::Write;
use std::path::PathBuf;

use crate::config::{
    validate_cohort_size, Effort, LaunchOptions, DEFAULT_CODEX_REVIEWERS, DEFAULT_EFFORT,
    DEFAULT_PLAN_CRITICS, DEFAULT_TIMEOUT_SECONDS,
};
use crate::launch::{run_foreground, start_background};
use crate::orchestrator::execute_run;
use crate::preflight::{prepare_run, RequestedMode};
use crate::{result_context, FactoryError, FactoryResult};

const USAGE: &str = "Usage:
  crab-factory run --prompt-file <request.md> [options]
  crab-factory start --prompt-file <request.md> [options] [--launcher <path>]
  crab-factory exec --run-dir <dir> --request-sha256 <hex>
  crab-factory status --run-dir <dir> [--json]
  crab-factory steer --run-dir <dir> (--message <text> | --message-file <path>)
  crab-factory configure --run-dir <dir> [--effort high|max] [--plan-critics N] [--codex-reviewers N]

Commands:
  run      execute in the foreground for CI and debugging
  start    prepare and launch a durable background run
  exec     internal single-shot executor for a prepared run
  status   inspect lifecycle, active workers, configuration, and controls
  steer    queue durable operator context for the next stage boundary
  configure queue allowed worker configuration for a future boundary

Security policy:
  Every model worker runs with unrestricted host permissions and network access.
  Codex/Claude nested-agent fan-out remains disabled; advisory stages are mutation-checked.
  Live controls never alter an already-running model process or the original request.

Run/start options:
  --prompt-file <path>              complete original coding request (required)
  --repo <path>                     repository or any subdirectory (default: cwd)
  --base <ref>                      committed worktree base (default: HEAD)
  --run-id <id>                     stable run identifier (sanitized, max 64 chars)
  --additional-review-rounds <N>    rounds after the mandatory first (default: 0, max: 100)
  --effort <high|max>               effort for every worker (default: high)
  --plan-critics <N>                Codex plan critics (default: 2, range: 1-8)
  --codex-reviewers <N>             Codex normal reviewers (default: 2, range: 1-8)
  --artifact-root <path>            artifact root (default: $HOME/.crab/code-factory/runs)
  --worktree-root <path>            worktree root (default: $HOME/.crab/code-factory/worktrees)
  --agent-timeout-seconds <S>       per-process timeout (default: 14400, range: 60-86400)
  --allow-dirty-source              use the committed base despite source-checkout changes
  --launcher <path>                 trusted start-only process-manager adapter
  -h, --help                        show this help

Example:
  crab-factory run --prompt-file request.md --additional-review-rounds 2
";

struct ConfigureOptions {
    run_dir: PathBuf,
    effort: Option<Effort>,
    plan_critics: Option<u8>,
    codex_reviewers: Option<u8>,
}

pub fn run_factory_cli<I, S>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let arguments: Vec<String> = args.into_iter().map(Into::into).collect();
    match dispatch(&arguments, stdout, stderr) {
        Ok(()) => 0,
        Err(error) => {
            let _ = writeln!(stderr, "error: {error}\n\n{USAGE}");
            2
        }
    }
}

fn dispatch(
    arguments: &[String],
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> FactoryResult<()> {
    let args = arguments.get(1..).unwrap_or_default();
    if args.is_empty() || matches!(args, [flag] if matches!(flag.as_str(), "-h" | "--help")) {
        write_help(stdout)?;
        return Ok(());
    }
    let command = require_some!(args.first(), FactoryError::new("missing subcommand"));
    let rest = &args[1..];
    if matches!(rest, [flag] if matches!(flag.as_str(), "-h" | "--help")) {
        write_help(stdout)?;
        return Ok(());
    }
    match command.as_str() {
        "run" => {
            let options = parse_launch_options(rest, false)?;
            let reserved = prepare_run(options, RequestedMode::Run)?;
            run_foreground(reserved, stdout)
        }
        "start" => {
            #[rustfmt::skip]
            let executable = try_mapped!(std::env::current_exe().and_then(std::fs::canonicalize), error => FactoryError::new(format!("could not resolve current executable: {error}")));
            let options = parse_launch_options(rest, true)?;
            let reserved = prepare_run(options, RequestedMode::Start)?;
            start_background(reserved, &executable, stdout, stderr)
        }
        "exec" => {
            let (run_dir, sha256) = parse_exec_options(rest)?;
            execute_run(&run_dir, &sha256, stdout)
        }
        "status" => {
            let (run_dir, json) = parse_status_options(rest)?;
            crate::controls::status(&run_dir, json, stdout)
        }
        "steer" => {
            let (run_dir, message) = parse_steer_options(rest)?;
            crate::controls::steer(&run_dir, message, stdout)
        }
        "configure" => {
            let options = parse_configure_options(rest)?;
            crate::controls::configure(
                &options.run_dir,
                options.effort,
                options.plan_critics,
                options.codex_reviewers,
                stdout,
            )
        }
        unknown => Err(FactoryError::new(format!("unknown subcommand: {unknown}"))),
    }
}

fn parse_status_options(args: &[String]) -> FactoryResult<(PathBuf, bool)> {
    let mut run_dir = None;
    let mut json = false;
    let mut index = 0;
    while index < args.len() {
        if args[index] == "--json" {
            if json {
                return Err(FactoryError::new("duplicate flag: --json"));
            }
            json = true;
            index += 1;
            continue;
        }
        let value = require_some!(
            args.get(index + 1),
            FactoryError::new(format!("missing value for {}", args[index]))
        )
        .clone();
        if args[index] == "--run-dir" {
            assign(&mut run_dir, PathBuf::from(value), "--run-dir")?;
        } else {
            return Err(FactoryError::new(format!("unknown flag: {}", args[index])));
        }
        index += 2;
    }
    Ok((
        require_some!(run_dir, FactoryError::new("--run-dir is required")),
        json,
    ))
}

fn parse_steer_options(args: &[String]) -> FactoryResult<(PathBuf, String)> {
    let mut run_dir = None;
    let mut message = None;
    let mut message_file = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = require_some!(
            args.get(index + 1),
            FactoryError::new(format!("missing value for {flag}"))
        )
        .clone();
        match flag.as_str() {
            "--run-dir" => assign(&mut run_dir, PathBuf::from(value), flag)?,
            "--message" => assign(&mut message, value, flag)?,
            "--message-file" => assign(&mut message_file, PathBuf::from(value), flag)?,
            _ => return Err(FactoryError::new(format!("unknown flag: {flag}"))),
        }
        index += 2;
    }
    if message.is_some() == message_file.is_some() {
        return Err(FactoryError::new(
            "provide exactly one of --message or --message-file",
        ));
    }
    let message = match message {
        Some(value) => value,
        None => {
            let path = require_some!(
                message_file,
                FactoryError::new("--message-file is required")
            );
            let bytes = std::fs::read(&path)
                .map_err(|error| FactoryError::io("read message file", &path, &error))?;
            String::from_utf8(bytes)
                .map_err(|_| FactoryError::new("steering message file is not UTF-8"))?
        }
    };
    Ok((
        require_some!(run_dir, FactoryError::new("--run-dir is required")),
        message,
    ))
}

fn parse_configure_options(args: &[String]) -> FactoryResult<ConfigureOptions> {
    let mut run_dir = None;
    let mut effort = None;
    let mut plan_critics = None;
    let mut codex_reviewers = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = require_some!(
            args.get(index + 1),
            FactoryError::new(format!("missing value for {flag}"))
        )
        .clone();
        match flag.as_str() {
            "--run-dir" => assign(&mut run_dir, PathBuf::from(value), flag)?,
            "--effort" => assign(&mut effort, Effort::parse(&value)?, flag)?,
            "--plan-critics" => assign(
                &mut plan_critics,
                validate_cohort_size(flag, parse_number(flag, &value)?)?,
                flag,
            )?,
            "--codex-reviewers" => assign(
                &mut codex_reviewers,
                validate_cohort_size(flag, parse_number(flag, &value)?)?,
                flag,
            )?,
            _ => return Err(FactoryError::new(format!("unknown flag: {flag}"))),
        }
        index += 2;
    }
    if effort.is_none() && plan_critics.is_none() && codex_reviewers.is_none() {
        return Err(FactoryError::new("configure requires at least one setting"));
    }
    Ok(ConfigureOptions {
        run_dir: require_some!(run_dir, FactoryError::new("--run-dir is required")),
        effort,
        plan_critics,
        codex_reviewers,
    })
}

fn write_help(stdout: &mut dyn Write) -> FactoryResult<()> {
    try_mapped!(
        stdout.write_all(USAGE.as_bytes()),
        error => FactoryError::new(format!("could not write help: {error}"))
    );
    Ok(())
}

fn parse_launch_options(args: &[String], allow_launcher: bool) -> FactoryResult<LaunchOptions> {
    let mut prompt_file = None;
    let mut repo = None;
    let mut base_ref = None;
    let mut run_id = None;
    let mut additional_review_rounds = None;
    let mut artifact_root = None;
    let mut worktree_root = None;
    let mut timeout = None;
    let mut allow_dirty_source = false;
    let mut launcher = None;
    let mut effort = None;
    let mut plan_critics = None;
    let mut codex_reviewers = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        if flag == "--allow-dirty-source" {
            if allow_dirty_source {
                return Err(FactoryError::new("duplicate flag: --allow-dirty-source"));
            }
            allow_dirty_source = true;
            index += 1;
            continue;
        }
        let value = require_some!(
            args.get(index + 1),
            FactoryError::new(format!("missing value for {flag}"))
        )
        .clone();
        match flag.as_str() {
            "--prompt-file" => assign(&mut prompt_file, PathBuf::from(value), flag)?,
            "--repo" => assign(&mut repo, PathBuf::from(value), flag)?,
            "--base" => assign(&mut base_ref, value, flag)?,
            "--run-id" => assign(&mut run_id, value, flag)?,
            "--additional-review-rounds" => {
                let parsed = parse_number::<u32>(flag, &value)?;
                assign(&mut additional_review_rounds, parsed, flag)?;
            }
            "--effort" => assign(&mut effort, Effort::parse(&value)?, flag)?,
            "--plan-critics" => {
                let parsed = validate_cohort_size(flag, parse_number::<u8>(flag, &value)?)?;
                assign(&mut plan_critics, parsed, flag)?;
            }
            "--codex-reviewers" => {
                let parsed = validate_cohort_size(flag, parse_number::<u8>(flag, &value)?)?;
                assign(&mut codex_reviewers, parsed, flag)?;
            }
            "--artifact-root" => assign(&mut artifact_root, PathBuf::from(value), flag)?,
            "--worktree-root" => assign(&mut worktree_root, PathBuf::from(value), flag)?,
            "--agent-timeout-seconds" => {
                let parsed = parse_number::<u64>(flag, &value)?;
                assign(&mut timeout, parsed, flag)?;
            }
            "--launcher" if allow_launcher => {
                assign(&mut launcher, PathBuf::from(value), flag)?;
            }
            "--launcher" => {
                return Err(FactoryError::new("--launcher is valid only with start"));
            }
            _ => return Err(FactoryError::new(format!("unknown flag: {flag}"))),
        }
        index += 2;
    }
    Ok(LaunchOptions {
        prompt_file: require_some!(prompt_file, FactoryError::new("--prompt-file is required")),
        repo: repo.unwrap_or(PathBuf::from(".")),
        base_ref: base_ref.unwrap_or("HEAD".to_string()),
        run_id,
        additional_review_rounds: additional_review_rounds.unwrap_or(0),
        artifact_root,
        worktree_root,
        agent_timeout_seconds: timeout.unwrap_or(DEFAULT_TIMEOUT_SECONDS),
        allow_dirty_source,
        launcher,
        effort: Some(effort.unwrap_or(DEFAULT_EFFORT)),
        plan_critics: Some(plan_critics.unwrap_or(DEFAULT_PLAN_CRITICS)),
        codex_reviewers: Some(codex_reviewers.unwrap_or(DEFAULT_CODEX_REVIEWERS)),
    })
}

fn parse_exec_options(args: &[String]) -> FactoryResult<(PathBuf, String)> {
    let mut run_dir = None;
    let mut sha256 = None;
    let mut index = 0;
    while index < args.len() {
        let flag = &args[index];
        let value = require_some!(
            args.get(index + 1),
            FactoryError::new(format!("missing value for {flag}"))
        )
        .clone();
        match flag.as_str() {
            "--run-dir" => assign(&mut run_dir, PathBuf::from(value), flag)?,
            "--request-sha256" => assign(&mut sha256, value, flag)?,
            _ => return Err(FactoryError::new(format!("unknown flag: {flag}"))),
        }
        index += 2;
    }
    let run_dir = require_some!(run_dir, FactoryError::new("--run-dir is required"));
    let sha256 = require_some!(sha256, FactoryError::new("--request-sha256 is required"));
    if sha256.len() != 64 || !sha256.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(FactoryError::new(
            "--request-sha256 must be exactly 64 hexadecimal characters",
        ));
    }
    Ok((run_dir, sha256.to_ascii_lowercase()))
}

fn assign<T>(slot: &mut Option<T>, value: T, flag: &str) -> FactoryResult<()> {
    if slot.replace(value).is_some() {
        Err(FactoryError::new(format!("duplicate flag: {flag}")))
    } else {
        Ok(())
    }
}

fn parse_number<T>(flag: &str, value: &str) -> FactoryResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    result_context(
        value.parse(),
        &format!("invalid numeric value for {flag}: {value}"),
    )
}

#[cfg(test)]
mod tests {
    use std::io::{Error, ErrorKind};

    use super::*;

    struct FailWriter;

    impl Write for FailWriter {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(Error::new(
                ErrorKind::BrokenPipe,
                "intentional write failure",
            ))
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Err(Error::other("intentional flush failure"))
        }
    }

    #[test]
    fn help_and_parse_errors_have_stable_exit_codes() {
        for args in [
            vec!["crab-factory", "--help"],
            vec!["crab-factory", "run", "--help"],
            vec!["crab-factory", "start", "-h"],
            vec!["crab-factory", "exec", "--help"],
        ] {
            let mut out = Vec::new();
            let mut err = Vec::new();
            assert_eq!(run_factory_cli(args, &mut out, &mut err), 0);
            let help = String::from_utf8(out).unwrap();
            assert!(help.contains("--additional-review-rounds <N>"));
            assert!(help.contains("unrestricted host permissions and network access"));
            assert!(err.is_empty());
        }
        let mut out = Vec::new();
        let mut err = Vec::new();
        assert_eq!(
            run_factory_cli(["crab-factory", "unknown"], &mut out, &mut err),
            2
        );
        assert!(String::from_utf8(err)
            .unwrap()
            .contains("unknown subcommand"));
        let mut stderr = Vec::new();
        assert_eq!(
            run_factory_cli(["crab-factory", "--help"], &mut FailWriter, &mut stderr),
            2
        );
        assert!(String::from_utf8(stderr)
            .unwrap()
            .contains("could not write help"));
        assert_eq!(
            run_factory_cli(
                ["crab-factory", "run", "--help"],
                &mut FailWriter,
                &mut Vec::new(),
            ),
            2
        );
        assert!(FailWriter.flush().is_err());
        let mut stdout = Vec::new();
        assert_eq!(
            run_factory_cli(["crab-factory", "unknown"], &mut stdout, &mut FailWriter),
            2
        );
    }

    #[test]
    fn launch_and_exec_parsers_validate_flags() {
        let options = parse_launch_options(
            &[
                "--prompt-file".into(),
                "request".into(),
                "--repo".into(),
                "repo".into(),
                "--additional-review-rounds".into(),
                "2".into(),
                "--agent-timeout-seconds".into(),
                "60".into(),
                "--allow-dirty-source".into(),
                "--effort".into(),
                "max".into(),
                "--plan-critics".into(),
                "8".into(),
                "--codex-reviewers".into(),
                "1".into(),
            ],
            false,
        )
        .unwrap();
        assert_eq!(options.additional_review_rounds, 2);
        assert!(options.allow_dirty_source);
        assert_eq!(options.effort, Some(Effort::Max));
        assert_eq!(options.plan_critics, Some(8));
        assert_eq!(options.codex_reviewers, Some(1));
        for (flag, value) in [
            ("--effort", "medium"),
            ("--plan-critics", "0"),
            ("--plan-critics", "9"),
            ("--codex-reviewers", "no"),
        ] {
            assert!(parse_launch_options(
                &[
                    "--prompt-file".into(),
                    "x".into(),
                    flag.into(),
                    value.into()
                ],
                false,
            )
            .is_err());
        }
        assert!(parse_launch_options(&[], false).is_err());
        assert!(parse_launch_options(&["--prompt-file".into()], false).is_err());
        assert!(parse_launch_options(&["--unknown".into(), "x".into()], false).is_err());
        assert!(parse_launch_options(
            &[
                "--prompt-file".into(),
                "x".into(),
                "--launcher".into(),
                "y".into()
            ],
            false
        )
        .is_err());
        assert!(parse_launch_options(
            &[
                "--prompt-file".into(),
                "x".into(),
                "--allow-dirty-source".into(),
                "--allow-dirty-source".into(),
            ],
            false,
        )
        .is_err());
        assert!(parse_launch_options(
            &[
                "--prompt-file".into(),
                "x".into(),
                "--launcher".into(),
                "/usr/bin/true".into(),
            ],
            true,
        )
        .unwrap()
        .launcher
        .is_some());
        assert!(parse_launch_options(
            &[
                "--prompt-file".into(),
                "x".into(),
                "--prompt-file".into(),
                "y".into()
            ],
            false
        )
        .is_err());
        assert!(parse_launch_options(
            &[
                "--prompt-file".into(),
                "x".into(),
                "--additional-review-rounds".into(),
                "no".into()
            ],
            false
        )
        .is_err());

        let hash = "a".repeat(64);
        assert_eq!(
            parse_exec_options(&[
                "--run-dir".into(),
                "run".into(),
                "--request-sha256".into(),
                hash.clone()
            ])
            .unwrap(),
            (PathBuf::from("run"), hash)
        );
        assert!(parse_exec_options(&[]).is_err());
        assert!(parse_exec_options(&[
            "--run-dir".into(),
            "x".into(),
            "--request-sha256".into(),
            "bad".into()
        ])
        .is_err());
        assert!(parse_exec_options(&["--unknown".into(), "x".into()]).is_err());
        assert!(parse_exec_options(&["--run-dir".into()]).is_err());
        let uppercase = "A".repeat(64);
        assert_eq!(
            parse_exec_options(&[
                "--run-dir".into(),
                "run".into(),
                "--request-sha256".into(),
                uppercase,
            ])
            .unwrap()
            .1,
            "a".repeat(64)
        );
    }

    #[test]
    fn live_control_parsers_cover_success_and_rejection_shapes() {
        assert_eq!(
            parse_status_options(&["--run-dir".into(), "run".into(), "--json".into()]).unwrap(),
            (PathBuf::from("run"), true)
        );
        for args in [
            vec!["--json".into(), "--json".into()],
            vec!["--run-dir".into()],
            vec!["--unknown".into(), "x".into()],
            Vec::new(),
        ] {
            assert!(parse_status_options(&args).is_err());
        }

        assert_eq!(
            parse_steer_options(&[
                "--run-dir".into(),
                "run".into(),
                "--message".into(),
                "hello".into(),
            ])
            .unwrap(),
            (PathBuf::from("run"), "hello".to_string())
        );
        let root = std::env::temp_dir().join(format!("crab-steer-parser-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let message = root.join("message.txt");
        std::fs::write(&message, "from file").unwrap();
        assert_eq!(
            parse_steer_options(&[
                "--run-dir".into(),
                "run".into(),
                "--message-file".into(),
                message.to_string_lossy().into_owned(),
            ])
            .unwrap()
            .1,
            "from file"
        );
        std::fs::write(&message, [0xff]).unwrap();
        assert!(parse_steer_options(&[
            "--run-dir".into(),
            "run".into(),
            "--message-file".into(),
            message.to_string_lossy().into_owned(),
        ])
        .is_err());
        std::fs::remove_file(&message).unwrap();
        assert!(parse_steer_options(&[
            "--run-dir".into(),
            "run".into(),
            "--message-file".into(),
            message.to_string_lossy().into_owned(),
        ])
        .is_err());
        for args in [
            Vec::new(),
            vec!["--run-dir".into(), "run".into()],
            vec![
                "--message".into(),
                "one".into(),
                "--message-file".into(),
                "two".into(),
            ],
            vec!["--unknown".into(), "x".into()],
            vec!["--message".into()],
        ] {
            assert!(parse_steer_options(&args).is_err());
        }
        std::fs::remove_dir_all(root).unwrap();

        let configured = parse_configure_options(&[
            "--run-dir".into(),
            "run".into(),
            "--effort".into(),
            "max".into(),
            "--plan-critics".into(),
            "1".into(),
            "--codex-reviewers".into(),
            "8".into(),
        ])
        .unwrap();
        assert_eq!(configured.effort, Some(Effort::Max));
        for args in [
            Vec::new(),
            vec!["--run-dir".into(), "run".into()],
            vec!["--effort".into(), "high".into()],
            vec!["--unknown".into(), "x".into()],
            vec!["--effort".into()],
        ] {
            assert!(parse_configure_options(&args).is_err());
        }
    }

    #[test]
    fn assign_rejects_duplicates() {
        let mut slot = None;
        assign(&mut slot, 1, "--one").unwrap();
        assert!(assign(&mut slot, 2, "--one").is_err());
        assert_eq!(slot, Some(2));
    }
}
