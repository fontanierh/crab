use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use crab_core::{parse_checkpoint_turn_document, write_pending_rotation, PendingRotation};

use crate::cli_support;

const USAGE: &str = "Usage:
  crab-rotate --state-dir <path> --checkpoint <json> [--force]
  crab-rotate --state-dir <path> --checkpoint-file <path> [--force]
  cat checkpoint.json | crab-rotate --state-dir <path> [--force]

Flags:
  --state-dir        path to the Crab state directory (e.g. /path/to/workspace/state)
  --checkpoint       checkpoint JSON string
  --checkpoint-file  path to JSON file containing checkpoint
  --force            bypass the 300-second rotation cooldown
  --help             show this help

If neither --checkpoint nor --checkpoint-file is given, reads JSON from stdin.";

const LAST_ROTATE_MARKER_FILE_NAME: &str = ".last_rotate_epoch_ms";
const ROTATION_COOLDOWN_SECONDS: u128 = 300;

pub fn run_rotate_cli<I, S>(
    args: I,
    stdin: &mut dyn Read,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let argv: Vec<String> = args.into_iter().map(Into::into).collect();
    // Capture stdin into a ref-cell-like binding so the closure can use it.
    let stdin_ref = std::cell::RefCell::new(stdin);
    cli_support::run_path_cli(
        &argv,
        USAGE,
        |command_args| execute(command_args, &mut *stdin_ref.borrow_mut()),
        stdout,
        stderr,
    )
}

fn execute(args: &[String], stdin: &mut dyn Read) -> Result<PathBuf, String> {
    let mut state_dir: Option<String> = None;
    let mut checkpoint_json: Option<String> = None;
    let mut checkpoint_file: Option<String> = None;
    let mut force = false;

    let filtered: Vec<String> = args
        .iter()
        .filter(|arg| match arg.as_str() {
            "--force" => {
                force = true;
                false
            }
            _ => true,
        })
        .cloned()
        .collect();

    cli_support::parse_flag_pairs(
        &filtered,
        |flag| matches!(flag, "--state-dir" | "--checkpoint" | "--checkpoint-file"),
        |flag, value| {
            let slot = match flag {
                "--state-dir" => &mut state_dir,
                "--checkpoint" => &mut checkpoint_json,
                _ => &mut checkpoint_file,
            };
            cli_support::assign_flag(slot, flag, value)
        },
    )?;

    let state_dir = cli_support::require_non_empty(state_dir, "--state-dir")?;

    if checkpoint_json.is_some() && checkpoint_file.is_some() {
        return Err("cannot use both --checkpoint and --checkpoint-file".to_string());
    }

    let state_path = PathBuf::from(&state_dir);

    if !force {
        let marker_path = rotation_marker_path(&state_path);
        if let Ok(contents) = std::fs::read_to_string(&marker_path) {
            if let Ok(last_ms) = contents.trim().parse::<u128>() {
                let now_ms = current_epoch_ms();
                if now_ms > last_ms {
                    let elapsed_s = (now_ms - last_ms) / 1000;
                    if elapsed_s < ROTATION_COOLDOWN_SECONDS {
                        return Err(format!(
                            "rotation cooldown: last rotation was {}s ago (minimum {}s). Use --force to override.",
                            elapsed_s, ROTATION_COOLDOWN_SECONDS
                        ));
                    }
                }
            }
        }
    }

    let raw_json = if let Some(json) = checkpoint_json {
        json
    } else if let Some(file_path) = checkpoint_file {
        if file_path.trim().is_empty() {
            return Err("--checkpoint-file must not be empty".to_string());
        }
        std::fs::read_to_string(&file_path)
            .map_err(|error| format!("failed to read checkpoint file {file_path:?}: {error}"))?
    } else {
        let mut buf = String::new();
        stdin
            .read_to_string(&mut buf)
            .map_err(|error| format!("failed to read checkpoint from stdin: {error}"))?;
        buf
    };

    let checkpoint =
        parse_checkpoint_turn_document(&raw_json).map_err(|error| error.to_string())?;

    let rotation = PendingRotation { checkpoint };
    let path = write_pending_rotation(&state_path, &rotation).map_err(|error| error.to_string())?;
    stamp_rotation_marker(&state_path);
    Ok(path)
}

fn rotation_marker_path(state_dir: &Path) -> PathBuf {
    state_dir.join(LAST_ROTATE_MARKER_FILE_NAME)
}

fn current_epoch_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis()
}

fn stamp_rotation_marker(state_dir: &Path) {
    let marker_path = rotation_marker_path(state_dir);
    let _ = std::fs::write(&marker_path, current_epoch_ms().to_string());
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, Cursor};
    use std::path::PathBuf;

    use crab_core::PENDING_ROTATIONS_DIR_NAME;

    use super::{
        current_epoch_ms, rotation_marker_path, run_rotate_cli, ROTATION_COOLDOWN_SECONDS,
    };
    use crate::cli_support::test_helpers::FailWriter;
    use crate::test_support::TempWorkspace;

    struct FailReader;

    impl io::Read for FailReader {
        fn read(&mut self, _buf: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "intentional read failure",
            ))
        }
    }

    fn valid_checkpoint_json() -> &'static str {
        r#"{"summary":"Session summary","decisions":["Use claude backend"],"open_questions":["Need model override?"],"next_actions":["Implement checkpoint writer"],"artifacts":[{"path":"src/main.rs","note":"entrypoint updated"}]}"#
    }

    fn run_with_optional_stdin(
        args: &[&str],
        stdin_content: Option<&str>,
    ) -> (i32, String, String) {
        let stdin_bytes = stdin_content.map_or_else(Vec::new, |s| s.as_bytes().to_vec());
        let mut stdin = Cursor::new(stdin_bytes);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status = run_rotate_cli(
            args.iter().copied().map(String::from).collect::<Vec<_>>(),
            &mut stdin,
            &mut stdout,
            &mut stderr,
        );
        (
            status,
            String::from_utf8(stdout).expect("stdout should be utf-8"),
            String::from_utf8(stderr).expect("stderr should be utf-8"),
        )
    }

    fn run(args: &[&str]) -> (i32, String, String) {
        run_with_optional_stdin(args, None)
    }

    fn run_with_stdin(args: &[&str], stdin_content: &str) -> (i32, String, String) {
        run_with_optional_stdin(args, Some(stdin_content))
    }

    fn assert_success_wrote_rotation(status: i32, stdout: &str, stderr: &str) {
        assert_eq!(status, 0, "stderr: {stderr}");
        assert!(stderr.is_empty());
        let printed_path = stdout.trim();
        assert!(printed_path.contains(PENDING_ROTATIONS_DIR_NAME));
        assert!(PathBuf::from(printed_path).exists());
    }

    fn marker_path(workspace: &TempWorkspace) -> PathBuf {
        rotation_marker_path(&workspace.path)
    }

    fn write_marker(workspace: &TempWorkspace, timestamp_ms: u128) -> String {
        fs::create_dir_all(&workspace.path).expect("dir should be creatable");
        fs::write(marker_path(workspace), timestamp_ms.to_string())
            .expect("marker write should succeed");
        workspace.path.to_string_lossy().to_string()
    }

    fn write_marker_contents(workspace: &TempWorkspace, contents: &str) -> String {
        fs::create_dir_all(&workspace.path).expect("dir should be creatable");
        fs::write(marker_path(workspace), contents).expect("marker write should succeed");
        workspace.path.to_string_lossy().to_string()
    }

    #[test]
    fn help_flag_prints_usage() {
        let (status, stdout, stderr) = run(&["crab-rotate", "--help"]);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stdout.contains("crab-rotate"));
        assert!(stdout.contains("--force"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn short_help_flag_prints_usage() {
        let (status, stdout, stderr) = run(&["crab-rotate", "-h"]);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stdout.contains("--force"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn successful_write_with_checkpoint_flag() {
        let temp = TempWorkspace::new("rotate-cli", "flag-success");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn successful_write_with_checkpoint_file_flag() {
        let temp = TempWorkspace::new("rotate-cli", "file-success");
        fs::create_dir_all(&temp.path).expect("dir should be creatable");
        let checkpoint_file = temp.path.join("checkpoint.json");
        fs::write(&checkpoint_file, valid_checkpoint_json()).expect("write should succeed");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint-file",
            &checkpoint_file.to_string_lossy(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn successful_write_from_stdin() {
        let temp = TempWorkspace::new("rotate-cli", "stdin-success");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run_with_stdin(
            &["crab-rotate", "--state-dir", &state_dir],
            valid_checkpoint_json(),
        );
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn auto_stamps_marker_on_success() {
        let temp = TempWorkspace::new("rotate-cli", "marker-success");
        let state_dir = temp.path.to_string_lossy().to_string();
        let before_ms = current_epoch_ms();

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);

        let marker_contents =
            fs::read_to_string(marker_path(&temp)).expect("marker file should be written");
        let stamped_ms = marker_contents
            .trim()
            .parse::<u128>()
            .expect("marker should contain epoch milliseconds");
        let after_ms = current_epoch_ms();
        assert!(
            stamped_ms >= before_ms,
            "stamped_ms={stamped_ms} before_ms={before_ms}"
        );
        assert!(
            stamped_ms <= after_ms,
            "stamped_ms={stamped_ms} after_ms={after_ms}"
        );
    }

    #[test]
    fn cooldown_blocks_rapid_rotation() {
        let temp = TempWorkspace::new("rotate-cli", "cooldown-block");
        let state_dir = write_marker(&temp, current_epoch_ms());

        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("rotation cooldown"));
        assert!(stderr.contains(&format!("minimum {}s", ROTATION_COOLDOWN_SECONDS)));
    }

    #[test]
    fn force_flag_bypasses_cooldown() {
        let temp = TempWorkspace::new("rotate-cli", "force-cooldown");
        let state_dir = write_marker(&temp, current_epoch_ms());

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
            "--force",
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn stale_marker_allows_rotation() {
        let temp = TempWorkspace::new("rotate-cli", "stale-marker");
        let stale_ms = current_epoch_ms() - ((ROTATION_COOLDOWN_SECONDS + 1) * 1000);
        let state_dir = write_marker(&temp, stale_ms);

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn future_marker_allows_rotation() {
        let temp = TempWorkspace::new("rotate-cli", "future-marker");
        let future_ms = current_epoch_ms() + ((ROTATION_COOLDOWN_SECONDS + 1) * 1000);
        let state_dir = write_marker(&temp, future_ms);

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn malformed_marker_allows_rotation() {
        let temp = TempWorkspace::new("rotate-cli", "bad-marker");
        let state_dir = write_marker_contents(&temp, "not-a-number");

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn missing_marker_allows_rotation() {
        let temp = TempWorkspace::new("rotate-cli", "missing-marker");
        let state_dir = temp.path.to_string_lossy().to_string();
        assert!(!marker_path(&temp).exists());

        let (status, stdout, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_success_wrote_rotation(status, &stdout, &stderr);
    }

    #[test]
    fn missing_state_dir_returns_error() {
        let (status, _, stderr) = run(&["crab-rotate", "--checkpoint", valid_checkpoint_json()]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing required flag --state-dir"));
        assert!(stderr.contains("Usage:"));
    }

    #[test]
    fn blank_state_dir_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            "  ",
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("--state-dir must not be empty"));
    }

    #[test]
    fn invalid_checkpoint_json_returns_error() {
        let temp = TempWorkspace::new("rotate-cli", "invalid-json");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            "{ bad json",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("error:"));
    }

    #[test]
    fn checkpoint_validation_error_returns_error() {
        let temp = TempWorkspace::new("rotate-cli", "validation-error");
        let state_dir = temp.path.to_string_lossy().to_string();
        let bad_checkpoint = r#"{"summary":" ","decisions":[],"open_questions":[],"next_actions":[],"artifacts":[]}"#;

        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            bad_checkpoint,
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("summary must not be empty"));
    }

    #[test]
    fn unknown_flag_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            "/tmp/state",
            "--checkpoint",
            valid_checkpoint_json(),
            "--extra",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("unknown flag --extra"));
    }

    #[test]
    fn positional_argument_returns_error() {
        let (status, _, stderr) = run(&["crab-rotate", "unexpected"]);
        assert_eq!(status, 1);
        assert!(stderr.contains("unexpected positional argument"));
    }

    #[test]
    fn duplicate_flags_return_errors() {
        for (flag, extra_args) in [
            (
                "--state-dir",
                vec![
                    "--state-dir",
                    "/tmp/a",
                    "--state-dir",
                    "/tmp/b",
                    "--checkpoint",
                    valid_checkpoint_json(),
                ],
            ),
            (
                "--checkpoint",
                vec![
                    "--state-dir",
                    "/tmp/a",
                    "--checkpoint",
                    valid_checkpoint_json(),
                    "--checkpoint",
                    valid_checkpoint_json(),
                ],
            ),
            (
                "--checkpoint-file",
                vec![
                    "--state-dir",
                    "/tmp/a",
                    "--checkpoint-file",
                    "/tmp/a.json",
                    "--checkpoint-file",
                    "/tmp/b.json",
                ],
            ),
        ] {
            let mut args = vec!["crab-rotate"];
            args.extend(extra_args);
            let (status, _, stderr) = run(&args);
            assert_eq!(status, 1, "flag={flag}");
            assert!(
                stderr.contains(&format!("duplicate flag {flag}")),
                "flag={flag} stderr={stderr}"
            );
        }
    }

    #[test]
    fn both_checkpoint_and_checkpoint_file_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            "/tmp/state",
            "--checkpoint",
            valid_checkpoint_json(),
            "--checkpoint-file",
            "/tmp/a.json",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("cannot use both --checkpoint and --checkpoint-file"));
    }

    #[test]
    fn missing_value_for_flag_returns_error() {
        let (status, _, stderr) =
            run(&["crab-rotate", "--state-dir", "/tmp/state", "--checkpoint"]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing value for flag --checkpoint"));
    }

    #[test]
    fn missing_value_before_next_flag_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing value for flag --state-dir"));
    }

    #[test]
    fn blank_checkpoint_file_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            "/tmp/state",
            "--checkpoint-file",
            "  ",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("--checkpoint-file must not be empty"));
    }

    #[test]
    fn checkpoint_file_not_found_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            "/tmp/state",
            "--checkpoint-file",
            "/nonexistent/checkpoint.json",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("failed to read checkpoint file"));
    }

    #[test]
    fn stdin_read_failure_returns_error() {
        let mut bad_stdin = FailReader;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status = run_rotate_cli(
            vec![
                "crab-rotate".to_string(),
                "--state-dir".to_string(),
                "/tmp/state".to_string(),
            ],
            &mut bad_stdin,
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(status, 1);
        let stderr = String::from_utf8(stderr).expect("stderr should be utf-8");
        assert!(stderr.contains("failed to read checkpoint from stdin"));
    }

    #[test]
    fn stdout_write_failure_returns_error() {
        let temp = TempWorkspace::new("rotate-cli", "write-failure");
        let state_dir = temp.path.to_string_lossy().to_string();
        let mut stdin = io::Cursor::new(Vec::<u8>::new());
        let mut bad_stdout = FailWriter;
        let mut stderr = Vec::new();
        let status = run_rotate_cli(
            vec![
                "crab-rotate".to_string(),
                "--state-dir".to_string(),
                state_dir,
                "--checkpoint".to_string(),
                valid_checkpoint_json().to_string(),
            ],
            &mut stdin,
            &mut bad_stdout,
            &mut stderr,
        );
        assert_eq!(status, 1);
        let stderr = String::from_utf8(stderr).expect("stderr should be utf-8");
        assert!(stderr.contains("failed to write output"));
    }

    #[test]
    fn write_rotation_failure_returns_error() {
        let temp = TempWorkspace::new("rotate-cli", "write-fail");
        let state_dir = crate::cli_support::test_helpers::make_blocked_state_dir(&temp);

        let (status, _, stderr) = run(&[
            "crab-rotate",
            "--state-dir",
            &state_dir,
            "--checkpoint",
            valid_checkpoint_json(),
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("error:"));
    }
}
