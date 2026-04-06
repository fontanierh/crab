use std::io::Write;
use std::path::PathBuf;

use crab_core::{
    write_graceful_steering_trigger, write_pending_trigger, write_steering_trigger, PendingTrigger,
};

use crate::cli_support;

const USAGE: &str = "Usage:
  crab-trigger --state-dir <path> --channel <channel_id> --message <text> [--steer | --steer-graceful]

Flags:
  --state-dir        path to the Crab state directory (e.g. /path/to/workspace/state)
  --channel          Discord channel ID to trigger
  --message          message content for the trigger
  --steer            steer the active session immediately (kills in-flight tools)
  --steer-graceful   steer at the next agentic loop boundary (waits for tools to finish)
  --help             show this help";

pub fn run_trigger_cli<I, S>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let argv: Vec<String> = args.into_iter().map(Into::into).collect();
    cli_support::run_path_cli(&argv, USAGE, execute, stdout, stderr)
}

fn execute(args: &[String]) -> Result<PathBuf, String> {
    let mut state_dir: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut message: Option<String> = None;
    let mut steer = false;
    let mut steer_graceful = false;

    // Filter out the boolean --steer / --steer-graceful flags before pair-parsing
    let filtered: Vec<String> = args
        .iter()
        .filter(|a| match a.as_str() {
            "--steer" => {
                steer = true;
                false
            }
            "--steer-graceful" => {
                steer_graceful = true;
                false
            }
            _ => true,
        })
        .cloned()
        .collect();

    if steer && steer_graceful {
        return Err("--steer and --steer-graceful are mutually exclusive".to_string());
    }

    cli_support::parse_flag_pairs(
        &filtered,
        |flag| matches!(flag, "--state-dir" | "--channel" | "--message"),
        |flag, value| {
            let slot = match flag {
                "--state-dir" => &mut state_dir,
                "--channel" => &mut channel,
                _ => &mut message,
            };
            cli_support::assign_flag(slot, flag, value)
        },
    )?;

    let state_dir = cli_support::require_non_empty(state_dir, "--state-dir")?;
    let channel = cli_support::require_non_empty(channel, "--channel")?;
    let message = cli_support::require_non_empty(message, "--message")?;

    let trigger = PendingTrigger {
        channel_id: channel,
        message,
    };

    let state_path = PathBuf::from(state_dir);
    if steer_graceful {
        write_graceful_steering_trigger(&state_path, &trigger)
    } else if steer {
        write_steering_trigger(&state_path, &trigger)
    } else {
        write_pending_trigger(&state_path, &trigger)
    }
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crab_core::{
        GRACEFUL_STEERING_DIR_NAME, PENDING_TRIGGERS_DIR_NAME, STEERING_TRIGGERS_DIR_NAME,
    };

    use super::run_trigger_cli;
    use crate::cli_support::test_helpers::FailWriter;
    use crate::test_support::TempWorkspace;

    fn run(args: &[&str]) -> (i32, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status = run_trigger_cli(
            args.iter().copied().map(String::from).collect::<Vec<_>>(),
            &mut stdout,
            &mut stderr,
        );
        (
            status,
            String::from_utf8(stdout).expect("stdout should be utf-8"),
            String::from_utf8(stderr).expect("stderr should be utf-8"),
        )
    }

    #[test]
    fn help_flag_prints_usage() {
        let (status, stdout, stderr) = run(&["crab-trigger", "--help"]);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stdout.contains("crab-trigger"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn short_help_flag_prints_usage() {
        let (status, stdout, stderr) = run(&["crab-trigger", "-h"]);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn successful_write_prints_path_and_returns_zero() {
        let temp = TempWorkspace::new("trigger-cli", "success");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            &state_dir,
            "--channel",
            "123456789",
            "--message",
            "Check deployment",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());

        let printed_path = stdout.trim();
        assert!(printed_path.contains(PENDING_TRIGGERS_DIR_NAME));
        assert!(PathBuf::from(printed_path).exists());
    }

    #[test]
    fn missing_state_dir_returns_error() {
        let (status, _, stderr) = run(&["crab-trigger", "--channel", "123", "--message", "hello"]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing required flag --state-dir"));
        assert!(stderr.contains("Usage:"));
    }

    #[test]
    fn missing_channel_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--message",
            "hello",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing required flag --channel"));
    }

    #[test]
    fn missing_message_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "123",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing required flag --message"));
    }

    #[test]
    fn blank_channel_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "  ",
            "--message",
            "hello",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("--channel must not be empty"));
    }

    #[test]
    fn blank_message_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "123",
            "--message",
            "  ",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("--message must not be empty"));
    }

    #[test]
    fn blank_state_dir_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "  ",
            "--channel",
            "123",
            "--message",
            "hello",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("--state-dir must not be empty"));
    }

    #[test]
    fn unknown_flag_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "123",
            "--message",
            "hello",
            "--extra",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("unknown flag --extra"));
    }

    #[test]
    fn positional_argument_returns_error() {
        let (status, _, stderr) = run(&["crab-trigger", "unexpected"]);
        assert_eq!(status, 1);
        assert!(stderr.contains("unexpected positional argument"));
    }

    #[test]
    fn duplicate_flag_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/a",
            "--state-dir",
            "/tmp/b",
            "--channel",
            "123",
            "--message",
            "hello",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("duplicate flag --state-dir"));
    }

    #[test]
    fn missing_value_for_flag_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "123",
            "--message",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing value for flag --message"));
    }

    #[test]
    fn missing_value_before_next_flag_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "--message",
            "hello",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing value for flag --channel"));
    }

    #[test]
    fn stdout_write_failure_returns_error() {
        let temp = TempWorkspace::new("trigger-cli", "write-failure");
        let state_dir = temp.path.to_string_lossy().to_string();
        let mut bad_stdout = FailWriter;
        let mut stderr = Vec::new();
        let status = run_trigger_cli(
            vec![
                "crab-trigger".to_string(),
                "--state-dir".to_string(),
                state_dir,
                "--channel".to_string(),
                "123".to_string(),
                "--message".to_string(),
                "hello".to_string(),
            ],
            &mut bad_stdout,
            &mut stderr,
        );
        assert_eq!(status, 1);
        let stderr = String::from_utf8(stderr).expect("stderr should be utf-8");
        assert!(stderr.contains("failed to write output"));
    }

    #[test]
    fn steer_flag_writes_to_steering_triggers_directory() {
        let temp = TempWorkspace::new("trigger-cli", "steer-flag");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            &state_dir,
            "--channel",
            "123456789",
            "--message",
            "Steer the session",
            "--steer",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());

        let printed_path = stdout.trim();
        assert!(
            printed_path.contains(STEERING_TRIGGERS_DIR_NAME),
            "path should contain steering_triggers directory, got: {printed_path}"
        );
        assert!(
            !printed_path.contains(PENDING_TRIGGERS_DIR_NAME),
            "path should NOT contain pending_triggers directory"
        );
        assert!(PathBuf::from(printed_path).exists());
    }

    #[test]
    fn steer_graceful_flag_writes_to_graceful_steering_directory() {
        let temp = TempWorkspace::new("trigger-cli", "steer-graceful-flag");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            &state_dir,
            "--channel",
            "123456789",
            "--message",
            "Graceful steer",
            "--steer-graceful",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());

        let printed_path = stdout.trim();
        assert!(
            printed_path.contains(GRACEFUL_STEERING_DIR_NAME),
            "path should contain graceful_steering directory, got: {printed_path}"
        );
        assert!(
            !printed_path.contains(PENDING_TRIGGERS_DIR_NAME),
            "path should NOT contain pending_triggers directory"
        );
        assert!(
            !printed_path.contains(STEERING_TRIGGERS_DIR_NAME),
            "path should NOT contain steering_triggers directory"
        );
        assert!(PathBuf::from(printed_path).exists());
    }

    #[test]
    fn steer_and_steer_graceful_together_returns_error() {
        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            "/tmp/state",
            "--channel",
            "123",
            "--message",
            "hello",
            "--steer",
            "--steer-graceful",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("mutually exclusive"));
    }

    #[test]
    fn write_trigger_failure_returns_error() {
        let temp = TempWorkspace::new("trigger-cli", "write-fail");
        let state_dir = crate::cli_support::test_helpers::make_blocked_state_dir(&temp);

        let (status, _, stderr) = run(&[
            "crab-trigger",
            "--state-dir",
            &state_dir,
            "--channel",
            "123",
            "--message",
            "hello",
        ]);
        assert_eq!(status, 1);
        assert!(stderr.contains("error:"));
    }
}
