use std::io::Write;
use std::path::PathBuf;

use crab_core::{write_pending_trigger, PendingTrigger};

const USAGE: &str = "Usage:
  crab-trigger --state-dir <path> --channel <channel_id> --message <text>

Flags:
  --state-dir    path to the Crab state directory (e.g. /path/to/workspace/state)
  --channel      Discord channel ID to trigger
  --message      message content for the trigger
  --help         show this help";

pub fn run_trigger_cli<I, S>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let argv: Vec<String> = args.into_iter().map(Into::into).collect();
    let command_args = argv.get(1..).unwrap_or(&[]);

    if command_args.iter().any(|a| a == "--help" || a == "-h") {
        let _ = writeln!(stdout, "{USAGE}");
        return 0;
    }

    match execute(command_args) {
        Ok(path) => match writeln!(stdout, "{}", path.display()) {
            Ok(()) => 0,
            Err(error) => {
                let _ = writeln!(stderr, "error: failed to write output: {error}");
                1
            }
        },
        Err(message) => {
            let _ = writeln!(stderr, "error: {message}");
            let _ = writeln!(stderr);
            let _ = writeln!(stderr, "{USAGE}");
            1
        }
    }
}

fn execute(args: &[String]) -> Result<PathBuf, String> {
    let mut state_dir: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut message: Option<String> = None;
    let mut index = 0usize;

    while index < args.len() {
        let flag = &args[index];
        if !flag.starts_with("--") {
            return Err(format!("unexpected positional argument {:?}", flag));
        }
        let slot = match flag.as_str() {
            "--state-dir" => &mut state_dir,
            "--channel" => &mut channel,
            "--message" => &mut message,
            other => return Err(format!("unknown flag {other}")),
        };
        let value = args
            .get(index.saturating_add(1))
            .ok_or_else(|| format!("missing value for flag {flag}"))?;
        if value.starts_with("--") {
            return Err(format!("missing value for flag {flag}"));
        }
        if slot.is_some() {
            return Err(format!("duplicate flag {flag}"));
        }
        *slot = Some(value.clone());
        index = index.saturating_add(2);
    }

    let state_dir = state_dir.ok_or("missing required flag --state-dir")?;
    if state_dir.trim().is_empty() {
        return Err("--state-dir must not be empty".to_string());
    }
    let channel = channel.ok_or("missing required flag --channel")?;
    if channel.trim().is_empty() {
        return Err("--channel must not be empty".to_string());
    }
    let message = message.ok_or("missing required flag --message")?;
    if message.trim().is_empty() {
        return Err("--message must not be empty".to_string());
    }

    let trigger = PendingTrigger {
        channel_id: channel,
        message,
    };

    write_pending_trigger(&PathBuf::from(state_dir), &trigger).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, ErrorKind, Write};
    use std::path::PathBuf;

    use crab_core::PENDING_TRIGGERS_DIR_NAME;

    use super::run_trigger_cli;
    use crate::test_support::TempWorkspace;

    #[derive(Default)]
    struct FailWriter;

    impl Write for FailWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "intentional write failure",
            ))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "intentional flush failure",
            ))
        }
    }

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
    fn write_trigger_failure_returns_error() {
        let temp = TempWorkspace::new("trigger-cli", "write-fail");
        fs::create_dir_all(&temp.path).expect("temp dir should be creatable");
        // Make the state dir a file so create_dir_all in write_pending_trigger fails.
        let fake_dir = temp.path.join("file-not-dir");
        fs::write(&fake_dir, "blocker").expect("write should succeed");
        let state_dir = fake_dir.to_string_lossy().to_string();

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

    #[test]
    fn fail_writer_flush_path_is_exercised() {
        let mut writer = FailWriter;
        let error = writer.flush().expect_err("flush should fail");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }
}
