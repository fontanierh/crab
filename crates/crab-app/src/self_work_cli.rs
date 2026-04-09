use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crab_core::{
    read_self_work_session, validate_new_self_work_start, write_pending_trigger,
    write_self_work_session_atomically, CrabError, PendingTrigger, SelfWorkSession,
    SelfWorkSessionLock, SelfWorkSessionStatus, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
};
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::cli_support;

const MAX_SELF_WORK_DURATION_MS: u64 = 3 * 60 * 60 * 1_000;
const USAGE: &str = "Usage:
  crab-self-work start --state-dir <path> --channel <channel_id> --goal <text> --end <rfc3339>
  crab-self-work stop --state-dir <path>
  crab-self-work status --state-dir <path>

Subcommands:
  start             create a new active self-work session and emit the initial trigger
  stop              mark the active self-work session as stopped
  status            print the current self-work session state as JSON

Flags:
  --state-dir       path to the Crab state directory (e.g. /path/to/workspace/state)
  --channel         Discord channel ID for the self-work lane (start only)
  --goal            goal text for the self-work session (start only)
  --end             RFC3339/ISO8601 session end timestamp (start only)
  --help            show this help";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SelfWorkCommand {
    Start,
    Stop,
    Status,
}

pub fn run_self_work_cli<I, S>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let argv = cli_support::collect_args(args);
    #[rustfmt::skip]
    let now_epoch_ms = match current_epoch_ms() { Ok(value) => value, Err(message) => return write_error(stderr, &message) };
    run_self_work_cli_with_now_epoch_ms(&argv, stdout, stderr, now_epoch_ms)
}

fn run_self_work_cli_with_now_epoch_ms(
    argv: &[String],
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    now_epoch_ms: u64,
) -> i32 {
    let command_args = argv.get(1..).unwrap_or(&[]);

    if command_args
        .iter()
        .any(|arg| arg == "--help" || arg == "-h")
    {
        let _ = writeln!(stdout, "{USAGE}");
        return 0;
    }

    let response = execute(command_args, now_epoch_ms);
    match response {
        Ok(value) => match write_json(stdout, &value) {
            Ok(()) => 0,
            Err(message) => write_error(stderr, &message),
        },
        Err(message) => write_error(stderr, &message),
    }
}

fn execute(args: &[String], now_epoch_ms: u64) -> Result<Value, String> {
    let Some(raw_command) = args.first() else {
        return Err("missing subcommand (expected start, stop, or status)".to_string());
    };
    let command = parse_command(raw_command)?;
    let command_args = &args[1..];

    match command {
        SelfWorkCommand::Start => execute_start(command_args, now_epoch_ms),
        SelfWorkCommand::Stop => execute_stop(command_args, now_epoch_ms),
        SelfWorkCommand::Status => execute_status(command_args),
    }
}

fn execute_start(args: &[String], now_epoch_ms: u64) -> Result<Value, String> {
    let mut state_dir: Option<String> = None;
    let mut channel: Option<String> = None;
    let mut goal: Option<String> = None;
    let mut end: Option<String> = None;

    cli_support::parse_flag_pairs(
        args,
        |flag| matches!(flag, "--state-dir" | "--channel" | "--goal" | "--end"),
        |flag, value| {
            let slot = match flag {
                "--state-dir" => &mut state_dir,
                "--channel" => &mut channel,
                "--goal" => &mut goal,
                _ => &mut end,
            };
            cli_support::assign_flag(slot, flag, value)
        },
    )?;

    let state_root = PathBuf::from(cli_support::require_non_empty(state_dir, "--state-dir")?);
    let channel_id = cli_support::require_non_empty(channel, "--channel")?;
    let goal = cli_support::require_non_empty(goal, "--goal")?;
    let end_raw = cli_support::require_non_empty(end, "--end")?;
    let (end_at_epoch_ms, end_at_iso8601) = parse_end_timestamp(&end_raw)?;

    if end_at_epoch_ms <= now_epoch_ms {
        return Err("--end must be in the future".to_string());
    }
    if end_at_epoch_ms > now_epoch_ms.saturating_add(MAX_SELF_WORK_DURATION_MS) {
        return Err("--end must be no more than 3 hours in the future".to_string());
    }

    let mut lock =
        SelfWorkSessionLock::acquire(&state_root, now_epoch_ms).map_err(crab_error_to_string)?;
    let existing = read_self_work_session(&state_root).map_err(crab_error_to_string)?;
    if let Err(error) = validate_new_self_work_start(existing.as_ref()) {
        return Err(error.to_string());
    }

    let session = SelfWorkSession {
        schema_version: CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
        session_id: format!("self-work:{now_epoch_ms}"),
        channel_id: channel_id.clone(),
        goal,
        started_at_epoch_ms: now_epoch_ms,
        started_at_iso8601: format_epoch_ms(now_epoch_ms)?,
        end_at_epoch_ms,
        end_at_iso8601,
        status: SelfWorkSessionStatus::Active,
        last_wake_triggered_at_epoch_ms: None,
        final_trigger_pending: false,
        stopped_at_epoch_ms: None,
        expired_at_epoch_ms: None,
        last_expiry_triggered_at_epoch_ms: None,
    };

    write_self_work_session_atomically(&state_root, &session).map_err(crab_error_to_string)?;
    write_pending_trigger(
        &state_root,
        &PendingTrigger {
            channel_id,
            message: build_start_trigger_message(&session, &state_root),
        },
    )
    .map_err(crab_error_to_string)?;
    lock.release().map_err(crab_error_to_string)?;

    session_response(Some(&session))
}

fn execute_stop(args: &[String], now_epoch_ms: u64) -> Result<Value, String> {
    let state_root = parse_state_root_arg(args)?;
    let mut lock =
        SelfWorkSessionLock::acquire(&state_root, now_epoch_ms).map_err(crab_error_to_string)?;
    let maybe_session = read_self_work_session(&state_root).map_err(crab_error_to_string)?;

    let response = stop_response(&state_root, maybe_session, now_epoch_ms)?;
    lock.release().map_err(crab_error_to_string)?;
    Ok(response)
}

fn execute_status(args: &[String]) -> Result<Value, String> {
    let state_root = parse_state_root_arg(args)?;
    let maybe_session = read_self_work_session(&state_root).map_err(crab_error_to_string)?;
    session_response(maybe_session.as_ref())
}

fn parse_state_root_arg(args: &[String]) -> Result<PathBuf, String> {
    let mut state_dir: Option<String> = None;
    let is_state_dir = |flag: &str| flag == "--state-dir";
    cli_support::parse_flag_pairs(args, is_state_dir, |flag, value| {
        cli_support::assign_flag(&mut state_dir, flag, value)
    })?;
    let state_dir = cli_support::require_non_empty(state_dir, "--state-dir")?;
    Ok(PathBuf::from(state_dir))
}

fn stop_response(
    state_root: &Path,
    maybe_session: Option<SelfWorkSession>,
    now_epoch_ms: u64,
) -> Result<Value, String> {
    match maybe_session {
        None => session_response(None),
        Some(mut session) => {
            if session.status == SelfWorkSessionStatus::Active {
                session.status = SelfWorkSessionStatus::Stopped;
                session.final_trigger_pending = false;
                session.stopped_at_epoch_ms = Some(now_epoch_ms);
                write_self_work_session_atomically(state_root, &session)
                    .map_err(crab_error_to_string)?;
            }
            session_response(Some(&session))
        }
    }
}

fn parse_command(value: &str) -> Result<SelfWorkCommand, String> {
    match value {
        "start" => Ok(SelfWorkCommand::Start),
        "stop" => Ok(SelfWorkCommand::Stop),
        "status" => Ok(SelfWorkCommand::Status),
        _ => Err(format!(
            "unknown subcommand {value:?} (expected start, stop, or status)"
        )),
    }
}

fn current_epoch_ms() -> Result<u64, String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(current_clock_error)?;
    u64::try_from(duration.as_millis()).map_err(epoch_ms_overflow_error)
}

fn parse_end_timestamp(raw_value: &str) -> Result<(u64, String), String> {
    let parsed = OffsetDateTime::parse(raw_value, &Rfc3339)
        .map_err(parse_end_timestamp_error)?
        .to_offset(time::UtcOffset::UTC);
    let epoch_ms = epoch_ms_from_datetime(parsed)?;
    let iso8601 = parsed
        .format(&Rfc3339)
        .map_err(format_end_timestamp_error)?;
    Ok((epoch_ms, iso8601))
}

fn format_epoch_ms(epoch_ms: u64) -> Result<String, String> {
    let datetime = datetime_from_epoch_ms(epoch_ms)?;
    datetime.format(&Rfc3339).map_err(format_timestamp_error)
}

fn epoch_ms_from_datetime(value: OffsetDateTime) -> Result<u64, String> {
    let unix_timestamp_nanos = value.unix_timestamp_nanos();
    if unix_timestamp_nanos < 0 {
        return Err("timestamp must not be before the unix epoch".to_string());
    }
    let unix_timestamp_ms = unix_timestamp_nanos / 1_000_000;
    u64::try_from(unix_timestamp_ms).map_err(timestamp_millis_overflow_error)
}

fn datetime_from_epoch_ms(epoch_ms: u64) -> Result<OffsetDateTime, String> {
    let nanos = i128::from(epoch_ms)
        .checked_mul(1_000_000)
        .ok_or(timestamp_i128_overflow_error())?;
    OffsetDateTime::from_unix_timestamp_nanos(nanos).map_err(build_timestamp_error)
}

fn write_json(stdout: &mut dyn Write, value: &Value) -> Result<(), String> {
    serde_json::to_writer(&mut *stdout, value).map_err(serialize_command_response_error)?;
    writeln!(stdout).map_err(write_command_response_error)?;
    Ok(())
}

fn write_error(stderr: &mut dyn Write, message: &str) -> i32 {
    let _ = writeln!(stderr, "error: {message}");
    let _ = writeln!(stderr);
    let _ = writeln!(stderr, "{USAGE}");
    1
}

fn session_response(session: Option<&SelfWorkSession>) -> Result<Value, String> {
    match session {
        Some(session) => serde_json::to_value(session).map_err(encode_session_error),
        None => Ok(json!({"state":"none"})),
    }
}

fn crab_error_to_string(error: CrabError) -> String {
    error.to_string()
}

fn current_clock_error(error: std::time::SystemTimeError) -> String {
    format!("failed to read system clock: {error}")
}

fn epoch_ms_overflow_error(_: std::num::TryFromIntError) -> String {
    "epoch milliseconds overflow u64".to_string()
}

fn parse_end_timestamp_error(error: time::error::Parse) -> String {
    format!("invalid --end timestamp: {error}")
}

fn format_end_timestamp_error(error: time::error::Format) -> String {
    format!("failed to format --end timestamp: {error}")
}

fn format_timestamp_error(error: time::error::Format) -> String {
    format!("failed to format timestamp: {error}")
}

fn timestamp_millis_overflow_error(_: std::num::TryFromIntError) -> String {
    "timestamp milliseconds overflow u64".to_string()
}

fn timestamp_i128_overflow_error() -> String {
    "timestamp milliseconds overflow i128".to_string()
}

fn build_timestamp_error(error: time::error::ComponentRange) -> String {
    format!("failed to build timestamp: {error}")
}

fn serialize_command_response_error(error: serde_json::Error) -> String {
    format!("failed to serialize command response: {error}")
}

fn write_command_response_error(error: std::io::Error) -> String {
    format!("failed to write command response: {error}")
}

fn encode_session_error(error: serde_json::Error) -> String {
    format!("failed to encode session: {error}")
}

pub(crate) fn build_start_trigger_message(session: &SelfWorkSession, state_root: &Path) -> String {
    format!(
        "[Crab Self-Work]\nevent: start\ngoal: {}\nchannel_id: {}\nsession_end: {}\ninstructions: Begin or resume autonomous work on this goal now. Continue working while useful work remains. If the goal is complete early, run `crab-self-work stop --state-dir {}`.",
        session.goal,
        session.channel_id,
        session.end_at_iso8601,
        state_root.display()
    )
}

pub(crate) fn build_wake_trigger_message(session: &SelfWorkSession, state_root: &Path) -> String {
    format!(
        "[Crab Self-Work]\nevent: wake\ngoal: {}\nchannel_id: {}\nsession_end: {}\ninstructions: This self-work session is still active and the lane is idle. Continue autonomous work on this goal if useful work remains. If the goal is complete, run `crab-self-work stop --state-dir {}`.",
        session.goal,
        session.channel_id,
        session.end_at_iso8601,
        state_root.display()
    )
}

pub(crate) fn build_expiry_trigger_message(session: &SelfWorkSession) -> String {
    format!(
        "[Crab Self-Work]\nevent: expiry\ngoal: {}\nchannel_id: {}\nsession_end: {}\ninstructions: The scheduled self-work session has ended. Wrap up any final notes for this goal now, then stop autonomous work. Do not continue self-work unless a new `crab-self-work start ...` command is issued later.",
        session.goal,
        session.channel_id,
        session.end_at_iso8601,
    )
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::time::{Duration, UNIX_EPOCH};

    use crab_core::{
        read_pending_triggers, write_self_work_session_atomically, SelfWorkSession,
        SelfWorkSessionStatus, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
    };
    use serde_json::{json, Value};
    use time::error::Format;
    use time::format_description::well_known::Rfc3339;
    use time::OffsetDateTime;

    use super::{
        build_timestamp_error, crab_error_to_string, current_clock_error, datetime_from_epoch_ms,
        encode_session_error, epoch_ms_from_datetime, epoch_ms_overflow_error, execute_status,
        execute_stop, format_end_timestamp_error, format_timestamp_error,
        parse_end_timestamp_error, run_self_work_cli, run_self_work_cli_with_now_epoch_ms,
        serialize_command_response_error, stop_response, timestamp_i128_overflow_error,
        timestamp_millis_overflow_error, write_command_response_error, write_json,
        MAX_SELF_WORK_DURATION_MS,
    };
    use crate::cli_support::test_helpers::FailWriter;
    use crate::test_support::TempWorkspace;

    struct FailOnNewlineWriter {
        bytes: Vec<u8>,
    }

    impl io::Write for FailOnNewlineWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if buf == b"\n" {
                return Err(io::Error::other("newline write failure"));
            }
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn run(args: &[&str], now_epoch_ms: u64) -> (i32, String, String) {
        let argv = args.iter().copied().map(String::from).collect::<Vec<_>>();
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status =
            run_self_work_cli_with_now_epoch_ms(&argv, &mut stdout, &mut stderr, now_epoch_ms);
        (
            status,
            String::from_utf8(stdout).expect("stdout should be utf-8"),
            String::from_utf8(stderr).expect("stderr should be utf-8"),
        )
    }

    fn parse_json(stdout: &str) -> Value {
        serde_json::from_str(stdout.trim()).expect("stdout should contain valid json")
    }

    fn active_sample_session() -> SelfWorkSession {
        serde_json::from_value(json!({
            "schema_version": CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
            "session_id": "self-work:1739173200000",
            "channel_id": "123456789",
            "goal": "Ship the feature",
            "started_at_epoch_ms": 1_739_173_200_000u64,
            "started_at_iso8601": "2025-02-10T10:00:00Z",
            "end_at_epoch_ms": 1_739_174_100_000u64,
            "end_at_iso8601": "2025-02-10T10:15:00Z",
            "status": "active",
            "last_wake_triggered_at_epoch_ms": null,
            "final_trigger_pending": false,
            "stopped_at_epoch_ms": null,
            "expired_at_epoch_ms": null,
            "last_expiry_triggered_at_epoch_ms": null
        }))
        .expect("sample session json should parse")
    }

    fn sample_session(status: SelfWorkSessionStatus) -> SelfWorkSession {
        let mut session = active_sample_session();
        session.status = status;

        match status {
            SelfWorkSessionStatus::Active => {}
            SelfWorkSessionStatus::Stopped => {
                session.stopped_at_epoch_ms = Some(1_739_173_600_000);
            }
            SelfWorkSessionStatus::Expired => {
                session.expired_at_epoch_ms = Some(1_739_174_100_000);
                session.last_expiry_triggered_at_epoch_ms = Some(1_739_174_100_000);
            }
        }

        session
    }

    #[test]
    fn help_flag_prints_usage() {
        let (status, stdout, stderr) = run(&["crab-self-work", "--help"], 1_739_173_200_000);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stdout.contains("crab-self-work start"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn wrapper_run_self_work_cli_uses_current_clock_for_status() {
        let temp = TempWorkspace::new("self-work-cli", "wrapper-status");
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();

        let status = run_self_work_cli(
            [
                "crab-self-work",
                "status",
                "--state-dir",
                temp.path.to_string_lossy().as_ref(),
            ],
            &mut stdout,
            &mut stderr,
        );

        assert_eq!(status, 0);
        assert_eq!(
            serde_json::from_slice::<Value>(&stdout).expect("stdout should be valid json"),
            json!({"state":"none"})
        );
        assert!(stderr.is_empty());
    }

    #[test]
    fn missing_subcommand_returns_error() {
        let (status, _, stderr) = run(&["crab-self-work"], 1_739_173_200_000);
        assert_eq!(status, 1);
        assert!(stderr.contains("missing subcommand"));
    }

    #[test]
    fn unknown_subcommand_returns_error() {
        let (status, _, stderr) = run(&["crab-self-work", "resume"], 1_739_173_200_000);
        assert_eq!(status, 1);
        assert!(stderr.contains("unknown subcommand"));
    }

    #[test]
    fn missing_required_flags_return_errors() {
        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                "/tmp/state",
                "--channel",
                "123",
                "--goal",
                "ship it",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("missing required flag --end"));
    }

    #[test]
    fn duplicate_flag_returns_error() {
        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                "/tmp/a",
                "--state-dir",
                "/tmp/b",
                "--channel",
                "123",
                "--goal",
                "ship it",
                "--end",
                "2025-02-10T10:30:00Z",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("duplicate flag --state-dir"));
    }

    #[test]
    fn blank_values_return_errors() {
        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                "/tmp/state",
                "--channel",
                "123",
                "--goal",
                "  ",
                "--end",
                "2025-02-10T10:30:00Z",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("--goal must not be empty"));
    }

    #[test]
    fn invalid_timestamp_returns_error() {
        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                "/tmp/state",
                "--channel",
                "123",
                "--goal",
                "ship it",
                "--end",
                "not-a-timestamp",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("invalid --end timestamp"));
    }

    #[test]
    fn past_end_timestamp_is_rejected() {
        let temp = TempWorkspace::new("self-work-cli", "past-end");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                &state_dir,
                "--channel",
                "123",
                "--goal",
                "ship it",
                "--end",
                "2024-01-01T00:00:00Z",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("--end must be in the future"));
    }

    #[test]
    fn over_three_hours_is_rejected() {
        let temp = TempWorkspace::new("self-work-cli", "over-3h");
        let state_dir = temp.path.to_string_lossy().to_string();
        let too_far = 1_739_173_200_000_u64 + MAX_SELF_WORK_DURATION_MS + 1;
        let too_far_iso = "2025-02-10T13:00:00.001Z";

        assert_eq!(
            too_far, 1_739_184_000_001,
            "test timestamp should remain aligned with the fixture string"
        );

        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                &state_dir,
                "--channel",
                "123",
                "--goal",
                "ship it",
                "--end",
                too_far_iso,
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("no more than 3 hours"));
    }

    #[test]
    fn start_success_writes_session_and_initial_trigger() {
        let temp = TempWorkspace::new("self-work-cli", "start-success");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                &state_dir,
                "--channel",
                "123456789",
                "--goal",
                "Ship the feature",
                "--end",
                "2025-02-10T10:30:00Z",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 0, "stderr: {stderr}");
        assert!(stderr.is_empty());

        let output = parse_json(&stdout);
        assert_eq!(output["status"], "active");
        assert_eq!(output["channel_id"], "123456789");
        assert_eq!(output["goal"], "Ship the feature");
        assert_eq!(output["end_at_iso8601"], "2025-02-10T10:30:00Z");

        let triggers = read_pending_triggers(&temp.path).expect("triggers should be readable");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].1.channel_id, "123456789");
        assert!(triggers[0].1.message.contains("event: start"));
        assert!(triggers[0].1.message.contains("Ship the feature"));
        assert!(triggers[0]
            .1
            .message
            .contains("crab-self-work stop --state-dir"));
    }

    #[test]
    fn active_session_rejects_new_start() {
        let temp = TempWorkspace::new("self-work-cli", "start-active");
        let state_dir = temp.path.to_string_lossy().to_string();
        write_self_work_session_atomically(
            &temp.path,
            &sample_session(SelfWorkSessionStatus::Active),
        )
        .expect("session should persist");

        let (status, _, stderr) = run(
            &[
                "crab-self-work",
                "start",
                "--state-dir",
                &state_dir,
                "--channel",
                "123456789",
                "--goal",
                "Ship the feature",
                "--end",
                "2025-02-10T10:30:00Z",
            ],
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        assert!(stderr.contains("active self-work session"));
    }

    #[test]
    fn stop_transitions_active_session_to_stopped() {
        let temp = TempWorkspace::new("self-work-cli", "stop-active");
        let state_dir = temp.path.to_string_lossy().to_string();
        write_self_work_session_atomically(
            &temp.path,
            &sample_session(SelfWorkSessionStatus::Active),
        )
        .expect("session should persist");

        let (status, stdout, stderr) = run(
            &["crab-self-work", "stop", "--state-dir", &state_dir],
            1_739_173_700_000,
        );
        assert_eq!(status, 0, "stderr: {stderr}");
        let output = parse_json(&stdout);
        assert_eq!(output["status"], "stopped");
        assert_eq!(output["stopped_at_epoch_ms"], 1_739_173_700_000_u64);
    }

    #[test]
    fn stop_is_idempotent_for_stopped_and_expired_sessions() {
        let stopped = TempWorkspace::new("self-work-cli", "stop-stopped");
        let stopped_state_dir = stopped.path.to_string_lossy().to_string();
        write_self_work_session_atomically(
            &stopped.path,
            &sample_session(SelfWorkSessionStatus::Stopped),
        )
        .expect("stopped session should persist");

        let (status, stdout, stderr) = run(
            &["crab-self-work", "stop", "--state-dir", &stopped_state_dir],
            1_739_173_800_000,
        );
        assert_eq!(status, 0, "stderr: {stderr}");
        assert_eq!(parse_json(&stdout)["status"], "stopped");

        let expired = TempWorkspace::new("self-work-cli", "stop-expired");
        let expired_state_dir = expired.path.to_string_lossy().to_string();
        write_self_work_session_atomically(
            &expired.path,
            &sample_session(SelfWorkSessionStatus::Expired),
        )
        .expect("expired session should persist");

        let (status, stdout, stderr) = run(
            &["crab-self-work", "stop", "--state-dir", &expired_state_dir],
            1_739_173_800_000,
        );
        assert_eq!(status, 0, "stderr: {stderr}");
        assert_eq!(parse_json(&stdout)["status"], "expired");
    }

    #[test]
    fn stop_returns_none_when_session_is_missing() {
        let temp = TempWorkspace::new("self-work-cli", "stop-none");
        let state_dir = temp.path.to_string_lossy().to_string();

        let (status, stdout, stderr) = run(
            &["crab-self-work", "stop", "--state-dir", &state_dir],
            1_739_173_800_000,
        );
        assert_eq!(status, 0, "stderr: {stderr}");
        assert_eq!(parse_json(&stdout), json!({"state":"none"}));
    }

    #[test]
    fn direct_execute_stop_and_status_cover_internal_success_paths() {
        let temp = TempWorkspace::new("self-work-cli", "direct-execute");
        let state_dir = temp.path.to_string_lossy().to_string();
        write_self_work_session_atomically(
            &temp.path,
            &sample_session(SelfWorkSessionStatus::Active),
        )
        .expect("session should persist");

        let status_before = execute_status(&["--state-dir".to_string(), state_dir.clone()])
            .expect("status should succeed");
        assert_eq!(status_before["status"], "active");

        let stopped = execute_stop(
            &["--state-dir".to_string(), state_dir.clone()],
            1_739_173_700_000,
        )
        .expect("stop should succeed");
        assert_eq!(stopped["status"], "stopped");

        let status_after =
            execute_status(&["--state-dir".to_string(), state_dir]).expect("status should succeed");
        assert_eq!(status_after["status"], "stopped");
    }

    #[test]
    fn epoch_ms_from_datetime_rejects_pre_epoch_values() {
        let error = epoch_ms_from_datetime(
            OffsetDateTime::from_unix_timestamp(-1).expect("timestamp should build"),
        )
        .expect_err("pre-epoch timestamps should fail");
        assert_eq!(error, "timestamp must not be before the unix epoch");
    }

    #[test]
    fn helper_error_renderers_are_stable() {
        let parse_error =
            OffsetDateTime::parse("not-a-timestamp", &Rfc3339).expect_err("parse should fail");
        assert!(parse_end_timestamp_error(parse_error).contains("invalid --end timestamp"));

        let format_error = Format::InvalidComponent("year");
        assert!(
            format_end_timestamp_error(format_error).contains("failed to format --end timestamp")
        );
        let format_error = Format::InvalidComponent("year");
        assert!(format_timestamp_error(format_error).contains("failed to format timestamp"));

        let system_time = UNIX_EPOCH
            .checked_sub(Duration::from_secs(1))
            .expect("pre-epoch system time should be constructible");
        let clock_error = system_time
            .duration_since(UNIX_EPOCH)
            .expect_err("pre-epoch duration should fail");
        assert!(current_clock_error(clock_error).contains("failed to read system clock"));

        let overflow_duration = Duration::new(u64::MAX / 1_000 + 1, 0);
        let overflow_error = u64::try_from(overflow_duration.as_millis())
            .expect_err("millisecond conversion should overflow");
        assert_eq!(
            epoch_ms_overflow_error(overflow_error),
            "epoch milliseconds overflow u64"
        );

        let timestamp_overflow = u64::try_from(i128::from(u64::MAX) + 1)
            .expect_err("timestamp conversion should overflow");
        assert_eq!(
            timestamp_millis_overflow_error(timestamp_overflow),
            "timestamp milliseconds overflow u64"
        );

        assert_eq!(
            timestamp_i128_overflow_error(),
            "timestamp milliseconds overflow i128"
        );

        let build_error = OffsetDateTime::from_unix_timestamp_nanos(i128::MAX)
            .expect_err("timestamp construction should fail");
        assert!(build_timestamp_error(build_error).contains("failed to build timestamp"));

        let io_error = io::Error::other("newline write failure");
        assert!(write_command_response_error(io_error).contains("failed to write command response"));

        assert!(
            serialize_command_response_error(serde_json::Error::io(io::Error::other(
                "serialize failure"
            )))
            .contains("failed to serialize command response")
        );
        assert!(
            encode_session_error(serde_json::Error::io(io::Error::other("encode failure")))
                .contains("failed to encode session")
        );

        let crab_error = crab_core::CrabError::InvariantViolation {
            context: "self_work_cli_test",
            message: "boom".to_string(),
        };
        assert!(crab_error_to_string(crab_error).contains("boom"));
    }

    #[test]
    fn direct_helpers_cover_additional_error_paths() {
        let overflow_duration = Duration::new(u64::MAX / 1_000 + 1, 0);
        let current_time = UNIX_EPOCH + overflow_duration;
        let clock_result = {
            let duration = current_time
                .duration_since(UNIX_EPOCH)
                .map_err(current_clock_error)
                .expect("future duration should succeed");
            u64::try_from(duration.as_millis()).map_err(epoch_ms_overflow_error)
        };
        assert_eq!(
            clock_result.expect_err("millisecond conversion should overflow"),
            "epoch milliseconds overflow u64"
        );

        assert!(datetime_from_epoch_ms(u64::MAX).is_err());
    }

    #[test]
    fn status_reports_none_active_stopped_and_expired() {
        let none_workspace = TempWorkspace::new("self-work-cli", "status-none");
        let none_state_dir = none_workspace.path.to_string_lossy().to_string();
        let (status, stdout, stderr) = run(
            &["crab-self-work", "status", "--state-dir", &none_state_dir],
            1_739_173_200_000,
        );
        assert_eq!(status, 0, "stderr: {stderr}");
        assert_eq!(parse_json(&stdout), json!({"state":"none"}));

        for (label, session_status) in [
            ("status-active", SelfWorkSessionStatus::Active),
            ("status-stopped", SelfWorkSessionStatus::Stopped),
            ("status-expired", SelfWorkSessionStatus::Expired),
        ] {
            let workspace = TempWorkspace::new("self-work-cli", label);
            let state_dir = workspace.path.to_string_lossy().to_string();
            write_self_work_session_atomically(&workspace.path, &sample_session(session_status))
                .expect("session should persist");

            let (status, stdout, stderr) = run(
                &["crab-self-work", "status", "--state-dir", &state_dir],
                1_739_173_200_000,
            );
            assert_eq!(status, 0, "stderr: {stderr}");
            assert_eq!(
                parse_json(&stdout)["status"],
                serde_json::Value::String(match session_status {
                    SelfWorkSessionStatus::Active => "active".to_string(),
                    SelfWorkSessionStatus::Stopped => "stopped".to_string(),
                    SelfWorkSessionStatus::Expired => "expired".to_string(),
                })
            );
        }
    }

    #[test]
    fn stdout_write_failure_returns_error() {
        let mut bad_stdout = FailWriter;
        let mut stderr = Vec::new();
        let status = run_self_work_cli_with_now_epoch_ms(
            &[
                "crab-self-work".to_string(),
                "status".to_string(),
                "--state-dir".to_string(),
                "/tmp/state".to_string(),
            ],
            &mut bad_stdout,
            &mut stderr,
            1_739_173_200_000,
        );
        assert_eq!(status, 1);
        let stderr = String::from_utf8(stderr).expect("stderr should be utf-8");
        assert!(stderr.contains("failed to"));
    }

    #[test]
    fn write_json_reports_newline_failures_and_session_encode_errors() {
        let mut writer = FailOnNewlineWriter { bytes: Vec::new() };
        let error = write_json(&mut writer, &json!({"ok":true}))
            .expect_err("newline write failure should surface");
        assert!(error.contains("failed to write command response"));
    }

    #[test]
    fn fail_on_newline_writer_flush_is_callable() {
        let mut writer = FailOnNewlineWriter { bytes: Vec::new() };
        io::Write::flush(&mut writer).expect("flush should succeed");
    }

    #[test]
    fn stop_response_surfaces_write_errors() {
        let temp = TempWorkspace::new("self-work-cli", "stop-response-write-error");
        std::fs::create_dir_all(&temp.path).expect("state root should exist");
        std::fs::create_dir_all(crab_core::self_work_session_path(&temp.path))
            .expect("session path directory should exist");

        let error = stop_response(
            &temp.path,
            Some(sample_session(SelfWorkSessionStatus::Active)),
            1_739_173_700_000,
        )
        .expect_err("write failure should surface");
        assert!(error.contains("self_work_session_write"));
    }
}
