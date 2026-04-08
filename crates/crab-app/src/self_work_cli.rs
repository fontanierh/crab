use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crab_core::{
    read_self_work_session, validate_new_self_work_start, write_pending_trigger,
    write_self_work_session_atomically, PendingTrigger, SelfWorkSession, SelfWorkSessionLock,
    SelfWorkSessionStatus, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
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
    let argv: Vec<String> = args.into_iter().map(Into::into).collect();
    let now_epoch_ms = match current_epoch_ms() {
        Ok(value) => value,
        Err(message) => return write_error(stderr, &message),
    };
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

    let mut lock = SelfWorkSessionLock::acquire(&state_root, now_epoch_ms)
        .map_err(|error| error.to_string())?;
    let existing = read_self_work_session(&state_root).map_err(|error| error.to_string())?;
    validate_new_self_work_start(existing.as_ref()).map_err(|error| error.to_string())?;

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

    write_self_work_session_atomically(&state_root, &session).map_err(|error| error.to_string())?;
    write_pending_trigger(
        &state_root,
        &PendingTrigger {
            channel_id,
            message: build_start_trigger_message(&session, &state_root),
        },
    )
    .map_err(|error| error.to_string())?;
    lock.release().map_err(|error| error.to_string())?;

    session_response(Some(&session))
}

fn execute_stop(args: &[String], now_epoch_ms: u64) -> Result<Value, String> {
    let mut state_dir: Option<String> = None;

    cli_support::parse_flag_pairs(
        args,
        |flag| flag == "--state-dir",
        |flag, value| cli_support::assign_flag(&mut state_dir, flag, value),
    )?;

    let state_root = PathBuf::from(cli_support::require_non_empty(state_dir, "--state-dir")?);
    let mut lock = SelfWorkSessionLock::acquire(&state_root, now_epoch_ms)
        .map_err(|error| error.to_string())?;
    let maybe_session = read_self_work_session(&state_root).map_err(|error| error.to_string())?;

    let response = match maybe_session {
        None => session_response(None),
        Some(mut session) => {
            if session.status == SelfWorkSessionStatus::Active {
                session.status = SelfWorkSessionStatus::Stopped;
                session.final_trigger_pending = false;
                session.stopped_at_epoch_ms = Some(now_epoch_ms);
                write_self_work_session_atomically(&state_root, &session)
                    .map_err(|error| error.to_string())?;
            }
            session_response(Some(&session))
        }
    }?;
    lock.release().map_err(|error| error.to_string())?;
    Ok(response)
}

fn execute_status(args: &[String]) -> Result<Value, String> {
    let mut state_dir: Option<String> = None;

    cli_support::parse_flag_pairs(
        args,
        |flag| flag == "--state-dir",
        |flag, value| cli_support::assign_flag(&mut state_dir, flag, value),
    )?;

    let state_root = PathBuf::from(cli_support::require_non_empty(state_dir, "--state-dir")?);
    let maybe_session = read_self_work_session(&state_root).map_err(|error| error.to_string())?;
    session_response(maybe_session.as_ref())
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
        .map_err(|error| format!("failed to read system clock: {error}"))?;
    u64::try_from(duration.as_millis()).map_err(|_| "epoch milliseconds overflow u64".to_string())
}

fn parse_end_timestamp(raw_value: &str) -> Result<(u64, String), String> {
    let parsed = OffsetDateTime::parse(raw_value, &Rfc3339)
        .map_err(|error| format!("invalid --end timestamp: {error}"))?
        .to_offset(time::UtcOffset::UTC);
    let epoch_ms = epoch_ms_from_datetime(parsed)?;
    let iso8601 = parsed
        .format(&Rfc3339)
        .map_err(|error| format!("failed to format --end timestamp: {error}"))?;
    Ok((epoch_ms, iso8601))
}

fn format_epoch_ms(epoch_ms: u64) -> Result<String, String> {
    let datetime = datetime_from_epoch_ms(epoch_ms)?;
    datetime
        .format(&Rfc3339)
        .map_err(|error| format!("failed to format timestamp: {error}"))
}

fn epoch_ms_from_datetime(value: OffsetDateTime) -> Result<u64, String> {
    let unix_timestamp_nanos = value.unix_timestamp_nanos();
    if unix_timestamp_nanos < 0 {
        return Err("timestamp must not be before the unix epoch".to_string());
    }
    let unix_timestamp_ms = unix_timestamp_nanos / 1_000_000;
    u64::try_from(unix_timestamp_ms).map_err(|_| "timestamp milliseconds overflow u64".to_string())
}

fn datetime_from_epoch_ms(epoch_ms: u64) -> Result<OffsetDateTime, String> {
    let nanos = i128::from(epoch_ms)
        .checked_mul(1_000_000)
        .ok_or_else(|| "timestamp milliseconds overflow i128".to_string())?;
    OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .map_err(|error| format!("failed to build timestamp: {error}"))
}

fn write_json(stdout: &mut dyn Write, value: &Value) -> Result<(), String> {
    serde_json::to_writer(&mut *stdout, value)
        .map_err(|error| format!("failed to serialize command response: {error}"))?;
    writeln!(stdout).map_err(|error| format!("failed to write command response: {error}"))?;
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
        Some(session) => serde_json::to_value(session)
            .map_err(|error| format!("failed to encode session: {error}")),
        None => Ok(json!({"state":"none"})),
    }
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
    use crab_core::{
        read_pending_triggers, write_self_work_session_atomically, SelfWorkSession,
        SelfWorkSessionStatus, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
    };
    use serde_json::{json, Value};

    use super::{run_self_work_cli_with_now_epoch_ms, MAX_SELF_WORK_DURATION_MS};
    use crate::cli_support::test_helpers::FailWriter;
    use crate::test_support::TempWorkspace;

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

    fn sample_session(status: SelfWorkSessionStatus) -> SelfWorkSession {
        SelfWorkSession {
            schema_version: CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
            session_id: "self-work:1739173200000".to_string(),
            channel_id: "123456789".to_string(),
            goal: "Ship the feature".to_string(),
            started_at_epoch_ms: 1_739_173_200_000,
            started_at_iso8601: "2025-02-10T10:00:00Z".to_string(),
            end_at_epoch_ms: 1_739_174_100_000,
            end_at_iso8601: "2025-02-10T10:15:00Z".to_string(),
            status,
            last_wake_triggered_at_epoch_ms: None,
            final_trigger_pending: false,
            stopped_at_epoch_ms: (status == SelfWorkSessionStatus::Stopped)
                .then_some(1_739_173_600_000),
            expired_at_epoch_ms: (status == SelfWorkSessionStatus::Expired)
                .then_some(1_739_174_100_000),
            last_expiry_triggered_at_epoch_ms: (status == SelfWorkSessionStatus::Expired)
                .then_some(1_739_174_100_000),
        }
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
}
