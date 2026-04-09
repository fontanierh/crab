use crab_app::{
    run_daemon_loop_with_transport, run_self_work_cli, DaemonConfig, DaemonDiscordIo,
    DaemonLoopControl,
};
use crab_core::{
    read_pending_triggers, read_self_work_session, write_self_work_session_atomically, CrabResult,
    SelfWorkSession, SelfWorkSessionStatus, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
};
use crab_discord::GatewayMessage;
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

mod support;

use support::{runtime_config_for_workspace, TempWorkspace};

#[derive(Default)]
struct SilentDiscordIo;

impl DaemonDiscordIo for SilentDiscordIo {
    fn next_gateway_message(&mut self) -> CrabResult<Option<GatewayMessage>> {
        Ok(None)
    }

    fn post_message(
        &mut self,
        _channel_id: &str,
        _delivery_id: &str,
        _content: &str,
    ) -> CrabResult<()> {
        Ok(())
    }

    fn edit_message(
        &mut self,
        _channel_id: &str,
        _delivery_id: &str,
        _content: &str,
    ) -> CrabResult<()> {
        Ok(())
    }
}

struct FixedControl {
    now_epoch_ms: u64,
}

impl DaemonLoopControl for FixedControl {
    fn now_epoch_ms(&mut self) -> CrabResult<u64> {
        Ok(self.now_epoch_ms)
    }

    fn should_shutdown(&self) -> bool {
        false
    }

    fn sleep_tick(&mut self, _tick_interval_ms: u64) -> CrabResult<()> {
        Ok(())
    }
}

fn format_epoch_ms(epoch_ms: u64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(epoch_ms) * 1_000_000)
        .expect("timestamp should be valid")
        .format(&Rfc3339)
        .expect("timestamp formatting should succeed")
}

fn sample_active_session(now_epoch_ms: u64, end_at_epoch_ms: u64) -> SelfWorkSession {
    SelfWorkSession {
        schema_version: CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
        session_id: format!("self-work:{now_epoch_ms}"),
        channel_id: "123456789".to_string(),
        goal: "Ship the feature".to_string(),
        started_at_epoch_ms: now_epoch_ms,
        started_at_iso8601: format_epoch_ms(now_epoch_ms),
        end_at_epoch_ms,
        end_at_iso8601: format_epoch_ms(end_at_epoch_ms),
        status: SelfWorkSessionStatus::Active,
        last_wake_triggered_at_epoch_ms: None,
        final_trigger_pending: false,
        stopped_at_epoch_ms: None,
        expired_at_epoch_ms: None,
        last_expiry_triggered_at_epoch_ms: None,
    }
}

fn daemon_config() -> DaemonConfig {
    DaemonConfig {
        bot_user_id: "bot-user".to_string(),
        tick_interval_ms: 1,
        max_iterations: Some(1),
    }
}

fn run_self_work_daemon(
    label: &str,
    session: SelfWorkSession,
    now_epoch_ms: u64,
) -> (TempWorkspace, std::path::PathBuf) {
    let workspace = TempWorkspace::new("self-work-public", label);
    let state_root = workspace.path.join("state");

    write_self_work_session_atomically(&state_root, &session).expect("session should persist");

    let runtime_config = runtime_config_for_workspace(&workspace.path);
    let mut control = FixedControl { now_epoch_ms };
    let stats = run_daemon_loop_with_transport(
        &runtime_config,
        &daemon_config(),
        SilentDiscordIo,
        &mut control,
    )
    .expect("daemon loop should succeed");
    assert_eq!(stats.iterations, 1);

    (workspace, state_root)
}

#[test]
fn public_cli_entry_covers_success_and_error_paths() {
    let workspace = TempWorkspace::new("self-work-public", "cli");
    let state_dir = workspace.path.to_string_lossy().to_string();

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = run_self_work_cli(["crab-self-work", "resume"], &mut stdout, &mut stderr);
    assert_eq!(status, 1);
    assert!(String::from_utf8(stderr)
        .expect("stderr should be utf-8")
        .contains("unknown subcommand"));

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = run_self_work_cli(
        [
            "crab-self-work",
            "status",
            "--state-dir",
            state_dir.as_str(),
        ],
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(status, 0);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&stdout).expect("stdout should be valid json"),
        serde_json::json!({"state":"none"})
    );
    assert!(stderr.is_empty());

    let end_at = (OffsetDateTime::now_utc() + Duration::hours(1))
        .format(&Rfc3339)
        .expect("timestamp formatting should succeed");
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = run_self_work_cli(
        [
            "crab-self-work",
            "start",
            "--state-dir",
            state_dir.as_str(),
            "--channel",
            "123456789",
            "--goal",
            "Ship the feature",
            "--end",
            end_at.as_str(),
        ],
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(status, 0, "stderr: {}", String::from_utf8_lossy(&stderr));
    let started =
        serde_json::from_slice::<serde_json::Value>(&stdout).expect("stdout should be valid json");
    assert_eq!(started["status"], "active");
    assert_eq!(started["goal"], "Ship the feature");

    let triggers = read_pending_triggers(&workspace.path).expect("triggers should be readable");
    assert_eq!(triggers.len(), 1);
    assert!(triggers[0].1.message.contains("event: start"));

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let status = run_self_work_cli(
        ["crab-self-work", "stop", "--state-dir", state_dir.as_str()],
        &mut stdout,
        &mut stderr,
    );
    assert_eq!(status, 0, "stderr: {}", String::from_utf8_lossy(&stderr));
    let stopped =
        serde_json::from_slice::<serde_json::Value>(&stdout).expect("stdout should be valid json");
    assert_eq!(stopped["status"], "stopped");

    let session = read_self_work_session(&workspace.path)
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(session.status, SelfWorkSessionStatus::Stopped);
    assert!(session.stopped_at_epoch_ms.is_some());
}

#[test]
fn public_daemon_loop_emits_self_work_wake_trigger() {
    let now_epoch_ms = 1_739_173_600_000;
    let (_workspace, state_root) = run_self_work_daemon(
        "wake",
        sample_active_session(now_epoch_ms - 240_000, now_epoch_ms + 900_000),
        now_epoch_ms,
    );

    let triggers = read_pending_triggers(&state_root).expect("triggers should be readable");
    assert_eq!(triggers.len(), 1);
    assert!(triggers[0].1.message.contains("event: wake"));
    assert!(triggers[0]
        .1
        .message
        .contains("crab-self-work stop --state-dir"));
}

#[test]
fn public_daemon_loop_emits_self_work_expiry_trigger() {
    let now_epoch_ms = 1_739_173_800_000;
    let (_workspace, state_root) = run_self_work_daemon(
        "expiry",
        sample_active_session(now_epoch_ms - 600_000, now_epoch_ms - 1),
        now_epoch_ms,
    );

    let triggers = read_pending_triggers(&state_root).expect("triggers should be readable");
    assert_eq!(triggers.len(), 1);
    assert!(triggers[0].1.message.contains("event: expiry"));

    let session = read_self_work_session(&state_root)
        .expect("session lookup should succeed")
        .expect("session should exist");
    assert_eq!(session.status, SelfWorkSessionStatus::Expired);
    assert_eq!(
        session.last_expiry_triggered_at_epoch_ms,
        Some(now_epoch_ms)
    );
}
