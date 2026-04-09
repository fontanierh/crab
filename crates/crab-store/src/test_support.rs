use std::collections::BTreeMap;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crab_core::{
    BackendKind, Checkpoint, CrabError, EventEnvelope, EventKind, EventSource, InferenceProfile,
    LaneState, LogicalSession, OutboundRecord, OwnerProfileMetadata, ProfileValueSource,
    ReasoningLevel, Run, RunProfileTelemetry, RunStatus, TokenAccounting,
};

use crate::helpers::{checkpoint_file_name, hex_encode, run_log_file_name};

pub(crate) fn sample_session(id: &str, last_activity: u64) -> LogicalSession {
    LogicalSession {
        id: id.to_string(),
        active_backend: BackendKind::Claude,
        active_profile: InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-sonnet-4-20250514".to_string(),
            reasoning_level: ReasoningLevel::Medium,
        },
        active_physical_session_id: Some("thread_1".to_string()),
        last_successful_checkpoint_id: Some("ckpt_1".to_string()),
        lane_state: LaneState::Idle,
        queued_run_count: 0,
        last_activity_epoch_ms: last_activity,
        token_accounting: TokenAccounting {
            input_tokens: 1,
            output_tokens: 2,
            total_tokens: 3,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
        },
        has_injected_bootstrap: false,
    }
}

pub(crate) fn temp_root(label: &str) -> PathBuf {
    let timestamp_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after unix epoch")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "crab-store-tests-{label}-{}-{timestamp_nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&root).expect("temporary test root should be creatable");
    root
}

pub(crate) fn cleanup(root: &Path) {
    let _ = fs::remove_dir_all(root);
}

pub(crate) fn root_as_file(label: &str) -> PathBuf {
    let root = temp_root(label);
    cleanup(&root);
    fs::write(&root, b"file-root").expect("test should create file at root path");
    root
}

pub(crate) fn sample_event(logical_session_id: &str, run_id: &str, sequence: u64) -> EventEnvelope {
    EventEnvelope {
        event_id: format!("evt-{run_id}-{sequence}"),
        run_id: run_id.to_string(),
        turn_id: Some(format!("turn:{run_id}")),
        lane_id: Some(logical_session_id.to_string()),
        logical_session_id: logical_session_id.to_string(),
        physical_session_id: Some("physical-1".to_string()),
        backend: Some(BackendKind::Claude),
        resolved_model: Some("claude-sonnet-4-20250514".to_string()),
        resolved_reasoning_level: Some("high".to_string()),
        profile_source: Some("fallback".to_string()),
        sequence,
        emitted_at_epoch_ms: 1_739_173_200_000 + sequence,
        source: EventSource::Backend,
        kind: EventKind::TextDelta,
        payload: BTreeMap::from([("text".to_string(), format!("delta-{sequence}"))]),
        profile: Some(sample_run_profile_telemetry()),
        idempotency_key: Some(format!("{run_id}:{sequence}")),
    }
}

pub(crate) fn sample_run(logical_session_id: &str, run_id: &str) -> Run {
    Run {
        id: run_id.to_string(),
        logical_session_id: logical_session_id.to_string(),
        physical_session_id: Some("physical-1".to_string()),
        status: RunStatus::Succeeded,
        user_input: "please continue".to_string(),
        delivery_channel_id: None,
        profile: sample_run_profile_telemetry(),
        queued_at_epoch_ms: 1_739_173_200_000,
        started_at_epoch_ms: Some(1_739_173_200_010),
        completed_at_epoch_ms: Some(1_739_173_200_100),
    }
}

pub(crate) fn sample_run_profile_telemetry() -> RunProfileTelemetry {
    RunProfileTelemetry {
        requested_profile: Some(InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-haiku-4-5-20251001".to_string(),
            reasoning_level: ReasoningLevel::XHigh,
        }),
        resolved_profile: InferenceProfile {
            backend: BackendKind::Claude,
            model: "claude-sonnet-4-20250514".to_string(),
            reasoning_level: ReasoningLevel::High,
        },
        backend_source: ProfileValueSource::SessionProfile,
        model_source: ProfileValueSource::BackendDefault,
        reasoning_level_source: ProfileValueSource::TurnOverride,
        fallback_applied: true,
        fallback_notes: vec![
            "claude-haiku-4-5-20251001 replaced with claude-sonnet-4-20250514".to_string(),
        ],
        sender_id: "123456789012345678".to_string(),
        sender_is_owner: true,
        resolved_owner_profile: Some(OwnerProfileMetadata {
            machine_location: Some("Berlin, Germany".to_string()),
            machine_timezone: Some("Europe/Paris".to_string()),
            default_backend: Some(BackendKind::Claude),
            default_model: Some("claude-sonnet-4-20250514".to_string()),
            default_reasoning_level: Some(ReasoningLevel::High),
        }),
    }
}

pub(crate) fn event_log_path(root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
    root.join("events")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(run_log_file_name(run_id))
}

pub(crate) fn run_path(root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
    root.join("runs")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(format!("{}.json", hex_encode(run_id.as_bytes())))
}

pub(crate) fn sample_checkpoint(
    logical_session_id: &str,
    run_id: &str,
    checkpoint_id: &str,
    created_at_epoch_ms: u64,
) -> Checkpoint {
    Checkpoint {
        id: checkpoint_id.to_string(),
        logical_session_id: logical_session_id.to_string(),
        run_id: run_id.to_string(),
        created_at_epoch_ms,
        summary: format!("summary for {checkpoint_id}"),
        memory_digest: format!("digest-{checkpoint_id}"),
        state: BTreeMap::from([
            ("lane_state".to_string(), "idle".to_string()),
            ("last_turn".to_string(), run_id.to_string()),
        ]),
    }
}

pub(crate) fn checkpoint_path(
    root: &Path,
    logical_session_id: &str,
    checkpoint_id: &str,
) -> PathBuf {
    root.join("checkpoints")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(checkpoint_file_name(checkpoint_id))
}

pub(crate) fn sample_outbound_record(
    logical_session_id: &str,
    run_id: &str,
    message_id: &str,
    edit_generation: u32,
    content_sha256: &str,
) -> OutboundRecord {
    OutboundRecord {
        record_id: format!("record-{run_id}-{message_id}-{edit_generation}"),
        logical_session_id: logical_session_id.to_string(),
        run_id: run_id.to_string(),
        channel_id: "channel-42".to_string(),
        message_id: message_id.to_string(),
        edit_generation,
        content_sha256: content_sha256.to_string(),
        delivered_at_epoch_ms: 1_739_173_200_000 + u64::from(edit_generation) + 1,
    }
}

pub(crate) fn outbound_run_path(root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
    root.join("outbound")
        .join(hex_encode(logical_session_id.as_bytes()))
        .join(run_log_file_name(run_id))
}

pub(crate) fn corrupt_index_file(root: &Path) -> PathBuf {
    let index_path = root.join("sessions.index.json");
    fs::write(&index_path, b"{ invalid json").expect("test should be able to corrupt index file");
    let backup_path = PathBuf::from(format!("{}.bak", index_path.display()));
    let _ = fs::remove_file(backup_path);
    index_path
}

pub(crate) fn assert_io_context(error: CrabError, expected_context: &'static str) {
    assert!(matches!(
        error,
        CrabError::Io {
            context,
            ..
        } if context == expected_context
    ));
}

#[cfg(unix)]
pub(crate) fn set_unix_mode(path: &Path, mode: u32) {
    let mut permissions = fs::metadata(path)
        .expect("target path should exist")
        .permissions();
    permissions.set_mode(mode);
    fs::set_permissions(path, permissions).expect("permissions update should succeed");
}
