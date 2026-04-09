use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crab_core::{
    Checkpoint, CrabError, CrabResult, LogicalSession, OutboundRecord, OwnerProfileMetadata, Run,
    RunProfileTelemetry,
};
use serde::de::DeserializeOwned;

use crate::session_store::SessionIndex;

pub(crate) fn read_json_with_backup<T>(path: &Path, context: &'static str) -> CrabResult<Option<T>>
where
    T: DeserializeOwned,
{
    if !path.exists() {
        return Ok(None);
    }

    match read_json_file(path, context) {
        Ok(value) => Ok(Some(value)),
        Err(CrabError::Serialization { .. }) => {
            let backup_path = backup_path(path);
            if !backup_path.exists() {
                return Err(CrabError::CorruptData {
                    context,
                    path: path.display().to_string(),
                });
            }

            match read_json_file(&backup_path, context) {
                Ok(value) => Ok(Some(value)),
                Err(_) => Err(CrabError::CorruptData {
                    context,
                    path: path.display().to_string(),
                }),
            }
        }
        Err(error) => Err(error),
    }
}

pub(crate) fn read_json_file<T>(path: &Path, context: &'static str) -> CrabResult<T>
where
    T: DeserializeOwned,
{
    let bytes = wrap_io(fs::read(path), context, path)?;
    match serde_json::from_slice(&bytes) {
        Ok(value) => Ok(value),
        Err(error) => Err(CrabError::Serialization {
            context,
            path: Some(path.display().to_string()),
            message: error.to_string(),
        }),
    }
}

pub(crate) fn write_logical_session_atomically(
    path: &Path,
    value: &LogicalSession,
    context: &'static str,
) -> CrabResult<()> {
    let encoded = serde_json::to_vec_pretty(value)
        .expect("logical session serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

pub(crate) fn write_run_atomically(
    path: &Path,
    value: &Run,
    context: &'static str,
) -> CrabResult<()> {
    let encoded = serde_json::to_vec_pretty(value).expect("run serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

pub(crate) fn write_session_index_atomically(
    path: &Path,
    value: &SessionIndex,
    context: &'static str,
) -> CrabResult<()> {
    let encoded =
        serde_json::to_vec_pretty(value).expect("session index serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

pub(crate) fn write_checkpoint_atomically(
    path: &Path,
    value: &Checkpoint,
    context: &'static str,
) -> CrabResult<()> {
    let encoded =
        serde_json::to_vec_pretty(value).expect("checkpoint serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

pub(crate) fn write_bytes_atomically(
    path: &Path,
    encoded: &[u8],
    context: &'static str,
) -> CrabResult<()> {
    let parent = path.parent().unwrap_or(Path::new("."));
    wrap_io(fs::create_dir_all(parent), context, parent)?;

    let temp_path = temp_path(path);
    wrap_io(fs::write(&temp_path, encoded), context, &temp_path)?;

    if path.exists() {
        let existing_bytes = wrap_io(fs::read(path), context, path)?;
        let backup = backup_path(path);
        wrap_io(fs::write(&backup, existing_bytes), context, &backup)?;
    }

    wrap_io(fs::rename(&temp_path, path), context, path)
}

pub(crate) fn backup_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.bak", path.display()))
}

pub(crate) fn temp_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.tmp-{}", path.display(), unix_epoch_nanos()))
}

pub(crate) fn unix_epoch_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

pub(crate) fn session_file_name(session_id: &str) -> String {
    let mut output = hex_encode(session_id.as_bytes());
    output.push_str(".json");
    output
}

pub(crate) fn run_log_file_name(run_id: &str) -> String {
    let mut output = hex_encode(run_id.as_bytes());
    output.push_str(".jsonl");
    output
}

pub(crate) fn checkpoint_file_name(checkpoint_id: &str) -> String {
    let mut output = hex_encode(checkpoint_id.as_bytes());
    output.push_str(".json");
    output
}

pub(crate) fn ensure_non_empty_field(
    context: &'static str,
    field_name: &'static str,
    value: &str,
) -> CrabResult<()> {
    if !value.trim().is_empty() {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context,
        message: format!("{field_name} must not be empty"),
    })
}

pub(crate) fn validate_checkpoint(checkpoint: &Checkpoint) -> CrabResult<()> {
    ensure_non_empty_field("checkpoint_validate", "id", &checkpoint.id)?;
    ensure_non_empty_field(
        "checkpoint_validate",
        "logical_session_id",
        &checkpoint.logical_session_id,
    )?;
    ensure_non_empty_field("checkpoint_validate", "run_id", &checkpoint.run_id)?;
    ensure_non_empty_field("checkpoint_validate", "summary", &checkpoint.summary)?;
    ensure_non_empty_field(
        "checkpoint_validate",
        "memory_digest",
        &checkpoint.memory_digest,
    )?;
    if checkpoint.created_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "checkpoint_validate",
            message: "created_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    for key in checkpoint.state.keys() {
        ensure_non_empty_field("checkpoint_validate", "state key", key)?;
    }
    Ok(())
}

pub(crate) fn validate_run(run: &Run) -> CrabResult<()> {
    ensure_non_empty_field("run_validate", "id", &run.id)?;
    ensure_non_empty_field(
        "run_validate",
        "logical_session_id",
        &run.logical_session_id,
    )?;
    ensure_non_empty_field("run_validate", "user_input", &run.user_input)?;
    if let Some(channel_id) = run.delivery_channel_id.as_deref() {
        ensure_non_empty_field("run_validate", "delivery_channel_id", channel_id)?;
    }
    ensure_non_empty_field(
        "run_validate",
        "profile.resolved_profile.model",
        &run.profile.resolved_profile.model,
    )?;

    if let Some(requested) = run.profile.requested_profile.as_ref() {
        ensure_non_empty_field(
            "run_validate",
            "profile.requested_profile.model",
            &requested.model,
        )?;
    }
    for note in &run.profile.fallback_notes {
        ensure_non_empty_field("run_validate", "profile.fallback_notes[]", note)?;
    }
    if run.profile.fallback_applied && run.profile.fallback_notes.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "run_validate",
            message: "profile.fallback_notes must not be empty when fallback_applied is true"
                .to_string(),
        });
    }
    validate_profile_sender_context("run_validate", &run.profile)?;

    Ok(())
}

pub(crate) fn validate_profile_sender_context(
    context: &'static str,
    profile: &RunProfileTelemetry,
) -> CrabResult<()> {
    ensure_non_empty_field(context, "profile.sender_id", &profile.sender_id)?;
    if !profile.sender_is_owner && profile.resolved_owner_profile.is_some() {
        return Err(CrabError::InvariantViolation {
            context,
            message: "profile.resolved_owner_profile must be absent when sender_is_owner is false"
                .to_string(),
        });
    }
    if let Some(owner_profile) = profile.resolved_owner_profile.as_ref() {
        validate_owner_profile_metadata(context, owner_profile)?;
    }
    Ok(())
}

pub(crate) fn validate_owner_profile_metadata(
    context: &'static str,
    owner_profile: &OwnerProfileMetadata,
) -> CrabResult<()> {
    if let Some(machine_location) = owner_profile.machine_location.as_ref() {
        ensure_non_empty_field(
            context,
            "profile.resolved_owner_profile.machine_location",
            machine_location,
        )?;
    }
    if let Some(machine_timezone) = owner_profile.machine_timezone.as_ref() {
        ensure_non_empty_field(
            context,
            "profile.resolved_owner_profile.machine_timezone",
            machine_timezone,
        )?;
    }
    if let Some(default_model) = owner_profile.default_model.as_ref() {
        ensure_non_empty_field(
            context,
            "profile.resolved_owner_profile.default_model",
            default_model,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_outbound_record(record: &OutboundRecord) -> CrabResult<()> {
    ensure_non_empty_field("outbound_record_validate", "record_id", &record.record_id)?;
    ensure_non_empty_field(
        "outbound_record_validate",
        "logical_session_id",
        &record.logical_session_id,
    )?;
    ensure_non_empty_field("outbound_record_validate", "run_id", &record.run_id)?;
    ensure_non_empty_field("outbound_record_validate", "channel_id", &record.channel_id)?;
    ensure_non_empty_field("outbound_record_validate", "message_id", &record.message_id)?;
    ensure_non_empty_field(
        "outbound_record_validate",
        "content_sha256",
        &record.content_sha256,
    )?;
    if record.delivered_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "outbound_record_validate",
            message: "delivered_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    Ok(())
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];

    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let upper = usize::from(byte >> 4);
        let lower = usize::from(byte & 0x0f);
        output.push(HEX[upper]);
        output.push(HEX[lower]);
    }
    output
}

pub(crate) fn io_error(context: &'static str, path: &Path, error: std::io::Error) -> CrabError {
    CrabError::Io {
        context,
        path: Some(path.display().to_string()),
        message: error.to_string(),
    }
}

pub(crate) fn wrap_io<T>(
    result: std::io::Result<T>,
    context: &'static str,
    path: &Path,
) -> CrabResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => Err(io_error(context, path, error)),
    }
}

pub(crate) fn write_all_with_context(
    writer: &mut impl Write,
    bytes: &[u8],
    context: &'static str,
    path: &Path,
) -> CrabResult<()> {
    wrap_io(writer.write_all(bytes), context, path)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use crab_core::LogicalSession;

    use crate::session_store::{SessionIndex, SessionIndexEntry};
    use crate::test_support::{
        assert_io_context, cleanup, sample_session, set_unix_mode, temp_root,
    };

    use super::{
        read_json_file, session_file_name, write_logical_session_atomically,
        write_session_index_atomically,
    };

    #[test]
    fn hex_encoded_file_name_is_deterministic() {
        let encoded = session_file_name("discord:channel:abc");
        assert_eq!(
            encoded,
            "646973636f72643a6368616e6e656c3a616263.json".to_string()
        );
    }

    #[test]
    fn write_session_index_atomically_round_trip() {
        let index = SessionIndex {
            sessions: BTreeMap::from([(
                "discord:channel:abc".to_string(),
                SessionIndexEntry {
                    file_name: "646973636f72643a6368616e6e656c3a616263.json".to_string(),
                    last_activity_epoch_ms: 42,
                },
            )]),
        };

        let root = temp_root("index-round-trip");
        let index_path = root.join("sessions.index.json");
        write_session_index_atomically(&index_path, &index, "session_index_write")
            .expect("index fixture write should succeed");
        let parsed: SessionIndex =
            read_json_file(&index_path, "session_index_read").expect("index fixture should parse");
        assert_eq!(parsed, index);
        cleanup(&root);
    }

    #[test]
    fn write_logical_session_atomically_round_trip() {
        let root = temp_root("session-round-trip");
        let path = root.join("session.json");
        let value = sample_session("discord:channel:test", 11);
        write_logical_session_atomically(&path, &value, "session_write")
            .expect("session fixture write should succeed");

        let parsed: LogicalSession =
            read_json_file(&path, "session_read").expect("session fixture should parse");
        assert_eq!(parsed, value);

        cleanup(&root);
    }

    #[test]
    fn write_logical_session_atomically_surfaces_parent_io_error() {
        let root = temp_root("parent-io-error");
        let parent_as_file = root.join("parent-file");
        fs::write(&parent_as_file, b"not-a-directory")
            .expect("test should be able to create parent file");

        let path = parent_as_file.join("child.json");
        let error = write_logical_session_atomically(
            &path,
            &sample_session("discord:channel:x", 9),
            "session_write",
        )
        .expect_err("invalid parent should return io error");
        assert!(matches!(
            error,
            crab_core::CrabError::Io {
                context: "session_write",
                ..
            }
        ));

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn write_logical_session_atomically_surfaces_temp_write_error() {
        let root = temp_root("temp-write-error");
        set_unix_mode(&root, 0o500);
        let path = root.join("session.json");
        let error = write_logical_session_atomically(
            &path,
            &sample_session("discord:channel:temp-write", 6),
            "session_write",
        )
        .expect_err("read-only parent should prevent temp write");
        assert_io_context(error, "session_write");
        set_unix_mode(&root, 0o700);

        cleanup(&root);
    }

    #[test]
    fn write_logical_session_atomically_surfaces_backup_write_error() {
        let root = temp_root("backup-write-error");
        let path = root.join("session.json");
        write_logical_session_atomically(
            &path,
            &sample_session("discord:channel:backup-write", 7),
            "session_write",
        )
        .expect("initial write should succeed");

        let backup_path = std::path::PathBuf::from(format!("{}.bak", path.display()));
        fs::create_dir_all(&backup_path).expect("test should create directory at backup path");
        let error = write_logical_session_atomically(
            &path,
            &sample_session("discord:channel:backup-write", 8),
            "session_write",
        )
        .expect_err("backup write should fail when backup path is a directory");
        assert_io_context(error, "session_write");

        cleanup(&root);
    }
}
