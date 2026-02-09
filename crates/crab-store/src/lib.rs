//! Storage primitives for Crab runtime state.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crab_core::{Checkpoint, CrabError, CrabResult, EventEnvelope, LogicalSession, OutboundRecord};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

const INDEX_FILE_NAME: &str = "sessions.index.json";
const SESSIONS_DIR_NAME: &str = "sessions";
const CHECKPOINTS_DIR_NAME: &str = "checkpoints";
const OUTBOUND_DIR_NAME: &str = "outbound";

#[derive(Debug, Clone)]
pub struct SessionStore {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
struct SessionIndex {
    sessions: BTreeMap<String, SessionIndexEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SessionIndexEntry {
    file_name: String,
    last_activity_epoch_ms: u64,
}

impl SessionStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn upsert_session(&self, session: &LogicalSession) -> CrabResult<()> {
        self.ensure_layout()?;

        let session_file_name = session_file_name(&session.id);
        let session_path = self.sessions_dir().join(&session_file_name);
        write_logical_session_atomically(&session_path, session, "session_write")?;

        let mut index = self.load_index()?;
        index.sessions.insert(
            session.id.clone(),
            SessionIndexEntry {
                file_name: session_file_name,
                last_activity_epoch_ms: session.last_activity_epoch_ms,
            },
        );
        self.persist_index(&index)
    }

    pub fn get_session(&self, session_id: &str) -> CrabResult<Option<LogicalSession>> {
        self.ensure_layout()?;
        let session_path = self.sessions_dir().join(session_file_name(session_id));
        read_json_with_backup(&session_path, "session_read")
    }

    pub fn list_session_ids(&self) -> CrabResult<Vec<String>> {
        self.ensure_layout()?;
        let index = self.load_index()?;
        Ok(index.sessions.keys().cloned().collect())
    }

    fn ensure_layout(&self) -> CrabResult<()> {
        wrap_io(
            fs::create_dir_all(&self.root),
            "session_store_layout",
            &self.root,
        )?;
        let sessions_dir = self.sessions_dir();
        wrap_io(
            fs::create_dir_all(&sessions_dir),
            "session_store_layout",
            &sessions_dir,
        )?;
        Ok(())
    }

    fn sessions_dir(&self) -> PathBuf {
        self.root.join(SESSIONS_DIR_NAME)
    }

    fn index_path(&self) -> PathBuf {
        self.root.join(INDEX_FILE_NAME)
    }

    fn load_index(&self) -> CrabResult<SessionIndex> {
        let index_path = self.index_path();
        match read_json_with_backup::<SessionIndex>(&index_path, "session_index_read") {
            Ok(Some(index)) => Ok(index),
            Ok(None) => Ok(SessionIndex::default()),
            Err(CrabError::CorruptData { .. }) => {
                let rebuilt = self.rebuild_index_from_session_files()?;
                self.persist_index(&rebuilt)?;
                Ok(rebuilt)
            }
            Err(error) => Err(error),
        }
    }

    fn persist_index(&self, index: &SessionIndex) -> CrabResult<()> {
        write_session_index_atomically(&self.index_path(), index, "session_index_write")
    }

    fn rebuild_index_from_session_files(&self) -> CrabResult<SessionIndex> {
        let sessions_dir = self.sessions_dir();
        let mut sessions = BTreeMap::new();
        let entries = wrap_io(
            fs::read_dir(&sessions_dir),
            "session_index_rebuild",
            &sessions_dir,
        )?;

        for entry in entries.flatten() {
            let file_name = entry.file_name().to_string_lossy().into_owned();
            let path = entry.path();
            if path.extension() != Some(OsStr::new("json")) {
                continue;
            }
            let maybe_session =
                match read_json_with_backup::<LogicalSession>(&path, "session_rebuild_read") {
                    Ok(session) => session,
                    Err(CrabError::CorruptData { .. }) => None,
                    Err(error) => return Err(error),
                };
            if let Some(session) = maybe_session {
                sessions.insert(
                    session.id.clone(),
                    SessionIndexEntry {
                        file_name,
                        last_activity_epoch_ms: session.last_activity_epoch_ms,
                    },
                );
            }
        }

        Ok(SessionIndex { sessions })
    }
}

#[derive(Debug, Clone)]
pub struct EventStore {
    root: PathBuf,
}

impl EventStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn append_event(&self, event: &EventEnvelope) -> CrabResult<()> {
        self.ensure_layout()?;

        let expected_sequence = self
            .replay_run(&event.logical_session_id, &event.run_id)?
            .last()
            .map_or(1, |last| last.sequence + 1);

        if event.sequence != expected_sequence {
            return Err(CrabError::InvariantViolation {
                context: "event_append_sequence",
                message: format!(
                    "expected sequence {expected_sequence}, got {}",
                    event.sequence
                ),
            });
        }

        let run_log_path = self.run_log_path(&event.logical_session_id, &event.run_id);
        let parent = run_log_path.parent().unwrap_or(Path::new("."));
        wrap_io(fs::create_dir_all(parent), "event_append_layout", parent)?;

        let encoded = serde_json::to_string(event)
            .expect("event envelope serialization should be infallible");
        let line = format!("{encoded}\n");
        let mut file = wrap_io(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&run_log_path),
            "event_append_open",
            &run_log_path,
        )?;
        wrap_io(
            file.write_all(line.as_bytes()),
            "event_append_write",
            &run_log_path,
        )
    }

    pub fn replay_run(
        &self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<Vec<EventEnvelope>> {
        self.ensure_layout()?;

        let run_log_path = self.run_log_path(logical_session_id, run_id);
        if !run_log_path.exists() {
            return Ok(Vec::new());
        }

        let content = wrap_io(
            fs::read_to_string(&run_log_path),
            "event_replay_read",
            &run_log_path,
        )?;

        let mut events = Vec::new();
        let mut expected_sequence = 1_u64;
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let parsed: EventEnvelope = match serde_json::from_str(line) {
                Ok(event) => event,
                Err(_) => {
                    return Err(CrabError::CorruptData {
                        context: "event_replay_parse",
                        path: run_log_path.display().to_string(),
                    });
                }
            };

            if parsed.sequence != expected_sequence {
                return Err(CrabError::InvariantViolation {
                    context: "event_replay_sequence",
                    message: format!(
                        "expected sequence {expected_sequence}, got {}",
                        parsed.sequence
                    ),
                });
            }

            expected_sequence += 1;
            events.push(parsed);
        }

        Ok(events)
    }

    fn ensure_layout(&self) -> CrabResult<()> {
        wrap_io(
            fs::create_dir_all(&self.root),
            "event_store_layout",
            &self.root,
        )?;
        let events_root = self.events_root();
        wrap_io(
            fs::create_dir_all(&events_root),
            "event_store_layout",
            &events_root,
        )?;
        Ok(())
    }

    fn events_root(&self) -> PathBuf {
        self.root.join("events")
    }

    fn session_events_dir(&self, logical_session_id: &str) -> PathBuf {
        self.events_root()
            .join(hex_encode(logical_session_id.as_bytes()))
    }

    fn run_log_path(&self, logical_session_id: &str, run_id: &str) -> PathBuf {
        self.session_events_dir(logical_session_id)
            .join(run_log_file_name(run_id))
    }
}

#[derive(Debug, Clone)]
pub struct CheckpointStore {
    root: PathBuf,
}

impl CheckpointStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn put_checkpoint(&self, checkpoint: &Checkpoint) -> CrabResult<()> {
        validate_checkpoint(checkpoint)?;
        self.ensure_layout()?;
        let checkpoint_path = self.checkpoint_path(&checkpoint.logical_session_id, &checkpoint.id);
        write_checkpoint_atomically(&checkpoint_path, checkpoint, "checkpoint_write")
    }

    pub fn get_checkpoint(
        &self,
        logical_session_id: &str,
        checkpoint_id: &str,
    ) -> CrabResult<Option<Checkpoint>> {
        self.ensure_layout()?;

        let checkpoint_path = self.checkpoint_path(logical_session_id, checkpoint_id);
        let maybe_checkpoint =
            read_json_with_backup::<Checkpoint>(&checkpoint_path, "checkpoint_read")?;
        if let Some(checkpoint) = maybe_checkpoint {
            if checkpoint.logical_session_id != logical_session_id {
                return Err(CrabError::InvariantViolation {
                    context: "checkpoint_get_session_mismatch",
                    message: format!(
                        "checkpoint {} belongs to {}, expected {}",
                        checkpoint.id, checkpoint.logical_session_id, logical_session_id
                    ),
                });
            }
            return Ok(Some(checkpoint));
        }
        Ok(None)
    }

    pub fn latest_checkpoint(&self, logical_session_id: &str) -> CrabResult<Option<Checkpoint>> {
        self.ensure_layout()?;

        let session_dir = self.session_checkpoint_dir(logical_session_id);
        if !session_dir.exists() {
            return Ok(None);
        }

        let entries = wrap_io(
            fs::read_dir(&session_dir),
            "checkpoint_latest_read_dir",
            &session_dir,
        )?;

        let mut latest: Option<Checkpoint> = None;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension() != Some(OsStr::new("json")) {
                continue;
            }

            let maybe_checkpoint = read_json_with_backup::<Checkpoint>(&path, "checkpoint_read")?;
            let Some(checkpoint) = maybe_checkpoint else {
                continue;
            };
            if checkpoint.logical_session_id != logical_session_id {
                return Err(CrabError::InvariantViolation {
                    context: "checkpoint_latest_session_mismatch",
                    message: format!(
                        "checkpoint {} belongs to {}, expected {}",
                        checkpoint.id, checkpoint.logical_session_id, logical_session_id
                    ),
                });
            }

            let should_replace = match latest.as_ref() {
                None => true,
                Some(current) => {
                    checkpoint.created_at_epoch_ms > current.created_at_epoch_ms
                        || (checkpoint.created_at_epoch_ms == current.created_at_epoch_ms
                            && checkpoint.id > current.id)
                }
            };
            if should_replace {
                latest = Some(checkpoint);
            }
        }

        Ok(latest)
    }

    fn ensure_layout(&self) -> CrabResult<()> {
        wrap_io(
            fs::create_dir_all(&self.root),
            "checkpoint_store_layout",
            &self.root,
        )?;
        let checkpoints_root = self.checkpoints_root();
        wrap_io(
            fs::create_dir_all(&checkpoints_root),
            "checkpoint_store_layout",
            &checkpoints_root,
        )?;
        Ok(())
    }

    fn checkpoints_root(&self) -> PathBuf {
        self.root.join(CHECKPOINTS_DIR_NAME)
    }

    fn session_checkpoint_dir(&self, logical_session_id: &str) -> PathBuf {
        self.checkpoints_root()
            .join(hex_encode(logical_session_id.as_bytes()))
    }

    fn checkpoint_path(&self, logical_session_id: &str, checkpoint_id: &str) -> PathBuf {
        self.session_checkpoint_dir(logical_session_id)
            .join(checkpoint_file_name(checkpoint_id))
    }
}

#[derive(Debug, Clone)]
pub struct OutboundRecordStore {
    root: PathBuf,
}

impl OutboundRecordStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn record_or_skip_duplicate(&self, record: &OutboundRecord) -> CrabResult<bool> {
        validate_outbound_record(record)?;
        self.ensure_layout()?;
        let existing = self.list_run_records(&record.logical_session_id, &record.run_id)?;
        for persisted in existing {
            let same_target = persisted.channel_id == record.channel_id
                && persisted.message_id == record.message_id
                && persisted.edit_generation == record.edit_generation;
            if !same_target {
                continue;
            }

            if persisted.content_sha256 == record.content_sha256 {
                return Ok(false);
            }

            return Err(CrabError::InvariantViolation {
                context: "outbound_record_conflict",
                message: format!(
                    "conflicting content hash for channel={}, message={}, edit_generation={}",
                    record.channel_id, record.message_id, record.edit_generation
                ),
            });
        }

        self.append_record(record)?;
        Ok(true)
    }

    pub fn list_run_records(
        &self,
        logical_session_id: &str,
        run_id: &str,
    ) -> CrabResult<Vec<OutboundRecord>> {
        self.ensure_layout()?;

        let records_path = self.run_records_path(logical_session_id, run_id);
        if !records_path.exists() {
            return Ok(Vec::new());
        }

        let content = wrap_io(
            fs::read_to_string(&records_path),
            "outbound_record_read",
            &records_path,
        )?;
        let mut records = Vec::new();
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }

            let parsed: OutboundRecord = match serde_json::from_str(line) {
                Ok(record) => record,
                Err(_) => {
                    return Err(CrabError::CorruptData {
                        context: "outbound_record_parse",
                        path: records_path.display().to_string(),
                    });
                }
            };

            if parsed.logical_session_id != logical_session_id || parsed.run_id != run_id {
                return Err(CrabError::InvariantViolation {
                    context: "outbound_record_identity_mismatch",
                    message: format!(
                        "record {} is for {}/{} but expected {}/{}",
                        parsed.record_id,
                        parsed.logical_session_id,
                        parsed.run_id,
                        logical_session_id,
                        run_id
                    ),
                });
            }
            records.push(parsed);
        }
        Ok(records)
    }

    fn append_record(&self, record: &OutboundRecord) -> CrabResult<()> {
        let records_path = self.run_records_path(&record.logical_session_id, &record.run_id);
        let parent = records_path.parent().unwrap_or(Path::new("."));
        wrap_io(fs::create_dir_all(parent), "outbound_record_layout", parent)?;

        let encoded = serde_json::to_string(record)
            .expect("outbound record serialization should be infallible");
        let line = format!("{encoded}\n");
        let mut file = wrap_io(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&records_path),
            "outbound_record_open",
            &records_path,
        )?;
        wrap_io(
            file.write_all(line.as_bytes()),
            "outbound_record_write",
            &records_path,
        )
    }

    fn ensure_layout(&self) -> CrabResult<()> {
        wrap_io(
            fs::create_dir_all(&self.root),
            "outbound_record_store_layout",
            &self.root,
        )?;
        let outbound_root = self.outbound_root();
        wrap_io(
            fs::create_dir_all(&outbound_root),
            "outbound_record_store_layout",
            &outbound_root,
        )?;
        Ok(())
    }

    fn outbound_root(&self) -> PathBuf {
        self.root.join(OUTBOUND_DIR_NAME)
    }

    fn session_outbound_dir(&self, logical_session_id: &str) -> PathBuf {
        self.outbound_root()
            .join(hex_encode(logical_session_id.as_bytes()))
    }

    fn run_records_path(&self, logical_session_id: &str, run_id: &str) -> PathBuf {
        self.session_outbound_dir(logical_session_id)
            .join(run_log_file_name(run_id))
    }
}

fn read_json_with_backup<T>(path: &Path, context: &'static str) -> CrabResult<Option<T>>
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

fn read_json_file<T>(path: &Path, context: &'static str) -> CrabResult<T>
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

fn write_logical_session_atomically(
    path: &Path,
    value: &LogicalSession,
    context: &'static str,
) -> CrabResult<()> {
    let encoded = serde_json::to_vec_pretty(value)
        .expect("logical session serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

fn write_session_index_atomically(
    path: &Path,
    value: &SessionIndex,
    context: &'static str,
) -> CrabResult<()> {
    let encoded =
        serde_json::to_vec_pretty(value).expect("session index serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

fn write_checkpoint_atomically(
    path: &Path,
    value: &Checkpoint,
    context: &'static str,
) -> CrabResult<()> {
    let encoded =
        serde_json::to_vec_pretty(value).expect("checkpoint serialization should be infallible");
    write_bytes_atomically(path, &encoded, context)
}

fn write_bytes_atomically(path: &Path, encoded: &[u8], context: &'static str) -> CrabResult<()> {
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

fn backup_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.bak", path.display()))
}

fn temp_path(path: &Path) -> PathBuf {
    PathBuf::from(format!("{}.tmp-{}", path.display(), unix_epoch_nanos()))
}

fn unix_epoch_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

fn session_file_name(session_id: &str) -> String {
    let mut output = hex_encode(session_id.as_bytes());
    output.push_str(".json");
    output
}

fn run_log_file_name(run_id: &str) -> String {
    let mut output = hex_encode(run_id.as_bytes());
    output.push_str(".jsonl");
    output
}

fn checkpoint_file_name(checkpoint_id: &str) -> String {
    let mut output = hex_encode(checkpoint_id.as_bytes());
    output.push_str(".json");
    output
}

fn ensure_non_empty_field(
    context: &'static str,
    field_name: &'static str,
    value: &str,
) -> CrabResult<()> {
    if value.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{field_name} must not be empty"),
        });
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: &Checkpoint) -> CrabResult<()> {
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

fn validate_outbound_record(record: &OutboundRecord) -> CrabResult<()> {
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

fn hex_encode(bytes: &[u8]) -> String {
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

fn io_error(context: &'static str, path: &Path, error: std::io::Error) -> CrabError {
    CrabError::Io {
        context,
        path: Some(path.display().to_string()),
        message: error.to_string(),
    }
}

fn wrap_io<T>(result: std::io::Result<T>, context: &'static str, path: &Path) -> CrabResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(error) => Err(io_error(context, path, error)),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crab_core::{
        BackendKind, Checkpoint, CrabError, EventEnvelope, EventKind, EventSource,
        InferenceProfile, LaneState, LogicalSession, OutboundRecord, ReasoningLevel,
        TokenAccounting,
    };

    use super::{
        checkpoint_file_name, read_json_file, run_log_file_name, session_file_name,
        write_logical_session_atomically, write_session_index_atomically, CheckpointStore,
        EventStore, OutboundRecordStore, SessionIndex, SessionStore,
    };

    #[test]
    fn upsert_and_load_round_trip() {
        let root = temp_root("upsert-and-load");
        let store = SessionStore::new(&root);
        let session = sample_session("discord:channel:abc", 101);

        store
            .upsert_session(&session)
            .expect("session upsert should succeed");

        let loaded = store
            .get_session(&session.id)
            .expect("session read should succeed")
            .expect("session should exist");
        assert_eq!(loaded, session);

        let ids = store
            .list_session_ids()
            .expect("listing session ids should succeed");
        assert_eq!(ids, vec![session.id]);

        cleanup(&root);
    }

    #[test]
    fn missing_session_returns_none() {
        let root = temp_root("missing-session");
        let store = SessionStore::new(&root);

        let loaded = store
            .get_session("discord:channel:missing")
            .expect("missing lookup should not error");
        assert!(loaded.is_none());

        cleanup(&root);
    }

    #[test]
    fn corrupted_primary_session_recovers_from_backup() {
        let root = temp_root("recover-from-backup");
        let store = SessionStore::new(&root);

        let original = sample_session("discord:channel:abc", 200);
        let updated = sample_session("discord:channel:abc", 300);

        store
            .upsert_session(&original)
            .expect("initial session write should succeed");
        store
            .upsert_session(&updated)
            .expect("second session write should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:abc"));
        fs::write(&session_path, b"{ invalid json")
            .expect("test should be able to corrupt session file");

        let recovered = store
            .get_session("discord:channel:abc")
            .expect("backup recovery should succeed")
            .expect("session should still be readable");
        assert_eq!(recovered, original);

        cleanup(&root);
    }

    #[test]
    fn corruption_without_backup_returns_error() {
        let root = temp_root("corrupt-without-backup");
        let store = SessionStore::new(&root);
        let session = sample_session("discord:channel:single-write", 50);

        store
            .upsert_session(&session)
            .expect("initial session write should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:single-write"));
        fs::write(&session_path, b"{ invalid json")
            .expect("test should be able to corrupt session file");

        let error = store
            .get_session("discord:channel:single-write")
            .expect_err("missing backup should return corruption error");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "session_read",
                path: session_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn corrupted_index_rebuilds_from_session_files() {
        let root = temp_root("index-rebuild");
        let store = SessionStore::new(&root);
        let first = sample_session("discord:channel:first", 1);
        let second = sample_session("discord:channel:second", 2);

        store
            .upsert_session(&first)
            .expect("first session write should succeed");
        store
            .upsert_session(&second)
            .expect("second session write should succeed");

        let index_path = corrupt_index_file(&root);
        fs::write(root.join("sessions").join("ignore.txt"), b"ignore")
            .expect("test should be able to create non-json file");
        fs::write(
            root.join("sessions").join("corrupt.json"),
            b"{ invalid json",
        )
        .expect("test should be able to create corrupt json file");

        let ids = store
            .list_session_ids()
            .expect("corrupt index should rebuild from state files");
        assert_eq!(
            ids,
            vec![
                "discord:channel:first".to_string(),
                "discord:channel:second".to_string()
            ]
        );

        let rebuilt_index: SessionIndex =
            read_json_file(&index_path, "session_index_read").expect("rebuilt index should parse");
        assert_eq!(rebuilt_index.sessions.len(), 2);

        cleanup(&root);
    }

    #[test]
    fn rebuild_index_propagates_io_errors_from_session_entries() {
        let root = temp_root("index-rebuild-io-error");
        let store = SessionStore::new(&root);
        store
            .upsert_session(&sample_session("discord:channel:valid", 5))
            .expect("session write should succeed");
        let _ = corrupt_index_file(&root);
        fs::create_dir_all(root.join("sessions").join("broken.json"))
            .expect("test should be able to create directory with json extension");

        let error = store
            .list_session_ids()
            .expect_err("rebuild should propagate io errors from session entries");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_rebuild_read",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn corrupted_primary_and_backup_returns_corruption_error() {
        let root = temp_root("corrupt-primary-and-backup");
        let store = SessionStore::new(&root);

        let original = sample_session("discord:channel:backup-corrupt", 10);
        let updated = sample_session("discord:channel:backup-corrupt", 11);
        store
            .upsert_session(&original)
            .expect("initial write should succeed");
        store
            .upsert_session(&updated)
            .expect("second write should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:backup-corrupt"));
        let backup_path = PathBuf::from(format!("{}.bak", session_path.display()));
        fs::write(&session_path, b"{ invalid json")
            .expect("test should be able to corrupt primary session file");
        fs::write(&backup_path, b"{ invalid json")
            .expect("test should be able to corrupt backup session file");

        let error = store
            .get_session("discord:channel:backup-corrupt")
            .expect_err("double corruption should return corruption error");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "session_read",
                path: session_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn atomic_write_leaves_no_temp_files() {
        let root = temp_root("no-temp-files");
        let store = SessionStore::new(&root);
        let session = sample_session("discord:channel:temp-check", 3);

        store
            .upsert_session(&session)
            .expect("session write should succeed");

        let root_entries: Vec<String> = fs::read_dir(&root)
            .expect("root directory should exist")
            .map(|entry| {
                entry
                    .expect("entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(root_entries.iter().all(|name| !name.contains(".tmp-")));

        let sessions_entries: Vec<String> = fs::read_dir(root.join("sessions"))
            .expect("sessions directory should exist")
            .map(|entry| {
                entry
                    .expect("entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(sessions_entries.iter().all(|name| !name.contains(".tmp-")));

        cleanup(&root);
    }

    #[test]
    fn io_error_from_session_read_is_returned() {
        let root = temp_root("session-read-io-error");
        let store = SessionStore::new(&root);
        store
            .list_session_ids()
            .expect("layout initialization should succeed");

        let session_path = root
            .join("sessions")
            .join(session_file_name("discord:channel:io"));
        fs::create_dir_all(&session_path).expect("test should create a directory at session path");

        let error = store
            .get_session("discord:channel:io")
            .expect_err("directory path should trigger io read failure");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_read",
                path: Some(ref path),
                ..
            } if path == &session_path.display().to_string()
        ));

        cleanup(&root);
    }

    #[test]
    fn io_error_from_index_read_is_returned() {
        let root = temp_root("index-read-io-error");
        let store = SessionStore::new(&root);
        store
            .list_session_ids()
            .expect("layout initialization should succeed");

        let index_path = root.join("sessions.index.json");
        fs::create_dir_all(&index_path).expect("test should create a directory at index path");

        let error = store
            .list_session_ids()
            .expect_err("directory index path should trigger io read failure");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "session_index_read",
                path: Some(ref path),
                ..
            } if path == &index_path.display().to_string()
        ));

        cleanup(&root);
    }

    fn sample_session(id: &str, last_activity: u64) -> LogicalSession {
        LogicalSession {
            id: id.to_string(),
            active_backend: BackendKind::Codex,
            active_profile: InferenceProfile {
                backend: BackendKind::Codex,
                model: "gpt-5-codex".to_string(),
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
            },
        }
    }

    fn temp_root(label: &str) -> PathBuf {
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

    fn cleanup(root: &PathBuf) {
        let _ = fs::remove_dir_all(root);
    }

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
                super::SessionIndexEntry {
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
            CrabError::Io {
                context: "session_write",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn upsert_propagates_layout_error() {
        let root = root_as_file("upsert-layout-error");
        let store = SessionStore::new(&root);
        let error = store
            .upsert_session(&sample_session("discord:channel:a", 1))
            .expect_err("root path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn get_session_propagates_layout_error() {
        let root = root_as_file("get-layout-error");
        let store = SessionStore::new(&root);
        let error = store
            .get_session("discord:channel:a")
            .expect_err("root path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn list_session_ids_propagates_layout_error() {
        let root = root_as_file("list-layout-error");
        let store = SessionStore::new(&root);
        let error = store
            .list_session_ids()
            .expect_err("root path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn list_session_ids_propagates_sessions_directory_layout_error() {
        let root = temp_root("sessions-layout-error");
        fs::write(root.join("sessions"), b"not-a-directory")
            .expect("test should be able to block sessions dir path");
        let store = SessionStore::new(&root);
        let error = store
            .list_session_ids()
            .expect_err("sessions path as file should fail layout creation");
        assert_io_context(error, "session_store_layout");
        cleanup(&root);
    }

    #[test]
    fn upsert_propagates_session_write_error() {
        let root = temp_root("upsert-write-error");
        let store = SessionStore::new(&root);
        store
            .list_session_ids()
            .expect("layout initialization should succeed");

        let session_id = "discord:channel:write-error";
        let session_path = root.join("sessions").join(session_file_name(session_id));
        fs::create_dir_all(&session_path)
            .expect("test should create directory at session file path");

        let error = store
            .upsert_session(&sample_session(session_id, 2))
            .expect_err("session write should fail when target is directory");
        assert_io_context(error, "session_write");

        cleanup(&root);
    }

    #[test]
    fn upsert_propagates_index_load_error() {
        let root = temp_root("upsert-index-load-error");
        let store = SessionStore::new(&root);
        fs::create_dir_all(root.join("sessions.index.json"))
            .expect("test should create directory at index path");

        let error = store
            .upsert_session(&sample_session("discord:channel:index-load", 3))
            .expect_err("index read should fail when index path is directory");
        assert_io_context(error, "session_index_read");

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn list_session_ids_propagates_rebuild_persist_error() {
        let root = temp_root("rebuild-persist-error");
        let store = SessionStore::new(&root);
        store
            .upsert_session(&sample_session("discord:channel:persist", 4))
            .expect("session write should succeed");

        let _ = corrupt_index_file(&root);

        set_unix_mode(&root, 0o500);
        let error = store
            .list_session_ids()
            .expect_err("rebuild persist should fail on read-only root");
        assert_io_context(error, "session_index_write");
        set_unix_mode(&root, 0o700);

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn list_session_ids_propagates_rebuild_read_dir_error() {
        let root = temp_root("rebuild-read-dir-error");
        let store = SessionStore::new(&root);
        store
            .upsert_session(&sample_session("discord:channel:read-dir", 5))
            .expect("session write should succeed");

        let _ = corrupt_index_file(&root);

        let sessions_dir = root.join("sessions");
        set_unix_mode(&sessions_dir, 0o000);
        let error = store
            .list_session_ids()
            .expect_err("rebuild should fail when sessions dir is unreadable");
        assert_io_context(error, "session_index_rebuild");
        set_unix_mode(&sessions_dir, 0o700);

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

        let backup_path = PathBuf::from(format!("{}.bak", path.display()));
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

    #[test]
    fn event_store_append_and_replay_round_trip() {
        let root = temp_root("event-round-trip");
        let store = EventStore::new(&root);
        let first = sample_event("discord:channel:events", "run-1", 1);
        let second = sample_event("discord:channel:events", "run-1", 2);

        store
            .append_event(&first)
            .expect("first event append should succeed");
        store
            .append_event(&second)
            .expect("second event append should succeed");

        let replayed = store
            .replay_run("discord:channel:events", "run-1")
            .expect("replay should succeed");
        assert_eq!(replayed, vec![first, second]);

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_missing_run_is_empty() {
        let root = temp_root("event-missing-run");
        let store = EventStore::new(&root);

        let replayed = store
            .replay_run("discord:channel:events", "missing-run")
            .expect("missing run should replay to empty");
        assert!(replayed.is_empty());

        cleanup(&root);
    }

    #[test]
    fn event_store_append_rejects_non_monotonic_sequence() {
        let root = temp_root("event-sequence-check");
        let store = EventStore::new(&root);
        let first = sample_event("discord:channel:events", "run-2", 1);
        let invalid = sample_event("discord:channel:events", "run-2", 3);

        store
            .append_event(&first)
            .expect("first event append should succeed");
        let error = store
            .append_event(&invalid)
            .expect_err("non-monotonic append should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "event_append_sequence",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_rejects_corrupt_json_line() {
        let root = temp_root("event-corrupt-json");
        let store = EventStore::new(&root);
        let first = sample_event("discord:channel:events", "run-3", 1);
        store
            .append_event(&first)
            .expect("first event append should succeed");

        let log_path = event_log_path(&root, "discord:channel:events", "run-3");
        fs::write(&log_path, "{\"bad\": true}\n")
            .expect("test should be able to corrupt run log file");

        let error = store
            .replay_run("discord:channel:events", "run-3")
            .expect_err("corrupt log should fail replay");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "event_replay_parse",
                path: log_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_rejects_non_monotonic_file_sequence() {
        let root = temp_root("event-sequence-corrupt");
        let store = EventStore::new(&root);

        let first = sample_event("discord:channel:events", "run-4", 1);
        let third = sample_event("discord:channel:events", "run-4", 3);
        let log_path = event_log_path(&root, "discord:channel:events", "run-4");
        fs::create_dir_all(
            log_path
                .parent()
                .expect("run log path should have a parent"),
        )
        .expect("test should create parent directories for log");
        let lines = format!(
            "{}\n{}\n",
            serde_json::to_string(&first).expect("serialize first event"),
            serde_json::to_string(&third).expect("serialize third event")
        );
        fs::write(&log_path, lines).expect("test should write malformed sequence log");

        let error = store
            .replay_run("discord:channel:events", "run-4")
            .expect_err("non-monotonic replay should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "event_replay_sequence",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn event_store_append_propagates_layout_error() {
        let root = root_as_file("event-append-layout-error");
        let store = EventStore::new(&root);
        let error = store
            .append_event(&sample_event("discord:channel:events", "run-5", 1))
            .expect_err("root path as file should fail event store layout");
        assert_io_context(error, "event_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn event_store_replay_propagates_layout_error() {
        let root = root_as_file("event-replay-layout-error");
        let store = EventStore::new(&root);
        let error = store
            .replay_run("discord:channel:events", "run-6")
            .expect_err("root path as file should fail event store layout");
        assert_io_context(error, "event_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[cfg(unix)]
    #[test]
    fn event_store_append_propagates_open_error() {
        let root = temp_root("event-open-error");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-7";
        let first = sample_event(logical_session_id, run_id, 1);
        store
            .append_event(&first)
            .expect("first event append should succeed");

        let run_log_path = event_log_path(&root, logical_session_id, run_id);
        set_unix_mode(&run_log_path, 0o400);

        let error = store
            .append_event(&sample_event(logical_session_id, run_id, 2))
            .expect_err("read-only log file should fail append open");
        assert_io_context(error, "event_append_open");
        set_unix_mode(&run_log_path, 0o600);

        cleanup(&root);
    }

    #[test]
    fn event_store_append_propagates_replay_error() {
        let root = temp_root("event-append-replay-error");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-8";
        let run_log_path = event_log_path(&root, logical_session_id, run_id);
        fs::create_dir_all(
            run_log_path
                .parent()
                .expect("run log path should have a parent"),
        )
        .expect("test should create run log parent");
        fs::write(&run_log_path, b"{ bad json\n").expect("test should be able to corrupt run log");

        let error = store
            .append_event(&sample_event(logical_session_id, run_id, 1))
            .expect_err("append should fail when replay cannot parse existing log");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "event_replay_parse",
                path: run_log_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn event_store_append_propagates_append_layout_error() {
        let root = temp_root("event-append-layout-subdir-error");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-9";
        let session_dir = root
            .join("events")
            .join(super::hex_encode(logical_session_id.as_bytes()));
        fs::create_dir_all(root.join("events")).expect("test should create events root");
        fs::write(&session_dir, b"not-a-directory")
            .expect("test should be able to block session events dir");

        let error = store
            .append_event(&sample_event(logical_session_id, run_id, 1))
            .expect_err("append should fail when session events dir path is a file");
        assert_io_context(error, "event_append_layout");

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_propagates_read_error() {
        let root = temp_root("event-replay-read-error");
        let store = EventStore::new(&root);
        let run_log_path = event_log_path(&root, "discord:channel:events", "run-10");
        fs::create_dir_all(&run_log_path).expect("test should create directory at run log path");

        let error = store
            .replay_run("discord:channel:events", "run-10")
            .expect_err("replay should fail when run log path is a directory");
        assert_io_context(error, "event_replay_read");

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_ignores_empty_lines() {
        let root = temp_root("event-replay-empty-lines");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-11";
        let first = sample_event(logical_session_id, run_id, 1);
        let log_path = event_log_path(&root, logical_session_id, run_id);
        fs::create_dir_all(
            log_path
                .parent()
                .expect("run log path should have a parent"),
        )
        .expect("test should create parent directories for log");
        let lines = format!(
            "{}\n\n{}\n",
            serde_json::to_string(&first).expect("serialize first event"),
            ""
        );
        fs::write(&log_path, lines).expect("test should write run log with empty line");

        let replayed = store
            .replay_run(logical_session_id, run_id)
            .expect("replay with empty lines should succeed");
        assert_eq!(replayed, vec![first]);

        cleanup(&root);
    }

    #[test]
    fn event_store_layout_events_root_error() {
        let root = temp_root("event-layout-events-root-error");
        fs::write(root.join("events"), b"not-a-directory")
            .expect("test should be able to block events root path");
        let store = EventStore::new(&root);

        let error = store
            .replay_run("discord:channel:events", "run-12")
            .expect_err("replay should fail when events root path is a file");
        assert_io_context(error, "event_store_layout");

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_put_get_and_latest_round_trip() {
        let root = temp_root("checkpoint-round-trip");
        let store = CheckpointStore::new(&root);
        let first = sample_checkpoint("discord:channel:checkpoints", "run-a", "ckpt-a", 100);
        let second = sample_checkpoint("discord:channel:checkpoints", "run-b", "ckpt-b", 200);

        store
            .put_checkpoint(&first)
            .expect("first checkpoint write should succeed");
        store
            .put_checkpoint(&second)
            .expect("second checkpoint write should succeed");

        let loaded = store
            .get_checkpoint("discord:channel:checkpoints", "ckpt-a")
            .expect("checkpoint read should succeed")
            .expect("checkpoint should exist");
        assert_eq!(loaded, first);

        let latest = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect("latest lookup should succeed")
            .expect("latest checkpoint should exist");
        assert_eq!(latest, second);

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_latest_uses_id_tiebreaker() {
        let root = temp_root("checkpoint-tiebreak");
        let store = CheckpointStore::new(&root);
        let first = sample_checkpoint("discord:channel:checkpoints", "run-1", "ckpt-a", 300);
        let second = sample_checkpoint("discord:channel:checkpoints", "run-2", "ckpt-z", 300);

        store
            .put_checkpoint(&first)
            .expect("first checkpoint write should succeed");
        store
            .put_checkpoint(&second)
            .expect("second checkpoint write should succeed");

        let latest = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect("latest lookup should succeed")
            .expect("latest checkpoint should exist");
        assert_eq!(latest.id, "ckpt-z");

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_latest_missing_session_is_none() {
        let root = temp_root("checkpoint-missing-latest");
        let store = CheckpointStore::new(&root);

        let latest = store
            .latest_checkpoint("discord:channel:missing")
            .expect("latest lookup for missing session should not fail");
        assert!(latest.is_none());

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_latest_ignores_non_json_files() {
        let root = temp_root("checkpoint-ignore-non-json");
        let store = CheckpointStore::new(&root);
        let checkpoint = sample_checkpoint("discord:channel:checkpoints", "run-1", "ckpt-a", 10);
        store
            .put_checkpoint(&checkpoint)
            .expect("checkpoint write should succeed");

        let session_dir = root
            .join("checkpoints")
            .join(super::hex_encode("discord:channel:checkpoints".as_bytes()));
        fs::write(session_dir.join("notes.txt"), b"ignore-me")
            .expect("test should be able to add non-json file");

        let latest = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect("latest lookup should ignore non-json files")
            .expect("latest checkpoint should still exist");
        assert_eq!(latest, checkpoint);

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn checkpoint_store_latest_ignores_broken_json_symlink_entries() {
        use std::os::unix::fs::symlink;

        let root = temp_root("checkpoint-ignore-broken-symlink");
        let store = CheckpointStore::new(&root);
        let checkpoint = sample_checkpoint("discord:channel:checkpoints", "run-2", "ckpt-b", 20);
        store
            .put_checkpoint(&checkpoint)
            .expect("checkpoint write should succeed");

        let session_dir = root
            .join("checkpoints")
            .join(super::hex_encode("discord:channel:checkpoints".as_bytes()));
        symlink(
            root.join("missing-target.json"),
            session_dir.join("dangling.json"),
        )
        .expect("test should be able to create broken json symlink");

        let latest = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect("latest lookup should ignore missing symlink targets")
            .expect("latest checkpoint should still exist");
        assert_eq!(latest, checkpoint);

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_get_missing_is_none() {
        let root = temp_root("checkpoint-missing-get");
        let store = CheckpointStore::new(&root);

        let loaded = store
            .get_checkpoint("discord:channel:missing", "ckpt-missing")
            .expect("missing checkpoint lookup should not fail");
        assert!(loaded.is_none());

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_rejects_invalid_schema() {
        let root = temp_root("checkpoint-invalid-schema");
        let store = CheckpointStore::new(&root);
        let mut invalid = sample_checkpoint("discord:channel:checkpoints", "run-1", "ckpt-a", 1);
        invalid.id = "   ".to_string();

        let error = store
            .put_checkpoint(&invalid)
            .expect_err("empty checkpoint id should fail validation");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_rejects_zero_created_at() {
        let root = temp_root("checkpoint-zero-created");
        let store = CheckpointStore::new(&root);
        let mut invalid = sample_checkpoint("discord:channel:checkpoints", "run-2", "ckpt-b", 9);
        invalid.created_at_epoch_ms = 0;

        let error = store
            .put_checkpoint(&invalid)
            .expect_err("zero created_at should fail validation");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_rejects_empty_state_keys() {
        let root = temp_root("checkpoint-empty-state-key");
        let store = CheckpointStore::new(&root);
        let mut invalid = sample_checkpoint("discord:channel:checkpoints", "run-3", "ckpt-c", 8);
        invalid.state = BTreeMap::from([("".to_string(), "value".to_string())]);

        let error = store
            .put_checkpoint(&invalid)
            .expect_err("empty state key should fail validation");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_get_recovers_from_backup() {
        let root = temp_root("checkpoint-recover-from-backup");
        let store = CheckpointStore::new(&root);
        let original = sample_checkpoint("discord:channel:checkpoints", "run-4", "ckpt-shared", 10);
        let updated = sample_checkpoint("discord:channel:checkpoints", "run-5", "ckpt-shared", 20);
        store
            .put_checkpoint(&original)
            .expect("initial checkpoint write should succeed");
        store
            .put_checkpoint(&updated)
            .expect("second checkpoint write should succeed");

        let path = checkpoint_path(&root, "discord:channel:checkpoints", "ckpt-shared");
        fs::write(&path, b"{ invalid json")
            .expect("test should be able to corrupt checkpoint file");

        let loaded = store
            .get_checkpoint("discord:channel:checkpoints", "ckpt-shared")
            .expect("backup checkpoint read should succeed")
            .expect("checkpoint should still be readable");
        assert_eq!(loaded, original);

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_get_detects_identity_mismatch() {
        let root = temp_root("checkpoint-get-identity-mismatch");
        let store = CheckpointStore::new(&root);
        let foreign = sample_checkpoint("discord:channel:foreign", "run-7", "ckpt-foreign", 30);
        let foreign_path = checkpoint_path(&root, "discord:channel:target", "ckpt-foreign");
        fs::create_dir_all(
            foreign_path
                .parent()
                .expect("checkpoint path should have parent"),
        )
        .expect("test should create checkpoint parent directory");
        let encoded =
            serde_json::to_vec_pretty(&foreign).expect("foreign checkpoint should serialize");
        fs::write(&foreign_path, encoded).expect("test should write foreign checkpoint");

        let error = store
            .get_checkpoint("discord:channel:target", "ckpt-foreign")
            .expect_err("session mismatch should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_get_session_mismatch",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_latest_detects_identity_mismatch() {
        let root = temp_root("checkpoint-latest-identity-mismatch");
        let store = CheckpointStore::new(&root);
        let good = sample_checkpoint("discord:channel:target", "run-8", "ckpt-good", 11);
        store
            .put_checkpoint(&good)
            .expect("valid checkpoint write should succeed");

        let foreign = sample_checkpoint("discord:channel:foreign", "run-9", "ckpt-foreign", 15);
        let foreign_path = checkpoint_path(&root, "discord:channel:target", "ckpt-foreign");
        let foreign_encoded =
            serde_json::to_vec_pretty(&foreign).expect("foreign checkpoint serialize");
        fs::write(&foreign_path, foreign_encoded).expect("test should write foreign checkpoint");

        let error = store
            .latest_checkpoint("discord:channel:target")
            .expect_err("latest lookup should reject mismatched checkpoint identity");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_latest_session_mismatch",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_layout_error_is_returned() {
        let root = root_as_file("checkpoint-layout-error");
        let store = CheckpointStore::new(&root);
        let error = store
            .put_checkpoint(&sample_checkpoint(
                "discord:channel:checkpoints",
                "run-10",
                "ckpt",
                1,
            ))
            .expect_err("root path as file should fail checkpoint layout");
        assert_io_context(error, "checkpoint_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn checkpoint_store_layout_root_dir_error_is_returned() {
        let root = temp_root("checkpoint-layout-root-dir-error");
        fs::write(root.join("checkpoints"), b"not-a-directory")
            .expect("test should be able to block checkpoints dir");
        let store = CheckpointStore::new(&root);

        let error = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect_err("blocked checkpoints dir should fail layout");
        assert_io_context(error, "checkpoint_store_layout");

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_get_propagates_read_error() {
        let root = temp_root("checkpoint-get-read-error");
        let store = CheckpointStore::new(&root);
        let checkpoint_path = checkpoint_path(&root, "discord:channel:checkpoints", "ckpt-read");
        fs::create_dir_all(&checkpoint_path)
            .expect("test should create directory at checkpoint file path");

        let error = store
            .get_checkpoint("discord:channel:checkpoints", "ckpt-read")
            .expect_err("directory path should fail checkpoint read");
        assert_io_context(error, "checkpoint_read");

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_get_layout_error_is_returned() {
        let root = root_as_file("checkpoint-get-layout-error");
        let store = CheckpointStore::new(&root);
        let error = store
            .get_checkpoint("discord:channel:checkpoints", "ckpt-layout")
            .expect_err("root path as file should fail checkpoint layout");
        assert_io_context(error, "checkpoint_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn checkpoint_store_latest_read_dir_error_is_returned() {
        let root = temp_root("checkpoint-latest-read-dir-error");
        let store = CheckpointStore::new(&root);
        let session_dir = root
            .join("checkpoints")
            .join(super::hex_encode("discord:channel:checkpoints".as_bytes()));
        fs::create_dir_all(root.join("checkpoints")).expect("test should create checkpoints root");
        fs::write(&session_dir, b"not-a-directory")
            .expect("test should block checkpoint session directory");

        let error = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect_err("session directory as file should fail read_dir");
        assert_io_context(error, "checkpoint_latest_read_dir");

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_rejects_all_required_text_fields() {
        let root = temp_root("checkpoint-required-fields");
        let store = CheckpointStore::new(&root);

        let mut missing_logical =
            sample_checkpoint("discord:channel:checkpoints", "run-11", "ckpt-x", 1);
        missing_logical.logical_session_id = " ".to_string();
        let logical_error = store
            .put_checkpoint(&missing_logical)
            .expect_err("logical_session_id is required");
        assert!(matches!(
            logical_error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        let mut missing_run =
            sample_checkpoint("discord:channel:checkpoints", "run-12", "ckpt-y", 2);
        missing_run.run_id = "".to_string();
        let run_error = store
            .put_checkpoint(&missing_run)
            .expect_err("run_id is required");
        assert!(matches!(
            run_error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        let mut missing_summary =
            sample_checkpoint("discord:channel:checkpoints", "run-13", "ckpt-z", 3);
        missing_summary.summary = "\n".to_string();
        let summary_error = store
            .put_checkpoint(&missing_summary)
            .expect_err("summary is required");
        assert!(matches!(
            summary_error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        let mut missing_digest =
            sample_checkpoint("discord:channel:checkpoints", "run-14", "ckpt-d", 4);
        missing_digest.memory_digest = "".to_string();
        let digest_error = store
            .put_checkpoint(&missing_digest)
            .expect_err("memory_digest is required");
        assert!(matches!(
            digest_error,
            CrabError::InvariantViolation {
                context: "checkpoint_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn checkpoint_store_latest_rejects_corrupt_json() {
        let root = temp_root("checkpoint-latest-corrupt");
        let store = CheckpointStore::new(&root);
        let path = checkpoint_path(&root, "discord:channel:checkpoints", "ckpt-corrupt");
        fs::create_dir_all(path.parent().expect("checkpoint path should have parent"))
            .expect("test should create checkpoint parent dir");
        fs::write(&path, b"{ bad json").expect("test should write corrupt checkpoint file");

        let error = store
            .latest_checkpoint("discord:channel:checkpoints")
            .expect_err("corrupt checkpoint should fail latest lookup");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "checkpoint_read",
                path: path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn outbound_store_record_and_list_round_trip() {
        let root = temp_root("outbound-round-trip");
        let store = OutboundRecordStore::new(&root);
        let first =
            sample_outbound_record("discord:channel:outbound", "run-a", "message-1", 0, "h1");
        let second =
            sample_outbound_record("discord:channel:outbound", "run-a", "message-1", 1, "h2");

        let persisted_first = store
            .record_or_skip_duplicate(&first)
            .expect("first outbound record should persist");
        let persisted_second = store
            .record_or_skip_duplicate(&second)
            .expect("second outbound record should persist");
        assert!(persisted_first);
        assert!(persisted_second);

        let records = store
            .list_run_records("discord:channel:outbound", "run-a")
            .expect("listing outbound records should succeed");
        assert_eq!(records, vec![first, second]);

        cleanup(&root);
    }

    #[test]
    fn outbound_store_suppresses_exact_duplicates() {
        let root = temp_root("outbound-duplicate");
        let store = OutboundRecordStore::new(&root);
        let record =
            sample_outbound_record("discord:channel:outbound", "run-b", "message-2", 3, "same");

        let first = store
            .record_or_skip_duplicate(&record)
            .expect("first write should persist");
        let second = store
            .record_or_skip_duplicate(&record)
            .expect("duplicate write should be skipped");
        assert!(first);
        assert!(!second);

        cleanup(&root);
    }

    #[test]
    fn outbound_store_rejects_conflicting_duplicate() {
        let root = temp_root("outbound-conflict");
        let store = OutboundRecordStore::new(&root);
        let baseline = sample_outbound_record(
            "discord:channel:outbound",
            "run-c",
            "message-3",
            2,
            "hash-a",
        );
        let conflict = sample_outbound_record(
            "discord:channel:outbound",
            "run-c",
            "message-3",
            2,
            "hash-b",
        );
        store
            .record_or_skip_duplicate(&baseline)
            .expect("initial outbound record should persist");

        let error = store
            .record_or_skip_duplicate(&conflict)
            .expect_err("same target with different hash should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "outbound_record_conflict",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn outbound_store_rejects_invalid_schema() {
        let root = temp_root("outbound-invalid-schema");
        let store = OutboundRecordStore::new(&root);
        let mut invalid = sample_outbound_record(
            "discord:channel:outbound",
            "run-d",
            "message-4",
            0,
            "hash-value",
        );
        invalid.channel_id = "  ".to_string();

        let error = store
            .record_or_skip_duplicate(&invalid)
            .expect_err("empty channel id should fail validation");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn outbound_store_rejects_zero_delivery_epoch() {
        let root = temp_root("outbound-zero-delivery");
        let store = OutboundRecordStore::new(&root);
        let mut invalid = sample_outbound_record(
            "discord:channel:outbound",
            "run-e",
            "message-5",
            0,
            "hash-value",
        );
        invalid.delivered_at_epoch_ms = 0;

        let error = store
            .record_or_skip_duplicate(&invalid)
            .expect_err("zero delivery timestamp should fail validation");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn outbound_store_missing_run_is_empty() {
        let root = temp_root("outbound-missing-run");
        let store = OutboundRecordStore::new(&root);

        let records = store
            .list_run_records("discord:channel:outbound", "run-missing")
            .expect("missing run should return empty list");
        assert!(records.is_empty());

        cleanup(&root);
    }

    #[test]
    fn outbound_store_list_rejects_corrupt_json() {
        let root = temp_root("outbound-corrupt-json");
        let store = OutboundRecordStore::new(&root);
        let run_path = outbound_run_path(&root, "discord:channel:outbound", "run-f");
        fs::create_dir_all(
            run_path
                .parent()
                .expect("outbound run path should have parent"),
        )
        .expect("test should create outbound run parent");
        fs::write(&run_path, b"{ bad json\n").expect("test should write corrupt outbound log");

        let error = store
            .list_run_records("discord:channel:outbound", "run-f")
            .expect_err("corrupt outbound log should fail parse");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "outbound_record_parse",
                path: run_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn outbound_store_list_ignores_empty_lines() {
        let root = temp_root("outbound-empty-lines");
        let store = OutboundRecordStore::new(&root);
        let record = sample_outbound_record(
            "discord:channel:outbound",
            "run-f2",
            "message-empty",
            0,
            "hash-empty",
        );
        let run_path = outbound_run_path(&root, "discord:channel:outbound", "run-f2");
        fs::create_dir_all(
            run_path
                .parent()
                .expect("outbound run path should have parent"),
        )
        .expect("test should create outbound run parent");
        let content = format!(
            "\n{}\n\n",
            serde_json::to_string(&record).expect("record should serialize")
        );
        fs::write(&run_path, content).expect("test should write outbound log with empty lines");

        let records = store
            .list_run_records("discord:channel:outbound", "run-f2")
            .expect("empty lines should be ignored");
        assert_eq!(records, vec![record]);

        cleanup(&root);
    }

    #[test]
    fn outbound_store_list_rejects_identity_mismatch() {
        let root = temp_root("outbound-identity-mismatch");
        let store = OutboundRecordStore::new(&root);
        let run_path = outbound_run_path(&root, "discord:channel:outbound", "run-g");
        fs::create_dir_all(
            run_path
                .parent()
                .expect("outbound run path should have parent"),
        )
        .expect("test should create outbound run parent");
        let foreign =
            sample_outbound_record("discord:channel:foreign", "run-g", "message-9", 0, "hash");
        let content = format!(
            "{}\n",
            serde_json::to_string(&foreign).expect("foreign record should serialize")
        );
        fs::write(&run_path, content).expect("test should write foreign outbound record");

        let error = store
            .list_run_records("discord:channel:outbound", "run-g")
            .expect_err("identity mismatch should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "outbound_record_identity_mismatch",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn outbound_store_layout_error_is_returned() {
        let root = root_as_file("outbound-layout-error");
        let store = OutboundRecordStore::new(&root);
        let error = store
            .record_or_skip_duplicate(&sample_outbound_record(
                "discord:channel:outbound",
                "run-h",
                "message-10",
                1,
                "hash",
            ))
            .expect_err("root path as file should fail outbound layout");
        assert_io_context(error, "outbound_record_store_layout");
        let _ = fs::remove_file(&root);
    }

    #[test]
    fn outbound_store_layout_root_dir_error_is_returned() {
        let root = temp_root("outbound-layout-root-dir-error");
        fs::write(root.join("outbound"), b"not-a-directory")
            .expect("test should be able to block outbound root");
        let store = OutboundRecordStore::new(&root);

        let error = store
            .list_run_records("discord:channel:outbound", "run-i")
            .expect_err("blocked outbound root should fail layout");
        assert_io_context(error, "outbound_record_store_layout");

        cleanup(&root);
    }

    #[test]
    fn outbound_store_append_layout_error_is_returned() {
        let root = temp_root("outbound-append-layout-error");
        let store = OutboundRecordStore::new(&root);
        let session_dir = root
            .join("outbound")
            .join(super::hex_encode("discord:channel:outbound".as_bytes()));
        fs::create_dir_all(root.join("outbound")).expect("test should create outbound root");
        fs::write(&session_dir, b"not-a-directory")
            .expect("test should be able to block session outbound path");

        let error = store
            .record_or_skip_duplicate(&sample_outbound_record(
                "discord:channel:outbound",
                "run-j",
                "message-11",
                0,
                "hash",
            ))
            .expect_err("blocked session outbound path should fail append layout");
        assert_io_context(error, "outbound_record_layout");

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn outbound_store_append_open_error_is_returned() {
        let root = temp_root("outbound-open-error");
        let store = OutboundRecordStore::new(&root);
        let initial = sample_outbound_record(
            "discord:channel:outbound",
            "run-k",
            "message-12",
            0,
            "hash-1",
        );
        store
            .record_or_skip_duplicate(&initial)
            .expect("initial outbound record should persist");

        let run_path = outbound_run_path(&root, "discord:channel:outbound", "run-k");
        set_unix_mode(&run_path, 0o400);
        let follow_up = sample_outbound_record(
            "discord:channel:outbound",
            "run-k",
            "message-12",
            1,
            "hash-2",
        );
        let error = store
            .record_or_skip_duplicate(&follow_up)
            .expect_err("read-only outbound log should fail append open");
        assert_io_context(error, "outbound_record_open");
        set_unix_mode(&run_path, 0o600);

        cleanup(&root);
    }

    #[test]
    fn outbound_store_read_error_is_returned() {
        let root = temp_root("outbound-read-error");
        let store = OutboundRecordStore::new(&root);
        let run_path = outbound_run_path(&root, "discord:channel:outbound", "run-l");
        fs::create_dir_all(&run_path).expect("test should create directory at outbound run path");

        let error = store
            .list_run_records("discord:channel:outbound", "run-l")
            .expect_err("directory run path should fail outbound read");
        assert_io_context(error, "outbound_record_read");

        cleanup(&root);
    }

    #[test]
    fn outbound_store_rejects_all_required_text_fields() {
        let root = temp_root("outbound-required-fields");
        let store = OutboundRecordStore::new(&root);

        let mut missing_record =
            sample_outbound_record("discord:channel:outbound", "run-m", "message-13", 0, "hash");
        missing_record.record_id = "".to_string();
        let record_error = store
            .record_or_skip_duplicate(&missing_record)
            .expect_err("record_id is required");
        assert!(matches!(
            record_error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        let mut missing_logical =
            sample_outbound_record("discord:channel:outbound", "run-m", "message-13", 0, "hash");
        missing_logical.logical_session_id = " ".to_string();
        let logical_error = store
            .record_or_skip_duplicate(&missing_logical)
            .expect_err("logical_session_id is required");
        assert!(matches!(
            logical_error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        let mut missing_run =
            sample_outbound_record("discord:channel:outbound", "run-m", "message-13", 0, "hash");
        missing_run.run_id = "".to_string();
        let run_error = store
            .record_or_skip_duplicate(&missing_run)
            .expect_err("run_id is required");
        assert!(matches!(
            run_error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        let mut missing_message =
            sample_outbound_record("discord:channel:outbound", "run-m", "message-13", 0, "hash");
        missing_message.message_id = "".to_string();
        let message_error = store
            .record_or_skip_duplicate(&missing_message)
            .expect_err("message_id is required");
        assert!(matches!(
            message_error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        let mut missing_hash =
            sample_outbound_record("discord:channel:outbound", "run-m", "message-13", 0, "hash");
        missing_hash.content_sha256 = " ".to_string();
        let hash_error = store
            .record_or_skip_duplicate(&missing_hash)
            .expect_err("content_sha256 is required");
        assert!(matches!(
            hash_error,
            CrabError::InvariantViolation {
                context: "outbound_record_validate",
                ..
            }
        ));

        cleanup(&root);
    }

    #[test]
    fn outbound_store_record_propagates_list_error() {
        let root = temp_root("outbound-propagates-list-error");
        let store = OutboundRecordStore::new(&root);
        let run_path = outbound_run_path(&root, "discord:channel:outbound", "run-n");
        fs::create_dir_all(
            run_path
                .parent()
                .expect("outbound run path should have parent"),
        )
        .expect("test should create outbound run parent");
        fs::write(&run_path, b"{ bad json").expect("test should write corrupt outbound run log");

        let error = store
            .record_or_skip_duplicate(&sample_outbound_record(
                "discord:channel:outbound",
                "run-n",
                "message-14",
                0,
                "hash-14",
            ))
            .expect_err("record insertion should fail when existing log is corrupt");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "outbound_record_parse",
                path: run_path.display().to_string(),
            }
        );

        cleanup(&root);
    }

    fn root_as_file(label: &str) -> PathBuf {
        let root = temp_root(label);
        cleanup(&root);
        fs::write(&root, b"file-root").expect("test should create file at root path");
        root
    }

    fn sample_event(logical_session_id: &str, run_id: &str, sequence: u64) -> EventEnvelope {
        EventEnvelope {
            event_id: format!("evt-{run_id}-{sequence}"),
            run_id: run_id.to_string(),
            logical_session_id: logical_session_id.to_string(),
            sequence,
            emitted_at_epoch_ms: 1_739_173_200_000 + sequence,
            source: EventSource::Backend,
            kind: EventKind::TextDelta,
            payload: BTreeMap::from([("text".to_string(), format!("delta-{sequence}"))]),
            idempotency_key: Some(format!("{run_id}:{sequence}")),
        }
    }

    fn event_log_path(root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        root.join("events")
            .join(super::hex_encode(logical_session_id.as_bytes()))
            .join(run_log_file_name(run_id))
    }

    fn sample_checkpoint(
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

    fn checkpoint_path(root: &Path, logical_session_id: &str, checkpoint_id: &str) -> PathBuf {
        root.join("checkpoints")
            .join(super::hex_encode(logical_session_id.as_bytes()))
            .join(checkpoint_file_name(checkpoint_id))
    }

    fn sample_outbound_record(
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

    fn outbound_run_path(root: &Path, logical_session_id: &str, run_id: &str) -> PathBuf {
        root.join("outbound")
            .join(super::hex_encode(logical_session_id.as_bytes()))
            .join(run_log_file_name(run_id))
    }

    fn corrupt_index_file(root: &Path) -> PathBuf {
        let index_path = root.join("sessions.index.json");
        fs::write(&index_path, b"{ invalid json")
            .expect("test should be able to corrupt index file");
        let backup_path = PathBuf::from(format!("{}.bak", index_path.display()));
        let _ = fs::remove_file(backup_path);
        index_path
    }

    fn assert_io_context(error: CrabError, expected_context: &'static str) {
        assert!(matches!(
            error,
            CrabError::Io {
                context,
                ..
            } if context == expected_context
        ));
    }

    #[cfg(unix)]
    fn set_unix_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path)
            .expect("target path should exist")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("permissions update should succeed");
    }
}
