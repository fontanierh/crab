use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use crab_core::{CrabError, CrabResult, EventEnvelope};

use crate::helpers::{
    hex_encode, run_log_file_name, validate_profile_sender_context, wrap_io, write_all_with_context,
};

#[derive(Debug, Clone)]
pub struct EventStore {
    root: PathBuf,
}

impl EventStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn append_event(&self, event: &EventEnvelope) -> CrabResult<u64> {
        self.ensure_layout()?;
        if let Some(profile) = event.profile.as_ref() {
            validate_profile_sender_context("event_validate", profile)?;
        }

        let replayed = self.replay_run(&event.logical_session_id, &event.run_id)?;
        if let (Some(last), Some(idempotency_key)) =
            (replayed.last(), event.idempotency_key.as_deref())
        {
            if last.idempotency_key.as_deref() == Some(idempotency_key) {
                return Ok(last.sequence);
            }
        }

        let expected_sequence = replayed.last().map_or(1, |last| last.sequence + 1);

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
        #[rustfmt::skip]
        write_all_with_context(&mut file, line.as_bytes(), "event_append_write", &run_log_path)?;

        Ok(event.sequence)
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
        let mut previous: Option<EventEnvelope> = None;
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
                if let Some(previous_event) = previous.as_ref() {
                    let is_duplicate = parsed.sequence == previous_event.sequence
                        && previous_event.idempotency_key.is_some()
                        && previous_event.idempotency_key == parsed.idempotency_key;
                    if is_duplicate {
                        #[cfg(not(coverage))]
                        tracing::warn!(
                            logical_session_id = %logical_session_id,
                            run_id = %run_id,
                            sequence = parsed.sequence,
                            idempotency_key = parsed.idempotency_key.as_deref().unwrap_or(""),
                            "deduplicated duplicate event log entry during replay"
                        );
                        continue;
                    }
                }
                return Err(CrabError::InvariantViolation {
                    context: "event_replay_sequence",
                    message: format!(
                        "expected sequence {expected_sequence}, got {}",
                        parsed.sequence
                    ),
                });
            }

            expected_sequence += 1;
            previous = Some(parsed.clone());
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Write;
    use std::path::Path;

    use crab_core::{CrabError, EventEnvelope};

    use crate::helpers::write_all_with_context;
    use crate::test_support::{
        assert_io_context, cleanup, event_log_path, root_as_file, sample_event, set_unix_mode,
        temp_root,
    };

    use super::EventStore;

    #[test]
    fn event_store_preserves_profile_telemetry_on_replay() {
        let root = temp_root("event-profile-telemetry");
        let store = EventStore::new(&root);
        let event = sample_event("discord:channel:events", "run-profile", 1);

        store
            .append_event(&event)
            .expect("event append should succeed");
        let replayed = store
            .replay_run("discord:channel:events", "run-profile")
            .expect("event replay should succeed");
        assert_eq!(replayed, vec![event]);

        cleanup(&root);
    }

    #[test]
    fn event_store_append_accepts_events_without_profile_context() {
        let root = temp_root("event-without-profile");
        let store = EventStore::new(&root);
        let mut event = sample_event("discord:channel:events", "run-profile-none", 1);
        event.profile = None;

        store
            .append_event(&event)
            .expect("event append should accept missing profile metadata");
        let replayed = store
            .replay_run("discord:channel:events", "run-profile-none")
            .expect("event replay should succeed");
        assert_eq!(replayed, vec![event]);

        cleanup(&root);
    }

    #[test]
    fn event_store_append_validates_profile_sender_context() {
        let root = temp_root("event-profile-validate");
        let store = EventStore::new(&root);
        let mut event = sample_event("discord:channel:events", "run-profile-invalid", 1);
        event
            .profile
            .as_mut()
            .expect("sample event should include profile")
            .sender_id = " ".to_string();

        let error = store
            .append_event(&event)
            .expect_err("blank sender id in profile should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "event_validate",
                message: "profile.sender_id must not be empty".to_string(),
            }
        );

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
    fn event_store_append_skips_duplicate_last_idempotency_key() {
        let root = temp_root("event-idempotent-append");
        let store = EventStore::new(&root);
        let first = sample_event("discord:channel:events", "run-idempotent", 1);
        let mut duplicate = first.clone();
        duplicate.event_id = "evt-run-idempotent-duplicate".to_string();

        let first_sequence = store
            .append_event(&first)
            .expect("first event append should succeed");
        let duplicate_sequence = store
            .append_event(&duplicate)
            .expect("duplicate append should be treated as idempotent");
        let replayed = store
            .replay_run("discord:channel:events", "run-idempotent")
            .expect("replay should succeed");

        assert_eq!(first_sequence, 1);
        assert_eq!(duplicate_sequence, 1);
        assert_eq!(replayed, vec![first]);

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_accepts_legacy_events_without_extended_metadata() {
        let root = temp_root("event-legacy-schema");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-legacy";
        let log_path = event_log_path(&root, logical_session_id, run_id);
        fs::create_dir_all(
            log_path
                .parent()
                .expect("legacy event log path should have parent"),
        )
        .expect("event parent directory should be creatable");

        let legacy_line = serde_json::json!({
            "event_id": "evt-legacy-1",
            "run_id": run_id,
            "logical_session_id": logical_session_id,
            "sequence": 1,
            "emitted_at_epoch_ms": 1_739_173_200_001_u64,
            "source": "backend",
            "kind": "text_delta",
            "payload": {
                "text": "legacy payload"
            },
            "profile": null,
            "idempotency_key": "legacy:1"
        })
        .to_string();
        fs::write(&log_path, format!("{legacy_line}\n"))
            .expect("legacy event log should be writable");

        let replayed = store
            .replay_run(logical_session_id, run_id)
            .expect("legacy event replay should succeed");
        assert_eq!(replayed.len(), 1);
        let legacy = &replayed[0];
        assert_eq!(legacy.event_id, "evt-legacy-1");
        assert_eq!(legacy.turn_id, None);
        assert_eq!(legacy.lane_id, None);
        assert_eq!(legacy.physical_session_id, None);
        assert_eq!(legacy.backend, None);
        assert_eq!(legacy.resolved_model, None);
        assert_eq!(legacy.resolved_reasoning_level, None);
        assert_eq!(legacy.profile_source, None);
        assert_eq!(
            legacy.payload.get("text"),
            Some(&"legacy payload".to_string())
        );

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
        write_serialized_event_log(&log_path, &[first.clone(), third]);

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
    fn event_store_replay_rejects_first_event_with_non_initial_sequence() {
        let root = temp_root("event-sequence-first-gap");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-4-first-gap";
        let second = sample_event(logical_session_id, run_id, 2);
        let log_path = event_log_path(&root, logical_session_id, run_id);
        write_serialized_event_log(&log_path, &[second]);

        let error = store.replay_run(logical_session_id, run_id).expect_err(
            "replay should reject a run whose first event does not start at sequence 1",
        );
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
    fn event_store_replay_deduplicates_exact_duplicate_sequence_entries() {
        let root = temp_root("event-sequence-duplicate-idempotent");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-duplicate";
        let first = sample_event(logical_session_id, run_id, 1);
        let duplicate = first.clone();
        let second = sample_event(logical_session_id, run_id, 2);
        let log_path = event_log_path(&root, logical_session_id, run_id);
        write_serialized_event_log(&log_path, &[first.clone(), duplicate, second.clone()]);

        let replayed = store
            .replay_run(logical_session_id, run_id)
            .expect("replay should deduplicate exact duplicates");
        assert_eq!(replayed, vec![first, second]);

        cleanup(&root);
    }

    #[test]
    fn event_store_replay_rejects_duplicate_sequence_with_different_idempotency_key() {
        let root = temp_root("event-sequence-duplicate-conflict");
        let store = EventStore::new(&root);
        let logical_session_id = "discord:channel:events";
        let run_id = "run-duplicate-conflict";
        let first = sample_event(logical_session_id, run_id, 1);
        let mut conflicting = first.clone();
        conflicting.event_id = "evt-conflicting".to_string();
        conflicting.idempotency_key = Some("run-duplicate-conflict:1:conflict".to_string());
        let log_path = event_log_path(&root, logical_session_id, run_id);
        write_serialized_event_log(&log_path, &[first, conflicting]);

        let error = store
            .replay_run(logical_session_id, run_id)
            .expect_err("conflicting duplicate sequence should still fail");
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

    #[cfg(unix)]
    #[test]
    fn event_store_append_propagates_write_error() {
        struct FailingWriter;

        impl Write for FailingWriter {
            fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "forced write failure",
                ))
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let mut writer = FailingWriter;
        writer
            .flush()
            .expect("flush should succeed before write failure path");
        let path = Path::new("/tmp/event-write-error.jsonl");
        let error = write_all_with_context(
            &mut writer,
            b"{\"event_id\":\"evt-1\"}\n",
            "event_append_write",
            path,
        )
        .expect_err("failing writer should surface event append write error");
        assert_eq!(
            error,
            CrabError::Io {
                context: "event_append_write",
                path: Some(path.display().to_string()),
                message: "forced write failure".to_string(),
            }
        );
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
            .join(crate::helpers::hex_encode(logical_session_id.as_bytes()));
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

    fn write_serialized_event_log(log_path: &Path, events: &[EventEnvelope]) {
        fs::create_dir_all(
            log_path
                .parent()
                .expect("run log path should have a parent"),
        )
        .expect("test should create parent directories for log");

        let lines = events
            .iter()
            .map(|event| serde_json::to_string(event).expect("event should serialize"))
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(log_path, format!("{lines}\n")).expect("test should write event log");
    }
}
