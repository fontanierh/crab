use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};

use crab_core::{CrabError, CrabResult, OutboundRecord};

use crate::helpers::{
    hex_encode, run_log_file_name, validate_outbound_record, wrap_io, write_all_with_context,
};

const OUTBOUND_DIR_NAME: &str = "outbound";

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
        write_all_with_context(
            &mut file,
            line.as_bytes(),
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

#[cfg(test)]
mod tests {
    use std::fs;

    use crab_core::CrabError;

    use crate::test_support::{
        assert_io_context, cleanup, outbound_run_path, root_as_file, sample_outbound_record,
        set_unix_mode, temp_root,
    };

    use super::OutboundRecordStore;

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
        let session_dir = root.join("outbound").join(crate::helpers::hex_encode(
            "discord:channel:outbound".as_bytes(),
        ));
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
}
