use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;

use crab_core::{Checkpoint, CrabError, CrabResult};

use crate::helpers::{
    checkpoint_file_name, hex_encode, read_json_with_backup, validate_checkpoint, wrap_io,
    write_checkpoint_atomically,
};

const CHECKPOINTS_DIR_NAME: &str = "checkpoints";

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
                Some(current) => checkpoint_is_newer_than(&checkpoint, current),
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

fn checkpoint_is_newer_than(candidate: &Checkpoint, current: &Checkpoint) -> bool {
    candidate.created_at_epoch_ms > current.created_at_epoch_ms
        || (candidate.created_at_epoch_ms == current.created_at_epoch_ms
            && candidate.id > current.id)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use crab_core::CrabError;

    use crate::test_support::{
        assert_io_context, checkpoint_path, cleanup, root_as_file, sample_checkpoint, temp_root,
    };

    use super::{checkpoint_is_newer_than, CheckpointStore};

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
    fn checkpoint_is_newer_than_respects_timestamp_and_id_tiebreakers() {
        let current = sample_checkpoint("discord:channel:checkpoints", "run-1", "ckpt-a", 200);

        let newer_timestamp =
            sample_checkpoint("discord:channel:checkpoints", "run-2", "ckpt-a", 201);
        assert!(checkpoint_is_newer_than(&newer_timestamp, &current));

        let equal_timestamp_lower_id =
            sample_checkpoint("discord:channel:checkpoints", "run-3", "ckpt-0", 200);
        assert!(!checkpoint_is_newer_than(
            &equal_timestamp_lower_id,
            &current
        ));

        let older_timestamp_higher_id =
            sample_checkpoint("discord:channel:checkpoints", "run-4", "ckpt-z", 199);
        assert!(!checkpoint_is_newer_than(
            &older_timestamp_higher_id,
            &current
        ));
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

        let session_dir = root.join("checkpoints").join(crate::helpers::hex_encode(
            "discord:channel:checkpoints".as_bytes(),
        ));
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

        let session_dir = root.join("checkpoints").join(crate::helpers::hex_encode(
            "discord:channel:checkpoints".as_bytes(),
        ));
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
        let session_dir = root.join("checkpoints").join(crate::helpers::hex_encode(
            "discord:channel:checkpoints".as_bytes(),
        ));
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
}
