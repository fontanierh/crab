use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::checkpoint_turn::CheckpointTurnDocument;
use crate::file_signal;
use crate::CrabResult;

pub const PENDING_ROTATIONS_DIR_NAME: &str = "pending_rotations";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingRotation {
    pub checkpoint: CheckpointTurnDocument,
}

pub fn validate_pending_rotation(rotation: &PendingRotation) -> CrabResult<()> {
    crate::parse_checkpoint_turn_document(
        &serde_json::to_string(&rotation.checkpoint)
            .expect("CheckpointTurnDocument serialization is infallible"),
    )?;
    Ok(())
}

pub fn write_pending_rotation(
    state_root: &Path,
    rotation: &PendingRotation,
) -> CrabResult<PathBuf> {
    validate_pending_rotation(rotation)?;
    file_signal::write_signal_file(
        state_root,
        PENDING_ROTATIONS_DIR_NAME,
        rotation,
        "pending_rotation_write",
    )
}

pub fn read_pending_rotations(state_root: &Path) -> CrabResult<Vec<(PathBuf, PendingRotation)>> {
    file_signal::read_signal_files(
        state_root,
        PENDING_ROTATIONS_DIR_NAME,
        "pending_rotation_read",
    )
}

pub fn consume_pending_rotation(path: &Path) -> CrabResult<()> {
    file_signal::consume_signal_file(path, "pending_rotation_consume")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        consume_pending_rotation, read_pending_rotations, validate_pending_rotation,
        write_pending_rotation, PendingRotation, PENDING_ROTATIONS_DIR_NAME,
    };
    use crate::test_support::TempDir;
    use crate::{CheckpointTurnArtifact, CheckpointTurnDocument, CrabError};

    fn sample_checkpoint() -> CheckpointTurnDocument {
        CheckpointTurnDocument {
            summary: "Session summary".to_string(),
            decisions: vec!["Use claude backend".to_string()],
            open_questions: vec!["Need model override?".to_string()],
            next_actions: vec!["Implement rotate CLI".to_string()],
            artifacts: vec![CheckpointTurnArtifact {
                path: "src/main.rs".to_string(),
                note: "entrypoint updated".to_string(),
            }],
        }
    }

    fn sample_rotation() -> PendingRotation {
        PendingRotation {
            checkpoint: sample_checkpoint(),
        }
    }

    #[test]
    fn validate_accepts_valid_rotation() {
        validate_pending_rotation(&sample_rotation()).expect("valid rotation should pass");
    }

    #[test]
    fn validate_rejects_blank_summary() {
        let mut rotation = sample_rotation();
        rotation.checkpoint.summary = " ".to_string();
        let error =
            validate_pending_rotation(&rotation).expect_err("blank summary should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "checkpoint_turn_schema",
                ..
            }
        ));
        assert!(error.to_string().contains("summary must not be empty"));
    }

    #[test]
    fn write_and_read_round_trip() {
        let temp = TempDir::new("pending-rotation", "write-read");
        let rotation = sample_rotation();

        let path = write_pending_rotation(&temp.root, &rotation).expect("write should succeed");
        assert!(path.exists());
        assert!(path.to_string_lossy().contains(PENDING_ROTATIONS_DIR_NAME));

        let rotations = read_pending_rotations(&temp.root).expect("read should succeed");
        assert_eq!(rotations.len(), 1);
        assert_eq!(rotations[0].0, path);
        assert_eq!(rotations[0].1, rotation);
    }

    #[test]
    fn consume_deletes_file() {
        let temp = TempDir::new("pending-rotation", "consume");
        let rotation = sample_rotation();

        let path = write_pending_rotation(&temp.root, &rotation).expect("write should succeed");
        assert!(path.exists());

        consume_pending_rotation(&path).expect("consume should succeed");
        assert!(!path.exists());
    }

    #[test]
    fn read_returns_empty_when_directory_missing() {
        let temp = TempDir::new("pending-rotation", "read-missing-dir");
        let rotations = read_pending_rotations(&temp.root).expect("read should succeed");
        assert!(rotations.is_empty());
    }

    /// Create a temp dir with one valid rotation file already written and the
    /// signals subdirectory available for further sabotage.
    fn temp_with_one_rotation(label: &str) -> (TempDir, std::path::PathBuf, PendingRotation) {
        let temp = TempDir::new("pending-rotation", label);
        let dir = temp.root.join(PENDING_ROTATIONS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        let rotation = sample_rotation();
        write_pending_rotation(&temp.root, &rotation).expect("write should succeed");
        (temp, dir, rotation)
    }

    #[test]
    fn read_skips_malformed_and_non_json_files() {
        let (temp, dir, rotation) = temp_with_one_rotation("read-malformed");

        fs::write(dir.join("bad-1.json"), "not-valid-json").expect("malformed write should work");
        fs::write(dir.join("readme.txt"), "not a rotation").expect("txt write should work");

        let rotations = read_pending_rotations(&temp.root).expect("read should succeed");
        assert_eq!(rotations.len(), 1);
        assert_eq!(rotations[0].1, rotation);
    }

    #[test]
    fn write_creates_directory_if_missing() {
        let temp = TempDir::new("pending-rotation", "write-creates-dir");
        let dir = temp.root.join(PENDING_ROTATIONS_DIR_NAME);
        assert!(!dir.exists());

        let rotation = sample_rotation();
        write_pending_rotation(&temp.root, &rotation).expect("write should succeed");
        assert!(dir.is_dir());
    }

    #[test]
    fn write_rejects_invalid_rotation() {
        let temp = TempDir::new("pending-rotation", "write-invalid");
        let mut rotation = sample_rotation();
        rotation.checkpoint.summary = " ".to_string();
        let error = write_pending_rotation(&temp.root, &rotation)
            .expect_err("should reject invalid rotation");
        assert!(error.to_string().contains("summary must not be empty"));
    }

    #[test]
    fn consume_missing_rotation_is_treated_as_success() {
        let temp = TempDir::new("pending-rotation", "consume-missing");
        let path = temp.root.join("nonexistent.json");
        consume_pending_rotation(&path).expect("consume of missing file should succeed");
    }

    #[test]
    fn consume_propagates_remove_file_error() {
        let temp = TempDir::new("pending-rotation", "consume-remove-error");
        let path = temp.root.join("directory-instead-of-file.json");
        fs::create_dir_all(&path).expect("test should be able to create directory at signal path");

        let error =
            consume_pending_rotation(&path).expect_err("consume should fail for non-file paths");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "pending_rotation_consume",
                path: Some(ref remove_path),
                ..
            } if remove_path == &path.display().to_string()
        ));
    }

    #[test]
    fn write_propagates_create_dir_error() {
        let temp = TempDir::new("pending-rotation", "write-create-dir-error");
        let fake_root = temp.root.join("file-not-dir");
        fs::write(&fake_root, "blocker").expect("write should succeed");
        let rotation = sample_rotation();
        let error = write_pending_rotation(&fake_root, &rotation)
            .expect_err("should fail when dir creation fails");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "pending_rotation_write",
                ..
            }
        ));
    }

    #[test]
    fn write_propagates_file_write_error() {
        let temp = TempDir::new("pending-rotation", "write-file-error");
        let dir = temp.root.join(PENDING_ROTATIONS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        let rotation = sample_rotation();
        let path =
            write_pending_rotation(&temp.root, &rotation).expect("first write should succeed");
        crate::test_support::sabotage_signal_dir_for_write(&path, &dir);
        let result = write_pending_rotation(&temp.root, &rotation);
        #[cfg(unix)]
        crate::test_support::restore_access(&dir, 0o755);
        #[cfg(unix)]
        {
            let error = result.expect_err("should fail when file write fails");
            assert!(matches!(
                error,
                CrabError::Io {
                    context: "pending_rotation_write",
                    ..
                }
            ));
        }
    }

    #[test]
    fn read_propagates_read_dir_error() {
        let temp = TempDir::new("pending-rotation", "read-dir-error");
        let dir = temp.root.join(PENDING_ROTATIONS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        #[cfg(unix)]
        crate::test_support::deny_access(&dir);
        let result = read_pending_rotations(&temp.root);
        #[cfg(unix)]
        crate::test_support::restore_access(&dir, 0o755);
        #[cfg(unix)]
        {
            let error = result.expect_err("should fail when read_dir fails");
            assert!(matches!(
                error,
                CrabError::Io {
                    context: "pending_rotation_read",
                    ..
                }
            ));
        }
    }

    #[test]
    fn read_skips_unreadable_files() {
        let (temp, dir, rotation) = temp_with_one_rotation("read-unreadable");

        let unreadable_path = dir.join("unreadable.json");
        fs::write(&unreadable_path, "content").expect("write should succeed");
        #[cfg(unix)]
        crate::test_support::deny_access(&unreadable_path);

        let rotations = read_pending_rotations(&temp.root).expect("read should succeed");
        assert_eq!(rotations.len(), 1);
        assert_eq!(rotations[0].1, rotation);

        #[cfg(unix)]
        crate::test_support::restore_access(&unreadable_path, 0o644);
    }

    #[test]
    fn serde_round_trip() {
        let rotation = sample_rotation();
        let json = serde_json::to_string(&rotation).expect("serialize should succeed");
        let deserialized: PendingRotation =
            serde_json::from_str(&json).expect("deserialize should succeed");
        assert_eq!(rotation, deserialized);
    }

    #[test]
    fn serde_rejects_unknown_fields() {
        let json = r#"{"checkpoint":{"summary":"s","decisions":[],"open_questions":[],"next_actions":[],"artifacts":[]},"extra":"oops"}"#;
        let result = serde_json::from_str::<PendingRotation>(json);
        assert!(result.is_err());
    }
}
