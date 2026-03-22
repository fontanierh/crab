use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::file_signal;
use crate::{CrabError, CrabResult};

pub const PENDING_TRIGGERS_DIR_NAME: &str = "pending_triggers";
pub const STEERING_TRIGGERS_DIR_NAME: &str = "steering_triggers";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingTrigger {
    pub channel_id: String,
    pub message: String,
}

pub fn validate_pending_trigger(trigger: &PendingTrigger) -> CrabResult<()> {
    if trigger.channel_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "pending_trigger_validation",
            message: "channel_id must not be empty".to_string(),
        });
    }
    if trigger.message.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "pending_trigger_validation",
            message: "message must not be empty".to_string(),
        });
    }
    Ok(())
}

pub fn write_pending_trigger(state_root: &Path, trigger: &PendingTrigger) -> CrabResult<PathBuf> {
    validate_pending_trigger(trigger)?;
    file_signal::write_signal_file(
        state_root,
        PENDING_TRIGGERS_DIR_NAME,
        trigger,
        "pending_trigger_write",
    )
}

pub fn read_pending_triggers(state_root: &Path) -> CrabResult<Vec<(PathBuf, PendingTrigger)>> {
    file_signal::read_signal_files(
        state_root,
        PENDING_TRIGGERS_DIR_NAME,
        "pending_trigger_read",
    )
}

pub fn consume_pending_trigger(path: &Path) -> CrabResult<()> {
    file_signal::consume_signal_file(path, "pending_trigger_consume")
}

pub fn write_steering_trigger(state_root: &Path, trigger: &PendingTrigger) -> CrabResult<PathBuf> {
    validate_pending_trigger(trigger)?;
    file_signal::write_signal_file(
        state_root,
        STEERING_TRIGGERS_DIR_NAME,
        trigger,
        "steering_trigger_write",
    )
}

pub fn read_steering_triggers(state_root: &Path) -> CrabResult<Vec<(PathBuf, PendingTrigger)>> {
    file_signal::read_signal_files(
        state_root,
        STEERING_TRIGGERS_DIR_NAME,
        "steering_trigger_read",
    )
}

pub fn consume_steering_trigger(path: &Path) -> CrabResult<()> {
    file_signal::consume_signal_file(path, "steering_trigger_consume")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        consume_pending_trigger, consume_steering_trigger, read_pending_triggers,
        read_steering_triggers, validate_pending_trigger, write_pending_trigger,
        write_steering_trigger, PendingTrigger, PENDING_TRIGGERS_DIR_NAME,
        STEERING_TRIGGERS_DIR_NAME,
    };
    use crate::test_support::TempDir;
    use crate::CrabError;

    fn sample_trigger() -> PendingTrigger {
        PendingTrigger {
            channel_id: "123456789".to_string(),
            message: "Check deployment status".to_string(),
        }
    }

    #[test]
    fn validate_rejects_blank_channel_id() {
        let trigger = PendingTrigger {
            channel_id: "  ".to_string(),
            message: "hello".to_string(),
        };
        let error = validate_pending_trigger(&trigger).expect_err("should reject blank channel");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "pending_trigger_validation",
                ..
            }
        ));
        assert!(error.to_string().contains("channel_id must not be empty"));
    }

    #[test]
    fn validate_rejects_blank_message() {
        let trigger = PendingTrigger {
            channel_id: "123".to_string(),
            message: "  ".to_string(),
        };
        let error = validate_pending_trigger(&trigger).expect_err("should reject blank message");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "pending_trigger_validation",
                ..
            }
        ));
        assert!(error.to_string().contains("message must not be empty"));
    }

    #[test]
    fn validate_accepts_valid_trigger() {
        validate_pending_trigger(&sample_trigger()).expect("valid trigger should pass");
    }

    #[test]
    fn write_and_read_round_trip() {
        let temp = TempDir::new("self-trigger", "write-read");
        let trigger = sample_trigger();

        let path = write_pending_trigger(&temp.root, &trigger).expect("write should succeed");
        assert!(path.exists());
        assert!(path.to_string_lossy().contains(PENDING_TRIGGERS_DIR_NAME));

        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].0, path);
        assert_eq!(triggers[0].1, trigger);
    }

    #[test]
    fn consume_deletes_file() {
        let temp = TempDir::new("self-trigger", "consume");
        let trigger = sample_trigger();

        let path = write_pending_trigger(&temp.root, &trigger).expect("write should succeed");
        assert!(path.exists());

        consume_pending_trigger(&path).expect("consume should succeed");
        assert!(!path.exists());
    }

    #[test]
    fn read_returns_empty_when_directory_missing() {
        let temp = TempDir::new("self-trigger", "read-missing-dir");
        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        assert!(triggers.is_empty());
    }

    /// Create a temp dir with one valid trigger file already written and the
    /// signals subdirectory available for further sabotage.
    fn temp_with_one_trigger(label: &str) -> (TempDir, std::path::PathBuf, PendingTrigger) {
        let temp = TempDir::new("self-trigger", label);
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        let trigger = sample_trigger();
        write_pending_trigger(&temp.root, &trigger).expect("write should succeed");
        (temp, dir, trigger)
    }

    #[test]
    fn read_skips_malformed_and_non_json_files() {
        let (temp, dir, trigger) = temp_with_one_trigger("read-malformed");

        fs::write(dir.join("bad-1.json"), "not-valid-json").expect("malformed write should work");
        fs::write(dir.join("readme.txt"), "not a trigger").expect("txt write should work");

        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].1, trigger);
    }

    #[test]
    fn write_creates_directory_if_missing() {
        let temp = TempDir::new("self-trigger", "write-creates-dir");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        assert!(!dir.exists());

        let trigger = sample_trigger();
        write_pending_trigger(&temp.root, &trigger).expect("write should succeed");
        assert!(dir.is_dir());
    }

    #[test]
    fn write_rejects_invalid_trigger() {
        let temp = TempDir::new("self-trigger", "write-invalid");
        let trigger = PendingTrigger {
            channel_id: "".to_string(),
            message: "hello".to_string(),
        };
        let error = write_pending_trigger(&temp.root, &trigger).expect_err("should reject invalid");
        assert!(error.to_string().contains("channel_id must not be empty"));
    }

    #[test]
    fn consume_returns_error_for_missing_file() {
        let temp = TempDir::new("self-trigger", "consume-missing");
        let path = temp.root.join("nonexistent.json");
        let error =
            consume_pending_trigger(&path).expect_err("consume of missing file should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "pending_trigger_consume",
                ..
            }
        ));
    }

    #[test]
    fn write_propagates_create_dir_error() {
        let temp = TempDir::new("self-trigger", "write-create-dir-error");
        let fake_root = temp.root.join("file-not-dir");
        fs::write(&fake_root, "blocker").expect("write should succeed");
        let trigger = sample_trigger();
        let error = write_pending_trigger(&fake_root, &trigger)
            .expect_err("should fail when dir creation fails");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "pending_trigger_write",
                ..
            }
        ));
    }

    #[test]
    fn write_propagates_file_write_error() {
        let temp = TempDir::new("self-trigger", "write-file-error");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        let trigger = sample_trigger();
        let path = write_pending_trigger(&temp.root, &trigger).expect("first write should succeed");
        crate::test_support::sabotage_signal_dir_for_write(&path, &dir);
        let result = write_pending_trigger(&temp.root, &trigger);
        #[cfg(unix)]
        crate::test_support::restore_access(&dir, 0o755);
        #[cfg(unix)]
        {
            let error = result.expect_err("should fail when file write fails");
            assert!(matches!(
                error,
                CrabError::Io {
                    context: "pending_trigger_write",
                    ..
                }
            ));
        }
    }

    #[test]
    fn read_propagates_read_dir_error() {
        let temp = TempDir::new("self-trigger", "read-dir-error");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        #[cfg(unix)]
        crate::test_support::deny_access(&dir);
        let result = read_pending_triggers(&temp.root);
        #[cfg(unix)]
        crate::test_support::restore_access(&dir, 0o755);
        #[cfg(unix)]
        {
            let error = result.expect_err("should fail when read_dir fails");
            assert!(matches!(
                error,
                CrabError::Io {
                    context: "pending_trigger_read",
                    ..
                }
            ));
        }
    }

    #[test]
    fn read_skips_unreadable_files() {
        let (temp, dir, trigger) = temp_with_one_trigger("read-unreadable");

        let unreadable_path = dir.join("unreadable.json");
        fs::write(&unreadable_path, "content").expect("write should succeed");
        #[cfg(unix)]
        crate::test_support::deny_access(&unreadable_path);

        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].1, trigger);

        #[cfg(unix)]
        crate::test_support::restore_access(&unreadable_path, 0o644);
    }

    #[test]
    fn serde_round_trip() {
        let trigger = sample_trigger();
        let json = serde_json::to_string(&trigger).expect("serialize should succeed");
        let deserialized: PendingTrigger =
            serde_json::from_str(&json).expect("deserialize should succeed");
        assert_eq!(trigger, deserialized);
    }

    #[test]
    fn serde_rejects_unknown_fields() {
        let json = r#"{"channel_id":"123","message":"hello","extra":"oops"}"#;
        let result = serde_json::from_str::<PendingTrigger>(json);
        assert!(result.is_err());
    }

    // ── Steering trigger tests ───────────────────────────────────────────

    #[test]
    fn steering_write_and_read_round_trip() {
        let temp = TempDir::new("self-trigger", "steering-write-read");
        let trigger = sample_trigger();

        let path =
            write_steering_trigger(&temp.root, &trigger).expect("steering write should succeed");
        assert!(path.exists());
        assert!(path.to_string_lossy().contains(STEERING_TRIGGERS_DIR_NAME));

        let triggers = read_steering_triggers(&temp.root).expect("steering read should succeed");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].0, path);
        assert_eq!(triggers[0].1, trigger);
    }

    #[test]
    fn steering_consume_deletes_file() {
        let temp = TempDir::new("self-trigger", "steering-consume");
        let trigger = sample_trigger();

        let path =
            write_steering_trigger(&temp.root, &trigger).expect("steering write should succeed");
        assert!(path.exists());

        consume_steering_trigger(&path).expect("steering consume should succeed");
        assert!(!path.exists());
    }

    #[test]
    fn steering_read_returns_empty_when_directory_missing() {
        let temp = TempDir::new("self-trigger", "steering-read-missing-dir");
        let triggers = read_steering_triggers(&temp.root).expect("steering read should succeed");
        assert!(triggers.is_empty());
    }

    #[test]
    fn steering_write_rejects_invalid_trigger() {
        let temp = TempDir::new("self-trigger", "steering-write-invalid");
        let trigger = PendingTrigger {
            channel_id: "".to_string(),
            message: "hello".to_string(),
        };
        let error = write_steering_trigger(&temp.root, &trigger)
            .expect_err("should reject invalid trigger");
        assert!(error.to_string().contains("channel_id must not be empty"));
    }

    #[test]
    fn steering_consume_returns_error_for_missing_file() {
        let temp = TempDir::new("self-trigger", "steering-consume-missing");
        let path = temp.root.join("nonexistent.json");
        let error =
            consume_steering_trigger(&path).expect_err("consume of missing file should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "steering_trigger_consume",
                ..
            }
        ));
    }
}
