use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{CrabError, CrabResult};

pub const PENDING_TRIGGERS_DIR_NAME: &str = "pending_triggers";

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

    let dir = state_root.join(PENDING_TRIGGERS_DIR_NAME);
    fs::create_dir_all(&dir).map_err(|error| CrabError::Io {
        context: "pending_trigger_write",
        path: Some(dir.to_string_lossy().to_string()),
        message: error.to_string(),
    })?;

    let json = serde_json::to_string(trigger).expect("PendingTrigger serialization is infallible");

    let timestamp_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let random_suffix = format!("{:04x}", timestamp_ms as u16 ^ std::process::id() as u16);
    let filename = format!("{timestamp_ms}-{random_suffix}.json");
    let path = dir.join(&filename);

    fs::write(&path, &json).map_err(|error| CrabError::Io {
        context: "pending_trigger_write",
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })?;

    Ok(path)
}

pub fn read_pending_triggers(state_root: &Path) -> CrabResult<Vec<(PathBuf, PendingTrigger)>> {
    let dir = state_root.join(PENDING_TRIGGERS_DIR_NAME);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }

    let entries = fs::read_dir(&dir).map_err(|error| CrabError::Io {
        context: "pending_trigger_read",
        path: Some(dir.to_string_lossy().to_string()),
        message: error.to_string(),
    })?;

    let mut results = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }
        let content = match fs::read_to_string(&path) {
            Ok(content) => content,
            Err(_) => continue,
        };
        let trigger: PendingTrigger = match serde_json::from_str(&content) {
            Ok(trigger) => trigger,
            Err(_) => continue,
        };
        results.push((path, trigger));
    }

    Ok(results)
}

pub fn consume_pending_trigger(path: &Path) -> CrabResult<()> {
    fs::remove_file(path).map_err(|error| CrabError::Io {
        context: "pending_trigger_consume",
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        consume_pending_trigger, read_pending_triggers, validate_pending_trigger,
        write_pending_trigger, PendingTrigger, PENDING_TRIGGERS_DIR_NAME,
    };
    use crate::CrabError;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempDir {
        root: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "crab-self-trigger-{label}-{}-{counter}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("temp dir should be creatable");
            Self { root }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(unix)]
    fn deny_access(path: &std::path::Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o000)).expect("chmod should succeed");
    }

    #[cfg(unix)]
    fn restore_access(path: &std::path::Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;
        let _ = fs::set_permissions(path, fs::Permissions::from_mode(mode));
    }

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
        let temp = TempDir::new("write-read");
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
        let temp = TempDir::new("consume");
        let trigger = sample_trigger();

        let path = write_pending_trigger(&temp.root, &trigger).expect("write should succeed");
        assert!(path.exists());

        consume_pending_trigger(&path).expect("consume should succeed");
        assert!(!path.exists());
    }

    #[test]
    fn read_returns_empty_when_directory_missing() {
        let temp = TempDir::new("read-missing-dir");
        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        assert!(triggers.is_empty());
    }

    #[test]
    fn read_skips_malformed_files() {
        let temp = TempDir::new("read-malformed");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");

        // Write a valid trigger
        let trigger = sample_trigger();
        write_pending_trigger(&temp.root, &trigger).expect("write should succeed");

        // Write a malformed JSON file
        fs::write(dir.join("bad-1.json"), "not-valid-json").expect("malformed write should work");

        // Write a non-JSON file (should be skipped)
        fs::write(dir.join("readme.txt"), "not a trigger").expect("txt write should work");

        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].1, trigger);
    }

    #[test]
    fn write_creates_directory_if_missing() {
        let temp = TempDir::new("write-creates-dir");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        assert!(!dir.exists());

        let trigger = sample_trigger();
        write_pending_trigger(&temp.root, &trigger).expect("write should succeed");
        assert!(dir.is_dir());
    }

    #[test]
    fn write_rejects_invalid_trigger() {
        let temp = TempDir::new("write-invalid");
        let trigger = PendingTrigger {
            channel_id: "".to_string(),
            message: "hello".to_string(),
        };
        let error = write_pending_trigger(&temp.root, &trigger).expect_err("should reject invalid");
        assert!(error.to_string().contains("channel_id must not be empty"));
    }

    #[test]
    fn consume_returns_error_for_missing_file() {
        let temp = TempDir::new("consume-missing");
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
        let temp = TempDir::new("write-create-dir-error");
        // Make the state_root a file so create_dir_all fails.
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
        let temp = TempDir::new("write-file-error");
        // Create the pending_triggers path as a directory with a name collision:
        // make the file-path itself a directory so fs::write to it fails.
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        // Write a valid trigger first to get the pattern, then sabotage.
        let trigger = sample_trigger();
        let path = write_pending_trigger(&temp.root, &trigger).expect("first write should succeed");
        // Replace the written file with a directory to make the next write collide.
        fs::remove_file(&path).expect("remove should succeed");
        fs::create_dir_all(&path).expect("sabotage dir should be creatable");
        // Next write will try a new filename, so we need to sabotage differently.
        // Instead, make the pending_triggers dir read-only so file creation fails.
        #[cfg(unix)]
        deny_access(&dir);
        let result = write_pending_trigger(&temp.root, &trigger);
        #[cfg(unix)]
        restore_access(&dir, 0o755);
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
        let temp = TempDir::new("read-dir-error");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");
        #[cfg(unix)]
        deny_access(&dir);
        let result = read_pending_triggers(&temp.root);
        #[cfg(unix)]
        restore_access(&dir, 0o755);
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
        let temp = TempDir::new("read-unreadable");
        let dir = temp.root.join(PENDING_TRIGGERS_DIR_NAME);
        fs::create_dir_all(&dir).expect("dir should be creatable");

        // Write a valid trigger.
        let trigger = sample_trigger();
        write_pending_trigger(&temp.root, &trigger).expect("write should succeed");

        // Create an unreadable .json file.
        let unreadable_path = dir.join("unreadable.json");
        fs::write(&unreadable_path, "content").expect("write should succeed");
        #[cfg(unix)]
        deny_access(&unreadable_path);

        let triggers = read_pending_triggers(&temp.root).expect("read should succeed");
        // The unreadable file should be skipped; only the valid trigger is returned.
        assert_eq!(triggers.len(), 1);
        assert_eq!(triggers[0].1, trigger);

        #[cfg(unix)]
        restore_access(&unreadable_path, 0o644);
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
}
