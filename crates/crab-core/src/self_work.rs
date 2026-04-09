use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{CrabError, CrabResult};

pub const SELF_WORK_SESSION_FILE_NAME: &str = "self_work_session.json";
pub const SELF_WORK_SESSION_LOCK_FILE_NAME: &str = "self_work_session.lock.json";
const SELF_WORK_SESSION_LOCK_STALE_AFTER_MS: u64 = 5 * 60 * 1_000;
pub const CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelfWorkSessionStatus {
    Active,
    Stopped,
    Expired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SelfWorkSession {
    pub schema_version: u32,
    pub session_id: String,
    pub channel_id: String,
    pub goal: String,
    pub started_at_epoch_ms: u64,
    pub started_at_iso8601: String,
    pub end_at_epoch_ms: u64,
    pub end_at_iso8601: String,
    pub status: SelfWorkSessionStatus,
    pub last_wake_triggered_at_epoch_ms: Option<u64>,
    pub final_trigger_pending: bool,
    pub stopped_at_epoch_ms: Option<u64>,
    pub expired_at_epoch_ms: Option<u64>,
    pub last_expiry_triggered_at_epoch_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SelfWorkSessionLockFile {
    pid: u32,
    acquired_at_epoch_ms: u64,
}

#[derive(Debug)]
pub struct SelfWorkSessionLock {
    lock_path: PathBuf,
    released: bool,
}

pub fn self_work_session_path(state_root: &Path) -> PathBuf {
    state_root.join(SELF_WORK_SESSION_FILE_NAME)
}

pub fn read_self_work_session(state_root: &Path) -> CrabResult<Option<SelfWorkSession>> {
    validate_state_root(state_root, "self_work_session_read")?;

    let session_path = self_work_session_path(state_root);
    if !session_path.exists() {
        return Ok(None);
    }

    let content = wrap_io(
        fs::read_to_string(&session_path),
        "self_work_session_read",
        &session_path,
    )?;
    let session: SelfWorkSession =
        serde_json::from_str(&content).map_err(|_| CrabError::CorruptData {
            context: "self_work_session_parse",
            path: session_path.to_string_lossy().to_string(),
        })?;
    validate_self_work_session(&session)?;
    Ok(Some(session))
}

pub fn write_self_work_session_atomically(
    state_root: &Path,
    session: &SelfWorkSession,
) -> CrabResult<()> {
    validate_state_root(state_root, "self_work_session_write")?;
    validate_self_work_session(session)?;
    wrap_io(
        fs::create_dir_all(state_root),
        "self_work_session_write",
        state_root,
    )?;

    let session_path = self_work_session_path(state_root);
    let temp_path = session_path.with_extension(format!("tmp-{}", std::process::id()));
    let encoded =
        serde_json::to_string_pretty(session).expect("self-work session serialization is stable");

    wrap_io(
        fs::write(&temp_path, encoded),
        "self_work_session_write",
        &temp_path,
    )?;
    wrap_io(
        fs::rename(&temp_path, &session_path),
        "self_work_session_write",
        &session_path,
    )
}

pub fn validate_new_self_work_start(existing: Option<&SelfWorkSession>) -> CrabResult<()> {
    if let Some(session) = existing {
        if session.status == SelfWorkSessionStatus::Active {
            return Err(CrabError::InvariantViolation {
                context: "self_work_session_start",
                message: format!(
                    "active self-work session {} already exists",
                    session.session_id
                ),
            });
        }
    }
    Ok(())
}

pub fn validate_self_work_session(session: &SelfWorkSession) -> CrabResult<()> {
    if session.schema_version != CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: format!(
                "schema_version must be {}, got {}",
                CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION, session.schema_version
            ),
        });
    }
    if session.session_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "session_id must not be empty".to_string(),
        });
    }
    if session.channel_id.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "channel_id must not be empty".to_string(),
        });
    }
    if session.goal.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "goal must not be empty".to_string(),
        });
    }
    if session.started_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "started_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    if session.end_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "end_at_epoch_ms must be greater than 0".to_string(),
        });
    }
    if session.end_at_epoch_ms <= session.started_at_epoch_ms {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "end_at_epoch_ms must be greater than started_at_epoch_ms".to_string(),
        });
    }
    if session.started_at_iso8601.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "started_at_iso8601 must not be empty".to_string(),
        });
    }
    if session.end_at_iso8601.trim().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "self_work_session_validation",
            message: "end_at_iso8601 must not be empty".to_string(),
        });
    }
    Ok(())
}

impl SelfWorkSessionLock {
    pub fn acquire(state_root: &Path, now_epoch_ms: u64) -> CrabResult<Self> {
        validate_state_root(state_root, "self_work_session_lock")?;
        if now_epoch_ms == 0 {
            return Err(CrabError::InvariantViolation {
                context: "self_work_session_lock",
                message: "now_epoch_ms must be greater than 0".to_string(),
            });
        }
        wrap_io(fs::create_dir_all(state_root), "self_work_session_lock", state_root)?;

        let lock_path = state_root.join(SELF_WORK_SESSION_LOCK_FILE_NAME);
        loop {
            let open_result = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path);

            match open_result {
                Ok(mut file) => {
                    let lock_file = SelfWorkSessionLockFile {
                        pid: std::process::id(),
                        acquired_at_epoch_ms: now_epoch_ms,
                    };
                    let encoded = serde_json::to_string(&lock_file)
                        .expect("self-work session lock serialization is stable");
                    wrap_io(file.write_all(encoded.as_bytes()), "self_work_session_lock", &lock_path)?;
                    return Ok(Self {
                        lock_path,
                        released: false,
                    });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    if !is_lock_stale(&lock_path, now_epoch_ms)? {
                        return Err(CrabError::InvariantViolation {
                            context: "self_work_session_lock",
                            message: format!(
                                "self-work session lock is held at {} (remove stale lock after validating no active command)",
                                lock_path.to_string_lossy()
                            ),
                        });
                    }

                    if let Err(remove_error) = fs::remove_file(&lock_path) {
                        if remove_error.kind() != ErrorKind::NotFound { return Err(CrabError::Io {
                            context: "self_work_session_lock",
                            path: Some(lock_path.to_string_lossy().to_string()),
                            message: format!("failed to remove stale lock: {remove_error}"),
                        }); }
                    }
                }
                Err(error) => {
                    return Err(CrabError::Io {
                        context: "self_work_session_lock",
                        path: Some(lock_path.to_string_lossy().to_string()),
                        message: error.to_string(),
                    });
                }
            }
        }
    }

    pub fn release(&mut self) -> CrabResult<()> {
        if self.released {
            return Ok(());
        }

        if let Err(error) = fs::remove_file(&self.lock_path) {
            if error.kind() != ErrorKind::NotFound {
                return Err(CrabError::Io {
                    context: "self_work_session_lock",
                    path: Some(self.lock_path.to_string_lossy().to_string()),
                    message: format!("failed to release lock: {error}"),
                });
            }
        }
        self.released = true;
        Ok(())
    }
}

impl Drop for SelfWorkSessionLock {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

fn validate_state_root(state_root: &Path, context: &'static str) -> CrabResult<()> {
    if state_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context,
            message: "state_root must not be empty".to_string(),
        });
    }
    Ok(())
}

fn is_lock_stale(lock_path: &Path, now_epoch_ms: u64) -> CrabResult<bool> {
    let content = match fs::read_to_string(lock_path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(CrabError::Io {
                context: "self_work_session_lock",
                path: Some(lock_path.to_string_lossy().to_string()),
                message: format!("failed to read lock file: {error}"),
            });
        }
    };

    let lock_file = match serde_json::from_str::<SelfWorkSessionLockFile>(&content) {
        Ok(lock_file) => lock_file,
        Err(_) => return Ok(true),
    };
    let age_ms = now_epoch_ms.saturating_sub(lock_file.acquired_at_epoch_ms);
    Ok(age_ms >= SELF_WORK_SESSION_LOCK_STALE_AFTER_MS)
}

fn wrap_io<T>(result: std::io::Result<T>, context: &'static str, path: &Path) -> CrabResult<T> {
    result.map_err(|error| CrabError::Io {
        context,
        path: Some(path.to_string_lossy().to_string()),
        message: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use super::{
        read_self_work_session, self_work_session_path, validate_new_self_work_start,
        validate_self_work_session, write_self_work_session_atomically, SelfWorkSession,
        SelfWorkSessionLock, SelfWorkSessionStatus, CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
        SELF_WORK_SESSION_FILE_NAME, SELF_WORK_SESSION_LOCK_FILE_NAME,
    };
    use crate::test_support::TempDir;
    use crate::CrabError;

    fn sample_session(status: SelfWorkSessionStatus) -> SelfWorkSession {
        SelfWorkSession {
            schema_version: CURRENT_SELF_WORK_SESSION_SCHEMA_VERSION,
            session_id: "self-work:1739173200000".to_string(),
            channel_id: "123456789".to_string(),
            goal: "Ship the feature".to_string(),
            started_at_epoch_ms: 1_739_173_200_000,
            started_at_iso8601: "2025-02-10T10:00:00Z".to_string(),
            end_at_epoch_ms: 1_739_174_100_000,
            end_at_iso8601: "2025-02-10T10:15:00Z".to_string(),
            status,
            last_wake_triggered_at_epoch_ms: Some(1_739_173_500_000),
            final_trigger_pending: false,
            stopped_at_epoch_ms: (status == SelfWorkSessionStatus::Stopped)
                .then_some(1_739_173_600_000),
            expired_at_epoch_ms: (status == SelfWorkSessionStatus::Expired)
                .then_some(1_739_174_100_000),
            last_expiry_triggered_at_epoch_ms: (status == SelfWorkSessionStatus::Expired)
                .then_some(1_739_174_100_000),
        }
    }

    #[test]
    fn read_returns_none_when_file_missing() {
        let temp = TempDir::new("self-work", "missing");
        let session = read_self_work_session(&temp.root).expect("read should succeed");
        assert!(session.is_none());
    }

    #[test]
    fn write_and_read_round_trip() {
        let temp = TempDir::new("self-work", "round-trip");
        let session = sample_session(SelfWorkSessionStatus::Active);

        write_self_work_session_atomically(&temp.root, &session).expect("write should succeed");

        let path = self_work_session_path(&temp.root);
        assert!(path.exists());
        assert!(path.to_string_lossy().contains(SELF_WORK_SESSION_FILE_NAME));

        let loaded = read_self_work_session(&temp.root)
            .expect("read should succeed")
            .expect("session should exist");
        assert_eq!(loaded, session);
    }

    #[test]
    fn new_start_rejects_active_session_and_allows_terminal_states() {
        validate_new_self_work_start(None).expect("missing session should be allowed");
        validate_new_self_work_start(Some(&sample_session(SelfWorkSessionStatus::Stopped)))
            .expect("stopped session should be allowed");
        validate_new_self_work_start(Some(&sample_session(SelfWorkSessionStatus::Expired)))
            .expect("expired session should be allowed");

        let error =
            validate_new_self_work_start(Some(&sample_session(SelfWorkSessionStatus::Active)))
                .expect_err("active session should be rejected");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "self_work_session_start",
                ..
            }
        ));
        assert!(error.to_string().contains("active self-work session"));
    }

    #[test]
    fn lock_rejects_concurrent_holder() {
        let temp = TempDir::new("self-work", "lock-held");
        let _lock =
            SelfWorkSessionLock::acquire(&temp.root, 1_739_173_200_000).expect("lock should work");

        let error = SelfWorkSessionLock::acquire(&temp.root, 1_739_173_200_001)
            .expect_err("second lock should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "self_work_session_lock",
                ..
            }
        ));
        assert!(error.to_string().contains(SELF_WORK_SESSION_LOCK_FILE_NAME));
    }

    #[test]
    fn lock_recovers_stale_file() {
        let temp = TempDir::new("self-work", "stale-lock");
        let stale_lock = temp.root.join(SELF_WORK_SESSION_LOCK_FILE_NAME);
        fs::write(
            &stale_lock,
            r#"{"pid":42,"acquired_at_epoch_ms":1739173200000}"#,
        )
        .expect("stale lock should be writable");

        let mut lock = SelfWorkSessionLock::acquire(&temp.root, 1_739_173_600_001)
            .expect("stale lock should be replaced");
        assert!(stale_lock.exists(), "fresh lock should exist");
        lock.release().expect("release should succeed");
        assert!(!stale_lock.exists(), "lock should be removed after release");
    }

    #[test]
    fn malformed_json_returns_corrupt_data_error() {
        let temp = TempDir::new("self-work", "corrupt");
        let path = temp.root.join(SELF_WORK_SESSION_FILE_NAME);
        fs::write(&path, "{not-json").expect("corrupt file should be writable");

        let error =
            read_self_work_session(&temp.root).expect_err("corrupt session should be rejected");
        assert_eq!(
            error,
            CrabError::CorruptData {
                context: "self_work_session_parse",
                path: path.to_string_lossy().to_string(),
            }
        );
    }

    #[test]
    fn validate_session_rejects_each_invalid_field() {
        let invalid_cases = [
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.schema_version = 0;
                    session
                },
                "schema_version must be 1, got 0",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.session_id = "  ".to_string();
                    session
                },
                "session_id must not be empty",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.channel_id = "  ".to_string();
                    session
                },
                "channel_id must not be empty",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.goal = " ".to_string();
                    session
                },
                "goal must not be empty",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.started_at_epoch_ms = 0;
                    session
                },
                "started_at_epoch_ms must be greater than 0",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.end_at_epoch_ms = 0;
                    session
                },
                "end_at_epoch_ms must be greater than 0",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.end_at_epoch_ms = session.started_at_epoch_ms;
                    session
                },
                "end_at_epoch_ms must be greater than started_at_epoch_ms",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.started_at_iso8601 = " ".to_string();
                    session
                },
                "started_at_iso8601 must not be empty",
            ),
            (
                {
                    let mut session = sample_session(SelfWorkSessionStatus::Active);
                    session.end_at_iso8601 = " ".to_string();
                    session
                },
                "end_at_iso8601 must not be empty",
            ),
        ];

        for (session, expected_message) in invalid_cases {
            let error =
                validate_self_work_session(&session).expect_err("invalid session should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "self_work_session_validation",
                    message: expected_message.to_string(),
                }
            );
        }
    }

    #[test]
    fn read_surfaces_io_error_when_session_path_is_a_directory() {
        let temp = TempDir::new("self-work", "read-io-error");
        let session_path = temp.root.join(SELF_WORK_SESSION_FILE_NAME);
        fs::create_dir_all(&session_path).expect("directory should be creatable");

        let error =
            read_self_work_session(&temp.root).expect_err("directory session path should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "self_work_session_read",
                ..
            }
        ));
    }

    #[test]
    fn write_rejects_empty_state_root_and_invalid_session() {
        let empty_path_error =
            write_self_work_session_atomically(Path::new(""), &sample_session(SelfWorkSessionStatus::Active))
                .expect_err("empty state root should fail");
        assert_eq!(
            empty_path_error,
            CrabError::InvariantViolation {
                context: "self_work_session_write",
                message: "state_root must not be empty".to_string(),
            }
        );

        let temp = TempDir::new("self-work", "write-invalid-session");
        let mut invalid_session = sample_session(SelfWorkSessionStatus::Active);
        invalid_session.goal = " ".to_string();
        let error = write_self_work_session_atomically(&temp.root, &invalid_session)
            .expect_err("invalid session should fail validation");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "self_work_session_validation",
                message: "goal must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn write_surfaces_create_write_and_rename_errors() {
        let temp = TempDir::new("self-work", "write-io-errors");
        let session = sample_session(SelfWorkSessionStatus::Active);

        let create_dir_root = temp.root.join("create-dir-error");
        fs::write(&create_dir_root, "not a directory").expect("file sentinel should be writable");
        let create_error = write_self_work_session_atomically(&create_dir_root, &session)
            .expect_err("create_dir_all error should surface");
        assert!(matches!(
            create_error,
            CrabError::Io {
                context: "self_work_session_write",
                ..
            }
        ));

        let write_root = temp.root.join("write-error");
        fs::create_dir_all(&write_root).expect("write error root should be creatable");
        let temp_path =
            self_work_session_path(&write_root).with_extension(format!("tmp-{}", std::process::id()));
        fs::create_dir_all(&temp_path).expect("temp path directory should be creatable");
        let write_error = write_self_work_session_atomically(&write_root, &session)
            .expect_err("temp file write error should surface");
        assert!(matches!(
            write_error,
            CrabError::Io {
                context: "self_work_session_write",
                ..
            }
        ));

        let rename_root = temp.root.join("rename-error");
        fs::create_dir_all(&rename_root).expect("rename error root should be creatable");
        fs::create_dir_all(rename_root.join(SELF_WORK_SESSION_FILE_NAME))
            .expect("session path directory should be creatable");
        let rename_error = write_self_work_session_atomically(&rename_root, &session)
            .expect_err("rename error should surface");
        assert!(matches!(
            rename_error,
            CrabError::Io {
                context: "self_work_session_write",
                ..
            }
        ));
    }

    #[test]
    fn lock_rejects_empty_state_root_and_zero_clock() {
        let empty_path_error = SelfWorkSessionLock::acquire(Path::new(""), 1)
            .expect_err("empty state root should fail");
        assert_eq!(
            empty_path_error,
            CrabError::InvariantViolation {
                context: "self_work_session_lock",
                message: "state_root must not be empty".to_string(),
            }
        );

        let temp = TempDir::new("self-work", "lock-zero-clock");
        let zero_clock_error = SelfWorkSessionLock::acquire(&temp.root, 0)
            .expect_err("zero clock should fail");
        assert_eq!(
            zero_clock_error,
            CrabError::InvariantViolation {
                context: "self_work_session_lock",
                message: "now_epoch_ms must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn lock_surfaces_lock_read_and_release_errors() {
        let temp = TempDir::new("self-work", "lock-io-errors");
        let lock_path = temp.root.join(SELF_WORK_SESSION_LOCK_FILE_NAME);
        fs::create_dir_all(&lock_path).expect("lock path directory should be creatable");

        let read_error = SelfWorkSessionLock::acquire(&temp.root, 1_739_173_200_000)
            .expect_err("directory lock path should fail to read");
        assert!(matches!(
            read_error,
            CrabError::Io {
                context: "self_work_session_lock",
                ..
            }
        ));

        fs::remove_dir_all(&lock_path).expect("directory lock path should be removable");
        let mut lock = SelfWorkSessionLock::acquire(&temp.root, 1_739_173_200_001)
            .expect("lock should be acquirable");
        fs::remove_file(&lock_path).expect("lock file should be removable");
        fs::create_dir_all(&lock_path).expect("lock path directory should be creatable again");
        let release_error = lock.release().expect_err("directory lock path should fail release");
        assert!(matches!(
            release_error,
            CrabError::Io {
                context: "self_work_session_lock",
                ..
            }
        ));
    }

    #[test]
    fn malformed_stale_lock_is_reclaimed_and_missing_release_file_is_ignored() {
        let temp = TempDir::new("self-work", "lock-invalid-json");
        let lock_path = temp.root.join(SELF_WORK_SESSION_LOCK_FILE_NAME);
        fs::write(&lock_path, "not-json").expect("invalid stale lock should be writable");

        let mut lock = SelfWorkSessionLock::acquire(&temp.root, 1_739_173_200_000)
            .expect("invalid stale lock should be replaced");
        fs::remove_file(&lock_path).expect("lock file should be removable");
        lock.release()
            .expect("missing lock file during release should be treated as success");
    }

    #[test]
    #[cfg(unix)]
    fn lock_surfaces_stale_remove_and_open_permission_errors() {
        let temp = TempDir::new("self-work", "lock-permissions");
        let remove_root = temp.root.join("remove-error");
        fs::create_dir_all(&remove_root).expect("remove error root should be creatable");
        let stale_lock = remove_root.join(SELF_WORK_SESSION_LOCK_FILE_NAME);
        fs::write(
            &stale_lock,
            r#"{"pid":42,"acquired_at_epoch_ms":1739173200000}"#,
        )
        .expect("stale lock should be writable");

        let original_permissions = fs::metadata(&remove_root)
            .expect("metadata should load")
            .permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o555);
        fs::set_permissions(&remove_root, read_only_permissions)
            .expect("directory should become read only");

        let remove_error = SelfWorkSessionLock::acquire(&remove_root, 1_739_173_800_000)
            .expect_err("stale lock removal permission error should surface");

        fs::set_permissions(&remove_root, original_permissions)
            .expect("directory permissions should be restorable");

        assert!(matches!(
            remove_error,
            CrabError::Io {
                context: "self_work_session_lock",
                ..
            }
        ));

        let open_root = temp.root.join("open-error");
        fs::create_dir_all(&open_root).expect("open error root should be creatable");
        let original_permissions = fs::metadata(&open_root)
            .expect("metadata should load")
            .permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o555);
        fs::set_permissions(&open_root, read_only_permissions)
            .expect("directory should become read only");

        let open_error = SelfWorkSessionLock::acquire(&open_root, 1_739_173_200_001)
            .expect_err("lock file open permission error should surface");

        fs::set_permissions(&open_root, original_permissions)
            .expect("directory permissions should be restorable");

        assert!(matches!(
            open_error,
            CrabError::Io {
                context: "self_work_session_lock",
                ..
            }
        ));
    }

    #[test]
    fn lock_release_is_idempotent_after_success() {
        let temp = TempDir::new("self-work", "lock-release-idempotent");
        let mut lock = SelfWorkSessionLock::acquire(&temp.root, 1_739_173_200_000)
            .expect("lock should be acquirable");
        lock.release().expect("initial release should succeed");
        lock.release().expect("second release should be a no-op");
    }
}
