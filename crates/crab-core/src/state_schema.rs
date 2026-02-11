use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{CrabError, CrabResult};

pub const STATE_SCHEMA_MARKER_FILE_NAME: &str = "schema_version.json";
const STATE_SCHEMA_LOCK_FILE_NAME: &str = "schema_migration.lock.json";
const MIGRATION_LOCK_STALE_AFTER_MS: u64 = 5 * 60 * 1_000;
pub const CURRENT_STATE_SCHEMA_VERSION: u32 = 1;
pub const MIN_SUPPORTED_STATE_SCHEMA_VERSION: u32 = 0;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateSchemaVersionMarker {
    pub version: u32,
    pub updated_at_epoch_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateSchemaMigrationEventKind {
    MigrationStarted,
    MigrationStep,
    MigrationCompleted,
    MigrationFailed,
}

impl StateSchemaMigrationEventKind {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::MigrationStarted => "migration_started",
            Self::MigrationStep => "migration_step",
            Self::MigrationCompleted => "migration_completed",
            Self::MigrationFailed => "migration_failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSchemaMigrationEvent {
    pub kind: StateSchemaMigrationEventKind,
    pub from_version: u32,
    pub to_version: u32,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSchemaMigrationOutcome {
    pub starting_version: u32,
    pub target_version: u32,
    pub migrated: bool,
    pub marker: StateSchemaVersionMarker,
    pub events: Vec<StateSchemaMigrationEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateSchemaCompatibilityStatus {
    Compatible,
    UnsupportedTooOld,
    UnsupportedTooNew,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateSchemaCompatibilityReport {
    pub marker_path: PathBuf,
    pub marker_present: bool,
    pub detected_version: u32,
    pub supported_min_version: u32,
    pub supported_max_version: u32,
    pub status: StateSchemaCompatibilityStatus,
}

impl StateSchemaCompatibilityReport {
    #[must_use]
    pub fn is_compatible(&self) -> bool {
        self.status == StateSchemaCompatibilityStatus::Compatible
    }
}

pub fn state_schema_marker_path(state_root: &Path) -> PathBuf {
    state_root.join(STATE_SCHEMA_MARKER_FILE_NAME)
}

pub fn evaluate_state_schema_compatibility(
    state_root: &Path,
) -> CrabResult<StateSchemaCompatibilityReport> {
    validate_state_root(state_root, "state_schema_compatibility")?;

    let marker_path = state_schema_marker_path(state_root);
    let maybe_marker = load_marker_if_present(state_root)?;
    let (marker_present, detected_version) = match maybe_marker {
        Some(marker) => (true, marker.version),
        None => (false, 0),
    };

    let status = classify_compatibility_status(
        detected_version,
        MIN_SUPPORTED_STATE_SCHEMA_VERSION,
        CURRENT_STATE_SCHEMA_VERSION,
    );

    Ok(StateSchemaCompatibilityReport {
        marker_path,
        marker_present,
        detected_version,
        supported_min_version: MIN_SUPPORTED_STATE_SCHEMA_VERSION,
        supported_max_version: CURRENT_STATE_SCHEMA_VERSION,
        status,
    })
}

pub fn ensure_state_schema_version(
    state_root: &Path,
    now_epoch_ms: u64,
) -> CrabResult<StateSchemaMigrationOutcome> {
    validate_state_root(state_root, "state_schema_migration")?;
    if now_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "state_schema_migration",
            message: "now_epoch_ms must be greater than 0".to_string(),
        });
    }

    wrap_io(
        fs::create_dir_all(state_root),
        "state_schema_migration",
        state_root,
    )?;

    let mut events = Vec::new();
    let starting_version = load_marker_if_present(state_root)?.map_or(0, |marker| marker.version);

    let migration_target = resolve_migration_target(starting_version)?;

    events.push(StateSchemaMigrationEvent {
        kind: StateSchemaMigrationEventKind::MigrationStarted,
        from_version: starting_version,
        to_version: migration_target,
        detail: None,
    });

    let mut migration_lock = MigrationLock::acquire(state_root, now_epoch_ms)?;
    let result = run_migrations(
        state_root,
        now_epoch_ms,
        starting_version,
        migration_target,
        &mut events,
    );

    if let Err(error) = result {
        events.push(StateSchemaMigrationEvent {
            kind: StateSchemaMigrationEventKind::MigrationFailed,
            from_version: starting_version,
            to_version: migration_target,
            detail: Some(error.to_string()),
        });
        let _ = migration_lock.release();
        return Err(error);
    }

    let marker = load_marker_if_present(state_root)?.ok_or(CrabError::InvariantViolation {
        context: "state_schema_migration",
        message: "schema marker missing after migration completion".to_string(),
    })?;

    events.push(StateSchemaMigrationEvent {
        kind: StateSchemaMigrationEventKind::MigrationCompleted,
        from_version: starting_version,
        to_version: marker.version,
        detail: Some(format!("migrated={}", starting_version != marker.version)),
    });
    migration_lock.release()?;

    Ok(StateSchemaMigrationOutcome {
        starting_version,
        target_version: marker.version,
        migrated: starting_version != marker.version,
        marker,
        events,
    })
}

fn run_migrations(
    state_root: &Path,
    now_epoch_ms: u64,
    starting_version: u32,
    migration_target: u32,
    events: &mut Vec<StateSchemaMigrationEvent>,
) -> CrabResult<()> {
    let mut current_version = starting_version;
    while current_version < migration_target {
        let next_version = current_version.saturating_add(1);
        run_single_migration_step(state_root, now_epoch_ms, current_version, next_version)?;
        events.push(StateSchemaMigrationEvent {
            kind: StateSchemaMigrationEventKind::MigrationStep,
            from_version: current_version,
            to_version: next_version,
            detail: Some(format!("v{current_version}_to_v{next_version}")),
        });
        current_version = next_version;
    }
    Ok(())
}

fn run_single_migration_step(
    state_root: &Path,
    now_epoch_ms: u64,
    from_version: u32,
    to_version: u32,
) -> CrabResult<()> {
    if from_version == 0 && to_version == 1 {
        let marker = StateSchemaVersionMarker {
            version: 1,
            updated_at_epoch_ms: now_epoch_ms,
        };
        write_marker_atomically(state_root, &marker, "state_schema_migration_step")?;
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: "state_schema_migration_step",
        message: format!("missing migration step for state schema {from_version} -> {to_version}"),
    })
}

fn resolve_migration_target(starting_version: u32) -> CrabResult<u32> {
    if starting_version > CURRENT_STATE_SCHEMA_VERSION {
        return Err(CrabError::InvariantViolation {
            context: "state_schema_migration",
            message: format!(
                "state schema version {starting_version} is newer than this binary supports (max {})",
                CURRENT_STATE_SCHEMA_VERSION
            ),
        });
    }

    Ok(CURRENT_STATE_SCHEMA_VERSION)
}

fn classify_compatibility_status(
    detected_version: u32,
    supported_min_version: u32,
    supported_max_version: u32,
) -> StateSchemaCompatibilityStatus {
    if detected_version < supported_min_version {
        return StateSchemaCompatibilityStatus::UnsupportedTooOld;
    }
    if detected_version > supported_max_version {
        return StateSchemaCompatibilityStatus::UnsupportedTooNew;
    }
    StateSchemaCompatibilityStatus::Compatible
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

fn load_marker_if_present(state_root: &Path) -> CrabResult<Option<StateSchemaVersionMarker>> {
    let marker_path = state_schema_marker_path(state_root);
    if !marker_path.exists() {
        return Ok(None);
    }

    let content = wrap_io(
        fs::read_to_string(&marker_path),
        "state_schema_marker_read",
        &marker_path,
    )?;

    let marker: StateSchemaVersionMarker =
        serde_json::from_str(&content).map_err(|_| CrabError::CorruptData {
            context: "state_schema_marker_parse",
            path: marker_path.to_string_lossy().to_string(),
        })?;

    if marker.version == 0 {
        return Err(CrabError::InvariantViolation {
            context: "state_schema_marker_parse",
            message: format!(
                "marker {} has invalid version 0",
                marker_path.to_string_lossy()
            ),
        });
    }
    if marker.updated_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: "state_schema_marker_parse",
            message: format!(
                "marker {} has invalid updated_at_epoch_ms=0",
                marker_path.to_string_lossy()
            ),
        });
    }

    Ok(Some(marker))
}

fn write_marker_atomically(
    state_root: &Path,
    marker: &StateSchemaVersionMarker,
    context: &'static str,
) -> CrabResult<()> {
    let marker_path = state_schema_marker_path(state_root);
    let temp_path = marker_path.with_extension(format!("tmp-{}", std::process::id()));

    let encoded =
        serde_json::to_string_pretty(marker).expect("state schema marker serialization is stable");

    wrap_io(fs::write(&temp_path, encoded), context, &temp_path)?;
    wrap_io(fs::rename(&temp_path, &marker_path), context, &marker_path)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MigrationLockFile {
    pid: u32,
    acquired_at_epoch_ms: u64,
}

#[derive(Debug)]
struct MigrationLock {
    lock_path: PathBuf,
    released: bool,
}

impl MigrationLock {
    fn acquire(state_root: &Path, now_epoch_ms: u64) -> CrabResult<Self> {
        let lock_path = state_root.join(STATE_SCHEMA_LOCK_FILE_NAME);

        loop {
            let open_result = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&lock_path);

            match open_result {
                Ok(mut file) => {
                    let lock_file = MigrationLockFile {
                        pid: std::process::id(),
                        acquired_at_epoch_ms: now_epoch_ms,
                    };
                    let encoded =
                        serde_json::to_string(&lock_file).expect("lock serialization is stable");
                    let context = "state_schema_migration_lock";
                    wrap_io(file.write_all(encoded.as_bytes()), context, &lock_path)?;
                    return Ok(Self {
                        lock_path,
                        released: false,
                    });
                }
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    if !is_lock_stale(&lock_path, now_epoch_ms)? {
                        return Err(CrabError::InvariantViolation {
                            context: "state_schema_migration_lock",
                            message: format!(
                                "migration lock is held at {} (remove stale lock after validating no active migration)",
                                lock_path.to_string_lossy()
                            ),
                        });
                    }

                    match fs::remove_file(&lock_path) {
                        Ok(()) => {}
                        Err(remove_error) if remove_error.kind() == ErrorKind::NotFound => {}
                        Err(remove_error) => {
                            return Err(CrabError::Io {
                                context: "state_schema_migration_lock",
                                path: Some(lock_path.to_string_lossy().to_string()),
                                message: format!("failed to remove stale lock: {remove_error}"),
                            });
                        }
                    }
                    continue;
                }
                Err(error) => {
                    return Err(CrabError::Io {
                        context: "state_schema_migration_lock",
                        path: Some(lock_path.to_string_lossy().to_string()),
                        message: error.to_string(),
                    });
                }
            }
        }
    }

    fn release(&mut self) -> CrabResult<()> {
        if self.released {
            return Ok(());
        }

        if let Err(error) = fs::remove_file(&self.lock_path) {
            if error.kind() != ErrorKind::NotFound {
                return Err(CrabError::Io {
                    context: "state_schema_migration_lock",
                    path: Some(self.lock_path.to_string_lossy().to_string()),
                    message: format!("failed to release lock: {error}"),
                });
            }
        }
        self.released = true;
        Ok(())
    }
}

impl Drop for MigrationLock {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

fn is_lock_stale(lock_path: &Path, now_epoch_ms: u64) -> CrabResult<bool> {
    let content = match fs::read_to_string(lock_path) {
        Ok(content) => content,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
        Err(error) => {
            return Err(CrabError::Io {
                context: "state_schema_migration_lock",
                path: Some(lock_path.to_string_lossy().to_string()),
                message: format!("failed to read lock file: {error}"),
            });
        }
    };

    let lock = match serde_json::from_str::<MigrationLockFile>(&content) {
        Ok(lock) => lock,
        Err(_) => return Ok(true),
    };

    let age_ms = now_epoch_ms.saturating_sub(lock.acquired_at_epoch_ms);
    Ok(age_ms >= MIGRATION_LOCK_STALE_AFTER_MS)
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
    use std::io::ErrorKind;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        classify_compatibility_status, ensure_state_schema_version,
        evaluate_state_schema_compatibility, is_lock_stale, resolve_migration_target,
        run_single_migration_step, state_schema_marker_path, wrap_io, MigrationLock,
        MigrationLockFile, StateSchemaCompatibilityStatus, StateSchemaMigrationEventKind,
        CURRENT_STATE_SCHEMA_VERSION, MIGRATION_LOCK_STALE_AFTER_MS,
        MIN_SUPPORTED_STATE_SCHEMA_VERSION,
    };
    use crate::CrabError;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug)]
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(label: &str) -> Self {
            let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "crab-state-schema-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("temp dir should be creatable");
            Self { path }
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn state_root(root: &Path) -> PathBuf {
        root.join("state")
    }

    #[test]
    fn compatibility_reports_legacy_when_marker_missing() {
        let root = TempDir::new("compat-legacy");
        let report = evaluate_state_schema_compatibility(&state_root(&root.path))
            .expect("compatibility should evaluate");

        assert!(!report.marker_present);
        assert_eq!(report.detected_version, 0);
        assert_eq!(report.supported_max_version, CURRENT_STATE_SCHEMA_VERSION);
        assert_eq!(report.status, StateSchemaCompatibilityStatus::Compatible);
        assert!(report.is_compatible());
    }

    #[test]
    fn compatibility_reports_too_new_state() {
        let root = TempDir::new("compat-too-new");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        fs::write(
            state_schema_marker_path(&state_root),
            r#"{"version":999,"updated_at_epoch_ms":1739173200000}"#,
        )
        .expect("marker should be writable");

        let report = evaluate_state_schema_compatibility(&state_root)
            .expect("compatibility should evaluate");
        assert!(report.marker_present);
        assert_eq!(report.detected_version, 999);
        assert_eq!(
            report.status,
            StateSchemaCompatibilityStatus::UnsupportedTooNew
        );
        assert!(!report.is_compatible());
    }

    #[test]
    fn ensure_state_schema_initializes_marker_and_is_noop_on_rerun() {
        let root = TempDir::new("migrate-init");
        let state_root = state_root(&root.path);

        let first = ensure_state_schema_version(&state_root, 1_739_173_200_000)
            .expect("first migration should succeed");
        assert_eq!(first.starting_version, 0);
        assert_eq!(first.target_version, CURRENT_STATE_SCHEMA_VERSION);
        assert!(first.migrated);
        assert_eq!(
            first
                .events
                .first()
                .expect("events should include started")
                .kind,
            StateSchemaMigrationEventKind::MigrationStarted
        );
        assert!(first
            .events
            .iter()
            .any(|event| event.kind == StateSchemaMigrationEventKind::MigrationStep));
        assert!(first
            .events
            .iter()
            .any(|event| event.kind == StateSchemaMigrationEventKind::MigrationCompleted));

        let second = ensure_state_schema_version(&state_root, 1_739_173_200_100)
            .expect("second migration should be no-op");
        assert_eq!(second.starting_version, CURRENT_STATE_SCHEMA_VERSION);
        assert_eq!(second.target_version, CURRENT_STATE_SCHEMA_VERSION);
        assert!(!second.migrated);
        assert!(!second
            .events
            .iter()
            .any(|event| event.kind == StateSchemaMigrationEventKind::MigrationStep));
    }

    #[test]
    fn ensure_state_schema_reclaims_stale_lock_and_blocks_active_lock() {
        let root = TempDir::new("migrate-lock");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should be creatable");

        let lock_path = state_root.join("schema_migration.lock.json");
        let stale = MigrationLockFile {
            pid: 1,
            acquired_at_epoch_ms: 1,
        };
        fs::write(
            &lock_path,
            serde_json::to_string(&stale).expect("stale lock serialization"),
        )
        .expect("stale lock should be writable");

        ensure_state_schema_version(&state_root, 1_739_173_200_000)
            .expect("stale lock should be reclaimed");
        assert!(!lock_path.exists());

        let active = MigrationLockFile {
            pid: 2,
            acquired_at_epoch_ms: 1_739_173_200_000,
        };
        fs::write(
            &lock_path,
            serde_json::to_string(&active).expect("active lock serialization"),
        )
        .expect("active lock should be writable");

        let error = ensure_state_schema_version(&state_root, 1_739_173_200_010)
            .expect_err("active lock should block migration");
        assert!(error.to_string().contains("migration lock is held"));
    }

    #[test]
    fn ensure_state_schema_rejects_corrupt_marker() {
        let root = TempDir::new("migrate-corrupt-marker");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        fs::write(state_schema_marker_path(&state_root), "not-json")
            .expect("corrupt marker should be writable");

        let error = ensure_state_schema_version(&state_root, 1_739_173_200_000)
            .expect_err("corrupt marker should fail");
        assert!(error.to_string().contains("state_schema_marker_parse"));
    }

    #[test]
    fn migration_event_kind_tokens_are_stable() {
        assert_eq!(
            StateSchemaMigrationEventKind::MigrationStarted.as_token(),
            "migration_started"
        );
        assert_eq!(
            StateSchemaMigrationEventKind::MigrationStep.as_token(),
            "migration_step"
        );
        assert_eq!(
            StateSchemaMigrationEventKind::MigrationCompleted.as_token(),
            "migration_completed"
        );
        assert_eq!(
            StateSchemaMigrationEventKind::MigrationFailed.as_token(),
            "migration_failed"
        );
    }

    #[test]
    fn ensure_state_schema_validates_inputs_and_layout_failures() {
        let empty_path = Path::new("");
        let empty_error = ensure_state_schema_version(empty_path, 1_739_173_200_000)
            .expect_err("empty root should fail");
        assert!(empty_error
            .to_string()
            .contains("state_root must not be empty"));

        let zero_now = TempDir::new("migrate-zero-now");
        let zero_error = ensure_state_schema_version(&state_root(&zero_now.path), 0)
            .expect_err("zero now should fail");
        assert!(zero_error
            .to_string()
            .contains("now_epoch_ms must be greater than 0"));

        let root = TempDir::new("migrate-layout-error");
        let file_path = root.path.join("state");
        fs::write(&file_path, "file").expect("fixture file should be writable");
        let layout_error = ensure_state_schema_version(&file_path, 1_739_173_200_000)
            .expect_err("file root should fail");
        assert!(layout_error.to_string().contains("state_schema_migration"));
    }

    #[test]
    fn compatibility_validates_root_and_handles_invalid_markers() {
        let empty =
            evaluate_state_schema_compatibility(Path::new("")).expect_err("empty path should fail");
        assert!(empty.to_string().contains("state_root must not be empty"));

        let root = TempDir::new("compat-invalid-marker");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should exist");

        fs::write(
            state_schema_marker_path(&state_root),
            r#"{"version":1,"updated_at_epoch_ms":0}"#,
        )
        .expect("marker should be writable");
        let zero_ts = evaluate_state_schema_compatibility(&state_root)
            .expect_err("zero updated_at should fail");
        assert!(zero_ts.to_string().contains("updated_at_epoch_ms=0"));

        fs::write(
            state_schema_marker_path(&state_root),
            r#"{"version":0,"updated_at_epoch_ms":1739173200000}"#,
        )
        .expect("marker should be writable");
        let zero_version =
            evaluate_state_schema_compatibility(&state_root).expect_err("zero version should fail");
        assert!(zero_version.to_string().contains("invalid version 0"));

        fs::remove_file(state_schema_marker_path(&state_root)).expect("remove file marker");
        fs::create_dir_all(state_schema_marker_path(&state_root)).expect("directory marker");
        let read_error = evaluate_state_schema_compatibility(&state_root)
            .expect_err("marker directory should fail read");
        assert!(read_error.to_string().contains("state_schema_marker_read"));
    }

    #[test]
    fn ensure_state_schema_emits_failure_when_step_write_fails() {
        let root = TempDir::new("migrate-step-write-fail");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should be creatable");
        let temp_path = state_schema_marker_path(&state_root)
            .with_extension(format!("tmp-{}", std::process::id()));
        fs::create_dir_all(&temp_path).expect("temp path directory should block write");

        let error = ensure_state_schema_version(&state_root, 1_739_173_200_000)
            .expect_err("step write failure should bubble");
        assert!(error.to_string().contains("state_schema_migration_step"));
    }

    #[test]
    fn compatibility_classifier_covers_all_statuses() {
        assert_eq!(
            classify_compatibility_status(1, 1, 2),
            StateSchemaCompatibilityStatus::Compatible
        );
        assert_eq!(
            classify_compatibility_status(0, 1, 2),
            StateSchemaCompatibilityStatus::UnsupportedTooOld
        );
        assert_eq!(
            classify_compatibility_status(3, 1, 2),
            StateSchemaCompatibilityStatus::UnsupportedTooNew
        );
        assert_eq!(MIN_SUPPORTED_STATE_SCHEMA_VERSION, 0);
    }

    #[test]
    fn migration_step_rejects_unknown_transition() {
        let root = TempDir::new("migrate-step-unknown");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should exist");
        let error = run_single_migration_step(&state_root, 1_739_173_200_000, 1, 2)
            .expect_err("unknown step should fail");
        assert!(error.to_string().contains("missing migration step"));
    }

    #[test]
    fn resolve_migration_target_rejects_too_new_schema() {
        let error = resolve_migration_target(CURRENT_STATE_SCHEMA_VERSION + 1)
            .expect_err("too-new version should be rejected");
        assert!(error
            .to_string()
            .contains("is newer than this binary supports"));
    }

    #[test]
    fn migration_lock_acquire_release_success_and_missing_lock_release_is_noop() {
        let root = TempDir::new("lock-acquire-release");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should exist");

        let lock_path = state_root.join("schema_migration.lock.json");
        let mut lock = MigrationLock::acquire(&state_root, 1_739_173_200_000)
            .expect("acquire should create lock");
        assert!(lock_path.exists());

        fs::remove_file(&lock_path).expect("lock file should be removable");
        lock.release()
            .expect("release should ignore missing lock file");
        assert!(lock.released);
    }

    #[cfg(unix)]
    #[test]
    fn migration_lock_returns_error_when_stale_lock_cannot_be_removed() {
        let root = TempDir::new("lock-stale-remove-fail");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should exist");

        let lock_path = state_root.join("schema_migration.lock.json");
        let stale = MigrationLockFile {
            pid: 42,
            acquired_at_epoch_ms: 1,
        };
        fs::write(
            &lock_path,
            serde_json::to_string(&stale).expect("stale lock serialization"),
        )
        .expect("stale lock should be writable");

        let original_permissions = fs::metadata(&state_root)
            .expect("metadata should be readable")
            .permissions()
            .mode();
        fs::set_permissions(&state_root, fs::Permissions::from_mode(0o555))
            .expect("state root should be set read-only");

        let now = MIGRATION_LOCK_STALE_AFTER_MS + 1_000;
        let error = MigrationLock::acquire(&state_root, now)
            .expect_err("stale lock remove failure should be surfaced");
        assert!(error.to_string().contains("failed to remove stale lock"));

        fs::set_permissions(
            &state_root,
            fs::Permissions::from_mode(original_permissions),
        )
        .expect("permissions should be restorable");
    }

    #[test]
    fn lock_helpers_cover_error_and_edge_paths() {
        let root = TempDir::new("lock-helper-paths");
        let state_root = state_root(&root.path);
        fs::create_dir_all(&state_root).expect("state root should exist");

        let lock_path = state_root.join("schema_migration.lock.json");
        fs::write(&lock_path, "not-json").expect("invalid lock file should be writable");
        assert!(is_lock_stale(&lock_path, 1_739_173_200_000).expect("invalid lock should be stale"));

        let missing = state_root.join("missing.lock.json");
        assert!(is_lock_stale(&missing, 1_739_173_200_000).expect("missing lock should be stale"));

        let read_error_path = state_root.join("dir-lock");
        fs::create_dir_all(&read_error_path).expect("dir lock should exist");
        let read_error =
            is_lock_stale(&read_error_path, 1_739_173_200_000).expect_err("dir lock should fail");
        assert!(read_error.to_string().contains("failed to read lock file"));

        let bad_state_root = root.path.join("state-root-as-file");
        fs::write(&bad_state_root, "file").expect("file root should be writable");
        let acquire_error = MigrationLock::acquire(&bad_state_root, 1_739_173_200_000)
            .expect_err("acquire should fail");
        assert!(acquire_error
            .to_string()
            .contains("state_schema_migration_lock"));

        let mut lock = MigrationLock {
            lock_path: read_error_path.clone(),
            released: false,
        };
        let release_error = lock
            .release()
            .expect_err("release on directory should fail");
        assert!(release_error.to_string().contains("failed to release lock"));
    }

    #[test]
    fn wrap_io_maps_error_context_and_path() {
        let error = wrap_io::<()>(
            Err(std::io::Error::new(ErrorKind::PermissionDenied, "denied")),
            "state_schema_wrap",
            Path::new("/tmp/state"),
        )
        .expect_err("wrap_io should map error");
        assert_eq!(
            error,
            CrabError::Io {
                context: "state_schema_wrap",
                path: Some("/tmp/state".to_string()),
                message: "denied".to_string(),
            }
        );
    }
}
