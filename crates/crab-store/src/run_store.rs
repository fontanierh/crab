use std::ffi::OsStr;
use std::fs;
use std::path::PathBuf;

use crab_core::{CrabError, CrabResult, Run};

use crate::helpers::{
    ensure_non_empty_field, hex_encode, read_json_with_backup, validate_run, wrap_io,
    write_run_atomically,
};

const RUNS_DIR_NAME: &str = "runs";

#[derive(Debug, Clone)]
pub struct RunStore {
    root: PathBuf,
}

impl RunStore {
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn upsert_run(&self, run: &Run) -> CrabResult<()> {
        let mut clamped = run.clone();
        clamp_run_timestamps(&mut clamped);
        validate_run(&clamped)?;
        self.ensure_layout()?;
        let run_path = self.run_path(&clamped.logical_session_id, &clamped.id);
        write_run_atomically(&run_path, &clamped, "run_write")
    }

    pub fn get_run(&self, logical_session_id: &str, run_id: &str) -> CrabResult<Option<Run>> {
        ensure_non_empty_field("run_get_validate", "logical_session_id", logical_session_id)?;
        ensure_non_empty_field("run_get_validate", "run_id", run_id)?;
        self.ensure_layout()?;

        let run_path = self.run_path(logical_session_id, run_id);
        let maybe_run = read_json_with_backup::<Run>(&run_path, "run_read")?;
        if let Some(run) = maybe_run {
            if run.logical_session_id != logical_session_id || run.id != run_id {
                return Err(CrabError::InvariantViolation {
                    context: "run_get_identity_mismatch",
                    message: format!(
                        "run {}/{} found at requested {}/{}",
                        run.logical_session_id, run.id, logical_session_id, run_id
                    ),
                });
            }
            return Ok(Some(run));
        }
        Ok(None)
    }

    pub fn list_run_ids(&self, logical_session_id: &str) -> CrabResult<Vec<String>> {
        ensure_non_empty_field(
            "run_list_validate",
            "logical_session_id",
            logical_session_id,
        )?;
        self.ensure_layout()?;

        let session_dir = self.session_runs_dir(logical_session_id);
        if !session_dir.exists() {
            return Ok(Vec::new());
        }

        let entries = wrap_io(
            fs::read_dir(&session_dir),
            "run_list_read_dir",
            &session_dir,
        )?;
        let mut run_ids = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension() != Some(OsStr::new("json")) {
                continue;
            }

            let maybe_run = read_json_with_backup::<Run>(&path, "run_list_read")?;
            let Some(run) = maybe_run else {
                continue;
            };
            if run.logical_session_id != logical_session_id {
                return Err(CrabError::InvariantViolation {
                    context: "run_list_session_mismatch",
                    message: format!(
                        "run {} belongs to {}, expected {}",
                        run.id, run.logical_session_id, logical_session_id
                    ),
                });
            }
            run_ids.push(run.id);
        }

        run_ids.sort();
        Ok(run_ids)
    }

    fn ensure_layout(&self) -> CrabResult<()> {
        wrap_io(
            fs::create_dir_all(&self.root),
            "run_store_layout",
            &self.root,
        )?;
        let runs_root = self.runs_root();
        wrap_io(
            fs::create_dir_all(&runs_root),
            "run_store_layout",
            &runs_root,
        )?;
        Ok(())
    }

    fn runs_root(&self) -> PathBuf {
        self.root.join(RUNS_DIR_NAME)
    }

    fn session_runs_dir(&self, logical_session_id: &str) -> PathBuf {
        self.runs_root()
            .join(hex_encode(logical_session_id.as_bytes()))
    }

    fn run_path(&self, logical_session_id: &str, run_id: &str) -> PathBuf {
        self.session_runs_dir(logical_session_id)
            .join(format!("{}.json", hex_encode(run_id.as_bytes())))
    }
}

/// Clamp completed_at to be >= started_at >= queued_at. NTP clock jitter on
/// very short runs (< 1 second) can cause sub-millisecond backward jumps. This
/// is harmless but would previously crash the daemon via validate_run.
fn clamp_run_timestamps(run: &mut Run) {
    if let Some(started_at) = run.started_at_epoch_ms {
        if started_at < run.queued_at_epoch_ms {
            eprintln!(
                "[store] clamping started_at ({started_at}) to queued_at ({})",
                run.queued_at_epoch_ms
            );
            run.started_at_epoch_ms = Some(run.queued_at_epoch_ms);
        }
    }
    if let Some(completed_at) = run.completed_at_epoch_ms {
        if completed_at < run.queued_at_epoch_ms {
            eprintln!(
                "[store] clamping completed_at ({completed_at}) to queued_at ({})",
                run.queued_at_epoch_ms
            );
            run.completed_at_epoch_ms = Some(run.queued_at_epoch_ms);
        }
        if let Some(started_at) = run.started_at_epoch_ms {
            if run.completed_at_epoch_ms.unwrap_or(0) < started_at {
                eprintln!(
                    "[store] clamping completed_at ({}) to started_at ({started_at})",
                    run.completed_at_epoch_ms.unwrap_or(0)
                );
                run.completed_at_epoch_ms = Some(started_at);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use crab_core::{
        BackendKind, CrabError, InferenceProfile, OwnerProfileMetadata, ReasoningLevel, Run,
    };

    use crate::helpers::hex_encode;
    use crate::test_support::{
        assert_io_context, cleanup, root_as_file, run_path, sample_run, temp_root,
    };

    use super::{clamp_run_timestamps, RunStore};

    #[test]
    fn run_store_upsert_get_and_list_round_trip() {
        let root = temp_root("run-round-trip");
        let store = RunStore::new(&root);
        let first = sample_run("discord:channel:runs", "run-a");
        let second = sample_run("discord:channel:runs", "run-b");

        store
            .upsert_run(&first)
            .expect("first upsert should succeed");
        store
            .upsert_run(&second)
            .expect("second upsert should succeed");

        let loaded = store
            .get_run("discord:channel:runs", "run-a")
            .expect("run lookup should succeed")
            .expect("run should exist");
        assert_eq!(loaded, first);

        let run_ids = store
            .list_run_ids("discord:channel:runs")
            .expect("run listing should succeed");
        assert_eq!(run_ids, vec!["run-a".to_string(), "run-b".to_string()]);

        cleanup(&root);
    }

    #[test]
    fn run_store_get_missing_run_returns_none() {
        let root = temp_root("run-missing");
        let store = RunStore::new(&root);
        let loaded = store
            .get_run("discord:channel:runs", "missing")
            .expect("missing lookup should not error");
        assert!(loaded.is_none());
        cleanup(&root);
    }

    #[test]
    fn run_store_list_missing_session_is_empty() {
        let root = temp_root("run-list-missing");
        let store = RunStore::new(&root);
        let run_ids = store
            .list_run_ids("discord:channel:runs")
            .expect("listing missing session should not fail");
        assert!(run_ids.is_empty());
        cleanup(&root);
    }

    #[test]
    fn run_store_rejects_invalid_run_metadata() {
        let root = temp_root("run-invalid");
        let store = RunStore::new(&root);

        let mut missing_id = sample_run("discord:channel:runs", "run-0");
        missing_id.id = " ".to_string();
        let error = store
            .upsert_run(&missing_id)
            .expect_err("blank run id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "id must not be empty".to_string(),
            }
        );

        let mut missing_session = sample_run("discord:channel:runs", "run-0b");
        missing_session.logical_session_id = " ".to_string();
        let error = store
            .upsert_run(&missing_session)
            .expect_err("blank logical_session_id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "logical_session_id must not be empty".to_string(),
            }
        );

        let mut missing_user_input = sample_run("discord:channel:runs", "run-0c");
        missing_user_input.user_input = " ".to_string();
        let error = store
            .upsert_run(&missing_user_input)
            .expect_err("blank user_input should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "user_input must not be empty".to_string(),
            }
        );

        let mut missing_resolved_model = sample_run("discord:channel:runs", "run-0d");
        missing_resolved_model.profile.resolved_profile.model = " ".to_string();
        let error = store
            .upsert_run(&missing_resolved_model)
            .expect_err("blank resolved model should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.resolved_profile.model must not be empty".to_string(),
            }
        );

        let mut missing_requested_model = sample_run("discord:channel:runs", "run-1");
        missing_requested_model.profile.requested_profile = Some(InferenceProfile {
            backend: BackendKind::Claude,
            model: " ".to_string(),
            reasoning_level: ReasoningLevel::Low,
        });
        let error = store
            .upsert_run(&missing_requested_model)
            .expect_err("blank requested model should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.requested_profile.model must not be empty".to_string(),
            }
        );

        let mut missing_fallback_note = sample_run("discord:channel:runs", "run-2");
        missing_fallback_note.profile.fallback_applied = true;
        missing_fallback_note.profile.fallback_notes.clear();
        let error = store
            .upsert_run(&missing_fallback_note)
            .expect_err("fallback note should be required");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.fallback_notes must not be empty when fallback_applied is true"
                    .to_string(),
            }
        );

        let mut blank_note = sample_run("discord:channel:runs", "run-3");
        blank_note.profile.fallback_notes = vec![" ".to_string()];
        let error = store
            .upsert_run(&blank_note)
            .expect_err("fallback note should not be blank");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.fallback_notes[] must not be empty".to_string(),
            }
        );

        let mut missing_sender_id = sample_run("discord:channel:runs", "run-3b");
        missing_sender_id.profile.sender_id = " ".to_string();
        let error = store
            .upsert_run(&missing_sender_id)
            .expect_err("sender id should not be blank");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.sender_id must not be empty".to_string(),
            }
        );

        let mut non_owner_with_owner_profile = sample_run("discord:channel:runs", "run-3c");
        non_owner_with_owner_profile.profile.sender_is_owner = false;
        let error = store
            .upsert_run(&non_owner_with_owner_profile)
            .expect_err("non-owner should not carry owner profile");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message:
                    "profile.resolved_owner_profile must be absent when sender_is_owner is false"
                        .to_string(),
            }
        );

        let mut blank_owner_default_model = sample_run("discord:channel:runs", "run-3d");
        blank_owner_default_model.profile.resolved_owner_profile = blank_owner_default_model
            .profile
            .resolved_owner_profile
            .map(|mut owner_profile| {
                owner_profile.default_model = Some(" ".to_string());
                owner_profile
            });
        let error = store
            .upsert_run(&blank_owner_default_model)
            .expect_err("owner default model should not be blank");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.resolved_owner_profile.default_model must not be empty"
                    .to_string(),
            }
        );

        let mut blank_owner_machine_location = sample_run("discord:channel:runs", "run-3e");
        blank_owner_machine_location.profile.resolved_owner_profile = blank_owner_machine_location
            .profile
            .resolved_owner_profile
            .map(|mut owner_profile| {
                owner_profile.machine_location = Some(" ".to_string());
                owner_profile
            });
        let error = store
            .upsert_run(&blank_owner_machine_location)
            .expect_err("owner machine location should not be blank");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.resolved_owner_profile.machine_location must not be empty"
                    .to_string(),
            }
        );

        let mut blank_owner_machine_timezone = sample_run("discord:channel:runs", "run-3f");
        blank_owner_machine_timezone.profile.resolved_owner_profile = blank_owner_machine_timezone
            .profile
            .resolved_owner_profile
            .map(|mut owner_profile| {
                owner_profile.machine_timezone = Some(" ".to_string());
                owner_profile
            });
        let error = store
            .upsert_run(&blank_owner_machine_timezone)
            .expect_err("owner machine timezone should not be blank");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_validate",
                message: "profile.resolved_owner_profile.machine_timezone must not be empty"
                    .to_string(),
            }
        );

        let mut started_before_queued = sample_run("discord:channel:runs", "run-4");
        started_before_queued.started_at_epoch_ms =
            Some(started_before_queued.queued_at_epoch_ms - 1);
        store
            .upsert_run(&started_before_queued)
            .expect("backward started_at should be clamped, not rejected");

        let mut completed_before_queued = sample_run("discord:channel:runs", "run-5");
        completed_before_queued.completed_at_epoch_ms =
            Some(completed_before_queued.queued_at_epoch_ms - 1);
        store
            .upsert_run(&completed_before_queued)
            .expect("backward completed_at should be clamped, not rejected");

        let mut completed_before_started = sample_run("discord:channel:runs", "run-6");
        completed_before_started.started_at_epoch_ms = Some(1_739_173_200_200);
        completed_before_started.completed_at_epoch_ms = Some(1_739_173_200_199);
        store
            .upsert_run(&completed_before_started)
            .expect("backward completed_at vs started_at should be clamped, not rejected");

        cleanup(&root);
    }

    #[test]
    fn clamp_run_timestamps_fixes_backward_started_at() {
        let mut run = sample_run("discord:channel:clamp", "run-clamp-1");
        run.started_at_epoch_ms = Some(run.queued_at_epoch_ms - 1);
        clamp_run_timestamps(&mut run);
        assert_eq!(run.started_at_epoch_ms, Some(run.queued_at_epoch_ms));
    }

    #[test]
    fn clamp_run_timestamps_fixes_backward_completed_at_vs_queued() {
        let mut run = sample_run("discord:channel:clamp", "run-clamp-2");
        run.started_at_epoch_ms = None;
        run.completed_at_epoch_ms = Some(run.queued_at_epoch_ms - 1);
        clamp_run_timestamps(&mut run);
        assert_eq!(run.completed_at_epoch_ms, Some(run.queued_at_epoch_ms));
    }

    #[test]
    fn clamp_run_timestamps_fixes_backward_completed_at_vs_started() {
        let mut run = sample_run("discord:channel:clamp", "run-clamp-3");
        run.started_at_epoch_ms = Some(run.queued_at_epoch_ms + 200);
        run.completed_at_epoch_ms = Some(run.queued_at_epoch_ms + 199);
        clamp_run_timestamps(&mut run);
        assert_eq!(
            run.completed_at_epoch_ms,
            Some(run.queued_at_epoch_ms + 200)
        );
    }

    #[test]
    fn clamp_run_timestamps_noop_when_ordered() {
        let mut run = sample_run("discord:channel:clamp", "run-clamp-4");
        run.started_at_epoch_ms = Some(run.queued_at_epoch_ms + 1);
        run.completed_at_epoch_ms = Some(run.queued_at_epoch_ms + 2);
        let started_before = run.started_at_epoch_ms;
        let completed_before = run.completed_at_epoch_ms;
        clamp_run_timestamps(&mut run);
        assert_eq!(run.started_at_epoch_ms, started_before);
        assert_eq!(run.completed_at_epoch_ms, completed_before);
    }

    #[test]
    fn run_store_upsert_allows_timestamp_equality_boundaries() {
        let root = temp_root("run-equality-boundaries");
        let store = RunStore::new(&root);

        let mut run = sample_run("discord:channel:runs", "run-eq");
        run.started_at_epoch_ms = Some(run.queued_at_epoch_ms);
        run.completed_at_epoch_ms = Some(run.queued_at_epoch_ms);

        store
            .upsert_run(&run)
            .expect("timestamp equality boundaries should be valid");

        let loaded = store
            .get_run(&run.logical_session_id, &run.id)
            .expect("run read should succeed")
            .expect("run should exist");
        assert_eq!(loaded, run);

        cleanup(&root);
    }

    #[test]
    fn run_store_get_validates_required_inputs() {
        let root = temp_root("run-get-validate");
        let store = RunStore::new(&root);

        let error = store
            .get_run(" ", "run-1")
            .expect_err("blank logical_session_id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_get_validate",
                message: "logical_session_id must not be empty".to_string(),
            }
        );

        let error = store
            .get_run("discord:channel:runs", " ")
            .expect_err("blank run_id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_get_validate",
                message: "run_id must not be empty".to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn run_store_get_propagates_run_read_error() {
        let root = temp_root("run-get-read-error");
        let store = RunStore::new(&root);
        let path = run_path(&root, "discord:channel:runs", "run-1");
        fs::create_dir_all(&path).expect("test should create directory at run file path");

        let error = store
            .get_run("discord:channel:runs", "run-1")
            .expect_err("directory at run path should fail read");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "run_read",
                path: Some(ref path_value),
                ..
            } if path_value == &path.display().to_string()
        ));

        cleanup(&root);
    }

    #[test]
    fn run_store_get_rejects_identity_mismatch() {
        let mut tampered = sample_run("discord:channel:other", "run-mismatch");
        tampered.id = "run-unexpected".to_string();
        assert_run_get_identity_error(
            "run-identity-mismatch",
            tampered,
            "mismatched run identity should fail",
            "run discord:channel:other/run-unexpected found at requested discord:channel:runs/run-1",
        );
    }

    #[test]
    fn run_store_get_rejects_session_mismatch_even_when_run_id_matches() {
        let tampered = sample_run("discord:channel:other", "run-1");
        assert_run_get_identity_error(
            "run-session-mismatch-only",
            tampered,
            "mismatched run session should fail",
            "run discord:channel:other/run-1 found at requested discord:channel:runs/run-1",
        );
    }

    #[test]
    fn run_store_get_rejects_run_id_mismatch_even_when_session_matches() {
        let mut tampered = sample_run("discord:channel:runs", "run-expected");
        tampered.id = "run-unexpected".to_string();
        assert_run_get_identity_error(
            "run-id-mismatch-only",
            tampered,
            "mismatched run id should fail",
            "run discord:channel:runs/run-unexpected found at requested discord:channel:runs/run-1",
        );
    }

    #[test]
    fn run_store_get_recovers_from_backup() {
        let root = temp_root("run-backup-recovery");
        let store = RunStore::new(&root);
        let first = sample_run("discord:channel:runs", "run-1");
        let mut second = sample_run("discord:channel:runs", "run-1");
        second.profile.fallback_applied = true;
        second.profile.fallback_notes = vec!["swapped".to_string()];
        store
            .upsert_run(&first)
            .expect("first run write should succeed");
        store
            .upsert_run(&second)
            .expect("second run write should succeed");

        let path = run_path(&root, "discord:channel:runs", "run-1");
        fs::write(&path, b"{ bad json").expect("test should corrupt primary run file");

        let loaded = store
            .get_run("discord:channel:runs", "run-1")
            .expect("backup recovery should succeed")
            .expect("run should exist");
        assert_eq!(loaded, first);

        cleanup(&root);
    }

    #[test]
    fn run_store_list_rejects_session_mismatch() {
        let root = temp_root("run-list-session-mismatch");
        let store = RunStore::new(&root);
        let path = run_path(&root, "discord:channel:runs", "run-1");
        write_run_fixture(&path, &sample_run("discord:channel:other", "run-1"));

        let error = store
            .list_run_ids("discord:channel:runs")
            .expect_err("mismatched session should fail list");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_list_session_mismatch",
                message:
                    "run run-1 belongs to discord:channel:other, expected discord:channel:runs"
                        .to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn run_store_operations_propagate_layout_errors() {
        let root = root_as_file("run-layout-errors");
        let store = RunStore::new(&root);

        let upsert_error = store
            .upsert_run(&sample_run("discord:channel:runs", "run-1"))
            .expect_err("layout failure should bubble from upsert");
        assert_io_context(upsert_error, "run_store_layout");

        let get_error = store
            .get_run("discord:channel:runs", "run-1")
            .expect_err("layout failure should bubble from get");
        assert_io_context(get_error, "run_store_layout");

        let list_error = store
            .list_run_ids("discord:channel:runs")
            .expect_err("layout failure should bubble from list");
        assert_io_context(list_error, "run_store_layout");

        let _ = fs::remove_file(&root);
    }

    #[test]
    fn run_store_operations_propagate_runs_root_layout_errors() {
        let root = temp_root("run-runs-root-layout-error");
        fs::write(root.join("runs"), b"not-a-directory").expect("test should block runs root path");
        let store = RunStore::new(&root);

        let upsert_error = store
            .upsert_run(&sample_run("discord:channel:runs", "run-1"))
            .expect_err("runs root path as file should fail layout creation");
        assert_io_context(upsert_error, "run_store_layout");

        let get_error = store
            .get_run("discord:channel:runs", "run-1")
            .expect_err("runs root path as file should fail layout creation");
        assert_io_context(get_error, "run_store_layout");

        let list_error = store
            .list_run_ids("discord:channel:runs")
            .expect_err("runs root path as file should fail layout creation");
        assert_io_context(list_error, "run_store_layout");

        cleanup(&root);
    }

    #[test]
    fn run_store_list_validates_required_inputs() {
        let root = temp_root("run-list-validate");
        let store = RunStore::new(&root);

        let error = store
            .list_run_ids(" ")
            .expect_err("blank logical_session_id should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_list_validate",
                message: "logical_session_id must not be empty".to_string(),
            }
        );

        cleanup(&root);
    }

    #[test]
    fn run_store_list_propagates_read_dir_error() {
        let root = temp_root("run-list-read-dir-error");
        let store = RunStore::new(&root);
        let session_dir = root
            .join("runs")
            .join(hex_encode("discord:channel:runs".as_bytes()));
        fs::create_dir_all(root.join("runs")).expect("test should create runs root");
        fs::write(&session_dir, b"not-a-directory")
            .expect("test should block session runs directory path");

        let error = store
            .list_run_ids("discord:channel:runs")
            .expect_err("session runs path as file should fail read_dir");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "run_list_read_dir",
                path: Some(ref path_value),
                ..
            } if path_value == &session_dir.display().to_string()
        ));

        cleanup(&root);
    }

    #[test]
    fn run_store_list_propagates_run_entry_read_error() {
        let root = temp_root("run-list-read-entry-error");
        let store = RunStore::new(&root);
        let path = run_path(&root, "discord:channel:runs", "run-1");
        fs::create_dir_all(path.parent().expect("run path should have parent"))
            .expect("test should create run parent");
        fs::create_dir_all(&path).expect("test should create directory with json extension");

        let error = store
            .list_run_ids("discord:channel:runs")
            .expect_err("directory entry with json extension should fail read");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "run_list_read",
                path: Some(ref path_value),
                ..
            } if path_value == &path.display().to_string()
        ));

        cleanup(&root);
    }

    #[test]
    fn run_store_list_ignores_non_json_entries() {
        let (root, store, session_dir) = seeded_run_listing_fixture("run-list-ignore-non-json");
        fs::write(session_dir.join("note.txt"), b"ignore")
            .expect("test should create non-json entry");

        let run_ids = store
            .list_run_ids("discord:channel:runs")
            .expect("listing should ignore non-json files");
        assert_eq!(run_ids, vec!["run-1".to_string()]);

        cleanup(&root);
    }

    #[cfg(unix)]
    #[test]
    fn run_store_list_ignores_broken_json_symlink_entries() {
        let (root, store, session_dir) = seeded_run_listing_fixture("run-list-broken-symlink");
        let symlink_path = session_dir.join("broken.json");
        std::os::unix::fs::symlink(session_dir.join("missing-target.json"), &symlink_path)
            .expect("test should create broken symlink");

        let run_ids = store
            .list_run_ids("discord:channel:runs")
            .expect("listing should ignore broken symlinks");
        assert_eq!(run_ids, vec!["run-1".to_string()]);

        cleanup(&root);
    }

    #[test]
    fn run_store_accepts_optional_profile_and_timestamps() {
        let root = temp_root("run-optional-fields");
        let store = RunStore::new(&root);

        let mut sparse = sample_run("discord:channel:runs", "run-optional-1");
        sparse.profile.requested_profile = None;
        sparse.profile.fallback_applied = false;
        sparse.profile.fallback_notes.clear();
        sparse.started_at_epoch_ms = None;
        sparse.completed_at_epoch_ms = None;
        store
            .upsert_run(&sparse)
            .expect("run with optional profile/timestamps should be accepted");

        let mut completed_without_started = sample_run("discord:channel:runs", "run-optional-2");
        completed_without_started.profile.requested_profile = None;
        completed_without_started.profile.fallback_applied = false;
        completed_without_started.profile.fallback_notes.clear();
        completed_without_started.started_at_epoch_ms = None;
        completed_without_started.completed_at_epoch_ms =
            Some(completed_without_started.queued_at_epoch_ms + 1);
        store
            .upsert_run(&completed_without_started)
            .expect("completed timestamp should be accepted without started timestamp");

        cleanup(&root);
    }

    #[test]
    fn run_store_accepts_non_owner_sender_without_owner_profile() {
        let root = temp_root("run-non-owner-sender-context");
        let store = RunStore::new(&root);

        let mut run = sample_run("discord:channel:runs", "run-sender-1");
        run.profile.sender_is_owner = false;
        run.profile.resolved_owner_profile = None;
        store
            .upsert_run(&run)
            .expect("non-owner sender context without owner profile should be accepted");

        cleanup(&root);
    }

    #[test]
    fn run_store_accepts_owner_profile_with_optional_fields_absent() {
        let root = temp_root("run-owner-profile-optional-fields");
        let store = RunStore::new(&root);

        let mut run = sample_run("discord:channel:runs", "run-sender-2");
        run.profile.resolved_owner_profile = Some(OwnerProfileMetadata {
            machine_location: None,
            machine_timezone: None,
            default_backend: None,
            default_model: None,
            default_reasoning_level: None,
        });
        store
            .upsert_run(&run)
            .expect("owner profile should allow optional metadata fields to be absent");

        cleanup(&root);
    }

    fn write_run_fixture(path: &Path, run: &Run) {
        fs::create_dir_all(path.parent().expect("run path should have parent"))
            .expect("test should create run parent directory");
        fs::write(
            path,
            serde_json::to_vec_pretty(run).expect("run should serialize"),
        )
        .expect("test should write run file");
    }

    fn assert_run_get_identity_error(
        root_label: &str,
        tampered: Run,
        expected_reason: &str,
        expected_message: &str,
    ) {
        let root = temp_root(root_label);
        let store = RunStore::new(&root);
        let path = run_path(&root, "discord:channel:runs", "run-1");
        write_run_fixture(&path, &tampered);

        let error = store
            .get_run("discord:channel:runs", "run-1")
            .expect_err(expected_reason);
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "run_get_identity_mismatch",
                message: expected_message.to_string(),
            }
        );

        cleanup(&root);
    }

    fn seeded_run_listing_fixture(root_label: &str) -> (PathBuf, RunStore, PathBuf) {
        let root = temp_root(root_label);
        let store = RunStore::new(&root);
        store
            .upsert_run(&sample_run("discord:channel:runs", "run-1"))
            .expect("run write should succeed");
        let session_dir = run_path(&root, "discord:channel:runs", "run-1")
            .parent()
            .expect("run path should have parent")
            .to_path_buf();

        (root, store, session_dir)
    }
}
