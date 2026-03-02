use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use crate::onboarding::{OnboardingCaptureDocument, ONBOARDING_SCHEMA_VERSION};
use crate::validation::validate_non_empty_text;
use crate::workspace::{BOOTSTRAP_FILE_NAME, MEMORY_FILE_NAME};
use crate::{CrabError, CrabResult, EventEnvelope, EventKind, EventSource, RunProfileTelemetry};

const ONBOARDING_COMPLETION_CONTEXT: &str = "onboarding_completion";

pub const ONBOARDING_MEMORY_BASELINE_START_MARKER: &str = "<!-- CRAB:ONBOARDING_BASELINE:START -->";
pub const ONBOARDING_MEMORY_BASELINE_END_MARKER: &str = "<!-- CRAB:ONBOARDING_BASELINE:END -->";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnboardingCompletionInput {
    pub logical_session_id: String,
    pub run_id: String,
    pub onboarding_session_id: String,
    pub completed_at_epoch_ms: u64,
    pub capture: OnboardingCaptureDocument,
    pub profile: Option<RunProfileTelemetry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnboardingCompletionOutcome {
    pub memory_path: String,
    pub bootstrap_retired: bool,
    pub emitted_event: EventEnvelope,
}

pub trait OnboardingCompletionEventRuntime {
    fn next_event_sequence(&self, logical_session_id: &str, run_id: &str) -> CrabResult<u64>;
    fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()>;
}

pub fn execute_onboarding_completion_protocol<R: OnboardingCompletionEventRuntime>(
    runtime: &mut R,
    workspace_root: &Path,
    input: &OnboardingCompletionInput,
) -> CrabResult<OnboardingCompletionOutcome> {
    validate_completion_input(workspace_root, input)?;

    let memory_path = workspace_root.join(MEMORY_FILE_NAME);
    let existing_memory = read_existing_regular_file(&memory_path)?;
    let managed_baseline_block = render_memory_baseline_block(input);
    let merged_memory = merge_memory_document(existing_memory.as_deref(), &managed_baseline_block)?;
    write_document(&memory_path, &merged_memory)?;

    let bootstrap_path = workspace_root.join(BOOTSTRAP_FILE_NAME);
    let bootstrap_retired = retire_bootstrap_file(&bootstrap_path)?;

    let sequence = runtime.next_event_sequence(&input.logical_session_id, &input.run_id)?;
    if sequence == 0 {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: "next event sequence must be greater than 0".to_string(),
        });
    }

    let event = build_completion_event(input, &memory_path, bootstrap_retired, sequence);
    runtime.append_event(&event)?;

    Ok(OnboardingCompletionOutcome {
        memory_path: display_path(&memory_path),
        bootstrap_retired,
        emitted_event: event,
    })
}

fn validate_completion_input(
    workspace_root: &Path,
    input: &OnboardingCompletionInput,
) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let workspace_metadata = fs::metadata(workspace_root).map_err(|error| CrabError::Io {
        context: ONBOARDING_COMPLETION_CONTEXT,
        path: Some(display_path(workspace_root)),
        message: error.to_string(),
    })?;

    if !workspace_metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: format!("{} must be a directory", display_path(workspace_root)),
        });
    }

    validate_non_empty_text(
        ONBOARDING_COMPLETION_CONTEXT,
        "logical_session_id",
        &input.logical_session_id,
    )?;
    validate_non_empty_text(ONBOARDING_COMPLETION_CONTEXT, "run_id", &input.run_id)?;
    validate_non_empty_text(
        ONBOARDING_COMPLETION_CONTEXT,
        "onboarding_session_id",
        &input.onboarding_session_id,
    )?;

    if input.completed_at_epoch_ms == 0 {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: "completed_at_epoch_ms must be greater than 0".to_string(),
        });
    }

    validate_capture_document(&input.capture)
}

fn validate_capture_document(capture: &OnboardingCaptureDocument) -> CrabResult<()> {
    validate_non_empty_text(
        ONBOARDING_COMPLETION_CONTEXT,
        "capture.schema_version",
        &capture.schema_version,
    )?;
    if capture.schema_version != ONBOARDING_SCHEMA_VERSION {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: format!(
                "capture.schema_version must be {:?}, got {:?}",
                ONBOARDING_SCHEMA_VERSION, capture.schema_version
            ),
        });
    }

    let required_fields = [
        ("capture.agent_identity", capture.agent_identity.as_str()),
        ("capture.owner_identity", capture.owner_identity.as_str()),
        (
            "capture.machine_location",
            capture.machine_location.as_str(),
        ),
        (
            "capture.machine_timezone",
            capture.machine_timezone.as_str(),
        ),
    ];
    for (field_name, field_value) in required_fields {
        validate_non_empty_text(ONBOARDING_COMPLETION_CONTEXT, field_name, field_value)?;
    }

    if capture.primary_goals.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: "capture.primary_goals must contain at least one goal".to_string(),
        });
    }

    for goal in &capture.primary_goals {
        validate_non_empty_text(
            ONBOARDING_COMPLETION_CONTEXT,
            "capture.primary_goals[]",
            goal,
        )?;
    }

    Ok(())
}

fn read_existing_regular_file(path: &Path) -> CrabResult<Option<String>> {
    if path.exists() && !path.is_file() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: format!("{} must be a regular file", display_path(path)),
        });
    }

    match fs::read_to_string(path) {
        Ok(content) => Ok(Some(content)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CrabError::Io {
            context: ONBOARDING_COMPLETION_CONTEXT,
            path: Some(display_path(path)),
            message: error.to_string(),
        }),
    }
}

fn write_document(path: &Path, content: &str) -> CrabResult<()> {
    fs::write(path, content).map_err(|error| CrabError::Io {
        context: ONBOARDING_COMPLETION_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn retire_bootstrap_file(bootstrap_path: &Path) -> CrabResult<bool> {
    match fs::metadata(bootstrap_path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(CrabError::InvariantViolation {
                    context: ONBOARDING_COMPLETION_CONTEXT,
                    message: format!("{} must be a regular file", display_path(bootstrap_path)),
                });
            }
            fs::remove_file(bootstrap_path).map_err(|error| CrabError::Io {
                context: ONBOARDING_COMPLETION_CONTEXT,
                path: Some(display_path(bootstrap_path)),
                message: error.to_string(),
            })?;
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(CrabError::Io {
            context: ONBOARDING_COMPLETION_CONTEXT,
            path: Some(display_path(bootstrap_path)),
            message: error.to_string(),
        }),
    }
}

fn merge_memory_document(existing: Option<&str>, managed_block: &str) -> CrabResult<String> {
    let fallback_document = "# MEMORY.md\n".to_string();
    let source = existing.unwrap_or(fallback_document.as_str());

    let start_index = source.find(ONBOARDING_MEMORY_BASELINE_START_MARKER);
    let end_index = source.find(ONBOARDING_MEMORY_BASELINE_END_MARKER);

    match (start_index, end_index) {
        (None, None) => {
            let mut merged = source.trim_end().to_string();
            if merged.trim().is_empty() {
                merged = "# MEMORY.md".to_string();
            }
            merged.push_str("\n\n");
            merged.push_str(ONBOARDING_MEMORY_BASELINE_START_MARKER);
            merged.push('\n');
            merged.push_str(managed_block.trim_end());
            merged.push('\n');
            merged.push_str(ONBOARDING_MEMORY_BASELINE_END_MARKER);
            merged.push('\n');
            Ok(merged)
        }
        (Some(_), None) => Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: format!(
                "{} exists in MEMORY.md but {} is missing",
                ONBOARDING_MEMORY_BASELINE_START_MARKER, ONBOARDING_MEMORY_BASELINE_END_MARKER
            ),
        }),
        (None, Some(_)) => Err(CrabError::InvariantViolation {
            context: ONBOARDING_COMPLETION_CONTEXT,
            message: format!(
                "{} exists in MEMORY.md but {} is missing",
                ONBOARDING_MEMORY_BASELINE_END_MARKER, ONBOARDING_MEMORY_BASELINE_START_MARKER
            ),
        }),
        (Some(start), Some(end)) => {
            if end < start {
                return Err(CrabError::InvariantViolation {
                    context: ONBOARDING_COMPLETION_CONTEXT,
                    message: format!(
                        "{} appears before {} in MEMORY.md",
                        ONBOARDING_MEMORY_BASELINE_END_MARKER,
                        ONBOARDING_MEMORY_BASELINE_START_MARKER
                    ),
                });
            }

            let managed_start = start + ONBOARDING_MEMORY_BASELINE_START_MARKER.len();
            let managed_end = end;
            let mut merged = String::new();
            merged.push_str(&source[..managed_start]);
            merged.push('\n');
            merged.push_str(managed_block.trim_end());
            merged.push('\n');
            merged.push_str(&source[managed_end..]);
            if !merged.ends_with('\n') {
                merged.push('\n');
            }
            Ok(merged)
        }
    }
}

fn render_memory_baseline_block(input: &OnboardingCompletionInput) -> String {
    let goals = input
        .capture
        .primary_goals
        .iter()
        .enumerate()
        .map(|(index, goal)| format!("{}. {}", index + 1, goal))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "## Managed Onboarding Baseline\n- Onboarding session id: {}\n- Completed at epoch ms: {}\n- Schema version: {}\n- Agent identity: {}\n- Owner identity: {}\n- Machine location: {}\n- Machine timezone: {}\n- Primary goals:\n{}",
        input.onboarding_session_id,
        input.completed_at_epoch_ms,
        input.capture.schema_version,
        input.capture.agent_identity,
        input.capture.owner_identity,
        input.capture.machine_location,
        input.capture.machine_timezone,
        goals
    )
}

fn build_completion_event(
    input: &OnboardingCompletionInput,
    memory_path: &Path,
    bootstrap_retired: bool,
    sequence: u64,
) -> EventEnvelope {
    let backend = input
        .profile
        .as_ref()
        .map(|profile| profile.resolved_profile.backend);
    let resolved_model = input
        .profile
        .as_ref()
        .map(|profile| profile.resolved_profile.model.clone());
    let resolved_reasoning_level = input.profile.as_ref().map(|profile| {
        profile
            .resolved_profile
            .reasoning_level
            .as_token()
            .to_string()
    });
    let profile_source = input
        .profile
        .as_ref()
        .map(|profile| profile.profile_source_token().to_string());

    let payload = BTreeMap::from([
        ("event".to_string(), "bootstrap_completed".to_string()),
        (
            "onboarding_session_id".to_string(),
            input.onboarding_session_id.clone(),
        ),
        ("memory_path".to_string(), display_path(memory_path)),
        (
            "bootstrap_retired".to_string(),
            bootstrap_retired.to_string(),
        ),
    ]);

    EventEnvelope {
        event_id: format!(
            "evt:onboarding-complete:{}:{}:{}",
            input.logical_session_id, input.run_id, sequence
        ),
        run_id: input.run_id.clone(),
        turn_id: Some(format!("turn:{}", input.run_id)),
        lane_id: Some(input.logical_session_id.clone()),
        logical_session_id: input.logical_session_id.clone(),
        physical_session_id: None,
        backend,
        resolved_model,
        resolved_reasoning_level,
        profile_source,
        sequence,
        emitted_at_epoch_ms: input.completed_at_epoch_ms,
        source: EventSource::System,
        kind: EventKind::RunNote,
        payload,
        profile: input.profile.clone(),
        idempotency_key: Some(format!(
            "onboarding-complete:{}:{}",
            input.logical_session_id, input.onboarding_session_id
        )),
    }
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::{
        BackendKind, CrabResult, InferenceProfile, ProfileValueSource, ReasoningLevel,
        RunProfileTelemetry,
    };

    use super::{
        execute_onboarding_completion_protocol, CrabError, EventEnvelope,
        OnboardingCaptureDocument, OnboardingCompletionEventRuntime, OnboardingCompletionInput,
        ONBOARDING_MEMORY_BASELINE_END_MARKER, ONBOARDING_MEMORY_BASELINE_START_MARKER,
    };
    use crate::workspace::{BOOTSTRAP_FILE_NAME, MEMORY_FILE_NAME};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    #[derive(Debug, Clone)]
    struct FakeRuntime {
        next_sequence_results: VecDeque<CrabResult<u64>>,
        append_results: VecDeque<CrabResult<()>>,
        appended_events: Vec<EventEnvelope>,
    }

    impl FakeRuntime {
        fn successful() -> Self {
            Self {
                next_sequence_results: VecDeque::from(vec![Ok(1)]),
                append_results: VecDeque::from(vec![Ok(())]),
                appended_events: Vec::new(),
            }
        }
    }

    impl OnboardingCompletionEventRuntime for FakeRuntime {
        fn next_event_sequence(&self, _logical_session_id: &str, _run_id: &str) -> CrabResult<u64> {
            self.next_sequence_results
                .front()
                .cloned()
                .unwrap_or_else(|| {
                    Err(CrabError::InvariantViolation {
                        context: "fake_onboarding_completion_runtime",
                        message: "missing scripted sequence result".to_string(),
                    })
                })
        }

        fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()> {
            match self.append_results.pop_front() {
                Some(Ok(())) => {
                    self.appended_events.push(event.clone());
                    if !self.next_sequence_results.is_empty() {
                        self.next_sequence_results.pop_front();
                    }
                    Ok(())
                }
                Some(Err(error)) => Err(error),
                None => Err(CrabError::InvariantViolation {
                    context: "fake_onboarding_completion_runtime",
                    message: "missing scripted append result".to_string(),
                }),
            }
        }
    }

    fn with_temp_workspace<T>(label: &str, test_fn: impl FnOnce(&Path) -> T) -> T {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let workspace_path = std::env::temp_dir().join(format!(
            "crab-onboarding-completion-{label}-{suffix}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&workspace_path);
        let result = test_fn(&workspace_path);
        let _ = fs::remove_dir_all(&workspace_path);
        result
    }

    fn capture() -> OnboardingCaptureDocument {
        OnboardingCaptureDocument {
            schema_version: "v1".to_string(),
            agent_identity: "Crab".to_string(),
            owner_identity: "Henry".to_string(),
            primary_goals: vec![
                "Keep quality at 100%".to_string(),
                "Ship the harness".to_string(),
            ],
            machine_location: "Paris, France".to_string(),
            machine_timezone: "Europe/Paris".to_string(),
        }
    }

    fn input() -> OnboardingCompletionInput {
        OnboardingCompletionInput {
            logical_session_id: "discord:channel:1".to_string(),
            run_id: "run-1".to_string(),
            onboarding_session_id: "onboarding-1".to_string(),
            completed_at_epoch_ms: 1_739_173_200_000,
            capture: capture(),
            profile: Some(sample_run_profile_telemetry()),
        }
    }

    fn sample_run_profile_telemetry() -> RunProfileTelemetry {
        RunProfileTelemetry {
            requested_profile: None,
            resolved_profile: InferenceProfile {
                backend: BackendKind::Claude,
                model: "claude-opus-4-6".to_string(),
                reasoning_level: ReasoningLevel::Medium,
            },
            backend_source: ProfileValueSource::GlobalDefault,
            model_source: ProfileValueSource::GlobalDefault,
            reasoning_level_source: ProfileValueSource::GlobalDefault,
            fallback_applied: false,
            fallback_notes: Vec::new(),
            sender_id: "111111111111111111".to_string(),
            sender_is_owner: true,
            resolved_owner_profile: None,
        }
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path)
            .expect("metadata should be readable")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("permissions should be writable");
    }

    fn assert_io_error_path(error: CrabError, expected_path: &Path) {
        assert!(matches!(
            error,
            CrabError::Io {
                context: "onboarding_completion",
                path: Some(path),
                ..
            } if path == expected_path.to_string_lossy()
        ));
    }

    fn assert_invariant_error(result: CrabResult<impl std::fmt::Debug>, expected_message: &str) {
        let error = result.expect_err("expected invariant violation");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "onboarding_completion",
                message: expected_message.to_string(),
            }
        );
    }

    #[test]
    fn merge_memory_document_rehydrates_blank_source() {
        let merged = super::merge_memory_document(Some(" \n"), "## Baseline")
            .expect("blank source should render fallback memory header");
        assert!(merged.starts_with("# MEMORY.md\n\n"));
        assert!(merged.contains("## Baseline"));
        assert!(merged.contains(ONBOARDING_MEMORY_BASELINE_START_MARKER));
        assert!(merged.contains(ONBOARDING_MEMORY_BASELINE_END_MARKER));
    }

    #[test]
    fn fake_runtime_append_requires_scripted_result() {
        let mut runtime = FakeRuntime {
            next_sequence_results: VecDeque::from(vec![Ok(1)]),
            append_results: VecDeque::new(),
            appended_events: Vec::new(),
        };
        let error = runtime
            .append_event(&sample_event())
            .expect_err("missing append script should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "fake_onboarding_completion_runtime",
                message: "missing scripted append result".to_string(),
            }
        );
    }

    #[test]
    fn fake_runtime_append_succeeds_when_sequence_queue_is_empty() {
        let mut runtime = FakeRuntime {
            next_sequence_results: VecDeque::new(),
            append_results: VecDeque::from(vec![Ok(())]),
            appended_events: Vec::new(),
        };

        runtime
            .append_event(&sample_event())
            .expect("append should succeed");

        assert_eq!(runtime.appended_events.len(), 1);
        assert!(runtime.next_sequence_results.is_empty());
    }

    fn sample_event() -> EventEnvelope {
        EventEnvelope {
            event_id: "evt-1".to_string(),
            run_id: "run-1".to_string(),
            turn_id: Some("turn:run-1".to_string()),
            lane_id: Some("discord:channel:1".to_string()),
            logical_session_id: "discord:channel:1".to_string(),
            physical_session_id: Some("physical-1".to_string()),
            backend: Some(BackendKind::Claude),
            resolved_model: Some("claude-opus-4-6".to_string()),
            resolved_reasoning_level: Some("medium".to_string()),
            profile_source: Some("global_default".to_string()),
            sequence: 1,
            emitted_at_epoch_ms: 1,
            source: crate::EventSource::System,
            kind: crate::EventKind::RunNote,
            payload: std::collections::BTreeMap::new(),
            profile: None,
            idempotency_key: None,
        }
    }

    #[cfg(unix)]
    #[test]
    fn helper_io_error_paths_are_exercised() {
        with_temp_workspace("helper-io-errors", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");

            let unreadable_file = workspace.join("unreadable.md");
            fs::write(&unreadable_file, "secret").expect("file should be writable");
            set_mode(&unreadable_file, 0o000);
            let read_error = super::read_existing_regular_file(&unreadable_file)
                .expect_err("unreadable file should fail");
            assert_io_error_path(read_error, &unreadable_file);
            set_mode(&unreadable_file, 0o644);

            let write_protected_file = workspace.join("write-protected.md");
            fs::write(&write_protected_file, "existing").expect("file should be writable");
            set_mode(&write_protected_file, 0o400);
            let write_error = super::write_document(&write_protected_file, "new content")
                .expect_err("write-protected file should fail");
            assert_io_error_path(write_error, &write_protected_file);
            set_mode(&write_protected_file, 0o600);

            let removable_bootstrap = workspace.join("removable-bootstrap.md");
            fs::write(&removable_bootstrap, "pending").expect("bootstrap should be writable");
            set_mode(workspace, 0o555);
            let remove_error = super::retire_bootstrap_file(&removable_bootstrap)
                .expect_err("read-only parent directory should fail remove");
            assert_io_error_path(remove_error, &removable_bootstrap);
            set_mode(workspace, 0o755);

            let blocked_parent = workspace.join("blocked");
            fs::create_dir_all(&blocked_parent).expect("blocked directory should be creatable");
            let blocked_bootstrap = blocked_parent.join("missing-bootstrap.md");
            set_mode(&blocked_parent, 0o000);
            let metadata_error = super::retire_bootstrap_file(&blocked_bootstrap)
                .expect_err("permission denied metadata should fail");
            assert_io_error_path(metadata_error, &blocked_bootstrap);
            set_mode(&blocked_parent, 0o755);
        });
    }

    #[cfg(unix)]
    #[test]
    fn completion_protocol_propagates_memory_write_errors() {
        with_temp_workspace("write-error", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");
            fs::write(workspace.join(MEMORY_FILE_NAME), "# MEMORY.md\n")
                .expect("memory file should be writable");
            set_mode(&workspace.join(MEMORY_FILE_NAME), 0o400);

            let mut runtime = FakeRuntime::successful();
            let error = execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                .expect_err("write-protected memory file should fail completion");
            assert_io_error_path(error, &workspace.join(MEMORY_FILE_NAME));
            assert!(workspace.join(BOOTSTRAP_FILE_NAME).exists());
            assert_eq!(runtime.appended_events.len(), 0);

            set_mode(&workspace.join(MEMORY_FILE_NAME), 0o600);
        });
    }

    #[test]
    fn completion_protocol_updates_memory_retires_bootstrap_and_emits_event() {
        with_temp_workspace("happy-path", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(
                workspace.join(MEMORY_FILE_NAME),
                "# MEMORY.md\n\nExisting user note.\n",
            )
            .expect("memory file should be writable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            let outcome = execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                .expect("completion protocol should succeed");

            assert_eq!(
                outcome.memory_path,
                workspace.join(MEMORY_FILE_NAME).to_string_lossy()
            );
            assert!(outcome.bootstrap_retired);
            assert_eq!(outcome.emitted_event.kind, crate::EventKind::RunNote);
            assert_eq!(outcome.emitted_event.source, crate::EventSource::System);
            assert_eq!(
                outcome.emitted_event.turn_id,
                Some("turn:run-1".to_string())
            );
            assert_eq!(
                outcome.emitted_event.lane_id,
                Some("discord:channel:1".to_string())
            );
            assert_eq!(outcome.emitted_event.physical_session_id, None);
            assert_eq!(outcome.emitted_event.backend, Some(BackendKind::Claude));
            assert_eq!(
                outcome.emitted_event.resolved_model,
                Some("claude-opus-4-6".to_string())
            );
            assert_eq!(
                outcome.emitted_event.resolved_reasoning_level,
                Some("medium".to_string())
            );
            assert_eq!(
                outcome.emitted_event.profile_source,
                Some("global_default".to_string())
            );
            assert_eq!(
                outcome
                    .emitted_event
                    .payload
                    .get("event")
                    .expect("event payload should exist"),
                "bootstrap_completed"
            );
            assert_eq!(runtime.appended_events.len(), 1);

            let memory = fs::read_to_string(workspace.join(MEMORY_FILE_NAME))
                .expect("memory should be readable");
            assert!(memory.contains(ONBOARDING_MEMORY_BASELINE_START_MARKER));
            assert!(memory.contains(ONBOARDING_MEMORY_BASELINE_END_MARKER));
            assert!(memory.contains("- Agent identity: Crab"));
            assert!(memory.contains("1. Keep quality at 100%"));
            assert!(memory.contains("Existing user note."));
            assert!(!workspace.join(BOOTSTRAP_FILE_NAME).exists());
        });
    }

    #[test]
    fn completion_protocol_is_retry_safe_after_event_append_failure() {
        with_temp_workspace("retry", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime {
                next_sequence_results: VecDeque::from(vec![Ok(1), Ok(2)]),
                append_results: VecDeque::from(vec![
                    Err(CrabError::InvariantViolation {
                        context: "event_append",
                        message: "boom".to_string(),
                    }),
                    Ok(()),
                ]),
                appended_events: Vec::new(),
            };

            let first_error =
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                    .expect_err("first attempt should fail on append");
            assert_eq!(
                first_error,
                CrabError::InvariantViolation {
                    context: "event_append",
                    message: "boom".to_string(),
                }
            );

            let memory_after_first = fs::read_to_string(workspace.join(MEMORY_FILE_NAME))
                .expect("memory should be written on first attempt");
            assert_eq!(
                memory_after_first
                    .matches(ONBOARDING_MEMORY_BASELINE_START_MARKER)
                    .count(),
                1
            );
            assert!(!workspace.join(BOOTSTRAP_FILE_NAME).exists());

            let second_outcome =
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                    .expect("second attempt should succeed");
            assert!(!second_outcome.bootstrap_retired);
            assert_eq!(runtime.appended_events.len(), 1);

            let memory_after_second = fs::read_to_string(workspace.join(MEMORY_FILE_NAME))
                .expect("memory should be readable");
            assert_eq!(
                memory_after_second
                    .matches(ONBOARDING_MEMORY_BASELINE_START_MARKER)
                    .count(),
                1
            );
        });
    }

    #[test]
    fn completion_protocol_replaces_existing_managed_baseline() {
        with_temp_workspace("replace-managed", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(
                workspace.join(MEMORY_FILE_NAME),
                format!(
                    "# MEMORY.md\n\n{ONBOARDING_MEMORY_BASELINE_START_MARKER}\nOLD\n{ONBOARDING_MEMORY_BASELINE_END_MARKER}\n\nTail note.\n"
                ),
            )
            .expect("memory file should be writable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                .expect("completion should succeed");

            let memory = fs::read_to_string(workspace.join(MEMORY_FILE_NAME))
                .expect("memory should be readable");
            assert!(!memory.contains("\nOLD\n"));
            assert!(memory.contains("Managed Onboarding Baseline"));
            assert!(memory.contains("Tail note."));
        });
    }

    #[test]
    fn completion_protocol_rejects_malformed_memory_markers() {
        with_temp_workspace("malformed-markers", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(
                workspace.join(MEMORY_FILE_NAME),
                format!(
                    "# MEMORY.md\n\n{ONBOARDING_MEMORY_BASELINE_START_MARKER}\nno end marker\n"
                ),
            )
            .expect("memory file should be writable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input()),
                "<!-- CRAB:ONBOARDING_BASELINE:START --> exists in MEMORY.md but <!-- CRAB:ONBOARDING_BASELINE:END --> is missing",
            );
        });
    }

    #[test]
    fn completion_protocol_rejects_malformed_memory_end_only_marker() {
        with_temp_workspace("malformed-end-only", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(
                workspace.join(MEMORY_FILE_NAME),
                format!("# MEMORY.md\n\n{ONBOARDING_MEMORY_BASELINE_END_MARKER}\n"),
            )
            .expect("memory file should be writable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input()),
                "<!-- CRAB:ONBOARDING_BASELINE:END --> exists in MEMORY.md but <!-- CRAB:ONBOARDING_BASELINE:START --> is missing",
            );
        });
    }

    #[test]
    fn completion_protocol_rejects_bootstrap_path_when_not_a_file() {
        with_temp_workspace("bootstrap-dir", |workspace| {
            fs::create_dir_all(workspace.join(BOOTSTRAP_FILE_NAME))
                .expect("bootstrap directory should be creatable");

            let mut runtime = FakeRuntime::successful();
            let expected = format!(
                "{} must be a regular file",
                workspace.join(BOOTSTRAP_FILE_NAME).to_string_lossy()
            );
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input()),
                &expected,
            );
        });
    }

    #[test]
    fn completion_protocol_rejects_workspace_and_input_invariants() {
        let mut runtime = FakeRuntime::successful();
        assert_invariant_error(
            execute_onboarding_completion_protocol(&mut runtime, Path::new(""), &input()),
            "workspace_root must not be empty",
        );

        with_temp_workspace("invalid-input", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");

            let mut blank_logical_session = input();
            blank_logical_session.logical_session_id = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(
                    &mut runtime,
                    workspace,
                    &blank_logical_session,
                ),
                "logical_session_id must not be empty",
            );

            let mut blank_run_id = input();
            blank_run_id.run_id = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_run_id),
                "run_id must not be empty",
            );

            let mut blank_onboarding = input();
            blank_onboarding.onboarding_session_id = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_onboarding),
                "onboarding_session_id must not be empty",
            );

            let mut zero_completed_at = input();
            zero_completed_at.completed_at_epoch_ms = 0;
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &zero_completed_at),
                "completed_at_epoch_ms must be greater than 0",
            );

            let mut bad_schema = input();
            bad_schema.capture.schema_version = "v2".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &bad_schema),
                "capture.schema_version must be \"v1\", got \"v2\"",
            );

            let mut empty_goals = input();
            empty_goals.capture.primary_goals.clear();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &empty_goals),
                "capture.primary_goals must contain at least one goal",
            );

            let mut blank_goal = input();
            blank_goal.capture.primary_goals = vec![" ".to_string()];
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_goal),
                "capture.primary_goals[] must not be empty",
            );

            let mut blank_agent = input();
            blank_agent.capture.agent_identity = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_agent),
                "capture.agent_identity must not be empty",
            );

            let mut blank_owner = input();
            blank_owner.capture.owner_identity = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_owner),
                "capture.owner_identity must not be empty",
            );

            let mut blank_location = input();
            blank_location.capture.machine_location = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_location),
                "capture.machine_location must not be empty",
            );

            let mut blank_timezone = input();
            blank_timezone.capture.machine_timezone = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &blank_timezone),
                "capture.machine_timezone must not be empty",
            );

            let mut blank_capture_schema = input();
            blank_capture_schema.capture.schema_version = " ".to_string();
            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(
                    &mut runtime,
                    workspace,
                    &blank_capture_schema,
                ),
                "capture.schema_version must not be empty",
            );
        });
    }

    #[test]
    fn completion_protocol_surfaces_runtime_and_workspace_errors() {
        with_temp_workspace("runtime-errors", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut zero_sequence_runtime = FakeRuntime {
                next_sequence_results: VecDeque::from(vec![Ok(0)]),
                append_results: VecDeque::from(vec![Ok(())]),
                appended_events: Vec::new(),
            };
            assert_invariant_error(
                execute_onboarding_completion_protocol(
                    &mut zero_sequence_runtime,
                    workspace,
                    &input(),
                ),
                "next event sequence must be greater than 0",
            );

            let mut bad_runtime = FakeRuntime {
                next_sequence_results: VecDeque::new(),
                append_results: VecDeque::new(),
                appended_events: Vec::new(),
            };
            let error =
                execute_onboarding_completion_protocol(&mut bad_runtime, workspace, &input())
                    .expect_err("missing sequence result should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "fake_onboarding_completion_runtime",
                    message: "missing scripted sequence result".to_string(),
                }
            );
        });

        with_temp_workspace("non-file-memory", |workspace| {
            fs::create_dir_all(workspace.join(MEMORY_FILE_NAME))
                .expect("memory directory should be creatable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            let expected = format!(
                "{} must be a regular file",
                workspace.join(MEMORY_FILE_NAME).to_string_lossy()
            );
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input()),
                &expected,
            );
        });

        with_temp_workspace("workspace-file", |workspace| {
            fs::write(workspace, "not-a-dir").expect("workspace file should be writable");
            let mut runtime = FakeRuntime::successful();
            let expected = format!("{} must be a directory", workspace.to_string_lossy());
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input()),
                &expected,
            );
        });

        with_temp_workspace("workspace-missing", |workspace| {
            let mut runtime = FakeRuntime::successful();
            let error = execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                .expect_err("missing workspace should fail");
            assert!(matches!(
                error,
                CrabError::Io {
                    context: "onboarding_completion",
                    ..
                }
            ));
        });
    }

    #[test]
    fn completion_protocol_rejects_reversed_marker_order() {
        with_temp_workspace("reversed-markers", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(
                workspace.join(MEMORY_FILE_NAME),
                format!(
                    "# MEMORY.md\n\n{ONBOARDING_MEMORY_BASELINE_END_MARKER}\n{ONBOARDING_MEMORY_BASELINE_START_MARKER}\n"
                ),
            )
            .expect("memory file should be writable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            assert_invariant_error(
                execute_onboarding_completion_protocol(&mut runtime, workspace, &input()),
                "<!-- CRAB:ONBOARDING_BASELINE:END --> appears before <!-- CRAB:ONBOARDING_BASELINE:START --> in MEMORY.md",
            );
        });
    }

    #[test]
    fn completion_protocol_creates_memory_when_missing() {
        with_temp_workspace("missing-memory", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            let outcome = execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                .expect("completion should succeed");
            assert!(outcome.bootstrap_retired);

            let memory =
                fs::read_to_string(workspace.join(MEMORY_FILE_NAME)).expect("memory should exist");
            assert!(memory.starts_with("# MEMORY.md\n\n"));
            assert!(memory.contains("Managed Onboarding Baseline"));
        });
    }

    #[test]
    fn completion_protocol_preserves_trailing_content_after_managed_block() {
        with_temp_workspace("tail-preservation", |workspace| {
            fs::create_dir_all(workspace).expect("workspace should be creatable");
            fs::write(
                workspace.join(MEMORY_FILE_NAME),
                format!(
                    "# MEMORY.md\n\n{ONBOARDING_MEMORY_BASELINE_START_MARKER}\nold\n{ONBOARDING_MEMORY_BASELINE_END_MARKER}\nwithout trailing newline"
                ),
            )
            .expect("memory should be writable");
            fs::write(workspace.join(BOOTSTRAP_FILE_NAME), "pending")
                .expect("bootstrap marker should be writable");

            let mut runtime = FakeRuntime::successful();
            execute_onboarding_completion_protocol(&mut runtime, workspace, &input())
                .expect("completion should succeed");

            let memory = fs::read_to_string(workspace.join(MEMORY_FILE_NAME))
                .expect("memory should be readable");
            assert!(memory.ends_with("without trailing newline\n"));
        });
    }
}
