use std::path::{Path, PathBuf};

use crab_core::{
    ensure_workspace_layout, CrabError, CrabResult, RuntimeConfig, WorkspaceBootstrapState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppStartupOutcome {
    pub workspace_root: PathBuf,
    pub bootstrap_state: WorkspaceBootstrapState,
    pub created_paths: Vec<String>,
    pub repaired_paths: Vec<String>,
    pub diagnostics: Vec<String>,
}

pub fn initialize_runtime_startup(config: &RuntimeConfig) -> CrabResult<AppStartupOutcome> {
    let workspace_root = resolve_workspace_root(&config.workspace_root)?;
    let ensure_outcome = ensure_workspace_layout(&workspace_root)?;
    let diagnostics = render_startup_diagnostics(
        ensure_outcome.bootstrap_state,
        &ensure_outcome.created_paths,
        &ensure_outcome.repaired_paths,
    );

    Ok(AppStartupOutcome {
        workspace_root,
        bootstrap_state: ensure_outcome.bootstrap_state,
        created_paths: ensure_outcome.created_paths,
        repaired_paths: ensure_outcome.repaired_paths,
        diagnostics,
    })
}

fn render_startup_diagnostics(
    bootstrap_state: WorkspaceBootstrapState,
    created_paths: &[String],
    repaired_paths: &[String],
) -> Vec<String> {
    let mut diagnostics = Vec::new();
    diagnostics.push(format!(
        "workspace.bootstrap_state:{}",
        bootstrap_state.as_token()
    ));

    if created_paths.is_empty() && repaired_paths.is_empty() {
        diagnostics.push("workspace.ensure:noop".to_string());
        return diagnostics;
    }

    for path in created_paths {
        diagnostics.push(format!("workspace.created:{path}"));
    }
    for path in repaired_paths {
        diagnostics.push(format!("workspace.repaired:{path}"));
    }
    diagnostics
}

fn resolve_workspace_root(raw_workspace_root: &str) -> CrabResult<PathBuf> {
    let home = std::env::var_os("HOME").map(PathBuf::from);
    resolve_workspace_root_with_home(raw_workspace_root, home.as_deref())
}

fn resolve_workspace_root_with_home(
    raw_workspace_root: &str,
    home_directory: Option<&Path>,
) -> CrabResult<PathBuf> {
    let normalized = raw_workspace_root.trim();
    if normalized.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "app_startup_workspace_root",
            message: "workspace_root must not be empty".to_string(),
        });
    }

    if normalized == "~" {
        return home_directory
            .map(Path::to_path_buf)
            .ok_or_else(|| missing_home_error("~"));
    }

    if let Some(relative_path) = normalized.strip_prefix("~/") {
        return home_directory
            .map(|home| home.join(relative_path))
            .ok_or_else(|| missing_home_error(raw_workspace_root));
    }

    Ok(PathBuf::from(normalized))
}

fn missing_home_error(value: &str) -> CrabError {
    CrabError::InvalidConfig {
        key: "CRAB_WORKSPACE_ROOT",
        value: value.to_string(),
        reason: "HOME is required for ~ expansion",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::fs;
    use std::path::{Path, PathBuf};

    use crab_core::{
        execute_onboarding_completion_protocol, CrabResult, EventEnvelope, EventKind,
        OnboardingCaptureDocument, OnboardingCompletionEventRuntime, OnboardingCompletionInput,
        WorkspaceBootstrapState, BOOTSTRAP_FILE_NAME, ONBOARDING_MEMORY_BASELINE_END_MARKER,
        ONBOARDING_MEMORY_BASELINE_START_MARKER, ONBOARDING_SCHEMA_VERSION,
    };

    use super::{
        initialize_runtime_startup, render_startup_diagnostics, resolve_workspace_root_with_home,
        AppStartupOutcome,
    };
    use crate::test_support::{
        runtime_config_for_workspace, runtime_config_for_workspace_root, TempWorkspace,
    };
    use crab_core::CrabError;

    #[derive(Debug, Clone)]
    struct FakeOnboardingCompletionRuntime {
        next_event_sequences: VecDeque<CrabResult<u64>>,
        appended_events: Vec<EventEnvelope>,
    }

    impl FakeOnboardingCompletionRuntime {
        fn with_sequence(sequence: u64) -> Self {
            Self {
                next_event_sequences: VecDeque::from(vec![Ok(sequence)]),
                appended_events: Vec::new(),
            }
        }
    }

    impl OnboardingCompletionEventRuntime for FakeOnboardingCompletionRuntime {
        fn next_event_sequence(&self, _logical_session_id: &str, _run_id: &str) -> CrabResult<u64> {
            self.next_event_sequences
                .front()
                .cloned()
                .unwrap_or_else(|| {
                    Err(CrabError::InvariantViolation {
                        context: "startup_test_onboarding_runtime",
                        message: "missing scripted event sequence".to_string(),
                    })
                })
        }

        fn append_event(&mut self, event: &EventEnvelope) -> CrabResult<()> {
            let _ = self.next_event_sequences.pop_front();
            self.appended_events.push(event.clone());
            Ok(())
        }
    }

    fn assert_state(outcome: &AppStartupOutcome, expected: WorkspaceBootstrapState) {
        assert_eq!(outcome.bootstrap_state, expected);
        assert!(outcome
            .diagnostics
            .iter()
            .any(|line| line == &format!("workspace.bootstrap_state:{}", expected.as_token())));
    }

    fn sample_onboarding_capture() -> OnboardingCaptureDocument {
        OnboardingCaptureDocument {
            schema_version: ONBOARDING_SCHEMA_VERSION.to_string(),
            agent_identity: "Crab".to_string(),
            owner_identity: "Henry".to_string(),
            primary_goals: vec![
                "Ship reliable automation".to_string(),
                "Keep strict quality gates".to_string(),
            ],
            machine_location: "Paris, France".to_string(),
            machine_timezone: "Europe/Paris".to_string(),
        }
    }

    #[test]
    fn fake_onboarding_completion_runtime_reports_missing_sequence() {
        let runtime = FakeOnboardingCompletionRuntime {
            next_event_sequences: VecDeque::new(),
            appended_events: Vec::new(),
        };
        let error = runtime
            .next_event_sequence("discord:channel:777", "run:discord:channel:777:onboarding")
            .expect_err("missing scripted sequence should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "startup_test_onboarding_runtime",
                message: "missing scripted event sequence".to_string(),
            }
        );
    }

    #[test]
    fn initialize_runtime_startup_initializes_workspace_before_runtime() {
        let workspace = TempWorkspace::new("startup", "init");
        let config = runtime_config_for_workspace(&workspace.path);
        let outcome = initialize_runtime_startup(&config).expect("startup should succeed");

        assert_state(&outcome, WorkspaceBootstrapState::NewWorkspace);
        assert_eq!(outcome.workspace_root, workspace.path);
        assert!(!outcome.created_paths.is_empty());
        assert!(outcome.repaired_paths.is_empty());
        assert!(workspace.path.join("AGENTS.md").is_file());
        assert!(workspace.path.join("memory/global").is_dir());
    }

    #[test]
    fn initialize_runtime_startup_reports_noop_when_workspace_is_stable() {
        let workspace = TempWorkspace::new("startup", "noop");
        let config = runtime_config_for_workspace(&workspace.path);
        initialize_runtime_startup(&config).expect("first startup should succeed");

        let outcome = initialize_runtime_startup(&config).expect("second startup should succeed");
        assert_state(&outcome, WorkspaceBootstrapState::PendingBootstrap);
        assert!(outcome.created_paths.is_empty());
        assert!(outcome.repaired_paths.is_empty());
        assert!(outcome
            .diagnostics
            .iter()
            .any(|line| line == "workspace.ensure:noop"));
    }

    #[test]
    fn initialize_runtime_startup_reports_ready_after_bootstrap_marker_removed() {
        let workspace = TempWorkspace::new("startup", "ready");
        let config = runtime_config_for_workspace(&workspace.path);
        initialize_runtime_startup(&config).expect("first startup should succeed");
        fs::remove_file(workspace.path.join(BOOTSTRAP_FILE_NAME))
            .expect("bootstrap marker should be removable");

        let outcome = initialize_runtime_startup(&config).expect("startup should succeed");
        assert_state(&outcome, WorkspaceBootstrapState::Ready);
        assert!(outcome.created_paths.is_empty());
        assert!(outcome.repaired_paths.is_empty());
    }

    #[test]
    fn first_interaction_onboarding_completion_transitions_workspace_to_ready() {
        let workspace = TempWorkspace::new("startup", "onboarding-first-interaction");
        let config = runtime_config_for_workspace(&workspace.path);

        let initial = initialize_runtime_startup(&config).expect("first startup should succeed");
        assert_state(&initial, WorkspaceBootstrapState::NewWorkspace);
        let pending = initialize_runtime_startup(&config).expect("second startup should succeed");
        assert_state(&pending, WorkspaceBootstrapState::PendingBootstrap);

        let mut runtime = FakeOnboardingCompletionRuntime::with_sequence(1);
        let completion_input = OnboardingCompletionInput {
            logical_session_id: "discord:channel:777".to_string(),
            run_id: "run:discord:channel:777:onboarding".to_string(),
            onboarding_session_id: "onboarding-1".to_string(),
            completed_at_epoch_ms: 1_739_173_200_100,
            capture: sample_onboarding_capture(),
            profile: None,
        };
        let completion_outcome = execute_onboarding_completion_protocol(
            &mut runtime,
            &workspace.path,
            &completion_input,
        )
        .expect("onboarding completion should succeed");
        assert!(completion_outcome.bootstrap_retired);
        assert_eq!(completion_outcome.emitted_event.kind, EventKind::RunNote);
        assert_eq!(runtime.appended_events.len(), 1);

        let memory_contents =
            fs::read_to_string(workspace.path.join("MEMORY.md")).expect("memory should be written");
        assert!(memory_contents.contains(ONBOARDING_MEMORY_BASELINE_START_MARKER));
        assert!(memory_contents.contains(ONBOARDING_MEMORY_BASELINE_END_MARKER));

        let ready = initialize_runtime_startup(&config).expect("startup should report ready");
        assert_state(&ready, WorkspaceBootstrapState::Ready);
    }

    #[test]
    fn initialize_runtime_startup_rejects_blank_workspace_root() {
        let config = runtime_config_for_workspace_root("   ");
        let error = initialize_runtime_startup(&config)
            .expect_err("blank workspace root should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "app_startup_workspace_root",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn initialize_runtime_startup_propagates_workspace_layout_errors() {
        let workspace = TempWorkspace::new("startup", "layout-error");
        fs::write(&workspace.path, "root-file").expect("root file should be writable");
        let config = runtime_config_for_workspace(&workspace.path);
        let error = initialize_runtime_startup(&config)
            .expect_err("workspace layout errors should propagate");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: format!("{} must be a directory", workspace.path.to_string_lossy()),
            }
        );
    }

    #[test]
    fn resolve_workspace_root_with_home_expands_tilde_forms() {
        let home = Path::new("/tmp/crab-home");
        assert_eq!(
            resolve_workspace_root_with_home("~", Some(home)).expect("~ should resolve with home"),
            PathBuf::from("/tmp/crab-home")
        );
        assert_eq!(
            resolve_workspace_root_with_home("~/workspace", Some(home))
                .expect("~/workspace should resolve with home"),
            PathBuf::from("/tmp/crab-home/workspace")
        );
        assert_eq!(
            resolve_workspace_root_with_home(" /tmp/crab/workspace ", Some(home))
                .expect("absolute path should be normalized"),
            PathBuf::from("/tmp/crab/workspace")
        );
    }

    #[test]
    fn resolve_workspace_root_with_home_rejects_missing_inputs() {
        let empty_error = resolve_workspace_root_with_home("   ", Some(Path::new("/tmp/home")))
            .expect_err("blank root should fail");
        assert_eq!(
            empty_error,
            CrabError::InvariantViolation {
                context: "app_startup_workspace_root",
                message: "workspace_root must not be empty".to_string(),
            }
        );

        let missing_home_for_tilde =
            resolve_workspace_root_with_home("~", None).expect_err("~ needs home");
        assert_eq!(
            missing_home_for_tilde,
            CrabError::InvalidConfig {
                key: "CRAB_WORKSPACE_ROOT",
                value: "~".to_string(),
                reason: "HOME is required for ~ expansion",
            }
        );

        let missing_home_for_relative_tilde = resolve_workspace_root_with_home("~/workspace", None)
            .expect_err("~/workspace needs home");
        assert_eq!(
            missing_home_for_relative_tilde,
            CrabError::InvalidConfig {
                key: "CRAB_WORKSPACE_ROOT",
                value: "~/workspace".to_string(),
                reason: "HOME is required for ~ expansion",
            }
        );
    }

    #[test]
    fn startup_diagnostics_include_mutation_details_or_noop() {
        let diagnostics = render_startup_diagnostics(
            WorkspaceBootstrapState::PendingBootstrap,
            &["/tmp/workspace/AGENTS.md".to_string()],
            &["/tmp/workspace/CLAUDE.md".to_string()],
        );
        assert_eq!(
            diagnostics,
            vec![
                "workspace.bootstrap_state:pending_bootstrap".to_string(),
                "workspace.created:/tmp/workspace/AGENTS.md".to_string(),
                "workspace.repaired:/tmp/workspace/CLAUDE.md".to_string(),
            ]
        );

        let noop =
            render_startup_diagnostics(WorkspaceBootstrapState::Ready, &Vec::new(), &Vec::new());
        assert_eq!(
            noop,
            vec![
                "workspace.bootstrap_state:ready".to_string(),
                "workspace.ensure:noop".to_string(),
            ]
        );
    }
}
