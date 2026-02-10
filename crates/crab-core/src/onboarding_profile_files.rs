use std::fs;
use std::path::Path;

use crate::onboarding::{OnboardingCaptureDocument, ONBOARDING_SCHEMA_VERSION};
use crate::validation::validate_non_empty_text;
use crate::workspace::{IDENTITY_FILE_NAME, SOUL_FILE_NAME, USER_FILE_NAME};
use crate::{CrabError, CrabResult};

const ONBOARDING_PROFILE_WRITER_CONTEXT: &str = "onboarding_profile_writer";

pub const ONBOARDING_MANAGED_START_MARKER: &str = "<!-- CRAB:ONBOARDING_MANAGED:START -->";
pub const ONBOARDING_MANAGED_END_MARKER: &str = "<!-- CRAB:ONBOARDING_MANAGED:END -->";
pub const ONBOARDING_NOTES_START_MARKER: &str = "<!-- CRAB:ONBOARDING_NOTES:START -->";
pub const ONBOARDING_NOTES_END_MARKER: &str = "<!-- CRAB:ONBOARDING_NOTES:END -->";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnboardingProfileDocuments {
    pub identity_document: String,
    pub user_document: String,
    pub soul_document: String,
    pub conflict_files: Vec<&'static str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnboardingProfileWriteOutcome {
    pub written_paths: Vec<String>,
    pub conflict_paths: Vec<String>,
}

pub fn build_onboarding_profile_documents(
    capture: &OnboardingCaptureDocument,
    existing_identity: Option<&str>,
    existing_user: Option<&str>,
    existing_soul: Option<&str>,
) -> CrabResult<OnboardingProfileDocuments> {
    validate_capture_document(capture)?;

    let identity_merge = merge_document(
        "IDENTITY.md",
        &render_identity_managed_block(capture),
        existing_identity,
    );
    let user_merge = merge_document(
        "USER.md",
        &render_user_managed_block(capture),
        existing_user,
    );
    let soul_merge = merge_document(
        "SOUL.md",
        &render_soul_managed_block(capture),
        existing_soul,
    );

    let mut conflict_files = Vec::new();
    if identity_merge.had_conflict {
        conflict_files.push(IDENTITY_FILE_NAME);
    }
    if user_merge.had_conflict {
        conflict_files.push(USER_FILE_NAME);
    }
    if soul_merge.had_conflict {
        conflict_files.push(SOUL_FILE_NAME);
    }

    Ok(OnboardingProfileDocuments {
        identity_document: identity_merge.document,
        user_document: user_merge.document,
        soul_document: soul_merge.document,
        conflict_files,
    })
}

pub fn persist_onboarding_profile_files(
    workspace_root: &Path,
    capture: &OnboardingCaptureDocument,
) -> CrabResult<OnboardingProfileWriteOutcome> {
    validate_workspace_root(workspace_root)?;

    let identity_path = workspace_root.join(IDENTITY_FILE_NAME);
    let user_path = workspace_root.join(USER_FILE_NAME);
    let soul_path = workspace_root.join(SOUL_FILE_NAME);

    let existing_identity = read_existing_file(&identity_path)?;
    let existing_user = read_existing_file(&user_path)?;
    let existing_soul = read_existing_file(&soul_path)?;

    let documents = build_onboarding_profile_documents(
        capture,
        existing_identity.as_deref(),
        existing_user.as_deref(),
        existing_soul.as_deref(),
    )?;

    write_document(&identity_path, &documents.identity_document)?;
    write_document(&user_path, &documents.user_document)?;
    write_document(&soul_path, &documents.soul_document)?;

    let written_paths = vec![
        display_path(&identity_path),
        display_path(&user_path),
        display_path(&soul_path),
    ];

    let conflict_paths = documents
        .conflict_files
        .iter()
        .map(|file_name| display_path(&workspace_root.join(file_name)))
        .collect::<Vec<_>>();

    Ok(OnboardingProfileWriteOutcome {
        written_paths,
        conflict_paths,
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MergeResult {
    document: String,
    had_conflict: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreservedNotes {
    notes: String,
    had_conflict: bool,
}

fn merge_document(title: &str, managed_block: &str, existing: Option<&str>) -> MergeResult {
    let preserved_notes = extract_preserved_notes(existing);
    let document = render_document(title, managed_block, &preserved_notes.notes);
    MergeResult {
        document,
        had_conflict: preserved_notes.had_conflict,
    }
}

fn extract_preserved_notes(existing: Option<&str>) -> PreservedNotes {
    let Some(existing_content) = existing else {
        return PreservedNotes {
            notes: "_No additional notes._".to_string(),
            had_conflict: false,
        };
    };

    let trimmed = existing_content.trim();
    if trimmed.is_empty() {
        return PreservedNotes {
            notes: "_No additional notes._".to_string(),
            had_conflict: false,
        };
    }

    let malformed_notes_markers = has_malformed_marker_pair(
        trimmed,
        ONBOARDING_NOTES_START_MARKER,
        ONBOARDING_NOTES_END_MARKER,
    );
    let malformed_managed_markers = has_malformed_marker_pair(
        trimmed,
        ONBOARDING_MANAGED_START_MARKER,
        ONBOARDING_MANAGED_END_MARKER,
    );

    if malformed_notes_markers || malformed_managed_markers {
        return PreservedNotes {
            notes: render_conflict_notes(trimmed),
            had_conflict: true,
        };
    }

    if let Some(notes_block) = extract_marker_block(
        trimmed,
        ONBOARDING_NOTES_START_MARKER,
        ONBOARDING_NOTES_END_MARKER,
    ) {
        let normalized_notes = notes_block.trim();
        if normalized_notes.is_empty() {
            return PreservedNotes {
                notes: "_No additional notes._".to_string(),
                had_conflict: false,
            };
        }
        return PreservedNotes {
            notes: normalized_notes.to_string(),
            had_conflict: false,
        };
    }

    PreservedNotes {
        notes: format!("## Preserved Legacy Content\n\n{}", trimmed),
        had_conflict: false,
    }
}

fn has_malformed_marker_pair(content: &str, start_marker: &str, end_marker: &str) -> bool {
    let start_index = content.find(start_marker);
    let end_index = content.find(end_marker);

    match (start_index, end_index) {
        (None, None) => false,
        (Some(_), None) | (None, Some(_)) => true,
        (Some(start), Some(end)) => end < start,
    }
}

fn extract_marker_block<'a>(
    content: &'a str,
    start_marker: &'a str,
    end_marker: &'a str,
) -> Option<&'a str> {
    let start_index = content.find(start_marker)?;
    let content_start = start_index + start_marker.len();
    let end_relative = content[content_start..].find(end_marker)?;
    let content_end = content_start + end_relative;
    Some(&content[content_start..content_end])
}

fn render_conflict_notes(existing_content: &str) -> String {
    format!(
        "## Preserved Legacy Content (Conflict - Manual Review Required)\n\n{}",
        existing_content.trim()
    )
}

fn render_document(title: &str, managed_block: &str, notes: &str) -> String {
    let mut document = String::new();
    document.push_str("# ");
    document.push_str(title);
    document.push_str("\n\n");
    document.push_str(ONBOARDING_MANAGED_START_MARKER);
    document.push('\n');
    document.push_str(managed_block.trim_end());
    document.push('\n');
    document.push_str(ONBOARDING_MANAGED_END_MARKER);
    document.push_str("\n\n");
    document.push_str(ONBOARDING_NOTES_START_MARKER);
    document.push('\n');
    document.push_str(notes.trim());
    document.push('\n');
    document.push_str(ONBOARDING_NOTES_END_MARKER);
    document.push('\n');
    document
}

fn render_identity_managed_block(capture: &OnboardingCaptureDocument) -> String {
    format!(
        "## Managed Onboarding Identity\n- Agent identity: {}\n- Owner identity: {}\n- Schema version: {}",
        capture.agent_identity, capture.owner_identity, capture.schema_version
    )
}

fn render_user_managed_block(capture: &OnboardingCaptureDocument) -> String {
    format!(
        "## Managed Owner Profile\n- Owner identity: {}\n- Machine location: {}\n- Machine timezone: {}",
        capture.owner_identity, capture.machine_location, capture.machine_timezone
    )
}

fn render_soul_managed_block(capture: &OnboardingCaptureDocument) -> String {
    let goals = capture
        .primary_goals
        .iter()
        .enumerate()
        .map(|(index, goal)| format!("{}. {}", index + 1, goal))
        .collect::<Vec<_>>()
        .join("\n");

    format!(
        "## Managed Mission Profile\n- Agent identity: {}\n- Owner identity: {}\n- Primary goals:\n{}",
        capture.agent_identity, capture.owner_identity, goals
    )
}

fn validate_capture_document(capture: &OnboardingCaptureDocument) -> CrabResult<()> {
    validate_non_empty_text(
        ONBOARDING_PROFILE_WRITER_CONTEXT,
        "schema_version",
        &capture.schema_version,
    )?;
    if capture.schema_version != ONBOARDING_SCHEMA_VERSION {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_PROFILE_WRITER_CONTEXT,
            message: format!(
                "schema_version must be {:?}, got {:?}",
                ONBOARDING_SCHEMA_VERSION, capture.schema_version
            ),
        });
    }

    validate_non_empty_text(
        ONBOARDING_PROFILE_WRITER_CONTEXT,
        "agent_identity",
        &capture.agent_identity,
    )?;
    validate_non_empty_text(
        ONBOARDING_PROFILE_WRITER_CONTEXT,
        "owner_identity",
        &capture.owner_identity,
    )?;
    validate_non_empty_text(
        ONBOARDING_PROFILE_WRITER_CONTEXT,
        "machine_location",
        &capture.machine_location,
    )?;
    validate_non_empty_text(
        ONBOARDING_PROFILE_WRITER_CONTEXT,
        "machine_timezone",
        &capture.machine_timezone,
    )?;

    if capture.primary_goals.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_PROFILE_WRITER_CONTEXT,
            message: "primary_goals must contain at least one goal".to_string(),
        });
    }
    for goal in &capture.primary_goals {
        validate_non_empty_text(ONBOARDING_PROFILE_WRITER_CONTEXT, "primary_goals[]", goal)?;
    }

    Ok(())
}

fn validate_workspace_root(workspace_root: &Path) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_PROFILE_WRITER_CONTEXT,
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let metadata = fs::metadata(workspace_root).map_err(|error| CrabError::Io {
        context: ONBOARDING_PROFILE_WRITER_CONTEXT,
        path: Some(display_path(workspace_root)),
        message: error.to_string(),
    })?;

    if !metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: ONBOARDING_PROFILE_WRITER_CONTEXT,
            message: format!("{} must be a directory", display_path(workspace_root)),
        });
    }

    Ok(())
}

fn read_existing_file(path: &Path) -> CrabResult<Option<String>> {
    match fs::metadata(path) {
        Ok(metadata) => {
            if !metadata.is_file() {
                return Err(CrabError::InvariantViolation {
                    context: ONBOARDING_PROFILE_WRITER_CONTEXT,
                    message: format!("{} must be a regular file", display_path(path)),
                });
            }
            let content = fs::read_to_string(path).map_err(|error| CrabError::Io {
                context: ONBOARDING_PROFILE_WRITER_CONTEXT,
                path: Some(display_path(path)),
                message: error.to_string(),
            })?;
            Ok(Some(content))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CrabError::Io {
            context: ONBOARDING_PROFILE_WRITER_CONTEXT,
            path: Some(display_path(path)),
            message: error.to_string(),
        }),
    }
}

fn write_document(path: &Path, content: &str) -> CrabResult<()> {
    fs::write(path, content).map_err(|error| CrabError::Io {
        context: ONBOARDING_PROFILE_WRITER_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::Path;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        build_onboarding_profile_documents, persist_onboarding_profile_files,
        ONBOARDING_MANAGED_END_MARKER, ONBOARDING_MANAGED_START_MARKER,
        ONBOARDING_NOTES_END_MARKER, ONBOARDING_NOTES_START_MARKER,
    };
    use crate::onboarding::{OnboardingCaptureDocument, ONBOARDING_SCHEMA_VERSION};
    use crate::workspace::{IDENTITY_FILE_NAME, SOUL_FILE_NAME, USER_FILE_NAME};
    use crate::CrabError;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn with_temp_workspace<T>(label: &str, test_fn: impl FnOnce(&Path) -> T) -> T {
        let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let workspace_path = std::env::temp_dir().join(format!(
            "crab-onboarding-profiles-{label}-{suffix}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&workspace_path);
        let result = test_fn(&workspace_path);
        let _ = fs::remove_dir_all(&workspace_path);
        result
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path)
            .expect("metadata should be readable")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("permissions should be settable");
    }

    fn seed_existing_profile_files(workspace_path: &Path) {
        for file_name in [IDENTITY_FILE_NAME, USER_FILE_NAME, SOUL_FILE_NAME] {
            fs::write(workspace_path.join(file_name), "existing").expect("file should be writable");
        }
    }

    fn assert_write_error_path(error: CrabError, expected_path: &Path) {
        assert!(matches!(
            error,
            CrabError::Io {
                context: "onboarding_profile_writer",
                path: Some(path),
                ..
            } if path == expected_path.to_string_lossy()
        ));
    }

    fn capture_document() -> OnboardingCaptureDocument {
        OnboardingCaptureDocument {
            schema_version: ONBOARDING_SCHEMA_VERSION.to_string(),
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

    #[test]
    fn build_documents_renders_managed_sections_with_default_notes() {
        let documents = build_onboarding_profile_documents(&capture_document(), None, None, None)
            .expect("documents should render");

        for rendered in [
            &documents.identity_document,
            &documents.user_document,
            &documents.soul_document,
        ] {
            assert!(rendered.contains(ONBOARDING_MANAGED_START_MARKER));
            assert!(rendered.contains(ONBOARDING_MANAGED_END_MARKER));
            assert!(rendered.contains(ONBOARDING_NOTES_START_MARKER));
            assert!(rendered.contains(ONBOARDING_NOTES_END_MARKER));
            assert!(rendered.contains("_No additional notes._"));
        }

        assert!(documents
            .identity_document
            .contains("- Agent identity: Crab"));
        assert!(documents
            .user_document
            .contains("- Machine timezone: Europe/Paris"));
        assert!(documents.soul_document.contains("1. Keep quality at 100%"));
        assert!(documents.conflict_files.is_empty());
    }

    #[test]
    fn build_documents_preserves_existing_notes_block() {
        let existing_identity = format!(
            "# IDENTITY.md\n\n{ONBOARDING_MANAGED_START_MARKER}\nold\n{ONBOARDING_MANAGED_END_MARKER}\n\n{ONBOARDING_NOTES_START_MARKER}\nKeep this user-authored note.\n{ONBOARDING_NOTES_END_MARKER}\n"
        );

        let documents = build_onboarding_profile_documents(
            &capture_document(),
            Some(existing_identity.as_str()),
            None,
            None,
        )
        .expect("documents should render");

        assert!(documents
            .identity_document
            .contains("Keep this user-authored note."));
        assert!(documents
            .identity_document
            .contains("- Agent identity: Crab"));
        assert!(documents.conflict_files.is_empty());
    }

    #[test]
    fn build_documents_preserves_legacy_content_without_markers() {
        let existing_user = "# USER.md\n\nLegacy user notes that must not be lost.";
        let documents = build_onboarding_profile_documents(
            &capture_document(),
            None,
            Some(existing_user),
            None,
        )
        .expect("documents should render");

        assert!(documents
            .user_document
            .contains("## Preserved Legacy Content"));
        assert!(documents
            .user_document
            .contains("Legacy user notes that must not be lost."));
        assert!(documents.conflict_files.is_empty());
    }

    #[test]
    fn build_documents_marks_conflict_on_malformed_markers() {
        let malformed_identity =
            format!("# IDENTITY.md\n\n{ONBOARDING_NOTES_START_MARKER}\nnotes without end marker");
        let malformed_user = format!(
            "# USER.md\n\n{ONBOARDING_MANAGED_END_MARKER}\n{ONBOARDING_MANAGED_START_MARKER}"
        );
        let malformed_soul =
            format!("# SOUL.md\n\n{ONBOARDING_NOTES_END_MARKER}\n{ONBOARDING_NOTES_START_MARKER}");

        let documents = build_onboarding_profile_documents(
            &capture_document(),
            Some(malformed_identity.as_str()),
            Some(malformed_user.as_str()),
            Some(malformed_soul.as_str()),
        )
        .expect("documents should render");

        assert_eq!(
            documents.conflict_files,
            vec![IDENTITY_FILE_NAME, USER_FILE_NAME, SOUL_FILE_NAME]
        );
        assert!(documents
            .identity_document
            .contains("Conflict - Manual Review Required"));
        assert!(documents
            .user_document
            .contains("Conflict - Manual Review Required"));
        assert!(documents
            .soul_document
            .contains("Conflict - Manual Review Required"));
    }

    #[test]
    fn build_documents_uses_placeholder_for_empty_existing_and_empty_notes_block() {
        let empty_identity = "   \n";
        let empty_notes_user = format!(
            "# USER.md\n\n{ONBOARDING_NOTES_START_MARKER}\n\n{ONBOARDING_NOTES_END_MARKER}\n"
        );

        let documents = build_onboarding_profile_documents(
            &capture_document(),
            Some(empty_identity),
            Some(empty_notes_user.as_str()),
            None,
        )
        .expect("documents should render");

        assert!(documents
            .identity_document
            .contains("_No additional notes._"));
        assert!(documents.user_document.contains("_No additional notes._"));
    }

    #[test]
    fn extract_marker_block_returns_none_when_end_marker_is_missing() {
        let content = format!("prefix\n{ONBOARDING_NOTES_START_MARKER}\nnotes without end marker");
        let extracted = super::extract_marker_block(
            &content,
            ONBOARDING_NOTES_START_MARKER,
            ONBOARDING_NOTES_END_MARKER,
        );
        assert!(extracted.is_none());
    }

    #[test]
    fn build_documents_rejects_invalid_capture_shapes() {
        let mut invalid_schema = capture_document();
        invalid_schema.schema_version = "v2".to_string();
        let schema_error = build_onboarding_profile_documents(&invalid_schema, None, None, None)
            .expect_err("schema mismatch should fail");
        assert_eq!(
            schema_error,
            CrabError::InvariantViolation {
                context: "onboarding_profile_writer",
                message: "schema_version must be \"v1\", got \"v2\"".to_string(),
            }
        );

        let mut empty_goals = capture_document();
        empty_goals.primary_goals.clear();
        let goals_error = build_onboarding_profile_documents(&empty_goals, None, None, None)
            .expect_err("empty goals should fail");
        assert_eq!(
            goals_error,
            CrabError::InvariantViolation {
                context: "onboarding_profile_writer",
                message: "primary_goals must contain at least one goal".to_string(),
            }
        );

        let mut blank_goal = capture_document();
        blank_goal.primary_goals = vec![" ".to_string()];
        let blank_goal_error = build_onboarding_profile_documents(&blank_goal, None, None, None)
            .expect_err("blank goal should fail");
        assert_eq!(
            blank_goal_error,
            CrabError::InvariantViolation {
                context: "onboarding_profile_writer",
                message: "primary_goals[] must not be empty".to_string(),
            }
        );

        type CaptureMutation = fn(&mut OnboardingCaptureDocument);
        let blank_field_cases: [(&str, CaptureMutation); 5] = [
            ("schema_version", |capture| {
                capture.schema_version = " ".to_string()
            }),
            ("agent_identity", |capture| {
                capture.agent_identity = " ".to_string()
            }),
            ("owner_identity", |capture| {
                capture.owner_identity = " ".to_string()
            }),
            ("machine_location", |capture| {
                capture.machine_location = " ".to_string()
            }),
            ("machine_timezone", |capture| {
                capture.machine_timezone = " ".to_string()
            }),
        ];

        for (field_name, apply_mutation) in blank_field_cases {
            let mut capture = capture_document();
            apply_mutation(&mut capture);
            let error = build_onboarding_profile_documents(&capture, None, None, None)
                .expect_err("blank required field should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "onboarding_profile_writer",
                    message: format!("{field_name} must not be empty"),
                }
            );
        }
    }

    #[test]
    fn persist_writes_profile_files() {
        with_temp_workspace("persist-writes", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            let outcome = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect("persist should succeed");

            assert_eq!(
                outcome.written_paths,
                vec![
                    workspace_path
                        .join(IDENTITY_FILE_NAME)
                        .to_string_lossy()
                        .to_string(),
                    workspace_path
                        .join(USER_FILE_NAME)
                        .to_string_lossy()
                        .to_string(),
                    workspace_path
                        .join(SOUL_FILE_NAME)
                        .to_string_lossy()
                        .to_string(),
                ]
            );
            assert!(outcome.conflict_paths.is_empty());

            let identity =
                fs::read_to_string(workspace_path.join(IDENTITY_FILE_NAME)).expect("identity file");
            let user = fs::read_to_string(workspace_path.join(USER_FILE_NAME)).expect("user file");
            let soul = fs::read_to_string(workspace_path.join(SOUL_FILE_NAME)).expect("soul file");
            assert!(identity.contains("## Managed Onboarding Identity"));
            assert!(user.contains("## Managed Owner Profile"));
            assert!(soul.contains("## Managed Mission Profile"));
        });
    }

    #[test]
    fn persist_preserves_notes_and_reports_conflicts() {
        with_temp_workspace("persist-conflict", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            let user_existing = format!(
                "# USER.md\n\n{ONBOARDING_NOTES_START_MARKER}\nKeep me\n{ONBOARDING_NOTES_END_MARKER}\n"
            );
            fs::write(workspace_path.join(USER_FILE_NAME), user_existing).expect("user file");

            let identity_conflict =
                format!("# IDENTITY.md\n\n{ONBOARDING_MANAGED_START_MARKER}\nmissing end");
            fs::write(workspace_path.join(IDENTITY_FILE_NAME), identity_conflict)
                .expect("identity file");

            let outcome = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect("persist should succeed");

            assert_eq!(
                outcome.conflict_paths,
                vec![workspace_path
                    .join(IDENTITY_FILE_NAME)
                    .to_string_lossy()
                    .to_string()]
            );

            let user = fs::read_to_string(workspace_path.join(USER_FILE_NAME)).expect("user file");
            assert!(user.contains("Keep me"));

            let identity =
                fs::read_to_string(workspace_path.join(IDENTITY_FILE_NAME)).expect("identity file");
            assert!(identity.contains("Conflict - Manual Review Required"));
        });
    }

    #[test]
    fn persist_rejects_invalid_workspace_roots() {
        let empty_path_error = persist_onboarding_profile_files(Path::new(""), &capture_document())
            .expect_err("empty workspace root should fail");
        assert_eq!(
            empty_path_error,
            CrabError::InvariantViolation {
                context: "onboarding_profile_writer",
                message: "workspace_root must not be empty".to_string(),
            }
        );

        with_temp_workspace("persist-invalid-root", |workspace_path| {
            fs::write(workspace_path, "not-a-dir").expect("root file should be writable");
            let not_directory_error =
                persist_onboarding_profile_files(workspace_path, &capture_document())
                    .expect_err("file root should fail");
            assert_eq!(
                not_directory_error,
                CrabError::InvariantViolation {
                    context: "onboarding_profile_writer",
                    message: format!("{} must be a directory", workspace_path.to_string_lossy()),
                }
            );

            let missing_root = workspace_path.join("missing");
            let missing_root_error =
                persist_onboarding_profile_files(&missing_root, &capture_document())
                    .expect_err("missing root should fail");
            assert!(matches!(
                missing_root_error,
                CrabError::Io {
                    context: "onboarding_profile_writer",
                    ..
                }
            ));
        });
    }

    #[test]
    fn persist_rejects_non_regular_existing_target_paths() {
        with_temp_workspace("persist-non-file", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");
            fs::create_dir_all(workspace_path.join(SOUL_FILE_NAME))
                .expect("soul directory should be creatable");

            let error = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect_err("directory target should fail");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "onboarding_profile_writer",
                    message: format!(
                        "{} must be a regular file",
                        workspace_path.join(SOUL_FILE_NAME).to_string_lossy()
                    ),
                }
            );
        });
    }

    #[test]
    fn persist_surfaces_user_read_errors() {
        with_temp_workspace("persist-user-read-error", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");
            fs::create_dir_all(workspace_path.join(USER_FILE_NAME))
                .expect("user directory should be creatable");

            let error = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect_err("user read should fail when target is a directory");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "onboarding_profile_writer",
                    message: format!(
                        "{} must be a regular file",
                        workspace_path.join(USER_FILE_NAME).to_string_lossy()
                    ),
                }
            );
        });
    }

    #[test]
    fn persist_surfaces_capture_validation_errors() {
        with_temp_workspace("persist-capture-error", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            let mut invalid_capture = capture_document();
            invalid_capture.schema_version = "v2".to_string();
            let error = persist_onboarding_profile_files(workspace_path, &invalid_capture)
                .expect_err("invalid capture should fail persist");
            assert_eq!(
                error,
                CrabError::InvariantViolation {
                    context: "onboarding_profile_writer",
                    message: "schema_version must be \"v1\", got \"v2\"".to_string(),
                }
            );
        });
    }

    #[test]
    fn persist_surfaces_write_errors() {
        with_temp_workspace("persist-write-error", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            seed_existing_profile_files(workspace_path);
            #[cfg(unix)]
            set_mode(&workspace_path.join(IDENTITY_FILE_NAME), 0o444);

            let error = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect_err("write should fail when target is read-only");
            assert_write_error_path(error, &workspace_path.join(IDENTITY_FILE_NAME));
            #[cfg(unix)]
            set_mode(&workspace_path.join(IDENTITY_FILE_NAME), 0o644);
        });
    }

    #[cfg(unix)]
    #[test]
    fn persist_surfaces_user_write_errors() {
        with_temp_workspace("persist-user-write-error", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            seed_existing_profile_files(workspace_path);
            set_mode(&workspace_path.join(USER_FILE_NAME), 0o444);

            let error = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect_err("write should fail when user file is read-only");
            assert_write_error_path(error, &workspace_path.join(USER_FILE_NAME));
            set_mode(&workspace_path.join(USER_FILE_NAME), 0o644);
        });
    }

    #[cfg(unix)]
    #[test]
    fn persist_surfaces_soul_write_errors() {
        with_temp_workspace("persist-soul-write-error", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            seed_existing_profile_files(workspace_path);
            set_mode(&workspace_path.join(SOUL_FILE_NAME), 0o444);

            let error = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect_err("write should fail when soul file is read-only");
            assert_write_error_path(error, &workspace_path.join(SOUL_FILE_NAME));
            set_mode(&workspace_path.join(SOUL_FILE_NAME), 0o644);
        });
    }

    #[cfg(unix)]
    #[test]
    fn persist_surfaces_read_and_metadata_permission_errors() {
        with_temp_workspace("persist-permissions", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");

            fs::write(workspace_path.join(IDENTITY_FILE_NAME), "existing")
                .expect("identity file should be writable");
            set_mode(&workspace_path.join(IDENTITY_FILE_NAME), 0o000);
            let read_error = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect_err("identity read should fail");
            assert_write_error_path(read_error, &workspace_path.join(IDENTITY_FILE_NAME));
            set_mode(&workspace_path.join(IDENTITY_FILE_NAME), 0o644);

            set_mode(workspace_path, 0o000);
            let metadata_error =
                persist_onboarding_profile_files(workspace_path, &capture_document())
                    .expect_err("child metadata lookup should fail");
            assert_write_error_path(metadata_error, &workspace_path.join(IDENTITY_FILE_NAME));
            set_mode(workspace_path, 0o755);
        });
    }

    #[cfg(not(unix))]
    #[test]
    fn persist_permission_error_paths_are_not_supported_on_non_unix() {
        with_temp_workspace("persist-permissions-non-unix", |workspace_path| {
            fs::create_dir_all(workspace_path).expect("workspace directory should be creatable");
            let outcome = persist_onboarding_profile_files(workspace_path, &capture_document())
                .expect("persist should succeed on non-unix permission model");
            assert!(outcome.conflict_paths.is_empty());
            assert_eq!(outcome.written_paths.len(), 3);
        });
    }
}
