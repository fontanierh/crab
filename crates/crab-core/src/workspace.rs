use std::fs;
use std::path::{Path, PathBuf};

use crate::validation::validate_non_empty_text;
use crate::{CrabError, CrabResult};

pub const AGENTS_FILE_NAME: &str = "AGENTS.md";
pub const SOUL_FILE_NAME: &str = "SOUL.md";
pub const IDENTITY_FILE_NAME: &str = "IDENTITY.md";
pub const USER_FILE_NAME: &str = "USER.md";
pub const MEMORY_FILE_NAME: &str = "MEMORY.md";
pub const BOOTSTRAP_FILE_NAME: &str = "BOOTSTRAP.md";
pub const CLAUDE_LINK_NAME: &str = "CLAUDE.md";
pub const AGENTS_SKILLS_ROOT_RELATIVE_PATH: &str = ".agents/skills";
pub const CLAUDE_SKILLS_LINK_RELATIVE_PATH: &str = ".claude/skills";
pub const SKILL_AUTHORING_POLICY_FILE_RELATIVE_PATH: &str =
    ".agents/skills/skill-authoring-policy/SKILL.md";
pub const SKILL_AUTHORING_POLICY_SKILL_NAME: &str = "skill-authoring-policy";

const MEMORY_DIR_NAME: &str = "memory";
const MEMORY_GLOBAL_DIR_NAME: &str = "global";
const MEMORY_USERS_DIR_NAME: &str = "users";
const AGENTS_DIR_NAME: &str = ".agents";
const SKILLS_DIR_NAME: &str = "skills";
const CLAUDE_DIR_NAME: &str = ".claude";
const SKILL_FILE_NAME: &str = "SKILL.md";
const CLAUDE_SKILLS_LINK_TARGET: &str = "../.agents/skills";

const AGENTS_TEMPLATE: &str = "\
# AGENTS.md
\n\
Workspace instructions for Crab.
\n\
- Keep behavior deterministic and actionable.
\n\
- Prefer memory search before claiming unknown facts.
\n";

const SOUL_TEMPLATE: &str = "\
# SOUL.md
\n\
Agent identity is pending onboarding.
\n\
Capture mission, values, and behavioral constraints here.
\n";

const IDENTITY_TEMPLATE: &str = "\
# IDENTITY.md
\n\
Identity profile is pending onboarding.
\n\
- Agent name:
\n\
- Agent role:
\n";

const USER_TEMPLATE: &str = "\
# USER.md
\n\
Owner profile is pending onboarding.
\n\
- Owner name:
\n\
- Preferred working style:
\n\
- Location/timezone:
\n";

const MEMORY_TEMPLATE: &str = "\
# MEMORY.md
\n\
Curated long-term memory.
\n\
Keep concise, factual notes that should persist across sessions.
\n";

const BOOTSTRAP_TEMPLATE: &str = "\
# BOOTSTRAP.md
\n\
Bootstrap is pending.
\n\
On first interaction, capture:
\n\
1. Who the agent is
\n\
2. Who the owner is
\n\
3. Primary goals
\n\
4. Machine location and timezone
\n\
After capture, update SOUL.md, IDENTITY.md, USER.md, MEMORY.md, then remove this file.
\n";

const SKILL_AUTHORING_POLICY_TEMPLATE: &str = "\
# SKILL.md
\n\
## Purpose
\n\
Guardrail for skill authoring in Crab workspaces.
\n\
## Mandatory Rules
\n\
- Canonical skills root is `.agents/skills`.
\n\
- Never create or edit skills outside `.agents/skills`.
\n\
- Treat `.claude/skills` as compatibility-only and keep it as a symlink to `.agents/skills`.
\n\
- When adding a skill, use `kebab-case` directory naming and include a `SKILL.md`.
\n\
## Required Structure
\n\
```
.agents/
  skills/
    <skill-name>/
      SKILL.md
```
\n\
## Frontmatter Convention
\n\
- Start each skill file with a concise title and purpose section.
\n\
- Include explicit trigger guidance so agents can discover when to apply the skill.
\n\
- Keep instructions deterministic and implementation-focused.
\n";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkspaceBootstrapState {
    NewWorkspace,
    PendingBootstrap,
    Ready,
}

impl WorkspaceBootstrapState {
    #[must_use]
    pub const fn as_token(self) -> &'static str {
        match self {
            Self::NewWorkspace => "new_workspace",
            Self::PendingBootstrap => "pending_bootstrap",
            Self::Ready => "ready",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceTemplate {
    pub file_name: &'static str,
    pub contents: &'static str,
}

const WORKSPACE_TEMPLATES: [WorkspaceTemplate; 6] = [
    WorkspaceTemplate {
        file_name: AGENTS_FILE_NAME,
        contents: AGENTS_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: SOUL_FILE_NAME,
        contents: SOUL_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: IDENTITY_FILE_NAME,
        contents: IDENTITY_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: USER_FILE_NAME,
        contents: USER_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: MEMORY_FILE_NAME,
        contents: MEMORY_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: BOOTSTRAP_FILE_NAME,
        contents: BOOTSTRAP_TEMPLATE,
    },
];

const REQUIRED_TEMPLATES: [WorkspaceTemplate; 5] = [
    WorkspaceTemplate {
        file_name: AGENTS_FILE_NAME,
        contents: AGENTS_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: SOUL_FILE_NAME,
        contents: SOUL_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: IDENTITY_FILE_NAME,
        contents: IDENTITY_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: USER_FILE_NAME,
        contents: USER_TEMPLATE,
    },
    WorkspaceTemplate {
        file_name: MEMORY_FILE_NAME,
        contents: MEMORY_TEMPLATE,
    },
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceEnsureOutcome {
    pub bootstrap_state: WorkspaceBootstrapState,
    pub created_paths: Vec<String>,
    pub repaired_paths: Vec<String>,
}

#[must_use]
pub fn default_workspace_templates() -> &'static [WorkspaceTemplate] {
    &WORKSPACE_TEMPLATES
}

#[must_use]
pub(crate) const fn bootstrap_template_contents() -> &'static str {
    BOOTSTRAP_TEMPLATE
}

pub fn ensure_workspace_layout(workspace_root: &Path) -> CrabResult<WorkspaceEnsureOutcome> {
    validate_workspace_root_path(workspace_root)?;

    let mut created_paths = Vec::new();
    let mut repaired_paths = Vec::new();

    let workspace_existed_before = workspace_root.exists();
    ensure_directory_exists(workspace_root, &mut created_paths, "workspace_layout")?;

    let mut required_template_created = false;
    for template in REQUIRED_TEMPLATES {
        let template_path = workspace_root.join(template.file_name);
        if !template_path.exists() {
            required_template_created = true;
        }
        ensure_template_file(&template_path, template.contents, &mut created_paths)?;
    }

    let bootstrap_path = workspace_root.join(BOOTSTRAP_FILE_NAME);
    let bootstrap_existed_before = bootstrap_path.exists();
    // A workspace directory may be pre-created by wrapper scripts or installers. If required
    // template files were missing (created during this ensure), treat the workspace as pending
    // onboarding by ensuring the bootstrap marker exists.
    if !workspace_existed_before || bootstrap_existed_before || required_template_created {
        ensure_template_file(&bootstrap_path, BOOTSTRAP_TEMPLATE, &mut created_paths)?;
    }

    ensure_memory_directories(workspace_root, &mut created_paths)?;
    ensure_claude_link(workspace_root, &mut created_paths, &mut repaired_paths)?;
    ensure_skills_layout(workspace_root, &mut created_paths, &mut repaired_paths)?;

    let bootstrap_state = resolve_bootstrap_state(workspace_existed_before, workspace_root);
    Ok(WorkspaceEnsureOutcome {
        bootstrap_state,
        created_paths,
        repaired_paths,
    })
}

pub fn detect_workspace_bootstrap_state(
    workspace_root: &Path,
) -> CrabResult<WorkspaceBootstrapState> {
    validate_workspace_root_path(workspace_root)?;
    if !workspace_root.exists() {
        return Ok(WorkspaceBootstrapState::NewWorkspace);
    }

    ensure_path_is_directory(workspace_root, "workspace_bootstrap_state")?;
    let bootstrap_path = workspace_root.join(BOOTSTRAP_FILE_NAME);
    if bootstrap_path.exists() {
        ensure_path_is_regular_file(&bootstrap_path, "workspace_bootstrap_state")?;
        return Ok(WorkspaceBootstrapState::PendingBootstrap);
    }
    Ok(WorkspaceBootstrapState::Ready)
}

pub fn ensure_user_memory_scope(
    workspace_root: &Path,
    discord_user_id: &str,
) -> CrabResult<PathBuf> {
    validate_workspace_root_path(workspace_root)?;
    validate_discord_user_id(discord_user_id)?;

    ensure_named_memory_scope(workspace_root, discord_user_id)
}

pub fn ensure_named_memory_scope(
    workspace_root: &Path,
    memory_scope_directory: &str,
) -> CrabResult<PathBuf> {
    validate_workspace_root_path(workspace_root)?;
    validate_memory_scope_directory(memory_scope_directory)?;

    let mut created_paths = Vec::new();
    ensure_memory_directories(workspace_root, &mut created_paths)?;

    let user_memory_path = workspace_root
        .join(MEMORY_DIR_NAME)
        .join(MEMORY_USERS_DIR_NAME)
        .join(memory_scope_directory);
    ensure_directory_exists(
        &user_memory_path,
        &mut created_paths,
        "workspace_memory_scope",
    )?;
    Ok(user_memory_path)
}

fn resolve_bootstrap_state(
    workspace_existed_before: bool,
    workspace_root: &Path,
) -> WorkspaceBootstrapState {
    if !workspace_existed_before {
        return WorkspaceBootstrapState::NewWorkspace;
    }

    let bootstrap_path = workspace_root.join(BOOTSTRAP_FILE_NAME);
    if bootstrap_path.exists() {
        return WorkspaceBootstrapState::PendingBootstrap;
    }
    WorkspaceBootstrapState::Ready
}

fn validate_workspace_root_path(workspace_root: &Path) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: "workspace_layout",
            message: "workspace_root must not be empty".to_string(),
        });
    }
    Ok(())
}

fn validate_discord_user_id(discord_user_id: &str) -> CrabResult<()> {
    validate_non_empty_text("workspace_memory_scope", "discord_user_id", discord_user_id)?;
    if !discord_user_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(CrabError::InvariantViolation {
            context: "workspace_memory_scope",
            message: format!(
                "discord_user_id must contain only digits, got {:?}",
                discord_user_id
            ),
        });
    }
    Ok(())
}

fn validate_memory_scope_directory(memory_scope_directory: &str) -> CrabResult<()> {
    validate_non_empty_text(
        "workspace_memory_scope",
        "memory_scope_directory",
        memory_scope_directory,
    )?;

    if memory_scope_directory
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: "workspace_memory_scope",
        message: format!(
            "memory_scope_directory must contain only ASCII letters, digits, '-' or '_', got {:?}",
            memory_scope_directory
        ),
    })
}

fn ensure_memory_directories(
    workspace_root: &Path,
    created_paths: &mut Vec<String>,
) -> CrabResult<()> {
    let memory_root = workspace_root.join(MEMORY_DIR_NAME);
    ensure_directory_exists(&memory_root, created_paths, "workspace_memory_layout")?;

    let global_memory_root = memory_root.join(MEMORY_GLOBAL_DIR_NAME);
    ensure_directory_exists(
        &global_memory_root,
        created_paths,
        "workspace_memory_layout",
    )?;

    let users_memory_root = memory_root.join(MEMORY_USERS_DIR_NAME);
    ensure_directory_exists(&users_memory_root, created_paths, "workspace_memory_layout")
}

fn ensure_skills_layout(
    workspace_root: &Path,
    created_paths: &mut Vec<String>,
    repaired_paths: &mut Vec<String>,
) -> CrabResult<()> {
    let skills_context = "workspace_skills_layout";

    let agents_root = workspace_root.join(AGENTS_DIR_NAME);
    ensure_directory_exists(&agents_root, created_paths, skills_context)?;

    let agents_skills_root = agents_root.join(SKILLS_DIR_NAME);
    ensure_directory_exists(&agents_skills_root, created_paths, skills_context)?;

    let policy_skill_root = agents_skills_root.join(SKILL_AUTHORING_POLICY_SKILL_NAME);
    ensure_directory_exists(&policy_skill_root, created_paths, skills_context)?;

    let policy_skill_file = policy_skill_root.join(SKILL_FILE_NAME);
    let policy_template = SKILL_AUTHORING_POLICY_TEMPLATE;
    ensure_template_file(&policy_skill_file, policy_template, created_paths)?;

    let claude_root = workspace_root.join(CLAUDE_DIR_NAME);
    ensure_directory_exists(&claude_root, created_paths, skills_context)?;
    ensure_claude_skills_link(&claude_root, created_paths, repaired_paths)
}

fn ensure_directory_exists(
    path: &Path,
    created_paths: &mut Vec<String>,
    context: &'static str,
) -> CrabResult<()> {
    if path.exists() {
        ensure_path_is_directory(path, context)?;
        return Ok(());
    }

    wrap_io(fs::create_dir_all(path), context, path)?;
    created_paths.push(display_path(path));
    Ok(())
}

fn ensure_template_file(
    path: &Path,
    contents: &str,
    created_paths: &mut Vec<String>,
) -> CrabResult<()> {
    if path.exists() {
        ensure_path_is_regular_file(path, "workspace_template_file")?;
        return Ok(());
    }

    wrap_io(
        fs::write(path, contents.as_bytes()),
        "workspace_template_file",
        path,
    )?;
    created_paths.push(display_path(path));
    Ok(())
}

fn ensure_claude_link(
    workspace_root: &Path,
    created_paths: &mut Vec<String>,
    repaired_paths: &mut Vec<String>,
) -> CrabResult<()> {
    let link_path = workspace_root.join(CLAUDE_LINK_NAME);
    ensure_relative_symlink(
        &link_path,
        Path::new(AGENTS_FILE_NAME),
        SymlinkKind::File,
        created_paths,
        repaired_paths,
    )
}

fn ensure_claude_skills_link(
    claude_root: &Path,
    created_paths: &mut Vec<String>,
    repaired_paths: &mut Vec<String>,
) -> CrabResult<()> {
    let link_path = claude_root.join(SKILLS_DIR_NAME);
    ensure_relative_symlink(
        &link_path,
        Path::new(CLAUDE_SKILLS_LINK_TARGET),
        SymlinkKind::Directory,
        created_paths,
        repaired_paths,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SymlinkKind {
    File,
    Directory,
}

fn ensure_relative_symlink(
    link_path: &Path,
    target: &Path,
    kind: SymlinkKind,
    created_paths: &mut Vec<String>,
    repaired_paths: &mut Vec<String>,
) -> CrabResult<()> {
    match fs::symlink_metadata(link_path) {
        Ok(metadata) if metadata.file_type().is_symlink() || metadata.is_file() => {
            let is_valid_symlink = metadata.file_type().is_symlink()
                && fs::read_link(link_path)
                    .ok()
                    .as_deref()
                    .is_some_and(|existing_target| existing_target == target);
            if is_valid_symlink {
                return Ok(());
            }
            replace_symlink(link_path, target, kind)?;
            repaired_paths.push(display_path(link_path));
            Ok(())
        }
        Ok(_) => Err(CrabError::InvariantViolation {
            context: "workspace_symlink_layout",
            message: format!("{} must be a file or symlink", display_path(link_path)),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            write_symlink(link_path, target, kind)?;
            created_paths.push(display_path(link_path));
            Ok(())
        }
        Err(error) => Err(CrabError::Io {
            context: "workspace_symlink_layout",
            path: Some(display_path(link_path)),
            message: error.to_string(),
        }),
    }
}

fn write_symlink(link_path: &Path, target: &Path, kind: SymlinkKind) -> CrabResult<()> {
    let create_result = match kind {
        SymlinkKind::File => create_file_symlink(target, link_path),
        SymlinkKind::Directory => create_directory_symlink(target, link_path),
    };
    wrap_io(create_result, "workspace_symlink_layout", link_path)
}

fn replace_symlink(link_path: &Path, target: &Path, kind: SymlinkKind) -> CrabResult<()> {
    wrap_io(
        fs::remove_file(link_path),
        "workspace_symlink_layout",
        link_path,
    )?;
    write_symlink(link_path, target, kind)
}

fn ensure_path_is_directory(path: &Path, context: &'static str) -> CrabResult<()> {
    let metadata = wrap_io(fs::metadata(path), context, path)?;
    if !metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{} must be a directory", display_path(path)),
        });
    }
    Ok(())
}

fn ensure_path_is_regular_file(path: &Path, context: &'static str) -> CrabResult<()> {
    let metadata = wrap_io(fs::metadata(path), context, path)?;
    if !metadata.is_file() {
        return Err(CrabError::InvariantViolation {
            context,
            message: format!("{} must be a regular file", display_path(path)),
        });
    }
    Ok(())
}

fn wrap_io<T>(result: std::io::Result<T>, context: &'static str, path: &Path) -> CrabResult<T> {
    result.map_err(|error| CrabError::Io {
        context,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

#[cfg(unix)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_file_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[cfg(unix)]
fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_directory_symlink(target: &Path, link: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_dir(target, link)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        create_directory_symlink, create_file_symlink, default_workspace_templates,
        detect_workspace_bootstrap_state, ensure_claude_link, ensure_claude_skills_link,
        ensure_directory_exists, ensure_named_memory_scope, ensure_path_is_directory,
        ensure_path_is_regular_file, ensure_template_file, ensure_user_memory_scope,
        ensure_workspace_layout, wrap_io, WorkspaceBootstrapState, AGENTS_FILE_NAME,
        AGENTS_SKILLS_ROOT_RELATIVE_PATH, BOOTSTRAP_FILE_NAME, CLAUDE_LINK_NAME,
        CLAUDE_SKILLS_LINK_RELATIVE_PATH, IDENTITY_FILE_NAME, MEMORY_FILE_NAME,
        SKILL_AUTHORING_POLICY_FILE_RELATIVE_PATH, SOUL_FILE_NAME, USER_FILE_NAME,
    };
    use crate::CrabError;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempWorkspace {
        path: PathBuf,
    }

    impl TempWorkspace {
        fn new(label: &str) -> Self {
            let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "crab-workspace-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            Self { path }
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        let mut permissions = fs::metadata(path)
            .expect("metadata should be readable")
            .permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("permissions should be settable");
    }

    #[test]
    fn template_set_contains_expected_files() {
        let templates = default_workspace_templates();
        let file_names: Vec<&str> = templates
            .iter()
            .map(|template| template.file_name)
            .collect();
        assert_eq!(
            file_names,
            vec![
                AGENTS_FILE_NAME,
                SOUL_FILE_NAME,
                IDENTITY_FILE_NAME,
                USER_FILE_NAME,
                MEMORY_FILE_NAME,
                BOOTSTRAP_FILE_NAME
            ]
        );
        assert!(templates
            .iter()
            .all(|template| !template.contents.trim().is_empty()));
    }

    #[test]
    fn bootstrap_state_token_mapping_is_stable() {
        assert_eq!(
            WorkspaceBootstrapState::NewWorkspace.as_token(),
            "new_workspace"
        );
        assert_eq!(
            WorkspaceBootstrapState::PendingBootstrap.as_token(),
            "pending_bootstrap"
        );
        assert_eq!(WorkspaceBootstrapState::Ready.as_token(), "ready");
    }

    #[test]
    fn ensure_workspace_creates_new_workspace_layout() {
        let workspace = TempWorkspace::new("layout-new");
        let outcome =
            ensure_workspace_layout(&workspace.path).expect("workspace should initialize");
        assert_eq!(
            outcome.bootstrap_state,
            WorkspaceBootstrapState::NewWorkspace
        );
        assert!(!outcome.created_paths.is_empty());
        assert!(outcome.repaired_paths.is_empty());
        assert!(!outcome.created_paths.is_empty() || !outcome.repaired_paths.is_empty());

        for required_file in [
            AGENTS_FILE_NAME,
            SOUL_FILE_NAME,
            IDENTITY_FILE_NAME,
            USER_FILE_NAME,
            MEMORY_FILE_NAME,
            BOOTSTRAP_FILE_NAME,
        ] {
            assert!(workspace.path.join(required_file).is_file());
        }
        assert!(workspace.path.join("memory/global").is_dir());
        assert!(workspace.path.join("memory/users").is_dir());

        let claude_link = workspace.path.join(CLAUDE_LINK_NAME);
        let metadata = fs::symlink_metadata(&claude_link).expect("symlink metadata should load");
        assert!(metadata.file_type().is_symlink());
        let target = fs::read_link(&claude_link).expect("symlink target should be readable");
        assert_eq!(target, PathBuf::from(AGENTS_FILE_NAME));

        let agents_skills = workspace.path.join(AGENTS_SKILLS_ROOT_RELATIVE_PATH);
        assert!(agents_skills.is_dir());
        let skill_policy_file = workspace
            .path
            .join(SKILL_AUTHORING_POLICY_FILE_RELATIVE_PATH);
        assert!(skill_policy_file.is_file());
        let skill_policy_contents =
            fs::read_to_string(&skill_policy_file).expect("policy skill should be readable");
        assert!(skill_policy_contents.contains("Canonical skills root is `.agents/skills`."));

        let claude_skills_link = workspace.path.join(CLAUDE_SKILLS_LINK_RELATIVE_PATH);
        let metadata =
            fs::symlink_metadata(&claude_skills_link).expect("skills symlink metadata should load");
        assert!(metadata.file_type().is_symlink());
        let target =
            fs::read_link(&claude_skills_link).expect("skills symlink target should be readable");
        assert_eq!(target, PathBuf::from("../.agents/skills"));
    }

    #[test]
    fn ensure_workspace_creates_bootstrap_marker_when_directory_preexists_but_templates_missing() {
        let workspace = TempWorkspace::new("layout-precreated-dir");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");

        let outcome = ensure_workspace_layout(&workspace.path).expect("layout should initialize");
        assert_eq!(
            outcome.bootstrap_state,
            WorkspaceBootstrapState::PendingBootstrap
        );
        assert!(workspace.path.join(BOOTSTRAP_FILE_NAME).is_file());
    }

    #[test]
    fn ensure_workspace_is_idempotent_for_pending_bootstrap() {
        let workspace = TempWorkspace::new("layout-pending");
        ensure_workspace_layout(&workspace.path).expect("initial layout should succeed");

        let outcome =
            ensure_workspace_layout(&workspace.path).expect("second ensure should succeed");
        assert_eq!(
            outcome.bootstrap_state,
            WorkspaceBootstrapState::PendingBootstrap
        );
        assert!(outcome.created_paths.is_empty());
        assert!(outcome.repaired_paths.is_empty());
    }

    #[test]
    fn ensure_workspace_does_not_recreate_bootstrap_marker_after_completion() {
        let workspace = TempWorkspace::new("layout-ready");
        ensure_workspace_layout(&workspace.path).expect("initial layout should succeed");
        fs::remove_file(workspace.path.join(BOOTSTRAP_FILE_NAME))
            .expect("bootstrap marker should be removable");

        let outcome =
            ensure_workspace_layout(&workspace.path).expect("ensure should still succeed");
        assert_eq!(outcome.bootstrap_state, WorkspaceBootstrapState::Ready);
        assert!(outcome.created_paths.is_empty());
        assert!(outcome.repaired_paths.is_empty());
        assert!(!workspace.path.join(BOOTSTRAP_FILE_NAME).exists());
    }

    #[test]
    fn detect_bootstrap_state_handles_matrix() {
        let workspace = TempWorkspace::new("state-matrix");
        let new_state =
            detect_workspace_bootstrap_state(&workspace.path).expect("state detection should work");
        assert_eq!(new_state, WorkspaceBootstrapState::NewWorkspace);

        ensure_workspace_layout(&workspace.path).expect("layout should initialize");
        let pending_state =
            detect_workspace_bootstrap_state(&workspace.path).expect("state detection should work");
        assert_eq!(pending_state, WorkspaceBootstrapState::PendingBootstrap);

        fs::remove_file(workspace.path.join(BOOTSTRAP_FILE_NAME))
            .expect("bootstrap marker should be removable");
        let ready_state =
            detect_workspace_bootstrap_state(&workspace.path).expect("state detection should work");
        assert_eq!(ready_state, WorkspaceBootstrapState::Ready);
    }

    #[test]
    fn detect_bootstrap_state_rejects_non_file_bootstrap_marker() {
        let workspace = TempWorkspace::new("state-invalid-bootstrap");
        fs::create_dir_all(workspace.path.join(BOOTSTRAP_FILE_NAME))
            .expect("directory marker should be creatable");
        let error = detect_workspace_bootstrap_state(&workspace.path)
            .expect_err("directory bootstrap marker should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_bootstrap_state",
                message: format!(
                    "{} must be a regular file",
                    workspace.path.join(BOOTSTRAP_FILE_NAME).to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_user_memory_scope_creates_path() {
        let workspace = TempWorkspace::new("user-memory");
        let user_memory_path = ensure_user_memory_scope(&workspace.path, "123456789")
            .expect("scope should be created");

        assert_eq!(
            user_memory_path,
            workspace
                .path
                .join("memory")
                .join("users")
                .join("123456789")
        );
        assert!(user_memory_path.is_dir());
        assert!(workspace.path.join("memory/global").is_dir());
    }

    #[test]
    fn ensure_named_memory_scope_supports_owner_directory() {
        let workspace = TempWorkspace::new("named-memory-owner");
        let owner_scope_path =
            ensure_named_memory_scope(&workspace.path, "owner").expect("owner scope should exist");

        assert_eq!(
            owner_scope_path,
            workspace.path.join("memory").join("users").join("owner")
        );
        assert!(owner_scope_path.is_dir());
    }

    #[test]
    fn ensure_named_memory_scope_rejects_invalid_directory_names() {
        for invalid_scope in ["", "../owner", "owner/name", "owner name", "owner:admin"] {
            let workspace = TempWorkspace::new("named-memory-invalid");
            let error = ensure_named_memory_scope(&workspace.path, invalid_scope)
                .expect_err("invalid scope directory should fail");
            assert!(matches!(
                error,
                CrabError::InvariantViolation {
                    context: "workspace_memory_scope",
                    ..
                }
            ));
        }
    }

    #[test]
    fn ensure_named_memory_scope_rejects_empty_workspace_path() {
        let error = ensure_named_memory_scope(Path::new(""), "owner")
            .expect_err("empty workspace root should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn ensure_user_memory_scope_rejects_invalid_ids() {
        for invalid_id in ["", "owner", "../1", "12-34"] {
            let workspace = TempWorkspace::new("invalid-user-memory");
            let error = ensure_user_memory_scope(&workspace.path, invalid_id)
                .expect_err("invalid id should fail");
            assert!(matches!(
                error,
                CrabError::InvariantViolation {
                    context: "workspace_memory_scope",
                    ..
                }
            ));
        }
    }

    #[test]
    fn ensure_directory_exists_rejects_file_path() {
        let workspace = TempWorkspace::new("dir-file");
        fs::create_dir_all(&workspace.path).expect("root should be creatable");
        let file_path = workspace.path.join("memory");
        fs::write(&file_path, "not a directory").expect("file should be writable");
        let mut created_paths = Vec::new();

        let error =
            ensure_directory_exists(&file_path, &mut created_paths, "workspace_memory_layout")
                .expect_err("existing file should fail directory check");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_memory_layout",
                message: format!("{} must be a directory", file_path.to_string_lossy()),
            }
        );
        assert!(created_paths.is_empty());
    }

    #[test]
    fn ensure_template_file_rejects_directory_path() {
        let workspace = TempWorkspace::new("template-dir");
        fs::create_dir_all(workspace.path.join("AGENTS.md"))
            .expect("directory should be creatable");
        let mut created_paths = Vec::new();

        let error =
            ensure_template_file(&workspace.path.join("AGENTS.md"), "x", &mut created_paths)
                .expect_err("directory path should fail file check");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_template_file",
                message: format!(
                    "{} must be a regular file",
                    workspace.path.join("AGENTS.md").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_claude_link_repairs_regular_file() {
        let workspace = TempWorkspace::new("claude-repair-file");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        fs::write(workspace.path.join(CLAUDE_LINK_NAME), "bad").expect("file should be writable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        ensure_claude_link(&workspace.path, &mut created_paths, &mut repaired_paths)
            .expect("link should be repaired");

        assert!(created_paths.is_empty());
        assert_eq!(repaired_paths.len(), 1);
        let target = fs::read_link(workspace.path.join(CLAUDE_LINK_NAME))
            .expect("repaired link should exist");
        assert_eq!(target, PathBuf::from(AGENTS_FILE_NAME));
    }

    #[test]
    fn ensure_claude_link_repairs_wrong_symlink_target() {
        let workspace = TempWorkspace::new("claude-repair-symlink");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        create_file_symlink(Path::new("SOUL.md"), &workspace.path.join(CLAUDE_LINK_NAME))
            .expect("seed symlink should be creatable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        ensure_claude_link(&workspace.path, &mut created_paths, &mut repaired_paths)
            .expect("link should be repaired");

        assert!(created_paths.is_empty());
        assert_eq!(repaired_paths.len(), 1);
        let target = fs::read_link(workspace.path.join(CLAUDE_LINK_NAME))
            .expect("repaired link should exist");
        assert_eq!(target, PathBuf::from(AGENTS_FILE_NAME));
    }

    #[test]
    fn ensure_claude_link_rejects_directory_entry() {
        let workspace = TempWorkspace::new("claude-dir");
        fs::create_dir_all(workspace.path.join(CLAUDE_LINK_NAME))
            .expect("directory entry should be creatable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_link(&workspace.path, &mut created_paths, &mut repaired_paths)
            .expect_err("directory entry should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_symlink_layout",
                message: format!(
                    "{} must be a file or symlink",
                    workspace.path.join(CLAUDE_LINK_NAME).to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_claude_link_reports_io_errors() {
        let workspace = TempWorkspace::new("claude-io");
        fs::write(&workspace.path, "file-root").expect("file root should be writable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_link(&workspace.path, &mut created_paths, &mut repaired_paths)
            .expect_err("non-directory root should trigger io error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_symlink_layout",
                ..
            }
        ));
    }

    #[test]
    fn ensure_claude_skills_link_repairs_regular_file() {
        let workspace = TempWorkspace::new("claude-skills-repair-file");
        fs::create_dir_all(workspace.path.join(".claude"))
            .expect("workspace .claude directory should be creatable");
        fs::write(workspace.path.join(".claude/skills"), "bad")
            .expect("skills file should be writable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        ensure_claude_skills_link(
            &workspace.path.join(".claude"),
            &mut created_paths,
            &mut repaired_paths,
        )
        .expect("skills link should be repaired");

        assert!(created_paths.is_empty());
        assert_eq!(repaired_paths.len(), 1);
        let target = fs::read_link(workspace.path.join(".claude/skills"))
            .expect("repaired skills symlink should exist");
        assert_eq!(target, PathBuf::from("../.agents/skills"));
    }

    #[test]
    fn ensure_claude_skills_link_repairs_wrong_symlink_target() {
        let workspace = TempWorkspace::new("claude-skills-repair-symlink");
        fs::create_dir_all(workspace.path.join(".claude"))
            .expect("workspace .claude directory should be creatable");
        create_directory_symlink(
            Path::new("../memory"),
            &workspace.path.join(".claude/skills"),
        )
        .expect("seed skills symlink should be creatable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        ensure_claude_skills_link(
            &workspace.path.join(".claude"),
            &mut created_paths,
            &mut repaired_paths,
        )
        .expect("skills link should be repaired");

        assert!(created_paths.is_empty());
        assert_eq!(repaired_paths.len(), 1);
        let target = fs::read_link(workspace.path.join(".claude/skills"))
            .expect("repaired skills symlink should exist");
        assert_eq!(target, PathBuf::from("../.agents/skills"));
    }

    #[test]
    fn ensure_claude_skills_link_rejects_directory_entry() {
        let workspace = TempWorkspace::new("claude-skills-dir");
        fs::create_dir_all(workspace.path.join(".claude/skills"))
            .expect("skills directory entry should be creatable");

        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_skills_link(
            &workspace.path.join(".claude"),
            &mut created_paths,
            &mut repaired_paths,
        )
        .expect_err("directory entry should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_symlink_layout",
                message: format!(
                    "{} must be a file or symlink",
                    workspace.path.join(".claude/skills").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn path_validators_reject_wrong_types() {
        let workspace = TempWorkspace::new("validators");
        fs::create_dir_all(&workspace.path).expect("workspace root should be creatable");
        let file_path = workspace.path.join("file.txt");
        fs::write(&file_path, "data").expect("file should be writable");
        let directory_path = workspace.path.join("directory");
        fs::create_dir_all(&directory_path).expect("directory should be creatable");

        let directory_error = ensure_path_is_directory(&file_path, "workspace_layout")
            .expect_err("file is not a directory");
        assert_eq!(
            directory_error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: format!("{} must be a directory", file_path.to_string_lossy()),
            }
        );

        let file_error = ensure_path_is_regular_file(&directory_path, "workspace_template_file")
            .expect_err("directory is not a regular file");
        assert_eq!(
            file_error,
            CrabError::InvariantViolation {
                context: "workspace_template_file",
                message: format!(
                    "{} must be a regular file",
                    directory_path.to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_empty_path() {
        let error =
            ensure_workspace_layout(Path::new("")).expect_err("empty workspace root should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_file_root_path() {
        let workspace = TempWorkspace::new("layout-file-root");
        fs::write(&workspace.path, "workspace is a file").expect("root file should be writable");
        let error =
            ensure_workspace_layout(&workspace.path).expect_err("file roots should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: format!("{} must be a directory", workspace.path.to_string_lossy()),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_required_template_when_it_is_a_directory() {
        let workspace = TempWorkspace::new("layout-template-dir");
        fs::create_dir_all(workspace.path.join(AGENTS_FILE_NAME))
            .expect("template directory should be creatable");
        let error = ensure_workspace_layout(&workspace.path)
            .expect_err("directory templates should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_template_file",
                message: format!(
                    "{} must be a regular file",
                    workspace.path.join(AGENTS_FILE_NAME).to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_bootstrap_marker_when_it_is_a_directory() {
        let workspace = TempWorkspace::new("layout-bootstrap-dir");
        fs::create_dir_all(&workspace.path).expect("workspace should be creatable");
        fs::create_dir_all(workspace.path.join(BOOTSTRAP_FILE_NAME))
            .expect("bootstrap directory should be creatable");

        let error = ensure_workspace_layout(&workspace.path)
            .expect_err("bootstrap directory should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_template_file",
                message: format!(
                    "{} must be a regular file",
                    workspace.path.join(BOOTSTRAP_FILE_NAME).to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_memory_root_file() {
        let workspace = TempWorkspace::new("layout-memory-file");
        fs::create_dir_all(&workspace.path).expect("workspace should be creatable");
        fs::write(workspace.path.join("memory"), "bad memory root")
            .expect("memory root file should be writable");

        let error = ensure_workspace_layout(&workspace.path)
            .expect_err("file-based memory root should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_memory_layout",
                message: format!(
                    "{} must be a directory",
                    workspace.path.join("memory").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_claude_directory_in_layout() {
        let workspace = TempWorkspace::new("layout-claude-dir");
        fs::create_dir_all(workspace.path.join(CLAUDE_LINK_NAME))
            .expect("claude directory should be creatable");
        let error = ensure_workspace_layout(&workspace.path)
            .expect_err("claude directory should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_symlink_layout",
                message: format!(
                    "{} must be a file or symlink",
                    workspace.path.join(CLAUDE_LINK_NAME).to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_workspace_rejects_claude_skills_directory_in_layout() {
        let workspace = TempWorkspace::new("layout-claude-skills-dir");
        fs::create_dir_all(workspace.path.join(".claude/skills"))
            .expect("claude skills directory should be creatable");
        let error = ensure_workspace_layout(&workspace.path)
            .expect_err("claude skills directory should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_symlink_layout",
                message: format!(
                    "{} must be a file or symlink",
                    workspace.path.join(".claude/skills").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn detect_bootstrap_state_rejects_empty_path() {
        let error = detect_workspace_bootstrap_state(Path::new(""))
            .expect_err("empty path should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn detect_bootstrap_state_rejects_file_root() {
        let workspace = TempWorkspace::new("state-file-root");
        fs::write(&workspace.path, "file root").expect("root file should be writable");
        let error = detect_workspace_bootstrap_state(&workspace.path)
            .expect_err("file root should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_bootstrap_state",
                message: format!("{} must be a directory", workspace.path.to_string_lossy()),
            }
        );
    }

    #[test]
    fn ensure_user_memory_scope_rejects_empty_workspace_path() {
        let error = ensure_user_memory_scope(Path::new(""), "123")
            .expect_err("empty workspace root should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_layout",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn ensure_user_memory_scope_rejects_file_workspace_root() {
        let workspace = TempWorkspace::new("scope-file-root");
        fs::write(&workspace.path, "workspace file").expect("root file should be writable");
        let error = ensure_user_memory_scope(&workspace.path, "123")
            .expect_err("file roots should be rejected");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_memory_layout",
                ..
            }
        ));
    }

    #[test]
    fn ensure_user_memory_scope_rejects_existing_user_file_path() {
        let workspace = TempWorkspace::new("scope-user-file");
        fs::create_dir_all(workspace.path.join("memory/users"))
            .expect("users directory should be creatable");
        fs::write(workspace.path.join("memory/users/123"), "file")
            .expect("user file should be writable");

        let error = ensure_user_memory_scope(&workspace.path, "123")
            .expect_err("user file should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_memory_scope",
                message: format!(
                    "{} must be a directory",
                    workspace.path.join("memory/users/123").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_user_memory_scope_rejects_global_memory_file() {
        let workspace = TempWorkspace::new("scope-global-file");
        fs::create_dir_all(workspace.path.join("memory")).expect("memory root should be creatable");
        fs::write(workspace.path.join("memory/global"), "file")
            .expect("global memory file should be writable");
        let error = ensure_user_memory_scope(&workspace.path, "123")
            .expect_err("global file should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "workspace_memory_layout",
                message: format!(
                    "{} must be a directory",
                    workspace.path.join("memory/global").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn ensure_directory_exists_wraps_create_dir_all_errors() {
        let workspace = TempWorkspace::new("dir-wrap-io");
        fs::create_dir_all(&workspace.path).expect("workspace should be creatable");
        fs::write(workspace.path.join("parent"), "file").expect("parent file should be writable");
        let mut created_paths = Vec::new();

        let missing_child = workspace.path.join("parent/child");
        let error = ensure_directory_exists(
            &missing_child,
            &mut created_paths,
            "workspace_memory_layout",
        )
        .expect_err("non-directory parent should fail create_dir_all");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_memory_layout",
                ..
            }
        ));
        assert!(created_paths.is_empty());
    }

    #[test]
    fn ensure_template_file_wraps_write_errors() {
        let workspace = TempWorkspace::new("template-wrap-io");
        fs::create_dir_all(&workspace.path).expect("workspace should be creatable");
        fs::write(workspace.path.join("parent"), "file").expect("parent file should be writable");
        let mut created_paths = Vec::new();

        let template_path = workspace.path.join("parent/AGENTS.md");
        let error = ensure_template_file(&template_path, "content", &mut created_paths)
            .expect_err("non-directory parent should fail file write");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_template_file",
                ..
            }
        ));
        assert!(created_paths.is_empty());
    }

    #[test]
    fn path_validators_wrap_metadata_errors_for_missing_paths() {
        let missing = TempWorkspace::new("missing-path").path.join("ghost");
        let directory_error = ensure_path_is_directory(&missing, "workspace_layout")
            .expect_err("missing paths should produce io error");
        assert!(matches!(
            directory_error,
            CrabError::Io {
                context: "workspace_layout",
                ..
            }
        ));

        let file_error = ensure_path_is_regular_file(&missing, "workspace_template_file")
            .expect_err("missing paths should produce io error");
        assert!(matches!(
            file_error,
            CrabError::Io {
                context: "workspace_template_file",
                ..
            }
        ));
    }

    #[test]
    fn wrap_io_maps_path_and_context() {
        let error = wrap_io::<()>(
            Err(std::io::Error::other("boom")),
            "workspace_layout",
            Path::new("/tmp/path"),
        )
        .expect_err("manual io error should map");
        assert_eq!(
            error,
            CrabError::Io {
                context: "workspace_layout",
                path: Some("/tmp/path".to_string()),
                message: "boom".to_string(),
            }
        );
    }

    #[cfg(unix)]
    #[test]
    fn ensure_claude_link_propagates_remove_failures() {
        let workspace = TempWorkspace::new("claude-remove-fail");
        fs::create_dir_all(&workspace.path).expect("workspace should be creatable");
        fs::write(workspace.path.join(CLAUDE_LINK_NAME), "stale").expect("file should be writable");

        set_mode(&workspace.path, 0o500);
        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_link(&workspace.path, &mut created_paths, &mut repaired_paths)
            .expect_err("read-only directory should fail remove_file");
        set_mode(&workspace.path, 0o700);

        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_symlink_layout",
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn ensure_claude_link_propagates_create_failures_for_missing_link() {
        let workspace = TempWorkspace::new("claude-create-fail");
        fs::create_dir_all(&workspace.path).expect("workspace should be creatable");

        set_mode(&workspace.path, 0o500);
        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_link(&workspace.path, &mut created_paths, &mut repaired_paths)
            .expect_err("read-only directory should fail symlink create");
        set_mode(&workspace.path, 0o700);

        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_symlink_layout",
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn ensure_claude_skills_link_propagates_remove_failures() {
        let workspace = TempWorkspace::new("claude-skills-remove-fail");
        fs::create_dir_all(workspace.path.join(".claude"))
            .expect("workspace .claude directory should be creatable");
        fs::write(workspace.path.join(".claude/skills"), "stale")
            .expect("skills file should be writable");

        set_mode(&workspace.path.join(".claude"), 0o500);
        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_skills_link(
            &workspace.path.join(".claude"),
            &mut created_paths,
            &mut repaired_paths,
        )
        .expect_err("read-only .claude directory should fail remove_file");
        set_mode(&workspace.path.join(".claude"), 0o700);

        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_symlink_layout",
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn ensure_claude_skills_link_propagates_create_failures_for_missing_link() {
        let workspace = TempWorkspace::new("claude-skills-create-fail");
        fs::create_dir_all(workspace.path.join(".claude"))
            .expect("workspace .claude directory should be creatable");

        set_mode(&workspace.path.join(".claude"), 0o500);
        let mut created_paths = Vec::new();
        let mut repaired_paths = Vec::new();
        let error = ensure_claude_skills_link(
            &workspace.path.join(".claude"),
            &mut created_paths,
            &mut repaired_paths,
        )
        .expect_err("read-only .claude directory should fail symlink create");
        set_mode(&workspace.path.join(".claude"), 0o700);

        assert!(matches!(
            error,
            CrabError::Io {
                context: "workspace_symlink_layout",
                ..
            }
        ));
    }
}
