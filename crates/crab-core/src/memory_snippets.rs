use std::fs;
use std::path::{Path, PathBuf};

use crate::context_assembly::ContextMemorySnippet;
use crate::validation::validate_non_empty_text;
use crate::{CrabError, CrabResult};

const MEMORY_SNIPPET_RESOLVER_CONTEXT: &str = "scoped_memory_snippet_resolver";
const MEMORY_DIRECTORY_NAME: &str = "memory";
const MEMORY_USERS_DIRECTORY_NAME: &str = "users";
const MEMORY_GLOBAL_DIRECTORY_NAME: &str = "global";

pub const DEFAULT_MEMORY_USER_FILE_LIMIT: usize = 3;
pub const DEFAULT_MEMORY_GLOBAL_FILE_LIMIT: usize = 2;
pub const DEFAULT_MEMORY_GLOBAL_RECENCY_DAYS: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedMemorySnippetResolverInput {
    pub workspace_root: PathBuf,
    pub user_scope_directory: String,
    pub include_global_memory: bool,
    pub reference_date: String,
    pub global_recency_days: u32,
    pub max_user_files: usize,
    pub max_global_files: usize,
}

impl ScopedMemorySnippetResolverInput {
    #[must_use]
    pub fn with_defaults(
        workspace_root: &Path,
        user_scope_directory: &str,
        include_global_memory: bool,
        reference_date: &str,
    ) -> Self {
        Self {
            workspace_root: workspace_root.to_path_buf(),
            user_scope_directory: user_scope_directory.to_string(),
            include_global_memory,
            reference_date: reference_date.to_string(),
            global_recency_days: DEFAULT_MEMORY_GLOBAL_RECENCY_DAYS,
            max_user_files: DEFAULT_MEMORY_USER_FILE_LIMIT,
            max_global_files: DEFAULT_MEMORY_GLOBAL_FILE_LIMIT,
        }
    }
}

pub fn resolve_scoped_memory_snippets(
    input: &ScopedMemorySnippetResolverInput,
) -> CrabResult<Vec<ContextMemorySnippet>> {
    validate_workspace_root(&input.workspace_root)?;
    validate_scope_directory(&input.user_scope_directory)?;

    let reference_date = if input.include_global_memory && input.max_global_files > 0 {
        if input.global_recency_days == 0 {
            return Err(CrabError::InvariantViolation {
                context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
                message: "global_recency_days must be greater than 0 when global memory is enabled"
                    .to_string(),
            });
        }
        Some(parse_calendar_day("reference_date", &input.reference_date)?)
    } else {
        None
    };

    let mut snippets = collect_user_snippets(
        &input.workspace_root,
        &input.user_scope_directory,
        input.max_user_files,
    )?;

    if let Some(reference_date) = reference_date {
        snippets.extend(collect_global_snippets(
            &input.workspace_root,
            reference_date,
            input.global_recency_days,
            input.max_global_files,
        )?);
    }

    snippets.sort_by(|left, right| {
        (
            left.path.as_str(),
            left.start_line,
            left.end_line,
            left.content.as_str(),
        )
            .cmp(&(
                right.path.as_str(),
                right.start_line,
                right.end_line,
                right.content.as_str(),
            ))
    });

    Ok(snippets)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct MemoryFileCandidate {
    date: CalendarDay,
    absolute_path: PathBuf,
    relative_path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct CalendarDay {
    year: i32,
    month: u32,
    day: u32,
}

impl CalendarDay {
    fn days_since_unix_epoch(self) -> i64 {
        let adjusted_year = self.year - i32::from(self.month <= 2);
        let era = if adjusted_year >= 0 {
            adjusted_year / 400
        } else {
            (adjusted_year - 399) / 400
        };
        let year_of_era = adjusted_year - (era * 400);
        let month = self.month as i32;
        let day_of_year =
            (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + self.day as i32 - 1;
        let day_of_era = year_of_era * 365 + (year_of_era / 4) - (year_of_era / 100) + day_of_year;
        era as i64 * 146_097 + day_of_era as i64 - 719_468
    }
}

fn collect_user_snippets(
    workspace_root: &Path,
    user_scope_directory: &str,
    max_files: usize,
) -> CrabResult<Vec<ContextMemorySnippet>> {
    let user_memory_root = workspace_root
        .join(MEMORY_DIRECTORY_NAME)
        .join(MEMORY_USERS_DIRECTORY_NAME)
        .join(user_scope_directory);
    let candidates = list_memory_file_candidates(
        &user_memory_root,
        &format!("{MEMORY_DIRECTORY_NAME}/{MEMORY_USERS_DIRECTORY_NAME}/{user_scope_directory}"),
    )?;
    materialize_snippets(candidates, max_files)
}

fn collect_global_snippets(
    workspace_root: &Path,
    reference_date: CalendarDay,
    recency_days: u32,
    max_files: usize,
) -> CrabResult<Vec<ContextMemorySnippet>> {
    let global_root = workspace_root
        .join(MEMORY_DIRECTORY_NAME)
        .join(MEMORY_GLOBAL_DIRECTORY_NAME);
    let candidates = list_memory_file_candidates(
        &global_root,
        &format!("{MEMORY_DIRECTORY_NAME}/{MEMORY_GLOBAL_DIRECTORY_NAME}"),
    )?;

    let recent_candidates = candidates
        .into_iter()
        .filter(|candidate| is_in_recent_window(candidate.date, reference_date, recency_days))
        .collect::<Vec<_>>();

    materialize_snippets(recent_candidates, max_files)
}

fn materialize_snippets(
    mut candidates: Vec<MemoryFileCandidate>,
    max_files: usize,
) -> CrabResult<Vec<ContextMemorySnippet>> {
    if max_files == 0 {
        return Ok(Vec::new());
    }

    candidates.sort_by(|left, right| right.date.cmp(&left.date));

    let mut snippets = Vec::new();
    for candidate in candidates.into_iter().take(max_files) {
        let raw_content = wrap_io(
            fs::read_to_string(&candidate.absolute_path),
            &candidate.absolute_path,
        )?;
        let normalized = raw_content.trim();
        if normalized.is_empty() {
            continue;
        }

        let line_count = normalized.lines().count();
        let end_line = line_count as u32;

        snippets.push(ContextMemorySnippet {
            path: candidate.relative_path,
            start_line: 1,
            end_line,
            content: normalized.to_string(),
        });
    }

    Ok(snippets)
}

fn is_in_recent_window(candidate: CalendarDay, reference: CalendarDay, recency_days: u32) -> bool {
    let delta = reference.days_since_unix_epoch() - candidate.days_since_unix_epoch();
    delta >= 0 && delta < i64::from(recency_days)
}

fn list_memory_file_candidates(
    directory: &Path,
    relative_prefix: &str,
) -> CrabResult<Vec<MemoryFileCandidate>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }

    ensure_directory(directory)?;

    let mut candidates = Vec::new();
    let entries = wrap_io(fs::read_dir(directory), directory)?;
    for entry in entries.flatten() {
        if !entry.path().is_file() {
            continue;
        }

        let file_name = entry.file_name().to_string_lossy().into_owned();
        let Some(stem) = file_name.strip_suffix(".md") else {
            continue;
        };
        let Some(date) = parse_calendar_day_optional(stem) else {
            continue;
        };

        candidates.push(MemoryFileCandidate {
            date,
            absolute_path: entry.path(),
            relative_path: format!("{relative_prefix}/{file_name}"),
        });
    }

    Ok(candidates)
}

fn parse_calendar_day(field: &str, value: &str) -> CrabResult<CalendarDay> {
    validate_non_empty_text(MEMORY_SNIPPET_RESOLVER_CONTEXT, field, value)?;
    let normalized = value.trim();
    parse_calendar_day_optional(normalized).ok_or(CrabError::InvariantViolation {
        context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
        message: format!("{field} must use YYYY-MM-DD format, got {:?}", value),
    })
}

fn parse_calendar_day_optional(value: &str) -> Option<CalendarDay> {
    if value.len() != 10 {
        return None;
    }
    let bytes = value.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }

    let year = &value[0..4];
    let month = &value[5..7];
    let day = &value[8..10];

    let year: i32 = year.parse().ok()?;
    let month: u32 = month.parse().ok()?;
    let day: u32 = day.parse().ok()?;

    if !(1..=12).contains(&month) {
        return None;
    }

    let max_day = days_in_month(year, month);
    if day == 0 || day > max_day {
        return None;
    }

    Some(CalendarDay { year, month, day })
}

fn days_in_month(year: i32, month: u32) -> u32 {
    if month == 2 {
        if is_leap_year(year) {
            return 29;
        }
        return 28;
    }

    if matches!(month, 4 | 6 | 9 | 11) {
        return 30;
    }

    31
}

fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

fn validate_workspace_root(workspace_root: &Path) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let metadata = wrap_io(fs::metadata(workspace_root), workspace_root)?;
    if !metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
            message: format!("{} must be a directory", display_path(workspace_root)),
        });
    }

    Ok(())
}

fn validate_scope_directory(user_scope_directory: &str) -> CrabResult<()> {
    validate_non_empty_text(
        MEMORY_SNIPPET_RESOLVER_CONTEXT,
        "user_scope_directory",
        user_scope_directory,
    )?;

    if user_scope_directory
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
        message: format!(
            "user_scope_directory must contain only ASCII letters, digits, '-' or '_', got {:?}",
            user_scope_directory
        ),
    })
}

fn ensure_directory(path: &Path) -> CrabResult<()> {
    if path.is_dir() {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
        message: format!("{} must be a directory", display_path(path)),
    })
}

fn wrap_io<T>(result: std::io::Result<T>, path: &Path) -> CrabResult<T> {
    result.map_err(|error| CrabError::Io {
        context: MEMORY_SNIPPET_RESOLVER_CONTEXT,
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
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::CrabError;

    use super::{resolve_scoped_memory_snippets, ScopedMemorySnippetResolverInput};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempWorkspace {
        path: PathBuf,
    }

    impl TempWorkspace {
        fn new(label: &str) -> Self {
            let suffix = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "crab-core-scoped-memory-{label}-{}-{suffix}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).expect("temp workspace should be creatable");
            Self { path }
        }

        fn write(&self, relative_path: &str, contents: &str) {
            let absolute_path = self.path.join(relative_path);
            let parent = absolute_path.parent().unwrap_or(Path::new("."));
            fs::create_dir_all(parent).expect("parent directory should be creatable");
            fs::write(absolute_path, contents).expect("fixture file should be writable");
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }

    fn base_input(workspace_root: &Path, scope: &str) -> ScopedMemorySnippetResolverInput {
        ScopedMemorySnippetResolverInput {
            workspace_root: workspace_root.to_path_buf(),
            user_scope_directory: scope.to_string(),
            include_global_memory: true,
            reference_date: "2026-02-10".to_string(),
            global_recency_days: 2,
            max_user_files: 8,
            max_global_files: 8,
        }
    }

    fn sorted_paths(snippets: &[crate::ContextMemorySnippet]) -> Vec<String> {
        snippets
            .iter()
            .map(|snippet| snippet.path.clone())
            .collect()
    }

    #[test]
    fn resolves_non_owner_scope_plus_recent_global_memory() {
        let workspace = TempWorkspace::new("non-owner");
        workspace.write("memory/users/111/2026-02-09.md", "u111 yesterday");
        workspace.write("memory/users/111/2026-02-10.md", "u111 today");
        workspace.write("memory/users/222/2026-02-10.md", "u222 today");
        workspace.write("memory/global/2026-02-10.md", "global today");
        workspace.write("memory/global/2026-02-09.md", "global yesterday");
        workspace.write("memory/global/2026-02-08.md", "global stale");

        let snippets = resolve_scoped_memory_snippets(&base_input(workspace.path(), "111"))
            .expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2026-02-09.md".to_string(),
                "memory/global/2026-02-10.md".to_string(),
                "memory/users/111/2026-02-09.md".to_string(),
                "memory/users/111/2026-02-10.md".to_string(),
            ]
        );
        assert!(!snippets
            .iter()
            .any(|snippet| snippet.path.contains("memory/users/222/")));
    }

    #[test]
    fn resolves_owner_scope_without_cross_user_leakage() {
        let workspace = TempWorkspace::new("owner");
        workspace.write("memory/users/owner/2026-02-10.md", "owner note");
        workspace.write("memory/users/123/2026-02-10.md", "other user note");
        workspace.write("memory/global/2026-02-10.md", "global note");

        let snippets = resolve_scoped_memory_snippets(&base_input(workspace.path(), "owner"))
            .expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2026-02-10.md".to_string(),
                "memory/users/owner/2026-02-10.md".to_string(),
            ]
        );
    }

    #[test]
    fn respects_user_and_global_bounds_deterministically() {
        let workspace = TempWorkspace::new("bounds");
        workspace.write("memory/users/111/2026-02-08.md", "u oldest");
        workspace.write("memory/users/111/2026-02-09.md", "u newer");
        workspace.write("memory/users/111/2026-02-10.md", "u newest");
        workspace.write("memory/global/2026-02-08.md", "g oldest");
        workspace.write("memory/global/2026-02-09.md", "g newer");
        workspace.write("memory/global/2026-02-10.md", "g newest");

        let mut input = base_input(workspace.path(), "111");
        input.global_recency_days = 3;
        input.max_user_files = 2;
        input.max_global_files = 1;

        let snippets = resolve_scoped_memory_snippets(&input).expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2026-02-10.md".to_string(),
                "memory/users/111/2026-02-09.md".to_string(),
                "memory/users/111/2026-02-10.md".to_string(),
            ]
        );
    }

    #[test]
    fn excludes_global_when_disabled() {
        let workspace = TempWorkspace::new("no-global");
        workspace.write("memory/users/111/2026-02-10.md", "user");
        workspace.write("memory/global/2026-02-10.md", "global");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;
        input.reference_date = " ".to_string();

        let snippets = resolve_scoped_memory_snippets(&input).expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec!["memory/users/111/2026-02-10.md".to_string()]
        );
    }

    #[test]
    fn includes_only_recent_global_days_and_excludes_future() {
        let workspace = TempWorkspace::new("recency-window");
        workspace.write("memory/users/111/2026-02-10.md", "user");
        workspace.write("memory/global/2026-02-11.md", "future");
        workspace.write("memory/global/2026-02-10.md", "today");
        workspace.write("memory/global/2026-02-09.md", "yesterday");
        workspace.write("memory/global/2026-02-08.md", "outside window");

        let snippets = resolve_scoped_memory_snippets(&base_input(workspace.path(), "111"))
            .expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2026-02-09.md".to_string(),
                "memory/global/2026-02-10.md".to_string(),
                "memory/users/111/2026-02-10.md".to_string(),
            ]
        );
    }

    #[test]
    fn ignores_non_markdown_and_invalid_date_files() {
        let workspace = TempWorkspace::new("invalid-files");
        workspace.write("memory/users/111/README.md", "not date based");
        workspace.write("memory/users/111/2026-02-10.txt", "wrong extension");
        workspace.write("memory/users/111/2026-02-10.md", "valid");
        workspace.write("memory/global/2026-13-10.md", "invalid month");
        workspace.write("memory/global/2026-02-10.md", "valid global");

        let snippets = resolve_scoped_memory_snippets(&base_input(workspace.path(), "111"))
            .expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2026-02-10.md".to_string(),
                "memory/users/111/2026-02-10.md".to_string(),
            ]
        );
    }

    #[test]
    fn skips_empty_files_after_trimming() {
        let workspace = TempWorkspace::new("empty-files");
        workspace.write("memory/users/111/2026-02-10.md", "\n\n   \n");
        workspace.write("memory/global/2026-02-10.md", "global");

        let snippets = resolve_scoped_memory_snippets(&base_input(workspace.path(), "111"))
            .expect("resolver should succeed");
        assert_eq!(
            sorted_paths(&snippets),
            vec!["memory/global/2026-02-10.md".to_string()]
        );
    }

    #[test]
    fn returns_empty_when_memory_directories_do_not_exist() {
        let workspace = TempWorkspace::new("missing-dirs");
        let snippets = resolve_scoped_memory_snippets(&base_input(workspace.path(), "111"))
            .expect("missing dirs should return empty snippets");
        assert!(snippets.is_empty());
    }

    #[test]
    fn rejects_blank_workspace_root() {
        let mut input = base_input(Path::new("/tmp"), "111");
        input.workspace_root = PathBuf::new();

        let error =
            resolve_scoped_memory_snippets(&input).expect_err("blank workspace root should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_missing_workspace_root() {
        let mut input = base_input(Path::new("/tmp"), "111");
        input.workspace_root =
            PathBuf::from("/tmp/crab-core-missing-workspace-root/does-not-exist");

        let error =
            resolve_scoped_memory_snippets(&input).expect_err("missing workspace should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "scoped_memory_snippet_resolver",
                path: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn rejects_workspace_root_that_is_not_directory() {
        let workspace = TempWorkspace::new("root-file");
        let root_file = workspace.path.join("workspace-file");
        fs::write(&root_file, "file").expect("file should be writable");

        let mut input = base_input(workspace.path(), "111");
        input.workspace_root = root_file.clone();

        let error = resolve_scoped_memory_snippets(&input)
            .expect_err("workspace root file should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: format!("{} must be a directory", root_file.to_string_lossy()),
            }
        );
    }

    #[test]
    fn rejects_invalid_scope_directory() {
        let workspace = TempWorkspace::new("invalid-scope");
        let mut input = base_input(workspace.path(), "../owner");

        let error = resolve_scoped_memory_snippets(&input)
            .expect_err("invalid scope directory should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: "user_scope_directory must contain only ASCII letters, digits, '-' or '_', got \"../owner\""
                    .to_string(),
            }
        );

        input.user_scope_directory = " ".to_string();
        let blank_error =
            resolve_scoped_memory_snippets(&input).expect_err("blank scope directory should fail");
        assert_eq!(
            blank_error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: "user_scope_directory must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_invalid_global_recency_configuration() {
        let workspace = TempWorkspace::new("invalid-recency");
        let mut input = base_input(workspace.path(), "111");
        input.global_recency_days = 0;

        let error = resolve_scoped_memory_snippets(&input)
            .expect_err("zero recency window should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: "global_recency_days must be greater than 0 when global memory is enabled"
                    .to_string(),
            }
        );
    }

    #[test]
    fn rejects_invalid_reference_date_when_global_is_enabled() {
        let workspace = TempWorkspace::new("invalid-reference-date");
        let mut input = base_input(workspace.path(), "111");
        input.reference_date = "2026-02-31".to_string();

        let error =
            resolve_scoped_memory_snippets(&input).expect_err("invalid date should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: "reference_date must use YYYY-MM-DD format, got \"2026-02-31\""
                    .to_string(),
            }
        );
    }

    #[test]
    fn rejects_blank_reference_date_when_global_is_enabled() {
        let workspace = TempWorkspace::new("blank-reference-date");
        let mut input = base_input(workspace.path(), "111");
        input.reference_date = "  ".to_string();

        let error = resolve_scoped_memory_snippets(&input)
            .expect_err("blank reference date should be rejected");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: "reference_date must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn does_not_require_reference_date_when_global_is_disabled_or_capped_to_zero() {
        let workspace = TempWorkspace::new("no-reference-required");
        workspace.write("memory/users/111/2026-02-10.md", "user");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;
        input.reference_date = " ".to_string();

        let snippets = resolve_scoped_memory_snippets(&input)
            .expect("reference date is optional when global memory is disabled");
        assert_eq!(snippets.len(), 1);

        input.include_global_memory = true;
        input.max_global_files = 0;
        let snippets_with_zero_global = resolve_scoped_memory_snippets(&input)
            .expect("reference date is optional when global file cap is zero");
        assert_eq!(snippets_with_zero_global.len(), 1);
    }

    #[test]
    fn preserves_line_metadata_and_trims_content() {
        let workspace = TempWorkspace::new("line-metadata");
        workspace.write("memory/users/111/2026-02-10.md", "\nalpha\nbeta\n\n");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let snippets = resolve_scoped_memory_snippets(&input).expect("resolver should succeed");
        assert_eq!(snippets.len(), 1);
        assert_eq!(snippets[0].path, "memory/users/111/2026-02-10.md");
        assert_eq!(snippets[0].start_line, 1);
        assert_eq!(snippets[0].end_line, 2);
        assert_eq!(snippets[0].content, "alpha\nbeta");
    }

    #[test]
    fn ignores_non_numeric_date_tokens() {
        let workspace = TempWorkspace::new("non-numeric-date");
        workspace.write("memory/users/111/ABCD-02-01.md", "bad year");
        workspace.write("memory/users/111/2026-AA-01.md", "bad month");
        workspace.write("memory/users/111/2026-02-AA.md", "bad day");
        workspace.write("memory/users/111/2026-02-01.md", "valid");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let snippets =
            resolve_scoped_memory_snippets(&input).expect("non numeric tokens should be ignored");
        assert_eq!(
            sorted_paths(&snippets),
            vec!["memory/users/111/2026-02-01.md".to_string()]
        );
    }

    #[cfg(unix)]
    #[test]
    fn surfaces_read_dir_io_errors() {
        use std::os::unix::fs::PermissionsExt;

        let workspace = TempWorkspace::new("read-dir-io");
        let scope_dir = workspace.path.join("memory/users/111");
        fs::create_dir_all(&scope_dir).expect("scope directory should exist");
        fs::set_permissions(&scope_dir, fs::Permissions::from_mode(0o000))
            .expect("scope permissions should be writable");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let error = resolve_scoped_memory_snippets(&input)
            .expect_err("unreadable scope directory should surface io error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "scoped_memory_snippet_resolver",
                path: Some(_),
                ..
            }
        ));

        fs::set_permissions(&scope_dir, fs::Permissions::from_mode(0o700))
            .expect("scope permissions should be restorable");
    }

    #[cfg(unix)]
    #[test]
    fn surfaces_read_file_io_errors() {
        use std::os::unix::fs::PermissionsExt;

        let workspace = TempWorkspace::new("read-file-io");
        let file_path = workspace.path.join("memory/users/111/2026-02-10.md");
        workspace.write("memory/users/111/2026-02-10.md", "secret");
        fs::set_permissions(&file_path, fs::Permissions::from_mode(0o000))
            .expect("file permissions should be writable");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let error = resolve_scoped_memory_snippets(&input)
            .expect_err("unreadable file should surface io error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "scoped_memory_snippet_resolver",
                path: Some(_),
                ..
            }
        ));

        fs::set_permissions(&file_path, fs::Permissions::from_mode(0o600))
            .expect("file permissions should be restorable");
    }

    #[test]
    fn supports_zero_user_file_cap() {
        let workspace = TempWorkspace::new("zero-user-cap");
        workspace.write("memory/users/111/2026-02-10.md", "user");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;
        input.max_user_files = 0;

        let snippets =
            resolve_scoped_memory_snippets(&input).expect("zero user cap should be supported");
        assert!(snippets.is_empty());
    }

    #[test]
    fn skips_directory_entries_inside_memory_scope() {
        let workspace = TempWorkspace::new("directory-entry");
        fs::create_dir_all(workspace.path.join("memory/users/111/2026-02-10.md"))
            .expect("directory fixture should be creatable");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let snippets =
            resolve_scoped_memory_snippets(&input).expect("directory entries should be ignored");
        assert!(snippets.is_empty());
    }

    #[test]
    fn ignores_non_iso_date_length_tokens() {
        let workspace = TempWorkspace::new("bad-date-len");
        workspace.write("memory/users/111/2026-2-01.md", "bad date length");
        workspace.write("memory/users/111/2026_02_01.md", "bad separators");
        workspace.write("memory/users/111/2026-02-01.md", "good date length");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let snippets = resolve_scoped_memory_snippets(&input)
            .expect("resolver should skip malformed date lengths");
        assert_eq!(
            sorted_paths(&snippets),
            vec!["memory/users/111/2026-02-01.md".to_string()]
        );
    }

    #[test]
    fn parses_thirty_and_thirty_one_day_month_boundaries() {
        let workspace = TempWorkspace::new("month-boundaries");
        workspace.write("memory/users/111/2026-01-31.md", "january");
        workspace.write("memory/users/111/2026-04-30.md", "april");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let snippets = resolve_scoped_memory_snippets(&input)
            .expect("valid month boundary dates should parse");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/users/111/2026-01-31.md".to_string(),
                "memory/users/111/2026-04-30.md".to_string(),
            ]
        );
    }

    #[test]
    fn recency_filter_handles_months_after_february() {
        let workspace = TempWorkspace::new("global-after-february");
        workspace.write("memory/global/2026-04-30.md", "april");
        workspace.write("memory/global/2026-04-29.md", "april previous");

        let mut input = base_input(workspace.path(), "111");
        input.max_user_files = 0;
        input.reference_date = "2026-04-30".to_string();
        input.global_recency_days = 2;

        let snippets = resolve_scoped_memory_snippets(&input)
            .expect("global recency should support post-february dates");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2026-04-29.md".to_string(),
                "memory/global/2026-04-30.md".to_string(),
            ]
        );
    }

    #[test]
    fn ancient_dates_follow_same_recency_rules() {
        let workspace = TempWorkspace::new("ancient-date");
        workspace.write("memory/global/0000-01-01.md", "ancient");

        let mut input = base_input(workspace.path(), "111");
        input.max_user_files = 0;
        input.reference_date = "0000-01-01".to_string();
        input.global_recency_days = 1;

        let snippets =
            resolve_scoped_memory_snippets(&input).expect("ancient dates should be supported");
        assert_eq!(
            sorted_paths(&snippets),
            vec!["memory/global/0000-01-01.md".to_string()]
        );
    }

    #[test]
    fn rejects_file_when_global_directory_path_is_not_a_directory() {
        let workspace = TempWorkspace::new("global-path-file");
        workspace.write("memory/users/111/2026-02-10.md", "user");
        workspace.write("memory/global", "not a directory");

        let error = resolve_scoped_memory_snippets(&base_input(workspace.path(), "111"))
            .expect_err("global path file should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: format!(
                    "{} must be a directory",
                    workspace.path.join("memory/global").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn rejects_file_when_user_scope_path_is_not_a_directory() {
        let workspace = TempWorkspace::new("user-path-file");
        workspace.write("memory/users/111", "not a directory");

        let mut input = base_input(workspace.path(), "111");
        input.include_global_memory = false;

        let error =
            resolve_scoped_memory_snippets(&input).expect_err("user scope file should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "scoped_memory_snippet_resolver",
                message: format!(
                    "{} must be a directory",
                    workspace.path.join("memory/users/111").to_string_lossy()
                ),
            }
        );
    }

    #[test]
    fn accepts_valid_leap_day_reference() {
        let workspace = TempWorkspace::new("leap-day");
        workspace.write("memory/users/111/2024-02-29.md", "user leap");
        workspace.write("memory/global/2024-02-29.md", "global leap");
        workspace.write("memory/global/2024-02-28.md", "global previous");

        let mut input = base_input(workspace.path(), "111");
        input.reference_date = "2024-02-29".to_string();

        let snippets = resolve_scoped_memory_snippets(&input).expect("leap day should parse");
        assert_eq!(
            sorted_paths(&snippets),
            vec![
                "memory/global/2024-02-28.md".to_string(),
                "memory/global/2024-02-29.md".to_string(),
                "memory/users/111/2024-02-29.md".to_string(),
            ]
        );
    }

    #[test]
    fn constructor_with_defaults_uses_expected_policy() {
        let workspace = TempWorkspace::new("defaults-constructor");
        workspace.write("memory/users/111/2026-02-10.md", "user");
        workspace.write("memory/global/2026-02-10.md", "global");

        let input = super::ScopedMemorySnippetResolverInput::with_defaults(
            workspace.path(),
            "111",
            true,
            "2026-02-10",
        );
        assert_eq!(input.max_user_files, 3);
        assert_eq!(input.max_global_files, 2);
        assert_eq!(input.global_recency_days, 2);

        let snippets = resolve_scoped_memory_snippets(&input).expect("defaults should resolve");
        assert_eq!(snippets.len(), 2);
    }
}
