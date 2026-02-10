use std::fs;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use crate::validation::validate_non_empty_text;
use crate::workspace::MEMORY_FILE_NAME;
use crate::{CrabError, CrabResult};

const MEMORY_GET_CONTEXT: &str = "memory_get";
const MEMORY_DIRECTORY_NAME: &str = "memory";
const MEMORY_USERS_DIRECTORY_NAME: &str = "users";
const MEMORY_GLOBAL_DIRECTORY_NAME: &str = "global";

pub const DEFAULT_MEMORY_GET_MAX_LINES: u32 = 64;
pub const DEFAULT_MEMORY_GET_MAX_CHARS: usize = 4_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryGetInput {
    pub workspace_root: PathBuf,
    pub user_scope_directory: String,
    pub include_global_memory: bool,
    pub path: String,
    pub start_line: u32,
    pub end_line: Option<u32>,
    pub max_lines: u32,
    pub max_chars: usize,
}

impl MemoryGetInput {
    #[must_use]
    pub fn with_defaults(
        workspace_root: &Path,
        user_scope_directory: &str,
        include_global_memory: bool,
        path: &str,
        start_line: u32,
        end_line: Option<u32>,
    ) -> Self {
        Self {
            workspace_root: workspace_root.to_path_buf(),
            user_scope_directory: user_scope_directory.to_string(),
            include_global_memory,
            path: path.to_string(),
            start_line,
            end_line,
            max_lines: DEFAULT_MEMORY_GET_MAX_LINES,
            max_chars: DEFAULT_MEMORY_GET_MAX_CHARS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryGetResult {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub content: String,
    pub truncated: bool,
}

pub fn get_memory(input: &MemoryGetInput) -> CrabResult<Option<MemoryGetResult>> {
    validate_input(input)?;
    let normalized_path = normalize_relative_path(&input.path)?;
    validate_allowed_path(
        &normalized_path,
        &input.user_scope_directory,
        input.include_global_memory,
    )?;

    let requested_end = resolve_end_line(input.start_line, input.end_line, input.max_lines)?;
    let absolute = input.workspace_root.join(&normalized_path);

    let metadata = match fs::symlink_metadata(&absolute) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(CrabError::Io {
                context: MEMORY_GET_CONTEXT,
                path: Some(display_path(&absolute)),
                message: error.to_string(),
            });
        }
    };

    if metadata.file_type().is_symlink() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!("{} must not be a symlink", display_path(&absolute)),
        });
    }
    if !metadata.is_file() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!("{} must be a regular file", display_path(&absolute)),
        });
    }

    ensure_no_symlink_components(&input.workspace_root, &normalized_path)?;

    let Some((actual_end_line, content, truncated)) =
        read_line_range(&absolute, input.start_line, requested_end, input.max_chars)?
    else {
        return Ok(None);
    };

    Ok(Some(MemoryGetResult {
        path: normalized_path,
        start_line: input.start_line,
        end_line: actual_end_line,
        content,
        truncated,
    }))
}

fn validate_input(input: &MemoryGetInput) -> CrabResult<()> {
    if input.workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let workspace_metadata = wrap_io(fs::metadata(&input.workspace_root), &input.workspace_root)?;
    if !workspace_metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!(
                "{} must be a directory",
                display_path(&input.workspace_root)
            ),
        });
    }

    validate_non_empty_text(
        MEMORY_GET_CONTEXT,
        "user_scope_directory",
        &input.user_scope_directory,
    )?;
    if !input
        .user_scope_directory
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!(
                "user_scope_directory must contain only ASCII letters, digits, '-' or '_', got {:?}",
                input.user_scope_directory
            ),
        });
    }

    if input.start_line == 0 {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: "start_line must be greater than 0".to_string(),
        });
    }
    if input.max_lines == 0 {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: "max_lines must be greater than 0".to_string(),
        });
    }
    if input.max_chars == 0 {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: "max_chars must be greater than 0".to_string(),
        });
    }

    Ok(())
}

fn normalize_relative_path(path: &str) -> CrabResult<String> {
    validate_non_empty_text(MEMORY_GET_CONTEXT, "path", path)?;
    if path.contains('\\') {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: "path must use '/' separators".to_string(),
        });
    }
    if path.starts_with('/') || path.ends_with('/') {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!(
                "path must be relative without leading/trailing '/': {:?}",
                path
            ),
        });
    }

    let segments = path.split('/').collect::<Vec<_>>();
    for segment in &segments {
        if segment.is_empty() || *segment == "." || *segment == ".." {
            return Err(CrabError::InvariantViolation {
                context: MEMORY_GET_CONTEXT,
                message: format!("path contains invalid segment: {:?}", segment),
            });
        }
    }

    Ok(path.to_string())
}

fn validate_allowed_path(
    relative_path: &str,
    user_scope_directory: &str,
    include_global_memory: bool,
) -> CrabResult<()> {
    if relative_path == MEMORY_FILE_NAME {
        return Ok(());
    }

    let parts = relative_path.split('/').collect::<Vec<_>>();
    match parts.as_slice() {
        [MEMORY_DIRECTORY_NAME, MEMORY_USERS_DIRECTORY_NAME, scope, file_name] => {
            if *scope != user_scope_directory {
                return Err(CrabError::InvariantViolation {
                    context: MEMORY_GET_CONTEXT,
                    message: format!(
                        "path {:?} is outside user scope {:?}",
                        relative_path, user_scope_directory
                    ),
                });
            }
            validate_markdown_file_name(file_name)
        }
        [MEMORY_DIRECTORY_NAME, MEMORY_GLOBAL_DIRECTORY_NAME, file_name] => {
            if !include_global_memory {
                return Err(CrabError::InvariantViolation {
                    context: MEMORY_GET_CONTEXT,
                    message: "global memory retrieval is disabled".to_string(),
                });
            }
            validate_markdown_file_name(file_name)
        }
        _ => Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!(
                "path {:?} must target MEMORY.md or memory/users/<scope>/*.md or memory/global/*.md",
                relative_path
            ),
        }),
    }
}

fn validate_markdown_file_name(file_name: &str) -> CrabResult<()> {
    let Some(stem) = file_name.strip_suffix(".md") else {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!("path file {:?} must end with .md", file_name),
        });
    };
    if stem.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: "path file stem must not be empty".to_string(),
        });
    }
    if !file_name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.')
    {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!(
                "path file {:?} must contain only ASCII letters, digits, '-', '_' or '.'",
                file_name
            ),
        });
    }
    Ok(())
}

fn resolve_end_line(start_line: u32, end_line: Option<u32>, max_lines: u32) -> CrabResult<u32> {
    let resolved_end = end_line.unwrap_or(start_line);
    if resolved_end < start_line {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!("end_line must be greater than or equal to start_line ({start_line})"),
        });
    }

    let span = resolved_end - start_line + 1;

    if span > max_lines {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_GET_CONTEXT,
            message: format!("requested line span {span} exceeds max_lines {max_lines}"),
        });
    }

    Ok(resolved_end)
}

fn read_line_range(
    absolute_path: &Path,
    start_line: u32,
    end_line: u32,
    max_chars: usize,
) -> CrabResult<Option<(u32, String, bool)>> {
    let file = wrap_io(File::open(absolute_path), absolute_path)?;
    let mut selected = Vec::new();
    let mut actual_end = 0u32;

    for (index, line_result) in BufReader::new(file).lines().enumerate() {
        let line_number = index as u32 + 1;
        if line_number < start_line {
            continue;
        }
        if line_number > end_line {
            break;
        }

        let line = line_result.map_err(|error| CrabError::Io {
            context: MEMORY_GET_CONTEXT,
            path: Some(display_path(absolute_path)),
            message: error.to_string(),
        })?;
        selected.push(line);
        actual_end = line_number;
    }

    if selected.is_empty() {
        return Ok(None);
    }

    let (content, truncated) = truncate_lines(&selected, max_chars);
    Ok(Some((actual_end, content, truncated)))
}

fn truncate_lines(lines: &[String], max_chars: usize) -> (String, bool) {
    let mut out = String::new();
    let mut used_chars = 0usize;
    let mut truncated = false;

    for (index, line) in lines.iter().enumerate() {
        let line_char_count = line.chars().count();
        let required = if index == 0 {
            line_char_count
        } else {
            line_char_count + 1
        };

        if index == 0 && line_char_count > max_chars {
            out = line.chars().take(max_chars).collect();
            truncated = true;
            break;
        }
        if index > 0 && used_chars + required > max_chars {
            truncated = true;
            break;
        }

        if index > 0 {
            out.push('\n');
            used_chars += 1;
        }
        out.push_str(line);
        used_chars += line_char_count;
    }

    (out, truncated)
}

fn wrap_io<T>(result: std::io::Result<T>, path: &Path) -> CrabResult<T> {
    result.map_err(|error| CrabError::Io {
        context: MEMORY_GET_CONTEXT,
        path: Some(display_path(path)),
        message: error.to_string(),
    })
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}

fn ensure_no_symlink_components(workspace_root: &Path, relative_path: &str) -> CrabResult<()> {
    let mut current = workspace_root.to_path_buf();
    for component in relative_path.split('/') {
        current.push(component);
        let metadata = wrap_io(fs::symlink_metadata(&current), &current)?;
        if metadata.file_type().is_symlink() {
            return Err(CrabError::InvariantViolation {
                context: MEMORY_GET_CONTEXT,
                message: format!("{} must not be a symlink", display_path(&current)),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::CrabError;

    use super::{
        ensure_no_symlink_components, get_memory, MemoryGetInput, DEFAULT_MEMORY_GET_MAX_CHARS,
        DEFAULT_MEMORY_GET_MAX_LINES,
    };

    static FIXTURE_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempFixture {
        root: PathBuf,
    }

    impl TempFixture {
        fn new(name: &str) -> Self {
            let id = FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "crab-memory-get-{name}-{}-{id}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("fixture should be creatable");
            Self { root }
        }

        fn file(&self, relative_path: &str, contents: &str) {
            let absolute = self.root.join(relative_path);
            let parent = absolute
                .parent()
                .expect("joined fixture path should include a parent");
            fs::create_dir_all(parent).expect("parent should exist");
            fs::write(absolute, contents).expect("fixture write should succeed");
        }

        fn dir(&self, relative_path: &str) {
            fs::create_dir_all(self.root.join(relative_path)).expect("directory should exist");
        }

        fn input(&self, path: &str, start_line: u32, end_line: Option<u32>) -> MemoryGetInput {
            MemoryGetInput {
                workspace_root: self.root.clone(),
                user_scope_directory: "111".to_string(),
                include_global_memory: true,
                path: path.to_string(),
                start_line,
                end_line,
                max_lines: 32,
                max_chars: 1_000,
            }
        }
    }

    impl Drop for TempFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(unix)]
    fn make_symlink(source: &Path, target: &Path) {
        std::os::unix::fs::symlink(source, target).expect("symlink should be creatable");
    }

    #[cfg(windows)]
    fn make_symlink(source: &Path, target: &Path) {
        std::os::windows::fs::symlink_file(source, target).expect("symlink should be creatable");
    }

    #[cfg(unix)]
    fn make_dir_symlink(source: &Path, target: &Path) {
        std::os::unix::fs::symlink(source, target).expect("symlink should be creatable");
    }

    #[cfg(windows)]
    fn make_dir_symlink(source: &Path, target: &Path) {
        std::os::windows::fs::symlink_dir(source, target).expect("symlink should be creatable");
    }

    #[cfg(unix)]
    fn set_mode(path: &Path, mode: u32) {
        use std::os::unix::fs::PermissionsExt;

        let mut permissions = fs::metadata(path).expect("path should exist").permissions();
        permissions.set_mode(mode);
        fs::set_permissions(path, permissions).expect("mode update should succeed");
    }

    #[test]
    fn defaults_are_stable() {
        let input =
            MemoryGetInput::with_defaults(Path::new("/tmp"), "owner", true, "MEMORY.md", 3, None);
        assert_eq!(input.max_lines, DEFAULT_MEMORY_GET_MAX_LINES);
        assert_eq!(input.max_chars, DEFAULT_MEMORY_GET_MAX_CHARS);
    }

    #[test]
    fn returns_curated_memory_single_line() {
        let fixture = TempFixture::new("curated");
        fixture.file("MEMORY.md", "alpha\nbeta\ngamma\n");

        let result =
            get_memory(&fixture.input("MEMORY.md", 2, None)).expect("memory get should succeed");

        assert_eq!(
            result,
            Some(super::MemoryGetResult {
                path: "MEMORY.md".to_string(),
                start_line: 2,
                end_line: 2,
                content: "beta".to_string(),
                truncated: false,
            })
        );
    }

    #[test]
    fn returns_user_memory_line_range() {
        let fixture = TempFixture::new("user-range");
        fixture.file(
            "memory/users/111/2026-02-10.md",
            "first\nsecond\nthird\nfourth",
        );

        let result = get_memory(&fixture.input("memory/users/111/2026-02-10.md", 2, Some(3)))
            .expect("memory get should succeed")
            .expect("range should exist");

        assert_eq!(result.path, "memory/users/111/2026-02-10.md");
        assert_eq!(result.start_line, 2);
        assert_eq!(result.end_line, 3);
        assert_eq!(result.content, "second\nthird");
        assert!(!result.truncated);
    }

    #[test]
    fn returns_global_memory_when_enabled() {
        let fixture = TempFixture::new("global");
        fixture.file("memory/global/2026-02-10.md", "g1\ng2\ng3");

        let result = get_memory(&fixture.input("memory/global/2026-02-10.md", 1, Some(2)))
            .expect("memory get should succeed")
            .expect("file should exist");

        assert_eq!(result.content, "g1\ng2");
        assert_eq!(result.end_line, 2);
    }

    #[test]
    fn can_disable_global_memory_reads() {
        let fixture = TempFixture::new("global-off");
        fixture.file("memory/global/2026-02-10.md", "global");

        let mut input = fixture.input("memory/global/2026-02-10.md", 1, None);
        input.include_global_memory = false;

        let error = get_memory(&input).expect_err("global read should fail when disabled");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "global memory retrieval is disabled".to_string(),
            }
        );
    }

    #[test]
    fn returns_none_when_file_is_missing() {
        let fixture = TempFixture::new("missing-file");
        let result = get_memory(&fixture.input("MEMORY.md", 1, None))
            .expect("missing file should be treated as empty");
        assert!(result.is_none());
    }

    #[test]
    fn returns_none_when_range_is_outside_file() {
        let fixture = TempFixture::new("outside-range");
        fixture.file("MEMORY.md", "one\ntwo");

        let result =
            get_memory(&fixture.input("MEMORY.md", 7, None)).expect("out-of-range should not fail");
        assert!(result.is_none());
    }

    #[test]
    fn returns_io_error_for_invalid_utf8_file_contents() {
        let fixture = TempFixture::new("invalid-utf8");
        let path = fixture.root.join("MEMORY.md");
        fs::write(path, vec![0xf0, 0x28, 0x8c, 0x28]).expect("fixture bytes should write");

        let error =
            get_memory(&fixture.input("MEMORY.md", 1, None)).expect_err("invalid utf8 should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "memory_get",
                ..
            }
        ));
    }

    #[test]
    fn range_can_extend_past_file_end() {
        let fixture = TempFixture::new("past-end");
        fixture.file("MEMORY.md", "one\ntwo");

        let result = get_memory(&fixture.input("MEMORY.md", 2, Some(9)))
            .expect("memory get should succeed")
            .expect("line should exist");

        assert_eq!(result.start_line, 2);
        assert_eq!(result.end_line, 2);
        assert_eq!(result.content, "two");
    }

    #[test]
    fn rejects_zero_start_line() {
        let fixture = TempFixture::new("zero-start");
        fixture.file("MEMORY.md", "one");

        let error =
            get_memory(&fixture.input("MEMORY.md", 0, None)).expect_err("start_line=0 should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "start_line must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn rejects_end_line_before_start_line() {
        let fixture = TempFixture::new("bad-range");
        fixture.file("MEMORY.md", "one");

        let error = get_memory(&fixture.input("MEMORY.md", 3, Some(2)))
            .expect_err("reverse ranges should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "end_line must be greater than or equal to start_line (3)".to_string(),
            }
        );
    }

    #[test]
    fn rejects_ranges_larger_than_max_lines() {
        let fixture = TempFixture::new("max-lines");
        fixture.file("MEMORY.md", "one\ntwo\nthree");

        let mut input = fixture.input("MEMORY.md", 1, Some(3));
        input.max_lines = 2;

        let error = get_memory(&input).expect_err("oversized span should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "requested line span 3 exceeds max_lines 2".to_string(),
            }
        );
    }

    #[test]
    fn rejects_zero_limits() {
        let fixture = TempFixture::new("zero-limits");
        fixture.file("MEMORY.md", "one");

        let mut max_lines = fixture.input("MEMORY.md", 1, None);
        max_lines.max_lines = 0;
        assert_eq!(
            get_memory(&max_lines).expect_err("max_lines=0 should fail"),
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "max_lines must be greater than 0".to_string(),
            }
        );

        let mut max_chars = fixture.input("MEMORY.md", 1, None);
        max_chars.max_chars = 0;
        assert_eq!(
            get_memory(&max_chars).expect_err("max_chars=0 should fail"),
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "max_chars must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn rejects_non_memory_paths_and_cross_scope_reads() {
        let fixture = TempFixture::new("scope");
        fixture.file("memory/users/111/2026-02-10.md", "allowed");
        fixture.file("memory/users/222/2026-02-10.md", "denied");

        let other_scope = get_memory(&fixture.input("memory/users/222/2026-02-10.md", 1, None))
            .expect_err("cross-scope read should fail");
        assert!(matches!(
            other_scope,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("outside user scope")
        ));

        let random_path = get_memory(&fixture.input("notes.md", 1, None))
            .expect_err("non-memory path should fail");
        assert!(matches!(
            random_path,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("must target MEMORY.md")
        ));
    }

    #[test]
    fn rejects_invalid_relative_paths() {
        let fixture = TempFixture::new("invalid-paths");
        fixture.file("MEMORY.md", "ok");

        for bad in [
            "/MEMORY.md",
            "MEMORY.md/",
            "memory/./a.md",
            "memory/../a.md",
        ] {
            let error =
                get_memory(&fixture.input(bad, 1, None)).expect_err("bad path shape should fail");
            assert!(matches!(
                error,
                CrabError::InvariantViolation {
                    context: "memory_get",
                    ..
                }
            ));
        }

        let backslash = get_memory(&fixture.input(r"memory\users\111\2026-02-10.md", 1, None))
            .expect_err("backslash path should fail");
        assert_eq!(
            backslash,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "path must use '/' separators".to_string(),
            }
        );
    }

    #[test]
    fn rejects_invalid_markdown_file_names() {
        let fixture = TempFixture::new("bad-file");
        fixture.file("memory/users/111/2026-02-10.md", "ok");

        for bad in [
            "memory/users/111/2026-02-10.txt",
            "memory/users/111/.md",
            "memory/users/111/hello world.md",
        ] {
            let error = get_memory(&fixture.input(bad, 1, None))
                .expect_err("invalid memory file name should fail");
            assert!(matches!(
                error,
                CrabError::InvariantViolation {
                    context: "memory_get",
                    ..
                }
            ));
        }
    }

    #[test]
    fn rejects_invalid_scope_directory_input() {
        let fixture = TempFixture::new("bad-scope");
        fixture.file("memory/users/111/2026-02-10.md", "ok");

        let mut input = fixture.input("memory/users/111/2026-02-10.md", 1, None);
        input.user_scope_directory = "../oops".to_string();

        let error = get_memory(&input).expect_err("invalid scope directory should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("user_scope_directory")
        ));
    }

    #[test]
    fn rejects_blank_scope_directory_input() {
        let fixture = TempFixture::new("blank-scope");
        fixture.file("MEMORY.md", "ok");

        let mut input = fixture.input("MEMORY.md", 1, None);
        input.user_scope_directory = "   ".to_string();

        let error = get_memory(&input).expect_err("blank scope should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "user_scope_directory must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_blank_path_input() {
        let fixture = TempFixture::new("blank-path");
        fixture.file("MEMORY.md", "ok");

        let mut input = fixture.input("MEMORY.md", 1, None);
        input.path = "  ".to_string();

        let error = get_memory(&input).expect_err("blank path should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "path must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_empty_or_non_directory_workspace_root() {
        let fixture = TempFixture::new("workspace");
        fixture.file("MEMORY.md", "ok");

        let mut empty = fixture.input("MEMORY.md", 1, None);
        empty.workspace_root = PathBuf::new();
        assert_eq!(
            get_memory(&empty).expect_err("empty workspace should fail"),
            CrabError::InvariantViolation {
                context: "memory_get",
                message: "workspace_root must not be empty".to_string(),
            }
        );

        let file_root = TempFixture::new("file-root");
        file_root.file("root-file", "x");
        let mut bad_root = fixture.input("MEMORY.md", 1, None);
        bad_root.workspace_root = file_root.root.join("root-file");
        let error = get_memory(&bad_root).expect_err("file workspace should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("must be a directory")
        ));
    }

    #[test]
    fn rejects_non_file_and_symlink_targets() {
        let fixture = TempFixture::new("target-shape");
        fixture.dir("memory/users/111/2026-02-10.md");

        let error = get_memory(&fixture.input("memory/users/111/2026-02-10.md", 1, None))
            .expect_err("directory targets should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("regular file")
        ));

        fixture.file("backing.md", "hello");
        make_symlink(
            &fixture.root.join("backing.md"),
            &fixture.root.join("memory/users/111/link.md"),
        );

        let symlink_error = get_memory(&fixture.input("memory/users/111/link.md", 1, None))
            .expect_err("symlink target should fail");
        assert!(matches!(
            symlink_error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("must not be a symlink")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn returns_io_error_when_target_metadata_is_inaccessible() {
        let fixture = TempFixture::new("metadata-io");
        fixture.dir("memory/users/111");
        set_mode(&fixture.root.join("memory/users/111"), 0o000);

        let result = get_memory(&fixture.input("memory/users/111/2026-02-10.md", 1, None));
        set_mode(&fixture.root.join("memory/users/111"), 0o700);

        let error = result.expect_err("metadata permission failure should return io error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "memory_get",
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_paths_that_resolve_outside_workspace() {
        let fixture = TempFixture::new("escape");
        fixture.dir("memory/users");
        let outside = std::env::temp_dir().join(format!(
            "crab-memory-get-outside-{}-{}",
            std::process::id(),
            FIXTURE_COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&outside);
        fs::create_dir_all(&outside).expect("outside fixture should be creatable");
        fs::write(outside.join("2026-02-10.md"), "outside").expect("outside fixture should write");
        make_dir_symlink(&outside, &fixture.root.join("memory/users/111"));

        let error = get_memory(&fixture.input("memory/users/111/2026-02-10.md", 1, None))
            .expect_err("escaped path should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_get",
                message
            } if message.contains("must not be a symlink")
        ));
        let _ = fs::remove_dir_all(&outside);
    }

    #[test]
    fn truncates_content_to_max_chars() {
        let fixture = TempFixture::new("truncate");
        fixture.file("MEMORY.md", "alpha\nbeta\ngamma");

        let mut input = fixture.input("MEMORY.md", 1, Some(3));
        input.max_chars = 6;

        let result = get_memory(&input)
            .expect("memory get should succeed")
            .expect("file should exist");
        assert_eq!(result.content, "alpha");
        assert!(result.truncated);
    }

    #[test]
    fn truncates_first_line_when_it_exceeds_max_chars() {
        let fixture = TempFixture::new("truncate-first-line");
        fixture.file("MEMORY.md", "alphabet\nsecond");

        let mut input = fixture.input("MEMORY.md", 1, Some(2));
        input.max_chars = 4;

        let result = get_memory(&input)
            .expect("memory get should succeed")
            .expect("file should exist");
        assert_eq!(result.content, "alph");
        assert!(result.truncated);
    }

    #[test]
    fn supports_blank_lines_and_empty_content_ranges() {
        let fixture = TempFixture::new("blank-lines");
        fixture.file("MEMORY.md", "line1\n\nline3");

        let result = get_memory(&fixture.input("MEMORY.md", 2, Some(2)))
            .expect("memory get should succeed")
            .expect("blank line should be retrievable");
        assert_eq!(result.content, "");
        assert_eq!(result.start_line, 2);
        assert_eq!(result.end_line, 2);
        assert!(!result.truncated);
    }

    #[cfg(unix)]
    #[test]
    fn returns_io_error_when_workspace_is_missing() {
        let fixture = TempFixture::new("workspace-missing");
        fixture.file("MEMORY.md", "ok");

        let mut input = fixture.input("MEMORY.md", 1, None);
        input.workspace_root = fixture.root.join("not-there");
        let error = get_memory(&input).expect_err("missing workspace should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "memory_get",
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn returns_io_error_when_file_open_fails() {
        let fixture = TempFixture::new("file-open");
        fixture.file("MEMORY.md", "secret");
        set_mode(&fixture.root.join("MEMORY.md"), 0o000);

        let result = get_memory(&fixture.input("MEMORY.md", 1, None));
        set_mode(&fixture.root.join("MEMORY.md"), 0o600);

        let error = result.expect_err("file permission failure should return io error");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "memory_get",
                ..
            }
        ));
    }

    #[test]
    fn symlink_component_checker_surfaces_io_errors() {
        let fixture = TempFixture::new("symlink-components-io");

        let error = ensure_no_symlink_components(&fixture.root, "missing/2026-02-10.md")
            .expect_err("missing component should fail");
        assert!(matches!(
            error,
            CrabError::Io {
                context: "memory_get",
                ..
            }
        ));
    }
}
