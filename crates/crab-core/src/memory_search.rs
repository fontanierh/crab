use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::validation::validate_non_empty_text;
use crate::workspace::MEMORY_FILE_NAME;
use crate::{CrabError, CrabResult};

const MEMORY_SEARCH_CONTEXT: &str = "memory_search";
const MEMORY_DIR_NAME: &str = "memory";
const MEMORY_USERS_DIR_NAME: &str = "users";
const MEMORY_GLOBAL_DIR_NAME: &str = "global";
const SNIPPET_CONTEXT_LINES: usize = 6;
const MIN_MATCH_SCORE: f64 = 0.12;

pub const DEFAULT_MEMORY_SEARCH_RESULT_LIMIT: usize = 6;
pub const DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS: usize = 700;
pub const DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemorySearchInput {
    pub workspace_root: PathBuf,
    pub user_scope_directory: String,
    pub include_global_memory: bool,
    pub query: String,
    pub max_results: usize,
    pub max_snippet_chars: usize,
    pub max_files: usize,
}

impl MemorySearchInput {
    #[must_use]
    pub fn with_defaults(
        workspace_root: &Path,
        user_scope_directory: &str,
        include_global_memory: bool,
        query: &str,
    ) -> Self {
        Self {
            workspace_root: workspace_root.to_path_buf(),
            user_scope_directory: user_scope_directory.to_string(),
            include_global_memory,
            query: query.to_string(),
            max_results: DEFAULT_MEMORY_SEARCH_RESULT_LIMIT,
            max_snippet_chars: DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS,
            max_files: DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MemorySearchResult {
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub score: f64,
    pub match_score: f64,
    pub recency_score: f64,
    pub snippet: String,
}

#[derive(Debug, Clone)]
struct SearchDocument {
    path: String,
    date_token: Option<String>,
    lines: Vec<String>,
    normalized_lines: Vec<String>,
    normalized_text: String,
}

#[derive(Debug, Clone)]
struct QueryFeatures {
    normalized_phrase: String,
    terms: Vec<String>,
}

#[derive(Debug, Clone)]
struct ProvisionalResult {
    path: String,
    date_token: Option<String>,
    start_line: u32,
    end_line: u32,
    snippet: String,
    score: f64,
    match_score: f64,
    recency_score: f64,
}

pub fn search_memory(input: &MemorySearchInput) -> CrabResult<Vec<MemorySearchResult>> {
    validate_search_input(input)?;
    validate_workspace_root(&input.workspace_root)?;
    validate_scope_directory(&input.user_scope_directory)?;

    let query = parse_query(&input.query)?;
    let documents = collect_documents(input)?;
    if documents.is_empty() {
        return Ok(Vec::new());
    }

    let mut provisional = documents
        .iter()
        .filter_map(|document| evaluate_document(document, &query, input.max_snippet_chars))
        .collect::<Vec<_>>();

    if provisional.is_empty() {
        return Ok(Vec::new());
    }

    apply_recency_scores(&mut provisional);
    provisional.sort_by(|left, right| {
        compare_f64_desc(left.score, right.score).then_with(|| left.path.cmp(&right.path))
    });
    provisional.truncate(input.max_results);

    Ok(provisional
        .into_iter()
        .map(|entry| MemorySearchResult {
            path: entry.path,
            start_line: entry.start_line,
            end_line: entry.end_line,
            score: entry.score,
            match_score: entry.match_score,
            recency_score: entry.recency_score,
            snippet: entry.snippet,
        })
        .collect())
}

fn validate_search_input(input: &MemorySearchInput) -> CrabResult<()> {
    if input.max_results == 0 {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: "max_results must be greater than 0".to_string(),
        });
    }
    if input.max_snippet_chars == 0 {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: "max_snippet_chars must be greater than 0".to_string(),
        });
    }
    if input.max_files == 0 {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: "max_files must be greater than 0".to_string(),
        });
    }
    Ok(())
}

fn parse_query(value: &str) -> CrabResult<QueryFeatures> {
    validate_non_empty_text(MEMORY_SEARCH_CONTEXT, "query", value)?;

    let normalized_phrase = normalize_for_match(value);
    let mut seen = BTreeSet::new();
    let mut terms = Vec::new();

    for token in normalized_phrase.split_whitespace() {
        if token.len() < 2 {
            continue;
        }
        if seen.insert(token.to_string()) {
            terms.push(token.to_string());
        }
    }

    if terms.is_empty() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: "query must contain at least one searchable token".to_string(),
        });
    }

    Ok(QueryFeatures {
        normalized_phrase,
        terms,
    })
}

fn collect_documents(input: &MemorySearchInput) -> CrabResult<Vec<SearchDocument>> {
    let mut documents = Vec::new();

    let memory_root = input.workspace_root.join(MEMORY_DIR_NAME);
    ensure_directory_if_present(&memory_root)?;

    let users_root = memory_root.join(MEMORY_USERS_DIR_NAME);
    ensure_directory_if_present(&users_root)?;

    let curated_path = input.workspace_root.join(MEMORY_FILE_NAME);
    if let Some(curated) = maybe_read_document(&curated_path, MEMORY_FILE_NAME)? {
        documents.push(curated);
    }

    let user_directory = input
        .workspace_root
        .join(MEMORY_DIR_NAME)
        .join(MEMORY_USERS_DIR_NAME)
        .join(&input.user_scope_directory);
    ensure_directory_if_present(&user_directory)?;
    collect_documents_from_directory(
        &user_directory,
        &format!(
            "{MEMORY_DIR_NAME}/{MEMORY_USERS_DIR_NAME}/{}",
            input.user_scope_directory
        ),
        &mut documents,
    );

    if input.include_global_memory {
        let global_root = memory_root.join(MEMORY_GLOBAL_DIR_NAME);
        ensure_directory_if_present(&global_root)?;

        let global_directory = input
            .workspace_root
            .join(MEMORY_DIR_NAME)
            .join(MEMORY_GLOBAL_DIR_NAME);
        collect_documents_from_directory(
            &global_directory,
            &format!("{MEMORY_DIR_NAME}/{MEMORY_GLOBAL_DIR_NAME}"),
            &mut documents,
        );
    }

    if documents.len() > input.max_files {
        documents = cap_documents_by_recency(documents, input.max_files);
    }

    documents.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(documents)
}

fn ensure_directory_if_present(directory: &Path) -> CrabResult<()> {
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(()),
    };
    if metadata.file_type().is_symlink() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: format!("{} must not be a symlink", display_path(directory)),
        });
    }
    if metadata.is_dir() {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: MEMORY_SEARCH_CONTEXT,
        message: format!("{} must be a directory", display_path(directory)),
    })
}

fn cap_documents_by_recency(
    mut documents: Vec<SearchDocument>,
    max_files: usize,
) -> Vec<SearchDocument> {
    let curated = documents
        .iter()
        .position(|document| document.path == MEMORY_FILE_NAME)
        .map(|index| documents.remove(index));

    documents.sort_by(|left, right| right.date_token.cmp(&left.date_token));

    let keep_limit = max_files.saturating_sub(usize::from(curated.is_some()));
    documents.truncate(keep_limit);

    documents.extend(curated);

    documents
}

fn collect_documents_from_directory(
    directory: &Path,
    relative_prefix: &str,
    documents: &mut Vec<SearchDocument>,
) {
    let Ok(metadata) = fs::symlink_metadata(directory) else {
        return;
    };
    if metadata.file_type().is_symlink() {
        return;
    }
    if !metadata.is_dir() {
        return;
    }

    let mut entries = Vec::new();
    let Ok(directory_entries) = fs::read_dir(directory) else {
        return;
    };
    for entry in directory_entries.flatten() {
        entries.push(entry);
    }

    entries.sort_by_key(|entry| entry.file_name());

    for entry in entries {
        let file_name = entry.file_name().to_string_lossy().into_owned();
        let absolute_path = entry.path();
        if !file_name.ends_with(".md") {
            continue;
        }

        let relative_path = format!("{relative_prefix}/{file_name}");
        if let Ok(Some(document)) = maybe_read_document(&absolute_path, &relative_path) {
            documents.push(document);
        }
    }
}

fn maybe_read_document(path: &Path, relative_path: &str) -> CrabResult<Option<SearchDocument>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return Ok(None),
    };
    if metadata.file_type().is_symlink() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: format!("{} must not be a symlink", display_path(path)),
        });
    }
    if !metadata.is_file() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: format!("{} must be a regular file", display_path(path)),
        });
    }

    let raw = wrap_io(fs::read_to_string(path), path)?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }

    let lines = trimmed
        .lines()
        .map(|line| line.trim_end().to_string())
        .collect::<Vec<_>>();

    let normalized_lines = lines
        .iter()
        .map(|line| normalize_for_match(line))
        .collect::<Vec<_>>();

    Ok(Some(SearchDocument {
        path: relative_path.to_string(),
        date_token: extract_date_token(relative_path),
        normalized_text: normalize_for_match(trimmed),
        lines,
        normalized_lines,
    }))
}

fn evaluate_document(
    document: &SearchDocument,
    query: &QueryFeatures,
    max_snippet_chars: usize,
) -> Option<ProvisionalResult> {
    let total_terms = query.terms.len() as f64;
    let mut matched_terms = BTreeSet::new();
    let mut total_term_hits = 0usize;
    let mut phrase_hits = 0usize;

    let mut best_line_index = 0usize;
    let mut best_line_score = -1.0f64;

    for (index, line) in document.normalized_lines.iter().enumerate() {
        let mut line_matched_terms = 0usize;
        let mut line_term_hits = 0usize;

        for term in &query.terms {
            let hits = count_occurrences(line, term);
            if hits > 0 {
                line_matched_terms += 1;
                line_term_hits += hits;
                matched_terms.insert(term.clone());
            }
        }

        let line_phrase_hits = if query.normalized_phrase.len() >= 3 {
            count_occurrences(line, &query.normalized_phrase)
        } else {
            0
        };

        total_term_hits += line_term_hits;
        phrase_hits += line_phrase_hits;

        let line_coverage = line_matched_terms as f64 / total_terms;
        let line_density = (line_term_hits as f64 / (total_terms * 2.0)).min(1.0);
        let line_phrase_bonus = if line_phrase_hits > 0 { 0.10 } else { 0.0 };
        let line_score = line_coverage * 0.70 + line_density * 0.20 + line_phrase_bonus;

        if line_score > best_line_score
            || ((line_score - best_line_score).abs() < f64::EPSILON && index < best_line_index)
        {
            best_line_score = line_score;
            best_line_index = index;
        }
    }

    if matched_terms.is_empty() {
        return None;
    }

    let coverage = matched_terms.len() as f64 / total_terms;
    let frequency = (total_term_hits as f64 / (total_terms * 4.0)).min(1.0);
    let phrase_present = query.normalized_phrase.len() >= 3
        && (phrase_hits > 0 || document.normalized_text.contains(&query.normalized_phrase));
    let phrase_bonus = if phrase_present { 0.10 } else { 0.0 };

    let match_score = clamp01(coverage * 0.65 + frequency * 0.25 + phrase_bonus);
    if match_score < MIN_MATCH_SCORE {
        return None;
    }

    let (start_line, end_line, snippet) =
        build_snippet(document, best_line_index, max_snippet_chars);

    Some(ProvisionalResult {
        path: document.path.clone(),
        date_token: document.date_token.clone(),
        start_line,
        end_line,
        snippet,
        score: match_score,
        match_score,
        recency_score: 0.0,
    })
}

fn apply_recency_scores(results: &mut [ProvisionalResult]) {
    let mut unique_dates = results
        .iter()
        .filter_map(|result| result.date_token.clone())
        .collect::<Vec<_>>();
    unique_dates.sort();
    unique_dates.dedup();
    unique_dates.sort_by(|left, right| right.cmp(left));

    let ranks = unique_dates
        .into_iter()
        .enumerate()
        .map(|(index, token)| (token, index))
        .collect::<BTreeMap<_, _>>();

    for result in results {
        result.recency_score = if result.path == MEMORY_FILE_NAME {
            0.06
        } else if let Some(date_token) = result.date_token.as_ref() {
            let rank = ranks.get(date_token).copied().unwrap_or(0);
            0.08 / (rank as f64 + 1.0)
        } else {
            0.0
        };

        result.score = clamp01(result.match_score * 0.92 + result.recency_score);
    }
}

fn build_snippet(
    document: &SearchDocument,
    anchor_line_index: usize,
    max_snippet_chars: usize,
) -> (u32, u32, String) {
    debug_assert!(!document.lines.is_empty());

    let mut start = anchor_line_index.saturating_sub(SNIPPET_CONTEXT_LINES / 2);
    let end = usize::min(document.lines.len(), start + SNIPPET_CONTEXT_LINES);
    start = usize::min(start, end.saturating_sub(SNIPPET_CONTEXT_LINES));

    let (snippet, line_count) =
        collect_lines_with_limit(&document.lines[start..end], max_snippet_chars);

    let start_line = start as u32 + 1;
    let safe_line_count = line_count.max(1);
    let end_line = start_line + safe_line_count as u32 - 1;
    let anchor_line = anchor_line_index as u32 + 1;

    if start_line <= anchor_line && anchor_line <= end_line {
        return (start_line, end_line, snippet);
    }

    let focused = document
        .lines
        .get(anchor_line_index)
        .map(|line| line.chars().take(max_snippet_chars).collect::<String>())
        .unwrap_or_default();
    (anchor_line, anchor_line, focused)
}

fn collect_lines_with_limit(lines: &[String], max_chars: usize) -> (String, usize) {
    debug_assert!(!lines.is_empty());

    let mut selected = Vec::new();
    let mut used_chars = 0usize;

    for line in lines {
        let line_chars = line.chars().count();
        let extra_chars = if selected.is_empty() {
            line_chars
        } else {
            line_chars + 1
        };

        if !selected.is_empty() && used_chars + extra_chars > max_chars {
            break;
        }

        if selected.is_empty() && line_chars > max_chars {
            selected.push(line.chars().take(max_chars).collect::<String>());
            break;
        }

        selected.push(line.clone());
        used_chars += extra_chars;
    }

    let line_count = selected.len();
    (selected.join("\n"), line_count)
}

fn normalize_for_match(value: &str) -> String {
    let mut output = String::new();
    let mut last_space = true;

    for character in value.chars() {
        if character.is_alphanumeric() {
            for lowered in character.to_lowercase() {
                output.push(lowered);
            }
            last_space = false;
            continue;
        }

        if !last_space {
            output.push(' ');
            last_space = true;
        }
    }

    output.trim().to_string()
}

fn count_occurrences(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() || haystack.is_empty() {
        return 0;
    }

    let mut count = 0usize;
    let mut start = 0usize;

    while start < haystack.len() {
        let Some(found) = haystack[start..].find(needle) else {
            break;
        };
        count += 1;
        start += found + needle.len();
    }

    count
}

fn extract_date_token(path: &str) -> Option<String> {
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let stem = file_name.trim_end_matches(".md");

    if stem.len() != 10 {
        return None;
    }

    let bytes = stem.as_bytes();
    if bytes[4] != b'-' || bytes[7] != b'-' {
        return None;
    }

    let year = match stem[0..4].parse::<i32>() {
        Ok(value) => value,
        Err(_) => return None,
    };
    let month = match stem[5..7].parse::<u32>() {
        Ok(value) => value,
        Err(_) => return None,
    };
    let day = match stem[8..10].parse::<u32>() {
        Ok(value) => value,
        Err(_) => return None,
    };

    if year < 1 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    Some(stem.to_string())
}

fn compare_f64_desc(left: f64, right: f64) -> Ordering {
    right.partial_cmp(&left).unwrap_or(Ordering::Equal)
}

fn clamp01(value: f64) -> f64 {
    value.clamp(0.0, 1.0)
}

fn validate_workspace_root(workspace_root: &Path) -> CrabResult<()> {
    if workspace_root.as_os_str().is_empty() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: "workspace_root must not be empty".to_string(),
        });
    }

    let metadata = wrap_io(fs::metadata(workspace_root), workspace_root)?;
    if !metadata.is_dir() {
        return Err(CrabError::InvariantViolation {
            context: MEMORY_SEARCH_CONTEXT,
            message: format!("{} must be a directory", display_path(workspace_root)),
        });
    }

    Ok(())
}

fn validate_scope_directory(scope_directory: &str) -> CrabResult<()> {
    validate_non_empty_text(
        MEMORY_SEARCH_CONTEXT,
        "user_scope_directory",
        scope_directory,
    )?;

    if scope_directory
        .chars()
        .all(|character| character.is_ascii_alphanumeric() || character == '-' || character == '_')
    {
        return Ok(());
    }

    Err(CrabError::InvariantViolation {
        context: MEMORY_SEARCH_CONTEXT,
        message: format!(
            "user_scope_directory must contain only ASCII letters, digits, '-' or '_', got {:?}",
            scope_directory
        ),
    })
}

fn wrap_io<T>(result: std::io::Result<T>, path: &Path) -> CrabResult<T> {
    result.map_err(|error| CrabError::Io {
        context: MEMORY_SEARCH_CONTEXT,
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

    use super::{
        collect_documents_from_directory, search_memory, MemorySearchInput,
        DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT, DEFAULT_MEMORY_SEARCH_RESULT_LIMIT,
        DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS,
    };

    static WORKSPACE_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TestWorkspace {
        root: PathBuf,
    }

    impl TestWorkspace {
        fn create(name: &str) -> Self {
            let id = WORKSPACE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "crab-memory-search-{name}-{}-{id}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("workspace fixture should be creatable");
            Self { root }
        }

        fn write(&self, relative_path: &str, content: &str) {
            let absolute = self.root.join(relative_path);
            let parent = absolute.parent().unwrap_or(Path::new("."));
            fs::create_dir_all(parent).expect("parent should be creatable");
            fs::write(absolute, content).expect("fixture file should be writable");
        }

        fn mkdir(&self, relative_path: &str) {
            fs::create_dir_all(self.root.join(relative_path))
                .expect("directory should be creatable");
        }

        #[cfg(unix)]
        fn chmod(&self, relative_path: &str, mode: u32) {
            use std::os::unix::fs::PermissionsExt;

            let absolute = self.root.join(relative_path);
            let mut permissions = fs::metadata(&absolute)
                .expect("path should exist for chmod")
                .permissions();
            permissions.set_mode(mode);
            fs::set_permissions(absolute, permissions).expect("chmod should succeed");
        }

        fn input(&self, query: &str) -> MemorySearchInput {
            MemorySearchInput {
                workspace_root: self.root.clone(),
                user_scope_directory: "111".to_string(),
                include_global_memory: true,
                query: query.to_string(),
                max_results: 6,
                max_snippet_chars: 180,
                max_files: 64,
            }
        }
    }

    impl Drop for TestWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn paths(results: &[super::MemorySearchResult]) -> Vec<String> {
        results.iter().map(|result| result.path.clone()).collect()
    }

    #[test]
    fn defaults_are_stable() {
        let input = MemorySearchInput::with_defaults(Path::new("/tmp"), "owner", true, "query");
        assert_eq!(input.max_results, DEFAULT_MEMORY_SEARCH_RESULT_LIMIT);
        assert_eq!(
            input.max_snippet_chars,
            DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS
        );
        assert_eq!(input.max_files, DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT);
    }

    #[test]
    fn searches_curated_user_and_global_memory() {
        let workspace = TestWorkspace::create("surface");
        workspace.write("MEMORY.md", "Owner timezone is Europe/Paris.");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "Machine location is Lyon.",
        );
        workspace.write(
            "memory/global/2026-02-10.md",
            "Deployment checklist is stable.",
        );

        let results =
            search_memory(&workspace.input("timezone paris")).expect("search should succeed");
        assert_eq!(results[0].path, "MEMORY.md");

        let deployment =
            search_memory(&workspace.input("deployment checklist")).expect("search should succeed");
        assert!(paths(&deployment)
            .iter()
            .any(|path| path == "memory/global/2026-02-10.md"));
    }

    #[test]
    fn excludes_other_user_memory_scope() {
        let workspace = TestWorkspace::create("scope");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "owner memory reminder and notes",
        );
        workspace.write("memory/users/222/2026-02-10.md", "private teammate memory");

        let results = search_memory(&workspace.input("memory")).expect("search should succeed");
        assert!(paths(&results)
            .iter()
            .all(|path| !path.contains("memory/users/222/")));
    }

    #[test]
    fn can_disable_global_memory() {
        let workspace = TestWorkspace::create("disable-global");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "owner preference: concise replies",
        );
        workspace.write("memory/global/2026-02-10.md", "global broadcast message");

        let mut input = workspace.input("broadcast");
        input.include_global_memory = false;

        let results = search_memory(&input).expect("search should succeed");
        assert!(results.is_empty());
    }

    #[test]
    fn recency_score_prefers_newer_dated_files() {
        let workspace = TestWorkspace::create("recency");
        workspace.write(
            "memory/users/111/2026-02-08.md",
            "checkpoint recovery playbook",
        );
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "checkpoint recovery playbook",
        );

        let results =
            search_memory(&workspace.input("checkpoint recovery")).expect("search should succeed");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].path, "memory/users/111/2026-02-10.md");
        assert!(results[0].recency_score > results[1].recency_score);
    }

    #[test]
    fn punctuation_only_query_is_rejected() {
        let workspace = TestWorkspace::create("bad-query");
        workspace.write("memory/users/111/2026-02-10.md", "some data");

        let error = search_memory(&workspace.input("!!! ... ???"))
            .expect_err("punctuation-only query should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "query must contain at least one searchable token".to_string(),
            }
        );
    }

    #[test]
    fn blank_query_is_rejected() {
        let workspace = TestWorkspace::create("blank-query");
        workspace.write("memory/users/111/2026-02-10.md", "some data");

        let error = search_memory(&workspace.input("   ")).expect_err("blank query should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "query must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_zero_limits() {
        let workspace = TestWorkspace::create("zero-limits");
        workspace.write("memory/users/111/2026-02-10.md", "some data");

        let mut zero_results = workspace.input("some");
        zero_results.max_results = 0;
        assert_eq!(
            search_memory(&zero_results).expect_err("max_results=0 should fail"),
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "max_results must be greater than 0".to_string(),
            }
        );

        let mut zero_chars = workspace.input("some");
        zero_chars.max_snippet_chars = 0;
        assert_eq!(
            search_memory(&zero_chars).expect_err("max_snippet_chars=0 should fail"),
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "max_snippet_chars must be greater than 0".to_string(),
            }
        );

        let mut zero_files = workspace.input("some");
        zero_files.max_files = 0;
        assert_eq!(
            search_memory(&zero_files).expect_err("max_files=0 should fail"),
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "max_files must be greater than 0".to_string(),
            }
        );
    }

    #[test]
    fn returns_empty_when_no_memory_files_exist() {
        let workspace = TestWorkspace::create("no-files");
        let results = search_memory(&workspace.input("whatever"))
            .expect("search should succeed when memory is absent");
        assert!(results.is_empty());
    }

    #[test]
    fn workspace_validation_catches_missing_and_non_directory_paths() {
        let mut missing = MemorySearchInput::with_defaults(
            Path::new("/tmp/crab-core-missing-memory-search-workspace"),
            "111",
            true,
            "query",
        );
        missing.max_files = 1;

        let missing_error =
            search_memory(&missing).expect_err("missing workspace should return io error");
        assert!(matches!(
            missing_error,
            CrabError::Io {
                context: "memory_search",
                path: Some(_),
                ..
            }
        ));

        let workspace = TestWorkspace::create("file-root");
        let root_file = workspace.root.join("root-file");
        fs::write(&root_file, "file").expect("root file should be writable");

        let mut input = workspace.input("query");
        input.workspace_root = root_file;

        let error = search_memory(&input).expect_err("file root should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("must be a directory")
        ));
    }

    #[test]
    fn rejects_invalid_scope_directory_name() {
        let workspace = TestWorkspace::create("bad-scope");
        workspace.write("memory/users/111/2026-02-10.md", "hello");

        let mut input = workspace.input("hello");
        input.user_scope_directory = "../hack".to_string();

        let error = search_memory(&input).expect_err("invalid scope name should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("user_scope_directory must contain only ASCII letters")
        ));
    }

    #[test]
    fn rejects_blank_scope_directory_name() {
        let workspace = TestWorkspace::create("blank-scope");
        workspace.write("memory/users/111/2026-02-10.md", "hello");

        let mut input = workspace.input("hello");
        input.user_scope_directory = " ".to_string();

        let error = search_memory(&input).expect_err("blank scope directory should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "user_scope_directory must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_blank_workspace_root_path() {
        let mut input = MemorySearchInput::with_defaults(Path::new("/tmp"), "111", true, "hello");
        input.workspace_root = PathBuf::new();

        let error = search_memory(&input).expect_err("blank workspace root should fail");
        assert_eq!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message: "workspace_root must not be empty".to_string(),
            }
        );
    }

    #[test]
    fn rejects_non_file_or_symlink_curated_memory() {
        let workspace = TestWorkspace::create("curated-shape");
        workspace.mkdir("MEMORY.md");
        workspace.write("memory/users/111/2026-02-10.md", "hello");

        let error =
            search_memory(&workspace.input("hello")).expect_err("directory MEMORY.md should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("MEMORY.md") && message.contains("regular file")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_curated_memory() {
        use std::os::unix::fs as unix_fs;

        let workspace = TestWorkspace::create("curated-symlink");
        workspace.write("memory/users/111/2026-02-10.md", "hello");
        workspace.write("real-memory.md", "actual contents");
        unix_fs::symlink(
            workspace.root.join("real-memory.md"),
            workspace.root.join("MEMORY.md"),
        )
        .expect("symlink should be creatable");

        let error =
            search_memory(&workspace.input("actual")).expect_err("symlink MEMORY.md should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("must not be a symlink")
        ));
    }

    #[test]
    fn rejects_non_directory_memory_roots_when_present() {
        let workspace = TestWorkspace::create("bad-memory-roots");
        workspace.write("memory/users", "file-not-directory");

        let error = search_memory(&workspace.input("anything"))
            .expect_err("non-directory users root should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("memory/users") && message.contains("directory")
        ));
    }

    #[test]
    fn rejects_non_directory_memory_root_when_present() {
        let workspace = TestWorkspace::create("memory-root-file");
        workspace.write("memory", "file-not-directory");

        let error =
            search_memory(&workspace.input("anything")).expect_err("memory root file should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("/memory") && message.contains("directory")
        ));
    }

    #[test]
    fn rejects_non_directory_user_scope_directory() {
        let workspace = TestWorkspace::create("scope-file");
        workspace.mkdir("memory/users");
        workspace.write("memory/users/111", "scope-file");

        let error = search_memory(&workspace.input("anything"))
            .expect_err("file-backed scope directory should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("memory/users/111") && message.contains("directory")
        ));
    }

    #[test]
    fn rejects_non_directory_global_root_when_enabled() {
        let workspace = TestWorkspace::create("global-root-file");
        workspace.mkdir("memory");
        workspace.write("memory/global", "file-not-directory");
        workspace.write("memory/users/111/2026-02-10.md", "local note");

        let error = search_memory(&workspace.input("note"))
            .expect_err("file-backed global root should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("memory/global") && message.contains("directory")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_memory_roots() {
        use std::os::unix::fs as unix_fs;

        let workspace = TestWorkspace::create("memory-root-symlink");
        workspace.mkdir("real-memory");
        unix_fs::symlink(
            workspace.root.join("real-memory"),
            workspace.root.join("memory"),
        )
        .expect("memory symlink should be creatable");

        let error = search_memory(&workspace.input("anything"))
            .expect_err("symlink memory root should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("must not be a symlink")
        ));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_symlink_user_scope_directory() {
        use std::os::unix::fs as unix_fs;

        let workspace = TestWorkspace::create("scope-symlink");
        workspace.mkdir("memory/users");
        workspace.mkdir("other-scope");
        unix_fs::symlink(
            workspace.root.join("other-scope"),
            workspace.root.join("memory/users/111"),
        )
        .expect("scope symlink should be creatable");

        let error = search_memory(&workspace.input("anything"))
            .expect_err("symlink scope directory should fail");
        assert!(matches!(
            error,
            CrabError::InvariantViolation {
                context: "memory_search",
                message,
            } if message.contains("must not be a symlink")
        ));
    }

    #[test]
    fn respects_file_cap_and_keeps_curated_memory() {
        let workspace = TestWorkspace::create("file-cap");
        workspace.write("MEMORY.md", "owner role and deploy mission");
        workspace.write("memory/users/111/2026-02-08.md", "deploy note");
        workspace.write("memory/users/111/2026-02-09.md", "deploy note");
        workspace.write("memory/users/111/2026-02-10.md", "deploy note");

        let mut input = workspace.input("deploy");
        input.max_files = 2;

        let results = search_memory(&input).expect("search should succeed");
        assert!(paths(&results).iter().any(|path| path == "MEMORY.md"));
        assert!(paths(&results)
            .iter()
            .all(|path| path != "memory/users/111/2026-02-08.md"));
    }

    #[test]
    fn snippet_is_windowed_and_truncated() {
        let workspace = TestWorkspace::create("snippet");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "line one\nline two\nline three with checkpoint recovery marker\nline four\nline five",
        );

        let mut input = workspace.input("checkpoint recovery marker");
        input.max_snippet_chars = 25;

        let results = search_memory(&input).expect("search should succeed");
        assert_eq!(results.len(), 1);
        let first = &results[0];
        assert!(first.start_line <= 3);
        assert!(first.end_line >= 3);
        assert!(first.snippet.chars().count() <= 25);
    }

    #[test]
    fn long_first_line_snippet_truncates_in_place() {
        let workspace = TestWorkspace::create("long-line");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "deploy deploy deploy deploy deploy deploy deploy deploy",
        );

        let mut input = workspace.input("deploy");
        input.max_snippet_chars = 10;

        let results = search_memory(&input).expect("search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].start_line, 1);
        assert_eq!(results[0].end_line, 1);
        assert!(results[0].snippet.chars().count() <= 10);
    }

    #[test]
    fn one_line_documents_keep_anchor_window() {
        let workspace = TestWorkspace::create("one-line");
        workspace.write("memory/users/111/note.md", "owner deploy note");

        let results =
            search_memory(&workspace.input("deploy note")).expect("search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].start_line, 1);
        assert_eq!(results[0].end_line, 1);
    }

    #[test]
    fn undated_paths_have_zero_recency_score() {
        let workspace = TestWorkspace::create("undated");
        workspace.write("memory/users/111/note.md", "owner deploy note");

        let results = search_memory(&workspace.input("deploy")).expect("search should succeed");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].recency_score, 0.0);
    }

    #[test]
    fn deterministic_tie_breaking_uses_path_then_line() {
        let workspace = TestWorkspace::create("tie-break");
        workspace.write("memory/users/111/2026-02-09.md", "same words here");
        workspace.write("memory/users/111/2026-02-10.md", "same words here");

        let results = search_memory(&workspace.input("same words")).expect("search should succeed");
        assert_eq!(
            paths(&results),
            vec![
                "memory/users/111/2026-02-10.md".to_string(),
                "memory/users/111/2026-02-09.md".to_string(),
            ]
        );
    }

    #[test]
    fn query_matching_is_case_insensitive_and_phrase_aware() {
        let workspace = TestWorkspace::create("case-phrase");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "The Owner Default Backend Is Claude for this machine.",
        );

        let results = search_memory(&workspace.input("owner default backend"))
            .expect("search should succeed");
        assert_eq!(results.len(), 1);
        assert!(results[0].match_score > 0.5);
    }

    #[test]
    fn query_parser_ignores_short_tokens_and_deduplicates_terms() {
        let workspace = TestWorkspace::create("query-parser");
        workspace.write("memory/users/111/2026-02-10.md", "deploy plan status");

        let results =
            search_memory(&workspace.input("a deploy deploy")).expect("search should succeed");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn phrase_length_below_three_skips_phrase_boost_path() {
        let workspace = TestWorkspace::create("short-phrase");
        workspace.write("memory/users/111/2026-02-10.md", "ab marker");

        let results = search_memory(&workspace.input("ab")).expect("search should succeed");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn low_match_score_entries_are_filtered_out() {
        let workspace = TestWorkspace::create("min-score");
        workspace.write("memory/users/111/2026-02-10.md", "onlyone");

        let results =
            search_memory(&workspace.input("onlyone two three four five six seven eight nine ten"))
                .expect("search should succeed");
        assert!(results.is_empty());
    }

    #[test]
    fn empty_lines_are_handled_in_grep_matching() {
        let workspace = TestWorkspace::create("empty-line");
        workspace.write("memory/users/111/2026-02-10.md", "deploy\n\nrecovery");

        let results = search_memory(&workspace.input("deploy")).expect("search should succeed");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn search_skips_empty_memory_files_after_trim() {
        let workspace = TestWorkspace::create("empty-files");
        workspace.write("memory/users/111/2026-02-10.md", "   \n   ");

        let results = search_memory(&workspace.input("anything")).expect("search should succeed");
        assert!(results.is_empty());
    }

    #[test]
    fn search_ignores_non_markdown_files() {
        let workspace = TestWorkspace::create("non-md");
        workspace.write("memory/users/111/2026-02-10.txt", "deploy plan");
        workspace.write("memory/users/111/2026-02-10.md", "actual deploy plan");

        let results = search_memory(&workspace.input("deploy")).expect("search should succeed");
        assert_eq!(
            paths(&results),
            vec!["memory/users/111/2026-02-10.md".to_string()]
        );
    }

    #[test]
    fn search_ignores_non_file_directory_entries() {
        let workspace = TestWorkspace::create("dir-entry");
        workspace.mkdir("memory/users/111/archive");
        workspace.write("memory/users/111/2026-02-10.md", "actual deploy plan");

        let results = search_memory(&workspace.input("deploy")).expect("search should succeed");
        assert_eq!(
            paths(&results),
            vec!["memory/users/111/2026-02-10.md".to_string()]
        );
    }

    #[test]
    fn helper_collection_skips_non_directory_paths() {
        let workspace = TestWorkspace::create("helper-non-dir");
        workspace.write("not-a-directory", "plain file");

        let mut documents = Vec::new();
        collect_documents_from_directory(
            &workspace.root.join("not-a-directory"),
            "memory/users/111",
            &mut documents,
        );

        assert!(documents.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn helper_collection_skips_symlink_paths() {
        use std::os::unix::fs as unix_fs;

        let workspace = TestWorkspace::create("helper-symlink");
        workspace.mkdir("real-memory-dir");
        workspace.write("real-memory-dir/2026-02-10.md", "deploy note");
        unix_fs::symlink(
            workspace.root.join("real-memory-dir"),
            workspace.root.join("memory-link"),
        )
        .expect("symlink should be creatable");

        let mut documents = Vec::new();
        collect_documents_from_directory(
            &workspace.root.join("memory-link"),
            "memory/users/111",
            &mut documents,
        );

        assert!(documents.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn helper_collection_skips_unreadable_directories() {
        let workspace = TestWorkspace::create("helper-unreadable");
        workspace.mkdir("memory/users/111");
        workspace.write("memory/users/111/2026-02-10.md", "deploy note");
        workspace.chmod("memory/users/111", 0o000);

        let mut documents = Vec::new();
        collect_documents_from_directory(
            &workspace.root.join("memory/users/111"),
            "memory/users/111",
            &mut documents,
        );
        workspace.chmod("memory/users/111", 0o755);

        assert!(documents.is_empty());
    }

    #[test]
    fn equal_scores_fall_back_to_path_ordering() {
        let workspace = TestWorkspace::create("equal-scores");
        workspace.write("memory/users/111/note-a.md", "shared phrase");
        workspace.write("memory/users/111/note-b.md", "shared phrase");

        let results =
            search_memory(&workspace.input("shared phrase")).expect("search should succeed");
        assert_eq!(
            paths(&results),
            vec![
                "memory/users/111/note-a.md".to_string(),
                "memory/users/111/note-b.md".to_string(),
            ]
        );
    }

    #[test]
    fn invalid_date_names_do_not_break_search() {
        let workspace = TestWorkspace::create("invalid-date");
        workspace.write("memory/users/111/2026-99-99.md", "deploy");
        workspace.write("memory/users/111/2026-aa-10.md", "deploy");
        workspace.write("memory/users/111/abcd-02-10.md", "deploy");
        workspace.write("memory/users/111/2026-02-xx.md", "deploy");
        workspace.write("memory/users/111/not-a-date.md", "deploy");

        let results = search_memory(&workspace.input("deploy")).expect("search should succeed");
        assert_eq!(results.len(), 5);
    }

    #[cfg(unix)]
    #[test]
    fn unreadable_markdown_file_is_skipped() {
        let workspace = TestWorkspace::create("permission");
        workspace.write("memory/users/111/2026-02-10.md", "deploy");
        workspace.chmod("memory/users/111/2026-02-10.md", 0o000);

        let results = search_memory(&workspace.input("deploy"));
        workspace.chmod("memory/users/111/2026-02-10.md", 0o644);

        let results = results.expect("search should not fail on unreadable entries");
        assert!(results.is_empty());
    }

    #[test]
    fn search_drops_unmatched_query() {
        let workspace = TestWorkspace::create("unmatched");
        workspace.write(
            "memory/users/111/2026-02-10.md",
            "owner likes concise updates",
        );

        let results = search_memory(&workspace.input("zqxxmn")).expect("search should succeed");
        assert!(results.is_empty());
    }
}
