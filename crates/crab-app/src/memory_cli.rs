use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::PathBuf;

use crab_core::{
    get_memory, search_memory, MemoryGetInput, MemorySearchInput, DEFAULT_MEMORY_GET_MAX_CHARS,
    DEFAULT_MEMORY_GET_MAX_LINES, DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT,
    DEFAULT_MEMORY_SEARCH_RESULT_LIMIT, DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS,
};
use serde_json::{json, Value};

const SEARCH_USAGE: &str = "Usage:
  crab-memory-search --workspace-root <path> --user-scope <scope> --query <text> [--max-results <n>] [--max-snippet-chars <n>] [--max-files <n>] [--no-global]

Flags:
  --workspace-root      workspace directory containing MEMORY.md and memory/
  --user-scope          user memory scope directory (for example: 1234567890)
  --query               search query text
  --max-results         optional positive integer (default: 6)
  --max-snippet-chars   optional positive integer (default: 700)
  --max-files           optional positive integer (default: 512)
  --no-global           disable memory/global lookup
  --help                show this help";

const GET_USAGE: &str = "Usage:
  crab-memory-get --workspace-root <path> --user-scope <scope> --path <relative-path> --start-line <n> [--end-line <n>] [--max-lines <n>] [--max-chars <n>] [--no-global]

Flags:
  --workspace-root      workspace directory containing MEMORY.md and memory/
  --user-scope          user memory scope directory (for example: 1234567890)
  --path                relative memory path (MEMORY.md | memory/users/<scope>/*.md | memory/global/*.md)
  --start-line          1-based starting line number
  --end-line            optional inclusive end line number
  --max-lines           optional positive integer (default: 64)
  --max-chars           optional positive integer (default: 4000)
  --no-global           disable retrieval from memory/global paths
  --help                show this help";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliCommand {
    Search,
    Get,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct ParsedFlags {
    values: BTreeMap<String, String>,
    switches: BTreeSet<String>,
}

pub fn run_memory_search_cli<I, S>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    run_memory_cli(args, stdout, stderr, CliCommand::Search)
}

pub fn run_memory_get_cli<I, S>(args: I, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    run_memory_cli(args, stdout, stderr, CliCommand::Get)
}

fn run_memory_cli<I, S>(
    args: I,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
    command: CliCommand,
) -> i32
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    let argv = collect_args(args);
    let command_args = argv.get(1..).unwrap_or(&[]);

    if is_help_request(command_args) {
        let _ = writeln!(stdout, "{}", usage_for(command));
        return 0;
    }

    let response = match command {
        CliCommand::Search => execute_search(command_args),
        CliCommand::Get => execute_get(command_args),
    };

    match response {
        Ok(json_value) => match write_json(stdout, &json_value) {
            Ok(()) => 0,
            Err(message) => write_error(stderr, &message, command),
        },
        Err(message) => write_error(stderr, &message, command),
    }
}

fn execute_search(args: &[String]) -> Result<Value, String> {
    let input = parse_search_input(args)?;
    let query = input.query.clone();
    let include_global_memory = input.include_global_memory;
    let results = search_memory(&input).map_err(|error| error.to_string())?;

    let serialized_results = results
        .into_iter()
        .map(|entry| {
            json!({
                "path": entry.path,
                "start_line": entry.start_line,
                "end_line": entry.end_line,
                "score": entry.score,
                "match_score": entry.match_score,
                "recency_score": entry.recency_score,
                "snippet": entry.snippet,
            })
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "query": query,
        "include_global_memory": include_global_memory,
        "results": serialized_results,
    }))
}

fn execute_get(args: &[String]) -> Result<Value, String> {
    let input = parse_get_input(args)?;
    let requested_path = input.path.clone();
    let include_global_memory = input.include_global_memory;
    let maybe_result = get_memory(&input).map_err(|error| error.to_string())?;

    match maybe_result {
        Some(result) => Ok(json!({
            "found": true,
            "include_global_memory": include_global_memory,
            "path": result.path,
            "start_line": result.start_line,
            "end_line": result.end_line,
            "truncated": result.truncated,
            "content": result.content,
        })),
        None => Ok(json!({
            "found": false,
            "include_global_memory": include_global_memory,
            "path": requested_path,
        })),
    }
}

fn parse_search_input(args: &[String]) -> Result<MemorySearchInput, String> {
    let parsed = parse_flags(
        args,
        &[
            "--workspace-root",
            "--user-scope",
            "--query",
            "--max-results",
            "--max-snippet-chars",
            "--max-files",
        ],
        &["--no-global"],
    )?;

    Ok(MemorySearchInput {
        workspace_root: PathBuf::from(require_flag(&parsed, "--workspace-root")?),
        user_scope_directory: require_flag(&parsed, "--user-scope")?,
        include_global_memory: !parsed.switches.contains("--no-global"),
        query: require_flag(&parsed, "--query")?,
        max_results: optional_usize(&parsed, "--max-results", DEFAULT_MEMORY_SEARCH_RESULT_LIMIT)?,
        max_snippet_chars: optional_usize(
            &parsed,
            "--max-snippet-chars",
            DEFAULT_MEMORY_SEARCH_SNIPPET_MAX_CHARS,
        )?,
        max_files: optional_usize(&parsed, "--max-files", DEFAULT_MEMORY_SEARCH_MAX_FILE_COUNT)?,
    })
}

fn parse_get_input(args: &[String]) -> Result<MemoryGetInput, String> {
    let parsed = parse_flags(
        args,
        &[
            "--workspace-root",
            "--user-scope",
            "--path",
            "--start-line",
            "--end-line",
            "--max-lines",
            "--max-chars",
        ],
        &["--no-global"],
    )?;

    Ok(MemoryGetInput {
        workspace_root: PathBuf::from(require_flag(&parsed, "--workspace-root")?),
        user_scope_directory: require_flag(&parsed, "--user-scope")?,
        include_global_memory: !parsed.switches.contains("--no-global"),
        path: require_flag(&parsed, "--path")?,
        start_line: required_u32(&parsed, "--start-line")?,
        end_line: parsed
            .values
            .get("--end-line")
            .map(|value| parse_positive_u32("--end-line", value))
            .transpose()?,
        max_lines: optional_u32(&parsed, "--max-lines", DEFAULT_MEMORY_GET_MAX_LINES)?,
        max_chars: optional_usize(&parsed, "--max-chars", DEFAULT_MEMORY_GET_MAX_CHARS)?,
    })
}

fn parse_flags(
    args: &[String],
    value_flags: &[&str],
    switch_flags: &[&str],
) -> Result<ParsedFlags, String> {
    let mut parsed = ParsedFlags::default();
    let mut index = 0usize;

    while let Some(flag) = args.get(index) {
        if !flag.starts_with("--") {
            return Err(format!("unexpected positional argument {:?}", flag));
        }
        if switch_flags.contains(&flag.as_str()) {
            if !parsed.switches.insert(flag.clone()) {
                return Err(format!("duplicate flag {}", flag));
            }
            index = index.saturating_add(1);
            continue;
        }
        if !value_flags.contains(&flag.as_str()) {
            return Err(format!("unknown flag {}", flag));
        }
        let Some(value) = args.get(index.saturating_add(1)) else {
            return Err(format!("missing value for flag {}", flag));
        };
        if value.starts_with("--") {
            return Err(format!("missing value for flag {}", flag));
        }
        if parsed.values.insert(flag.clone(), value.clone()).is_some() {
            return Err(format!("duplicate flag {}", flag));
        }
        index = index.saturating_add(2);
    }

    Ok(parsed)
}

fn require_flag(parsed: &ParsedFlags, flag: &'static str) -> Result<String, String> {
    let value = parsed
        .values
        .get(flag)
        .ok_or_else(|| format!("missing required flag {}", flag))?;
    if value.trim().is_empty() {
        return Err(format!("{} must not be empty", flag));
    }
    Ok(value.clone())
}

fn required_u32(parsed: &ParsedFlags, flag: &'static str) -> Result<u32, String> {
    let value = require_flag(parsed, flag)?;
    parse_positive_u32(flag, &value)
}

fn optional_u32(parsed: &ParsedFlags, flag: &'static str, default: u32) -> Result<u32, String> {
    parsed
        .values
        .get(flag)
        .map_or(Ok(default), |value| parse_positive_u32(flag, value))
}

fn optional_usize(
    parsed: &ParsedFlags,
    flag: &'static str,
    default: usize,
) -> Result<usize, String> {
    parsed
        .values
        .get(flag)
        .map_or(Ok(default), |value| parse_positive_usize(flag, value))
}

fn parse_positive_u32(flag: &'static str, value: &str) -> Result<u32, String> {
    let parsed = value
        .parse::<u32>()
        .map_err(|_| format!("{} must be a positive integer, got {:?}", flag, value))?;
    if parsed == 0 {
        return Err(format!(
            "{} must be a positive integer, got {:?}",
            flag, value
        ));
    }
    Ok(parsed)
}

fn parse_positive_usize(flag: &'static str, value: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("{} must be a positive integer, got {:?}", flag, value))?;
    if parsed == 0 {
        return Err(format!(
            "{} must be a positive integer, got {:?}",
            flag, value
        ));
    }
    Ok(parsed)
}

fn collect_args<I, S>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    args.into_iter().map(Into::into).collect()
}

fn is_help_request(args: &[String]) -> bool {
    args.iter().any(|value| value == "--help" || value == "-h")
}

fn usage_for(command: CliCommand) -> &'static str {
    match command {
        CliCommand::Search => SEARCH_USAGE,
        CliCommand::Get => GET_USAGE,
    }
}

fn write_json(stdout: &mut dyn Write, value: &Value) -> Result<(), String> {
    serde_json::to_writer(&mut *stdout, value)
        .map_err(|error| format!("failed to serialize command response: {error}"))?;
    writeln!(stdout).map_err(|error| format!("failed to write command response: {error}"))?;
    Ok(())
}

fn write_error(stderr: &mut dyn Write, message: &str, command: CliCommand) -> i32 {
    let _ = writeln!(stderr, "error: {}", message);
    let _ = writeln!(stderr);
    let _ = writeln!(stderr, "{}", usage_for(command));
    2
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::{self, ErrorKind, Write};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use serde_json::Value;

    use super::{run_memory_get_cli, run_memory_search_cli};

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    struct TempWorkspace {
        root: PathBuf,
    }

    impl TempWorkspace {
        fn new(label: &str) -> Self {
            let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "crab-memory-cli-{label}-{}-{counter}",
                std::process::id()
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("temp workspace root should be creatable");
            Self { root }
        }

        fn write_file(&self, relative_path: &str, content: &str) {
            let absolute = self.root.join(relative_path);
            let parent = absolute.parent().expect("joined path should have a parent");
            fs::create_dir_all(parent).expect("parent should be creatable");
            fs::write(absolute, content).expect("file should be writable");
        }
    }

    impl Drop for TempWorkspace {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[derive(Default)]
    struct FailWriter;

    impl Write for FailWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "intentional write failure",
            ))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::new(
                ErrorKind::BrokenPipe,
                "intentional flush failure",
            ))
        }
    }

    #[derive(Default)]
    struct FailOnNewlineWriter;

    impl Write for FailOnNewlineWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            if buf == b"\n" {
                return Err(io::Error::new(
                    ErrorKind::BrokenPipe,
                    "intentional newline write failure",
                ));
            }
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn decode_json(bytes: &[u8]) -> Value {
        serde_json::from_slice(bytes).expect("stdout should contain valid JSON")
    }

    fn run_search(args: &[&str]) -> (i32, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status = run_memory_search_cli(
            args.iter().copied().map(String::from).collect::<Vec<_>>(),
            &mut stdout,
            &mut stderr,
        );
        (
            status,
            String::from_utf8(stdout).expect("stdout should be utf-8"),
            String::from_utf8(stderr).expect("stderr should be utf-8"),
        )
    }

    fn run_get(args: &[&str]) -> (i32, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status = run_memory_get_cli(
            args.iter().copied().map(String::from).collect::<Vec<_>>(),
            &mut stdout,
            &mut stderr,
        );
        (
            status,
            String::from_utf8(stdout).expect("stdout should be utf-8"),
            String::from_utf8(stderr).expect("stderr should be utf-8"),
        )
    }

    #[test]
    fn search_cli_help_mode_prints_usage() {
        let (status, stdout, stderr) = run_search(&["crab-memory-search", "--help"]);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stdout.contains("crab-memory-search"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn search_cli_returns_json_results() {
        let workspace = TempWorkspace::new("search-results");
        workspace.write_file(
            "MEMORY.md",
            "# Curated Memory\nThe owner prefers Rust and deterministic tests.\n",
        );
        workspace.write_file(
            "memory/users/user-1/2026-02-10.md",
            "Discussed lane scheduler behavior and Rust adapter polish.\n",
        );
        workspace.write_file(
            "memory/global/2026-02-09.md",
            "Global note about deployment checklist.\n",
        );

        let workspace_root = workspace.root.to_string_lossy().to_string();
        let (status, stdout, stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            &workspace_root,
            "--user-scope",
            "user-1",
            "--query",
            "Rust scheduler",
            "--max-results",
            "3",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());

        let response = decode_json(stdout.as_bytes());
        assert_eq!(response["query"], "Rust scheduler");
        assert_eq!(response["include_global_memory"], true);
        let results = response["results"]
            .as_array()
            .expect("results should be an array");
        assert!(!results.is_empty());
        assert!(results
            .iter()
            .any(|entry| entry["path"] == "memory/users/user-1/2026-02-10.md"));
    }

    #[test]
    fn search_cli_respects_no_global_flag() {
        let workspace = TempWorkspace::new("search-no-global");
        workspace.write_file("MEMORY.md", "Curated facts.\n");
        workspace.write_file(
            "memory/users/user-1/2026-02-10.md",
            "No matching term here.\n",
        );
        workspace.write_file(
            "memory/global/2026-02-10.md",
            "Only global file mentions launch-window.\n",
        );

        let workspace_root = workspace.root.to_string_lossy().to_string();
        let (status, stdout, stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            &workspace_root,
            "--user-scope",
            "user-1",
            "--query",
            "launch-window",
            "--no-global",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());
        let response = decode_json(stdout.as_bytes());
        assert_eq!(response["include_global_memory"], false);
        let results = response["results"]
            .as_array()
            .expect("results should be an array");
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn search_cli_reports_parse_errors() {
        let (missing_status, _, missing_stderr) = run_search(&["crab-memory-search"]);
        assert_eq!(missing_status, 2);
        assert!(missing_stderr.contains("missing required flag --workspace-root"));
        assert!(missing_stderr.contains("crab-memory-search"));

        let (unknown_status, _, unknown_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
            "rust",
            "--wat",
            "nope",
        ]);
        assert_eq!(unknown_status, 2);
        assert!(unknown_stderr.contains("unknown flag --wat"));
    }

    #[test]
    fn search_cli_covers_additional_flag_validation_paths() {
        let (positional_status, _, positional_stderr) =
            run_search(&["crab-memory-search", "unexpected"]);
        assert_eq!(positional_status, 2);
        assert!(positional_stderr.contains("unexpected positional argument"));

        let (duplicate_switch_status, _, duplicate_switch_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
            "rust",
            "--no-global",
            "--no-global",
        ]);
        assert_eq!(duplicate_switch_status, 2);
        assert!(duplicate_switch_stderr.contains("duplicate flag --no-global"));

        let (missing_value_status, _, missing_value_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
        ]);
        assert_eq!(missing_value_status, 2);
        assert!(missing_value_stderr.contains("missing value for flag --query"));

        let (missing_value_before_flag_status, _, missing_value_before_flag_stderr) =
            run_search(&[
                "crab-memory-search",
                "--workspace-root",
                "/tmp/workspace",
                "--user-scope",
                "user-1",
                "--query",
                "--max-results",
                "2",
            ]);
        assert_eq!(missing_value_before_flag_status, 2);
        assert!(missing_value_before_flag_stderr.contains("missing value for flag --query"));

        let (blank_scope_status, _, blank_scope_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            " ",
            "--query",
            "rust",
        ]);
        assert_eq!(blank_scope_status, 2);
        assert!(blank_scope_stderr.contains("--user-scope must not be empty"));

        let (blank_query_status, _, blank_query_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
            " ",
        ]);
        assert_eq!(blank_query_status, 2);
        assert!(blank_query_stderr.contains("--query must not be empty"));

        let (invalid_max_results_status, _, invalid_max_results_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
            "rust",
            "--max-results",
            "abc",
        ]);
        assert_eq!(invalid_max_results_status, 2);
        assert!(invalid_max_results_stderr
            .contains("--max-results must be a positive integer, got \"abc\""));

        let (zero_max_snippet_status, _, zero_max_snippet_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
            "rust",
            "--max-snippet-chars",
            "0",
        ]);
        assert_eq!(zero_max_snippet_status, 2);
        assert!(zero_max_snippet_stderr
            .contains("--max-snippet-chars must be a positive integer, got \"0\""));

        let (invalid_max_files_status, _, invalid_max_files_stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--query",
            "rust",
            "--max-files",
            "abc",
        ]);
        assert_eq!(invalid_max_files_status, 2);
        assert!(invalid_max_files_stderr
            .contains("--max-files must be a positive integer, got \"abc\""));
    }

    #[test]
    fn search_cli_surfaces_runtime_errors_from_core() {
        let workspace = TempWorkspace::new("search-runtime-error");
        workspace.write_file("workspace-root-is-file", "not-a-directory");
        let invalid_root = workspace
            .root
            .join("workspace-root-is-file")
            .to_string_lossy()
            .to_string();

        let (status, _, stderr) = run_search(&[
            "crab-memory-search",
            "--workspace-root",
            &invalid_root,
            "--user-scope",
            "user-1",
            "--query",
            "rust",
        ]);
        assert_eq!(status, 2);
        assert!(stderr.contains("memory_search invariant violation"));
        assert!(stderr.contains("must be a directory"));
    }

    #[test]
    fn search_cli_reports_json_write_failures() {
        let workspace = TempWorkspace::new("search-write-failure");
        workspace.write_file("MEMORY.md", "memory\n");
        workspace.write_file("memory/users/user-1/2026-02-10.md", "memory\n");
        let workspace_root = workspace.root.to_string_lossy().to_string();

        let mut bad_stdout = FailWriter;
        let mut stderr = Vec::new();
        let status = run_memory_search_cli(
            vec![
                "crab-memory-search".to_string(),
                "--workspace-root".to_string(),
                workspace_root,
                "--user-scope".to_string(),
                "user-1".to_string(),
                "--query".to_string(),
                "memory".to_string(),
            ],
            &mut bad_stdout,
            &mut stderr,
        );
        assert_eq!(status, 2);
        let stderr = String::from_utf8(stderr).expect("stderr should be utf-8");
        assert!(stderr.contains("failed to serialize command response"));
    }

    #[test]
    fn search_cli_reports_newline_write_failures() {
        let workspace = TempWorkspace::new("search-newline-write-failure");
        workspace.write_file("MEMORY.md", "memory\n");
        workspace.write_file("memory/users/user-1/2026-02-10.md", "memory\n");
        let workspace_root = workspace.root.to_string_lossy().to_string();

        let mut stdout = FailOnNewlineWriter;
        let mut stderr = Vec::new();
        let status = run_memory_search_cli(
            vec![
                "crab-memory-search".to_string(),
                "--workspace-root".to_string(),
                workspace_root,
                "--user-scope".to_string(),
                "user-1".to_string(),
                "--query".to_string(),
                "memory".to_string(),
            ],
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(status, 2);
        let stderr = String::from_utf8(stderr).expect("stderr should be utf-8");
        assert!(stderr.contains("failed to write command response"));
    }

    #[test]
    fn get_cli_help_mode_prints_usage() {
        let (status, stdout, stderr) = run_get(&["crab-memory-get", "-h"]);
        assert_eq!(status, 0);
        assert!(stdout.contains("Usage:"));
        assert!(stdout.contains("crab-memory-get"));
        assert!(stderr.is_empty());
    }

    #[test]
    fn get_cli_returns_found_payload() {
        let workspace = TempWorkspace::new("get-found");
        workspace.write_file(
            "memory/users/user-1/2026-02-10.md",
            "line one\nline two\nline three\n",
        );

        let workspace_root = workspace.root.to_string_lossy().to_string();
        let (status, stdout, stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &workspace_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "2",
            "--end-line",
            "3",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());

        let response = decode_json(stdout.as_bytes());
        assert_eq!(response["found"], true);
        assert_eq!(response["path"], "memory/users/user-1/2026-02-10.md");
        assert_eq!(response["start_line"], 2);
        assert_eq!(response["end_line"], 3);
        assert_eq!(response["truncated"], false);
        assert_eq!(response["content"], "line two\nline three");
    }

    #[test]
    fn get_cli_returns_not_found_payload() {
        let workspace = TempWorkspace::new("get-not-found");
        workspace.write_file("MEMORY.md", "curated\n");
        let workspace_root = workspace.root.to_string_lossy().to_string();
        let (status, stdout, stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &workspace_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
            "--no-global",
        ]);
        assert_eq!(status, 0);
        assert!(stderr.is_empty());
        let response = decode_json(stdout.as_bytes());
        assert_eq!(response["found"], false);
        assert_eq!(response["path"], "memory/users/user-1/2026-02-10.md");
        assert_eq!(response["include_global_memory"], false);
    }

    #[test]
    fn get_cli_reports_parse_and_value_errors() {
        let (missing_status, _, missing_stderr) = run_get(&["crab-memory-get"]);
        assert_eq!(missing_status, 2);
        assert!(missing_stderr.contains("missing required flag --workspace-root"));

        let (invalid_status, _, invalid_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "0",
        ]);
        assert_eq!(invalid_status, 2);
        assert!(invalid_stderr.contains("--start-line must be a positive integer"));

        let (non_numeric_status, _, non_numeric_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "abc",
        ]);
        assert_eq!(non_numeric_status, 2);
        assert!(non_numeric_stderr.contains("--start-line must be a positive integer"));

        let (blank_scope_status, _, blank_scope_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            " ",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
        ]);
        assert_eq!(blank_scope_status, 2);
        assert!(blank_scope_stderr.contains("--user-scope must not be empty"));

        let (blank_path_status, _, blank_path_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--path",
            " ",
            "--start-line",
            "1",
        ]);
        assert_eq!(blank_path_status, 2);
        assert!(blank_path_stderr.contains("--path must not be empty"));

        let (blank_start_line_status, _, blank_start_line_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            "/tmp/workspace",
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            " ",
        ]);
        assert_eq!(blank_start_line_status, 2);
        assert!(blank_start_line_stderr.contains("--start-line must not be empty"));
    }

    #[test]
    fn get_cli_rejects_duplicate_flags() {
        let (status, _, stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            "/tmp/workspace",
            "--workspace-root",
            "/tmp/workspace2",
            "--user-scope",
            "user-1",
            "--path",
            "MEMORY.md",
            "--start-line",
            "1",
        ]);
        assert_eq!(status, 2);
        assert!(stderr.contains("duplicate flag --workspace-root"));
    }

    #[test]
    fn get_cli_covers_optional_and_runtime_error_paths() {
        let workspace = TempWorkspace::new("get-optional-runtime-paths");
        workspace.write_file("MEMORY.md", "curated memory line\n");
        workspace.write_file(
            "memory/users/user-1/2026-02-10.md",
            "line one\nline two\nline three\n",
        );
        workspace.write_file("workspace-root-is-file", "not-a-directory");

        let valid_root = workspace.root.to_string_lossy().to_string();
        let invalid_root = workspace
            .root
            .join("workspace-root-is-file")
            .to_string_lossy()
            .to_string();

        let (default_optional_status, default_optional_stdout, default_optional_stderr) =
            run_get(&[
                "crab-memory-get",
                "--workspace-root",
                &valid_root,
                "--user-scope",
                "user-1",
                "--path",
                "memory/users/user-1/2026-02-10.md",
                "--start-line",
                "1",
            ]);
        assert_eq!(default_optional_status, 0);
        assert!(default_optional_stderr.is_empty());
        let default_optional_response = decode_json(default_optional_stdout.as_bytes());
        assert_eq!(default_optional_response["found"], true);
        assert_eq!(default_optional_response["end_line"], 1);

        let (invalid_end_line_status, _, invalid_end_line_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &valid_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
            "--end-line",
            "abc",
        ]);
        assert_eq!(invalid_end_line_status, 2);
        assert!(invalid_end_line_stderr.contains("--end-line must be a positive integer"));

        let (invalid_max_lines_status, _, invalid_max_lines_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &valid_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
            "--max-lines",
            "0",
        ]);
        assert_eq!(invalid_max_lines_status, 2);
        assert!(invalid_max_lines_stderr.contains("--max-lines must be a positive integer"));

        let (invalid_max_chars_status, _, invalid_max_chars_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &valid_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
            "--max-chars",
            "abc",
        ]);
        assert_eq!(invalid_max_chars_status, 2);
        assert!(invalid_max_chars_stderr.contains("--max-chars must be a positive integer"));

        let (duplicate_switch_status, _, duplicate_switch_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &valid_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
            "--no-global",
            "--no-global",
        ]);
        assert_eq!(duplicate_switch_status, 2);
        assert!(duplicate_switch_stderr.contains("duplicate flag --no-global"));

        let (runtime_error_status, _, runtime_error_stderr) = run_get(&[
            "crab-memory-get",
            "--workspace-root",
            &invalid_root,
            "--user-scope",
            "user-1",
            "--path",
            "memory/users/user-1/2026-02-10.md",
            "--start-line",
            "1",
        ]);
        assert_eq!(runtime_error_status, 2);
        assert!(runtime_error_stderr.contains("memory_get invariant violation"));
        assert!(runtime_error_stderr.contains("must be a directory"));
    }

    #[test]
    fn fail_writer_flush_path_is_exercised() {
        let mut writer = FailWriter;
        let error = writer.flush().expect_err("flush should fail");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }

    #[test]
    fn fail_on_newline_writer_flush_path_is_exercised() {
        let mut writer = FailOnNewlineWriter;
        writer
            .flush()
            .expect("flush should succeed for newline writer");
    }
}
