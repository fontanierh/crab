use std::io::Write;
use std::path::PathBuf;

pub(crate) fn collect_args<I, S>(args: I) -> Vec<String>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
{
    args.into_iter().map(Into::into).collect()
}

/// Shared CLI runner: checks for --help/-h, dispatches to `execute`, and formats
/// success (path on stdout) or error (message + usage on stderr).
pub(crate) fn run_path_cli(
    argv: &[String],
    usage: &str,
    execute: impl FnOnce(&[String]) -> Result<PathBuf, String>,
    stdout: &mut dyn Write,
    stderr: &mut dyn Write,
) -> i32 {
    let command_args = argv.get(1..).unwrap_or(&[]);

    if command_args.iter().any(|a| a == "--help" || a == "-h") {
        let _ = writeln!(stdout, "{usage}");
        return 0;
    }

    match execute(command_args) {
        Ok(path) => match writeln!(stdout, "{}", path.display()) {
            Ok(()) => 0,
            Err(error) => {
                let _ = writeln!(stderr, "error: failed to write output: {error}");
                1
            }
        },
        Err(message) => {
            let _ = writeln!(stderr, "error: {message}");
            let _ = writeln!(stderr);
            let _ = writeln!(stderr, "{usage}");
            1
        }
    }
}

/// Consume the next argument as a value for `flag`, returning an error if missing or
/// if the next token looks like another flag.
pub(crate) fn require_flag_value(
    args: &[String],
    index: usize,
    flag: &str,
) -> Result<String, String> {
    let value = args
        .get(index.saturating_add(1))
        .ok_or_else(|| format!("missing value for flag {flag}"))?;
    if value.starts_with("--") {
        return Err(format!("missing value for flag {flag}"));
    }
    Ok(value.clone())
}

/// Iterate over flag-value pairs in `args`, calling `on_flag` for each recognized
/// flag. `known_flags` is called first with the flag name to validate it (return
/// `true` if recognized, `false` to emit an unknown-flag error). Then the value is
/// consumed and passed to `on_flag`.
pub(crate) fn parse_flag_pairs(
    args: &[String],
    known_flags: impl Fn(&str) -> bool,
    mut on_flag: impl FnMut(&str, String) -> Result<(), String>,
) -> Result<(), String> {
    let mut index = 0usize;
    while index < args.len() {
        let flag = &args[index];
        if !flag.starts_with("--") {
            return Err(format!("unexpected positional argument {:?}", flag));
        }
        if !known_flags(flag) {
            return Err(format!("unknown flag {flag}"));
        }
        let value = require_flag_value(args, index, flag)?;
        on_flag(flag, value)?;
        index = index.saturating_add(2);
    }
    Ok(())
}

/// Assign `value` to `slot` if empty, or return a duplicate-flag error.
pub(crate) fn assign_flag(
    slot: &mut Option<String>,
    flag: &str,
    value: String,
) -> Result<(), String> {
    if slot.is_some() {
        return Err(format!("duplicate flag {flag}"));
    }
    *slot = Some(value);
    Ok(())
}

/// Require a flag to have been supplied (Some) and non-blank.
pub(crate) fn require_non_empty(slot: Option<String>, flag_name: &str) -> Result<String, String> {
    let value = slot.ok_or_else(|| format!("missing required flag {flag_name}"))?;
    if value.trim().is_empty() {
        return Err(format!("{flag_name} must not be empty"));
    }
    Ok(value)
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::io::{self, ErrorKind, Write};

    #[derive(Default)]
    pub(crate) struct FailWriter;

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

    /// Create a state dir path that will cause `write_pending_*` to fail
    /// because it is a regular file instead of a directory.
    pub(crate) fn make_blocked_state_dir(workspace: &crate::test_support::TempWorkspace) -> String {
        std::fs::create_dir_all(&workspace.path).expect("temp dir should be creatable");
        let fake_dir = workspace.path.join("file-not-dir");
        std::fs::write(&fake_dir, "blocker").expect("write should succeed");
        fake_dir.to_string_lossy().to_string()
    }

    #[test]
    fn fail_writer_write_returns_broken_pipe() {
        let mut writer = FailWriter;
        let error = writer.write(b"test").expect_err("write should fail");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }

    #[test]
    fn fail_writer_flush_returns_broken_pipe() {
        let mut writer = FailWriter;
        let error = writer.flush().expect_err("flush should fail");
        assert_eq!(error.kind(), ErrorKind::BrokenPipe);
    }
}
