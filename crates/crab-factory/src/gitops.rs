use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use sha2::{Digest, Sha256};

use crate::workers::{supervise, CancelFlags, CommandSpec, OutputPlan, SupervisorErrorKind};
use crate::{io_result, read_bytes, result_context, sha256_hex, FactoryError, FactoryResult};

#[derive(Debug, Clone)]
pub(crate) struct GitRunner {
    executable: PathBuf,
    cancellation: Arc<AtomicBool>,
    timeout: Duration,
}

#[derive(Debug)]
struct GitOutput {
    code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl GitRunner {
    pub(crate) fn new(
        executable: PathBuf,
        cancellation: Arc<AtomicBool>,
        timeout: Duration,
    ) -> Self {
        Self {
            executable,
            cancellation,
            timeout,
        }
    }

    pub(crate) fn discover_toplevel(&self, path: &Path) -> FactoryResult<PathBuf> {
        let output = self.checked([
            OsString::from("-C"),
            path.as_os_str().to_os_string(),
            OsString::from("rev-parse"),
            OsString::from("--show-toplevel"),
        ])?;
        let text = required_stdout(&output.stdout, "git repository root")?;
        let path = Path::new(text.trim());
        #[rustfmt::skip]
        let root = io_result(fs::canonicalize(path), "canonicalize git repository root", path)?;
        Ok(root)
    }

    pub(crate) fn resolve_commit(&self, repo: &Path, reference: &str) -> FactoryResult<String> {
        let expression = format!("{reference}^{{commit}}");
        let output = self.run([
            OsString::from("-C"),
            repo.as_os_str().to_os_string(),
            OsString::from("rev-parse"),
            OsString::from("--verify"),
            OsString::from("--quiet"),
            OsString::from("--end-of-options"),
            OsString::from(expression),
        ])?;
        if output.code != 0 {
            return Err(FactoryError::new(format!(
                "base ref does not resolve to a committed SHA: {reference}"
            )));
        }
        let sha = required_stdout(&output.stdout, "resolved base SHA")?;
        Ok(sha.trim().to_string())
    }

    pub(crate) fn source_status(&self, repo: &Path) -> FactoryResult<Vec<u8>> {
        Ok(self
            .checked([
                OsString::from("-C"),
                repo.as_os_str().to_os_string(),
                OsString::from("status"),
                OsString::from("--porcelain"),
            ])?
            .stdout)
    }

    pub(crate) fn branch_exists(&self, repo: &Path, branch: &str) -> FactoryResult<bool> {
        let reference = format!("refs/heads/{branch}");
        let output = self.run([
            OsString::from("-C"),
            repo.as_os_str().to_os_string(),
            OsString::from("show-ref"),
            OsString::from("--verify"),
            OsString::from("--quiet"),
            OsString::from(reference),
        ])?;
        match output.code {
            0 => Ok(true),
            1 => Ok(false),
            code => Err(command_failure("git show-ref", code, &output)),
        }
    }

    pub(crate) fn add_worktree(
        &self,
        repo: &Path,
        branch: &str,
        worktree: &Path,
        base_sha: &str,
    ) -> FactoryResult<()> {
        self.checked([
            OsString::from("-C"),
            repo.as_os_str().to_os_string(),
            OsString::from("worktree"),
            OsString::from("add"),
            OsString::from("-b"),
            OsString::from(branch),
            worktree.as_os_str().to_os_string(),
            OsString::from(base_sha),
        ])?;
        Ok(())
    }

    pub(crate) fn head_sha(&self, worktree: &Path) -> FactoryResult<String> {
        let output = self.checked([
            OsString::from("-C"),
            worktree.as_os_str().to_os_string(),
            OsString::from("rev-parse"),
            OsString::from("HEAD"),
        ])?;
        Ok(required_stdout(&output.stdout, "worktree HEAD")?
            .trim()
            .to_string())
    }

    pub(crate) fn symbolic_branch(&self, worktree: &Path) -> FactoryResult<String> {
        let output = self.checked([
            OsString::from("-C"),
            worktree.as_os_str().to_os_string(),
            OsString::from("symbolic-ref"),
            OsString::from("--quiet"),
            OsString::from("HEAD"),
        ])?;
        Ok(required_stdout(&output.stdout, "worktree branch")?
            .trim()
            .to_string())
    }

    pub(crate) fn fingerprint(&self, worktree: &Path) -> FactoryResult<WorktreeFingerprint> {
        let output = self.checked([
            OsString::from("-C"),
            worktree.as_os_str().to_os_string(),
            OsString::from("status"),
            OsString::from("--porcelain=v1"),
            OsString::from("-z"),
            OsString::from("--untracked-files=all"),
        ])?;
        let paths = status_paths(&output.stdout);
        let mut files = BTreeMap::new();
        for bytes in paths {
            let relative = PathBuf::from(OsString::from_vec(bytes));
            let absolute = worktree.join(&relative);
            let value = if absolute.is_symlink() {
                let target =
                    io_result(fs::read_link(&absolute), "read worktree symlink", &absolute)?;
                sha256_hex(target.as_os_str().as_bytes())
            } else if absolute.is_file() {
                sha256_hex(&read_bytes(&absolute, "worktree file")?)
            } else {
                "absent".to_string()
            };
            files.insert(relative.to_string_lossy().into_owned(), value);
        }
        let mut digest = Sha256::new();
        digest.update(&output.stdout);
        for (path, hash) in &files {
            digest.update(path.as_bytes());
            digest.update([0]);
            digest.update(hash.as_bytes());
            digest.update([0]);
        }
        Ok(WorktreeFingerprint {
            digest: format!("{:x}", digest.finalize()),
            files,
        })
    }

    fn checked<I>(&self, args: I) -> FactoryResult<GitOutput>
    where
        I: IntoIterator<Item = OsString>,
    {
        let output = self.run(args)?;
        if output.code == 0 {
            Ok(output)
        } else {
            Err(command_failure("git", output.code, &output))
        }
    }

    fn run<I>(&self, args: I) -> FactoryResult<GitOutput>
    where
        I: IntoIterator<Item = OsString>,
    {
        let result = match supervise(
            CommandSpec::isolated(
                self.executable.clone(),
                args.into_iter().collect(),
                None,
                None,
                self.timeout,
                CancelFlags::global_only(Arc::clone(&self.cancellation)),
            ),
            OutputPlan::Capture,
        ) {
            Ok(result) => result,
            Err(error) => {
                let prefix = if error.kind == SupervisorErrorKind::Cancelled {
                    "git command interrupted"
                } else {
                    "git command failed under supervision"
                };
                return Err(FactoryError::new(format!("{prefix}: {}", error.detail())));
            }
        };
        Ok(GitOutput {
            code: result.returncode,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorktreeFingerprint {
    digest: String,
    files: BTreeMap<String, String>,
}

impl WorktreeFingerprint {
    pub(crate) fn changed_paths(&self, other: &Self) -> Vec<String> {
        self.files
            .keys()
            .chain(other.files.keys())
            .filter(|path| self.files.get(*path) != other.files.get(*path))
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn synthetic(digest: &str, files: &[(&str, &str)]) -> Self {
        Self {
            digest: digest.to_string(),
            files: files
                .iter()
                .map(|(path, hash)| ((*path).to_string(), (*hash).to_string()))
                .collect(),
        }
    }
}

fn status_paths(status: &[u8]) -> Vec<Vec<u8>> {
    let mut paths = Vec::new();
    let mut rename_target = false;
    for entry in status
        .split(|byte| *byte == 0)
        .filter(|entry| !entry.is_empty())
    {
        if rename_target {
            paths.push(entry.to_vec());
            rename_target = false;
            continue;
        }
        if entry.len() >= 3 && entry[2] == b' ' {
            let status = &entry[..2];
            paths.push(entry[3..].to_vec());
            rename_target = status.iter().any(|byte| matches!(*byte, b'R' | b'C'));
        }
    }
    paths.sort();
    paths.dedup();
    paths
}

fn required_stdout(bytes: &[u8], label: &str) -> FactoryResult<String> {
    let output = result_context(
        std::str::from_utf8(bytes),
        &format!("{label} was not UTF-8"),
    )?;
    if output.trim().is_empty() {
        return Err(FactoryError::new(format!("{label} was empty")));
    }
    Ok(output.to_string())
}

fn command_failure(command: &str, code: i32, output: &GitOutput) -> FactoryError {
    let diagnostic = if output.stderr.is_empty() {
        &output.stdout
    } else {
        &output.stderr
    };
    let diagnostic = String::from_utf8_lossy(diagnostic);
    FactoryError::new(format!(
        "{command} exited {code}: {}",
        diagnostic.trim().chars().take(2_000).collect::<String>()
    ))
}

pub(crate) fn assert_identity(
    git: &GitRunner,
    worktree: &Path,
    base_sha: &str,
    branch: &str,
    stage: &str,
) -> FactoryResult<()> {
    let head = git.head_sha(worktree)?;
    if head != base_sha {
        return Err(FactoryError::new(format!(
            "{stage} changed worktree HEAD: expected {base_sha}, found {head}"
        )));
    }
    let expected = format!("refs/heads/{branch}");
    let actual = git.symbolic_branch(worktree)?;
    if actual != expected {
        return Err(FactoryError::new(format!(
            "{stage} changed worktree branch: expected {expected}, found {actual}"
        )));
    }
    Ok(())
}

pub(crate) fn assert_unchanged(
    before: &WorktreeFingerprint,
    after: &WorktreeFingerprint,
    stage: &str,
) -> FactoryResult<()> {
    if before.digest == after.digest {
        return Ok(());
    }
    let paths = before.changed_paths(after);
    let detail = if paths.is_empty() {
        "git status changed".to_string()
    } else {
        paths.join(", ")
    };
    Err(FactoryError::new(format!(
        "read-only stage {stage} modified the worktree: {detail}"
    )))
}

#[cfg(test)]
#[path = "gitops/tests/mod.rs"]
mod tests;
