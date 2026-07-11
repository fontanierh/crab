#![allow(dead_code)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use serde_json::Value;
use sha2::Digest;

static COUNTER: AtomicU64 = AtomicU64::new(0);

pub(crate) struct Fixture {
    pub(crate) root: PathBuf,
    pub(crate) repo: PathBuf,
    pub(crate) prompt: PathBuf,
    pub(crate) runs: PathBuf,
    pub(crate) worktrees: PathBuf,
    pub(crate) receipts: PathBuf,
    pub(crate) fake_bin: PathBuf,
    pub(crate) run_id: String,
    pub(crate) first_sha: String,
    path: String,
}

impl Fixture {
    pub(crate) fn new(label: &str, scenario: &str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "crab-factory-e2e-{label}-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).expect("fixture root");
        let repo = root.join("source repo");
        fs::create_dir_all(repo.join("subdir")).expect("repo");
        run_git(&repo, &["init", "-q"]);
        run_git(&repo, &["config", "user.name", "Factory Test"]);
        run_git(&repo, &["config", "user.email", "factory@example.invalid"]);
        fs::write(repo.join("AGENTS.md"), "# Fixture policy\n").expect("agents");
        fs::write(repo.join("source.txt"), "base\n").expect("source");
        run_git(&repo, &["add", "."]);
        run_git(&repo, &["commit", "-qm", "base"]);
        let first_sha = git_output(&repo, &["rev-parse", "HEAD"]);
        fs::write(repo.join("source.txt"), "tip\n").expect("tip source");
        run_git(&repo, &["add", "source.txt"]);
        run_git(&repo, &["commit", "-qm", "tip"]);

        let prompt = root.join("request with spaces.md");
        fs::write(&prompt, "Implement caf\u{e9}.  ").expect("prompt");
        let receipts = root.join("receipts");
        let fake_bin = root.join("fake bin");
        fs::create_dir_all(&receipts).expect("receipts");
        fs::create_dir_all(&fake_bin).expect("fake bin");
        write_fake_workers(&fake_bin, &receipts, scenario);
        let old_path = std::env::var("PATH").expect("PATH");
        let path = format!("{}:{old_path}", fake_bin.display());
        Self {
            root: root.clone(),
            repo,
            prompt,
            runs: root.join("run artifacts"),
            worktrees: root.join("isolated worktrees"),
            receipts,
            fake_bin,
            run_id: format!("fixture-{label}"),
            first_sha,
            path,
        }
    }

    pub(crate) fn command(&self, additional_rounds: u32) -> Command {
        let mut command = Command::new(factory_binary());
        command.args(self.run_arguments(additional_rounds).into_iter().skip(1));
        self.configure(&mut command);
        command
    }

    pub(crate) fn run_arguments(&self, additional_rounds: u32) -> Vec<String> {
        vec![
            "crab-factory".to_string(),
            "run".to_string(),
            "--prompt-file".to_string(),
            self.prompt.to_string_lossy().into_owned(),
            "--repo".to_string(),
            self.repo.join("subdir").to_string_lossy().into_owned(),
            "--run-id".to_string(),
            self.run_id.clone(),
            "--artifact-root".to_string(),
            self.runs.to_string_lossy().into_owned(),
            "--worktree-root".to_string(),
            self.worktrees.to_string_lossy().into_owned(),
            "--agent-timeout-seconds".to_string(),
            "60".to_string(),
            "--additional-review-rounds".to_string(),
            additional_rounds.to_string(),
        ]
    }

    pub(crate) fn start_command(&self) -> Command {
        let mut command = Command::new(factory_binary());
        command.args([
            "start",
            "--prompt-file",
            self.prompt.to_str().expect("prompt path"),
            "--repo",
            self.repo.to_str().expect("repo path"),
            "--run-id",
            &self.run_id,
            "--artifact-root",
            self.runs.to_str().expect("runs path"),
            "--worktree-root",
            self.worktrees.to_str().expect("worktree path"),
            "--agent-timeout-seconds",
            "60",
        ]);
        self.configure(&mut command);
        command
    }

    pub(crate) fn configure(&self, command: &mut Command) {
        command
            .env("PATH", &self.path)
            .env("DISCORD_TOKEN", "must-not-leak")
            .env("GIT_DIR", self.root.join("hostile-git-dir"))
            .env("GIT_WORK_TREE", self.root.join("hostile-work-tree"))
            .env("CARGO_TARGET_DIR", self.root.join("hostile-target"))
            .env("MAKEFLAGS", "--invalid-hostile-flag")
            .env("BASH_ENV", self.root.join("hostile-bash-env"))
            .env("CODEX_SANDBOX", "read-only")
            .env("CODEX_SANDBOX_NETWORK_DISABLED", "1")
            .env("CLAUDE_DISABLE_NETWORK", "1")
            .env("OPENAI_OFFLINE", "1")
            .env("ANTHROPIC_NO_NETWORK", "1")
            .env("HTTP_PROXY", "http://proxy.invalid")
            .env("SSL_CERT_FILE", self.root.join("test-cert-bundle.pem"));
    }

    pub(crate) fn run(&self, additional_rounds: u32) -> Output {
        self.command(additional_rounds)
            .output()
            .expect("factory binary")
    }

    pub(crate) fn run_dir(&self) -> PathBuf {
        self.runs.join(&self.run_id)
    }

    pub(crate) fn environment_path(&self) -> &str {
        &self.path
    }

    pub(crate) fn worktree(&self) -> PathBuf {
        self.worktrees.join(&self.run_id)
    }

    pub(crate) fn manifest(&self) -> Value {
        serde_json::from_slice(&fs::read(self.run_dir().join("manifest.json")).expect("manifest"))
            .expect("manifest JSON")
    }

    pub(crate) fn receipts(&self, provider: &str) -> Vec<Value> {
        let mut paths = fs::read_dir(&self.receipts)
            .expect("receipt dir")
            .map(|entry| entry.expect("receipt entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(provider) && name.ends_with(".receipt"))
            })
            .collect::<Vec<_>>();
        paths.sort();
        paths.into_iter().map(|path| read_receipt(&path)).collect()
    }

    pub(crate) fn source_head(&self) -> String {
        git_output(&self.repo, &["rev-parse", "HEAD"])
    }

    pub(crate) fn source_branch(&self) -> String {
        git_output(&self.repo, &["symbolic-ref", "HEAD"])
    }

    pub(crate) fn launcher(&self, mode: &str) -> PathBuf {
        let path = self.fake_bin.join(format!("launcher-{mode}"));
        let receipt = python_string(&self.receipts.join(format!("launcher-{mode}.json")));
        let child_log = python_string(&self.root.join(format!("launcher-{mode}-child.log")));
        let body = match mode {
            "success" => format!(
                r#"#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
pathlib.Path({receipt}).write_text(json.dumps({{'argv': sys.argv[1:]}}), encoding='utf-8')
log = open({child_log}, 'ab', buffering=0)
run_dir = sys.argv[sys.argv.index('--run-dir') + 1]
child = subprocess.Popen(sys.argv[2:], stdin=subprocess.DEVNULL, stdout=log, stderr=log, cwd=run_dir, start_new_session=True)
pathlib.Path(os.environ['CRAB_FACTORY_LAUNCH_PID_RECEIPT']).write_text('PID=' + str(child.pid) + '\n', encoding='utf-8')
print('PID=' + str(child.pid))
"#
            ),
            "noop" => format!(
                "#!/usr/bin/env python3\nimport json, os, pathlib, sys\npathlib.Path({receipt}).write_text(json.dumps({{'argv': sys.argv[1:]}}), encoding='utf-8')\npid = os.getpid()\npathlib.Path(os.environ['CRAB_FACTORY_LAUNCH_PID_RECEIPT']).write_text('PID=' + str(pid) + '\\n', encoding='utf-8')\nprint('PID=' + str(pid))\n"
            ),
            "no-pid" => format!(
                "#!/usr/bin/env python3\nimport json, pathlib, sys\npathlib.Path({receipt}).write_text(json.dumps({{'argv': sys.argv[1:]}}), encoding='utf-8')\nprint('submitted')\n"
            ),
            "invalid-pid" => format!(
                "#!/usr/bin/env python3\nimport json, os, pathlib, sys\npathlib.Path({receipt}).write_text(json.dumps({{'argv': sys.argv[1:]}}), encoding='utf-8')\npathlib.Path(os.environ['CRAB_FACTORY_LAUNCH_PID_RECEIPT']).write_text('not-a-pid\\n', encoding='utf-8')\nprint('submitted')\n"
            ),
            "fail" => format!(
                "#!/usr/bin/env python3\nimport pathlib, sys\npathlib.Path({receipt}).write_text('failed launcher', encoding='utf-8')\nprint('launcher failed', file=sys.stderr)\nraise SystemExit(7)\n"
            ),
            "spawn-fail" | "spawn-timeout" => {
                let ending = if mode == "spawn-fail" {
                    "raise SystemExit(7)"
                } else {
                    "import time\ntime.sleep(120)"
                };
                format!(
                    r#"#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys
pathlib.Path({receipt}).write_text(json.dumps({{'argv': sys.argv[1:]}}), encoding='utf-8')
log = open({child_log}, 'ab', buffering=0)
run_dir = sys.argv[sys.argv.index('--run-dir') + 1]
child = subprocess.Popen(sys.argv[2:], stdin=subprocess.DEVNULL, stdout=log, stderr=log, cwd=run_dir, start_new_session=True)
pathlib.Path(os.environ['CRAB_FACTORY_LAUNCH_PID_RECEIPT']).write_text('PID=' + str(child.pid) + '\n', encoding='utf-8')
print('PID=' + str(child.pid), flush=True)
{ending}
"#
                )
            }
            "sleep" => "#!/bin/sh\nprintf 'launcher waiting\\n'\n/bin/sleep 120\n".to_string(),
            other => panic!("unknown launcher mode {other}"),
        };
        write_executable(&path, &body);
        path
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

pub(crate) fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(crate) fn assert_failure(output: &Output, expected: &str) {
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected),
        "expected {expected:?}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(crate) fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("JSON file")).expect("valid JSON")
}

pub(crate) fn mode(path: &Path) -> u32 {
    fs::metadata(path).unwrap().permissions().mode() & 0o777
}

pub(crate) fn wait_for(path: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while !path.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(25));
    }
    assert!(path.exists(), "timed out waiting for {}", path.display());
}

pub(crate) fn wait_for_pid_exit(pid: i32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let result = unsafe { libc::kill(pid, 0) };
        if result == -1 && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH) {
            return;
        }
        assert!(Instant::now() < deadline, "PID {pid} did not exit");
        std::thread::sleep(Duration::from_millis(25));
    }
}

fn run_git(repo: &Path, args: &[&str]) {
    let status = Command::new("git")
        .args(args)
        .current_dir(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .status()
        .expect("git command");
    assert!(status.success(), "git {args:?}");
}

fn git_output(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .args(args)
        .current_dir(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .output()
        .expect("git output");
    assert!(output.status.success());
    String::from_utf8(output.stdout).unwrap().trim().to_string()
}

fn write_executable(path: &Path, content: &str) {
    fs::write(path, content).expect("write fake executable");
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).expect("chmod fake executable");
}

fn factory_binary() -> &'static str {
    option_env!("CARGO_BIN_EXE_crab-factory").unwrap_or("/usr/bin/false")
}

fn python_string(value: &Path) -> String {
    format!(
        "'{}'",
        value
            .to_string_lossy()
            .replace('\\', "\\\\")
            .replace('\'', "\\'")
    )
}

fn write_fake_workers(bin: &Path, receipts: &Path, scenario: &str) {
    let worker = bin.join("factory-fake-worker");
    write_executable(&worker, include_str!("fake_worker.sh"));
    for (name, provider) in [("codex", "codex"), ("claude", "claude"), ("make", "make")] {
        let wrapper = format!(
            "#!/bin/sh\nexec {} {} {} {} \"$@\"\n",
            shell_string(&worker),
            shell_word(provider),
            shell_word(scenario),
            shell_string(receipts),
        );
        write_executable(&bin.join(name), &wrapper);
    }
    write_executable(
        &bin.join("cargo"),
        "#!/bin/sh\nif [ \"$1\" = llvm-cov ]; then echo 'cargo-llvm-cov fake 1.0'; else echo 'cargo fake 1.0'; fi\n",
    );
    write_executable(&bin.join("rg"), "#!/bin/sh\necho 'rg fake'\n");
    write_executable(&bin.join("npx"), "#!/bin/sh\necho 'npx fake'\n");
}

fn read_receipt(path: &Path) -> Value {
    let text = fs::read_to_string(path).expect("receipt");
    let mut argv = Vec::new();
    let mut environment = serde_json::Map::new();
    let mut fields = serde_json::Map::new();
    for line in text.lines() {
        let parts = line.splitn(3, '\t').collect::<Vec<_>>();
        match parts.as_slice() {
            ["argv", value] => argv.push(Value::String((*value).to_string())),
            ["cwd", value] => {
                fields.insert("cwd".to_string(), Value::String((*value).to_string()));
            }
            ["stdin", value] => {
                let bytes = fs::read(value).expect("receipt stdin");
                fields.insert(
                    "stdin_sha256".to_string(),
                    Value::String(format!("{:x}", sha2::Sha256::digest(&bytes))),
                );
                fields.insert("stdin_length".to_string(), Value::from(bytes.len()));
            }
            ["field", name, value] => {
                fields.insert((*name).to_string(), Value::String((*value).to_string()));
            }
            ["env", name, "present"] => {
                environment.insert((*name).to_string(), Value::String("present".to_string()));
            }
            ["env", name, "absent"] => {
                environment.insert((*name).to_string(), Value::Null);
            }
            _ => panic!("invalid receipt line in {}: {line}", path.display()),
        }
    }
    fields.insert("argv".to_string(), Value::Array(argv));
    fields.insert("env".to_string(), Value::Object(environment));
    Value::Object(fields)
}

fn shell_word(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\\''"))
}
fn shell_string(value: &Path) -> String {
    format!("'{}'", value.to_string_lossy().replace('\'', "'\\''"))
}
