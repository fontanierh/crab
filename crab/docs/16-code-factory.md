# Repository-Owned Code Factory

## Purpose and scope

`crab-factory` is repository developer tooling for making isolated, reviewed changes to Crab. It
does not replace the live Discord connector or daemon. Every run starts from a committed SHA in a
new Git worktree, preserves the caller's checkout, and leaves both its worktree and audit artifacts
for operator handoff. The factory never commits, pushes, merges, opens a PR, deploys, or changes
issues and external services.

The implementation is owned by this repository. Its pinned review rubric is third-party Cursor
content vendored verbatim under the upstream MIT License; provenance and the required notice live
beside it in `crates/crab-factory/vendor/`. A fresh clone does not read any private workspace path
or machine-specific state outside the configured roots.

## Build and test from a fresh clone

```bash
cargo build -p crab-factory
./target/debug/crab-factory --help

# Equivalent development invocation
cargo run -p crab-factory -- --help
```

The model-free tests create real temporary Git repositories and use lightweight generated shell
fakes for Claude, Codex, Make, and quality tools. They require Git, a POSIX userland, and Python 3
for launcher-adapter scenarios, but do not compile a secondary test binary or require model
credentials:

```bash
cargo test -p crab-factory --locked
```

Real runs require all of the following on `PATH`:

- `git`, `make`, Claude Code (`claude`), and Codex (`codex`);
- the stable Rust toolchain selected by `rust-toolchain.toml` and `cargo`;
- `cargo-llvm-cov` 0.6.21 plus `llvm-tools-preview`;
- `rg` (ripgrep), Node with `npx`, `bash`, `python3`, `find`, `sort`, `grep`, `sed`, `dirname`,
  `mkdir`, and `pwd`.

Install the coverage prerequisites with:

```bash
cargo install cargo-llvm-cov --version 0.6.21 --locked
rustup component add llvm-tools-preview
```

Preflight resolves tools to absolute paths, checks the four primary tool versions, probes
`cargo llvm-cov`, verifies the remaining gate commands, resolves the base commit, checks source
state and destination collisions, and validates path containment before creating any run artifact.

## Pipeline and configurable cohorts

Every code-changing request follows this pipeline. Both providers default to effort `high`:

1. Claude Code `claude-fable-5`, effort `high`, produces the plan.
2. Two independent `gpt-5.6-sol` Codex critics run concurrently at `high` reasoning.
3. Fable 5 compiles their critiques into one self-contained directive.
4. One GPT-5.6 Sol agent implements the directive.
5. Each normal review round runs two Codex reviewers and one Fable reviewer concurrently, then
   Fable compiles a `CLEAN` or `CHANGES_REQUIRED` verdict. Findings are addressed by one Codex
   agent. The first clean verdict stops normal review early.
6. A fresh Codex reviewer always applies the complete pinned Cursor thermonuclear rubric. A
   separate Codex agent addresses accepted findings.
7. The worktree runs the repository's canonical `make quality` gate after remediation.

There is one mandatory normal review round. `--additional-review-rounds N` permits up to `N`
more, with a maximum of 100. A finding in the last permitted round is addressed without inventing
another review. Nested Codex multi-agent behavior and Claude's Agent tool are disabled.

Run/start accepts `--effort high|max`, `--plan-critics N`, and `--codex-reviewers N`; both count
bounds are 1–8. Selected values are recorded as prepared and effective configuration and apply to
every matching worker role. Model names, Fable synthesis, the mandatory Fable normal reviewer, and
the single thermonuclear Codex reviewer are not configurable. Count changes never split a cohort
into specialties: every member still receives identical prompt bytes.

Every parallel cohort receives one immutable in-memory byte buffer over stdin. The artifact copy,
cohort record, and every agent record share its SHA-256; the artifact is re-hashed after the cohort.
Workers independently assess the whole request—cohorts are never split into specialties. The
first failed cohort member cancels and reaps all peers.

## Intentional full-permission model policy

Every model worker in every role—planning, critique, compilation, implementation, normal review,
review remediation, thermonuclear review, and thermonuclear remediation—runs with unrestricted
host permissions and host network access. This dangerous mode is intentional operator policy. The
factory does not provide a filesystem sandbox or a network sandbox for model processes, so run it
only on a host and repository the operator is willing to expose to those models.

Codex workers use this fixed permission portion of the argument vector:

```text
--dangerously-bypass-approvals-and-sandbox --disable multi_agent
```

No Codex worker receives `--sandbox read-only` or `--sandbox workspace-write`. Claude Code workers
use this fixed permission and tool portion:

```text
--dangerously-skip-permissions --tools default --disallowedTools Agent
```

Claude therefore retains its default tool set; it does not run in plan mode or a tool-free mode,
and Edit/Write tools are not denied. Only nested fan-out is disabled: Codex multi-agent mode and
Claude's Agent tool remain unavailable. The exact commands, disabled sandbox state, permission
mode, and enabled network access are recorded in `manifest.json` and asserted from fake-worker
receipts in the model-free integration tests.

Full process capability does not broaden task authorization. Planning, critique, compilation, and
review prompts explicitly forbid file changes and external mutations. Git fingerprints plus
HEAD/branch checks enforce worktree immutability after those advisory stages; write-stage prompts
still restrict edits to the isolated worktree and forbid commits, pushes, PRs, deploys, and external
service changes.

## Foreground and durable launch modes

Foreground mode is best for CI and debugging:

```bash
./target/debug/crab-factory run \
  --prompt-file request.md \
  --repo . \
  --additional-review-rounds 2
```

`--repo` may name the repository root or any subdirectory. The default roots are
`$HOME/.crab/code-factory/runs` and `$HOME/.crab/code-factory/worktrees`; both can be overridden:

```bash
./target/debug/crab-factory run \
  --prompt-file request.md \
  --artifact-root /durable/factory-runs \
  --worktree-root /durable/factory-worktrees \
  --allow-dirty-source
```

`--allow-dirty-source` acknowledges caller-checkout changes; only the resolved committed base is
placed in the isolated worktree, so those changes are not copied.

Built-in background mode starts a new session and returns after recording its PID:

```bash
./target/debug/crab-factory start --prompt-file request.md --repo .
```

The command prints the exact stop command, `kill -TERM <recorded-pid>`. The same PID is in
`launch.json`; signaling it with SIGTERM cancels active workers, kills their process groups, writes
terminal status, and preserves artifacts. `factory.log` contains background progress.

An operator process manager can be integrated with an explicit launcher adapter:

```bash
./target/debug/crab-factory start \
  --prompt-file request.md \
  --launcher /opt/crab/bin/factory-launcher
```

The adapter receives a safe argument vector:

```text
<launcher> <process-name> <crab-factory-path> exec --run-dir <dir> --request-sha256 <hash>
```

The factory pre-creates a private PID-receipt file and passes its path in
`CRAB_FACTORY_LAUNCH_PID_RECEIPT`. The adapter must durably detach the received command in a
session led by that process, immediately write exactly `PID=<pid>` plus a newline to the receipt,
and return promptly. The factory rejects a successful launcher response without that handshake.
On launcher failure, cancellation, or timeout, it terminates the receipted executor and confirms
that it exited before terminalizing the run, preventing executor/manifest races. It records a
successful PID in `launch.json` and prints `kill -TERM <pid>` as the stop command. A `proc.sh` or
remote-tmux adapter should create its session using these arguments without constructing a shell
command from them. The launcher is trusted
operator plumbing and is the sole subprocess that inherits the full parent environment; all model,
Git, and quality processes use the allowlist below.

## Live controls

A prepared, non-terminal run can be inspected and steered without changing its original request:

```bash
crab-factory status --run-dir /path/to/run --json
crab-factory steer --run-dir /path/to/run --message "Preserve compatibility."
crab-factory steer --run-dir /path/to/run --message-file operator-note.md
crab-factory configure --run-dir /path/to/run --effort max --codex-reviewers 3
```

Controls apply only at a prompt boundary, before the prompt artifact is written and the next
worker or cohort is spawned. They never alter, kill, or restart an already-running model process.
Steering is appended to later prompts inside explicit delimiters; the immutable request bytes and
hash remain unchanged. Effort applies at the next worker boundary. Count knobs apply only at their
own cohort boundary, and a too-late change is recorded and rejected honestly.

The private `controls/` directory contains immutable sequence records and the executor-owned
`state.json` ledger. Records bind the run ID and request hash and carry payload and record hashes.
Writes use staging, file fsync, atomic rename, and directory fsync; reads reject symlinks and wrong
ownership or modes. Manifest acceptance/application/rejection events are an idempotent projection
of the ledger. Terminalization uses the controls lock to reject unresolved knobs, and terminal runs
reject new controls.

Status reports lifecycle, the current or between-stages position, active worker labels and PIDs
where available, launch PID, prepared/effective configuration, per-knob dispositions (including
the earliest eligible boundary for pending controls), and the last applied sequence. The JSON form
exposes the same facts. Configuration and prepared tool-path fields are additive optional
schema-version-1 fields:
legacy runs remain status-readable, while execution and control writes reject them as predating
live-control support. Persisted identity, request/base, resolved tool paths, paths, timeouts, and
bounds are validated.
Worker permissions/network, disabled nested agents, and the mandatory thermonuclear stage remain
structurally hardcoded and argv-tested rather than configurable inputs.

Review synthesis accepts exactly one trimmed line equal to `VERDICT: CLEAN` or
`VERDICT: CHANGES_REQUIRED` anywhere in a report. Prefixed or trailing prose is tolerated; missing,
duplicated, or conflicting verdict lines fail closed.

## Isolation and enforcement

The original request is copied byte-for-byte, including Unicode, trailing whitespace, and the
presence or absence of a final newline. This deliberately differs from the prototype's newline
normalization. `00-request.md` is mode `0400`, hashed before reservation, and verified again by the
single-shot `exec` command before worktree creation. The internal executor also takes a nonblocking
exclusive lock and accepts only an `initializing` manifest. Reservation creates `.lock` as a typed
run marker before execution. `exec` first validates that existing marker without writing anything;
an arbitrary directory is rejected without creating `.lock`, `manifest.json`, or `final-status.md`.

Advisory read-only stages are bracketed by a Git worktree fingerprint and HEAD/branch checks even
though their model processes have unrestricted host permissions. Write stages must leave HEAD at
the base SHA and stay on `factory/<run-id>`, so commits and branch switches fail the run. `make
quality` must leave the fingerprint unchanged. Ignored build outputs are not fingerprinted; tracked
and untracked Git-visible paths are.

Every external process is started in its own process group under a bounded supervisor. Timeout,
SIGINT, SIGTERM, or cohort cancellation sends SIGKILL to the whole group, reaps the leader, and
sweeps descendants even when the leader exited first.

Small captured outputs from Git commands, tool probes, and launchers are capped at 1 MiB per
stream. Exceeding the cap terminates the process group; timeout and overflow diagnostics retain a
bounded excerpt, and launcher failures preserve captured output in `launch-error.txt`.

Workers, Git, probes, and `make quality` receive only these parent variables when present:

- `PATH`, `HOME`, `USER`, `LOGNAME`, `SHELL`, `TMPDIR`, `TERM`, `LANG`, `LC_ALL`, `LC_CTYPE`, `TZ`;
- upper- and lower-case HTTP/HTTPS/no-proxy variables plus `SSL_CERT_FILE` and `SSL_CERT_DIR`;
- `CARGO_HOME`, `RUSTUP_HOME`, and the XDG cache/config/data/state homes;
- names beginning `ANTHROPIC_`, `CLAUDE_`, `OPENAI_`, or `CODEX_`.

Crab/Discord secrets, `GIT_*`, `MAKEFLAGS`, `MAKEFILES`, `CARGO_TARGET_DIR`, `RUSTC_WRAPPER`,
`BASH_ENV`, and `ENV` are not inherited. Provider-prefixed variables whose names indicate disabled
networking (`NETWORK_DISABLED`, `DISABLE_NETWORK`, `NO_NETWORK`, or `OFFLINE`) or select/advertise
a sandbox (`SANDBOX`, including `CODEX_SANDBOX`) are also removed, so the broad provider allowlist
cannot defeat the full-network, no-sandbox contract. Proxy and certificate variables remain
available, and the factory does not set a network-disable variable. Environment values are never
recorded.

## Artifacts and outcomes

The private mode-`0700` run directory contains:

```text
00-request.md
launch.json
manifest.json
.lock
launcher-pid               # explicit launcher handshake
controls/.controls.lock
controls/state.json
controls/NNNNNN-{steer,configure}.json
prompts/*.md
logs/<agent-label>.log
01-plan/ ... 06-thermo-nuclear-review/
quality/make-quality.log
factory.log                 # background runs
launch-error.txt            # launcher failures only
final-status.md
```

Files are mode `0600` except the read-only request snapshot. `manifest.json` records the request
hash, source/base/worktree facts, tool paths and versions, exact worker commands, models, the global
unrestricted-host/disabled-sandbox/enabled-network policy, each provider's dangerous permission
mode, prompt hashes, timestamps, cohorts, per-agent states and logs, ordered events, review
checkpoints, and terminal result. It is atomically replaced after each update. Once a run directory
is reserved, every reachable failure writes a failed manifest and `final-status.md`; validation
rejections before reservation write only to stderr and never alter a colliding run.

Factory-created roots and run subdirectories use mode `0700`. Explicit, pre-existing artifact or
worktree roots keep their operator-selected permissions; the factory does not silently chmod a
shared parent directory.

Terminal outcomes mean:

- `clean`: a post-change independent review found no actionable issues;
- `addressed_unverified`: the final permitted or thermonuclear review found issues that were
  addressed without a subsequent independent review;
- `failed`: preparation after reservation, execution, isolation, worker, verdict, timeout, or
  quality enforcement failed.

Both successful outcomes return exit code zero. Inspect the manifest or `final-status.md` to retain
the distinction. Reports are generated artifacts; do not hand-edit them.

## Quality, handoff, and cleanup

The final gate is exactly `make quality`. Its 99.5% function, 98.93% region, and 99.4% line
thresholds exercise the working tree. Its coverage gate also enforces 95% changed executable-line
coverage, including the under-20-line 100% rule, against the factory worktree's resolved base. The
factory uses the repository gate unchanged rather than maintaining a separate PR-only patch gate.

The factory intentionally preserves its branch, worktree, and artifacts on success and failure.
The operator should inspect the reports and diff, make any desired commits outside the factory,
open the PR through the normal repository workflow, and only then clean up explicitly:

```bash
git worktree remove /path/from/final-status
git branch -D factory/<run-id>
rm -rf /path/to/run-artifacts/<run-id>
```

For troubleshooting, start with `final-status.md`, then `quality/make-quality.log`, the relevant
per-agent log, and `factory.log` for background execution. Empty output, invalid verdicts, tool
failures, prompt mutation, worktree mutation, and timeouts are reported with the relevant artifact
path.
