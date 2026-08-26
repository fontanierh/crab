# AGENTS.md

Project operating rules for all human and AI contributors.

## 1. Non-Negotiable Quality Bar

- Write meaningful tests for behavior and regressions; coverage numbers are diagnostic only.
- Prefer real integration behavior over mocks; use mocks only at unavoidable boundaries.
- Tests must verify meaningful behavior, not implementation trivia.
- Tests must stay fast; slow tests are treated as quality regressions.
- No dead code in committed code.
- Refactor material duplication when it harms maintenance; small repetitions are acceptable.
- Code must pass formatter and linter checks before merge.

## 2. Testing Policy

- Write tests with implementation work; no deferred “test later”.
- Prefer integration tests first, then unit tests where targeted isolation is useful.
- Avoid snapshot-heavy tests unless snapshots capture stable, valuable behavior.
- Avoid asserting private/internal details when public behavior can be asserted.
- Each bug fix must include a regression test.
- Flaky tests are blocking; fix or remove immediately.
- Use deterministic tests: fixed seeds and bounded timeouts.
- Do not use unbounded sleeps or wall-clock-dependent assertions.
- Maintain a practical total test runtime budget and optimize regressions quickly.

### Mocking Rules

- Default: no mocks.
- Allowed only for true external boundaries (network APIs, Discord transport, system clock/time, filesystem failure injection, spawned CLI process seams).
- When mocking, keep one focused seam and still exercise realistic flows elsewhere.

## 3. Coverage Enforcement

- Coverage is not a merge gate and has no percentage floor.
- `make coverage` generates a full LCOV report when a contributor wants one.
- `make coverage-quick` and `make coverage-diagnostics` provide focused feedback.
- Do not distort production code, formatting, or control flow solely to improve a coverage metric.
- `cargo-llvm-cov` and LLVM tools are optional local dependencies.

## 4. Dead Code and Static Hygiene

- Treat rustc warnings and Clippy correctness, suspicious, and performance findings as errors.
- Keep Clippy style and complexity findings visible as warnings, not merge blockers.
- Deny unused/dead items (e.g. `dead_code`, `unused_imports`, `unused_variables`) at crate lint level.
- Enforce public API wiring: every `pub fn` must have at least one cross-file usage, or visibility must
  be reduced (`pub(crate)`/private).
- Run `make clippy`; its ordered repository wrapper is the lint-policy source for Clippy runs.
- Run `cargo fmt --all -- --check`.

## 5. Duplication Control

- `make duplication-check` is an optional production-code report, not a merge gate.
- It ignores tests and only reports clones of at least 10 lines / 100 tokens above 10% duplication.
- Prefer a small, readable repetition over a premature abstraction; refactor when a repeated concept
  is likely to evolve together.

## 6. CI Gates (Must All Pass)

`make quality` runs exactly these three blocking gates in order across Crab v1 and v2. CI uses the
same checks over the conservatively selected workspace scope:

1. `fmt` — both Rust workspaces.
2. `clippy` — v1's ordered repository lint policy plus strict v2 workspace Clippy.
3. `tests` — both Rust workspace suites plus first-party v2 bridge tests.

Coverage, duplication, public-API analysis, and workflow-tool tests remain available as explicit
diagnostics. They are not required on every product-code change.

## 7. Implementation Standards

- Keep modules small and cohesive.
- Prefer explicit types and errors over implicit behavior.
- Avoid panics in production paths; return typed errors.
- Log with structured, actionable messages.
- Keep bootstrap-context observability intact:
  emit injected context size metrics (`injected_context_tokens`, `injected_context_chars`) when a
  new physical session receives full context injection.
- Document non-obvious design decisions in code comments or short ADR notes.
- When rejecting user actions (queue overflow, unsupported operation, invalid command), return explicit user-facing reasons; avoid opaque failure text.

## 8. Change Discipline

- Keep PRs scoped and atomic.
- Include: what changed, why, and how it was validated.
- Update docs/config/scripts when quality gates change.
- Keep architecture docs current when behavior changes (`crab/DESIGN.md` and `crab/docs/`).
- When behavior ships, update the relevant docs and issue/project status in the same work session
  (do not defer synchronization to a later pass).
- Do not merge if any gate is skipped.
- No `TODO`/`FIXME` in committed code without a linked GitHub issue in this repository.
- Before commit/push, verify `git status` contains only intentional changes; remove accidental tool/agent artifact files.
- If unexpected files or directories appear during implementation, pause and confirm handling before proceeding.

## 9. Toolchain and Lockfiles

- Keep `rust-toolchain.toml` pinned to the exact supported Rust release (`1.93.0` currently).
- Update the pin deliberately, roughly once per Rust minor: use a scoped PR, update CI and the
  pin together, and fix newly surfaced lints in that same PR.
- Commit `Cargo.lock` for reproducible builds.

## 10. Issue Tracking Boundaries

- For this project, only create/edit/reference issues in this repository.
- Do not create/edit issues in external repositories while working on this codebase.
- Keep issue and project status current while implementing:
  use `status:todo`, `status:in-progress`, `status:done` labels and mirror the same state in the project board.
- Update acceptance checklist/task list in the issue as scope changes.
- Link implementation artifacts (commit/PR) from the issue and close completed issues promptly.

## 11. Enforced Quality Setup (Current)

The repository now enforces quality with executable gates and CI automation.

### Canonical agent loop

1. Run read-only prerequisite checks: `make doctor`.
2. During edits, run changed-scope validation: `make check`.
3. Before handoff, run every authoritative gate: `make quality`.

Bare `make` is read-only help. `make quick` is only a deprecated alias for `make check`.
`DRY_RUN=1 make check` prints the selected scope and exact commands. Workflow tooling under
Changes to workflow tooling should run the deterministic offline `make gate-tests` suite.

### Code Quality Report (Generated)

- Generate/update `CODE_QUALITY_REPORT.md` with:
  `./scripts/gen_code_quality_report.sh`
- Capture runtime/density baseline snapshots with:
  `make quality-baseline`
- Policy:
  - `CODE_QUALITY_REPORT.md` must be derived from the generator script; do not hand-edit it.
  - Do not commit tool output directories (for example `mutants.out*/` from `cargo mutants`).

### Gate commands (local + CI)

- Format check:
  `make fmt-check`
- Lint:
  `make clippy`
- Public API wiring check:
  `make public-api-check`
- Tests:
  `make test`
- Full coverage report (optional):
  `make coverage`
- Faster changed-package coverage report (optional):
  `make coverage-quick`
- Coverage diagnostics (actionable uncovered line locations):
  `make coverage-diagnostics`
- Coverage modes are `worktree` (default local), guarded `staged`, and guarded `committed` (CI).
  Every mode requires a resolvable merge base. Coverage commands always generate fresh data in a
  worktree-local target; pre-existing `--lcov` input is diagnostic-only.
- Duplication report (optional, findings are non-blocking):
  `make duplication-check`

### Status and exit behavior

Repository workflow scripts use `0` for success, `1` for a gate failure, and `2` for usage,
environment or stale-attestation errors. `make quality` writes atomic
`quality/status.json` with the Git identity, tool versions, full check outcomes and rerun
commands, and start/end fingerprints over HEAD, index consistency, file modes, and all non-ignored
working-tree content. The index must globally be either HEAD or the fully staged validated worktree;
cross-file partial staging and intent-to-add entries are rejected. Executable-mode hashing follows
Git's owner-execute bit. A valid pass has matching fingerprints and zero skipped required gates.
`make quality-status` returns 1 only for a genuine, still-current failed gate result; invalid,
malformed, or stale artifacts return 2. Run it to verify the artifact still attests the current
tree; do not hand off a stale, invalid, failed, or skipped gate.
Scalar types are exact: booleans and floats are never accepted where integer schema, count, or exit
code fields are required, and durations must be finite and non-negative.

### Worktrees and optional shared builds

Set `CRAB_SHARED_TARGET_DIR` to an external absolute directory to opt into a per-repository Cargo
target namespace for build, Clippy, and test commands. The value is validated after symlink
resolution, must be writable, and must be outside every worktree and Git common directory.
Coverage always overrides it with a worktree-local target because concurrent LLVM coverage runs
clean and consume profile state. Shared top-level binaries can be overwritten by another
worktree; gates never rely on an artifact persisting after the command finishes.

### Source-of-truth files

- Rust workspace and lints:
  `Cargo.toml`
- Pinned toolchain/components:
  `rust-toolchain.toml`
- Reproducible dependency graph:
  `Cargo.lock`
- Local gate runner:
  `Makefile`
- Coverage gate script:
  `scripts/coverage_gate.sh`
- Duplication config:
  `.jscpd.json`
- CI workflow:
  `.github/workflows/quality.yml`
- PR quality checklist:
  `.github/pull_request_template.md`
- Issue templates:
  `.github/ISSUE_TEMPLATE/task.yml`
  `.github/ISSUE_TEMPLATE/bug.yml`
  `.github/ISSUE_TEMPLATE/config.yml`

### Required local prerequisites

- Exact Rust `1.93.0` and v2 Rust `1.97.1` toolchains with rustfmt and Clippy installed through
  their respective `rust-toolchain.toml` files.
- Python 3.11 or newer (stdlib-only workflow tooling).
- Node.js 20 or newer when v2 first-party bridge tests are selected.
- Optional coverage diagnostics: `cargo-llvm-cov` 0.6.21 and `llvm-tools-preview`.
- Optional duplication diagnostics: `jscpd` 4.0.5 (Node.js/npm only for installation).
- Optional public-API diagnostics: ripgrep (`rg`).

### CI behavior

- CI uses one job with exact per-workspace toolchain pins and changed-scope format, Clippy, and tests.
- Immediately after checkout, a Git-only classifier provides an explicit docs-only skip before
  toolchain/cache/package setup.
- Pull requests use the PR base SHA. Pushes use the non-zero event `before` SHA or first parent;
  scope selection conservatively falls back to the full workspace if neither baseline resolves.

## 12. Deferred for Later

- Branch protection and required CI check enforcement.
- Dependency/security gates (`cargo deny`, `cargo audit`).
- Mutation testing on critical modules (useful, but enable once core foundations exist).

## 13. GitHub Project Tracking (Maintainer)

- All workstream/task issues for this repo must be tracked in the maintainer's private GitHub Project
  (`Crab Workstreams`). This is for maintainer use only.
- Keep project visibility as `PRIVATE`.
- When creating new implementation issues, add them to this project immediately.
- Do not track this repo's work in external projects.

### Project Ops Commands

Replace `<owner>` with the repository owner's GitHub username and `<project-number>` with the
project number before running these commands.

- Create project:
  `gh project create --owner <owner> --title "Crab Workstreams"`
- Enforce private visibility:
  `gh project edit <project-number> --owner <owner> --visibility PRIVATE`
- Add an issue to project:
  `gh project item-add <project-number> --owner <owner> --url https://github.com/<owner>/<repo>/issues/<id>`
- List project items:
  `gh project item-list <project-number> --owner <owner> --limit 500`

## 14. Target Machine Remote Access (Tailscale + tmux)

- Keep target-machine credentials only in local gitignored files under `.crab-secrets/`.
- Current local credential file path:
  `.crab-secrets/target-machine.env`
- Required file mode:
  `chmod 600 .crab-secrets/target-machine.env`

### Discord Secrets (Local, Gitignored)

- Store Discord bot secrets locally under:
  `.crab-secrets/discord.env`
- Required file mode:
  `chmod 600 .crab-secrets/discord.env`
- Do not paste tokens into issues, commits, or chat logs. Prefer editing the secret file locally.

### tmux-first rule

- All long-running/operational work on the Crab target machine must run inside tmux on the
  target machine itself (not local tmux on the controller laptop).
- Session naming convention on target machine:
  `crab-main` for primary operations, or `crab-<task>` for scoped work.

### Exact operator flow

1. Load credentials locally:
   `set -a; source .crab-secrets/target-machine.env; set +a`
2. SSH to target machine:
   `ssh "$CRAB_TARGET_USER@$CRAB_TARGET_HOST"`
3. Ensure `tmux` is available on the target machine:
   - If `tmux` is not found, it is expected to be installed via Homebrew at `/opt/homebrew/bin/tmux`.
   - If your shell does not have Homebrew on `PATH`, run:
     `eval "$(/opt/homebrew/bin/brew shellenv zsh)"`
   - If Homebrew is missing, install it using the official Homebrew installer, then:
     `brew install tmux`
4. On target machine, start/attach tmux:
   `tmux new -As crab-main`
5. Run commands/services inside that remote tmux session.
6. Detach from remote tmux without stopping work:
   `Ctrl-b d`
7. Reattach later on target:
   `tmux attach -t crab-main`

### macOS Code Signing Note (Target Machine)

On newer macOS builds (observed on macOS 26.2), Rust binaries that ship with a linker-produced
ad-hoc signature (`linker-signed`) may be killed immediately by the OS when executed after being
copied in (for example via `scp`), showing up as `SIGKILL` in the connector logs.

Remediation on the target machine: re-sign the executables locally with `codesign`:

- `codesign --force --sign - ~/crab-bin/crabd`
- `codesign --force --sign - ~/crab-bin/crab-discord-connector`
- `codesign --force --sign - ~/crab-bin/crabctl`

Track upstream fix work in GitHub issue `#151`.

### Avoiding Orphaned `crabd` Processes

The runtime is a parent process (`crab-discord-connector`) that spawns a child process (`crabd`).
If the parent is terminated abruptly, the child can remain alive and keep mutating Crab state.

Current behavior:

- `crab-discord-connector` handles `SIGTERM` and shuts down cleanly so its `crabd` child is not
  orphaned (landed in main on 2026-02-13).

Operator check (target machine):

- `pgrep -x crab-discord-connector | wc -l` should be `1`
- `pgrep -x crabd | wc -l` should be `1`

### Non-interactive command execution (remote tmux)

- For automation, remote commands should create/use tmux on the target machine:
  `ssh ... 'tmux new-session -d -s crab-<task> \"<cmd>\"'`
- Use `tmux send-keys` + `tmux capture-pane` remotely when command output must be collected
  without interactive attachment.

## 15. Skills Governance Policy

- Canonical skills location in Crab workspaces is `.agents/skills`.
- `.claude/skills` is compatibility-only and must remain a symlink to `.agents/skills`.
- Do not create/update skills anywhere else in workspace/runtime operations.
- Built-in required policy skill path:
  `.agents/skills/skill-authoring-policy/SKILL.md`
- For skill-authoring tasks, read and follow that policy first.

## 16. Persisted State Evolution Policy

- Treat any persisted runtime-state schema change as a migration decision point.
- Default policy: additive-only schema evolution.
- If a non-additive or compatibility-impacting change is required, ship:
  - a schema-version bump,
  - an explicit startup migration step (`vN -> vN+1`),
  - compatibility preflight behavior/doctor output updates,
  - regression tests for forward migration and idempotent rerun.
- Do not merge persisted-state changes without documenting migration/compatibility impact in
  `crab/docs/07-storage-and-state-model.md` and updating the related issue checklist.
