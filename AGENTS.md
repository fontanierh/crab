# AGENTS.md

Project operating rules for all human and AI contributors.

## 1. Non-Negotiable Quality Bar

- Maintain `100%` test coverage for production code.
- Coverage must be checked automatically and fail the build if it drops below `100%`.
- Prefer real integration behavior over mocks; use mocks only at unavoidable boundaries.
- Tests must verify meaningful behavior, not implementation trivia.
- Tests must stay fast; slow tests are treated as quality regressions.
- No dead code in committed code.
- Keep duplication low and actively refactor repeated logic.
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

- Coverage check is required in CI and local pre-merge validation.
- Preferred Rust tool: `cargo-llvm-cov`.
- Enforce `100%` line/function minimum and `0` uncovered lines/functions in CI.
- Coverage reports must be reproducible from a single documented command.
- Note: `cargo-llvm-cov` enables `cfg(coverage)`. Be careful when adding logging with `tracing::*`
  macros: they can introduce coverage gaps (especially multi-line invocations or rarely-hit branches).
  If coverage fails after adding logs, fix it by:
  - adding tests that exercise the relevant branches, and/or
  - scoping purely-observability statements behind `#[cfg(not(coverage))]` when the tool's mapping
    produces false negatives (do not hide business logic from coverage).

Required outcome:
- Any uncovered production line/function fails validation.
- Region coverage can be reported for diagnostics, but is not a hard gate due known false negatives around monomorphized generic code.
- Current documented exception: `make coverage-gate` excludes `crates/crab-app/src/installer.rs`
  via `--ignore-filename-regex` because of a reproducible `cargo-llvm-cov` line-mapping false
  negative in this file. Treat this as temporary and remove the exclusion once the coverage tool
  behavior is fixed.

## 4. Dead Code and Static Hygiene

- Treat warnings as errors in CI.
- Deny unused/dead items (e.g. `dead_code`, `unused_imports`, `unused_variables`) at crate lint level.
- Run `cargo clippy --all-targets --all-features -- -D warnings`.
- Run `cargo fmt --all -- --check`.

## 5. Duplication Control

- Measure duplication with an automated tool (e.g. `jscpd`), and fail when above threshold.
- Start strict for production Rust source; exclude generated files, fixtures, and vendored code.
- If logic is repeated more than once, extract common abstractions unless it clearly harms readability.

## 6. CI Gates (Must All Pass)

- `fmt` check
- `clippy` with warnings denied
- full test suite
- coverage gate at 100%
- duplication gate

No bypasses on main branch.

## 7. Implementation Standards

- Keep modules small and cohesive.
- Prefer explicit types and errors over implicit behavior.
- Avoid panics in production paths; return typed errors.
- Log with structured, actionable messages.
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

- Keep `rust-toolchain.toml` in repo and pin channel to `stable` (latest stable Rust).
- Commit `Cargo.lock` for reproducible builds.

## 10. Issue Tracking Boundaries

- For this project, only create/edit/reference issues in:
  `https://github.com/fontanierh/2026-02-06-autofun`.
- Do not create/edit issues in external repositories while working on this codebase.
- Keep issue and project status current while implementing:
  use `status:todo`, `status:in-progress`, `status:done` labels and mirror the same state in the project board.
- Update acceptance checklist/task list in the issue as scope changes.
- Link implementation artifacts (commit/PR) from the issue and close completed issues promptly.

## 11. Enforced Quality Setup (Current)

The repository now enforces quality with executable gates and CI automation.

### Canonical local command

- Run all required checks with:
  `make quality`

### Code Quality Report (Generated)

- Generate/update `CODE_QUALITY_REPORT.md` with:
  `./scripts/gen_code_quality_report.sh`
- Policy:
  - `CODE_QUALITY_REPORT.md` must be derived from the generator script; do not hand-edit it.
  - Do not commit tool output directories (for example `mutants.out*/` from `cargo mutants`).

### Gate commands (local + CI)

- Format check:
  `make fmt-check`
- Lint:
  `make clippy`
- Dead code/static check:
  `make deadcode-check`
- Tests:
  `make test`
- Coverage gate (100% lines/functions, 0 uncovered lines/functions):
  `make coverage-gate`
- Note: the coverage gate command currently includes
  `--ignore-filename-regex 'crates/crab-app/src/installer.rs'` for the documented tool-mapping
  false negative.
- Duplication gate:
  `make duplication-check`

### Source-of-truth files

- Rust workspace and lints:
  `Cargo.toml`
- Pinned toolchain/components:
  `rust-toolchain.toml`
- Reproducible dependency graph:
  `Cargo.lock`
- Local gate runner:
  `Makefile`
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

- Rust stable toolchain (installed via `rust-toolchain.toml`).
- `cargo-llvm-cov` (compatible pinned version):
  `cargo install cargo-llvm-cov --version 0.6.21 --locked`
- LLVM tools component:
  `rustup component add llvm-tools-preview`
- Node runtime available for `npx` (used by `jscpd`).

### CI behavior

- CI runs `fmt`, `clippy`, `deadcode-check`, `test`, `coverage-gate`, and `duplication-check`.
- Any gate failure blocks merge readiness.

## 12. Deferred for Later

- Branch protection and required CI check enforcement.
- Dependency/security gates (`cargo deny`, `cargo audit`).
- Mutation testing on critical modules (useful, but enable once core foundations exist).

## 13. GitHub Project Tracking (Private)

- All workstream/task issues for this repo must be tracked in the private GitHub Project:
  `https://github.com/users/fontanierh/projects/1` (`Crab Workstreams`).
- Keep project visibility as `PRIVATE`.
- When creating new implementation issues, add them to this project immediately.
- Do not track this repo's work in external projects.

### Project Ops Commands

- Create project:
  `gh project create --owner fontanierh --title "Crab Workstreams"`
- Enforce private visibility:
  `gh project edit 1 --owner fontanierh --visibility PRIVATE`
- Add an issue to project:
  `gh project item-add 1 --owner fontanierh --url https://github.com/fontanierh/2026-02-06-autofun/issues/<id>`
- List project items:
  `gh project item-list 1 --owner fontanierh --limit 500`

## 14. Target Machine Remote Access (Tailscale + tmux)

- Keep target-machine credentials only in local gitignored files under `.crab-secrets/`.
- Current local credential file path:
  `.crab-secrets/target-machine.env`
- Required file mode:
  `chmod 600 .crab-secrets/target-machine.env`

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
3. On target machine, start/attach tmux:
   `tmux new -As crab-main`
4. Run commands/services inside that remote tmux session.
5. Detach from remote tmux without stopping work:
   `Ctrl-b d`
6. Reattach later on target:
   `tmux attach -t crab-main`

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
