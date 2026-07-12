# AGENTS.md

Project operating rules for all human and AI contributors.

## 1. Non-Negotiable Quality Bar

- Maintain demanding aggregate and changed-line coverage for production code.
- Coverage must be checked automatically and fail below the documented thresholds.
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
- Enforce `99.5%` function, `98.93%` region, and `99.4%` line coverage in CI.
- Enforce `95%` coverage of changed executable production lines, with a small-patch floor.
- Coverage reports must be reproducible from a single documented command.

### Aggregate and patch coverage gates

| Gate | Threshold | Scope | Enforced by |
|------|-----------|-------|-------------|
| Function coverage | `99.5%` | All production code | `--fail-under-functions 99.5` |
| Region coverage | `98.93%` | All production code | `--fail-under-regions 98.93` |
| Line coverage | `99.4%` | All production code | `--fail-under-lines 99.4` |
| Patch coverage | `95%` | Changed executable production lines | `scripts/patch_coverage.py` |

Patch allowance is `floor(0.05 × changed executable lines)`, so patches under 20 executable
lines still require 100%. The gate reports exact uncovered lines in a complete JSON artifact.
Production coverage means `crates/*/src/**/*.rs`, excluding crate `tests/` trees and
`src/test_support.rs`.

### `tracing` macros and `cfg(not(coverage))`

`tracing::*` macros (`info!`, `warn!`, `debug!`) expand under `cargo-llvm-cov` into closures
and branches that create false coverage gaps, even when the code path is fully exercised by
tests. This is a known `cargo llvm-cov` + `tracing` interaction.

**Why `cfg(not(coverage))` guards exist:** Removing all 16 guards was tested (2026-04-09). Region
coverage passes (99.50%), but function coverage drops to 99.96% (1 macro-introduced closure
counted as an uncovered function). Patch coverage would also reject the changes on PR. The
guards remain necessary to avoid macro-generated false gaps in aggregate and patch results.

**Rules for `cfg(not(coverage))` usage:**
- Only use on `tracing::*` macro invocations and their immediately-surrounding blocks.
- Never hide business logic, error handling, or state mutations behind `cfg(not(coverage))`.
- When a block mixes tracing with logic (e.g., computing values only for log output),
  confine the guard to the smallest scope that includes only the tracing call and any
  variables used exclusively by that call.
- Add a `#[cfg(coverage)] let _ = var;` suppression when the guard causes an unused-variable
  warning in coverage builds.

### Multi-line call sites and `#[rustfmt::skip]`

`cargo llvm-cov` maps coverage regions per source line. When `rustfmt` splits a single
function call across multiple lines, each line becomes a separate coverage region. If the
call is on a path not always taken, some lines show as uncovered even though the call is
atomic. Use `#[rustfmt::skip]` to keep such calls on one line:

```rust
#[rustfmt::skip]
let result = some_function(arg1, arg2, arg3)?;
```

This is only needed for function calls where splitting would create false coverage gaps.
Do not blanket-apply it.

Required outcome:
- Total function/region/line coverage stays at or above `99.5%` / `98.93%` / `99.4%`.
- Changed executable lines meet the 95% gate and its small-patch floor.
- Changed production files absent from LCOV fail closed.
- Missing-line output is diagnostic outside the changed patch gate.

## 4. Dead Code and Static Hygiene

- Treat rustc warnings and Clippy correctness, suspicious, and performance findings as errors.
- Keep Clippy style and complexity findings visible as warnings, not merge blockers.
- Deny unused/dead items (e.g. `dead_code`, `unused_imports`, `unused_variables`) at crate lint level.
- Enforce public API wiring: every `pub fn` must have at least one cross-file usage, or visibility must
  be reduced (`pub(crate)`/private).
- Run `make clippy`; its ordered repository wrapper is the lint-policy source for Clippy runs.
- Run `cargo fmt --all -- --check`.

## 5. Duplication Control

- Measure duplication with an automated tool (e.g. `jscpd`), and fail when above threshold.
- Start strict for production Rust source; exclude generated files, fixtures, and vendored code.
- Duplication gate is production-focused:
  `scripts/duplication_check.sh` runs `jscpd` over crate Rust sources with explicit ignore rules
  (for example `src/test_support.rs`).
- If logic is repeated more than once, extract common abstractions unless it clearly harms readability.

## 6. CI Gates (Must All Pass)

`make quality` runs exactly these seven gates in this order. CI enforces the same set across its
two required jobs (`fast` and `coverage`):

1. `fmt` — `cargo fmt --all -- --check`.
2. `clippy` — repository lint policy; rustc warnings and correctness/suspicious/performance are
   denied, while style/complexity remain visible.
3. `tests` — full workspace suite in the normal non-coverage configuration.
4. `public-api` — cross-file public API wiring.
5. `duplication` — production Rust duplication gate.
6. `gate-tests` — deterministic offline workflow-tool tests.
7. `coverage` — fresh aggregate coverage at `99.5%` functions / `98.93%` regions / `99.4%` lines,
   plus `95%` patch coverage.

No bypasses on main branch.

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
`scripts/` is governed by the deterministic offline `make gate-tests` suite.

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
- Coverage gate (`99.5%` functions, `98.93%` regions, `99.4%` lines, and patch coverage):
  `make coverage-gate`
- Faster changed-package coverage with report-only aggregates and blocking patch coverage:
  `make coverage-quick`
- Coverage diagnostics (actionable uncovered line locations):
  `make coverage-diagnostics`
- Coverage modes are `worktree` (default local), guarded `staged`, and guarded `committed` (CI).
  Every mode requires a resolvable merge base. Coverage commands always generate fresh data in a
  worktree-local target; pre-existing `--lcov` input is diagnostic-only.
- Duplication gate:
  `make duplication-check`

### Status and exit behavior

Repository workflow scripts use `0` for success, `1` for a gate failure, and `2` for usage,
environment, baseline, or stale-attestation errors. `make quality` writes atomic
`quality/status.json` with the Git/base identity, tool versions, full check outcomes and rerun
commands, and start/end fingerprints over HEAD, index consistency, file modes, and all non-ignored
working-tree content. The index must globally be either HEAD or the fully staged validated worktree;
cross-file partial staging and intent-to-add entries are rejected. Executable-mode hashing follows
Git's owner-execute bit. A valid pass has matching fingerprints and zero skipped required gates.
`make quality-status` returns 1 only for a genuine, still-current failed gate result; invalid,
malformed, or stale artifacts return 2. Run it to verify the artifact still attests the current
tree; do not hand off a stale, invalid, failed, or skipped gate.

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

- Exact Rust `1.93.0` toolchain/components installed via `rust-toolchain.toml`.
- `cargo-llvm-cov` (compatible pinned version):
  `cargo install cargo-llvm-cov --version 0.6.21 --locked`
- LLVM tools component:
  `rustup component add llvm-tools-preview`
- Python 3.11 or newer (stdlib-only workflow tooling).
- Exact runnable `jscpd` 4.0.5; Node.js/npm are needed only to install or change it
  (`npm install --global jscpd@4.0.5`).
- Ripgrep (`rg`) for public API checks.

### CI behavior

- CI uses the exact toolchain pin, changed-scope format/Clippy/tests, public API checks, workflow
  tests, fresh coverage, and duplication checks; the redundant standalone `cargo check` is gone.
- Immediately after checkout, a Git-only classifier keeps the existing `fast` and `coverage` job
  names green with an explicit docs-only skip, before toolchain/cache/package setup.
- Pull requests use the PR base SHA. Pushes use the non-zero event `before` SHA or first parent;
  coverage fails closed if neither baseline resolves.
- Any required gate failure or skip blocks merge readiness.

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
