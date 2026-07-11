# Agent Workflow Implementation Report

## Scope and baseline

This change is based on `fb8faf678fe08aa4157833464a4f55c3fe600c79`. It changes repository
workflow/configuration only, except for removing the conflicting crate-local lint attribute from
`crates/crab-core/src/lib.rs`. Crab runtime behavior and persisted state are unchanged.

The pre-change experience required agents to reconstruct intent from Make targets, shell scripts,
CI, and prose. Bare `make` mutated Rust sources. `make quick` ran format checking, workspace Clippy,
another all-target workspace check, public-API scanning, and the full test suite. Patch coverage was
silent without `BASE_REF`, ignored uncommitted changes, and emitted incomplete diagnostics. CI
floated on `stable`, making unrelated changes vulnerable to new style lints. Cargo artifacts were
cold in isolated worktrees, while LLVM coverage and ordinary builds had no distinct sharing policy.

## Selected design and alternatives

- A repository-owned `doctor -> check -> quality` surface was chosen over adding another external
  task runner. It keeps setup deterministic and command discovery available through read-only
  `make` help.
- Git-first package selection with conservative full fallback was chosen over file-glob-only rules
  or always-full checks. It understands reverse dependencies without allowing metadata failure to
  hide real compiler diagnostics.
- The redundant standalone `cargo check` was removed. Clippy retains matching all-target/all-feature
  compile coverage; tests remain a separate, meaningful behavior traversal.
- The manifest documents the lint tri-state. A compiled fixture proved that manifest priority alone
  cannot lower Clippy tool lints after rustc `deny(warnings)`, so one repository wrapper applies
  `--force-warn` to style/complexity while keeping rustc and high-signal Clippy groups fatal.
- Exact Rust and cargo-llvm-cov pins were chosen over floating stable and flag fallbacks. Pin updates
  are deliberate maintenance PRs.
- Opt-in shared Cargo targets were chosen over making `sccache` a new prerequisite. The concurrent
  experiment passed; per-repository namespacing and path validation prevent cross-repository or
  source-tree mixing. Coverage remains unconditionally local because LLVM profile cleanup is not
  concurrency-safe.
- A fingerprinted atomic status artifact was chosen over console-only success or reusable LCOV
  provenance. Authoritative coverage always starts fresh; status attests the exact start/end tree.
- Existing CI job names with an immediate post-checkout docs classifier were chosen over workflow
  path filters, which can leave required checks absent or misleading.

## Coverage evidence and thresholds

A fresh, isolated full coverage run on the untouched base used the host's Rust 1.93.0 binaries and
cargo-llvm-cov 0.6.21:

| Measure | Base result | New floor | Headroom |
|---|---:|---:|---:|
| Functions | 100.0000% (2623/2623) | 99.5% | 0.5000 pp |
| Regions | 99.6020% (53554/53768) | 99.0% | 0.6020 pp |
| Lines | 99.9223% (37301/37330) | 99.4% | 0.5223 pp |

The cold run took 734.92s. Changed executable lines require 95%; the allowed uncovered count is
`floor(0.05 × changed lines)`, retaining 100% for patches below 20 executable lines. Changed code
files absent from LCOV fail closed, and JSON diagnostics are never truncated.

## Command and timing evidence

The sandbox could not create the requested base worktree because the Git common directory at
`/Users/jim/crab-source/.git` is read-only. The legacy timing therefore used a network-free
`git archive` materialization of the exact base SHA under `/private/tmp`; no source checkout or
repository state was changed. Shared-cache testing used a separate local clone whose two worktrees
were genuinely detached and divergent.

Legacy `make -n quick` enumerated five commands: format check, Clippy, `cargo check`, public-API
scan, and tests. Three were Cargo compilation/test traversals. The legacy CI fast path separately
had two compilation traversals (Clippy and the redundant check). The new representative loop
selected one leaf package (`crab-app`) and enumerated format, scoped Clippy, and scoped tests: two
Cargo traversals and no redundant check.

| Loop | Cold | Warm runs | Warm median |
|---|---:|---:|---:|
| Base `make quick` | 998.73s | 39.73s, 39.63s | 39.68s |
| New `make check` (`crab-app` edit) | 463.60s | 15.73s, 16.22s | 15.98s |

This is a 53.6% cold-loop reduction and a 59.7% warm-loop reduction in the observed environment.
The user-facing command count is now three stable decisions: discover/preflight (`make doctor`),
iterate (`make check`), and attest (`make quality`).

Two successful full-workspace `make quality` runs after cache warm-up took 327.00s and 280.41s
(median 303.71s). The first had to rebuild Clippy artifacts after switching from the floating alias
to the temporary exact-toolchain alias; the second reused them. A separate truly cold end-to-end
quality timing is not claimed: its dominant cold components were measured independently above
(463.60s changed-scope build/test and 734.92s isolated coverage), which avoids presenting a summed
estimate as an observed wall time.

## Shared-target experiment

Two detached worktrees in a local clone used divergent commits `1e0e51f…` and `e371fe9…`, one
external `CRAB_SHARED_TARGET_DIR`, offline Cargo, and the same per-repository namespace. Both ran
scoped `crab-telemetry` Clippy and tests concurrently. Cargo visibly waited on build/artifact locks;
both commands succeeded, both `git status --short` outputs remained empty, neither checkout gained
a `target/`, and the shared base contained exactly one repository namespace.

| Concurrent phase | Cold elapsed per worktree | Warm elapsed per worktree |
|---|---:|---:|
| Clippy | 34.20s / 34.21s | 1.16s / 1.17s |
| Tests | 39.28s / 39.26s | 1.23s / 1.21s |

Cold concurrent writers serialize and therefore do not improve first-run throughput; reuse is the
benefit. The option remains disabled by default, and docs call out file locks and top-level binary
overwrite behavior.

## Validation record

- `make gate-tests`: 51 deterministic tests cover shared/ambient target isolation,
  focused-coverage skip, read-only bare Make behavior, immediate CI classification ordering, and
  exact duplication-tool preflight.
- Full-workspace Clippy under Rust 1.93.0 binaries: passed; two existing style findings were visible
  and nonfatal as designed.
- Fresh `make coverage-gate`: passed at 100.0000% functions (2623/2623), 99.6076% regions
  (53557/53768), and 99.9304% lines (37304/37330); this tooling-only patch added no executable
  production line, so the patch result was an explicit 0/0 pass.
- Deprecated `make quick` delegation: passed the conservative full scope (format, Clippy, and all
  workspace tests).
- An initial full-orchestrator run exposed the inherited implicit `npx --yes` install at the
  duplication gate. The gate now requires local jscpd 4.0.5, doctor checks it up front, and CI
  installs that exact version explicitly after docs-only classification.
- The post-fix authoritative `make quality` passed all seven required checks in 327.00s with
  matching start/end fingerprints and zero skips. Status inspection then found and corrected a
  display-only cargo-llvm-cov version probe. The subsequent 280.41s run passed with the correct
  Rust, Clippy, and cargo-llvm-cov versions in status; a last gate follows this report update.
- Persistent installation of the explicit rustup alias was blocked by sandbox permissions. For
  validation, a temporary rustup home linked the already-installed, byte-identical Rust 1.93.0
  toolchain and a temporary PATH exposed cached jscpd 4.0.5; `make doctor` then passed every check.
  No repository path or user tool installation was modified.
- Final format/tests/quick/status results are recorded in the handoff report after the final pass.
