# Agent Workflow Implementation Report

## Scope and repository identity

This final audit applies to committed implementation `57866774c0843e9637cc73df99e625e368fd82cc`.
Its merge base with `origin/main` is `1e0e51f8efb1dfd6cde8b9f7a0d5605e057927a7`, the merged
`crab-factory` base. The workspace has nine members, including `crates/crab-factory`, and all nine
inherit the workspace lint table.

The final-audit patch changes repository workflow tooling, deterministic workflow tests, CI, and
documentation only. It adds no Rust production changes; the authoritative patch-coverage result is
therefore an explicit 0/0 pass. Crab runtime behavior and persisted-state compatibility are unchanged.

The canonical surface remains:

```text
make doctor  ->  make check  ->  make quality
preflight        edit loop       seven-gate handoff attestation
```

## Final audit decisions

- The authoritative gate retains all seven checks, in fixed order: `fmt`, `clippy`, `tests`,
  `public-api`, `duplication`, `gate-tests`, and `coverage`. The standalone normal-configuration
  test traversal has measured correctness value and is not redundant with coverage. In
  `crates/crab-app/src/daemon.rs`, lines 472-494 provide the deterministic
  `cfg(any(test, coverage))` `ClaudeProcess`, while lines 496-537 begin production-only state and
  the real `cfg(not(any(test, coverage)))` implementation. `turn_executor.rs:2153` similarly
  selects the real attachment downloader only outside test/coverage builds, and
  `crab-discord-connector/src/main.rs:376` has explicit coverage-only branches. LLVM coverage sets
  `cfg(coverage)`, so normal `cargo test --workspace` is the only quality gate compiling and
  exercising the non-coverage binary configuration through integration entrypoints. It also keeps
  native doctest execution automatic.
- Changed-scope collection includes deletions and disables rename detection, causing both sides of
  cross-package renames to participate. Deleted crate paths map lexically, deleted workflow inputs
  force full scope, and unmapped paths fail conservatively to the full workspace.
- Docs-only classification now requires both an approved suffix and an exact documentation
  location/file allowlist. Files under `crates/`, `scripts/`, `crab/config/`, unknown root paths,
  and binary assets under docs locations run code gates.
- Patch diff parsing is hunk-state-aware, including added content beginning with `++`; type changes
  participate because the production-line collector no longer filters diff statuses.
- Direct Cargo-target dry-run parsing splits on the first literal `--`, accepts conventional flag
  ordering, performs all validation, and creates no local/shared target directory.
- Staged and committed patch modes reject every non-documentation divergence between the working
  tree and selected snapshot, including manifests, lockfiles, fixtures, configuration, and the gate
  tooling itself. This conservative clean-tree policy is intentional because gates execute their
  working copies.
- Status schema 2 fingerprints HEAD plus non-ignored working content and executable modes. It
  rejects split staged/unstaged content, restored staged deletions, conflicts, hidden
  `assume-unchanged`/`skip-worktree` entries, and `core.filemode=false`. The check record must match
  the exact ordered seven-gate policy with seven successful exit-zero entries and no setup error.
  Staging exactly the validated bytes remains valid, preserving quality → `git add -A` → verify →
  commit.
- Repository-managed target, quality/log, status, and coverage paths reject symlink components and
  unsafe final files. Shared namespaces are independently validated. Ambient external
  `CARGO_TARGET_DIR` is rejected; explicit external sharing remains available only through
  `CRAB_SHARED_TARGET_DIR`.
- Coverage gate, quick, report, and diagnostic entrypoints invalidate the exact artifacts they will
  produce before execution. A failed run cannot leave prior green LCOV, summary, patch, or
  diagnostic output looking current.
- Doctor mirrors coverage baseline precedence (`BASE_SHA`, `BASE_REF`, `origin/main`) and verifies a
  merge base. Node/npm become informational when exact runnable jscpd 4.0.5 is already available.
- Both CI jobs retain the stable `fast` and `coverage` names, classify docs immediately after
  checkout, run on `ubuntu-24.04`, pin every action to a verified 40-hex commit, and pin Node
  20.20.2. Accepted residual drift is explicitly limited to npm's transitive resolution for exact
  `jscpd@4.0.5` and apt-provided ripgrep.
- Generated-report provenance now distinguishes a bare commit from that commit plus uncommitted
  worktree changes. Unused workflow helpers were removed, Python caches are ignored, all workspace
  lint inheritance is tested, and the Cargo manifest explains why the ordered Clippy wrapper is
  required for the style/complexity warning policy.

## Historical evidence retained from the implementation

These figures predate this final rebased audit and are retained as historical evidence, not
re-measured claims:

- Legacy loop: 998.73s cold and 39.68s warm median.
- Changed-scope loop: 463.60s cold and 15.98s warm median.
- Divergent-worktree shared cache: both worktrees clean and successful; approximately 34s Clippy
  plus 39s tests cold and approximately 1.2s each warm.
- Old-base coverage: 100.0000% functions, 99.6020% regions, and 99.9223% lines.
- Pre-rebase implementation coverage: 100.0000% functions, 99.6076% regions, and 99.9304% lines.
- Prior quality median: 303.71s; three implementation-stage quality runs passed with matching
  fingerprints and zero skips.

## Untouched rebased baseline

Before the first final-audit edit, a full `make quality` run passed on the untouched
`57866774…` tree in 1501.70s wall time. Per-gate durations from schema-1 `status.json` were:

| Gate | Seconds |
|---|---:|
| fmt | 2.287 |
| clippy | 244.388 |
| tests | 450.726 |
| public-api | 19.780 |
| duplication | 15.875 |
| gate-tests (pre-audit suite) | 26.923 |
| coverage | 740.006 |

This was a cold/cache-confounded baseline. It is recorded to satisfy before/after evidence, not to
attribute the later wall-time difference to removal of a gate; no gate was removed.

## Post-change measurement run

After all implementation changes and initial report regeneration, a refreshed full worktree-mode
`make quality` passed in 501.27s wall time. Schema-2 status recorded matching 24-entry dirty-tree
fingerprints, no setup error, seven successful checks, zero skips, and these durations:

| Gate | Seconds |
|---|---:|
| fmt | 2.042 |
| clippy | 1.860 |
| tests | 101.433 |
| public-api | 18.887 |
| duplication | 16.020 |
| gate-tests (90 tests) | 68.787 |
| coverage | 290.550 |

The 1000.43s wall-time reduction from the untouched run is primarily warm-cache/runtime variance;
it is not presented as a structural speedup. The correctness-motivated normal test gate remains.

Fresh authoritative coverage from that measurement run was:

| Measure | Covered / total | Result | Floor |
|---|---:|---:|---:|
| Functions | 2896 / 2896 | 100.0000% | 99.5% |
| Regions | 58512 / 59077 | 99.0436% | 99.0% |
| Lines | 40643 / 40758 | 99.7178% | 99.4% |
| Changed executable production lines | 0 / 0 | 100.0000% | 95% with small-patch floor |

## Deterministic validation coverage

`make gate-tests` now runs 90 offline tests. The regressions cover deletion-only and rename scope,
the exact docs allowlist, hunk line mapping, type changes, whole-tree snapshot guards, schema-2
attestation and malformed artifacts, mode/rename/untracked mutations, staging validated bytes,
hidden index flags, non-mutating dry-run argument orders, local/shared path symlinks, stale coverage
artifacts, doctor baseline precedence and jscpd installer nuance, coverage failure propagation,
immutable CI pins, generated-artifact ignores, and nine-member lint inheritance.

Before report regeneration, `make doctor`, `DRY_RUN=1 make check`, `make gate-tests`, and the required
deprecated `make quick` compatibility path all passed. `make quick` conservatively selected the full
workspace and passed format (2.34s), Clippy (21.65s), and normal tests (102.04s).

## Final attestation sequencing

This report and `CODE_QUALITY_REPORT.md` are tracked inputs, so the measurement status is intentionally
made stale by report generation. The final sequence is `make doctor` → `make gate-tests` →
`make quality` → `make quality-status`, with no tracked edits afterward. The resulting ignored
schema-2 `quality/status.json` is the proof that the final report-bearing tree has matching
fingerprints and zero skipped required gates. Failed runs invalidate status and their output
artifacts first; an unavailable baseline fails closed with fetch/`BASE_SHA` guidance.
