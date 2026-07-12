# Agent Workflow Remediation Implementation Report

## Scope and provenance

This report covers the uncommitted agent/code-factory workflow remediation based on commit
`ad41a780166a1af5a7e6582212121520ea215e3d`. The Rust workspace has nine members, including
`crab-factory`; all nine inherit the workspace lint policy. The remediation changes workflow
tooling, deterministic tests, CI policy, generated evidence, and documentation only. It changes no
Crab production Rust behavior or persisted runtime state, so authoritative patch coverage is an
explicit 0/0 pass.

The canonical workflow remains:

```text
make doctor  ->  make check  ->  make quality
preflight        edit loop       seven-gate handoff attestation
```

The generated `CODE_QUALITY_REPORT.md` derives provenance from the body actually published. Its
current header correctly identifies the base commit plus uncommitted worktree changes.

## Remediation decisions

1. Missing-baseline and Git-diff failures in `make check` now select full-workspace fmt, Clippy, and
   tests instead of treating an empty changed-file list as a successful skip. Dry-run exposes the
   fallback and exact full commands.
2. Schema-3 attestation rejects intent-to-add entries and any cross-file mixture of staged and
   unstaged/untracked paths. The index must globally be HEAD or the fully staged validated tree;
   complete `quality -> git add -A -> quality-status` remains valid.
3. Staged and committed coverage guards use the same `core.filemode` and hidden-index-flag
   preflight as tree attestation. Worktree mode remains intentionally permissive at that standalone
   guard because authoritative quality already fingerprints it.
4. Coverage validates the lexical `target/llvm-cov-worktree/instrumented` child, rejects symlinks,
   drops shared-target opt-in, and overrides both `CARGO_LLVM_COV_TARGET_DIR` and
   `CARGO_LLVM_COV_BUILD_DIR`. Coverage dry-run prints all three target variables without creating
   directories.
5. Working-tree mode hashing follows Git's owner-execute bit (`stat.S_IXUSR`); group/other execute
   bits no longer create false identity changes or mask an owner-execute change.
6. One shared policy excludes `src/test_support.rs` from patch classification and every aggregate
   LCOV/summary export. Each fresh LCOV is checked after generation and fails closed if an excluded
   source appears.
7. `scripts/lcov_stats.py` makes `LF`/`LH` authoritative for single-record totals and treats `DA:0`
   only as location evidence. Duplicate records merge hits per source line and fail when incomplete
   DA universes cannot be reconciled. Diagnostics, baselines, and generated hotspots share it.
8. Report, gate, and diagnostic commands invalidate the complete authoritative companion set
   (`lcov.info`, `summary.json`, `patch-coverage.json`, and `uncovered_locations.txt`) before
   replacing LCOV. Focused coverage retains its separate `quick-*` provenance set.
9. `scripts/quality_baseline.py` replaces the shell collector. It validates the baseline directory
   and four leaves before execution, preserves child exit classification, requires valid LCOV,
   atomically rewrites history, and publishes `latest.json` last. Production LOC now uses the same
   exclusion predicate as coverage.
10. GitHub Actions grants only `contents: read`; both checkout steps set
    `persist-credentials: false`. The stable `fast` and `coverage` names and classifier-first
    ordering are unchanged.
11. Quality/check validate `quality` and `quality/logs` lexically before scope/status work. Every
    planned log leaf is validated before the first gate executes, so internal and external symlink
    redirection cannot receive logs or status side effects.
12. `quality-status` returns 1 only for the genuine fail-fast gate-result shape while the failed
    artifact still attests the live tree. Invalid, unknown, malformed, non-UTF-8, setup-error, or
    stale artifacts return 2; passing artifacts require an explicit null `setup_error`.
13. A shared Cargo target base and its final repository namespace must be disjoint from every Git
    worktree and the Git common directory in both containment directions. Tests use real detached
    linked worktrees at and beneath the candidate cache.
14. `scripts/publish_report.py` compares the rendered body with the committed report body. Equal
    content preserves committed bytes; changed content receives dirty provenance and is published
    through a mode-preserving atomic replace. Malformed baseline history now fails report generation
    closed.
15. `AGENTS.md` names the exact seven gates once and in authoritative order: fmt, Clippy, tests,
    public API, duplication, workflow gate tests, and coverage. Contributor, workflow, factory, CI,
    and generated-report documentation use the same policy.

## Coverage universe and threshold evidence

Aggregate production coverage is Rust under `crates/*/src/**/*.rs`, excluding crate `tests/` trees
and every `src/test_support.rs`. The exclusion reduced the measured function universe from the
historical 2,896 functions to 2,862 and the line universe from 40,758 to 40,457; it did not conceal
uncovered production code. The patch gate remains 95%, with 100% required below 20 changed
executable lines.

The first fresh post-exclusion measurement (`make coverage`) reported 58,134/58,699 regions, or
99.03746231%. Applying the documented rule
`floor((R - 0.10) * 100) / 100` selects a 98.93% region floor. The dedicated measurement-quality
run exercised one additional region (58,135/58,699), while the final report-bearing quality run
returned to 58,134/58,699. The report therefore uses the lower reproducible result, which leaves
0.10746231 percentage points of headroom and safely covers the observed one-region execution
variance. Functions stay at 99.5% and lines at 99.4%; uncovered regions remain visible in LLVM
diagnostics and the generated hotspot evidence.

Fresh final report-bearing coverage was:

| Measure | Covered / total | Result | Floor |
|---|---:|---:|---:|
| Functions | 2,862 / 2,862 | 100.0000% | 99.5% |
| Regions | 58,134 / 58,699 | 99.0375% | 98.93% |
| Lines | 40,342 / 40,457 | 99.7157% | 99.4% |
| Changed executable production lines | 0 / 0 | 100.0000% | 95% with small-patch floor |

Shared LCOV accounting reports 115 uncovered lines across 10 production files. The baseline metric
universe contains 58,153 production source lines after excluding `test_support.rs`, 1,170 Rust test
attributes, 20.12 test attributes/KLOC, and 17 `cfg(not(coverage))` occurrences.

## Deterministic regression coverage

`make gate-tests` runs 149 offline tests. New regressions cover conservative changed-scope fallback,
partial staging/reset and intent-to-add, hidden coverage inputs, owner-execute semantics, LLVM build
directory isolation, real linked-worktree cache collisions, generated LCOV exclusion, LF/LH gaps
without zero-hit rows, duplicate-record merging, cross-command artifact invalidation, baseline and
report symlink/atomicity failures, CI least privilege, lexical gate-log validation, and every
`quality-status` exit-classification/tampering case.

Repeated final coverage validation also exposed an existing 40ms process-startup assumption in a
`crab-factory` timeout diagnostic test. The test now uses a bounded one-second timeout, preserving
the same timeout behavior and separator-branch coverage while remaining reliable under LLVM
instrumentation; production supervisor behavior is unchanged.

The workspace contains 1,168 discovered tests under `cargo test --workspace --all-features --locked
-- --list`. Attribute count and discovered-test count intentionally differ because generated or
parameterized test surfaces do not map one-to-one to source attributes.

## Baseline and measurement runs

The real hardened baseline capture succeeded and atomically published one deduplicated history
entry. It measured a cold normal test run at 677s and a warm isolated coverage gate at 282s. Those
figures are environment/cache evidence, not a claimed structural speedup.

After implementation, threshold propagation, documentation, and baseline capture—but before the two
tracked reports were published—the dedicated schema-3 measurement `make quality` passed in 782.96s
wall time with matching 29-entry fingerprints, null setup error, seven successes, and zero skips:

| Gate | Seconds |
|---|---:|
| fmt | 2.163 |
| clippy | 229.394 |
| tests | 98.158 |
| public-api | 19.013 |
| duplication | 15.865 |
| gate-tests (149 tests) | 136.570 |
| coverage | 280.753 |

Earlier checkpoint evidence is retained only as historical context: the pre-remediation report
recorded 2,896/2,896 functions, 58,512/59,077 regions (99.0436%), and 40,643/40,758 lines
(99.7178%) before the production-file exclusion. Those figures are not used for the new threshold
or final claims.

## Final attestation sequence

The measurement status is intentionally made stale when this tracked report and
`CODE_QUALITY_REPORT.md` are published. Final proof therefore follows report publication in this
order: `make doctor`, `make gate-tests`, `make quality`, then `make quality-status`, with no tracked
edit afterward. The resulting ignored schema-3 `quality/status.json` is the authoritative proof of
matching fingerprints and zero required skips; coverage totals are compared read-only with the
measurement table above.
