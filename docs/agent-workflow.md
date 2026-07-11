# Agent Development Workflow

Crab owns a three-command edit/inspect/validate loop:

```text
make doctor  ->  make check  ->  make quality
preflight        edit loop       handoff attestation
```

Bare `make` prints read-only help. Repository workflow scripts use exit `0` for success, `1` for a
gate failure, and `2` for usage, environment, unresolved-baseline, or stale-attestation errors.
GNU Make preserves success versus failure, although Make itself may report a generic nonzero code.

## Preflight

`make doctor` is read-only. It does not install software, create cache directories, or invoke a
network. It checks Git, Python 3, the exact Rust 1.93.0 toolchain and required components,
`cargo-llvm-cov` 0.6.21, Node/npm, jscpd 4.0.5, ripgrep, checkout/worktree shape, and target-directory safety.
Every failure includes a concrete remediation command.

Rust is intentionally pinned to an exact release so a newly published style lint cannot break an
unrelated pull request. Update the pin roughly once per Rust minor in a deliberate PR that updates
`rust-toolchain.toml` and CI together and fixes newly surfaced lints in that same PR.

## Changed-scope edit loop

`make check` obtains changed paths with Git before running Cargo. `DRY_RUN=1 make check` prints the
scope and exact commands without executing them. The default `worktree` mode includes committed,
staged, unstaged, and untracked files relative to the merge base with `origin/main`. CI uses
`DIFF_MODE=committed BASE_SHA=<sha>`.

The selector handles docs-only and full-workspace triggers before Cargo metadata. For crate edits,
it reads `cargo metadata --no-deps --locked --offline` and follows `packages[].dependencies` to add
reverse dependents. Metadata failure records the reason and falls back to the full workspace so the
real compiler diagnostic can surface. Workspace manifests, `Cargo.lock`, the toolchain, Makefile,
CI, `.cargo/`, and workflow scripts trigger full scope. Unknown code paths also select full scope.

The loop performs one format check, one scoped Clippy traversal, and one scoped test traversal. It
does not repeat Clippy with a standalone all-target `cargo check`. `make quick` remains only as a
deprecated compatibility alias.

## Lint policy

Rust compiler warnings are fatal. Clippy `correctness`, `suspicious`, and `perf` findings are fatal;
`style` and `complexity` remain visible warnings. `Cargo.toml` documents the policy, every member
must set `[lints] workspace = true`, and `scripts/clippy_policy.py` supplies the final ordered flags.
The wrapper uses `--force-warn` for style/complexity because Cargo's manifest-level
`deny(warnings)` otherwise re-fatalizes those tool lint groups. `make gate-tests` compiles permanent
offline fixtures proving all three outcomes.

## Coverage workflows

`make coverage-quick` creates fresh coverage for changed packages, enforces worktree patch
coverage, and reports aggregate totals without blocking on them. With no changed Rust files it exits
successfully with an explicit skip. `make coverage-gate` always creates fresh full-workspace LCOV
and is authoritative:

| Measure | Floor |
|---|---:|
| Functions | 99.5% |
| Regions | 99.0% |
| Lines | 99.4% |
| Changed executable lines | 95% |

The patch allowance is `floor(0.05 × changed executable lines)`, so fewer than 20 executable lines
still require 100%. Duplicate LCOV source/line records are summed. A changed production file with
added non-comment code but no LCOV source record fails closed. Human diagnostics compact locations
into ranges; `coverage/patch-coverage.json` always contains every uncovered changed line.

Coverage includes Rust under `crates/*/src/`, excluding crate `tests/` trees and
`src/test_support.rs`. The three diff modes are deliberate:

- `worktree` (local default) compares merge base to the working tree and matches what coverage built.
- `staged` rejects any unstaged or untracked Rust before evaluating the index.
- `committed` rejects any Rust content that differs from HEAD and is used by CI/handoff automation.

All modes require a resolvable baseline. `--base-sha` wins; otherwise `origin/main` is used. A
missing baseline exits 2 with fetch guidance. The public `patch_coverage.py --lcov` option is only a
non-authoritative test/diagnostic injection; Make targets never reuse an old LCOV file.

Every `cargo llvm-cov` entry point overrides ambient and shared targets with
`target/llvm-cov-worktree`. LLVM coverage cleans and consumes profile state, so sharing that target
between concurrent worktrees is unsafe. `make coverage-diagnostics` also generates fresh data.

## Worktree-aware build reuse

Build, Clippy, and test commands may opt into cross-worktree Cargo artifacts:

```bash
CRAB_SHARED_TARGET_DIR=/absolute/external/cache make check
```

The variable is Crab-specific; ambient `CARGO_TARGET_DIR` is not interpreted as consent. At doctor
and command execution time, the base must be absolute, writable, outside all repository worktrees
and the Git common directory after symlink resolution. Crab appends a repository-identity namespace
so unrelated repositories cannot mix. Nothing is generated in a checkout. Cargo coordinates
concurrent writers, but top-level binaries can be overwritten by another worktree; gates consume
only each command's exit and never assume an artifact remains afterward. Coverage never shares it.

## Authoritative status

`make quality` runs format, full Clippy, full tests, public-API wiring, production duplication,
workflow tests, and fresh aggregate/patch coverage. Console output is one status/duration line per
check; complete output is retained under `quality/logs/`. Failures show a focused tail and exact
rerun command. `VERBOSE=1` streams the same complete log live.

Before running, the orchestrator removes stale `quality/status.json`. It hashes HEAD plus the sorted
path/content hashes of every tracked modification and every non-ignored untracked file at start and
end. The status is atomically replaced and records schema/timestamps, branch and SHAs, diff mode,
dirty state, tool versions, every check's status/reason/duration/exit/log/rerun fields, result, and
next actions. A tree change during validation makes the result `invalid` (exit 2). A required skip
can never produce `passed`. `make quality-status` verifies a prior pass still matches the current
tree.

## CI docs-only behavior

Both stable required job names (`fast` and `coverage`) run the Git-only docs classifier immediately
after checkout, before Rust, caches, Node, or tools. Markdown/documentation-only changes finish green
with an explicit job-summary skip. Unknown paths run all checks. Pull requests use their base SHA;
pushes use a valid non-zero event `before` SHA or `HEAD^`. Scope selection falls back to full on an
unresolved base, while authoritative patch coverage fails closed.
