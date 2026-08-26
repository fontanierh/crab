# Contributing to Crab

See `AGENTS.md` for the full project operating rules.

## Quick Start

```sh
git clone <repo-url>
cd crab-source
# Install prerequisites (see below)
make doctor
make check
make quality
```

`make quality` runs format, Clippy, and tests across Crab v1, Crab v2, and first-party v2 bridges.
All three grouped gates must pass before opening a PR.

## Prerequisites

- **Rust 1.93.0 and 1.97.1** via [rustup](https://rustup.rs/), including rustfmt and Clippy
- **Python 3.11+**
- **Node.js 20+** for first-party v2 bridge tests
- Optional diagnostics: **cargo-llvm-cov 0.6.21**, **jscpd 4.0.5**, and **ripgrep**

`make doctor` checks exact versions and prints remediation without installing anything.

## Quality Expectations

- **Meaningful tests.** Test behavior and regressions. Coverage is diagnostic and has no merge floor.
- **No dead code.** The codebase compiles with `#![deny(dead_code)]`. Remove unused items rather than suppressing the lint.
- **Clippy policy.** Rust warnings and correctness/suspicious/performance findings fail. Style and
  complexity suggestions remain visible warnings. Use `make clippy`, not ad hoc flags.
- **Pragmatic duplication.** `make duplication-check` reports substantial production clones, but
  findings do not block a change. Extract shared logic when it improves maintenance.

## PR Process

- Keep PRs small and atomic. One logical change per PR is strongly preferred.
- Format, Clippy, and tests must pass in CI.
- PR description must cover three things:
  1. **What** changed
  2. **Why** it was needed
  3. **How** it was validated (specific test names or manual steps)
- Avoid force-pushing after review has started; add fixup commits instead.
- The final `quality/status.json` must say `passed`, have matching fingerprints, and contain no
  skipped gate. Nothing tracked may change after that run without rerunning `make quality`.

See [docs/agent-workflow.md](docs/agent-workflow.md) for changed-scope selection, optional
diagnostics, structured status, and shared build artifacts.

## Testing Philosophy

- **Integration first.** Prefer tests that exercise real behavior end-to-end over unit tests that test internals in isolation.
- **Minimal mocking.** Only mock at true external boundaries (network, filesystem, time). Do not mock your own code.
- **Meaningful assertions.** Test observable behavior and outcomes, not implementation details.
- **Regression tests for bugs.** Every bug fix must include a test that would have caught the original defect. Reference the issue in the test doc comment.
