# Contributing to Crab

See `AGENTS.md` for the full project operating rules.

## Quick Start

```sh
git clone <repo-url>
cd crab-source
# Install prerequisites (see below)
make quality
```

`make quality` runs the full local gate: fmt, clippy, tests with coverage, and duplication checks. All checks must pass before opening a PR.

## Prerequisites

- **Rust stable** via [rustup](https://rustup.rs/)
- **cargo-llvm-cov** 0.6.21: `cargo install cargo-llvm-cov --version 0.6.21`
- **llvm-tools-preview**: `rustup component add llvm-tools-preview`
- **Node.js** (for jscpd duplicate detection): `npm install -g jscpd`

## Quality Expectations

- **100% test coverage.** Every line and branch must be covered. Coverage is enforced by `make quality`; untested code will not merge.
- **No dead code.** The codebase compiles with `#![deny(dead_code)]`. Remove unused items rather than suppressing the lint.
- **Clippy clean.** Run with `--deny warnings`. Fix warnings; do not use `#[allow(...)]` without a clear justification in a comment.
- **No duplication.** jscpd enforces a duplication threshold. Extract shared logic rather than copying it.

## PR Process

- Keep PRs small and atomic. One logical change per PR is strongly preferred.
- All CI gates must pass (fmt, clippy, coverage, duplication).
- PR description must cover three things:
  1. **What** changed
  2. **Why** it was needed
  3. **How** it was validated (specific test names or manual steps)
- Avoid force-pushing after review has started; add fixup commits instead.

## Testing Philosophy

- **Integration first.** Prefer tests that exercise real behavior end-to-end over unit tests that test internals in isolation.
- **Minimal mocking.** Only mock at true external boundaries (network, filesystem, time). Do not mock your own code.
- **Meaningful assertions.** Test observable behavior and outcomes, not implementation details.
- **Regression tests for bugs.** Every bug fix must include a test that would have caught the original defect. Reference the issue in the test doc comment.
