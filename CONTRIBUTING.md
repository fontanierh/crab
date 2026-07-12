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

`make quality` runs the full local gate in order: fmt, Clippy, normal tests, public-API wiring,
duplication, deterministic workflow gate tests, and coverage. All checks must pass before opening a
PR.

## Prerequisites

- **Rust 1.93.0** via [rustup](https://rustup.rs/), including rustfmt, Clippy, and LLVM tools
- **cargo-llvm-cov 0.6.21**: `cargo install cargo-llvm-cov --version 0.6.21 --locked`
- **Python 3.11+**, exact runnable **jscpd 4.0.5**, and **ripgrep**; **Node.js/npm** are
  needed only to install or change jscpd

`make doctor` checks exact versions and prints remediation without installing anything.

## Quality Expectations

- **Coverage gate.** `make quality` enforces 95% functions, regions, lines, and changed executable
  lines. Under 20 changed executable lines, patch coverage is 100%.
- **No dead code.** The codebase compiles with `#![deny(dead_code)]`. Remove unused items rather than suppressing the lint.
- **Clippy policy.** Rust warnings and correctness/suspicious/performance findings fail. Style and
  complexity suggestions remain visible warnings. Use `make clippy`, not ad hoc flags.
- **No production duplication.** jscpd blocks duplicated production Rust code; test-code duplication is reported informationally. Extract shared logic rather than copying it.

## PR Process

- Keep PRs small and atomic. One logical change per PR is strongly preferred.
- All seven CI gates must pass (fmt, Clippy, tests, public-API wiring, duplication, workflow gate
  tests, and coverage).
- PR description must cover three things:
  1. **What** changed
  2. **Why** it was needed
  3. **How** it was validated (specific test names or manual steps)
- Avoid force-pushing after review has started; add fixup commits instead.
- The final `quality/status.json` must say `passed`, have matching fingerprints, and contain no
  skipped gate. Nothing tracked may change after that run without rerunning `make quality`.

See [docs/agent-workflow.md](docs/agent-workflow.md) for changed-scope selection, coverage modes,
structured status, CI docs-only behavior, and opt-in shared build artifacts.

## Testing Philosophy

- **Integration first.** Prefer tests that exercise real behavior end-to-end over unit tests that test internals in isolation.
- **Minimal mocking.** Only mock at true external boundaries (network, filesystem, time). Do not mock your own code.
- **Meaningful assertions.** Test observable behavior and outcomes, not implementation details.
- **Regression tests for bugs.** Every bug fix must include a test that would have caught the original defect. Reference the issue in the test doc comment.
