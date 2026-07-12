# CLAUDE.md

Concise agent guide for this repository. See AGENTS.md for complete project rules.

## Quick Start
- Preflight: `make doctor` (read-only)
- Build: `cargo build --workspace`
- Fast changed-scope validation: `make check`
- Full quality gate: `make quality`
- Tests only: `make test`

## Repo Map
- `crab-core`: Domain types, context assembly, memory, onboarding, validation, config
- `crab-app`: Application layer; daemon, turn executor, CLI binaries (`crabd`, `crab-trigger`, `crab-rotate`, `crab-memory-search`, `crab-memory-get`, `crab-self-work`)
- `crab-backends`: Backend adapter trait plus Claude Code implementation
- `crab-discord`: Discord protocol types, streaming delivery, gateway ingress, idempotent ledger
- `crab-discord-connector`: Discord bot binary; gateway bridge, message routing, delivery
- `crab-store`: Persistent storage; sessions, runs, events, checkpoints, outbound records
- `crab-scheduler`: Lane-based FIFO scheduler with global concurrency cap
- `crab-telemetry`: Structured logging setup
- `crab-factory`: Repository developer tooling; isolated worktree and multi-model change pipeline

## Hotspot Files
- `crates/crab-app/src/turn_executor.rs` (7913 lines): Turn lifecycle, context building, backend dispatch, streaming, rotation
- `crates/crab-app/src/daemon.rs` (4968 lines): Main daemon loop, session management, lane orchestration
- `crates/crab-store/src/lib.rs` (4053 lines): All persistent store implementations
- `crates/crab-discord-connector/src/main.rs` (2527 lines): Discord gateway bridge

## If You Touch X, Run Y
| Touch area | Run |
| --- | --- |
| Any `.rs` file | `make check` |
| Coverage-sensitive code (`src/` files in crates) | `make coverage-quick` while editing, then `make quality` |
| CI workflow (`.github/workflows/`) | `make gate-tests`, then `make quality` |
| `Makefile` or `scripts/` | `make quality` |
| `AGENTS.md` or `docs/` | `make check`; keep docs synced with behavior |

## Quality Gates
- `99.5%` function, `98.93%` region, and `99.4%` line coverage
- Patch coverage: 95% of changed executable lines, with 100% required below 20 lines
- Zero duplication on production code
- No dead code, no unused imports, and every `pub fn` must have cross-file usage
- Rust warnings and Clippy correctness/suspicious/perf denied; style/complexity warned

## Common Agent Tasks
- New feature: implement in the appropriate crate, add tests, run `make quality`
- Bug fix: add a regression test first, fix the bug, run `make coverage-gate`
- New runtime CLI binary: add it under `crates/crab-app/src/bin/` and wire it in `Cargo.toml`
- Repository developer tooling: keep it in `crab-factory`, separate from the live runtime

## Key Conventions
- No `TODO` or `FIXME` without a linked GitHub issue
- No mocks except at true external boundaries
- `cfg(not(coverage))` only for `tracing` macros; see AGENTS.md section 3
- `#[rustfmt::skip]` only for multi-line calls that create false coverage gaps
- Prefer integration tests; use unit tests where isolation is useful
- Keep `Cargo.lock` committed
