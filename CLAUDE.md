# CLAUDE.md

Concise agent guide for this repository. See AGENTS.md for complete project rules.

## Quick Start
- Build: `cargo build --workspace`
- Fast validation: `make quick` (~13s)
- Full quality gate: `make quality` (~20s)
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

## Hotspot Files
- `crates/crab-app/src/turn_executor.rs` (7913 lines): Turn lifecycle, context building, backend dispatch, streaming, rotation
- `crates/crab-app/src/daemon.rs` (4968 lines): Main daemon loop, session management, lane orchestration
- `crates/crab-store/src/lib.rs` (4053 lines): All persistent store implementations
- `crates/crab-discord-connector/src/main.rs` (2527 lines): Discord gateway bridge

## If You Touch X, Run Y
| Touch area | Run |
| --- | --- |
| Any `.rs` file | `make quick` |
| Coverage-sensitive code (`src/` files in crates) | `make coverage-gate` |
| CI workflow (`.github/workflows/`) | Review only; no local gate required |
| `Makefile` or `scripts/` | `make quality` |
| `AGENTS.md` or `docs/` | No gate; keep docs synced with behavior |

## Quality Gates
- `100%` function coverage and `99%` region coverage
- PR patch coverage: every changed production line must be covered
- Zero duplication on production code
- No dead code, no unused imports, and every `pub fn` must have cross-file usage
- Warnings denied; `clippy` all denied

## Common Agent Tasks
- New feature: implement in the appropriate crate, add tests, run `make quality`
- Bug fix: add a regression test first, fix the bug, run `make coverage-gate`
- New CLI binary: add it under `crates/crab-app/src/bin/` and wire it in `Cargo.toml`

## Key Conventions
- No `TODO` or `FIXME` without a linked GitHub issue
- No mocks except at true external boundaries
- `cfg(not(coverage))` only for `tracing` macros; see AGENTS.md section 3
- `#[rustfmt::skip]` only for multi-line calls that create false coverage gaps
- Prefer integration tests; use unit tests where isolation is useful
- Keep `Cargo.lock` committed
