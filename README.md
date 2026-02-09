# Crab

Crab is a Rust harness for running coding agents (Claude Code, Codex CLI, OpenCode) behind a Discord bot.

## Current Status (as of February 9, 2026)

- `WS0` complete: workspace/bootstrap + enforced quality gates.
- `WS1` complete: domain model plus session/event/checkpoint/outbound persistence stores.
- `WS2` complete: per-session FIFO lanes, global concurrency cap, lane state machine, and explicit queue-overflow rejection messaging.
- Next implementation target: `WS3` (Discord transport).

## Docs

- Design: `crab/DESIGN.md`
- Workstreams: `crab/WORKSTREAMS.md`
- Project rules: `AGENTS.md`

## Quality Gates

Run all checks:

```bash
make quality
```

Individual checks:

- `make fmt-check`
- `make clippy`
- `make deadcode-check`
- `make test`
- `make coverage-gate`
- `make duplication-check`

## Prerequisites

- Rust stable toolchain (from `rust-toolchain.toml`)
- `cargo-llvm-cov`:

```bash
cargo install cargo-llvm-cov --version 0.6.21 --locked
```

- LLVM tools component:

```bash
rustup component add llvm-tools-preview
```

- Node runtime (for `npx jscpd`)
