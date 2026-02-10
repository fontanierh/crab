# Crab

Crab is a Rust harness for running coding agents (Claude Code, Codex CLI, OpenCode) behind a Discord bot.

## Current Status (as of February 10, 2026)

- `WS0` complete: workspace/bootstrap + enforced quality gates.
- `WS1` complete: domain model plus session/event/checkpoint/outbound persistence stores.
- `WS2` complete: per-session FIFO lanes, global concurrency cap, lane state machine, and explicit queue-overflow rejection messaging.
- `WS3` complete: Discord ingress/routing, streaming delivery, idempotency policy, and message constraints.
- `WS4` complete: backend contract and Claude adapter.
- `WS5` complete: Codex adapter lifecycle/protocol/recovery/unattended policy.
- `WS6` complete: OpenCode adapter lifecycle/protocol/recovery.
- `WS7` complete: model/reasoning policy resolution, compatibility, fallback, mapping, and telemetry persistence.
- `WS8` complete: trigger evaluation, hidden memory flush/checkpoint turns, fallback checkpointing, and rotation sequence.
- `WS9` in progress: `WS9-T1` cancellation semantics, `WS9-T2` startup reconciliation, `WS9-T3` heartbeat loops, and `WS9-T4` operator commands complete.
- Next implementation target: `WS9-T5` (structured diagnostics).

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
