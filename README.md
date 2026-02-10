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
- `WS9` complete: cancellation semantics, startup reconciliation, heartbeat loops, operator commands, and structured diagnostics.
- `WS10` complete: workspace bootstrap templates, lifecycle ensure, bootstrap state detection, safe memory scope layout, and app startup integration diagnostics.
- `WS11` complete: owner trust mapping, sender identity resolution, owner-aware run metadata, owner-only operator gates, and privacy safeguards for per-user memory scope isolation.
- `WS14` complete: deterministic memory recall core (`memory_search`/`memory_get`), CLI exposure (`crab-memory-search`/`crab-memory-get`), citation/disclosure policy, and flush/recall continuity tests are complete.
- `WS15` complete: composition-root wiring, turn executor pipeline, event envelope parity, delivery/replay integration, and deployment readiness integration tests are complete.
- `WS16` complete: comprehensive architecture handbook covering onboarding, sessions, rotation/checkpointing, memory/context, reliability/recovery, backend contracts, state model, and deployment-gap tracking.
- `WS17` in progress:
  - `WS17-T1` complete: runtime config surface for rotation/heartbeat policy.
  - `WS17-T2` complete: token accounting propagation from normalized usage payloads.
  - `WS17-T3` complete: post-run rotation trigger wiring + owner-only `/compact confirm` and `/reset confirm`.
  - `WS17-T4` and `WS17-T5` pending.

## Docs

- Design: `crab/DESIGN.md`
- Workstreams: `crab/WORKSTREAMS.md`
- Architecture handbook: `crab/docs/README.md`
- Initial turn/onboarding: `crab/docs/01-initial-turn-and-onboarding.md`
- Sessions/lanes/turn flow: `crab/docs/02-sessions-lanes-and-turn-lifecycle.md`
- Rotation/checkpoint/compaction: `crab/docs/03-rotation-checkpoint-and-compaction.md`
- Memory/context model: `crab/docs/04-memory-model-and-context.md`
- Reliability/recovery/delivery: `crab/docs/05-reliability-delivery-and-recovery.md`
- Backends/inference profiles: `crab/docs/06-backend-contract-and-inference-profiles.md`
- Storage/state model: `crab/docs/07-storage-and-state-model.md`
- Deployment readiness gaps: `crab/docs/08-deployment-readiness-gaps.md`
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

## Memory CLI Commands

Run memory recall commands with Cargo:

```bash
cargo run -p crab-app --bin crab-memory-search -- \
  --workspace-root ~/.crab/workspace \
  --user-scope 1234567890 \
  --query "owner timezone"
```

```bash
cargo run -p crab-app --bin crab-memory-get -- \
  --workspace-root ~/.crab/workspace \
  --user-scope 1234567890 \
  --path memory/users/1234567890/2026-02-10.md \
  --start-line 1 --end-line 40
```

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
