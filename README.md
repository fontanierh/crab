# Crab

Crab is a Rust harness for running coding agents (Claude Code, Codex CLI, OpenCode) behind a Discord bot.

## Current Status (as of February 12, 2026)

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
- `WS17` complete: runtime policy config, token-accounting propagation, rotation trigger wiring, owner manual compact/reset commands, startup reconciliation + deterministic heartbeat scheduling integration, and deployment-gap validation/docs sync.
  - Note: token-threshold compaction is evaluated against session token usage since the last successful rotation (token accounting is reset on rotation). Default compaction threshold is `120000`.
- `WS18` in progress:
  - `WS18-T1` complete: Discord runtime adapter boundary for ingress + outbound send/edit with deterministic retry/rate-limit handling.
  - `WS18-T2` complete: `crabd` daemon runtime binary is implemented and tested (startup, daemon loop, heartbeat/reconciliation wiring, graceful shutdown, stdio JSONL transport).
  - Backend runtime wiring for Codex/OpenCode/Claude execution plus hidden checkpoint-turn execution is integrated in daemon runtime flow.
  - Discord connector runtime is implemented: `crab-discord-connector` bridges Discord Gateway/REST <-> `crabd` JSONL.
  - `WS18-T3` complete: Discord provisioning + secret operations runbook is documented.
  - `WS18-T4` complete: target-machine service + operations playbook is documented.
  - `WS18-T5` in progress: checklist and evidence docs are complete; target-machine execution evidence + final GO/NO-GO recording remain.
- `WS19` complete: cross-platform installer (`crabctl`) supports `install`, `upgrade`, `rollback`, and `doctor` for macOS/Linux, including prerequisite bootstrap, runtime layout/service provisioning, and idempotent rerun safety.
- `WS20` complete: canonical skills root bootstrap (`.agents/skills`), Claude compatibility symlink enforcement (`.claude/skills -> ../.agents/skills`), built-in skill-authoring policy file, prompt-contract governance section, and startup diagnostics coverage.
- `WS21` complete:
  - `WS21-T1` complete: workspace git persistence config model (`enabled`, `remote`, `branch`, commit identity, push policy) with eager validation for malformed/unsafe values.
  - `WS21-T2` complete: startup-time workspace git repository bootstrap/binding is implemented (`crab-core::ensure_workspace_git_repository` + `crab-app::initialize_runtime_startup`) with safe external-repo guardrails, deterministic branch bootstrap on empty repos, and remote-origin binding validation.
  - `WS21-T3` complete: deterministic workspace commit triggers are implemented via `crab-core::maybe_commit_workspace_snapshot` with standardized commit trailers (`Crab-Trigger`, `Crab-Logical-Session-Id`, `Crab-Run-Id`, `Crab-Checkpoint-Id`, `Crab-Run-Status`, `Crab-Commit-Key`) and replay-safe duplicate-key handling for restart scenarios.
  - `WS21-T4` complete: async push queue is implemented with durable retry state (`state/workspace_git_push_queue.json`), bounded exponential backoff, idempotent enqueue by commit key, daemon-loop non-blocking processing, and restart-safe recovery semantics.
  - `WS21-T5` complete: secret-safe staging guardrails are enforced before automated commits (denylist + allowlist override policy), with commit-time staging audit metadata and runtime skipped-path diagnostics.
  - `WS21-T6` complete: divergence/conflict push failures are classified deterministically (`non_fast_forward`/`diverged_history`), escalated to `manual_recovery_required`, and exposed with operator recovery commands.
- `WS22` complete for current scoped deliverables:
  - `WS22-T1` complete: global state schema marker + startup migrator pipeline (`state/schema_version.json`, migration locking, idempotent reruns, startup migration telemetry events).
  - `WS22-T2` complete: actionable compatibility preflight is wired into `crabctl upgrade` and `crabctl doctor`, including explicit remediation commands and distinct blocked-upgrade exit code (`3`).
  - `WS22-T3` complete: migration fixture coverage exists for initial `v0 -> v1`, no-op rerun, stale-lock recovery, active-lock blocking, and corrupt-marker handling.
  - `WS22-T4` complete: schema evolution governance is documented in contributor policy/docs.
  - `WS22-T5` complete as design scope: snapshot/restore command-path design note is documented and command implementation remains intentionally deferred.
- `WS23` complete:
  - `WS23-T1` complete: runtime no longer inline-injects `AGENTS.md`; prompt contract is injected as its own context section.
  - `WS23-T2` complete: bootstrap context is injected once per physical session; reused sessions receive raw user input only.
  - `WS23-T3` complete: char-truncation path removed; managed docs now use strict token budgets with explicit overflow errors.
  - `WS23-T4` complete: OpenCode uses native reasoning mapping without synthetic prompt guidance suffix.
  - `WS23-T5` complete: docs and tests are aligned with the new context lifecycle semantics.
- `WS24` complete:
  - `WS24-T1` complete: pending-bootstrap gate enforces owner-DM-only execution until onboarding completes.
  - `WS24-T2` complete: rotation-time hidden onboarding extraction can complete onboarding from conversational context.
  - `WS24-T3` complete: initial physical-session context includes `CRAB_RUNTIME_BRIEF` with onboarding guidance when applicable, and bootstrap injection logs context size (`injected_context_tokens`, `injected_context_chars`) for debugging.

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
- Discord provisioning/secrets: `crab/docs/09-discord-provisioning-and-secrets.md`
- Target machine operations: `crab/docs/10-target-machine-operations.md`
- Feature integration matrix: `crab/docs/11-feature-integration-matrix.md`
- Snapshot/restore design note (deferred hardening): `crab/docs/12-snapshot-restore-design-note.md`
- Deployment acceptance evidence worksheet: `crab/docs/13-deployment-acceptance-evidence.md`
- Dated end-to-end runtime chat flow (as of 2026-02-12): `crab/docs/14-overall-chat-flow-2026-02-12.md`
- Project rules: `AGENTS.md`

## Quality Gates

Run all checks:

```bash
make quality
```

Fast local preflight (non-gating):

```bash
make quick
```

Individual checks:

- `make fmt-check`
- `make clippy`
- `make deadcode-check`
- `make public-api-check`
- `make coverage-gate`
- `make coverage-diagnostics`
- `make duplication-check`

Coverage note:
- `make coverage-gate` currently applies `--ignore-filename-regex 'crates/crab-app/src/installer.rs'`
  due to a reproducible `cargo-llvm-cov` line-mapping false negative in that file.
- If `make coverage-gate` fails, run `make coverage-diagnostics` to generate actionable uncovered
  line diagnostics under `coverage/uncovered_locations.txt`.

Duplication note:
- `make duplication-check` runs `jscpd` against crate Rust sources with explicit ignore rules
  (for example `src/test_support.rs`) so merge blocking stays focused on production code.

Baseline/trend note:
- Capture runtime and density baselines with:
  `make quality-baseline`
- Regenerate `CODE_QUALITY_REPORT.md` (includes trend sections when baselines exist):
  `make quality-report`

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

## Installer Commands (`crabctl`)

Build installer binary:

```bash
cargo build -p crab-app --bin crabctl
```

Dry-run install plan:

```bash
./target/debug/crabctl install --target macos --dry-run
```

Upgrade:

```bash
./target/debug/crabctl upgrade --target macos --release-id <release-id>
```

Rollback and diagnostics:

```bash
./target/debug/crabctl rollback --target macos
./target/debug/crabctl doctor --target macos
```

## Runtime Launch

Build binaries:

```bash
cargo build -p crab-app -p crab-discord-connector
```

Run connector + daemon (requires Discord env vars such as `CRAB_DISCORD_TOKEN` and `CRAB_BOT_USER_ID`):

```bash
cargo run -p crab-discord-connector -- --crabd ./target/debug/crabd
```
