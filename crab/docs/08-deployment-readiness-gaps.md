# Deployment Readiness Gaps

## Scope

This document tracks unresolved runtime and deployment gaps before Crab is deployed on
the target machine.

## Status Snapshot (February 12, 2026)

Implemented and validated in repository code/tests:

- Runtime policy config surface in `RuntimeConfig` (rotation, reconciliation, heartbeat, owner profile defaults).
- Workspace git persistence config model in `RuntimeConfig` (`enabled`, `remote`, `branch`, commit identity, push policy) with eager validation.
- Workspace git bootstrap/binding on startup when enabled:
  - initializes workspace-local repository only at `CRAB_WORKSPACE_ROOT`
  - rejects nested/external repository mutation
  - enforces deterministic branch bootstrap on empty repositories
  - validates/binds `origin` against configured private remote
- Workspace git commit trigger/metadata policy:
  - successful run finalization attempts deterministic workspace commits
  - rotation checkpoints are committed with a dedicated trigger metadata path
  - commit trailers carry run/session/checkpoint correlation fields and replay-safe `Crab-Commit-Key`
  - commit staging policy enforces secret/transient guardrails before automated commits and records
    `Crab-Staging-Policy-Version`, `Crab-Staging-Skipped-Count`, and `Crab-Staging-Skipped-Rules`
  - skipped staging paths are emitted to runtime diagnostics for operator audit visibility
- Workspace git divergence/conflict recovery policy:
  - non-fast-forward/diverged push failures are classified as manual-recovery-required
  - queue entries are exhausted immediately for manual-recovery classes (no endless retries)
  - outcomes include `failure_kind` and deterministic recovery command guidance
- Agent-driven rotation via `crab-rotate` CLI:
  - agent produces checkpoint JSON and invokes `crab-rotate`
  - runtime picks up pending rotation signal after the current turn completes
  - checkpoint is persisted, physical session is ended, session handle is cleared
  - `rotate-session` skill guides the agent through the rotation protocol
  - no hidden turns, no automatic triggers, no operator rotation commands
- Startup reconciliation and deterministic heartbeat scheduling:
  - `boot_runtime_with_processes*`
  - `run_startup_reconciliation_on_boot`
  - `run_heartbeat_if_due`
- Runtime turn-context assembly is wired end-to-end in daemon execution:
  - prompt contract compilation per run profile/surface
  - bootstrap workspace document injection (`SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, `CRAB_RUNTIME_BRIEF`, `PROMPT_CONTRACT`)
  - reused physical sessions receive raw turn input only
  - scoped memory snippet injection + token-budget validation + diagnostics
  - latest checkpoint summary injection from persistent store
  - bootstrap context size observability logging (`injected_context_tokens`, `injected_context_chars`)
- Production daemon binary exists: `crabd` (`crates/crab-app/src/bin/crabd.rs`) with:
  - startup boot + reconciliation
  - backend lifecycle ensure
  - ingress poll + lane dispatch loop
  - heartbeat execution
  - graceful shutdown control
- First-party Discord connector process now exists: `crab-discord-connector`
  (`crates/crab-discord-connector/src/main.rs`) and performs:
  - Discord Gateway ingress -> `CrabdInboundFrame::GatewayMessage` JSONL into `crabd` stdin
  - `crabd` outbound `CrabdOutboundOp` (`op=post|edit`) JSONL handling via Discord REST
  - explicit `CrabdOutboundReceipt` JSONL receipts back into `crabd` stdin for every outbound op
  - retry/rate-limit handling on Discord REST delivery paths
  - persisted deterministic `delivery_id` -> actual Discord message id mapping for edit continuity
- Discord provisioning/secrets runbook exists: `crab/docs/09-discord-provisioning-and-secrets.md`.
- Target-machine operations runbook exists: `crab/docs/10-target-machine-operations.md`.
- Cross-platform installer baseline is implemented (`crabctl`):
  - `install`/`upgrade`/`rollback`/`doctor` command surface for `macos` and `linux`
  - prerequisite bootstrap + version verification path
  - idempotent runtime layout/service provisioning with deterministic dry-run plans
- Runtime state schema evolution safety baseline:
  - global state marker at `workspace/state/schema_version.json`
  - startup migration engine (stepwise, idempotent, lock-protected)
  - `crabctl upgrade`/`doctor` compatibility preflight with actionable remediation output
  - distinct blocked-upgrade exit code (`3`) for incompatible state versions
- First-interaction onboarding completion capture path is runtime-wired:
  - while bootstrap is pending, non-owner and owner non-DM messages are blocked by runtime gate
  - owner DM receives runtime onboarding guidance to gather required fields naturally
  - owner-submitted strict onboarding JSON capture is validated
  - `SOUL.md`/`IDENTITY.md`/`USER.md` managed sections are updated and conflicts are surfaced
  - onboarding completion protocol writes managed `MEMORY.md` baseline and retires `BOOTSTRAP.md`
  - rotation can run hidden extraction to complete onboarding when strict capture is derivable
- OpenCode session recovery used a shared backend helper (historical; OpenCode adapter removed in PR #164):
  - daemon OpenCode execution bridge routed materialization/recovery through
    `crab_backends::recover_opencode_session`
  - recoverable session faults rotated sessions through a single recovery primitive with
    deterministic bookkeeping
- Quality engineering ergonomics are standardized:
  - strict gate: `make quality`
  - fast local preflight: `make quick`
  - coverage diagnostics helper (`lines`): `make coverage-diagnostics`
  - baseline/trend capture helpers: `make quality-baseline` + `make quality-report`

Important runtime shape today:

- `crabd` and connector are separate processes with a JSONL IPC boundary.
- Connector ingress writes `CrabdInboundFrame` (`kind=gateway_message`) JSON lines to `crabd` stdin.
- Connector emits `CrabdInboundFrame` (`kind=outbound_receipt`) JSON receipts back to `crabd` for every outbound operation.
- `crabd` outbound lines are `CrabdOutboundOp` JSON payloads with `op: post|edit`, each including:
  - `op_id` for request/receipt correlation
  - deterministic `delivery_id` for idempotent post/edit mapping across restarts

## Remaining Gaps

### Gap 1: Connector <-> `crabd` Delivery-Receipt Contract (CLOSED)

Closed (February 10, 2026):

- Connector and `crabd` exchange an explicit receipt for every outbound delivery operation.
- Connector is idempotent on `post` when a mapping already exists for the `(channel_id, delivery_id)` pair.
- `crabd` considers Discord output delivered only after a positive receipt is received.

Impact:

This closes the reliability gap where connector/process failure could desynchronize `crabd` outbound persistence
from actual Discord delivery.

Exit evidence:

- Receipt and timeout behavior is covered in `crates/crab-app/src/bin/crabd.rs` tests.
- End-to-end delivery wiring is covered in `crates/crab-app/src/daemon.rs` tests.
- Connector idempotency and error receipts are covered in `crates/crab-discord-connector/src/main.rs` tests.

### Gap 2: Deployment Acceptance Execution Evidence

Current status:

- Acceptance checklist is now defined below.
- Execution evidence on the target machine is still pending.

Impact:

- Readiness cannot be declared until the checklist is run and recorded against the real host.

Required work:

- Execute checklist on target machine.
- Record evidence links (logs/screenshots/notes) and final go/no-go decision in
  `crab/docs/13-deployment-acceptance-evidence.md`.

### Gap 2A: Real Backend Turn Execution + Hidden Checkpoint Backend Turn Wiring (CLOSED)

Current status:

- `DaemonTurnRuntime::execute_backend_turn` now delegates through a daemon backend bridge.
- Claude runs are wired through `DaemonClaudeExecutionBridge` in daemon runtime execution.
- Hidden checkpoint turns in rotation now execute through the runtime backend path with strict
  checkpoint schema parsing/validation and retry policy.
- Deterministic fallback checkpoint construction now applies only when backend checkpoint-turn
  execution/parsing fails.
- The same `TurnExecutorRuntime::execute_backend_turn` path is used for normal turns and hidden
  checkpoint turns, so checkpoint output now comes from the active real backend execution path.

Note: Codex CLI and OpenCode backend execution paths were removed (PRs #163, #164).

Impact:

- Runtime/backend integration gap is closed for Claude Code execution path.
- Hidden checkpoint output is generated through runtime backend execution with strict schema
  validation and deterministic fallback-on-failure behavior.

Exit evidence:

- Daemon runtime backend integration tests in `crates/crab-app/src/daemon.rs` cover:
  - Claude execution through daemon runtime execution
  - turn-context assembly and checkpoint-summary injection
- Rotation tests in `crates/crab-app/src/turn_executor.rs` cover:
  - hidden checkpoint backend turn path
  - strict checkpoint schema retries
  - fallback-on-failure checkpoint generation

## Deployment Acceptance Checklist (WS18-T5)

Run all checks on the target machine using production-like config:

- [ ] Cold workspace first boot creates/repairs required files and links (`AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, `CLAUDE.md -> AGENTS.md`, `.agents/skills`, `.claude/skills -> ../.agents/skills`, built-in skill policy file, `memory/` layout).
- [ ] First owner onboarding interaction persists owner/agent identity context through the onboarding capture contract.
- [ ] Normal owner run processes end-to-end (`ingress -> lane -> backend -> Discord delivery`) with persisted run/event metadata.
- [ ] Non-owner run obeys per-user memory scope and disclosure policy.
- [ ] Restart during/after a run replays missing outbound delivery without duplicate messages/edits.
- [ ] Agent-driven rotation via `crab-rotate` CLI executes checkpoint persistence + session rotation end-to-end.
- [ ] Heartbeat escalates correctly for stalled run/backend/dispatcher scenarios.
- [ ] Service restart/reboot persistence is validated by operations playbook steps (`crab/docs/10-target-machine-operations.md`).
- [ ] `make quality` passes on deployment commit.

Go/no-go rule:

- `GO` only if every checklist item passes and no unresolved `Critical`/`High` incidents remain.

## Residual Risks (After Gap Closure)

- External connector and `crabd` split-process architecture introduces an IPC boundary that must be monitored.
- Discord platform-side behavior changes (rate limits, gateway event policy) can affect runtime reliability and require connector updates.

## Recommended Closure Order

1. Execute acceptance checklist on target machine and capture evidence (Gap 2).

## Exit Criteria For "Deployment Ready"

- First-run onboarding path works end-to-end on cold workspace.
- Normal run path plus replay survives process restart without duplicate Discord output.
- Agent-driven rotation via `crab-rotate` CLI executes checkpoint persistence and session rotation.
- Heartbeat handles stalled run/backend/dispatcher cases with expected escalation.
- Documentation and runbooks match actual runtime behavior.
- Deployment acceptance checklist is fully executed with recorded evidence.
