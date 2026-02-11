# Deployment Readiness Gaps

## Scope

This document tracks unresolved runtime and deployment gaps before Crab is deployed on
the target machine.

## Status Snapshot (February 11, 2026)

Implemented and validated in repository code/tests:

- Runtime policy config surface in `RuntimeConfig` (rotation, reconciliation, heartbeat, owner profile defaults).
- Workspace git persistence config model in `RuntimeConfig` (`enabled`, `remote`, `branch`, commit identity, push policy) with eager validation.
- Session token accounting aggregation from normalized backend usage payloads at turn finalization.
- Rotation trigger execution in finalization (`evaluate_rotation_triggers` -> `execute_rotation_sequence`).
- Token-threshold compaction is evaluated against session token usage since the last successful
  rotation (token accounting is reset on rotation). Default threshold is `120000`.
- Owner-only manual rotation commands:
  - `/compact confirm`
  - `/reset confirm`
- Startup reconciliation and deterministic heartbeat scheduling:
  - `boot_runtime_with_processes*`
  - `run_startup_reconciliation_on_boot`
  - `run_heartbeat_if_due`
- Production daemon binary exists: `crabd` (`crates/crab-app/src/bin/crabd.rs`) with:
  - startup boot + reconciliation
  - backend lifecycle ensure (Codex/OpenCode managers)
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
- Record evidence links (logs/screenshots/notes) and final go/no-go decision.

## Deployment Acceptance Checklist (WS18-T5)

Run all checks on the target machine using production-like config:

- [ ] Cold workspace first boot creates/repairs required files and links (`AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, `CLAUDE.md -> AGENTS.md`, `.agents/skills`, `.claude/skills -> ../.agents/skills`, built-in skill policy file, `memory/` layout).
- [ ] First owner interaction runs onboarding prompts and persists owner/agent identity context.
- [ ] Normal owner run processes end-to-end (`ingress -> lane -> backend -> Discord delivery`) with persisted run/event metadata.
- [ ] Non-owner run obeys per-user memory scope and disclosure policy.
- [ ] Restart during/after a run replays missing outbound delivery without duplicate messages/edits.
- [ ] Token-threshold compaction trigger executes hidden memory flush + checkpoint + session rotation.
- [ ] Inactivity trigger executes rotation behavior after configured timeout.
- [ ] Manual `/compact confirm` and `/reset confirm` commands execute only for owner and are audited.
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
- Compaction triggers execute deterministic flush/checkpoint rotation.
- Heartbeat handles stalled run/backend/dispatcher cases with expected escalation.
- Documentation and runbooks match actual runtime behavior.
- Deployment acceptance checklist is fully executed with recorded evidence.
