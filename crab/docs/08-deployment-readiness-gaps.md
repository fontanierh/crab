# Deployment Readiness Gaps

## Scope

This document tracks unresolved runtime and deployment gaps before Crab is deployed on
the target machine.

## Status Snapshot (February 10, 2026)

Implemented and validated in repository code/tests:

- Runtime policy config surface in `RuntimeConfig` (rotation, reconciliation, heartbeat, owner profile defaults).
- Session token accounting aggregation from normalized backend usage payloads at turn finalization.
- Rotation trigger execution in finalization (`evaluate_rotation_triggers` -> `execute_rotation_sequence`).
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
  - Discord Gateway ingress -> `GatewayMessage` JSONL into `crabd` stdin
  - `crabd` outbound `post|edit` JSONL handling via Discord REST
  - retry/rate-limit handling on Discord REST delivery paths
  - persisted synthetic->actual message id mapping for edit continuity
- Discord provisioning/secrets runbook exists: `crab/docs/09-discord-provisioning-and-secrets.md`.
- Target-machine operations runbook exists: `crab/docs/10-target-machine-operations.md`.

Important runtime shape today:

- `crabd` and connector are separate processes with a JSONL IPC boundary.
- Connector ingress writes raw `GatewayMessage` JSON lines to `crabd` stdin.
- `crabd` outbound lines are JSON payloads with `op: post|edit`.

## Remaining Gaps

### Gap 1: Connector <-> `crabd` Delivery-Receipt Contract

Current status:

- Connector is implemented and can post/edit Discord messages from `crabd` outbound ops.
- Current IPC contract is one-way for delivery operations (`crabd` writes op, connector executes).
- `crabd` does not currently receive a delivery receipt/ack with final Discord message id.

Impact:

- Delivery failure after connector retries can still desynchronize `crabd` persistence from
  real Discord state in edge cases.
- Full replay/idempotency guarantees across connector failure boundaries are weaker than
  the desired long-term contract.

Required work:

- Extend IPC protocol with explicit delivery receipts:
  - include operation request id in outbound op frame
  - connector returns success/failure receipt with actual Discord message id for `post`
  - `crabd` persists outbound records only after positive receipt
- Add crash/restart tests covering partial delivery, retry, and replay without duplication.

Exit evidence:

- Replay tests prove no lost/duplicate delivery when connector fails mid-delivery.

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

- [ ] Cold workspace first boot creates/repairs required files (`AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, `CLAUDE.md -> AGENTS.md`, `memory/` layout).
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

1. Close delivery-receipt contract gap between connector and `crabd` (Gap 1).
2. Execute acceptance checklist on target machine and capture evidence (Gap 2).

## Exit Criteria For "Deployment Ready"

- First-run onboarding path works end-to-end on cold workspace.
- Normal run path plus replay survives process restart without duplicate Discord output.
- Compaction triggers execute deterministic flush/checkpoint rotation.
- Heartbeat handles stalled run/backend/dispatcher cases with expected escalation.
- Documentation and runbooks match actual runtime behavior.
- Deployment acceptance checklist is fully executed with recorded evidence.
