# Deployment Readiness Gaps

## Scope

This document tracks unresolved runtime and deployment gaps before Crab is deployed on
the target machine.

## Status Snapshot (February 10, 2026)

Recently closed:

- Runtime policy config surface is now implemented in `RuntimeConfig`.
- Session token accounting is now aggregated from normalized backend usage payloads at
  turn finalization.
- Turn executor finalization now invokes
  `evaluate_rotation_triggers` + `execute_rotation_sequence`.
- Owner-only manual rotation commands are implemented:
  - `/compact confirm`
  - `/reset confirm`
- Rotation lifecycle/audit run-note events are persisted for automatic and manual rotation.

Runtime policy env keys now supported:

- `CRAB_COMPACTION_TOKEN_THRESHOLD` (default `80000`)
- `CRAB_INACTIVITY_TIMEOUT_SECS` (default `1800`)
- `CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS` (default `90`)
- `CRAB_HEARTBEAT_INTERVAL_SECS` (default `10`)
- `CRAB_RUN_STALL_TIMEOUT_SECS` (default `90`)
- `CRAB_BACKEND_STALL_TIMEOUT_SECS` (default `30`)
- `CRAB_DISPATCHER_STALL_TIMEOUT_SECS` (default `20`)

## Closed Gaps

### Closed Gap A: Rotation Trigger Integration In Turn Finalization

- Closed on February 10, 2026 in `main` commit `583ec53`.
- `TurnExecutor` now evaluates rotation triggers and executes rotation sequence in
  post-run finalization.

### Closed Gap B: Manual Compact/Reset Operator Surface

- Closed on February 10, 2026 in `main` commit `583ec53`.
- Owner-only `/compact confirm` and `/reset confirm` are now parsed, authorized,
  audited, and wired through rotation execution.

## Gap 1: Startup Reconciliation + Heartbeat Runtime Loop Wiring

Current status:

- `execute_startup_reconciliation` and `execute_heartbeat_cycle` primitives exist and are tested.
- production runtime loop does not yet call them on deterministic schedule.

Impact:

- crash recovery and stall handling contracts are implemented in isolation but not yet
  executed by a long-running runtime process.

Required work:

- invoke reconciliation on startup and heartbeat cycles on configured interval without
  duplicate user-visible delivery.

## Gap 2: Full Bot Runtime Entrypoint

Current status:

- `crab-app` exposes composition/orchestration library and memory CLIs.
- no production Discord runtime binary is present yet.

Impact:

- deployment cannot run a single managed service process.

Required work:

- add runtime executable wiring startup, ingress loop, scheduler dispatch, heartbeat loop,
  reconciliation, and graceful shutdown.

## Recommended Closure Order

1. startup reconciliation + heartbeat schedule integration
2. production runtime binary and deployment playbook

## Exit Criteria For "Deployment Ready"

- first-run onboarding path works end-to-end on cold workspace
- normal run path plus replay survives process restart without duplicate Discord output
- compaction triggers execute deterministic flush/checkpoint rotation
- heartbeat handles stalled run/backend/dispatcher cases with expected escalation
- documentation and runbook match actual runtime behavior
