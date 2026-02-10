# Deployment Readiness Gaps

## Scope

This document tracks unresolved runtime and deployment gaps before Crab is deployed on
the target machine.

## Status Snapshot (February 10, 2026)

Recently closed:

- Runtime policy config surface is now implemented in `RuntimeConfig`.
- Session token accounting is now aggregated from normalized backend usage payloads at
  turn finalization.

Runtime policy env keys now supported:

- `CRAB_COMPACTION_TOKEN_THRESHOLD` (default `80000`)
- `CRAB_INACTIVITY_TIMEOUT_SECS` (default `1800`)
- `CRAB_STARTUP_RECONCILIATION_GRACE_PERIOD_SECS` (default `90`)
- `CRAB_HEARTBEAT_INTERVAL_SECS` (default `10`)
- `CRAB_RUN_STALL_TIMEOUT_SECS` (default `90`)
- `CRAB_BACKEND_STALL_TIMEOUT_SECS` (default `30`)
- `CRAB_DISPATCHER_STALL_TIMEOUT_SECS` (default `20`)

## Gap 1: Rotation Trigger Integration In Turn Finalization

Current status:

- Rotation primitives exist in `crab-core`.
- Turn executor finalization still does not invoke
  `evaluate_rotation_triggers` + `execute_rotation_sequence`.

Impact:

- token/inactivity/manual rotation policy is not yet active end-to-end.

Required work:

- wire trigger evaluation and rotation sequence execution into post-run finalization.

## Gap 2: Manual Compact/Reset Operator Surface

Current status:

- owner-gated operator framework exists for profile/onboarding commands.
- explicit compact/reset commands are not yet part of operator parsing/app flow.

Impact:

- no explicit owner command path to force compaction/reset rotation.

Required work:

- add owner-only compact/reset commands and persist audit events.

## Gap 3: Startup Reconciliation + Heartbeat Runtime Loop Wiring

Current status:

- `execute_startup_reconciliation` and `execute_heartbeat_cycle` primitives exist and are tested.
- production runtime loop does not yet call them on deterministic schedule.

Impact:

- crash recovery and stall handling contracts are implemented in isolation but not yet
  executed by a long-running runtime process.

Required work:

- invoke reconciliation on startup and heartbeat cycles on configured interval without
  duplicate user-visible delivery.

## Gap 4: Full Bot Runtime Entrypoint

Current status:

- `crab-app` exposes composition/orchestration library and memory CLIs.
- no production Discord runtime binary is present yet.

Impact:

- deployment cannot run a single managed service process.

Required work:

- add runtime executable wiring startup, ingress loop, scheduler dispatch, heartbeat loop,
  reconciliation, and graceful shutdown.

## Recommended Closure Order

1. rotation trigger + sequence wiring
2. manual compact/reset commands
3. startup reconciliation + heartbeat schedule integration
4. production runtime binary and deployment playbook

## Exit Criteria For "Deployment Ready"

- first-run onboarding path works end-to-end on cold workspace
- normal run path plus replay survives process restart without duplicate Discord output
- compaction triggers execute deterministic flush/checkpoint rotation
- heartbeat handles stalled run/backend/dispatcher cases with expected escalation
- documentation and runbook match actual runtime behavior
