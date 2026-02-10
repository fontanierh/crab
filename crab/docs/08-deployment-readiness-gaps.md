# Deployment Readiness Gaps

## Scope

This document lists concrete gaps to close before deploying Crab on the target machine.

## Summary

Core architecture modules are present and heavily tested, but a few integration points remain to fully match the design contract in production runtime behavior.

## Gap 1: Rotation Trigger Integration In Turn Finalization

Current status:

- Rotation primitives exist in `crab-core`.
- Turn executor finalization currently completes runs without invoking rotation trigger evaluation/sequence.

Impact:

- token/inactivity/manual rotation policy will not fire end-to-end yet.

Required work:

- wire `evaluate_rotation_triggers` and `execute_rotation_sequence` into app runtime flow.

## Gap 2: Token Accounting Propagation For Compaction Decisions

Current status:

- backend normalizers emit usage notes/events.
- session token accounting fields exist.
- turn executor does not currently aggregate/update session token accounting from backend events.

Impact:

- token-threshold compaction trigger cannot operate reliably.

Required work:

- parse usage run-note payloads and persist token accounting updates at run completion.

## Gap 3: Runtime Config Surface For Rotation/Heartbeat Policies

Current status:

- `RuntimeConfig` currently includes token/workspace/max lanes/owner settings.
- compaction threshold, inactivity timeout, and heartbeat timeout fields are not yet represented in runtime config loader.

Impact:

- deployment policy tuning requires code-level defaults instead of explicit config.

Required work:

- extend config schema and env parsing for compaction/inactivity/heartbeat policy values.

## Gap 4: Manual Compact/Reset Operator Surface

Current status:

- operator commands currently cover backend/model/reasoning/profile and onboarding reset/rerun.
- compact/reset commands from design are not yet exposed in operator parser.

Impact:

- no explicit operator-triggered rotation command path.

Required work:

- add and test owner-only manual compact/reset command handling.

## Gap 5: Full Bot Runtime Entrypoint

Current status:

- `crab-app` currently exposes library orchestration and memory CLI binaries.
- no production Discord bot runtime binary is present yet in `crab-app/src/bin`.

Impact:

- deployment cannot run a single packaged runtime process yet.

Required work:

- add runtime executable wiring startup, ingress loop, scheduler dispatch, heartbeats, and reconciliation.

## Recommended Closure Order

1. runtime config extensions
2. token accounting propagation
3. rotation trigger + sequence wiring
4. manual compact/reset commands
5. final bot runtime binary integration and deployment playbook

## Exit Criteria For "Deployment Ready"

- first-run onboarding path works end-to-end on cold workspace
- normal run path plus replay survives process restart without duplicate Discord output
- compaction triggers execute deterministic flush/checkpoint rotation
- heartbeat handles stalled run/backend/dispatcher cases with expected escalation
- documentation and runbook match actual runtime behavior
