# Reliability, Delivery, And Recovery

## Scope

This document describes how Crab avoids duplicate output, recovers from crashes, and maintains forward progress.

## Decisions

- Decision: persist normalized event envelopes for every run.
  - Rationale: deterministic replay, auditability, and recovery from partial failure.

- Decision: delivery is idempotent and content-hash checked.
  - Rationale: avoid Discord duplicate spam after retries/restarts.

- Decision: startup reconciliation is explicit and stateful.
  - Rationale: stale in-flight runs must be settled deterministically after restart.

## Event Envelope

`EventEnvelope` includes run/turn/lane/session/profile metadata plus per-run monotonic sequence and idempotency key (`crates/crab-core/src/domain.rs`).

Minimum event classes include:

- run state transitions
- text deltas
- tool calls/results
- approval requests
- run notes/errors

## Idempotent Delivery

`IdempotentDeliveryLedger` (`crates/crab-discord/src/lib.rs`) keys delivery records by:

- logical session id
- run id
- channel id
- message id
- edit generation
- content hash

Behavior:

- identical re-send for same target/edit generation is skipped
- conflicting hash for same target/edit generation is rejected as invariant violation
- successful sends are persisted as outbound records

## Replay Behavior

`TurnExecutor.replay_delivery_for_run` replays persisted text-delta events and re-applies idempotent delivery checks before sending.

Result:

- already sent chunks are skipped
- missing chunks can be delivered
- replay count is observable

## Startup Reconciliation

`execute_startup_reconciliation` (`crates/crab-core/src/startup_reconciliation.rs`) on boot:

1. restart backend managers
2. scan sessions and runs
3. detect stale in-flight runs (running beyond grace period)
4. mark stale runs as cancelled
5. append synthetic recovery run-state event
6. clear active physical handles when needed

This forces next user turn to start with a valid physical session.

## Heartbeat Cycle

`execute_heartbeat_cycle` (`crates/crab-core/src/heartbeat.rs`) runs three health loops:

- run heartbeat: cancel stalled active runs
- backend heartbeat: restart unhealthy persistent backends
- dispatcher heartbeat: nudge scheduler if queued work is stuck

If cancel request itself fails, runtime escalates to hard-stop-and-rotate for that run.

## Cancellation Semantics

Lane scheduler and runtime support:

- cancel active lane run
- cancel queued run by id
- preserve FIFO order for remaining queued runs

Lane/state invariants prevent ambiguous cancellation transitions.

## Failure Model Summary

- Delivery failure after event append: replay can re-deliver missing output.
- Process restart during running turn: startup reconciliation settles stale run.
- Persistent backend unhealthy: heartbeat restarts manager.
- Cancel path broken: heartbeat escalates to hard-stop-and-rotate.

## Current Status

Implemented:

- idempotent delivery ledger + outbound record persistence
- replay path in turn executor
- startup reconciliation primitives
- heartbeat policy engine

Integration expectation:

- production runtime loop should invoke startup reconciliation and heartbeat on schedule.
