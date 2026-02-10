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

`DiscordRuntimeAdapter` (`crates/crab-discord/src/runtime_adapter.rs`) wraps outbound send/edit
transport with bounded retry semantics:

- retryable failures retry with configured fixed backoff
- rate-limited failures retry using `retry_after_ms` when present (clamped to configured max backoff)
- fatal failures fail immediately
- `mark_sent` is only called after successful delivery, preserving ledger correctness on retries

## Connector Receipt Protocol (`crabd` <-> `crab-discord-connector`)

Crab runs Discord I/O via a separate connector process. The IPC contract is JSONL and the
shared frame types live in `crates/crab-discord/src/lib.rs`:

- `CrabdOutboundOp` (emitted by `crabd` stdout, consumed by connector)
- `CrabdInboundFrame` (emitted by connector, consumed by `crabd` stdin)
- `CrabdOutboundReceipt` (carried by `CrabdInboundFrame::OutboundReceipt`)

Outbound operations:

- `crabd` emits `CrabdOutboundOp` frames with:
  - `op_id`: per-operation request id used to correlate receipts.
  - `delivery_id`: deterministic identifier for the delivery target (stable across retries/restarts).

Receipts:

- Connector emits exactly one `CrabdOutboundReceipt` per outbound op (success or error).
- `crabd` considers the Discord side effect complete only after receiving an `ok` receipt.
  This is required for correctness of outbound persistence and replay after restart.

Idempotency:

- Connector persists a `(channel_id, delivery_id) -> discord_message_id` mapping.
- On `post` when a mapping already exists, the connector does not post again; it returns an
  `ok` receipt immediately with the existing Discord message id.
- `edit` requires an existing mapping; missing mapping produces an `error` receipt.

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
- Connector crash/receipt loss: replay re-emits the same `delivery_id` and connector de-dupes based on its mapping store.
- Process restart during running turn: startup reconciliation settles stale run.
- Persistent backend unhealthy: heartbeat restarts manager.
- Cancel path broken: heartbeat escalates to hard-stop-and-rotate.

## Current Status

Implemented:

- idempotent delivery ledger + outbound record persistence
- replay path in turn executor
- startup reconciliation primitives
- heartbeat policy engine
- app-runtime maintenance integration:
  - `boot_runtime_with_processes*` runs startup reconciliation before runtime begins dispatching
  - `run_heartbeat_if_due` executes deterministic heartbeat ticks based on configured interval
- Discord runtime adapter seam with deterministic retry/rate-limit handling for send/edit operations

Remaining gap:

- Deployment acceptance evidence is still pending on the target machine (see `crab/docs/08-deployment-readiness-gaps.md`).
