# Rotation, Checkpoint, And Compaction

## Scope

This document covers the compaction/rotation protocol and the hidden turns used to preserve continuity.

## Core Decision

Compaction is harness-owned.

- Backends generate events and token usage telemetry.
- Crab decides when to rotate.
- Crab owns the hidden memory flush + checkpoint protocol.

This avoids backend-specific auto-summarization behavior controlling durable state transitions.

## Trigger Model

Rotation triggers are defined by `evaluate_rotation_triggers` in `crates/crab-core/src/rotation.rs`.

Possible triggers:

- manual compact
- manual reset
- backend compaction (PreCompact hook signal)
- inactivity timeout while lane is idle

Trigger input includes:

- current time
- last activity time
- lane idle status
- backend compaction signaled (bool)
- inactivity timeout
- optional manual request

Backend compaction signal mechanism:

- Claude Code fires a `PreCompact` hook before auto-compacting context.
- The hook writes a signal file at `<state_root>/compaction-signal` via
  `touch state/compaction-signal` (configured in `.claude/settings.json`).
- `TurnExecutor` checks for signal file existence before building rotation trigger input.
- On successful rotation with `BackendCompaction` trigger, the signal file is consumed (deleted).
- This replaces the previous token-threshold approach which was broken because cumulative
  `cache_read_input_tokens` caused the threshold to fire after every single turn.

## Hidden Step A: Memory Flush

Memory flush contract (`crates/crab-core/src/memory_flush.rs`):

- hidden turn prompt asks agent to persist durable memory facts
- the last non-empty line of assistant output must be one of:
  - `NO_REPLY`
  - `MEMORY_FLUSH_DONE`
- preceding text (reasoning, tool output deltas) is tolerated as long as the sentinel
  appears on the final non-empty line
- flush output is suppressed from user-visible chat

Flush failures are non-blocking in rotation sequence; they are captured as rotation metadata.

## Hidden Step B: Checkpoint Turn

Checkpoint contract (`crates/crab-core/src/checkpoint_turn.rs`):

```json
{
  "summary": "string",
  "decisions": ["string"],
  "open_questions": ["string"],
  "next_actions": ["string"],
  "artifacts": [{"path": "string", "note": "string"}]
}
```

Strictness:

- unknown fields rejected (`deny_unknown_fields`)
- required fields validated
- strings must be non-empty

Retry policy:

- max attempts = 2 (one retry)
- retry prompt includes parse/schema error and required schema

Fallback policy:

- if checkpoint output remains invalid, Crab builds deterministic fallback checkpoint from transcript tail and durable metadata (`crates/crab-core/src/checkpoint_fallback.rs`)

## Rotation Sequence

Sequence executor (`crates/crab-core/src/rotation_sequence.rs`):

1. run hidden memory flush (best effort)
2. run hidden checkpoint turn (or fallback)
3. persist checkpoint
4. end physical session
5. clear active physical handle

Result includes:

- checkpoint id
- whether fallback was used
- optional memory flush error
- optional checkpoint-turn error

## Checkpoint Persistence And Reuse

Persistent checkpoint record (`Checkpoint`):

- `id`
- `logical_session_id`
- `run_id`
- `created_at_epoch_ms`
- `summary`
- `memory_digest`
- `state` map

Only latest checkpoint summary is injected by default into future context.

## Backend Responsibilities (Thin Glue)

Per backend, only these pieces are backend-specific:

- run hidden memory flush turn and return text output
- run hidden checkpoint turn and return text output
- extract text from backend event stream reliably
- keep hidden-turn output out of user-visible Discord delivery

All schema validation/retry/fallback logic is shared in `crab-core`.

## Current Status

Implemented:

- trigger evaluator primitives
- hidden memory flush primitives and `TurnExecutor` backend wiring (memory flush turn
  sends prompt to backend, drains stream, validates ack token; errors are non-blocking)
- checkpoint parser/retry/fallback primitives
- rotation sequence primitives
- token accounting propagation from normalized backend usage payloads into
  `LogicalSession.token_accounting` during run finalization
- `TurnExecutor` post-run finalization wiring into
  `evaluate_rotation_triggers` + `execute_rotation_sequence`
- owner-only manual rotation commands:
  - `/compact confirm`
  - `/reset confirm`
- auditable operator and rotation lifecycle run-note events for manual and automatic rotation

Deployment note:

- production runtime wiring is now implemented (`crabd` + `crab-discord-connector`).
- remaining deployment blocker is execution evidence for the target-machine acceptance checklist
  (Gap 2 in `crab/docs/08-deployment-readiness-gaps.md`).
