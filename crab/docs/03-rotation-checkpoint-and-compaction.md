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
- token compaction threshold reached
- inactivity timeout while lane is idle

Trigger input includes:

- current time
- last activity time
- lane idle status
- optional token usage total
- compaction threshold
- inactivity timeout
- optional manual request

Token usage source for trigger evaluation:

- `TurnExecutor` now parses normalized backend usage payloads and persists cumulative
  `LogicalSession.token_accounting` on run completion.
- Supported normalized payload key families:
  - `run_usage_input_tokens` / `run_usage_output_tokens` / `run_usage_total_tokens`
    / `run_usage_cache_read_input_tokens` / `run_usage_cache_creation_input_tokens`
  - `usage_input_tokens` / `usage_output_tokens` / `usage_total_tokens`
    / `usage_cache_read_input_tokens` / `usage_cache_creation_input_tokens`
  - `input_tokens` / `output_tokens` / `total_tokens`
    / `cache_read_input_tokens` / `cache_creation_input_tokens`
- Token-triggered compaction consumes `token_accounting.context_window_tokens()` from
  persisted session state. This computes `cache_read_input_tokens +
  cache_creation_input_tokens + input_tokens`, which represents the true context window
  consumption (non-cached input tokens alone massively undercount actual usage when prompt
  caching is active).
- On successful rotation, Crab resets `LogicalSession.token_accounting` back to 0 so the
  threshold represents tokens since the last rotation (avoids retriggering compaction on
  every subsequent run).

## Hidden Step A: Memory Flush

Memory flush contract (`crates/crab-core/src/memory_flush.rs`):

- hidden turn prompt asks agent to persist durable memory facts
- assistant must return exactly one token:
  - `NO_REPLY`
  - `MEMORY_FLUSH_DONE`
- any other output is invalid
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
- hidden memory flush primitives
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
