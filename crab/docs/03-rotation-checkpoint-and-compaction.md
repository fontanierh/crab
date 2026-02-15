# Rotation and Checkpoint

## Scope

This document covers the agent-driven rotation protocol used to preserve continuity across backend session boundaries.

## Core Decision

Rotation is agent-driven.

- The agent decides when to rotate (context too large, task boundary, operator request).
- The agent produces the checkpoint itself (structured JSON).
- The agent signals rotation via the `crab-rotate` CLI.
- Crab executes the rotation sequence (persist checkpoint, end session, clear handle).

This eliminates all automatic triggers, hidden turns, and harness-owned summarization.

## Agent Workflow

The `rotate-session` skill (bootstrapped at `.agents/skills/rotate-session/SKILL.md`) guides the agent through:

1. Persist important facts to memory files (`MEMORY.md`, `memory/` directory).
2. Build a checkpoint JSON document.
3. Call `crab-rotate` to signal rotation.

## Checkpoint Schema

```json
{
  "summary": "string (required, non-empty)",
  "decisions": ["string (non-empty)"],
  "open_questions": ["string (non-empty)"],
  "next_actions": ["string (non-empty)"],
  "artifacts": [{"path": "string (non-empty)", "note": "string (non-empty)"}]
}
```

Strictness:

- unknown fields rejected (`deny_unknown_fields`)
- required fields validated
- strings must be non-empty

Schema validation is in `crates/crab-core/src/checkpoint_turn.rs` (`parse_checkpoint_turn_document()`).

## `crab-rotate` CLI

```
Usage:
  crab-rotate --state-dir <path> --checkpoint <json>
  crab-rotate --state-dir <path> --checkpoint-file <path>
  cat checkpoint.json | crab-rotate --state-dir <path>
```

The CLI validates the checkpoint, builds a `PendingRotation` record, and writes it to `<state_dir>/pending_rotations/<id>.json`.

Implementation: `crates/crab-app/src/rotate_cli.rs`.

## Pending Rotation Signal

The `pending_rotations/` directory under the state root holds JSON files signaling requested rotations. Each file contains a `PendingRotation` wrapping a validated `CheckpointTurnDocument`.

Functions (`crates/crab-core/src/pending_rotation.rs`):

- `write_pending_rotation()` — writes a timestamped signal file
- `read_pending_rotations()` — reads all pending signal files (skips malformed)
- `consume_pending_rotation()` — deletes a consumed signal file

## Rotation Sequence

After each turn completes, `TurnExecutor` checks for pending rotation files. If found:

1. Set lane state to `Rotating`.
2. Emit `rotation_started` run-note event.
3. Execute rotation sequence (`crates/crab-core/src/rotation_sequence.rs`):
   a. Persist the agent-provided checkpoint.
   b. End the physical backend session.
   c. Clear the active physical session handle.
4. Consume the pending rotation file.
5. Emit `rotation_completed` run-note event with checkpoint ID.
6. Set lane state back to `Idle`.

## Checkpoint Persistence and Reuse

Persistent checkpoint record (`Checkpoint`):

- `id`
- `logical_session_id`
- `run_id`
- `created_at_epoch_ms`
- `summary`
- `memory_digest`
- `state` map

Only the latest checkpoint summary is injected into future context.

## Environment

The `CRAB_STATE_DIR` environment variable is set for the backend, pointing to the state directory. The agent uses this to call `crab-rotate`.

## System Prompt

The system prompt (`CRAB_RUNTIME_BRIEF_BASE` in `crates/crab-app/src/daemon.rs`) tells the agent:

> When your context gets large, use the rotate-session skill to checkpoint and rotate.
