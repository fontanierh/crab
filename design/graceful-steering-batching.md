# Design: Batch Steering Trigger Messages

## Problem

When multiple steering triggers arrive for the same lane (e.g., a user sends 3 messages while Crab is busy), the current implementation creates a separate `QueuedRun` for each trigger. This causes **cascade interruptions**: run 1 gets steered by trigger A, then run A starts and immediately gets steered by trigger B, then run B starts and gets steered by trigger C, and so on. Each run is wasted — the agent never gets to see all the pending messages together.

Additionally, `check_for_steering_trigger` and `check_for_graceful_steering_trigger` have an **early-return bug**: they `return Ok(true)` on the first trigger matching the current lane, leaving remaining triggers unconsumed on disk. These get picked up on the next poll cycle as individual runs, feeding the cascade.

### Prior art: duplicate collapse (reverted)

Commit `8786a0f` attempted to fix this via `lane_has_queued_run` — skipping enqueue if the lane already had a queued run. This was **reverted in `eebf825`** because it silently dropped messages: the second trigger's file was consumed (deleted) but its message was never enqueued, so the content was lost forever.

## Solution

**Batch all pending steering triggers by channel, joining messages into a single combined run.**

When steering triggers are consumed (either at a boundary during an active run, or in the daemon idle loop), we:

1. Read ALL trigger files from the directory
2. Sort by filename (which embeds a timestamp) to preserve chronological order
3. Group messages by `channel_id`
4. For each channel, enqueue ONE run with all messages joined by `\n`
5. Consume (delete) ALL trigger files

This preserves every message (no silent drops), reduces N runs to 1 per channel, and the agent sees the full conversation context in a single turn.

## Changes

### 1. `file_signal::read_signal_files` — sort by filename

**File:** `crates/crab-core/src/file_signal.rs`

Currently returns files in `fs::read_dir` order (unspecified). Add a sort by filename after collecting, so trigger messages are ordered chronologically:

```rust
results.sort_by(|(a, _), (b, _)| a.file_name().cmp(&b.file_name()));
```

Since filenames are `{timestamp_ms}-{suffix}.json`, lexicographic sort equals chronological sort.

### 2. `TurnExecutor::consume_and_batch_triggers` — new shared helper

**File:** `crates/crab-app/src/turn_executor.rs`

New method that replaces the per-trigger loop in both `check_for_steering_trigger` and `check_for_graceful_steering_trigger`:

```rust
fn consume_and_batch_triggers(
    &mut self,
    triggers: &[(PathBuf, PendingTrigger)],
    current_logical_session_id: &str,
    consume_fn: fn(&Path) -> CrabResult<()>,
    label: &str,
) -> CrabResult<bool>
```

Logic:
- If `triggers` is empty, return `Ok(false)`
- Group messages by `channel_id` using `BTreeMap<&str, Vec<&str>>` (preserving order from sorted input)
- For each channel, call `enqueue_pending_trigger(channel_id, combined_message)` where `combined_message = messages.join("\n")`
- Track whether any enqueued run matched `current_logical_session_id`
- Consume ALL trigger files regardless of enqueue success (they've been read; leaving them would cause duplicates on retry)
- Return whether any matched the current lane

### 3. Refactor `check_for_steering_trigger` and `check_for_graceful_steering_trigger`

Both methods become thin wrappers that read triggers and delegate to `consume_and_batch_triggers`.

### 4. Daemon idle loop — same batching

**File:** `crates/crab-app/src/daemon.rs`

Replace the per-trigger loops for steering and graceful steering with calls to `executor.consume_and_batch_triggers`. The method is made `pub` for this purpose.

Pass `""` as `current_logical_session_id` since there's no active lane when idle (the return value is ignored).

### 5. Daemon idle loop — batch pending triggers too

The pending trigger loop in the daemon also processes triggers one-at-a-time. Apply the same batching to avoid creating N runs when N messages arrive for the same channel. This uses the same `consume_and_batch_triggers` helper with `crab_core::consume_pending_trigger`.

## Edge Cases

### Multiple channels in the same batch
If triggers A (channel 777) and B (channel 999) are both pending, two separate runs are created — one per channel. This is correct: they're different conversations.

### Enqueue failure for one channel
If enqueue fails for channel 777 but succeeds for 999, the trigger files for BOTH are still consumed. The failed channel's messages are lost, but this matches current behavior (single-trigger enqueue failures also lose the message). The alternative — leaving files on disk for retry — risks infinite retry loops on persistent errors (e.g., blank channel_id).

### Gateway message + file triggers at the same boundary
`check_for_steering_message` checks gateway messages first, then falls through to `check_for_steering_trigger`. If a gateway message matches the lane, all file-based triggers are still consumed on the next poll cycle (200ms later). This is acceptable — the gateway message steers immediately, and the file triggers create a queued run for after.

### Trigger arrives between batch-read and batch-consume
A trigger file written after `read_signal_files` but before all `consume_fn` calls will NOT be in the current batch. It will be picked up on the next poll cycle. This is fine — the 200ms poll interval makes this a very small window, and the trigger isn't lost.

### Message ordering
With the sort fix in `read_signal_files`, messages within a batch are chronologically ordered. The joined message preserves this order.

## What this does NOT change

- Immediate vs graceful steering semantics (unchanged)
- Immediate steering priority over graceful (unchanged)
- Boundary detection logic (unchanged)
- Run status after steering: `Cancelled` for steered, `Succeeded` for natural completion (unchanged)
- Scheduler lane concurrency model (unchanged)
- Pending trigger consumption in daemon (now batched, same semantics)
