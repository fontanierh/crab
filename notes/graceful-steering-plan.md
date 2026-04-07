# Graceful Steering for Crab

## Problem

When a steering trigger arrives during an active run, Crab immediately SIGKILLs the `claude --print` process. This kills any in-flight tool calls (Bash commands, Agent subprocesses), orphaning their child processes and losing their results. We want a "graceful steer" mode that waits for the current agentic loop iteration (tool execution batch) to complete before interrupting.

## Background: Current Architecture

### Steering Flow (turn_executor.rs, lines 514-555)

1. Streaming loop polls for backend events with 200ms timeout
2. Every poll cycle, `check_for_steering_message()` reads `steering_triggers/` directory
3. If a steering trigger matches the current lane, it calls `interrupt_backend_turn()`
4. `interrupt_backend_turn()` sets an `AtomicBool` flag (daemon.rs line 574)
5. `run_claude_turn()` checks this flag every 500ms in its read loop (daemon.rs line 758)
6. When flag is set: `child.kill()` (SIGKILL), emit `TurnInterrupted`, return Ok

### Claude Code Stream Events (ClaudeRawEvent enum, claude.rs)

```
TextDelta { text }           -- assistant text chunk
ToolCall { id, name, input } -- tool invocation
ToolResult { id, name, out } -- tool execution result
Usage { tokens... }          -- token accounting
RunNote { note }             -- informational
TurnCompleted { stop_reason }-- turn done
TurnInterrupted { reason }   -- interrupted
Error { message }            -- error
```

### Agentic Loop Iteration Pattern in Stream

```
[Iteration 1]
  TextDelta, TextDelta, ...     (assistant generates response)
  ToolCall, ToolCall, ...       (1+ tool calls in batch)
  ToolResult, ToolResult, ...   (results after execution)
  --- LOOP BOUNDARY ---         (CC sends results to API, waits for next response)
[Iteration 2]
  TextDelta, TextDelta, ...     (next response)
  ToolCall, ...
  ToolResult, ...
  --- LOOP BOUNDARY ---
[Final]
  TextDelta, TextDelta, ...
  TurnCompleted
```

The boundary between iterations is: after the last ToolResult of a batch, before the first TextDelta/ToolCall of the next iteration.

## Design

### New Trigger Type: Graceful Steering

**File-based signal:** New directory `graceful_steering/` alongside `pending_triggers/` and `steering_triggers/`.

**CLI flag:** `crab-trigger --steer-graceful` writes to `graceful_steering/` instead of `steering_triggers/` or `pending_triggers/`.

**Same PendingTrigger struct**, same validation, just different directory and different consumption semantics.

### Changes by File

#### 1. crab-core/src/self_trigger.rs

Add:
```rust
pub const GRACEFUL_STEERING_DIR_NAME: &str = "graceful_steering";

pub fn write_graceful_steering_trigger(state_root: &Path, trigger: &PendingTrigger) -> CrabResult<PathBuf>
pub fn read_graceful_steering_triggers(state_root: &Path) -> CrabResult<Vec<(PathBuf, PendingTrigger)>>
pub fn consume_graceful_steering_trigger(path: &Path) -> CrabResult<()>
```

These are identical to the steering trigger functions but use `GRACEFUL_STEERING_DIR_NAME`. Follow the exact same pattern as `write_steering_trigger` / `read_steering_triggers` / `consume_steering_trigger`.

Add corresponding unit tests following the same pattern as the existing steering trigger tests:
- `graceful_steering_write_and_read_round_trip`
- `graceful_steering_consume_deletes_file`
- `graceful_steering_read_returns_empty_when_directory_missing`
- `graceful_steering_write_rejects_invalid_trigger`
- `graceful_steering_consume_returns_error_for_missing_file`

#### 2. crab-core/src/lib.rs

Export the new functions and constant:
```rust
pub use self_trigger::{
    // existing exports...
    write_graceful_steering_trigger,
    read_graceful_steering_triggers,
    consume_graceful_steering_trigger,
    GRACEFUL_STEERING_DIR_NAME,
};
```

#### 3. crab-app/src/trigger_cli.rs

Add `--steer-graceful` flag alongside existing `--steer`:

```rust
// In the filter closure for flag parsing:
if a.as_str() == "--steer-graceful" {
    steer_graceful = true;
    false
}

// In execute():
if steer_graceful {
    write_graceful_steering_trigger(&state_path, &trigger)
} else if steer {
    write_steering_trigger(&state_path, &trigger)
} else {
    write_pending_trigger(&state_path, &trigger)
}
```

Reject `--steer` and `--steer-graceful` together (mutual exclusion).

Update USAGE string to document the new flag.

Add unit tests:
- `steer_graceful_flag_writes_to_graceful_steering_directory`
- `steer_and_steer_graceful_together_returns_error`

#### 4. crab-app/src/turn_executor.rs

This is the core change. Modify the streaming loop to support graceful steering.

**New method on TurnExecutor:**
```rust
fn check_for_graceful_steering_trigger(&mut self, current_logical_session_id: &str) -> CrabResult<bool> {
    let state_root = self.composition.state_stores.root.clone();
    let triggers = crab_core::read_graceful_steering_triggers(&state_root)?;
    for (trigger_path, trigger) in triggers {
        match self.enqueue_pending_trigger(&trigger.channel_id, &trigger.message) {
            Ok(queued) => {
                crab_core::consume_graceful_steering_trigger(&trigger_path)?;
                if queued.logical_session_id == current_logical_session_id {
                    return Ok(true);
                }
            }
            Err(_error) => {
                #[cfg(not(coverage))]
                tracing::warn!(
                    channel_id = %trigger.channel_id,
                    error = %_error,
                    "failed to enqueue graceful steering trigger"
                );
            }
        }
    }
    Ok(false)
}
```

**Modify the streaming loop** (lines ~514-555):

Add state tracking:
```rust
let mut last_event_was_tool_result = false;
let mut graceful_steer_pending = false;
```

After processing each backend event, add loop boundary detection:
```rust
match &backend_event.kind {
    BackendEventKind::ToolResult { .. } => {
        last_event_was_tool_result = true;
    }
    BackendEventKind::TextDelta { .. } | BackendEventKind::ToolCall { .. } => {
        if last_event_was_tool_result && graceful_steer_pending {
            // Loop boundary detected: tools finished, new iteration starting.
            // Kill now before the new iteration's work begins.
            let _ = self.runtime.interrupt_backend_turn(&physical_session, &turn_id);
            steered = true;
            break;
        }
        last_event_was_tool_result = false;
    }
    BackendEventKind::TurnCompleted { .. } => {
        // Turn completed naturally. If graceful steer was pending, we're done.
        // The steering message is already enqueued and will be dispatched next.
        if graceful_steer_pending {
            steered = true;
        }
    }
    _ => {}
}
```

In the existing steering check section (both event and timeout branches), add graceful steering check:
```rust
// Existing immediate steering check (unchanged):
if self.check_for_steering_message(&run.logical_session_id)? {
    let _ = self.runtime.interrupt_backend_turn(&physical_session, &turn_id);
    steered = true;
    break;
}

// NEW: graceful steering check (does NOT break immediately):
if !graceful_steer_pending && self.check_for_graceful_steering_trigger(&run.logical_session_id)? {
    graceful_steer_pending = true;
    // Don't break! Let the current iteration finish.
}
```

**Unit tests to add:**
- `graceful_steering_trigger_waits_for_tool_result_before_interrupting` -- trigger arrives during TextDelta, verify it waits past ToolCall+ToolResult, interrupts at next TextDelta
- `graceful_steering_trigger_for_different_lane_does_not_steer` -- cross-lane graceful trigger consumed but no interrupt
- `graceful_steering_trigger_interrupts_at_turn_completed_if_no_more_tools` -- if turn ends naturally with graceful pending, mark steered
- `graceful_and_immediate_steering_together_immediate_wins` -- if both arrive, immediate steering takes priority
- `graceful_steering_enqueue_error_does_not_abort_run` -- bad trigger doesn't crash

#### 5. crab-app/src/daemon.rs

**Idle loop:** Add graceful steering trigger consumption (like existing steering triggers):
```rust
// After the existing steering trigger consumption block:
for (trigger_path, trigger) in
    crab_core::read_graceful_steering_triggers(&executor.composition().state_stores.root)?
{
    match executor.enqueue_pending_trigger(&trigger.channel_id, &trigger.message) {
        Ok(_) => {
            crab_core::consume_graceful_steering_trigger(&trigger_path)?;
            stats.ingested_triggers = stats.ingested_triggers.saturating_add(1);
        }
        Err(_error) => {
            #[cfg(not(coverage))]
            tracing::warn!(
                channel_id = %trigger.channel_id,
                error = %_error,
                "failed to enqueue graceful steering trigger from idle loop"
            );
        }
    }
}
```

Add test: `daemon_loop_ingests_graceful_steering_triggers_when_idle`

## Implementation Order

1. `crab-core/src/self_trigger.rs` -- new functions + tests
2. `crab-core/src/lib.rs` -- exports
3. `crab-app/src/trigger_cli.rs` -- new flag + tests
4. `crab-app/src/turn_executor.rs` -- core logic + tests (biggest change)
5. `crab-app/src/daemon.rs` -- idle loop consumption + test

## Edge Cases

- **Graceful + immediate steer at same time:** Immediate wins because `check_for_steering_message` runs first and breaks immediately.
- **Graceful steer during text-only turn (no tools):** Waits for TurnCompleted, which is correct -- the turn finishes naturally.
- **Multiple graceful steers during one turn:** Flag is set once, messages pile up in queue. All get processed after the steer.
- **Stall timeout:** If a tool execution hangs beyond `CRAB_BACKEND_STALL_TIMEOUT_SECS`, the existing stall detection in `run_claude_turn` kills the process regardless of graceful steer.
- **CC parallel tool execution:** CC runs up to 10 concurrent-safe tools in parallel. All ToolResults arrive before any new TextDelta. So the boundary detection is clean.
- **Usage events between ToolResult and TextDelta:** Usage events don't reset `last_event_was_tool_result`, so they don't interfere with boundary detection. The `_` arm in the match handles this.
- **RunNote events:** Same as Usage -- ignored by boundary detection.

## Testing Strategy

1. All new functions get unit tests following existing patterns
2. The turn_executor tests use `FakeRuntime` with scripted events, so we can precisely control event ordering to test boundary detection
3. The daemon idle loop test follows the existing `daemon_loop_ingests_steering_triggers_when_idle` pattern
4. CI must be green: `cargo test --workspace`

## Files Changed (Summary)

| File | Lines Added (est.) |
|------|-------------------|
| crab-core/src/self_trigger.rs | ~50 (3 fns + 5 tests) |
| crab-core/src/lib.rs | ~5 (exports) |
| crab-app/src/trigger_cli.rs | ~40 (flag + 2 tests) |
| crab-app/src/turn_executor.rs | ~150 (logic + 5 tests) |
| crab-app/src/daemon.rs | ~40 (idle loop + 1 test) |
| **Total** | **~285 lines** |
