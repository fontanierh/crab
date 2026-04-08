# Plan: Steering Message Wrapping

## Problem

When Crab's graceful steering interrupts an active agent run to deliver a user's follow-up message, the steering message becomes the input for the *next* turn. Because it arrives as a raw, unwrapped user message, the agent (Claude Code) has no indication that the previous turn was interrupted or that this message is a follow-up. The agent treats it as a brand-new request, often re-answering or losing conversational context.

Real example: user says "Ok lets hide on phone yeah", then sends "did you do it?" while the agent is mid-run. After steering, the agent sees only "did you do it?" as a fresh turn and responds "Which one? A few things landed recently..." with no connection to the prior conversation.

## Background: How Claude Code Handles This Natively

In Claude Code's own source (`src/utils/messages.ts`), when a user sends a message during tool execution, CC wraps it via `wrapCommandText()`:

```
The user sent a new message while you were working:
{raw message}

IMPORTANT: After completing your current task, you MUST address the user's message above. Do not ignore it.
```

This is injected mid-turn via a pending message queue and wrapped in `<system-reminder>` tags. However, Crab's model is different: the previous turn is terminated (RunStatus::Cancelled) and the steering message starts a fresh turn. So the CC wrapper doesn't directly apply, but the pattern of contextualizing the message does.

## Solution

Wrap steering messages with a `<system-reminder>` tag before they become the next turn's input. The wrapper tells the agent:
1. This message arrived while you were working (it's not a cold start)
2. It's likely a follow-up or clarification (not a new topic)
3. Don't re-answer or restart previous work

### Wrapper format

```
<system-reminder>
The user sent this additional message while you were working. It is likely a follow-up or clarification to your current conversation, not a new request. Continue naturally, incorporating this input. Do not re-answer or restart your previous work.
</system-reminder>
{raw steering message}
```

## Implementation

### File: `crates/crab-app/src/turn_executor.rs`

**1. Add `wrap_steering_message()` function** (module-level, before `#[cfg(test)]`):

```rust
fn wrap_steering_message(raw: &str) -> String {
    format!(
        "<system-reminder>\n\
         The user sent this additional message while you were working. \
         It is likely a follow-up or clarification to your current conversation, \
         not a new request. Continue naturally, incorporating this input. \
         Do not re-answer or restart your previous work.\n\
         </system-reminder>\n\
         {raw}"
    )
}
```

**2. Modify `consume_and_batch_triggers()`** to conditionally wrap:

- At the top of the function, derive `is_steering` from the existing `_label` parameter:
  ```rust
  let is_steering = _label == "steering" || _label == "graceful steering";
  ```
- After building `combined`, wrap it before passing to `enqueue_pending_trigger`:
  ```rust
  let enqueue_content = if is_steering {
      wrap_steering_message(&combined)
  } else {
      combined.clone()
  };
  ```
- Pass `enqueue_content` to `self.enqueue_pending_trigger(channel_id, &enqueue_content)`.

**3. Update existing tests:**

- `batch_triggers_combines_same_channel_messages_with_delimiter`: change `assert_eq!` on `run.user_input` to `assert!(contains(...))` since the raw message is now wrapped.
- `batch_triggers_single_trigger_not_delimited`: same pattern.

**4. Add new tests:**

- `wrap_steering_message_includes_system_reminder_and_raw_content`: unit test for the wrapper function.
- `batch_triggers_non_steering_label_does_not_wrap`: verifies that triggers with a non-steering label (e.g. "pending") pass through unwrapped.

### What is NOT changed

- `enqueue_pending_trigger()` itself is unchanged (it remains a general-purpose enqueue).
- Normal pending triggers (cron jobs, self-triggers) are not affected.
- The steering detection logic (boundary detection, interrupt_backend_turn, graceful polling) is untouched.
- No changes outside `turn_executor.rs`.

## Call sites that pass through `consume_and_batch_triggers`

| Caller | Label | Wrapping? |
|--------|-------|-----------|
| `check_for_steering_trigger()` | `"steering"` | Yes |
| `check_for_graceful_steering_trigger()` | `"graceful steering"` | Yes |
| `daemon.rs` idle loop (steering) | `"steering"` | Yes |
| `daemon.rs` idle loop (graceful) | `"graceful steering"` | Yes |

Normal pending triggers go through `enqueue_pending_trigger()` directly (in daemon.rs idle loop), bypassing `consume_and_batch_triggers` entirely. They are unaffected.

## Risk Assessment

- **Low risk**: single-file change, no structural modifications, no new dependencies.
- **Backwards compatible**: the wrapper is purely additive content in the user_input field.
- **Testable**: 2 new unit tests + 2 updated assertions. Full suite (266 tests) passes.
- **Reversible**: removing the wrapper is a one-line change (remove the `if is_steering` conditional).
