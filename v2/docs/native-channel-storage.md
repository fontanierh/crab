# Native channel state

`native-channel` stores durable bindings, accepted turns, publication receipts, interrupts, and
session replacements. The
lossless ACP stream remains authoritative in `agent-host`; channel state references it by session
and sequence instead of copying it into a second event journal.

## Schema lifecycle

- `PRAGMA user_version = 1` creates `bindings`, `turns`, `publications`, `interrupts`, and
  `session_replacements` atomically.
- WAL mode, foreign keys, a five-second busy timeout, and full synchronous writes are mandatory.
- Unknown schema versions fail closed with `StorageUnavailable`.
- ACP processes cannot survive a runtime restart. Reopening marks live bindings `Failed`; an
  explicit `replace_session` attaches a fresh session while retaining channel identity. Startup
  can inspect a binding without its dead session or find it by `(adapter_id, channel_id)` after a
  crash between binding creation and route registration.

## Durable invariants

- One live `(adapter_id, channel_id)` binding targets exactly one ACP session.
- `(binding_id, session_id, client_turn_id)` deduplicates an immutable channel turn.
- Queue and steer map through a generated Boxology `agent-host` import; no implementation crate is
  linked across the box boundary.
- Replay reads every ordered ACP event, including client-to-agent messages and tool activity.
- Adapter publication is acknowledged strictly in sequence. The submitted event must match the
  authoritative ACP record; retries return the same deterministic delivery receipt.
- Interrupt is a separate operation: it cancels the active run and leaves accepted queued turns for
  the agent host to drain in stable order. Interrupt and session-replacement reasons are retained as
  operator audit records.
