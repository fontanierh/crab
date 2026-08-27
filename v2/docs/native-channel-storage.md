# Native channel state

`native-channel` stores durable bindings, accepted turns, publication receipts, interrupts, and
session replacements. The
lossless ACP stream remains authoritative in `agent-host`; channel state references it by session
and sequence instead of copying it into a second event journal.

## Schema lifecycle

- `PRAGMA user_version = 2` creates `bindings`, `turns`, `publications`, `interrupts`, and
  `session_replacements` atomically.
- Schema v1 migrates additively: turns gain nullable interruption receipts plus interrupting-request
  and reason fields, preserving prior queue/steer rows unchanged.
- WAL mode, foreign keys, a five-second busy timeout, and full synchronous writes are mandatory.
- Unknown schema versions fail closed with `StorageUnavailable`.
- ACP processes cannot survive a runtime restart. Reopening marks live bindings `Failed` while
  retaining their session and delivery cursors. Startup first asks `agent-host` to resume the exact
  session, then `recover_session` reattaches the unchanged binding. Only explicit resume
  unavailability permits `replace_session` to attach a fresh session. Startup can inspect a binding
  without a live session or find it by `(adapter_id, channel_id)` after a crash between binding
  creation and route registration.

## Durable invariants

- One live `(adapter_id, channel_id)` binding targets exactly one ACP session.
- `(binding_id, session_id, client_turn_id)` deduplicates an immutable channel turn.
- Native prompt JSON is capped at 2 MiB before parsing or durable turn storage. Larger media uses
  resource links or content handles.
- Queue and steer map through a generated Boxology `agent-host` import; no implementation crate is
  linked across the box boundary.
- Replay reads every ordered ACP event, including client-to-agent messages and tool activity, even
  while the binding is failed or detached; the durable `agent-host` journal does not depend on a
  live adapter process.
- Owner discovery reads a bounded newest-first binding catalog from the same transactionally
  durable rows. The catalog includes pending-turn counts but deliberately omits opaque adapter
  destination metadata; no schema migration is required.
- Adapter publication is acknowledged strictly in sequence. The submitted event must match the
  authoritative ACP record; retries return the same deterministic delivery receipt.
- Native UI interrupt is a separate operation: it cancels the active run and leaves accepted queued
  turns for the agent host to drain in stable order. Automatic interrupting ingress instead uses
  one `accept_interrupting_turn` call that accepts the turn before cancellation can drain the queue.
  Both interruption paths and session-replacement reasons retain operator audit records.
- Session replacement atomically installs the fresh session and adapter metadata while preserving
  the binding identity. A failed replacement leaves the previous binding untouched.
- Same-session recovery requires the binding to be `Failed`, proves the expected session is live,
  and atomically returns it to `Attached`. It preserves binding/session identity plus publication
  and reconciliation cursors; exact retries are idempotent.
