# Agent host state

`agent-host` owns one SQLite database and one in-memory actor per live ACP session. The actor owns
the connection to exactly one supervised ACP subprocess; the database is the durable operator/UI
view.

![Negotiated ACP v1 steering](acp-v1-steering-flow.png)

## Schema lifecycle

- `PRAGMA user_version = 1` creates `sessions`, `prompts`, `events`, and `permissions` atomically.
- WAL mode, foreign keys, a five-second busy timeout, and full synchronous writes are mandatory.
- Unknown schema versions fail closed with `StorageUnavailable`.
- A process cannot survive a Crab restart. Reopening marks `Starting`, `Ready`, `Busy`, `Detaching`
  or `Stopping` sessions and their unfinished prompts as `Failed`; their native events remain
  readable. Graceful shutdown commits `Detached` instead, which remains eligible for explicit
  [native resume](agent-session-resume.md) without changing either identity or replaying Crab
  bootstrap context.

## Durable invariants

- `(session_id, client_turn_id)` deduplicates one immutable prompt; a changed retry is rejected.
- `native_prompt_json` is the exact JSON array carried in ACP's `prompt` field.
- Native stdin/stdout JSON-RPC lines are stored byte-for-byte with direction and per-session order.
- Every accepted run ends with exactly one `RunFinished` event. A native ACP terminal response or
  idle update remains authoritative; if a prompt instead returns a JSON-RPC error, Crab preserves
  that error and atomically appends a minimal `crab/run_finished` lifecycle notification before
  clearing the active run.
- ACP v1 has one foreground run; queued prompts drain FIFO after its prompt response. A configured
  `_session/steering` extension may contribute to that run only after `initialize` advertises it.
  Crab requests `promptRequired` on an idle race and starts the continuation itself through normal
  `session/prompt`, so no detached turn escapes the durable lifecycle.
- ACP v2 steering contributes to the active run; queued prompts wait for an `idle` state update.
- Permission requests and the automatically selected strongest allow response are audit records,
  never human-gated work.
- Configured stdio MCP declarations are attached to every `session/new` and `session/resume`. Crab
  injects canonical state/workspace paths and session identity; draft ACP v2 must advertise
  `session.mcp.stdio`.
- Host-wide detach cancels active work, waits for acknowledgement, fails unfinished prompts and
  tears down ACP process groups without sending `session/close`.
- Explicit close sends native `session/close`, commits `Stopped` and is never resumable.
