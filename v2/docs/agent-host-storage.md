# Agent host state

`agent-host` owns one SQLite database and one in-memory actor per live ACP session. The actor owns
the connection to exactly one supervised ACP subprocess; the database is the durable operator/UI
view.

## Schema lifecycle

- `PRAGMA user_version = 1` creates `sessions`, `prompts`, `events`, and `permissions` atomically.
- WAL mode, foreign keys, a five-second busy timeout, and full synchronous writes are mandatory.
- Unknown schema versions fail closed with `StorageUnavailable`.
- A process cannot survive a Crab restart. Reopening marks `Starting`, `Ready`, `Busy`, or
  `Stopping` sessions and their unfinished prompts as `Failed`; their native events remain readable.

## Durable invariants

- `(session_id, client_turn_id)` deduplicates one immutable prompt; a changed retry is rejected.
- `native_prompt_json` is the exact JSON array carried in ACP's `prompt` field.
- Native stdin/stdout JSON-RPC lines are stored byte-for-byte with direction and per-session order.
- ACP v1 has one foreground run; queued prompts drain FIFO after its prompt response.
- ACP v2 steering contributes to the active run; queued prompts wait for an `idle` state update.
- Permission requests and the automatically selected strongest allow response are audit records,
  never human-gated work.
- Configured stdio MCP declarations are attached to every `session/new`. Crab injects canonical
  state/workspace paths and session identity; draft ACP v2 must advertise `session.mcp.stdio`.
- Closing sends the native `session/close` request. Dropping a host tears down every remaining ACP
  process group through the official SDK transport guard.
