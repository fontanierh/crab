# Crab - Design Document

A meta-harness that wraps CLI coding agents (Claude Code, Codex CLI, OpenCode) and exposes them through Discord. Inspired by [OpenClaw](https://github.com/openclaw/openclaw)'s architecture for memory, persona, and session management — but built in Rust, and backed by existing CLI agents rather than raw LLM APIs.

Informed by [Rivet's sandbox-agent](https://github.com/rivet-dev/sandbox-agent) which implements a universal adapter over these same agents.

## Architecture Overview

```
Discord Server (primary UI)
    │
    ├── #project-x    ──┐
    ├── #calendar       │  each channel/DM = one logical session
    ├── DM: @user      ──┘
    │
    ▼
┌──────────────────────────────────────────────────┐
│                  Crab (Rust)                     │
│                                                  │
│  Discord Bot                                     │
│    ├── Message listener (all channels/DMs)       │
│    ├── Server management (create channels,       │
│    │   pins, topics, threads)                    │
│    └── Full admin permissions                    │
│                                                  │
│  Session Router                                  │
│    ├── Maps Discord channel → logical session    │
│    └── Manages physical session lifecycle        │
│                                                  │
│  Workspace                                       │
│    ├── SOUL.md                                   │
│    ├── IDENTITY.md                               │
│    ├── AGENTS.md                                 │
│    ├── MEMORY.md                                 │
│    ├── USER.md                                   │
│    └── memory/                                   │
│         └── YYYY-MM-DD.md                        │
│                                                  │
│  Backend Manager                                 │
│    ├── Claude Code backend (subprocess per turn) │
│    ├── Codex CLI backend (persistent app-server) │
│    └── OpenCode backend (persistent HTTP server) │
└──────────────────────────────────────────────────┘
```

## Core Concepts

### Logical Session

A logical session is the ongoing conversation in a Discord channel or DM. It lives as long as the channel exists. It holds:

- A reference to the Discord channel ID
- An ordered list of checkpoints (summaries of past physical sessions)
- The currently active physical session (if any)
- An inactivity timer
- The backend to use (Claude Code, Codex, or OpenCode — configurable per channel)

### Physical Session

A physical session is a short-lived agent conversation that gets recycled via summarization. It maps to:

- Claude Code: a `--resume`-able session identified by `session_id`
- Codex CLI: a thread in the persistent `codex app-server` process, identified by `thread_id`
- OpenCode: a session on the persistent HTTP server, identified by session ID

A new physical session is spawned when:

1. A message arrives and no active physical session exists
2. Token budget is exceeded (tracked via token usage from agent event streams)
3. Backend switch (e.g., Claude Code <-> Codex)
4. Inactivity timeout fires and a new message arrives later

Every physical session is bootstrapped with:

- Workspace files (SOUL.md, IDENTITY.md, AGENTS.md, USER.md)
- MEMORY.md + relevant memory/*.md files
- The latest checkpoint (if any)

### Checkpoint

A checkpoint is a summary produced by the agent at the end of a physical session. It captures:

- Key decisions and conclusions from the conversation
- Open questions or pending tasks
- Relevant context the next session needs

The checkpoint is stored by the harness (not in the workspace) and injected into the next physical session's bootstrap context.

### Inactivity Timeout

Each logical session has a configurable inactivity timer (default: 30 minutes). When it fires:

1. Memory flush: agent is asked to write durable notes to `memory/YYYY-MM-DD.md`
2. Checkpoint: agent produces a summary of the conversation
3. Physical session is killed

Next message in that channel bootstraps a fresh physical session with the checkpoint. This ensures:

- Stale sessions don't hold onto outdated context
- Fresh sessions pick up any changes to SOUL.md, AGENTS.md, etc.
- Memory is durably saved before the session dies

## Workspace

The workspace is a directory on disk shared across all sessions. It contains persona, identity, memory, and instruction files that are injected into every physical session.

```
~/.crab/workspace/
├── SOUL.md          # Persona: tone, personality, behavioral boundaries
├── IDENTITY.md      # Name, emoji/avatar
├── AGENTS.md        # Operating instructions, rules, conventions
├── USER.md          # User profile and preferences
├── MEMORY.md        # Curated long-term memory (manually maintained)
└── memory/
    ├── 2026-02-08.md  # Daily memory log (agent-written)
    └── 2026-02-09.md
```

### File Purposes

**SOUL.md** — Defines who the agent *is*. Personality traits, communication style, behavioral boundaries. Injected into every session's system prompt.

**IDENTITY.md** — Agent's name and visual identity. Used by the Discord bot for display purposes.

**AGENTS.md** — Operating instructions. What the agent should and shouldn't do, coding conventions, project-specific rules. This is the equivalent of OpenClaw's AGENTS.md or Claude Code's CLAUDE.md.

**USER.md** — Profile of the user(s). Preferences, timezone, communication style, relevant background.

**MEMORY.md** — Curated long-term memory. Unlike `memory/*.md` (which the agent writes to), this file is meant to be human-curated. Contains the most important persistent facts.

**memory/YYYY-MM-DD.md** — Daily memory logs. The agent writes here during memory flush (before compaction or inactivity timeout). These accumulate over time and serve as the agent's long-term recall.

### Context Injection

When bootstrapping a physical session, the harness composes a system prompt from workspace files:

```
[SOUL.md content]
[IDENTITY.md content]
[AGENTS.md content]
[USER.md content]

## Long-term Memory
[MEMORY.md content]
[memory/today.md content]
[memory/yesterday.md content]

## Session Context
[Latest checkpoint, if any]
```

This composed prompt is injected via:
- Claude Code: `--system-prompt` or `--append-system-prompt-file`
- Codex CLI: written to `~/.codex/prompts/` before launch, or via app-server config
- OpenCode: custom agent config or instructions in `opencode.json`

Note: all three agents also auto-read instruction files from the working directory (Claude Code reads `CLAUDE.md`, Codex and OpenCode read `AGENTS.md`). We set the CWD to the workspace directory, so we can place an `AGENTS.md` there that all backends pick up natively. For Claude Code, we symlink `CLAUDE.md -> AGENTS.md` in the workspace.

## Backend Architecture

The three backends fall into two process models (learned from [sandbox-agent](https://github.com/rivet-dev/sandbox-agent)):

### Process Models

| Model | Backends | How it works |
|---|---|---|
| **Subprocess per turn** | Claude Code | Process spawned per message, reads JSONL from stdout. Resume via `--resume`. Stdin can stay open with `--input-format stream-json` for multi-turn within one process. |
| **Persistent server** | Codex, OpenCode | Single long-lived process. Sessions multiplexed via JSON-RPC (Codex `app-server`) or HTTP API (OpenCode `serve`). No per-turn process overhead. |

### Claude Code Backend

Claude Code is spawned as a subprocess per message exchange:

```
First message:
  claude -p "<message>" \
    --system-prompt-file /tmp/crab-context-XXXX.md \
    --output-format stream-json

Subsequent messages (same physical session):
  claude -p "<message>" \
    --resume <session_id> \
    --output-format stream-json
```

- Each response includes `session_id` in the JSON output — must be captured for `--resume`
- Events arrive as JSONL on stdout: `assistant`, `tool_use`, `tool_result`, `result`, etc.
- Token usage reported in `result` events
- Process exits after each turn; `--resume` restores conversation history on next spawn

Alternatively, Claude Code supports `--input-format stream-json` to keep stdin open for multi-turn interaction within a single process. This avoids process spawn overhead but makes error recovery harder.

### Codex CLI Backend

Codex runs as a persistent app-server process with JSON-RPC over stdio:

```
Spawn once (on first Codex session):
  codex app-server

Initialize:
  → {"jsonrpc":"2.0","id":1,"method":"initialize","params":{...}}
  ← {"jsonrpc":"2.0","id":1,"result":{...}}
  → {"jsonrpc":"2.0","method":"initialized"}

Per physical session:
  → {"jsonrpc":"2.0","id":N,"method":"thread/start","params":{"instructions":"..."}}
  ← thread_id in response

Per message:
  → {"jsonrpc":"2.0","id":N,"method":"turn/start","params":{"thread_id":"...","prompt":"..."}}
  ← Streaming notifications on stdout (item.started, item.delta, item.completed, etc.)
```

- One `codex app-server` process handles all Codex sessions concurrently via `thread_id`
- Sessions are multiplexed — no per-turn process overhead
- If the server crashes, restart it and create new threads (old threads are lost)
- Token usage reported in `turn.completed` notifications

### OpenCode Backend

OpenCode runs as a persistent HTTP server:

```
Spawn once (on first OpenCode session):
  opencode serve --port <port>

Create session:
  POST http://localhost:<port>/session
  → session_id

Send message:
  POST http://localhost:<port>/session/<session_id>/prompt (async)
  GET  http://localhost:<port>/event/subscribe (SSE stream)

Resume session:
  Same session_id — server maintains state internally
```

- Server persists across all sessions; port in range 4200-4300
- SSE event stream for real-time updates (`message.part.updated`, `session.idle`, etc.)
- Also usable via CLI: `opencode run -s <session_id> --format json "message"`
- Token usage available in assistant message `info` field

### Fallback: CLI Mode for Codex and OpenCode

If the persistent server approach has issues, both Codex and OpenCode support simple subprocess-per-turn as a fallback:

```
Codex:    codex exec "message" --json
          codex exec resume <session_id> "message" --json

OpenCode: opencode run "message" --format json
          opencode run -s <session_id> "message" --format json
```

## Unified Event Schema

All three backends emit different event formats. The harness normalizes them into a unified schema (inspired by sandbox-agent's approach):

```rust
enum CrabEvent {
    /// Agent is producing text output
    TextDelta { text: String },

    /// Agent is using a tool
    ToolUse { id: String, name: String, input: serde_json::Value },

    /// Tool produced a result
    ToolResult { id: String, output: String, is_error: bool },

    /// Turn completed with token usage
    TurnComplete { tokens: TokenUsage },

    /// Agent encountered an error
    Error { message: String },
}

struct TokenUsage {
    input_tokens: u64,
    output_tokens: u64,
    cache_read_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
}
```

When a backend doesn't emit certain events natively, the harness injects synthetic events to maintain a consistent sequence (sandwich-agent calls these "synthetic events").

## Session Lifecycle

```
                    Message arrives in #channel
                            │
                            ▼
                 ┌─ Active physical session? ─┐
                 │                            │
                Yes                           No
                 │                            │
                 ▼                            ▼
          Reset inactivity           Latest checkpoint?
          timer                       │            │
                 │                   Yes           No
                 ▼                    │            │
          Send message via            ▼            ▼
          backend.send()         Bootstrap     Bootstrap
                 │               with ckpt     fresh
                 │                    │            │
                 ▼                    └─────┬──────┘
          Parse response                   │
                 │                         ▼
                 │                  Create new physical
                 │                  session via backend
                 │                         │
                 ▼                         ▼
          Check token usage          Send message
          from TurnComplete                │
                 │                         ▼
           ┌─────┴──────┐           Parse response
           │             │                 │
        Under          Over                ▼
        budget         budget        Set inactivity timer
           │             │
           ▼             ▼
         Done      Trigger compact:
                   1. Memory flush
                   2. Checkpoint
                   3. End physical session
                   4. Bootstrap fresh
                   5. Continue
```

### Inactivity Timeout Flow

```
Timer fires (30 min idle)
        │
        ▼
  Send to agent:
  "Session ending due to inactivity.
   Save important context to memory/YYYY-MM-DD.md.
   Then produce a checkpoint summary."
        │
        ▼
  Agent writes memory/ files + returns summary
        │
        ▼
  Store checkpoint
        │
        ▼
  End physical session
        │
        ▼
  Logical session is now idle (no active physical session)
```

### Compact Flow (Token Budget Exceeded)

```
Token usage from TurnComplete exceeds threshold
        │
        ▼
  Send to agent:
  "This session is being compacted.
   1. Save any important context to memory/YYYY-MM-DD.md
   2. Produce a checkpoint summary of our conversation"
        │
        ▼
  Agent writes memory/ files + returns summary
        │
        ▼
  Store checkpoint
        │
        ▼
  End physical session
        │
        ▼
  Bootstrap fresh physical session with checkpoint
        │
        ▼
  Continue with pending message (if any)
```

## Backend Trait

```rust
#[async_trait]
trait Backend: Send + Sync {
    /// Start a new physical session.
    /// `context` contains the composed system prompt (SOUL + MEMORY + checkpoint).
    /// Returns a session handle with the backend-specific session ID.
    async fn create_session(&self, context: &SessionContext) -> Result<PhysicalSession>;

    /// Send a message in an existing physical session.
    /// Returns a stream of normalized CrabEvents.
    async fn send_message(
        &self,
        session: &mut PhysicalSession,
        message: &str,
    ) -> Result<Pin<Box<dyn Stream<Item = CrabEvent> + Send>>>;

    /// End a physical session (cleanup resources).
    /// For Claude Code: no-op (process already exited).
    /// For Codex: optionally end the thread.
    /// For OpenCode: optionally delete the session.
    async fn end_session(&self, session: &PhysicalSession) -> Result<()>;
}
```

## Discord Integration

### Bot Permissions

The bot has full admin access to the Discord server:
- Read/send messages in all channels and DMs
- Create/delete/rename channels
- Create threads
- Pin messages, set channel topics
- Manage roles (optional)
- Add reactions

### Message Routing

```
Discord message event
        │
        ▼
  Extract channel_id + author info
        │
        ▼
  Look up logical session for channel_id
  (create one if it doesn't exist)
        │
        ▼
  Route to session handler
        │
        ▼
  Agent response streamed back to Discord
  (edit message in place as chunks arrive)
```

### Discord-Specific Features

- **Streaming responses**: edit the bot's reply message as text chunks arrive from the agent
- **Long responses**: split into multiple messages if over Discord's 2000-char limit
- **Channel creation**: agent can request channel creation via tool use or explicit command
- **Thread support**: optionally map threads to sub-sessions
- **Typing indicator**: show typing while agent is processing

## Storage Layout

```
~/.crab/
├── config.toml                    # Global configuration
├── workspace/                     # Shared workspace (SOUL, MEMORY, etc.)
│   ├── SOUL.md
│   ├── IDENTITY.md
│   ├── AGENTS.md
│   ├── CLAUDE.md -> AGENTS.md     # Symlink for Claude Code compatibility
│   ├── USER.md
│   ├── MEMORY.md
│   └── memory/
│       ├── 2026-02-08.md
│       └── 2026-02-09.md
├── sessions/
│   ├── index.json                 # Maps channel_id → logical session metadata
│   └── <channel_id>/
│       ├── session.json           # Logical session state
│       ├── checkpoints/
│       │   ├── 001.md             # Checkpoint from 1st physical session
│       │   ├── 002.md             # Checkpoint from 2nd physical session
│       │   └── 003.md
│       └── transcript.jsonl       # Full message log (Discord messages + agent responses)
└── logs/
    └── crab.log
```

## Configuration

```toml
# ~/.crab/config.toml

[discord]
token = "..."                      # or via DISCORD_TOKEN env var

[defaults]
backend = "claude-code"            # default backend for new channels
inactivity_timeout_secs = 1800     # 30 minutes
max_tokens_per_physical_session = 80000

[backends.claude-code]
binary = "claude"                  # path to claude CLI
# Process model: subprocess per turn with --resume

[backends.codex]
binary = "codex"                   # path to codex CLI
# Process model: persistent app-server (JSON-RPC over stdio)

[backends.opencode]
binary = "opencode"                # path to opencode CLI
port_range = [4200, 4300]          # port range for HTTP server
# Process model: persistent HTTP server

# Per-channel overrides
[channels."project-x"]
backend = "codex"
inactivity_timeout_secs = 3600     # 1 hour for long-running projects

[channels."calendar"]
backend = "claude-code"
inactivity_timeout_secs = 900      # 15 minutes for quick tasks
```

## Open Questions

1. **Memory search**: OpenClaw uses vector embeddings + BM25 for searching memory files. Do we need this, or is injecting recent memory files (today + yesterday) sufficient to start? Could add semantic search later.

2. **Multi-user**: For now this is single-user (your personal assistant). If multiple people talk in the same channel, should each get their own session, or share the channel session?

3. **File I/O from agent**: The agent needs to write to `memory/` during flush. We set CWD to the workspace directory so the agent's file-write tools can access `memory/` directly.

4. **Agent-initiated actions**: Can the agent proactively create Discord channels, post messages, etc.? This would require exposing Discord actions as tools to the agent (via MCP or custom tool definitions).

5. **Checkpoint chaining**: Should we keep only the latest checkpoint, or maintain a chain? A chain gives richer context but costs more tokens. Could keep last N checkpoints, or summarize the chain itself.

6. **Error handling**: What happens if the CLI agent or persistent server crashes mid-session? We should detect this and either retry or start a fresh session with the last checkpoint. For persistent servers (Codex, OpenCode), we need health checks and auto-restart.

7. **Human-in-the-loop**: All three agents can ask permission questions (file writes, command execution). Should we auto-approve (like sandbox-agent's `acceptEdits` mode), or surface these as Discord messages for the user to approve?
