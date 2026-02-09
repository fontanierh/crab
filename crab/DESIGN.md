# Crab - Design Document

A meta-harness that wraps CLI coding agents (Claude Code, Codex CLI) and exposes them through Discord. Inspired by [OpenClaw](https://github.com/openclaw/openclaw)'s architecture for memory, persona, and session management — but built in Rust, and backed by existing CLI agents rather than raw LLM APIs.

## Architecture Overview

```
Discord Server (primary UI)
    │
    ├── #project-x    ──┐
    ├── #calendar       │  each channel/DM = one logical session
    ├── DM: @user      ──┘
    │
    ▼
┌──────────────────────────────────────────────┐
│              Crab (Rust)                     │
│                                              │
│  Discord Bot                                 │
│    ├── Message listener (all channels/DMs)   │
│    ├── Server management (create channels,   │
│    │   pins, topics, threads)                │
│    └── Full admin permissions                │
│                                              │
│  Session Router                              │
│    ├── Maps Discord channel → logical session│
│    └── Manages physical session lifecycle    │
│                                              │
│  Workspace                                   │
│    ├── SOUL.md                               │
│    ├── IDENTITY.md                           │
│    ├── AGENTS.md                             │
│    ├── MEMORY.md                             │
│    ├── USER.md                               │
│    └── memory/                               │
│         └── YYYY-MM-DD.md                    │
│                                              │
│  Backend Manager                             │
│    ├── Claude Code backend                   │
│    └── Codex CLI backend                     │
└──────────────────────────────────────────────┘
```

## Core Concepts

### Logical Session

A logical session is the ongoing conversation in a Discord channel or DM. It lives as long as the channel exists. It holds:

- A reference to the Discord channel ID
- An ordered list of checkpoints (summaries of past physical sessions)
- The currently active physical session (if any)
- An inactivity timer
- The backend to use (Claude Code or Codex, configurable per channel)

### Physical Session

A physical session is a single CLI agent invocation. It's short-lived and gets recycled. It maps to:

- Claude Code: a `--resume`-able session identified by `session_id`
- Codex CLI: a session in `~/.codex/sessions/`

A new physical session is spawned when:

1. A message arrives and no active physical session exists
2. Token budget is getting large (heuristic: turn count or estimated tokens)
3. Backend switch (Claude Code <-> Codex)
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

When bootstrapping a physical session, the harness composes a system prompt from these files:

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
- Codex CLI: written to `~/.codex/prompts/` before launch

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
          --resume $sid          Bootstrap     Bootstrap
                 │               with ckpt     fresh
                 │                    │            │
                 ▼                    └─────┬──────┘
          Parse response                   │
                 │                         ▼
                 │                  Spawn new physical
                 │                  session (CLI)
                 │                         │
                 ▼                         ▼
          Check turn count /         Send message
          token estimate                   │
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
                   3. Kill session
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
  Kill physical session
        │
        ▼
  Logical session is now idle (no active physical session)
```

### Compact Flow (Token Budget Exceeded)

```
Turn count or token estimate exceeds threshold
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
  Kill physical session
        │
        ▼
  Bootstrap fresh physical session with checkpoint
        │
        ▼
  Continue with pending message (if any)
```

## Backend Trait

```rust
trait Backend {
    /// Start a new physical session, returning a handle.
    /// `context` contains the composed system prompt (SOUL + MEMORY + checkpoint).
    async fn create_session(&self, context: SessionContext) -> Result<PhysicalSession>;

    /// Send a message in an existing physical session.
    /// Returns a stream of events (text chunks, tool use, errors).
    async fn send_message(
        &self,
        session: &mut PhysicalSession,
        message: &str,
    ) -> Result<EventStream>;

    /// Resume a previously created physical session.
    async fn resume_session(&self, session: &PhysicalSession) -> Result<()>;
}
```

### Claude Code Backend

```
Spawn: claude -p "<message>" --system-prompt "<context>" --output-format stream-json
Resume: claude -p "<message>" --resume <session_id> --output-format stream-json
```

- Parse `session_id` from each JSON response to track for `--resume`
- Events arrive as JSONL on stdout

### Codex CLI Backend

```
Spawn: codex exec "<message>" --json
Resume: codex exec resume <session_id> "<message>" --json
```

- Session ID must be read from `~/.codex/sessions/`
- Events arrive as JSONL on stdout

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
max_turns_per_physical_session = 20
# max_tokens_per_physical_session = 80000  # alternative to turn count

[backends.claude-code]
binary = "claude"                  # path to claude CLI
default_flags = ["--output-format", "stream-json"]

[backends.codex]
binary = "codex"                   # path to codex CLI
default_flags = ["--json"]

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

2. **Token counting**: We don't have direct visibility into the agent's context window. Turn count is a rough proxy. We could parse the JSONL events for token usage info (both Claude Code and Codex report this in their event streams).

3. **Multi-user**: For now this is single-user (your personal assistant). If multiple people talk in the same channel, should each get their own session, or share the channel session?

4. **File I/O from agent**: The agent needs to write to `memory/` during flush. Both Claude Code and Codex have file-write tools, but they operate in the CWD. We'd need to set the CWD to the workspace, or give the agent explicit paths.

5. **Agent-initiated actions**: Can the agent proactively create Discord channels, post messages, etc.? This would require exposing Discord actions as tools to the agent (via MCP or custom tool definitions).

6. **Checkpoint chaining**: Should we keep only the latest checkpoint, or maintain a chain? A chain gives richer context but costs more tokens. Could keep last N checkpoints, or summarize the chain itself.

7. **Error handling**: What happens if the CLI agent crashes mid-session? We should detect this and either retry or start a fresh session with the last checkpoint.
