# Crab - Design Document

Rust harness that runs coding agents (Claude Code, Codex CLI, OpenCode) behind a Discord bot.

This document defines the v1 architecture and operational semantics.

Implementation breakdown is tracked in `crab/WORKSTREAMS.md`.
Companion deep-dive architecture docs are tracked in `crab/docs/README.md`.

## 1) Goals and Non-Goals

### Goals

- Provide one autonomous Discord-first coding agent runtime.
- Support multiple backend agents behind one unified runtime contract.
- Keep behavior reliable under long-running autonomous operation.
- Preserve high-quality context through explicit memory and checkpointing.
- Keep delivery semantics simple and non-buggy (idempotent, recoverable, observable).

### Non-Goals (v1)

- Multi-agent orchestration or parallel specialist swarms.
- Complex permission workflows in Discord (default is autonomous operation).
- Dependency/security gates and branch protection policy enforcement (deferred).
- Pi agent integration unless it exposes a stable service interface we can adapt cleanly.

## 2) Core Principles

- Autonomy first: default execution policy is auto-approve.
- Deterministic control plane: explicit state machine for sessions, runs, and resets.
- Durable events: every run is reconstructible from persisted envelopes.
- Explicit memory lifecycle: memory flush and checkpoint are separate protocol steps.
- Trusted workspace model: users are trusted, but memory is still scoped per user.

## 3) High-Level Architecture

```
Discord Gateway
  -> Session Router
  -> Lane Scheduler
  -> Backend Adapter (Claude | Codex | OpenCode)
  -> Event Log + Transcript Store
  -> Checkpoint Store
  -> Memory Files + Memory CLI Commands
```

Major subsystems:

- Discord Bot: ingress, streaming edits, commands, server management.
- Session Router: maps Discord contexts to logical sessions.
- Lane Scheduler: per-session FIFO + global concurrency cap.
- Backend Manager: lifecycle for subprocess/persistent backends.
- Context Builder: workspace instructions + memory + checkpoint injection.
- State Store: sessions, checkpoints, events, run metadata.

Quality engineering workflow:

- Strict merge gate remains `make quality` with hard coverage requirements (100% lines/functions,
  0 uncovered lines/functions).
- Developer fast lane is `make quick` (non-gating) for iteration speed.
- Coverage regression triage is standardized via `make coverage-diagnostics`.

## 4) Session and Lane Model

### 4.1 Logical Session

A logical session is the durable conversation identity.

Default mapping:

- Guild channel: `discord:channel:<channel_id>`
- Thread: `discord:thread:<thread_id>`
- DM: `discord:dm:<user_id>`

Each logical session stores:

- Active backend type.
- Active inference profile (`model`, `reasoning_level`).
- Active physical session handle (optional).
- Last successful checkpoint id.
- Queue/lane state.
- Last activity timestamp.
- Token/accounting summaries.

### 4.2 Physical Session

Backend-specific execution state reused across turns until rotation/reset:

- Claude Code: resumable session id.
- Codex CLI: thread id on one `codex app-server`.
- OpenCode: server-side session id.

### 4.3 Lane Model (Queue Semantics)

Each logical session has exactly one lane.

Rules:

- Per-lane execution is strictly FIFO.
- At most one active run per lane.
- Global scheduler enforces `max_concurrent_lanes` across all lanes.
- New messages enqueue on the lane (bounded queue).
- On overflow: reject newest message with explicit error note (no silent drops).

Lane state machine:

- `Idle`: no active run.
- `Running`: one active run streaming events.
- `Cancelling`: interruption requested, awaiting backend confirmation/timeout.
- `Rotating`: flush/checkpoint/session handoff in progress.

This gives deterministic ordering while still allowing cross-channel parallelism.

## 5) Backend Integration

### 5.1 Unified Backend Contract

```rust
#[async_trait]
trait Backend: Send + Sync {
    async fn create_session(&self, ctx: &SessionContext) -> Result<PhysicalSession>;

    async fn send_turn(
        &self,
        session: &mut PhysicalSession,
        input: TurnInput,
    ) -> Result<Pin<Box<dyn Stream<Item = BackendEvent> + Send>>>;

    async fn interrupt_turn(
        &self,
        session: &PhysicalSession,
        turn_id: &str,
    ) -> Result<()>;

    async fn end_session(&self, session: &PhysicalSession) -> Result<()>;
}
```

All backend streams are normalized into Crab event envelopes (section 9).

### 5.2 Claude Code

Model:

- Subprocess per turn with resume id.
- Stateless process lifecycle; state sits in Claude session id.

Notes:

- Best-effort interruption uses process termination semantics.
- Recovery after crash is simple: spawn next turn with last known resume id; if invalid, rotate physical session from checkpoint.

### 5.3 Codex CLI (validated on 2026-02-09, `codex-cli 0.98.0`)

Model:

- One persistent `codex app-server` process.
- Multiple sessions via `threadId`.

Key request methods:

- `thread/start`
- `thread/resume`
- `turn/start` with required `threadId` and `input` (array of user inputs)
- `turn/interrupt` with required `threadId` and `turnId`

Important server-side requests/events:

- Approval requests (command/file change): `item/*/requestApproval`
- User input request (experimental): `item/tool/requestUserInput`

Crab behavior:

- Default policy: auto-approve locally; still log approval request envelopes.
- `item/tool/requestUserInput` is handled as unattended mode by default (reject with a deterministic message, emit event, continue/fail the run according to backend response).
- If app-server dies: restart process, create/resume thread where possible; if not possible, rotate physical session using latest checkpoint.

### 5.4 OpenCode

Model:

- Persistent HTTP server.
- Session ids managed by OpenCode service.
- Streaming via server event stream.

Crab behavior:

- Keep one managed OpenCode server process.
- Health-check and restart if unreachable.
- On unrecoverable session loss, rotate physical session from latest checkpoint.

### 5.5 Model Selection and Reasoning Policy

Crab resolves an inference profile for every run:

- `backend`
- `model`
- `reasoning_level`

Canonical `reasoning_level` values:

- `none`
- `minimal`
- `low`
- `medium`
- `high`
- `xhigh`

Resolution precedence (highest first):

1. Per-turn override
2. Session profile
3. Channel override
4. Backend default
5. Global default

Validation and fallback policy:

- `strict`: reject the run if model/reasoning is unsupported for the selected backend.
- `compatible` (default): choose the nearest compatible backend setting, emit a run note, and persist the fallback in run metadata.

Backend mapping rules:

- Claude Code:
  - `model`: resolved to a backend-supported Claude model alias.
  - `reasoning_level`: mapped by adapter to Claude-supported thinking mode; unsupported values are clamped under fallback policy.
- Codex CLI:
  - `model`: sent via `turn/start` model override when provided.
  - `reasoning_level`: mapped directly to `turn/start.effort` (`none|minimal|low|medium|high|xhigh`).
- OpenCode:
  - `model`: applied through OpenCode session/turn model controls.
  - `reasoning_level`: mapped directly to OpenCode native reasoning controls on session/turn config.

Profile stickiness:

- The resolved profile is sticky across physical session rotations inside the same logical session.
- It changes only via explicit command/override or config reload.

Operator controls:

- `/backend <name>`
- `/model <name-or-auto>`
- `/reasoning <none|minimal|low|medium|high|xhigh>`
- `/profile` (show effective resolved profile and source)

### 5.6 Memory Access Surface (Prompt + CLI)

Crab uses a prompt-first memory workflow and does not register backend-specific custom memory tools.

Memory access is exposed as CLI commands available to the agent runtime:

- `crab-memory-search` for ranked keyword + recency recall across `MEMORY.md` and `memory/`.
- `crab-memory-get` for exact line-range retrieval from allowed memory paths.
- Native file search/read fallback (`rg`/`grep` + direct file reads over `memory/`).

Rules:

- Keep the command surface uniform across Claude/Codex/OpenCode.
- Keep command behavior deterministic and path-safe.
- Defer embedding/vector semantic search in v1; keyword + curated memory is the default.

## 6) Workspace, Context, and Memory

### 6.1 Workspace

Single shared workspace:

```
~/.crab/workspace/
  SOUL.md
  IDENTITY.md
  AGENTS.md
  CLAUDE.md -> AGENTS.md
  USER.md
  MEMORY.md
  .agents/
    skills/
      skill-authoring-policy/
        SKILL.md
  .claude/
    skills -> ../.agents/skills
  memory/
    global/YYYY-MM-DD.md
    users/<discord_user_id>/YYYY-MM-DD.md
```

### 6.2 Injection Order

Crab composes bootstrap context only when a physical session is first materialized
(`last_turn_id == None`). Reused physical sessions receive raw user input only.

Bootstrap context order:

1. `SOUL.md`
2. `IDENTITY.md`
3. `USER.md`
4. `MEMORY.md` (curated long-term)
5. Memory snippets/files (global + current author scope)
6. Latest checkpoint summary
7. `CRAB_RUNTIME_BRIEF` (runtime/framework orientation for the backend session)
8. Prompt contract (`RUNTIME_PROFILE`, memory policy, skills governance, owner/runtime/messaging notes)
9. Current turn input

Rendered context envelope uses XML boundaries so user input and Crab-provided context are explicit:

- `<crab_turn_context>` as root
- `<crab_system_context>` for Crab-managed context sections
- `<crab_user_input>` for user turn payload (`<turn_input>`)
- section bodies are wrapped in CDATA

`AGENTS.md` is not inline-injected; backend runtimes load it natively from workspace.

Managed-doc token budgets are strict and non-truncating:

- `SOUL.md`: 2048 tokens
- `IDENTITY.md`: 2048 tokens
- `USER.md`: 2048 tokens
- `MEMORY.md`: 16000 tokens
- `CRAB_RUNTIME_BRIEF`: 1024 tokens
- `PROMPT_CONTRACT`: 4096 tokens
- `LATEST_CHECKPOINT`: 4096 tokens
- `TURN_INPUT`: 4096 tokens

If a section exceeds its budget, the turn is rejected with an explicit invariant error; Crab does
not silently truncate context.

### 6.3 Memory Scope Policy

Trust model: server participants are trusted.

Memory policy:

- Use per-user memory files by default (`memory/users/<author_id>/...`).
- Use global memory for channel-wide durable facts.
- Inject current author memory + recent global memory for the turn.

### 6.4 Memory Recall (Prompt + CLI)

Crab memory recall is prompt-driven and CLI-executed.

Prompt rules in system instructions:

- Before claiming information is unknown, search `MEMORY.md` and `memory/`.
- Prefer `crab-memory-search` first, then `crab-memory-get` for exact citations.
- If needed, run direct `rg`/`grep` and file reads over memory files.

Citation/disclosure policy:

- `citation_mode=auto` (default):
  - Direct messages: include citations (`path#Lx` or `path#Lx-Ly`) when memory materially influences the answer.
  - Shared Discord contexts: suppress file/line citations by default.
  - Shared non-owner runs: add explicit disclosure sentence when memory influenced the answer.
- `citation_mode=on`: always include citations, including shared contexts.
- `citation_mode=off`: never include file/line citations and do not add disclosure text.
- Policy applies equally to `crab-memory-search`, `crab-memory-get`, and native `rg`/`grep`/reads over memory files.

### 6.5 Skills Governance

Crab enforces one canonical skills root across backends:

- Canonical root: `.agents/skills`.
- Claude compatibility path: `.claude/skills -> ../.agents/skills`.
- Built-in required policy skill:
  - `.agents/skills/skill-authoring-policy/SKILL.md`
  - enforces that new/updated skills must live under `.agents/skills`.

Prompt contract includes a dedicated `SKILLS_GOVERNANCE` section before owner/runtime notes so
skill-authoring tasks consistently follow this policy.

## 7) Turn Lifecycle

1. Ingress: Discord message arrives.
2. Route: resolve logical session + lane.
3. Enqueue: append run request to lane FIFO.
4. Dequeue: scheduler starts run when lane head and global capacity available.
5. Resolve effective inference profile (`backend`, `model`, `reasoning_level`).
6. Ensure physical session exists (create or resume).
7. Build bootstrap context only for new physical sessions; otherwise use raw turn input.
8. Start backend turn.
9. Stream backend events -> normalize -> persist -> deliver.
10. Finalize run:
   - update usage and last activity
   - evaluate compaction/inactivity/reset rules
   - ack completion in lane

## 8) Memory Flush and Checkpoint Protocol (Explicit Spec)

Memory flush and checkpoint are separate hidden operations.

They run in this order whenever a rotation/reset requires state handoff.

### 8.1 Trigger Conditions

Run flush+checkpoint when:

- Token budget compaction threshold exceeded.
- Inactivity timeout expires and lane is idle.
- Manual reset/compact command.

Token budget source:

- Prefer backend-reported usage.
- Fall back to local token estimation when a backend does not provide usage for a completed turn.

### 8.2 Step A: Memory Flush Turn (hidden)

Purpose: persist durable facts to memory files.

Protocol:

- Execute hidden turn with flush instructions.
- Agent may write memory files using normal shell/file-edit primitives.
- Expected text output is one of:
  - `NO_REPLY`
  - `MEMORY_FLUSH_DONE`

Harness behavior:

- Output is not posted to Discord.
- Timeout/failure does not block checkpoint step; it is logged.
- Only one flush per compaction cycle (`memory_flush_cycle_id`).

### 8.3 Step B: Checkpoint Turn (hidden)

Purpose: produce resumable handoff summary.

Expected output: strict JSON object

```json
{
  "summary": "string",
  "decisions": ["string"],
  "open_questions": ["string"],
  "next_actions": ["string"],
  "artifacts": [{"path": "string", "note": "string"}]
}
```

Harness behavior:

- Validate JSON schema.
- On parse/validation failure: retry once with corrective prompt.
- If still invalid: write fallback checkpoint generated from transcript tail + durable metadata.

Checkpoint storage:

- Persist every checkpoint (append-only).
- Inject only latest checkpoint by default into new physical sessions.

## 9) Event Envelope and Persistence

Every normalized event is stored as an envelope.

```rust
struct EventEnvelope {
    event_id: String,            // ulid
    ts_unix_ms: u64,
    run_id: String,
    turn_id: String,
    lane_id: String,
    logical_session_id: String,
    physical_session_id: Option<String>,
    backend: BackendKind,
    resolved_model: Option<String>,
    resolved_reasoning_level: Option<String>,
    profile_source: Option<String>, // turn | session | channel | backend_default | global_default | fallback
    seq: u64,                    // per-run monotonic sequence
    kind: EventKind,
    payload: serde_json::Value,
}
```

Why this is required:

- Replay UI updates after restart.
- Deterministic dedupe for outbound edits/posts.
- Accurate audit trail for approvals, tool calls, and cancellations.
- Crash recovery from partially completed runs.

### 9.1 Event Kinds

Minimum set:

- `run.started`
- `turn.started`
- `text.delta`
- `tool.call`
- `tool.result`
- `approval.requested`
- `turn.completed`
- `turn.interrupted`
- `checkpoint.created`
- `run.failed`

## 10) Discord Delivery Semantics

### 10.1 Normal Reply vs Discord Tool

Default behavior:

- Normal assistant output is delivered by Crab as the channel reply stream.

`discord` tool behavior:

- Used for explicit Discord actions (send/edit/delete/react/moderation/proactive ops).

Duplicate suppression:

- If a messaging tool sends to the same provider+target for the same run, suppress duplicate final plain-text confirmation reply.

### 10.2 Idempotent Message Delivery and Editing

For each run, Crab tracks outbound records keyed by:

- `(logical_session_id, turn_id, chunk_index)`

Rules:

- Prefer editing one in-flight bot message while streaming.
- If message length or format constraints require split, create deterministic chunk indices.
- Before sending/editing chunk `n`, check if record exists with same content hash.
  - If yes: skip.
  - If no: apply edit/post, await delivery confirmation, then persist record.
- On Discord API errors that invalidate message id (deleted/not found), create a replacement message and remap current stream target.

## 11) Cancellation Semantics

### 11.1 User-Facing Cancellation

`/cancel` applies to active lane run only.

Behavior:

- Mark run state `Cancelling`.
- Send backend-specific interrupt:
  - Claude: terminate process.
  - Codex: `turn/interrupt` with `threadId`, `turnId`.
  - OpenCode: server cancel endpoint/stream interruption.
- Stop streaming to Discord.
- Persist `turn.interrupted` envelope.
- Leave physical session alive unless backend integrity requires rotation.

### 11.2 Queue Cancellation

- Pending (not started) run can be removed by id.
- FIFO ordering for remaining runs is preserved.

## 12) Reset and Rotation Policy

A session rotates physical state when a trigger from section 8.1 fires, plus backend switches.

Rotation sequence:

1. Memory flush (hidden)
2. Checkpoint (hidden)
3. End physical session
4. Clear active physical handle
5. Next run bootstraps fresh physical session with latest checkpoint

## 13) Crash Recovery

### 13.1 Durable State Before Side Effects

Before starting backend turn:

- Persist `run.started` with lane/session ids.
- Persist run metadata (backend handle, resolved profile, input hash, delivery target).

### 13.2 Startup Reconciliation

On process boot:

- Load lanes and find runs in `Running`/`Cancelling` older than grace window.
- Mark them `RecoveredAsInterrupted`.
- Emit synthetic interruption/failure envelopes.
- Reconcile backend managers:
  - restart Codex/OpenCode services if dead.
  - treat stale backend handles as invalid.
- Keep logical session; force new physical session on next user message.

### 13.3 Delivery Recovery

Because outbound records are persisted, replay can continue without duplicate spam:

- Already committed chunks are not resent.
- Incomplete streams resume from next unsent chunk or finalize with an interruption note.

## 14) Heartbeat and Health

Crab runs three heartbeat loops:

- Run heartbeat: updates `last_progress_at` while a turn streams.
- Backend heartbeat: verifies persistent services (Codex/OpenCode) on interval.
- Dispatcher heartbeat: checks lane scheduler forward progress.

Stall policy:

- If active run has no progress beyond `run_stall_timeout_secs`, attempt cancellation.
- If cancellation fails, hard-stop backend handle, mark run failed, and rotate physical session.

Heartbeat events are internal envelopes, not user-facing chat output.

## 15) Discord Permissions

Crab assumes full server ownership for the main bot.

Required practical scope includes:

- Read/send/edit messages
- Manage channels/threads/topics
- Reactions
- Role and moderation actions

This is intentional for autonomous server configuration and operations.

## 16) Storage Layout

```
~/.crab/
  config.toml
  workspace/
    SOUL.md
    IDENTITY.md
    AGENTS.md
    CLAUDE.md -> AGENTS.md
    USER.md
    MEMORY.md
    .agents/
      skills/
        skill-authoring-policy/
          SKILL.md
    .claude/
      skills -> ../.agents/skills
    memory/
      global/
        YYYY-MM-DD.md
      users/
        <discord_user_id>/
          YYYY-MM-DD.md
  sessions/
    index.json
    <logical_session_id>/
      session.json
      checkpoints/
        000001.json
        000002.json
      events.jsonl
      outbound.jsonl
  backends/
    codex/
      manager_state.json
    opencode/
      manager_state.json
  logs/
    crab.log
```

## 17) Configuration (Draft)

```toml
[discord]
token = "${DISCORD_TOKEN}"

[defaults]
backend = "codex"
inactivity_timeout_secs = 1800
max_concurrent_lanes = 6
lane_queue_limit = 32
compaction_token_threshold = 120000
startup_reconciliation_grace_period_secs = 90
heartbeat_interval_secs = 10
run_stall_timeout_secs = 90
backend_stall_timeout_secs = 30
dispatcher_stall_timeout_secs = 20

[defaults.inference]
model = "auto"
reasoning_level = "medium"
fallback_policy = "compatible"   # strict | compatible

[approvals]
default_policy = "auto"   # auto | ask

[backends.claude]
binary = "claude"
[backends.claude.inference]
default_model = "auto"
supported_reasoning_levels = ["none", "low", "medium", "high"]

[backends.codex]
binary = "codex"
mode = "app-server"
[backends.codex.inference]
default_model = "auto"

[backends.opencode]
binary = "opencode"
serve_port = 4210
[backends.opencode.inference]
default_model = "auto"
# reasoning_level is always mapped through native OpenCode session/turn config

[memory]
access_mode = "cli"             # cli (v1)
search_mode = "keyword"         # keyword (v1); semantic deferred
citation_mode = "auto"          # auto | on | off
inject_days = 2
per_user_scope = true

[workspace.git_persistence]
enabled = false
remote = "git@github.com:owner/private-crab-workspace.git"   # required for on-commit push policy
branch = "main"
commit_name = "Crab Workspace Bot"
commit_email = "crab@localhost"
push_policy = "on-commit"        # on-commit | manual

[channels."discord:channel:1234567890"]
backend = "claude"
model = "auto"
reasoning_level = "high"
inactivity_timeout_secs = 3600
```

Workspace git bootstrap policy (implemented in WS21-T2):

- When enabled, startup may initialize a repository only at the workspace root.
- If workspace is nested inside a different repository, startup fails fast to avoid mutating external paths.
- Branch bootstrap is deterministic for empty repos (`HEAD` is set to configured `branch`).
- `origin` remote is validated/bound against configured remote when provided.

Workspace git commit policy (implemented in WS21-T3):

- Commit attempts are triggered from turn finalization after successful runs.
- Rotation runs attempt a `rotation_checkpoint` commit first, then a `run_finalized` commit.
  The second attempt may intentionally no-op when no additional filesystem changes exist.
- Commit metadata schema is standardized through trailers:
  - `Crab-Commit-Version`
  - `Crab-Trigger`
  - `Crab-Logical-Session-Id`
  - `Crab-Run-Id`
  - `Crab-Checkpoint-Id`
  - `Crab-Run-Status`
  - `Crab-Emitted-At-Epoch-Ms`
  - `Crab-Commit-Key`
- Replay safety uses deterministic commit keys. If HEAD already contains the same
  key and there are no pending workspace changes, Crab marks the commit attempt as
  already persisted and does not write a duplicate commit.

Workspace git async push policy (implemented in WS21-T4):

- When `workspace.git_persistence.push_policy = "on-commit"`, Crab enqueues push intents
  after successful commit persistence instead of blocking run completion.
- Queue state is durable at `state/workspace_git_push_queue.json` and survives process restarts.
- Queue entries are idempotent by commit key:
  - same `commit_key` + same `commit_id` => dedupe/no-op
  - same `commit_key` + different `commit_id` => invariant violation
- Daemon loop runs one due push attempt per tick:
  - success removes the entry
  - failure records attempt count and schedules bounded exponential backoff
  - attempts are capped and then marked exhausted (non-fatal to runtime flow)
- Push processing failures are non-blocking for ingress/dispatch and heartbeat flow;
  runtime liveness is preserved even under sustained remote outages.

State schema evolution policy (implemented in WS22):

- Persisted runtime state carries a global marker at `workspace/state/schema_version.json`:
  - `version`
  - `updated_at_epoch_ms`
- Startup runs a migration engine before runtime stores/process loops:
  - deterministic stepwise `vN -> vN+1`
  - migration lock (`schema_migration.lock.json`) to prevent concurrent migrators
  - idempotent rerun behavior
  - explicit failure on missing migration step
- `crabctl upgrade` and `crabctl doctor` run compatibility preflight against supported version
  range and emit actionable remediation commands when incompatible.
- Contributor rule: schema changes should be additive-only unless paired with a migration step.

## 18) Deferred Items

- Branch protection and required CI checks.
- Dependency/security policy gates (`cargo deny` / `cargo audit`).
- Multi-agent orchestration.
- Pi agent adapter (only if stable transport contract is available).
