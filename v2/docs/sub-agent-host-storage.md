# Sub-agent host state and process contract

`sub-agent-host` gives Crab one portable sub-agent model across ACP harnesses. Each child is a new
`agent-host` session, which owns a separate ACP process; parent and child continue independently.

![Sub-agent host flow](sub-agent-host-flow.png)

## Context modes

| Requested mode | Current realization | Guarantee |
|---|---|---|
| Fresh | `FreshSession` | Only explicit child bootstrap metadata and task are sent. |
| Inherit parent | `NativeAcpFork` | At the exact idle head, an advertised native fork preserves opaque agent context in a separately supervised process. |
| Inherit parent fallback | `PortableSnapshot` | When explicitly allowed, message events through an immutable parent cursor are injected, capped at 4 MiB. |

Native inheritance requires the same agent implementation, an advertised draft `session/fork`
capability, and an unchanged idle parent cursor. Portable inheritance preserves the visible
conversation, including exact native message JSON and direction, but does not claim hidden
model/provider state. If native fork is unavailable and the caller forbids the portable snapshot,
spawn fails closed.

## Durable layout

```text
runtime-state/
└── sub-agent-host.sqlite
    ├── sub_agents       identity, context realization, lifecycle and child cursor
    ├── interactions     bidirectional idempotent delivery state
    ├── events           ordered local lifecycle, interaction and ACP journal
    └── native_events    exact child-sequence deduplication
```

- SQLite schema v1 uses WAL, foreign keys, full synchronous writes and fail-closed version checks.
- Spawn and message retries use caller IDs plus canonical request fingerprints; changed retries are
  rejected instead of silently reusing prior work.
- Initial tasks and parent-to-child native prompts are capped at 2 MiB before parsing or durable
  interaction storage. Generated child-to-parent prompts obey the same boundary.
- Parent-to-child and child-to-parent sends support queue, steer and actor-serialized
  cancel-then-queue. The interrupting input is accepted before completion can drain older work;
  delivery never waits for model completion.
- A background cursor pump copies every child ACP event, including tool calls and agent-owned
  compaction events, into the sub-agent journal without narrowing the native JSON. Natural
  completion removes the token-matched task entry; a stale pump cannot remove its replacement, and
  explicit stop or host drop removes then aborts the current task.
- On reopen, active records become explicit recovery candidates and transport-ambiguous pending
  interactions become failed so they can be retried with the same caller ID. Delivered
  interactions, the ordered event journal and the exact child ACP cursor remain unchanged.
- Startup recovers configured parent sessions first, then reconciles each child within its durable
  `crash_restart_limit`. Recovery accepts only the same Crab child ID, native ACP ID and agent; it
  increments `restart_count` and restarts the event pump from the stored cursor.
- Recovery never opens a replacement child, replays inherited/bootstrap context or resends the
  initial task. Disabled or exhausted budgets, unavailable parents/sessions, identity drift and
  hard failures remain inspectable as `Failed`. See the
  [rendered recovery flow](sub-agent-recovery-flow.png) and
  [rendered pump lifecycle](sub-agent-pump-lifecycle.png).
