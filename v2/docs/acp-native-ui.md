# ACP native UI

## Decision

Use the MIT-licensed [T3 Code](https://github.com/pingdotgg/t3code) as Crab's first native UI and
maintain the integration in the [Crab fork](https://github.com/fontanierh/t3code). T3 supplies mature
web, desktop, mobile and remote clients; Crab remains the only owner of agent sessions and processes.

![T3 Code attach architecture](t3code-channel-flow.png)

The decision was validated against `pingdotgg/t3code@f6f2be3`, the Crab fork at
`fontanierh/t3code@95efc82`, and `deepseek-ai/deepseek-harness@b150a55` on 2026-08-27.

## Fit

| Candidate | Reusable strengths | Crab adaptation or mismatch |
|---|---|---|
| T3 Code | Multi-surface product, remote transport, rich tool/session UI, reusable ACP client runtime and provider drivers | The Crab provider launches an attach-only facade instead of an agent, and exposes Queue, Steer and Interrupt across web, desktop and mobile |
| DeepSeek Harness | Rich plugin UI and a tested ACP agent server | Its ACP endpoint is explicitly automation-only and its UI is coupled to the DeepSeek Harness runtime |
| ACP UI | Direct ACP client with a smaller integration surface | Less complete remote/mobile control surface than T3 Code |
| acp-components | Good typed components for a future custom client | Requires Crab to own and finish the surrounding product |

## Attach seam

The T3 Crab provider starts one lightweight `crab-v2-acp-channel` process per T3 thread. That process
speaks standard ACP over stdio, but only attaches to the single long-running Crab runtime over a
versioned local IPC transport. It must never launch or own the underlying ACP agent.

The complete attach slice is implemented. Crab supplies the authenticated local Boxology transport
and ACP stdio facade; the Crab fork of T3 ships a built-in provider across web, desktop and mobile.
The tested compatibility point is `fontanierh/t3code@95efc82`, which contains the Crab delivery
controls and upstream `pingdotgg/t3code@f6f2be3`. The sync passed 92 focused Crab/UI tests plus
server, web, client-runtime and scripts typechecks.

![ACP stdio facade flow](acp-channel-facade-flow.png)

| T3 / ACP operation | Crab operation |
|---|---|
| `session/new`, `session/load`, `session/resume` | Create or attach one durable native-channel binding |
| `session/prompt` in queue mode | `accept_turn(Queue)` |
| `session/prompt` in steer mode | `accept_turn(Steer)` |
| `session/cancel` | `interrupt_and_drain` |
| `session/update` | Ordered replay, validated or projected into ACP v1 with facade session IDs |

T3 launches:

```text
crab-v2-acp-channel
├── --state-dir <Crab state>
├── --agent <configured agent ID>
├── --adapter t3code
└── --bootstrap-file <optional prompt>
```

The facade advertises the `crab-local` ACP authentication method; the actual credential remains the
owner-only IPC token loaded inside the process. Prompts default to queue mode. The T3 provider can
set `_meta.crab.inputMode` to `queue` or `steer`, and `_meta.crab.turnId` to a durable idempotency
key. Interrupt remains the standard `session/cancel` action.

Each facade process admits at most 128 live sessions and rejects loaded session IDs above 256 bytes.
Attachment is serialized by session ID, so concurrent retries reuse one binding and one event pump;
unrelated sessions still attach concurrently. Admission is fail-fast, and the session owns its slot
until the ACP connection drops. Weak per-ID locks are pruned after use.

![ACP facade session admission](acp-facade-session-admission.png)

Queue and steer must be separate UI actions or an explicit composer mode. Interrupt remains a
separate action. The proxy may rewrite transport-local request and session IDs, but it must retain
every native agent update needed to render thoughts, plans, tools, terminals, diffs, usage and
compaction.

The native-channel journal remains the lossless source of truth. On the ACP v1 stdio connection,
the facade forwards validated v1 updates and uses ACP's explicit conversion layer for representable
v2 updates. V2-only lifecycle and terminal variants stay in the journal instead of being emitted as
invalid v1 notifications; foreground completion is returned by the v1 `session/prompt` response.

Crab owns the underlying agent and its tool authority. The facade rejects client-supplied MCP
servers, the T3 provider sends `mcpServers: []`, and T3's per-thread MCP bearer credential never
crosses the attach seam. [Crab #206](https://github.com/fontanierh/crab/issues/206) records the
delivered vertical slice.
