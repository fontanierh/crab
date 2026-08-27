# Crab v2

Crab v2 is a small, ACP-native host. Boxology contracts own its domain boundaries; the executable
composition restores configured sessions and continuously routes durable ingress.

![Crab v2 architecture](docs/crab-v2-architecture.png)

```text
v2/
├── agent-host/       ACP lifecycle, native event stream, mandatory host authority
├── native-channel/   one channel ↔ one ACP session, with the full native view
├── channel-gateway/  idempotent UI attachment and physical-session recovery
├── bridge-host/      supervised external integrations, auth and selected delivery
├── runtime-control/  immutable process and semantic-config attestation
├── sub-agent-host/   supervised ACP subprocesses with bidirectional live interaction
├── trigger-inbox/    transactional SQLite ingress used by bridges, cron and self-work
├── turn-router/      durable target resolution, lane ordering and trigger settlement
└── runtime/          strict topology, restart recovery and lane supervision
```

| | Channel | Bridge |
|---|---|---|
| Purpose | Native agent interface | Communicate with an external system |
| Output | Every ordered ACP event, including tool activity | Explicitly selected outbound messages |
| Session model | Exactly one live ACP session | May address many channels/sessions |
| Ingress | User turns | Durable Crab triggers |

## Input modes

| Mode | Active work |
|---|---|
| Queue | Wait for idle; preserve FIFO |
| Steer | Contribute immediately when negotiated; never silently interrupt |
| Interrupt and steer | Cooperative ACP cancel, then drain accepted input immediately |

Bridges select one mode when registered. Native channels select queue/steer per input and expose
interrupt as a separate explicit action.

## Deliberate constraints

- ACP owns compaction. Crab preserves draft compaction lifecycle events when available but has no
  `compact` operation because ACP does not define one.
- ACP v2 draft makes `session/prompt` non-blocking and allows new input during active work. ACP v1
  queues by default; configured adapters may negotiate `_session/steering`. Crab's Claude preset
  and Codex compatibility preset enable it and retain ownership when a racing turn is already idle.
- Crab owns sub-agents as separately supervised ACP subprocesses. Fresh sessions are portable;
  inherited sessions prefer advertised ACP native forks at an exact idle cursor and may explicitly
  fall back to a visible-history snapshot. Parent and child exchange durable non-blocking queue,
  steer or interrupt messages in both directions.
- Agents run only after a fail-closed preflight proves permission bypass, no sandbox, unrestricted
  filesystem/network access and working passwordless `sudo`. Required ACP session options are then
  applied and verified before readiness. The Claude preset requires `bypassPermissions` and Opus;
  the Codex preset requires `agent-full-access`, GPT-5.6-Sol and high reasoning. Their first-party
  macOS probes actively verify the host, exact adapter and network conditions rather than trusting
  configuration claims.
- Bridges are packages the agent may add. Crab owns supervision, auth state, health and delivery
  semantics—not service-specific behavior. WhatsApp ships as the first first-party package.
- Tests target useful contract and composition behavior. There is no percentage coverage gate.
- `agent-host` runs real ACP v1/v2 subprocesses with mandatory authority preflight, durable
  prompts/events/permissions, queue/steer/cancel, explicit native resume, and process-group
  shutdown. Recovery preserves both session IDs and the event cursor while revalidating authority,
  MCP tools and required policy; it never replays bootstrap or owns compaction. Graceful runtime
  shutdown detaches the host-owned live set without native close, while explicit close remains
  destructive. ACP stdout frames are rejected above 16 MiB before parsing or journaling; bounded
  adapter stderr and terminal causes remain available only through the
  owner-authenticated `crab-v2-agent` operator CLI. See the
  [private diagnostics](docs/agent-diagnostics.md), [resume flow](docs/agent-session-resume.md) and
  [detach flow](docs/runtime-detach.md). Its
  [state contract](docs/agent-host-storage.md) is schema-versioned from day one; the
  [rendered flow](docs/agent-host-flow.png) shows the process boundary.
- `native-channel` is a durable Boxology consumer of `agent-host`: it binds one UI to one session,
  routes queue/steer and explicit interrupt, replays the complete bidirectional ACP stream, and
  confirms adapter publication in order. Failed bindings can recover the exact resumed session
  without changing identity or delivery cursors. Its owner-only `crab-v2-channel` client discovers
  bindings, pending work and event cursors without exposing adapter destination metadata. See its
  [operator flow](docs/channel-operations.md), [state contract](docs/native-channel-storage.md),
  [rendered host flow](docs/native-channel-flow.png) and
  [rendered operator flow](docs/channel-operations-flow.png).
- `channel-gateway` is the single lifecycle path for configured and dynamic UI attachment. It
  reuses a matching live binding, resumes matching unavailable sessions before replacement, and
  rejects changed intent while a session is live. Only explicit resume unavailability falls back
  to replacement; hard recovery failures stay visible. See the
  [rendered attach flow](docs/channel-gateway-flow.png).
- `bridge-host` supervises agent-installed JSON-lines packages, brokers private file-backed
  credentials, actively probes health and credential validity, and applies bounded restart
  backoff. Optional generation-fixed alert targets receive one durable queue turn per actionable
  incident and one recovery turn; failed enqueues retry without duplication. Package-originated
  ingress is acknowledged only after the generated `trigger-inbox` import commits it. Bounded
  inbound media is fsynced into host-owned private content and only
  validated handles become ACP resource links; mutable service credentials use fingerprint-CAS
  atomic snapshots; selected outbound delivery is durable and idempotent. `crab-v2-bridge` exposes
  its complete operator lifecycle through authenticated local IPC without disclosing credential
  material. See the
  [operator flow](docs/bridge-operations.md),
  [state contract](docs/bridge-host-storage.md) and [rendered flow](docs/bridge-host-flow.png).
- Every configured ACP parent and child receives 15 strict native bridge tools. An agent can install
  a package it wrote, change or retire it under generation control, authenticate it, validate its
  credentials, stage bounded workspace files into Crab-owned content, and send deliberately
  selected output without gaining credential-store access. See the
  [agent bridge boundary](docs/native-bridge-tools.md).
- `sub-agent-host` composes through the generated `agent-host` import, durably journals both
  message directions and the complete child ACP stream. Inherited children prefer advertised ACP
  `session/fork` at an exact idle parent cursor; the fork runs in its own supervised adapter
  process, and visible-history replay remains an explicit fallback. After parents recover, eligible
  children
  resume their exact native sessions within a durable restart budget; identities, journals and
  cursors stay continuous, while every non-resumable child fails explicitly without replacement.
  `crab-v2-sub-agent` exposes spawn, bidirectional messaging, cursor events, status and idempotent
  stop through the owner-only local IPC. See the
  [control flow](docs/sub-agent-control.md), [state contract](docs/sub-agent-host-storage.md) and
  [rendered host flow](docs/sub-agent-host-flow.png), [context flow](docs/sub-agent-context-flow.png)
  and [recovery flow](docs/sub-agent-recovery-flow.png).
- Every configured ACP session can receive Crab's six native sub-agent tools through a first-party
  stdio MCP server. Parents and children share the toolset; child-to-parent delivery is enabled only
  when Crab injects child identity. See the [native tool boundary](docs/native-sub-agent-tools.md).
- Every Crab-owned ACP/MCP stdio server rejects incoming frames above 16 MiB before UTF-8 or JSON
  parsing. This covers the native channel facade, bridge tools and sub-agent tools through one
  shared transport constructor. See the [rendered stdio boundary](docs/native-stdio-boundary.png).
- Startup rejects runtime configuration above 8 MiB and bootstrap prompts above 2 MiB before
  whole-file allocation. The configured runtime and standalone ACP channel facade share the same
  bootstrap reader. See the [rendered startup boundary](docs/startup-input-boundary.png).
- `trigger-inbox` is implemented with durable deduplication, FIFO leases and restart recovery.
  `crab-v2-trigger` exposes its enqueue capability through owner-only authenticated local IPC for
  cron, self-work and operator ingress. Its
  [storage contract](docs/trigger-inbox-storage.md) is schema-versioned from day one.
- `turn-router` resolves bridge/scheduler/self-work ingress without pretending those sources are
  native channels. It serializes each lane, maps queue/steer/interrupt explicitly, and settles only
  after durable channel acceptance. See its [state contract](docs/turn-router-storage.md) and
  [rendered flow](docs/turn-router-flow.png).
- `crab-v2` loads secret-free schema-v1 topology, opens fresh ACP sessions, recovers persisted
  bindings/routes, exposes an owner-only authenticated local Boxology endpoint, and continuously
  drains every configured trigger lane. `crab-v2-health` reconciles that configured topology with
  the running process's immutable config/PID attestation plus live channel, ACP-session, bridge and
  credential evidence, distinguishing deploy-safe readiness from full health. See the [startup
  contract](docs/runtime-startup.md), [runtime health contract](docs/runtime-health.md) and [local
  transport contract](docs/channel-ipc.md).
- `make v2-bundle` builds the complete locked release closure, vendors the Claude and Codex ACP
  adapters plus WhatsApp production dependencies, verifies every entry, and publishes atomically.
  The resulting directory needs no Rust, npm, `npx`, install, or package fetch at runtime. Its
  bundled tool selects Claude or Codex on first install and remains the one atomic update recipe:
  immutable releases, twelve production binaries, a single launchd-owned runtime,
  configuration-aware authenticated readiness and verified
  rollback. See the [rendered bundle
  flow](docs/runtime-bundle-flow.png), [deployment flow](docs/runtime-deploy-flow.png), and [release
  recipe](docs/runtime-bundle.md).
- The first native UI ships in the Crab fork of T3 Code. Its built-in provider runs across web,
  desktop and mobile; `crab-v2-acp-channel` attaches each T3 thread to the single Crab-owned runtime
  without transferring session or tool authority. See the
  [decision and attach seam](docs/acp-native-ui.md) and
  [rendered facade flow](docs/acp-channel-facade-flow.png).

## Validate

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
boxology check --base origin/main
```

Build the clean-machine runtime artifact from a clean commit:

```sh
make v2-bundle
```

Crab v2 pins Boxology's complete runtime and CLI toolchain to current `main` revision `4dd0088`.
This is the `0.1.1` release plus the optional-field wire-semantics and truthful generator-provenance
fixes. Regenerate or check with the matching CLI:

```sh
cargo install boxology-cli --git https://github.com/fontanierh/boxology \
  --rev 4dd00888445c6506704a3e3f69932a3c4bc32efa --locked
```

The published ACP SDK 2.0.0 is patched at reviewed fork revision `3722fbf` until upstream
[rust-sdk #341](https://github.com/agentclientprotocol/rust-sdk/pull/341) ships. The patch bounds
newline-delimited input framing; Crab keeps the exact 2.0.0 API and schema dependency closure.

A vertical slice that changes a box, its composition and the platform lockfile still triggers the
known single-owner limitation tracked in
[Boxology #712](https://github.com/fontanierh/boxology/issues/712); every executable check passes.
