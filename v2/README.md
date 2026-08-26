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
  can queue portably; true steering requires an advertised agent extension.
- Crab owns sub-agents as separately supervised ACP subprocesses. Fresh sessions and inherited
  visible-history snapshots work now; the contract reserves truthful native ACP fork reporting.
  Parent and child exchange durable non-blocking queue, steer or interrupt messages in both
  directions.
- Agents run only after a fail-closed preflight proves permission bypass, no sandbox, unrestricted
  filesystem/network access and working passwordless `sudo`. Required ACP session options are then
  applied and verified before readiness; the pinned Claude preset requires
  `bypassPermissions` and the Opus picker. Its first-party macOS probe actively verifies the host,
  adapter and network conditions rather than trusting configuration claims.
- Bridges are packages the agent may add. Crab owns supervision, auth state, health and delivery
  semantics—not service-specific behavior. WhatsApp is the first intended first-party package.
- Tests target useful contract and composition behavior. There is no percentage coverage gate.
- `agent-host` runs real ACP v1/v2 subprocesses with mandatory authority preflight, durable
  prompts/events/permissions, queue/steer/cancel, and native process-group shutdown. Its
  [state contract](docs/agent-host-storage.md) is schema-versioned from day one; the
  [rendered flow](docs/agent-host-flow.png) shows the process boundary.
- `native-channel` is a durable Boxology consumer of `agent-host`: it binds one UI to one session,
  routes queue/steer and explicit interrupt, replays the complete bidirectional ACP stream, and
  confirms adapter publication in order. See its [state contract](docs/native-channel-storage.md)
  and [rendered flow](docs/native-channel-flow.png).
- `channel-gateway` is the single lifecycle path for configured and dynamic UI attachment. It
  reuses a matching live binding, replaces only unavailable physical sessions, and rejects changed
  intent while a session is live. See the [rendered attach flow](docs/channel-gateway-flow.png).
- `bridge-host` supervises agent-installed JSON-lines packages, brokers private file-backed
  credentials, actively probes health and credential validity, and applies bounded restart
  backoff. Package-originated ingress is acknowledged only after the generated `trigger-inbox`
  import commits it; mutable service credentials use fingerprint-CAS atomic snapshots; selected
  outbound delivery is durable and idempotent. `crab-v2-bridge` exposes its complete operator
  lifecycle through authenticated local IPC without disclosing credential material. See the
  [operator flow](docs/bridge-operations.md),
  [state contract](docs/bridge-host-storage.md) and [rendered flow](docs/bridge-host-flow.png).
- `sub-agent-host` composes through the generated `agent-host` import, durably journals both
  message directions and the complete child ACP stream, and fails closed when requested context or
  crash recovery cannot be honored. See its [state contract](docs/sub-agent-host-storage.md) and
  [rendered flow](docs/sub-agent-host-flow.png).
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
  drains every configured trigger lane. See the [startup contract](docs/runtime-startup.md) and
  [local transport contract](docs/channel-ipc.md).
- `make v2-bundle` builds the complete locked release closure, vendors the Claude ACP adapter and
  WhatsApp production dependencies, verifies every entry, and publishes atomically. The resulting
  directory needs no Rust, npm, `npx`, install, or package fetch at runtime. See the
  [rendered bundle flow](docs/runtime-bundle-flow.png) and [release recipe](docs/runtime-bundle.md).
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

A vertical slice that changes a box, its composition and the platform lockfile still triggers the
known single-owner limitation tracked in
[Boxology #712](https://github.com/fontanierh/boxology/issues/712); every executable check passes.
