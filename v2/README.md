# Crab v2 contract draft

Crab v2 is a small, ACP-native host. These contracts describe its boundaries before any runtime
implementation is selected.

![Crab v2 architecture](docs/crab-v2-architecture.png)

```text
v2/
├── agent-host/       ACP lifecycle, native event stream, mandatory host authority
├── native-channel/   one channel ↔ one ACP session, with the full native view
├── bridge-host/      supervised external integrations, auth and selected delivery
├── sub-agent-host/   supervised ACP subprocesses with bidirectional live interaction
├── trigger-inbox/    durable at-least-once ingress used by bridges, cron and self-work
└── runtime/          thin composition; no domain policy
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
- Crab owns sub-agents as separately supervised ACP subprocesses. They support both fresh and
  inherited parent context, report whether inheritance used native ACP fork or a portable visible
  snapshot, and exchange durable non-blocking messages in both directions.
- Agents run only after a fail-closed preflight proves permission bypass, no sandbox, unrestricted
  filesystem/network access and working passwordless `sudo`.
- Bridges are packages the agent may add. Crab owns supervision, auth state, health and delivery
  semantics—not service-specific behavior. WhatsApp is the first intended first-party package.
- Tests target useful contract and composition behavior. There is no percentage coverage gate.
- Implementations in this draft return an explicit `DraftOnly` error; they do not fake a runtime.
- For a native UI, start by testing an off-the-shelf ACP client; build on reusable ACP components
  only if that cannot attach cleanly. See [the UI landscape](docs/acp-native-ui.md).

## Validate

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
boxology check --base origin/main
```

Crab v2 uses Boxology's published exact `0.1.1` registry crates and tools; no local checkout or Git
dependency is required because this workspace does not use `boxology-http`. This release fixes the
generated-format stability, new nested-root classification and generated-contract bootstrap gaps
found while preparing this draft.
