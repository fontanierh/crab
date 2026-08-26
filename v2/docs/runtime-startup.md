# Runtime startup

`crab-v2` turns the seven-box composition into one supervised process. `channel-gateway` is
stateless, so the state layout still contains six SQLite stores.

![Configured runtime flow](runtime-startup-flow.png)

```text
runtime state/
├── agent-host.sqlite
├── bridge-credentials/
├── bridge-host.sqlite
├── channel-ipc.sock       owner-only, ephemeral
├── channel-ipc.token      owner-only, persistent
├── native-channel.sqlite
├── sub-agent-host.sqlite
├── trigger-inbox.sqlite
└── turn-router.sqlite
```

## Configuration

Schema `1` is strict JSON. It declares ACP commands, native channels and trigger lanes. Relative
paths resolve beside the config file. Secrets are referenced only by environment-variable name;
unknown fields, missing variables, broken references and zero-valued worker bounds fail startup.

Start from [`runtime.example.json`](../runtime/runtime.example.json):

Replace the example executable paths and agent-specific authority flags; they are placeholders,
not a portable ACP command line.

Each `sessionOptions` entry is a required ACP policy, not a UI default. After `session/new`, Crab
sets every value through `session/set_config_option` and checks the returned effective state before
marking the session ready. An unsupported option, a rewritten value or an omitted result fails the
session closed.

![ACP session policy negotiation](acp-session-policy-flow.png)

### Claude Opus preset

[`runtime.claude-opus.example.json`](../runtime/runtime.claude-opus.example.json) is the first real
agent preset. It pins the official Claude ACP adapter to `0.70.0`, selects
`mode=bypassPermissions`, and selects the exact effective picker ID `model=opus[1m]`. The official
adapter currently canonicalizes the shorthand `opus` to that account-offered Opus 5 picker; the
exact value makes a future rewrite fail closed. The package pin and resolved model are separate
things.

The example deliberately uses `/usr/bin/false` for `authorityProbe`, so copying it cannot weaken
the mandatory host preflight. Replace that command with the deployment's audited probe and replace
the working directory. Claude authentication remains owned by the native Claude login; no token is
stored in this JSON.

```sh
cargo run -p crab-v2-runtime --bin crab-v2 -- \
  --config runtime/runtime.json \
  --state-dir /private/path/to/crab-v2-state
```

## Trigger ingress

`crab-v2-trigger` is the single supported recipe for cron, self-work and operator ingress. It reads
the owner-only token from the state directory and calls generated `trigger-inbox.enqueue`; it never
opens SQLite directly. Retries must reuse the same source ID and deduplication key.

```sh
crab-v2-trigger \
  --state-dir /private/path/to/crab-v2-state \
  --channel primary --lane primary \
  --source self-work --source-id jim --dedupe-key follow-up-42 \
  --mode queue --message "Continue the accepted follow-up"
```

Use `--message-json` for a native payload and `--not-before-ms` for delayed delivery. Modes are
`queue`, `steer` and `interrupt-and-steer`; queue is the default.

## Restart contract

| Persisted state | Startup action |
|---|---|
| Matching live binding | Reuse its session; never start a duplicate process |
| Matching unavailable binding | Open one ACP session and atomically replace session + adapter metadata |
| Binding created before route CAS | Find it by channel/adapter identity, recover it, then register the route |
| No binding | Open one ACP session, bind it, then register the route |
| Changed intent with a live session | Fail with an attachment conflict; never replace live work implicitly |

Agent-owned compaction is not resumed or reconstructed. Durable trigger IDs still deduplicate
retries, and each configured lane is drained serially until SIGINT/SIGTERM stops workers and closes
the local endpoint, then the live ACP sessions. UI process disconnects never close a Crab-owned
session. See the [local transport contract](channel-ipc.md).
