# Runtime startup

`crab-v2` turns the six-box composition into one supervised process.

![Configured runtime flow](runtime-startup-flow.png)

```text
runtime state/
├── agent-host.sqlite
├── bridge-credentials/
├── bridge-host.sqlite
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

```sh
cargo run -p crab-v2-runtime --bin crab-v2 -- \
  --config runtime/runtime.json \
  --state-dir /private/path/to/crab-v2-state
```

## Restart contract

| Persisted state | Startup action |
|---|---|
| Route + matching binding | Open a fresh ACP session and atomically replace the dead session |
| Binding created before route CAS | Find it by channel/adapter identity, replace, then register route |
| No binding | Bind the fresh session, then register the route |
| Stale adapter metadata | Detach the stale binding and create the configured binding |

Agent-owned compaction is not resumed or reconstructed. Durable trigger IDs still deduplicate
retries, and each configured lane is drained serially until SIGINT/SIGTERM stops workers and closes
the live ACP sessions.
