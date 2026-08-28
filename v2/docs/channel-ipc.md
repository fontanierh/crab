# Local capability IPC

The long-running Crab runtime exposes selected generated Boxology capabilities through one local
Unix socket. UI adapters attach without inheriting agent ownership; trigger producers and bridge
operators never open Crab's databases or credential store.

![Authenticated local channel flow](channel-ipc-flow.png)

| Wire capability | Boxology owner |
|---|---|
| `channel-gateway.attach_channel` | `channel-gateway` |
| `native-channel.accept_turn` | `native-channel` |
| `native-channel.interrupt_and_drain` | `native-channel` |
| `native-channel.channel_status` | `native-channel` |
| `native-channel.list_bindings` | `native-channel` |
| `native-channel.binding_summary` | `native-channel` |
| `native-channel.replay_native_events` | `native-channel` |
| `trigger-inbox.enqueue` | `trigger-inbox` |
| `runtime-control.runtime_status` | `runtime-control` |
| `bridge-host.register_bridge` | `bridge-host` |
| `bridge-host.list_bridge_page` | `bridge-host` |
| `bridge-host.replace_bridge` | `bridge-host` |
| `bridge-host.unregister_bridge` | `bridge-host` |
| `bridge-host.reconcile_bridge` | `bridge-host` |
| `bridge-host.begin_authentication` | `bridge-host` |
| `bridge-host.submit_authentication` | `bridge-host` |
| `bridge-host.validate_credentials` | `bridge-host` |
| `bridge-host.invalidate_credentials` | `bridge-host` |
| `bridge-host.import_content` | `bridge-host` |
| `bridge-host.deliver_message` | `bridge-host` |
| `bridge-host.delivery_status` | `bridge-host` |
| `bridge-host.bridge_status` | `bridge-host` |
| `bridge-host.suspend_bridge` | `bridge-host` |
| `bridge-host.stop_bridge` | `bridge-host` |
| `agent-host.session_status` | `agent-host` |
| `agent-host.list_sessions` | `agent-host` |
| `agent-host.read_diagnostics` | `agent-host` |
| `sub-agent-host.spawn` | `sub-agent-host` |
| `sub-agent-host.send_to_child` | `sub-agent-host` |
| `sub-agent-host.send_to_parent` | `sub-agent-host` |
| `sub-agent-host.read_events` | `sub-agent-host` |
| `sub-agent-host.status` | `sub-agent-host` |
| `sub-agent-host.stop` | `sub-agent-host` |

## Boundary

- `channel-ipc.sock` and `channel-ipc.token` are mode `0600` beneath the canonical state directory.
- The 256-bit token survives runtime restarts; a malformed, symlinked or group/world-accessible
  token fails startup.
- Each request is one bounded JSON line with protocol version, request ID, authentication,
  qualified capability and canonical Boxology JSON input. Unknown fields and capabilities fail
  closed.
- One ten-second deadline covers connect, write and response read as a single operation. A peer
  that accepts without replying returns the stable `local IPC request timed out` error.
- The server handles at most 64 connections concurrently and caps every accepted connection at 30
  seconds, including partial frames and capability execution. Expired peers receive no response;
  their task and socket are released, and shutdown still preempts a full connection set.
- Responses preserve Boxology domain-error tags and canonical contract output. The bridge CLI
  exposes auth presentations but never credential handles or material. Private agent diagnostics
  cross only on an explicit operator request and are never available to agent MCP tools.
- `bridge-host.list_bridge_page` requires a 1…256 limit, accepts an optional identity cursor and an
  active-only/tombstone filter, and returns the filter-wide total plus continuation evidence. The
  compatibility `list_bridges` capability is not routed through owner IPC.
- `runtime-control.runtime_status` is immutable for the process lifetime and contains only the
  resolved semantic-config fingerprint, startup time and PID.
- Client disconnect only closes that transport connection. Session replacement and shutdown remain
  explicit Crab operations.
- The token is loaded from the state directory by the local client. It never appears in CLI
  arguments, output or trigger records.

See [native channel operations](channel-operations.md),
[private agent diagnostics](agent-diagnostics.md), [bridge operations](bridge-operations.md),
[configuration-aware runtime health](runtime-health.md) and [realtime sub-agent
control](sub-agent-control.md) for the typed operator workflows.
