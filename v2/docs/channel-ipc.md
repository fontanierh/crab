# Native channel IPC

The long-running Crab runtime exposes selected generated Boxology capabilities through one local
Unix socket. UI adapters attach to Crab; they never inherit ownership of the underlying ACP agent.

![Authenticated local channel flow](channel-ipc-flow.png)

| Wire capability | Boxology owner |
|---|---|
| `channel-gateway.attach_channel` | `channel-gateway` |
| `native-channel.accept_turn` | `native-channel` |
| `native-channel.interrupt_and_drain` | `native-channel` |
| `native-channel.channel_status` | `native-channel` |
| `native-channel.replay_native_events` | `native-channel` |

## Boundary

- `channel-ipc.sock` and `channel-ipc.token` are mode `0600` beneath the canonical state directory.
- The 256-bit token survives runtime restarts; a malformed, symlinked or group/world-accessible
  token fails startup.
- Each request is one bounded JSON line with protocol version, request ID, authentication,
  qualified capability and canonical Boxology JSON input. Unknown fields and capabilities fail
  closed.
- Responses preserve Boxology domain-error tags and canonical contract output. Private producer
  diagnostics and credentials are not forwarded.
- Client disconnect only closes that transport connection. Session replacement and shutdown remain
  explicit Crab operations.
