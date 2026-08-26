# Bridge host state and package protocol

`bridge-host` owns bridge lifecycle, policy, authentication references, health, inbound audit and
selected outbound delivery. Service-specific behavior stays in agent-installable subprocesses.

![Bridge host flow](bridge-host-flow.png)

## Durable layout

```text
runtime-state/
├── bridge-host.sqlite       specs, generations, health, auth metadata, ingress, deliveries
└── bridge-credentials/      opaque-handle JSON files; directory 0700, files 0600
```

- SQLite schema v1 uses WAL, foreign keys, full synchronous writes and fail-closed version checks.
- A bridge ID has one immutable generation. Package, configuration or ingress-mode changes use
  compare-and-swap replacement and append a generation audit row.
- Runtime restart preserves desired state and credential handles, invalidates stale health and
  pending challenges, then restarts desired packages under their configured budget.
- Credential bytes never enter SQLite, errors or debug output. The host actively revalidates the
  opaque handle at the registered interval. Renderable authentication challenges are returned to
  the caller, while SQLite retains only challenge metadata—not QR or response payloads.
- Inbound deduplication is `(bridge_id, external_event_id)`; acknowledgement follows durable
  `trigger-inbox.enqueue`. The registered generation—not the event—selects queue, steer or
  interrupt-and-steer.
- Outbound deduplication is `(bridge_id, message_id)` with a stable package idempotency key.

## Process protocol

Each package is an absolute executable speaking bounded JSON-RPC-style messages, one JSON object
per line. Crab clears the child environment and passes only explicitly named variables.

| Direction | Methods |
|---|---|
| Host → package | `bridge/initialize`, `bridge/health`, `bridge/auth/*`, `bridge/deliver`, `bridge/shutdown` |
| Package → host | `bridge/inbound` |

The transport multiplexes responses and ordered inbound calls without blocking health or delivery
RPCs. `bridge/inbound` receives a success response only after durable trigger enqueue; packages can
safely retry the same external event ID after a crash or timeout.

Crab terminates the package process group on stop/drop, probes immediately and on every health
interval, applies exponential backoff, and refuses starts beyond the configured restart window.
