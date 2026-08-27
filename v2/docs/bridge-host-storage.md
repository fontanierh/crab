# Bridge host state and package protocol

`bridge-host` owns bridge lifecycle, policy, authentication references, health, inbound audit and
selected outbound delivery. Service-specific behavior stays in agent-installable subprocesses.

![Bridge host flow](bridge-host-flow.png)

![Bridge management and restart flow](bridge-management-flow.png)

![Durable bridge incident flow](bridge-incidents-flow.png)

## Durable layout

```text
runtime-state/
├── bridge-host.sqlite       specs, generations, health, auth metadata, ingress, deliveries
├── bridge-credentials/      opaque-handle JSON files; directory 0700, files 0600
└── bridge-content/          host-named media + metadata; directory 0700, files 0600
```

- SQLite schema v3 uses WAL, foreign keys, full synchronous writes and fail-closed version checks.
  Existing registrations migrate to `RuntimeConfigured` ownership without a conflict or new
  generation; schema-v1 registrations also gain a null alert target.
- Each bridge generation is immutable. Package, configuration, management or ingress-mode changes
  use compare-and-swap replacement and append a generation audit row.
- Agent-managed unregistration is generation-CAS protected. It stops supervision, revokes the
  opaque credential and advances to an `Unregistered` tombstone. Historical ingress, deliveries
  and their private content remain available to queued work and audit; registering the same ID
  later starts a fresh credential-less generation. Runtime-configured registrations cannot use this
  path.
- Runtime restart preserves desired state and credential handles, invalidates stale health and
  pending challenges, then restarts desired packages under their configured budget. Startup stops
  only removed `RuntimeConfigured` registrations; `AgentManaged` bridges survive static topology
  changes and continue under the same supervisor policy.
- An optional generation-fixed alert target turns actionable package, service and credential
  failures into durable queue-mode triggers. SQLite owns the incident sequence and enqueue receipt,
  so retries never duplicate a notification. A fully healthy probe closes the episode and emits one
  recovery trigger; absent targets remain deliberately silent.
- Credential bytes never enter SQLite, errors or debug output. The host actively revalidates the
  opaque handle at the registered interval. Renderable authentication challenges are returned to
  the caller, while SQLite retains only challenge metadata—not QR or response payloads. Starting a
  challenge atomically supersedes the prior pending challenge; expiry and completion are durable,
  and only the newest 64 terminal challenges per bridge remain in history.
- Inbound deduplication is `(bridge_id, external_event_id)`; acknowledgement follows durable
  `trigger-inbox.enqueue`. The registered generation—not the event—selects queue, steer or
  interrupt-and-steer. The router delivers interrupt-and-steer through atomic downstream
  acceptance, so the triggering input cannot lose a cancellation race.
- Package uploads and agent-requested local-file imports are capped at 8 MiB and deterministically
  deduplicated by bridge, stable source identity, metadata and bytes. Agent imports require an
  absolute regular non-symlink source and a stable read. The host copies into a private path and
  returns a percent-encoded `file://` handle. Ingress and delivery accept that handle only for the
  originating bridge with the exact stored media type/name and recheck the file size/hash before
  use.
- Outbound deduplication is `(bridge_id, message_id)` with a stable package idempotency key.

## Process protocol

Each package is an absolute executable speaking protocol v2 as bounded JSON-RPC-style messages,
one JSON object per line. Crab clears the child environment, passes only explicitly named
variables, and fails launch when a named variable is unavailable.

| Direction | Methods |
|---|---|
| Host → package | `bridge/initialize`, `bridge/health`, `bridge/auth/*`, `bridge/deliver`, `bridge/shutdown` |
| Package → host | `bridge/content/put`, `bridge/inbound`, `bridge/credential/update` |

The transport multiplexes responses and ordered inbound calls without blocking health or delivery
RPCs. Media bytes cross only the private bounded stdio protocol: `bridge/content/put` fsyncs content
before returning its handle, and `bridge/inbound` receives success only after durable trigger
enqueue. Both operations are idempotent for a retried external event.

Authentication completion is two-phase: Crab stores the initial secret under an opaque handle,
then `bridge/auth/committed` permits live credential updates. Each update supplies the previous
canonical-JSON SHA-256 fingerprint. Crab accepts it only from the active package instance, compares
the current fingerprint, atomically replaces and fsyncs the same mode-0600 credential file, then
returns the new fingerprint. Stale processes and out-of-order snapshots fail without secret-bearing
diagnostics.

Crab terminates the package process group on stop/drop, probes immediately and on every health
interval, applies exponential backoff, and refuses starts beyond the configured restart window.
