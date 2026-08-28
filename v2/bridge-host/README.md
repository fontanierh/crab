# Bridge host

`bridge-host` owns generic external-message admission, durable ingress routing and selected
delivery. Service packages never bypass its identity, JSON or attachment limits.

## Control boundary

![Bridge control admission boundary](bridge-control-boundary.png)

Every control request is semantically bounded before the bridge ID can enter the per-bridge lock
registry or reach a package/store. This applies equally to read-only status lookups, lifecycle
commands and the ephemeral authentication exchange.

| Boundary | Maximum |
|---|---:|
| Bridge identity | 512 bytes |
| Delivery message / authentication challenge ID | 1,024 bytes |
| Authentication context JSON object | 64 KiB |
| Authentication response JSON | 1 MiB |

Authentication methods must be known contract variants. Invalid requests fail without acquiring a
lock, calling a bridge package or touching durable state.

## Credential boundary

![Bridge credential snapshot boundary](bridge-credential-boundary.png)

Initial authentication results and live package rotations share one 8 MiB canonical-JSON limit.
The host validates each snapshot before credential serialization or provider access. Live updates
also validate bridge identity and the prior SHA-256 fingerprint before acquiring the global update
mutex, then replace the opaque credential only when the fingerprint still matches.

Durable bridge state stores the returned provider handle, account hint and validation metadata—not
the secret snapshot. The credential provider retains its independent 16 MiB defense-in-depth limit.

## Registration boundary

![Bridge registration admission boundary](bridge-registration-boundary.png)

Raw bridge specs are byte- and count-bounded before the bridge ID enters the per-bridge lock
registry. The host then parses strict launch/configuration objects and checks launch paths, argv,
environment names, authentication methods and restart policy before persistence or process launch.

| Boundary | Maximum |
|---|---:|
| Bridge / package identity | 512 bytes |
| Display name | 1,024 bytes |
| Launch / configuration JSON | 256 KiB / 1 MiB |
| Authentication methods | 6 unique known methods |
| Executable / working-directory path | 4,096 bytes |
| Launch arguments | 64 × 4,096 bytes |
| Environment names | 128 × 255 bytes |
| Process starts per restart window | 64 |

Registration validates path shape without requiring a stopped package to be running. Actual launch
also requires the configured working directory to exist.

## Package result boundary

![Bridge package result admission boundary](bridge-result-boundary.png)

The 16 MiB transport-frame ceiling is not a durable-data policy. Every package result is checked
again by the host before operator response or persistence. Oversized health and supervisor results
are package protocol failures; invalid delivery results leave the delivery retryable.

| Package result | Maximum |
|---|---:|
| Health / credential / delivery detail JSON | 64 KiB |
| Authentication presentation JSON | 1 MiB |
| Credential snapshot JSON | 8 MiB |
| Account hint | 1,024 bytes |
| External delivery ID | 1,024 bytes |

Direct health observations use the same detail limit and are rejected before per-bridge locking.

## Message boundary

![Bridge message admission boundary](bridge-message-boundary.png)

Inbound and outbound messages are byte- and count-bounded before bridge locking, JSON parsing,
attachment ownership checks, fingerprinting or persistence. The final normalized inbound trigger
is rechecked against the trigger inbox ceiling.

| Boundary | Maximum |
|---|---:|
| Bridge / channel identity | 512 bytes |
| Event, message or idempotency identity | 1,024 bytes |
| Sender or destination JSON | 64 KiB |
| Message JSON | 896 KiB |
| Normalized inbound trigger | 1 MiB |
| Attachments | 64 |
| Media type / name / content handle | 255 / 1,024 / 4,096 bytes |
