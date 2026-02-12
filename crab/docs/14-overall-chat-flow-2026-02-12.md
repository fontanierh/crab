# Overall Chat Flow (2026-02-12)

## Scope

This is the end-to-end runtime flow for a real chat with Crab as of February 12, 2026.

It describes what happens from process startup, to Discord message ingestion, to backend
execution, delivery, rotation, and recovery.

## 0) Process Startup

1. `crab-discord-connector` starts and launches `crabd` as a child process.
2. `crabd` validates config, workspace layout, and state schema compatibility/migrations.
3. `crabd` runs startup reconciliation:
   - stale `running` runs are repaired
   - non-idle session handles are cleared when required
   - backend managers are restarted if needed
4. `crabd` enters daemon loop:
   - poll ingress frames from connector
   - dispatch queued runs by lane scheduler rules
   - run heartbeat on schedule

## 1) Ingress From Discord

1. Connector receives `MESSAGE_CREATE` from Discord Gateway.
2. Bot/self messages are filtered out.
3. Connector writes `CrabdInboundFrame(kind=gateway_message)` JSONL to `crabd` stdin.
4. `crabd` maps message routing key to `logical_session_id`:
   - channel: `discord:channel:<channel_id>`
   - thread: `discord:thread:<thread_id>`
   - DM: `discord:dm:<user_id>`
5. `crabd` persists the gateway `channel_id` onto the queued `Run` as `delivery_channel_id`.
   - For DMs, this is required: Discord replies must target the DM channel id (not the user id
     embedded in the logical session id).

## 2) Run Creation + Lane Queue

1. `crabd` builds deterministic `run_id = run:<logical_session_id>:<message_id>`.
2. Run is enqueued into that session lane (FIFO per session, one active run per lane).
3. Run/session metadata is persisted before dispatch.
4. Dispatcher starts run when lane is eligible under global concurrency cap.

## 3) Identity/Trust/Profile Resolution

1. Sender identity is resolved (owner vs non-owner, canonical user key).
2. Profile is resolved with precedence rules (owner defaults, backend defaults, request-level overrides).
3. Compatibility/fallback policy is applied for backend/model/reasoning.
4. Run profile telemetry is attached to run/event records.

## 4) Physical Session Handling

1. Runtime checks whether this logical session already has an active physical backend session.
2. If missing, runtime creates one and binds it.
3. If present, runtime reuses it (context continuity lives in backend session).

## 5) Turn Context Injection (Critical Behavior)

### New physical session (bootstrap turn)

Runtime composes a full structured context and sends it as the turn input envelope:

1. `SOUL.md`
2. `IDENTITY.md`
3. `USER.md`
4. `MEMORY.md`
5. `MEMORY_SNIPPETS`
6. `LATEST_CHECKPOINT`
7. `CRAB_RUNTIME_BRIEF`
8. `PROMPT_CONTRACT`
9. `TURN_INPUT`

All managed sections are token-budget validated with explicit failure on overflow (no truncation).
The envelope is XML-structured with explicit boundaries:

- `<crab_turn_context>` root
- `<crab_system_context>` for Crab-injected context
- `<crab_user_input>` for user-provided turn input
- Runtime logs bootstrap-context size (`injected_context_tokens`, `injected_context_chars`) for
  physical-session init observability.

### Reused physical session

Runtime sends raw user turn input only. It does not re-inject the full bootstrap context every turn.

## 6) Backend Turn Execution

1. Runtime executes turn through backend bridge (Codex/OpenCode/Claude).
2. Streamed backend events are normalized and appended as `EventEnvelope` records.
3. Usage metadata (tokens) is collected from normalized usage events.
4. Final run status resolves to `success`, `failed`, or `cancelled`.

## 7) Discord Delivery + Idempotency

1. Assistant output is buffered/rendered and emitted as outbound ops:
   - first chunk: `post`
   - later updates: `edit`
2. Each op includes deterministic `delivery_id` and `op_id`.
3. Connector executes Discord REST call, persists `delivery_id -> discord_message_id`, and returns an explicit receipt.
4. `crabd` marks delivery sent only after positive receipt.
5. On restart/replay, existing delivery records prevent duplicate sends and allow safe continuation of edits.

## 8) Rotation/Compaction

After normal run finalization, runtime evaluates rotation triggers:

1. token threshold (`CRAB_COMPACTION_TOKEN_THRESHOLD`, default `120000`)
2. inactivity timeout (`CRAB_INACTIVITY_TIMEOUT_SECS`, default `1800`)
3. owner manual commands (`/compact confirm`, `/reset confirm`)

If triggered, rotation sequence executes:

1. hidden memory-flush turn
2. hidden checkpoint turn (strict JSON schema with retry policy)
3. deterministic fallback checkpoint if hidden checkpoint turn fails/invalid
4. checkpoint persistence
5. physical session clear/rotate
6. token accounting reset for next compaction cycle

## 9) Heartbeat + Stall Recovery

Heartbeat periodically checks:

1. stalled active run (`CRAB_RUN_STALL_TIMEOUT_SECS`)
2. unhealthy backend manager (`CRAB_BACKEND_STALL_TIMEOUT_SECS`)
3. stalled dispatcher (`CRAB_DISPATCHER_STALL_TIMEOUT_SECS`)

Escalation path:

1. request cancel
2. hard stop and release lane if needed
3. restart backend manager when unhealthy
4. append audit/diagnostic events for operator visibility

## 10) Crash/Restart Recovery

1. Startup reconciliation repairs stale runtime state.
2. Event/outbound stores replay missing delivery to reach exactly-once visible behavior in Discord.
3. Session/run invariants are revalidated before processing new work.

## 11) What Users See In Discord

Visible:

- assistant text responses (post/edit stream; split into consecutive messages on blank-line boundaries and the 2000-char Discord limit)
- explicit owner command acknowledgments (for supported operator commands)

Not visible:

- hidden maintenance turns (memory flush/checkpoint)
- internal event envelopes and diagnostics
- raw checkpoint JSON payloads

## Current Known Gaps (Release Readiness)

1. Deployment acceptance evidence on the target machine is still pending (`crab/docs/13-deployment-acceptance-evidence.md`, issue `#92`).
