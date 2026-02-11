# Storage And State Model

## Scope

This document defines persistent runtime state, partitioning strategy, and data integrity guarantees.

## Decisions

- Decision: all runtime state is local-file durable.
  - Rationale: crash recovery must not depend on external databases.

- Decision: partition by logical session id.
  - Rationale: deterministic replay boundaries and simpler lifecycle operations.

- Decision: event log is append-only.
  - Rationale: preserves audit history and enables replay/reconstruction.

## State Root

App composition creates a state root under workspace:

- `workspace/state/`

Stores are mounted from that root (`crates/crab-app/src/composition.rs`).

Global state-schema marker:

- `workspace/state/schema_version.json`
- JSON schema:
  - `version` (`u32`)
  - `updated_at_epoch_ms` (`u64`)

Current schema policy constants (`crates/crab-core/src/state_schema.rs`):

- `MIN_SUPPORTED_STATE_SCHEMA_VERSION=0`
- `CURRENT_STATE_SCHEMA_VERSION=1`

Compatibility interpretation:

- missing marker is treated as legacy `version=0`
- versions in `[min, max]` are compatible
- versions below `min` or above `max` are incompatible

## Store Responsibilities

### SessionStore

Persists `LogicalSession` records and a session index:

- lane state
- active backend/profile
- active physical session id
- last checkpoint pointer
- token accounting summary

### RunStore

Persists `Run` records per logical session:

- queue/running/completed status
- profile telemetry snapshot
- timestamps

### EventStore

Append-only run event log:

- monotonic sequence validation
- per-run replay API
- supports legacy envelope compatibility paths

### CheckpointStore

Persists checkpoint snapshots and provides latest-checkpoint lookup.

### OutboundRecordStore

Persists Discord delivery records for idempotent replay/deduplication.

## Data Model Anchors

Core domain structs in `crates/crab-core/src/domain.rs`:

- `LogicalSession`
- `PhysicalSession`
- `Run`
- `EventEnvelope`
- `Checkpoint`
- `OutboundRecord`

## Partitioning And Identity

Partition keys:

- logical session id for session/run/event/checkpoint/outbound grouping
- run id for event and outbound replay scope

Identity guarantees:

- run lookup validates `(logical_session_id, run_id)` consistency
- event sequence is monotonic within run
- replay APIs are deterministic with sorted/event-order semantics

## Integrity And Durability Behavior

Store layer includes:

- layout ensure (directory creation)
- atomic-write helpers for core records
- backup-aware reads for corruption recovery paths
- explicit invariant errors for shape or identity mismatch

State-schema migration layer includes:

- startup migration execution before runtime store/process loops begin
- migration lock file at `workspace/state/schema_migration.lock.json` to prevent concurrent migrators
- stale lock reclamation for abandoned lock files
- deterministic stepwise migration (`vN -> vN+1`) with strict step registry
- idempotent reruns (already-migrated state is a no-op)
- explicit failure on unknown migration step or incompatible schema version

Structured migration events are emitted in startup state:

- `migration_started`
- `migration_step`
- `migration_completed`
- `migration_failed`

## Operational Consequences

- Restart can recover by replaying persisted events and outbound records.
- Crash during delivery can still avoid duplicate output via outbound dedupe store.
- Corrupted index/log paths can be surfaced with explicit context-rich errors.
- Upgrade tooling can evaluate state compatibility using `schema_version.json` without mutating data.
