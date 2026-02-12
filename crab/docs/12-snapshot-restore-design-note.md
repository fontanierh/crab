# Snapshot/Restore Design Note (Deferred WS22-T5)

## Purpose

Define a pragmatic snapshot/restore path for upgrade safety without blocking current deployment
readiness.

This note is intentionally design-only. It does not introduce runtime or installer behavior yet.

## Problem Statement

Crab already has:

- schema marker + startup migrator (`state/schema_version.json`)
- compatibility preflight in `crabctl doctor` and `crabctl upgrade`
- rollback for binaries/releases

What is still missing is data rollback safety when an upgrade introduces unexpected runtime or
migration behavior after preflight passes.

## Scope Candidates

### Option A: Keep Current State (No Snapshot Commands)

Description:

- Continue with manual host-level backup practices only.
- Rely on schema preflight + binary rollback + operator playbooks.

Pros:

- zero implementation effort
- no new operational surface area

Cons:

- recovery depends on external backup discipline
- higher MTTR under state regression incidents

Complexity:

- Engineering: none
- Operations: high (manual, potentially inconsistent)

### Option B: Minimal `crabctl` Local Snapshot/Restore (Recommended)

Description:

- Add explicit `crabctl snapshot create` and `crabctl snapshot restore`.
- Snapshot includes:
  - workspace content (`CRAB_WORKSPACE_ROOT`)
  - state directories/files (sessions/runs/events/checkpoints/outbound/schema marker)
  - environment metadata (non-secret manifest only: key presence + selected non-sensitive config)
- Snapshot artifact is local, immutable, timestamped, and checksummed.
- Restore is explicit, host-local, and requires service stop + confirmation.

Pros:

- strong recovery path with moderate implementation cost
- deterministic operator workflow with documented steps
- no external backup service dependency

Cons:

- local disk consumption
- requires careful guardrails around secrets and partial restore failures

Complexity estimate:

- Engineering: medium (roughly 3-5 focused tasks)
- Operations: medium (new runbook + retention policy)

### Option C: Full Backup/Restore Platform Integration

Description:

- integrate remote object storage, retention policies, encryption, and scheduled backups
- include remote snapshot catalog and cross-host restore flows

Pros:

- strongest disaster recovery story
- better long-term fleet operations

Cons:

- high implementation and operational complexity
- introduces external infrastructure dependency and secret-management requirements

Complexity estimate:

- Engineering: high
- Operations: high

## Recommended Direction

Adopt Option B as the default post-deployment hardening step.

Rationale:

- Option A is too fragile for long-lived unattended operation.
- Option C is over-scoped for current single-host deployment stage.
- Option B materially reduces rollback risk while staying operationally manageable.

## Proposed Command Surface (Design)

No implementation in this issue. Proposed shapes:

```bash
crabctl snapshot create --target <macos|linux> [--output <path>] [--label <name>]
crabctl snapshot list --target <macos|linux>
crabctl snapshot restore --target <macos|linux> --snapshot <id> --confirm
```

Behavioral expectations:

- `create`:
  - validates runtime is quiesced or captures with explicit consistency mode
  - writes manifest + checksum + compressed payload
  - excludes known transient/cache artifacts
- `restore`:
  - blocks unless service is stopped (or stops service explicitly with confirmation)
  - validates artifact checksum and compatibility metadata
  - restores atomically (staging + swap) where feasible
  - emits clear rollback instructions on failure

## Data and Security Boundaries

Snapshot payload:

- include workspace and state data
- include env metadata manifest (non-secret)
- never include raw secrets from `/etc/crab/crab.env` by default

Optional future extension:

- explicit `--include-secrets` mode with strict warnings and encrypted artifact requirements

## Operational Tradeoffs

Disk/capacity:

- snapshots can be large due to workspace and event logs
- require retention policy (for example latest `N` snapshots or age-based pruning)

Recovery speed:

- local snapshots are fast to restore on same host
- no cross-host guarantees in minimal design

Failure handling:

- restore should be idempotent and resumable where possible
- command must leave actionable diagnostics and non-destructive recovery path

## Sequencing Recommendation

Current phase:

- keep deployment critical path focused on target-machine acceptance evidence (`WS18-T5`)

Next hardening phase:

1. implement minimal snapshot create
2. implement restore flow + safety guards
3. add integration tests for happy path and interrupted restore
4. update `crab/docs/10-target-machine-operations.md` with concrete procedures

## Trigger Conditions to Promote from Deferred -> Active

Promote snapshot/restore implementation immediately when any of the following occurs:

- more than one production incident requires manual state recovery
- upcoming migration step is non-trivial (schema/data rewrite risk rises)
- multi-host or remote unattended upgrades become standard workflow

## Go/No-Go

- Go for Option B in the next hardening wave after deployment acceptance is complete.
- No-Go for Option C until operational scale justifies external backup infrastructure.
