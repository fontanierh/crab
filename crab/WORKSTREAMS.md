# Crab Workstreams and Task Breakdown

This document breaks the Crab implementation into execution workstreams and issue-sized tasks.

## 1) Planning Constraints

- Track all issues and tasks only in `fontanierh/2026-02-06-autofun`.
- Keep tasks small enough for one PR each whenever possible.
- Preserve project quality gates from `AGENTS.md`:
  - 100% test coverage (with enforcement command in CI)
  - minimal mocking, meaningful tests
  - dead-code checks
  - duplication checks
  - lint + format must pass

## 2) Workstream Overview

| Workstream | Goal | Output |
|---|---|---|
| WS0 | Repo and quality foundations | Rust workspace, CI, quality gates |
| WS1 | Domain and persistence core | Session/run/event/checkpoint stores |
| WS2 | Lane scheduler | Deterministic FIFO + global concurrency |
| WS3 | Discord transport | Ingress, routing, streaming, idempotent delivery |
| WS4 | Backend trait + Claude | First functional backend adapter |
| WS5 | Codex adapter | Persistent app-server integration |
| WS6 | OpenCode adapter | Persistent HTTP integration |
| WS7 | Model/reasoning policy | Effective profile resolution and fallback |
| WS8 | Memory flush + checkpoint | Hidden protocol turns and rotation handoff |
| WS9 | Reliability + ops UX | Cancel, recovery, heartbeat, operator commands |

## 3) Detailed Workstreams and Tasks

### WS0 - Repo and Quality Foundations

### WS0-T1 - Rust workspace bootstrap
- Create crate layout (`core`, `store`, `scheduler`, `discord`, `backends`, `app`).
- Add shared error/result types and config loading skeleton.
- Done criteria: workspace builds with `cargo check --workspace`.

### WS0-T2 - Quality toolchain and commands
- Add formatter/linter/test commands and make targets.
- Add coverage command and threshold set to 100%.
- Add dead-code and duplication checks.
- Done criteria: one command runs all gates locally.

### WS0-T3 - CI pipeline
- Add CI jobs for fmt, lint, tests, coverage, dead-code, duplication.
- Fail build on any gate failure.
- Done criteria: CI green on baseline branch.

### WS0-T4 - Developer docs
- Document local workflow commands and failure triage.
- Done criteria: `README.md` links to quality commands.

### WS1 - Domain and Persistence Core

### WS1-T1 - Core domain model
- Define `LogicalSession`, `PhysicalSession`, `Run`, `LaneState`, `Checkpoint`, `EventEnvelope`, `OutboundRecord`.
- Done criteria: serde round-trip tests for all core structs.

### WS1-T2 - Session store
- Implement session index and per-session state files.
- Include atomic write and corruption-safe read strategy.
- Done criteria: crash-safe write tests pass.

### WS1-T3 - Event store
- Append-only event log with monotonic per-run sequence checks.
- Done criteria: ordering and replay reconstruction tests pass.

### WS1-T4 - Checkpoint store
- Store validated checkpoint JSON documents.
- Provide latest-checkpoint lookup API.
- Done criteria: retrieval and schema validation tests pass.

### WS1-T5 - Outbound record store
- Persist outbound message/edit records for idempotency.
- Done criteria: duplicate suppression tests pass.

### WS2 - Lane Scheduler

### WS2-T1 - Per-session FIFO queue
- One queue per logical session, strict FIFO semantics.
- Done criteria: deterministic order tests pass.

### WS2-T2 - Global concurrency cap
- Enforce `max_concurrent_lanes` across all lanes.
- Done criteria: concurrent scheduling tests pass.

### WS2-T3 - Lane state machine
- Implement `Idle`, `Running`, `Cancelling`, `Rotating` transitions.
- Done criteria: invalid transition tests fail correctly.

### WS2-T4 - Overflow policy
- Reject new message on queue limit with explicit user-facing reason.
- Done criteria: queue overflow integration tests pass.

### WS3 - Discord Transport

### WS3-T1 - Gateway ingress
- Connect bot, receive messages for channels/threads/DMs.
- Done criteria: routing key extraction tests pass.

### WS3-T2 - Session routing
- Map Discord context to logical session ids.
- Done criteria: stable mapping tests for channel/thread/DM pass.

### WS3-T3 - Streaming delivery
- Stream assistant output using edit-in-place behavior.
- Done criteria: chunked streaming tests pass.

### WS3-T4 - Idempotent delivery/edit policy
- Use outbound record store to avoid duplicate posts/edits.
- Done criteria: retry/replay does not duplicate messages.

### WS3-T5 - Discord message constraints
- Handle 2000-char limit and deterministic chunk indexing.
- Done criteria: long-message split tests pass.

### WS4 - Backend Contract and Claude Adapter

### WS4-T1 - Backend trait implementation boundary
- Implement adapter harness around unified backend trait.
- Done criteria: fake backend conformance tests pass.

### WS4-T2 - Claude session lifecycle
- Create/send/interrupt/end for Claude backend.
- Done criteria: lifecycle tests with fixture streams pass.

### WS4-T3 - Claude event normalization
- Map Claude stream to Crab event envelopes.
- Done criteria: normalization snapshot tests pass.

### WS4-T4 - Claude usage accounting
- Extract tokens and persist run usage metadata.
- Done criteria: accounting tests pass.

### WS5 - Codex Adapter

### WS5-T1 - Codex manager process
- Manage long-lived `codex app-server` lifecycle.
- Done criteria: startup/restart tests pass.

### WS5-T2 - Codex protocol primitives
- Implement `thread/start`, `thread/resume`, `turn/start`, `turn/interrupt`.
- Done criteria: protocol contract tests pass.

### WS5-T3 - Codex event normalization
- Normalize notifications and requests to Crab envelopes.
- Done criteria: end-to-end fixture tests pass.

### WS5-T4 - Approval and user-input unattended policy
- Auto-approve configured requests, reject/handle user-input requests deterministically.
- Done criteria: unattended policy tests pass.

### WS5-T5 - Failure recovery path
- Recover from app-server crash with resume-or-rotate logic.
- Done criteria: crash recovery integration tests pass.

### WS6 - OpenCode Adapter

### WS6-T1 - OpenCode server lifecycle
- Start/health-check/restart OpenCode service.
- Done criteria: lifecycle tests pass.

### WS6-T2 - OpenCode session and turn APIs
- Create session, send prompt, interrupt, end session.
- Done criteria: adapter contract tests pass.

### WS6-T3 - OpenCode event normalization
- Map stream events to Crab envelope schema.
- Done criteria: normalization tests pass.

### WS6-T4 - Failure recovery path
- Handle server/session loss via rotate from checkpoint.
- Done criteria: recovery tests pass.

### WS7 - Model and Reasoning Policy

### WS7-T1 - Effective profile resolver
- Resolve `backend`, `model`, `reasoning_level` using precedence policy.
- Done criteria: precedence matrix tests pass.

### WS7-T2 - Compatibility validator
- Validate requested model/reasoning against backend support.
- Done criteria: compatibility tests pass.

### WS7-T3 - Fallback policy
- Implement `strict` and `compatible` modes.
- Done criteria: fallback behavior tests pass.

### WS7-T4 - Adapter mappings
- Map canonical reasoning to backend-native controls.
- Done criteria: mapping tests pass per backend.

### WS7-T5 - Profile persistence and telemetry
- Persist resolved profile in run metadata and events.
- Done criteria: persistence/replay tests pass.

### WS8 - Memory Flush, Checkpoint, and Rotation

### WS8-T1 - Trigger evaluator
- Evaluate compaction, inactivity, and manual reset/compact triggers.
- Done criteria: trigger decision tests pass.

### WS8-T2 - Hidden memory flush turn
- Run memory flush protocol, suppress user-visible output.
- Done criteria: hidden-turn behavior tests pass.

### WS8-T3 - Hidden checkpoint turn
- Enforce checkpoint JSON schema and retry-on-parse-failure.
- Done criteria: schema and retry tests pass.

### WS8-T4 - Fallback checkpoint generator
- Produce deterministic fallback checkpoint from transcript tail.
- Done criteria: fallback tests pass.

### WS8-T5 - Rotation sequence
- Execute flush -> checkpoint -> end session -> clear handle.
- Done criteria: rotation integration tests pass.

### WS9 - Reliability and Operator UX

### WS9-T1 - Cancellation semantics
- `/cancel` for active run plus queued run cancellation by id.
- Done criteria: cancellation tests pass.

### WS9-T2 - Startup reconciliation
- Recover orphaned runs and stale backend handles on boot.
- Done criteria: crash-restart replay tests pass.

### WS9-T3 - Heartbeat loops
- Run, backend, and dispatcher heartbeats with stall policy.
- Done criteria: stall handling tests pass.

### WS9-T4 - Operator commands
- Implement `/backend`, `/model`, `/reasoning`, `/profile`.
- Done criteria: command behavior tests pass.

### WS9-T5 - Structured diagnostics
- Add high-signal logs and run/session correlation ids.
- Done criteria: diagnostics fixture tests pass.

## 4) Dependency Order and Critical Path

Execution order:

1. WS0 -> WS1 -> WS2 -> WS3
2. WS4 in parallel with WS7 after WS1 and WS2
3. WS5 and WS6 after WS4 baseline contract is stable
4. WS8 after at least one backend is production-ready (WS4+)
5. WS9 after WS3 and one backend are stable

Critical path to MVP:

- WS0, WS1, WS2, WS3, WS4, WS7, WS8, WS9

Codex/OpenCode parity path:

- WS5, WS6

## 5) Milestone Cut Plan

### Milestone M1 - Single-backend vertical slice
- Scope: WS0, WS1, WS2, WS3, WS4 (Claude only, minimal commands).
- Exit criteria: end-to-end Discord -> Claude -> Discord loop stable.

### Milestone M2 - Reliability + memory semantics
- Scope: WS7, WS8, WS9.
- Exit criteria: profile policy, flush/checkpoint rotation, cancel/recovery/heartbeat all green.

### Milestone M3 - Backend parity
- Scope: WS5, WS6.
- Exit criteria: Codex and OpenCode both pass adapter conformance and replay/recovery tests.

## 6) Issue Template for Task Tickets

Use this exact checklist when creating each task issue:

- Goal
- In-scope changes
- Out-of-scope changes
- Acceptance criteria
- Test plan (including coverage impact)
- Risks
- Rollback plan
