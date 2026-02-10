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
| WS10 | Workspace bootstrap runtime | Deterministic identity/memory file lifecycle |
| WS11 | Owner model and trust mapping | Sender->owner resolution and policy context |
| WS12 | First-run onboarding flow | Bootstrap conversation and identity capture |
| WS13 | Context and prompt assembly | Ordered context injection and prompt contract |
| WS14 | Memory recall runtime | Prompt + CLI memory retrieval (`memory_search`/`memory_get` via CLI surface) |
| WS15 | End-to-end runtime orchestration | `discord -> lane -> backend -> store -> discord` loop |
| WS16 | Comprehensive architecture documentation | Design decisions, lifecycle docs, deployment runbook/gaps |

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

### WS10 - Workspace Bootstrap Runtime

### WS10-T1 - Bootstrap template set
- Define default templates for `AGENTS.md`, `SOUL.md`, `IDENTITY.md`, `USER.md`, `MEMORY.md`, and `BOOTSTRAP.md`.
- Keep templates small and deterministic for testability.
- Done criteria: template load/render tests pass.

### WS10-T2 - Workspace ensure lifecycle
- Implement startup ensure logic for workspace root and required files.
- Create/repair `CLAUDE.md -> AGENTS.md` symlink idempotently.
- Done criteria: repeated ensure runs are no-op and tested.

### WS10-T3 - Bootstrap state detector
- Detect first-run state (new workspace or pending bootstrap completion).
- Surface bootstrap state in runtime/session metadata.
- Done criteria: bootstrap state matrix tests pass.

### WS10-T4 - Memory directory bootstrap
- Ensure `memory/global/` and `memory/users/<discord_user_id>/` layout exists.
- Keep per-user memory roots deterministic and safe.
- Done criteria: layout and path-safety tests pass.

### WS10-T5 - Startup integration in app runtime
- Wire workspace ensure into process startup before accepting Discord ingress.
- Emit startup diagnostics when files are created/repaired.
- Done criteria: startup integration tests pass.

### WS11 - Owner Model and Trust Mapping

### WS11-T1 - Owner config schema
- Add owner identity config (`discord_user_ids`, aliases, profile defaults, machine location/timezone fields).
- Validate config with explicit error messages.
- Done criteria: config parse/validation tests pass.

### WS11-T2 - Sender identity resolver
- Resolve inbound Discord sender to canonical user key and owner boolean.
- Support DM/thread/channel consistently.
- Done criteria: resolver matrix tests pass.

### WS11-T3 - Owner-aware run context
- Persist `sender_id`, `sender_is_owner`, and resolved owner profile in run metadata/events.
- Make metadata available to prompt assembly.
- Done criteria: persistence and replay tests pass.

### WS11-T4 - Owner policy gates
- Define owner-only operator/admin command behavior.
- Keep default assistant behavior autonomous (no Discord approval UI dependency).
- Done criteria: command authorization tests pass.

### WS11-T5 - Trust and privacy safeguards
- Enforce per-user memory scope default on shared Discord surfaces.
- Prevent owner-specific context bleed to non-owner runs.
- Done criteria: privacy regression tests pass.

### WS12 - First-Run Onboarding Flow

### WS12-T1 - Onboarding question contract
- Define first-conversation capture contract covering who the agent is, who the owner is, primary goals, and machine location/timezone.
- Done criteria: onboarding prompt contract tests pass.

### WS12-T2 - Bootstrap state machine
- Implement onboarding states: `Pending`, `InProgress`, `Completed`, `Skipped`.
- Support resume after interruption/restart.
- Done criteria: state transition tests pass.

### WS12-T3 - Identity/profile file writers
- Write normalized updates to `IDENTITY.md`, `USER.md`, and `SOUL.md`.
- Preserve user-authored sections where possible.
- Done criteria: merge/update behavior tests pass.

### WS12-T4 - Bootstrap completion protocol
- On successful onboarding: update memory baseline and remove/retire `BOOTSTRAP.md`.
- Emit bootstrap completion event envelope.
- Done criteria: completion protocol tests pass.

### WS12-T5 - Manual onboarding commands
- Add operator command(s) to re-run onboarding or reset bootstrap state.
- Keep behavior explicit and auditable.
- Done criteria: manual reset/restart tests pass.

### WS13 - Context and Prompt Assembly

### WS13-T1 - Injection-order context assembler
- Implement design-specified order:
  `SOUL.md -> IDENTITY.md -> AGENTS.md -> USER.md -> MEMORY.md -> memory snippets -> latest checkpoint -> turn input`.
- Done criteria: deterministic ordering tests pass.

### WS13-T2 - Scoped memory snippet resolver
- Inject per-user + recent global memory according to policy.
- Keep context small and deterministic.
- Done criteria: scope and selection tests pass.

### WS13-T3 - Prompt contract compiler
- Build backend-neutral system prompt sections:
  memory recall guidance (CLI-first), owner context line, runtime notes, messaging semantics.
- Done criteria: prompt snapshot tests pass.

### WS13-T4 - Context budget + truncation policy
- Add predictable truncation behavior with explicit truncation markers.
- Ensure budgeting is stable across runs.
- Done criteria: token/char budget tests pass.

### WS13-T5 - Context diagnostics
- Emit context report (files injected, sizes, truncation decisions) for debugging.
- Done criteria: diagnostics report fixture tests pass.

### WS14 - Memory Recall Runtime (Prompt + CLI)

### WS14-T1 - `memory_search` implementation
- Implement ranked keyword + recency search over `MEMORY.md` + memory files.
- Return scored snippets with path metadata.
- Keep v1 search mode deterministic; defer embedding/vector semantic search.
- Done criteria: search correctness tests pass.

### WS14-T2 - `memory_get` implementation
- Implement safe line-range retrieval by relative path.
- Reject path traversal and invalid ranges.
- Done criteria: retrieval and safety tests pass.

### WS14-T3 - CLI exposure + prompt/backend parity wiring
- Expose memory recall uniformly via CLI commands (`crab-memory-search`, `crab-memory-get`) and prompt contract across Claude/Codex/OpenCode.
- Avoid backend-specific custom memory tool registration.
- Keep CLI contracts stable and adapter-agnostic.
- Done criteria: backend prompt/wiring conformance tests pass.

### WS14-T4 - Citation and disclosure policy
- Add citation mode policy (`auto|on|off`) for memory CLI snippets.
- Default behavior should reduce leakage in shared contexts.
- Done criteria: citation policy tests pass.

### WS14-T5 - Flush + CLI interaction tests
- Validate hidden memory flush writes are discoverable by memory CLI commands in subsequent turns.
- Done criteria: end-to-end memory continuity tests pass.

### WS15 - End-to-End Runtime Orchestration

### WS15-T1 - `crab-app` composition root
- Build runtime wiring for stores, scheduler, Discord transport, and backend managers.
- Done criteria: composition tests pass.

### WS15-T2 - Turn executor pipeline
- Implement `ingress -> route -> enqueue -> dispatch -> context build -> backend turn -> finalize`.
- Done criteria: pipeline integration tests pass.

### WS15-T3 - Event envelope parity
- Align persisted event envelope fields with design requirements (run/turn/lane/session/profile/sequence metadata).
- Done criteria: schema parity + replay tests pass.

### WS15-T4 - Delivery and replay integration
- Connect normalized events to Discord delivery with idempotent resend/edit recovery.
- Done criteria: crash-replay duplicate-suppression tests pass.

### WS15-T5 - Deployment readiness slice
- Add integration tests for first interaction onboarding, normal owner/non-owner runs, and restart recovery continuity.
- Done criteria: end-to-end suite green with quality gates.

### WS16 - Comprehensive Documentation

### WS16-T1 - Documentation architecture map
- Create documentation index and reading order under `crab/docs/`.
- Define conventions for decisions, sequences, invariants, and status notes.
- Done criteria: `crab/docs/README.md` covers scope and navigation.

### WS16-T2 - Initial-turn and onboarding spec
- Document first-run bootstrap flow, onboarding schema, lifecycle state machine, and file mutation semantics.
- Include sequence diagram and failure/retry behavior.
- Done criteria: onboarding doc clearly maps startup -> capture -> completion -> ready state.

### WS16-T3 - Session/lane/runtime flow spec
- Document logical vs physical sessions, lane queue semantics, and turn lifecycle.
- Clarify run/event identities and ordering guarantees.
- Done criteria: flow is reconstructible from docs without reading code.

### WS16-T4 - Rotation/memory/reliability deep dives
- Document compaction trigger model, hidden flush/checkpoint protocol, fallback checkpointing, memory recall/context assembly, and crash recovery/delivery replay semantics.
- Done criteria: docs define exact contracts and invariants for recovery-critical behavior.

### WS16-T5 - Deployment readiness gap register
- Maintain a concrete list of unresolved integration gaps and closure order.
- Keep this register updated as implementation lands.
- Done criteria: `crab/docs/08-deployment-readiness-gaps.md` is current and actionable.

## 4) Dependency Order and Critical Path

Execution order:

1. WS0 -> WS1 -> WS2 -> WS3
2. WS4 in parallel with WS7 after WS1 and WS2
3. WS5 and WS6 after WS4 baseline contract is stable
4. WS8 after at least one backend is production-ready (WS4+)
5. WS9 after WS3 and one backend are stable
6. WS10 before WS12, WS13, WS14, and WS15
7. WS11 in parallel with WS10; required before owner-aware WS12/WS13/WS15 behavior
8. WS12 and WS13 after WS10 and WS11
9. WS14 after WS10 and WS13
10. WS15 after WS10, WS11, WS12, WS13, and WS14
11. WS16 runs across all phases; must be current before deployment milestones are declared complete

Critical path to MVP:

- WS0, WS1, WS2, WS3, WS4, WS7, WS8, WS9, WS10, WS11, WS12, WS13, WS14, WS15, WS16

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

### Milestone M4 - Identity/bootstrap vertical slice
- Scope: WS10, WS11, WS12, WS13.
- Exit criteria: first run reliably captures identity/owner context, writes workspace files, and injects context in deterministic order.

### Milestone M5 - Memory recall + full runtime loop
- Scope: WS14, WS15.
- Exit criteria: prompt + CLI memory recall and end-to-end orchestration are production-ready for deployment on target machine.

### Milestone M6 - Documentation And Deployment Handoff
- Scope: WS16.
- Exit criteria: architecture docs comprehensively cover onboarding, sessions, rotation, memory, reliability, and current deployment gaps.

## 6) Issue Template for Task Tickets

Use this exact checklist when creating each task issue:

- Goal
- In-scope changes
- Out-of-scope changes
- Acceptance criteria
- Test plan (including coverage impact)
- Risks
- Rollback plan
