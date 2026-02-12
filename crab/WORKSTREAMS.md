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
| WS17 | Runtime deployment gap closure | Rotation/config/token-accounting/manual controls/heartbeats wired end-to-end |
| WS18 | Discord auth + production deployment | Real Discord bot runtime, OAuth provisioning runbook, target-machine service playbook |
| WS19 | Cross-platform installer and host provisioning | Idempotent macOS/Linux install, service setup, upgrade/rollback tooling |
| WS20 | Skills compatibility and governance | Canonical `.agents/skills` layout, Claude compatibility symlink, built-in skill authoring policy |
| WS21 | Workspace private Git persistence | Durable workspace history in private repo with safe async push/retry |
| WS22 | Runtime state evolution and migration safety | Versioned state marker, startup migrator, actionable compatibility preflight, migration governance |
| WS23 | Session context lifecycle hardening | Bootstrap-only context injection, token-capped managed docs, native backend reasoning mapping |
| WS24 | Onboarding runtime gate and conversational completion | Owner-DM bootstrap gate, rotation-time extraction, runtime brief context guidance |

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

### WS17 - Runtime Deployment Gap Closure

### WS17-T1 - Rotation and heartbeat runtime config surface
- Extend runtime config/env parsing with compaction threshold, inactivity timeout, stale-run reconciliation grace, and heartbeat policy values.
- Keep validation explicit and fail-fast on invalid values.
- Update docs: `crab/DESIGN.md` and `crab/docs/08-deployment-readiness-gaps.md`.
- Done criteria: config parse/validation tests pass and docs reflect supported env keys.

### WS17-T2 - Token accounting propagation in turn finalization
- Parse normalized backend usage run-note payloads and aggregate into session token accounting.
- Persist updated accounting deterministically at run completion.
- Update docs: `crab/docs/03-rotation-checkpoint-and-compaction.md` and `crab/docs/08-deployment-readiness-gaps.md`.
- Done criteria: accounting persistence tests pass and compaction trigger input can be computed from stored state.

### WS17-T3 - Rotation trigger wiring + owner manual compact/reset commands
- Wire `evaluate_rotation_triggers` and `execute_rotation_sequence` into post-run finalization flow.
- Add owner-only manual compact/reset operator commands with explicit audit events.
- Update docs: `crab/docs/03-rotation-checkpoint-and-compaction.md`, `crab/docs/05-reliability-delivery-and-recovery.md`, and `crab/docs/08-deployment-readiness-gaps.md`.
- Done criteria: automatic and manual rotation integration tests pass; unauthorized callers are rejected.

### WS17-T4 - Startup reconciliation and heartbeat loop integration
- Wire startup reconciliation at boot and heartbeat execution on a deterministic runtime schedule.
- Ensure escalation paths are observable and do not duplicate user-visible Discord output.
- Update docs: `crab/docs/05-reliability-delivery-and-recovery.md`.
- Done criteria: crash/restart + stall handling integration tests pass with deterministic outcomes.

### WS17-T5 - Deployment-gap closure validation and documentation sync
- Add integration tests that exercise full gap closure path (token accounting -> trigger -> rotation -> replay safety).
- Reconcile and close resolved items in `crab/docs/08-deployment-readiness-gaps.md` with concrete status notes.
- Update `README.md` deployment-readiness section with current state.
- Done criteria: gap register is current, and quality gates stay green.

### WS18 - Discord Auth and Production Deployment

### WS18-T1 - Real Discord runtime adapter
- Implement Discord client runtime boundary for ingress events and outbound send/edit operations.
- Enforce idempotent delivery semantics with retry/rate-limit handling compatible with persisted outbound records.
- Update docs: `crab/docs/02-sessions-lanes-and-turn-lifecycle.md` and `crab/docs/05-reliability-delivery-and-recovery.md`.
- Done criteria: Discord transport integration tests pass against runtime adapter seams.

### WS18-T2 - Production bot runtime binary
- Add production runtime binary (for example `crabd`) that loads config, composes runtime, runs startup hooks, ingests Discord events, dispatches queued turns, and handles graceful shutdown.
- Keep backend manager lifecycle and lane scheduler execution explicit and observable.
- Update docs: `README.md` and `crab/docs/README.md`.
- Done criteria: binary runs end-to-end locally with documented launch command.

### WS18-T3 - Discord OAuth/install + token operations runbook
- Document Discord Developer Portal provisioning flow: app creation, bot setup, required intents, OAuth scopes, invite URL generation, and permission profile.
- Document token storage, rotation, and incident response expectations for `CRAB_DISCORD_TOKEN`.
- Update docs: add `crab/docs/09-discord-provisioning-and-secrets.md` and link from `crab/docs/README.md`.
- Done criteria: operator can provision and secure bot credentials using only repository docs.

### WS18-T4 - Target-machine service and operational playbook
- Provide service templates/scripts for persistent runtime execution on target machine (launchd/systemd as applicable).
- Document bootstrap, restart policy, logging, upgrade, rollback, and disaster-recovery steps.
- Update docs: add `crab/docs/10-target-machine-operations.md`.
- Done criteria: documented service setup supports reboot persistence and crash restart.

### WS18-T5 - Deployment acceptance checklist and final documentation pass
- Add an explicit deployment checklist covering onboarding first turn, owner profile capture, idempotent replay, rotation triggers, and heartbeat behavior.
- Ensure final docs are coherent and non-duplicative across `README.md`, `crab/DESIGN.md`, and `crab/docs/`.
- Update `crab/docs/08-deployment-readiness-gaps.md` with final go/no-go criteria and residual risks.
- Done criteria: checklist exists, is executable, and can be attached to deployment evidence.
 - Implemented so far:
   - deployment checklist + go/no-go rule in `crab/docs/08-deployment-readiness-gaps.md`
   - deployment acceptance evidence worksheet in `crab/docs/13-deployment-acceptance-evidence.md`
   - first-interaction onboarding completion capture wiring in normal turn flow (`turn_executor`)
   - OpenCode shared recovery-helper wiring in daemon execution bridge (`recover_opencode_session`)
   - docs reconciled with current runtime wiring (`README.md`, gap register, integration matrix)
 - Still required:
   - execute checklist on target machine and fill evidence worksheet

### WS18-T6 - Connector delivery receipt protocol hardening
- Extend connector <-> `crabd` IPC contract to include operation request ids and delivery receipts.
- Persist outbound delivery records in `crabd` only after positive connector receipt containing final Discord message id.
- Add replay/crash tests for delivery failure boundaries (post success + receipt loss, edit retry, connector restart mid-delivery).
- Update docs: `crab/docs/05-reliability-delivery-and-recovery.md`, `crab/docs/08-deployment-readiness-gaps.md`, and `crab/docs/10-target-machine-operations.md`.
- Done criteria: replay/idempotency tests prove no duplicate/lost delivery across connector/process crashes.

### WS18-T7 - Reproducible Code Quality Report + artifact policy
- Provide a generator for `CODE_QUALITY_REPORT.md` and document how to run it.
- Ensure mutation-testing outputs and other analysis artifacts are ignored and do not pollute `git status`.
- Update docs: `AGENTS.md`.
- Done criteria: report is reproducible from script, and repo stays clean after running quality tooling.

### WS18-T8 - Dependency audit hygiene (time crate vulnerability)
- Keep `Cargo.lock` updated and resolve high/medium RustSec advisories promptly.
- Update docs: `CODE_QUALITY_REPORT.md` and/or `crab/docs/08-deployment-readiness-gaps.md` when it impacts deploy readiness.
- Done criteria: `cargo audit` reports no known vulnerabilities in the dependency graph.

### WS18-T9 - Structured logging for runtime binaries
- Add structured logging for `crabd` and `crab-discord-connector`.
- Ensure logging never corrupts JSONL transport (stdout reserved for IPC frames).
- Update docs: `crab/docs/10-target-machine-operations.md`.
- Done criteria: operators can control verbosity via `RUST_LOG` and logs are present in service output.

### WS18-T10 - Mutation testing boundary coverage improvements
- Add targeted boundary-value tests based on mutation testing results.
- Prefer small refactors that improve testability and reduce reliance on IO ordering.
- Done criteria: previously missed mutants are caught by tests, without weakening existing quality gates.

### WS19 - Cross-Platform Installer and Host Provisioning

Status (as of 2026-02-11): WS19-T1 through WS19-T5 completed.
Delivered:
- `crabctl` installer binary and command surface (`install`, `upgrade`, `rollback`, `doctor`)
- macOS/Linux target planning and deterministic dry-run execution model
- prerequisite/tool bootstrap with version verification output
- runtime filesystem layout + service-definition provisioning (`launchd`/`systemd`)
- idempotent rerun semantics, conflict/failure detection, and rollback state handling

### WS19-T1 - Installer CLI and execution model
- Define installer entrypoint (for example `crabctl install`) with explicit target OS selection and dry-run mode.
- Support both macOS and Linux host profiles with deterministic command plans.
- Update docs: `README.md` and `crab/docs/10-target-machine-operations.md`.
- Done criteria: installer command renders reproducible plan for both platforms and executes on target host.
 - Implemented:
   - `crates/crab-app/src/bin/crabctl.rs` entrypoint with `crab_app::run_installer_cli`.
   - strict arg parser/validator in `crates/crab-app/src/installer.rs` for `--target`, `--dry-run`, and host layout flags.
   - deterministic action log output for plan/apply paths.

### WS19-T2 - Host prerequisites and toolchain bootstrap (macOS + Linux)
- Install/validate required host tools (`tmux`, `git`, `gh`, `jq`, `rg`, Rust toolchain, `cargo-llvm-cov`, Node runtime).
- Ensure shell profile wiring is correct for non-interactive service shells.
- Update docs: `crab/docs/10-target-machine-operations.md`.
- Done criteria: one installer run brings a clean host to required baseline with version verification output.
 - Implemented:
   - prerequisite specs and version checks for required tools in `PREREQUISITES`.
   - platform-specific install commands for macOS (`brew`) and Linux (`apt-get`) with dry-run support.
   - actionable logging for install attempts, version capture, and failure paths.

### WS19-T3 - Runtime filesystem and service provisioning (launchd + systemd)
- Provision `/opt/crab`, `/etc/crab`, `/var/lib/crab`, `/var/log/crab` layout with least-privilege ownership policy.
- Install/update service units:
  - macOS `launchd` (`com.crab.runtime`)
  - Linux `systemd` (`crab.service`)
- Done criteria: service boots on restart/reboot and runs with documented env file policy on both platforms.
 - Implemented:
   - idempotent runtime layout provisioning (`ensure_runtime_layout`) for `/opt/crab`, `/etc/crab`, `/var/lib/crab`, `/var/log/crab`.
   - deterministic env template provisioning (`/etc/crab/crab.env`) with unix mode hardening to `0600`.
   - service file rendering + install for both targets (`render_systemd_service`, `render_launchd_service`).

### WS19-T4 - Upgrade, rollback, and diagnostics commands
- Add `upgrade`, `rollback`, and `doctor` installer commands.
- `doctor` must validate runtime binary presence, env config sanity, service status, and log path accessibility.
- Update docs: `crab/docs/10-target-machine-operations.md` and `crab/docs/08-deployment-readiness-gaps.md`.
- Done criteria: operator can execute full upgrade and rollback path with deterministic diagnostics output.
 - Implemented:
   - `upgrade` preserves previous release pointer before switching `current`.
   - `rollback` restores previous release and updates shim symlinks deterministically.
   - `doctor` emits pass/fail checks and exits non-zero when unhealthy.

### WS19-T5 - Installer safety and idempotency hardening
- Make install/provision actions idempotent and safe on partial failure.
- Add failure-injection tests for interrupted install, existing conflicting files, and missing privileges.
- Done criteria: rerunning installer after partial failure converges to healthy desired state without destructive resets.
 - Implemented:
   - reusable idempotent ensure helpers for directories/files/symlinks/binary copies with explicit conflict errors.
   - stateful rollback metadata file (`/var/lib/crab/install-state/previous_release.txt`) for reversible release changes.
   - failure-injection and idempotency coverage in installer tests (`install_is_idempotent_on_second_run`, conflict/failure/rollback/doctor branches).

### WS20 - Skills Compatibility and Governance

Status (as of 2026-02-11): completed.
Delivered:
- canonical `.agents/skills` bootstrap
- `.claude/skills -> ../.agents/skills` compatibility symlink repair path
- built-in `skill-authoring-policy` skill
- prompt-contract `SKILLS_GOVERNANCE` section
- startup diagnostics + tests for skills layout/governance

### WS20-T1 - Canonical skills layout in workspace bootstrap
- Extend workspace bootstrap to ensure `.agents/skills/` exists in every workspace.
- Keep `CLAUDE.md -> AGENTS.md` behavior unchanged and add explicit skills-path diagnostics.
- Done criteria: cold workspace bootstrap creates skills root deterministically.

### WS20-T2 - Claude compatibility symlink policy
- Ensure `.claude/skills` exists as a symlink to `.agents/skills`; repair drifted links/files idempotently.
- Enforce path safety invariants (no directory traversal, no non-symlink collisions without explicit repair path).
- Done criteria: repeated bootstrap/repair keeps Claude skills path aligned with canonical path.

### WS20-T3 - Built-in skill authoring policy
- Add a built-in required skill that instructs agents creating/updating skills to place them only under `.agents/skills`.
- Include explicit folder structure, naming rules, and frontmatter requirements in this governance skill.
- Done criteria: prompts and runtime context expose this policy before skill-authoring tasks.

### WS20-T4 - Multi-backend skill loading contract (Codex + OpenCode + Claude)
- Formalize and document skills discovery policy using official backend docs:
  - Codex reads `.agents/skills`.
  - OpenCode reads `.agents/skills`.
  - Claude Code uses `.claude/skills` compatibility path.
- Update docs: `crab/DESIGN.md`, `crab/docs/06-backend-contract-and-inference-profiles.md`, and `crab/docs/10-target-machine-operations.md`.
- Done criteria: backend adapters and docs align on one canonical skills source of truth.

### WS20-T5 - Skills governance tests and diagnostics
- Add tests for bootstrap creation, symlink repair, governance skill presence, and context-injection ordering.
- Add runtime diagnostics note showing resolved canonical skills path and compatibility path status.
- Done criteria: skills layout/governance behavior is fully covered and visible in startup diagnostics.

### WS21 - Workspace Private Git Persistence

Status (as of 2026-02-11): WS21-T1 through WS21-T6 completed.

### WS21-T1 - Git persistence config model
- Add explicit runtime config for workspace git persistence (`enabled`, `remote`, `branch`, commit identity, push policy).
- Validate config eagerly and fail fast on malformed/unsafe values.
- Done criteria: config surface is documented and covered by parser validation tests.

### WS21-T2 - Repository bootstrap and binding
- Initialize workspace as git repo when enabled, or bind to existing repo safely.
- Support private remote setup and deterministic branch bootstrap policy.
- Done criteria: first-run bootstrap creates/validates repo state without mutating unrelated paths.

### WS21-T3 - Commit trigger policy and metadata
- Define when commits are created (for example on successful run finalization and rotation checkpoints).
- Standardize commit message schema and include run/session correlation metadata.
- Done criteria: commit cadence is deterministic and replay-safe under restart conditions.
 - Implemented:
   - `crab-core::maybe_commit_workspace_snapshot` with strict request validation and deterministic commit key schema.
   - Trigger wiring in `TurnExecutor` for successful run finalization plus rotation checkpoints.
   - Standardized commit trailers (`Crab-Trigger`, `Crab-Logical-Session-Id`, `Crab-Run-Id`, `Crab-Checkpoint-Id`, `Crab-Run-Status`, `Crab-Commit-Key`) with replay-safe duplicate-key detection.

### WS21-T4 - Async push queue with retry/backoff
- Push to private remote asynchronously with bounded retry/backoff and durable retry state.
- Ensure push failures never block turn execution or Discord delivery semantics.
- Done criteria: push failure/recovery tests prove runtime liveness and eventual sync.
 - Implemented:
   - Durable queue model in `crab-core` (`WorkspaceGitPushRequest`, enqueue/tick outcomes, persisted queue state in `state/workspace_git_push_queue.json`).
   - Idempotent enqueue by `commit_key` with conflict rejection when the same key is reused with a different commit id.
   - Bounded retry/backoff (`WORKSPACE_GIT_PUSH_MAX_ATTEMPTS` + exponential backoff with cap) and deterministic exhaustion state.
   - Daemon-loop integration that processes queue ticks non-blockingly and never blocks message intake/dispatch when push attempts fail.
   - Recovery tests for retry scheduling, retry exhaustion, and success after remote recovery.

### WS21-T5 - Secret safety and path guardrails
- Enforce exclusion policy (`.env`, secrets files, token dumps, generated transient artifacts) before commit.
- Add allowlist/denylist guardrails with explicit audit notes when files are skipped.
- Done criteria: sensitive files cannot be committed by automated persistence path.
 - Implemented:
   - Deterministic staging policy in `crab-core::stage_workspace_changes` with deny rules for dotenv/secret/credential/key/transient paths and explicit allow overrides for `.env.example`/`.env.sample`/`.env.template`.
   - Commit trailers include staging policy metadata (`Crab-Staging-Policy-Version`, `Crab-Staging-Skipped-Count`, `Crab-Staging-Skipped-Rules`).
   - `WorkspaceGitCommitOutcome.staging_skipped_paths` is emitted and `TurnExecutor` logs skipped paths for operator audit visibility.

### WS21-T6 - Divergence and conflict recovery policy
- Handle non-fast-forward, force-pushed remote, and local divergence cases with deterministic fallback behavior.
- Provide operator-visible recovery commands and clear diagnostics.
- Update docs: `crab/docs/05-reliability-delivery-and-recovery.md` and `crab/docs/10-target-machine-operations.md`.
- Done criteria: divergence scenarios are tested and recoverable without workspace corruption.
 - Implemented:
   - Push failure classifier in `crab-core` marks non-fast-forward/diverged-history failures as `manual_recovery_required` (no wasteful retry loop).
   - `WorkspaceGitPushTickOutcome` now carries `failure_kind` and deterministic `recovery_commands`.
   - Queue entries are exhausted immediately for manual-recovery classes, preserving runtime liveness and preventing endless retry churn.

### WS22 - Runtime State Evolution and Migration Safety

Status (as of 2026-02-12): scoped deliverables complete (`WS22-T1` to `WS22-T4` implemented;
`WS22-T5` design note complete with command implementation intentionally deferred).

### WS22-T1 - Global state schema marker + startup migrator pipeline
- Add a global state schema marker at `state/schema_version.json` with explicit schema (`version`, `updated_at_epoch_ms`).
- Run a startup migration engine before runtime stores/process loops begin:
  - stepwise migrations (`vN -> vN+1`)
  - deterministic ordering
  - idempotent reruns
  - explicit lock to prevent concurrent migrator execution
- Emit structured migration telemetry (`migration_started`, `migration_step`, `migration_completed`, `migration_failed`).
- Update docs: `crab/docs/07-storage-and-state-model.md`, `crab/docs/10-target-machine-operations.md`, and `README.md`.
- Done criteria: cold start initializes marker, older state upgrades deterministically, partial-failure rerun resumes safely, and no-op rerun is proven by tests.
 - Implemented:
   - `crab-core::state_schema` module with marker schema (`state/schema_version.json`) and stepwise migrator.
   - Startup migration lock (`schema_migration.lock.json`) with stale-lock reclamation and active-lock blocking.
   - Startup integration in composition path so migration runs before runtime stores/process loops.
   - Structured migration event model (`migration_started`, `migration_step`, `migration_completed`, `migration_failed`) persisted in startup outcome and logged by daemon startup.

### WS22-T2 - Actionable compatibility preflight in `crabctl doctor` and `crabctl upgrade`
- Add a shared preflight check consumed by both `doctor` and `upgrade`.
- Block incompatible upgrades with explicit operator remediation output:
  - detected state version and supported binary range
  - exact blocking reason (for example, missing migration step)
  - exact next-action commands (upgrade path or rollback path)
- Use a distinct non-zero exit code for "upgrade blocked by state compatibility".
- Update docs: `crab/docs/10-target-machine-operations.md` and `README.md`.
- Done criteria: blocked upgrade output is directly actionable without code lookup, and behavior is covered by integration tests.
 - Implemented:
   - Shared compatibility preflight in installer path using state marker + supported range evaluation.
   - `crabctl upgrade` now blocks incompatible state before any install mutation and returns distinct exit code `3`.
   - `crabctl doctor` now reports explicit state-schema compatibility pass/fail and remediation commands.

### WS22-T3 - Migration test harness and case-by-case fixtures
- Build reusable migration fixture harness for state snapshots and startup migration execution.
- For each introduced migration, add targeted fixtures/tests:
  - `N-1 -> N` forward migration
  - idempotent rerun
  - restart/resume after interrupted migration step
- Keep coverage policy unchanged (100% for touched production paths).
- Done criteria: each migration step ships with deterministic fixtures and regression tests.
 - Implemented for current `v0 -> v1` step:
   - deterministic migration tests for cold-start initialization and idempotent rerun
   - stale-lock recovery and active-lock blocking tests
   - corrupt-marker regression test
   - installer integration tests for upgrade-blocked and doctor-preflight reporting paths

### WS22-T4 - Schema evolution policy and contributor guardrails
- Establish and document policy: persisted schema changes must be additive-only unless paired with a migration.
- Add contributor checklist updates for persisted-state changes:
  - schema version impact decision
  - migration required/not required with justification
  - compatibility and rollback notes
  - required tests
- Update docs: `AGENTS.md`, `crab/DESIGN.md`, and `crab/docs/07-storage-and-state-model.md`.
- Done criteria: policy is explicit in contributor docs and referenced by implementation tasks/PRs.
 - Implemented:
   - contributor policy added to `AGENTS.md` (additive-only schema default; migration requirements when compatibility changes)
   - design/state docs updated with marker+migration+preflight behavior

### WS22-T5 - (Deferred) snapshot/restore command path for upgrades
- Evaluate and design a minimal pre-upgrade snapshot/restore path (`workspace + state + env metadata`) in `crabctl`.
- Keep this out of immediate deployment critical path; implement after WS22-T1/T2/T4 unless risk profile changes.
- Done criteria: decision doc exists with complexity estimate and go/no-go recommendation for implementation scope.
 - Implemented:
   - decision note added in `crab/docs/12-snapshot-restore-design-note.md`
   - options/tradeoffs documented (no snapshot vs minimal local `crabctl` snapshot/restore vs full platform integration)
   - recommendation captured: implement minimal local snapshot/restore in next hardening wave after deployment acceptance
   - trigger conditions defined for promoting this deferred work to active implementation

### WS23 - Session Context Lifecycle Hardening

### WS23-T1 - Stop inline `AGENTS.md` injection in runtime turn context
- Remove `AGENTS.md` from inline context assembly payload.
- Keep `AGENTS.md` as workspace policy file and `CLAUDE.md -> AGENTS.md` compatibility link.
- Ensure prompt contract remains explicit as its own context section.
- Done criteria: runtime context no longer contains an `AGENTS.md` section.
 - Implemented:
   - `ContextAssemblyInput` no longer carries `agents_document`; replaced with explicit `prompt_contract`.
   - daemon context assembly no longer reads/inlines `AGENTS.md`.

### WS23-T2 - Inject full bootstrap context once per physical session
- Inject full context only when physical session is newly materialized.
- Reused physical sessions should receive only raw turn input.
- Preserve checkpoint/bootstrap continuity for newly created sessions.
- Done criteria: repeated turns in same physical session do not replay large static context.
 - Implemented:
   - `TurnExecutor` now passes `inject_bootstrap_context` based on `physical_session.last_turn_id`.
   - daemon runtime returns raw `run.user_input` when bootstrap injection is not requested.

### WS23-T3 - Replace char truncation with token-capped managed docs and full injection
- Remove character-based truncation and truncation markers.
- Enforce strict token budgets for managed docs and bootstrap sections.
- Fail fast with explicit invariant errors on overflow.
- Done criteria: no silent context truncation paths remain.
 - Implemented:
   - `context_budget` now enforces token budgets (`SOUL`/`IDENTITY`/`USER` 2048, `MEMORY` 16000, `PROMPT_CONTRACT` 4096, checkpoint/turn input 4096).
   - budget overflow is explicit error; no truncation/drop markers are produced.

### WS23-T4 - Remove synthetic OpenCode reasoning suffix
- Stop appending synthetic `[opencode_reasoning_guidance]` prompt suffix.
- Map reasoning to native OpenCode session/turn controls directly.
- Done criteria: OpenCode prompts are unmodified turn context and reasoning is carried by config.
 - Implemented:
   - `map_opencode_inference_profile` now always maps native reasoning level directly.
   - daemon OpenCode bridge now sends turn context as-is with no guidance suffix path.

### WS23-T5 - Context lifecycle docs and coverage hardening
- Update architecture docs to reflect bootstrap-only injection and token-cap policy.
- Add/adjust tests across core/runtime to lock semantics.
- Done criteria: docs and tests are aligned with runtime behavior.
 - Implemented:
   - context assembly/budget/diagnostics tests updated for new schema and policy.
   - daemon and turn-executor tests assert bootstrap-only context injection behavior.
   - design + handbook docs updated (`DESIGN.md`, `02`, `04`, `06`, `08`, `11`).

### WS24 - Onboarding Runtime Gate and Conversational Completion

### WS24-T1 - Enforce pending-onboarding owner-DM gate
- While `BOOTSTRAP.md` exists, allow only owner DM traffic to proceed.
- Non-owner and owner non-DM turns should be blocked with explicit user-facing guidance and audit notes.
- Done criteria: pending-bootstrap traffic policy is deterministic and covered by runtime tests.
 - Implemented:
   - `TurnExecutor::resolve_pending_onboarding_gate` enforces owner-DM-only execution while bootstrap is pending.
   - blocked turns append run-note diagnostics and emit deterministic Discord responses.

### WS24-T2 - Add rotation-time onboarding extraction for conversational first-run
- During rotation, when bootstrap is still pending in owner DM sessions, run hidden extraction to resolve strict onboarding capture from conversation context.
- If extraction is incomplete, continue rotation without completing onboarding and emit diagnostics.
- Done criteria: successful, incomplete, parse-error, and apply-error paths are deterministic and tested.
 - Implemented:
   - `build_onboarding_extraction_prompt` + `ONBOARDING_CAPTURE_INCOMPLETE_TOKEN` added in core onboarding module.
   - `TurnExecutor::maybe_complete_pending_onboarding_from_rotation` added with explicit note events for incomplete/parse/apply outcomes.

### WS24-T3 - Add initial physical-session runtime brief context section
- Add a dedicated runtime brief context section for initial physical-session bootstrap context.
- Include onboarding guidance in owner DM while bootstrap is pending.
- Done criteria: runtime brief is injected in context assembly/budget/diagnostics and validated by daemon tests.
 - Implemented:
   - `CRAB_RUNTIME_BRIEF` added to context injection order and strict token budgeting.
   - daemon runtime now renders and injects onboarding-aware runtime brief during bootstrap-context assembly.
   - docs updated (`README.md`, onboarding spec, integration matrix, deployment-gap register, overall flow doc).

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
12. WS17 after WS15; required before production deployment cutover
13. WS18 after WS17 and WS3; required for target-machine rollout
14. WS20 after WS10 and WS13; required before deployment so skills behavior is deterministic across backends
15. WS21 after WS15 and WS17; required before deployment evidence is finalized
16. WS19 after WS18, WS20, and WS21; final installation/provisioning cutover on target macOS/Linux hosts
17. WS22 after WS1, WS10, and WS19; required before declaring deployment-readiness closure for in-place upgrades
18. WS23 after WS13 and WS15; required before deployment to avoid duplicate context replay and silent truncation behavior
19. WS24 after WS12, WS13, WS15, and WS23; required before deployment so first-run onboarding remains owner-scoped and conversationally recoverable

Critical path to MVP:

- WS0, WS1, WS2, WS3, WS4, WS7, WS8, WS9, WS10, WS11, WS12, WS13, WS14, WS15, WS16, WS17, WS18, WS20, WS21, WS19, WS22, WS23, WS24

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

### Milestone M7 - Production Deployment Cutover
- Scope: WS17, WS18.
- Exit criteria: runtime gap register resolved, production bot runtime is executable on target machine, and Discord provisioning/service runbooks are validated end-to-end.

### Milestone M8 - Skills + Persistence + Installer Hardening
- Scope: WS19, WS20, WS21.
- Exit criteria: canonical skills policy is enforced across backends, workspace state is durably persisted to private git, and installer-based deployment on macOS/Linux is reproducible and idempotent.

### Milestone M9 - Runtime Evolution Safety
- Scope: WS22, WS23, WS24.
- Exit criteria: runtime state upgrades are safe/actionable; context lifecycle semantics are deterministic (bootstrap-only injection, token-capped no-truncation policy, native backend reasoning mapping); and onboarding remains owner-DM scoped with conversational completion fallback during rotation.

## 6) Issue Template for Task Tickets

Use this exact checklist when creating each task issue:

- Goal
- In-scope changes
- Out-of-scope changes
- Acceptance criteria
- Test plan (including coverage impact)
- Risks
- Rollback plan
