# Deployment Acceptance Evidence

## Scope

This document is the execution log for `WS18-T5` deployment acceptance.

- Checklist source: `crab/docs/08-deployment-readiness-gaps.md`
- Runbook source: `crab/docs/10-target-machine-operations.md`

Use this file to record evidence links and final go/no-go decision.

## Current State (as of February 12, 2026)

- Repository-level/staging evidence: available (tests + docs + command outputs).
- Target-machine execution evidence: pending (must be recorded before final GO).

## Staging Evidence Map

The table below maps each acceptance item to currently available repository/staging evidence.

| Checklist item | Repository/staging evidence |
| --- | --- |
| Cold workspace first boot creates/repairs required files and links | `crates/crab-core/src/workspace.rs` tests (`ensure_workspace_creates_new_workspace_layout`, link repair tests), `crates/crab-app/src/startup.rs` tests (`initialize_runtime_startup_initializes_workspace_before_runtime`) |
| First owner interaction runs onboarding prompts and persists owner/agent identity context | `crates/crab-app/src/startup.rs` tests (`first_interaction_onboarding_completion_transitions_workspace_to_ready`), onboarding modules in `crates/crab-core/src/onboarding*.rs` |
| Normal owner run processes end-to-end (`ingress -> lane -> backend -> Discord delivery`) | `crates/crab-app/src/daemon.rs` tests (`daemon_loop_dispatches_claude_owner_turn_and_shuts_down_claude_session`), `crates/crab-app/src/turn_executor.rs` end-to-end dispatch tests |
| Non-owner run obeys per-user memory scope and disclosure policy | `crates/crab-core/src/trust.rs` tests, `crates/crab-core/src/memory_snippets.rs` tests (`resolves_non_owner_scope_plus_recent_global_memory`) |
| Restart during/after a run replays missing outbound delivery without duplicate messages/edits | `crates/crab-app/src/turn_executor.rs` tests (`restart_recovery_replays_missing_delivery_and_continues_next_run`, replay delivery tests) |
| Token-threshold compaction trigger executes hidden memory flush + checkpoint + session rotation | `crates/crab-app/src/turn_executor.rs` tests (`token_trigger_rotation_uses_backend_hidden_checkpoint_turn_without_fallback`, fallback variants) |
| Inactivity trigger executes rotation behavior after configured timeout | `crates/crab-core/src/rotation.rs` tests (`triggers_inactivity_when_idle_and_timeout_elapsed`) plus runtime wiring in turn executor tests |
| Manual `/compact confirm` and `/reset confirm` commands execute only for owner and are audited | `crates/crab-app/src/turn_executor.rs` tests for owner-only enforcement and audit event behavior |
| Heartbeat escalates correctly for stalled run/backend/dispatcher scenarios | `crates/crab-app/src/daemon.rs` heartbeat-action tests and `crates/crab-app/src/maintenance.rs` escalation tests |
| Service restart/reboot persistence is validated by operations playbook steps | Runbook exists (`crab/docs/10-target-machine-operations.md`), target-host execution still pending |
| `make quality` passes on deployment commit | `make quality` passed on main at `02280f1` (2026-02-12); rerun `make quality` on final deployment candidate commit before GO |

## Target-Machine Execution Log (Fill During Deployment)

Record each checklist item with explicit pass/fail and evidence path.

| Item | Status (`pass`/`fail`/`n/a`) | Evidence (log path, screenshot, command output) | Notes |
| --- | --- | --- | --- |
| Cold workspace first boot asset creation/repair | pending |  |  |
| Owner onboarding first interaction | pending |  |  |
| Owner normal run E2E | pending |  |  |
| Non-owner memory scope/disclosure behavior | pending |  |  |
| Restart replay without duplicate output | pending |  |  |
| Token-threshold compaction rotation | pending |  |  |
| Inactivity-triggered rotation | pending |  |  |
| Owner-only manual compact/reset commands | pending |  |  |
| Heartbeat stall escalation | pending |  |  |
| Service restart/reboot persistence | pending |  |  |
| `make quality` on deployment commit | pending |  |  |

## Final Decision Block

- Run date:
- Operator:
- Deployment commit SHA:
- `GO` / `NO-GO`:
- Blocking incidents (if any):
- Follow-up actions:
