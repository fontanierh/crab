# Feature Integration Matrix

## Purpose

This document maps major Crab feature surfaces to runtime wiring points so we can verify that
core modules are actually exercised in production flow.

## Status Snapshot (February 12, 2026)

## Fully Wired

- Run profile resolution:
  - Core: `sender_identity`, `trust`, `profile`
  - Runtime: `crates/crab-app/src/daemon.rs` (`resolve_run_profile`)
  - Coverage: daemon runtime tests
- Turn context assembly pipeline:
  - Core: `prompt_contract`, `memory_snippets`, `context_assembly`, `context_budget`,
    `context_diagnostics`
  - Runtime: `crates/crab-app/src/daemon.rs` (`build_turn_context`)
  - Coverage: daemon context wiring tests + full workspace integration tests
- Rotation trigger + checkpoint persistence:
  - Core: `rotation`, `rotation_sequence`, `checkpoint_fallback`
  - Runtime: `crates/crab-app/src/turn_executor.rs` (`maybe_execute_rotation*`)
  - Coverage: turn executor rotation tests
- Startup reconciliation + heartbeat:
  - Core: `startup_reconciliation`, `heartbeat`
  - Runtime: `crates/crab-app/src/maintenance.rs`
  - Coverage: maintenance + daemon loop tests
- Memory CLI surface:
  - Core: `memory_search`, `memory_get`
  - Runtime: `crates/crab-app/src/memory_cli.rs` and binaries
  - Coverage: CLI tests
- Daemon backend execution bridge:
  - Runtime: `crates/crab-app/src/daemon.rs` (`execute_backend_turn`, `DaemonBackendBridge`)
  - Codex path executes through `crates/crab-app/src/daemon_backend_bridge.rs`.
  - OpenCode path executes through `OpenCodeExecutionBridge` transport-backed runtime path.
  - Claude path executes through `DaemonClaudeExecutionBridge` in daemon runtime flow.
  - Coverage: daemon runtime integration tests
- Hidden checkpoint backend turn:
  - Core: `checkpoint_turn`, `checkpoint_fallback`, `rotation_sequence`
  - Runtime: `crates/crab-app/src/turn_executor.rs` (`run_hidden_checkpoint_turn`)
  - Uses the same runtime backend execution path as normal turns
  - Enforces strict schema parse/retry behavior; fallback only when backend output/execution fails
  - Coverage: turn executor rotation/checkpoint tests
- OpenCode recovery helper integration:
  - Core: `recover_opencode_session`
  - Runtime: `crates/crab-app/src/daemon.rs` (`OpenCodeExecutionBridge::recover_session_with_helper`)
  - Materialization/recovery path now uses shared recovery primitive instead of ad-hoc retry-only session creation
  - Coverage: daemon OpenCode execution bridge tests + HTTP transport end-session contract test
- First-interaction onboarding completion capture:
  - Core: `parse_onboarding_capture_document`, `persist_onboarding_profile_files`,
    `execute_onboarding_completion_protocol`
  - Runtime: `crates/crab-app/src/turn_executor.rs` (`maybe_complete_pending_onboarding_capture`)
  - Behavior: while bootstrap is pending, owner-submitted onboarding capture JSON is validated, profile files are updated, `MEMORY.md` baseline is written, and `BOOTSTRAP.md` is retired in normal run flow
  - Coverage: turn executor onboarding completion/rejection tests

## Deferred / Partial

- First-interaction prompt orchestration:
  - Onboarding completion capture is runtime-wired for strict owner JSON submissions while bootstrap
    is pending.
  - Automatic hidden question-asking orchestration that elicits that JSON capture from arbitrary
    first messages is still deferred.

## API Wiring Guardrail

- Quality gate: `make public-api-check`
- Script: `scripts/public_api_usage_check.sh`
- Policy: every `pub fn` must have at least one cross-file usage, otherwise reduce visibility or
  wire it.
