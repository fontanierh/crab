# Feature Integration Matrix

## Purpose

This document maps major Crab feature surfaces to runtime wiring points so we can verify that
core modules are actually exercised in production flow.

## Status Snapshot (February 11, 2026)

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

## Partially Wired / Fallback-Only

- Daemon backend execution bridge:
  - `crates/crab-app/src/daemon.rs` now delegates turn execution via a backend bridge.
  - Codex path uses the `daemon_backend_bridge` transport seam.
  - OpenCode path now uses transport-backed runtime execution and maps OpenCode API responses into
    normalized backend events (including usage metadata from backend envelopes).
  - Claude daemon execution path is not yet wired through this bridge.
- Hidden checkpoint backend turn:
  - Core primitives are wired in runtime (`build_checkpoint_prompt`, parse/resolve helpers,
    backend checkpoint-turn execution in `crates/crab-app/src/turn_executor.rs`).
  - Runtime enforces strict checkpoint schema parsing/validation, retries once, and uses
    deterministic fallback only when backend checkpoint-turn execution/output fails.
  - Impact: wiring is in place, but daemon backend execution is still stubbed so production daemon
    runs do not yet emit real provider checkpoint content.

## Not Yet Runtime-Wired

- Backend session recovery helpers:
  - `recover_codex_session` is invoked via `crates/crab-app/src/daemon_backend_bridge.rs`.
  - `recover_opencode_session` exists in `crab-backends` but is not yet used by app runtime
    wiring.
- First-interaction onboarding runtime path:
  - Onboarding schema/prompt/completion modules are implemented and tested.
  - Automatic first-turn runtime orchestration for onboarding capture is still pending integration.

## API Wiring Guardrail

- Quality gate: `make public-api-check`
- Script: `scripts/public_api_usage_check.sh`
- Policy: every `pub fn` must have at least one cross-file usage, otherwise reduce visibility or
  wire it.
