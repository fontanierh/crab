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
  - Behavior: bootstrap context injected once per physical session; reused sessions send raw user input only
  - Guardrails: token-capped managed docs with explicit failure on budget overflow (no truncation)
  - Observability: bootstrap context size is logged (`injected_context_tokens`, `injected_context_chars`)
  - Coverage: daemon context wiring tests + full workspace integration tests
- Agent-driven rotation + checkpoint persistence:
  - Core: `pending_rotation`, `rotate_cli`, `checkpoint`
  - Runtime: `crates/crab-app/src/turn_executor.rs` (pending rotation pickup after turn),
    `crates/crab-app/src/rotate_cli.rs` (`crab-rotate` binary writes pending rotation signal)
  - Behavior: agent produces checkpoint JSON, invokes `crab-rotate` CLI; runtime picks up
    pending rotation signal after the current turn, persists checkpoint, ends physical session,
    clears session handle. No hidden turns or automatic triggers.
  - Coverage: turn executor rotation tests, rotate CLI tests
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
  - Claude Code path executes through `DaemonClaudeExecutionBridge` in daemon runtime flow.
  - Coverage: daemon runtime integration tests
  - Note: Codex CLI and OpenCode execution bridge paths were removed (PRs #163, #164).
- `crab-rotate` CLI + `rotate-session` skill:
  - Core: `rotate_cli`, `pending_rotation`
  - Runtime: `crates/crab-app/src/rotate_cli.rs` (`crab-rotate` binary)
  - Skill: `.agents/skills/rotate-session/` guides the agent through producing checkpoint JSON
    and invoking `crab-rotate`
  - Behavior: agent-produced checkpoint is written as a pending rotation signal; no hidden
    checkpoint turns or hidden memory flush turns exist
  - Coverage: rotate CLI tests
- OpenCode recovery helper integration (historical; OpenCode adapter removed in PR #164):
  - Core: `recover_opencode_session`
  - Runtime: `crates/crab-app/src/daemon.rs` (`OpenCodeExecutionBridge::recover_session_with_helper`)
  - Materialization/recovery path used shared recovery primitive instead of ad-hoc retry-only session creation
- First-interaction onboarding completion capture:
  - Core: `parse_onboarding_capture_document`, `persist_onboarding_profile_files`,
    `execute_onboarding_completion_protocol`, `build_onboarding_extraction_prompt`
  - Runtime:
    - `crates/crab-app/src/turn_executor.rs` (`resolve_pending_onboarding_gate`,
      `maybe_complete_pending_onboarding_capture`, `maybe_complete_pending_onboarding_from_rotation`)
    - `crates/crab-app/src/daemon.rs` (`render_crab_runtime_brief`)
  - Behavior:
    - while bootstrap is pending, only owner DM is allowed to proceed
    - owner DM includes onboarding guidance in runtime brief
    - strict onboarding capture JSON can complete onboarding directly
    - rotation can run hidden extraction and complete onboarding when strict capture can be resolved
    - completion writes managed profile files, updates `MEMORY.md`, and retires `BOOTSTRAP.md`
  - Coverage: daemon + turn executor onboarding gate/completion/rotation extraction tests

- Discord file/image attachment support:
  - Connector: `crates/crab-discord-connector/src/main.rs` (Serenity attachment extraction)
  - Core types: `GatewayAttachment` in `crates/crab-discord/src/lib.rs`
  - Runtime: `crates/crab-app/src/turn_executor.rs` (`build_user_input_with_attachments`,
    `cleanup_attachment_directory`)
  - Behavior:
    - Attachments are downloaded to `state/attachments/{run_id}/` and annotated in `user_input`
    - Claude Code reads files (including images) via Read tool
    - Attachment directory cleaned up at run finalization
  - Coverage: unit + integration tests in turn_executor + serde round-trip tests in crab-discord

## API Wiring Guardrail

- Quality gate: `make public-api-check`
- Script: `scripts/public_api_usage_check.sh`
- Policy: every `pub fn` must have at least one cross-file usage, otherwise reduce visibility or
  wire it.
