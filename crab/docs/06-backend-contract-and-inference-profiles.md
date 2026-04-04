# Backend Contract And Inference Profiles

## Scope

This document defines the backend adapter boundary and model/reasoning profile resolution rules.

## Decisions

- Decision: all backends conform to one runtime contract.
  - Rationale: shared orchestration, replay, and reliability code.

- Decision: profile resolution is explicit and source-tracked.
  - Rationale: deterministic behavior and auditable fallback decisions.

- Decision: unattended mode by default for interactive backend requests.
  - Rationale: autonomous operation on agent-owned machine.

## Unified Backend Contract

`Backend` trait (`crates/crab-backends/src/lib.rs`) requires:

- `create_session`
- `send_turn`
- `interrupt_turn`
- `end_session`

All backend outputs must normalize into `BackendEvent` stream (`TextDelta`, `ToolCall`, `ToolResult`, `RunNote`, `TurnCompleted`, `TurnInterrupted`, `Error`).

## Backend-Specific Notes

### Claude Code (current implementation)

- process-oriented adapter
- resumable backend session id
- usage accounting extracted from Claude usage events and surfaced as run notes

### Codex and OpenCode (historical reference)

> **Note:** The Codex CLI and OpenCode adapters were removed from the implementation (PRs #163 and #164). The notes below are preserved as reference for the backend contract design and for future adapter implementations.

**Codex:**

- persistent `app-server` managed by lifecycle manager
- protocol primitives include:
  - `thread/start`
  - `thread/resume`
  - `turn/start`
  - `turn/interrupt`
- event normalizer maps Codex notifications/requests into shared backend events
- unattended policy handles approval and user-input requests deterministically

**OpenCode:**

- persistent server lifecycle manager
- protocol primitives include create/send/interrupt/end
- event normalizer maps OpenCode stream events into shared backend events

## Skills Discovery Contract

Crab keeps a single canonical skills source of truth in the workspace:

- Canonical path: `.agents/skills`
- Claude compatibility path: `.claude/skills -> ../.agents/skills`

Backend expectations:

- Claude Code: reads `.claude/skills` (symlinked to canonical path by workspace bootstrap)
- Future adapters should read `.agents/skills` (the canonical path).

Skill authoring policy is reinforced at prompt-contract level and by a built-in required skill:
`.agents/skills/skill-authoring-policy/SKILL.md`.

## Inference Profile Resolution

Effective profile components:

- backend
- model
- reasoning level (`none|minimal|low|medium|high|xhigh`)

Resolution precedence (`crates/crab-core/src/profile.rs`):

1. turn override
2. session profile
3. channel override
4. backend default
5. global default

Telemetry captures source for each resolved value.

## Compatibility And Fallback

Compatibility policy (`strict` vs `compatible`) is enforced in core fallback logic.

- `strict`: unsupported request is rejected
- `compatible`: nearest backend-compatible mapping is selected with fallback notes

## Backend Mapping

`crates/crab-backends/src/profile_mapping.rs`:

- Claude Code: map canonical reasoning levels to Claude thinking bands

Historical mappings for Codex (`effort`) and OpenCode (native session/turn controls) are preserved in DESIGN.md sections 5.3 and 5.4 for extensibility reference.

## Operator Controls

Owner-authorized operator commands:

- `/backend <claude>`
- `/model <value>`
- `/reasoning <none|minimal|low|medium|high|xhigh>`
- `/profile`

Changing backend requires rotation (`requires_rotation=true`) and clears active physical handle.

## Current Status

Implemented:

- adapter contract and normalized event shape
- Claude Code manager/protocol/normalization module
- profile resolution, source telemetry, fallback mapping
- owner-only operator command authorization

Note: Codex CLI and OpenCode adapters were removed (PRs #163, #164). The contract remains extensible for future adapters.

Operational note:

- backend process launch flags and host runtime wiring should explicitly enforce harness-owned compaction policy during deployment hardening.
