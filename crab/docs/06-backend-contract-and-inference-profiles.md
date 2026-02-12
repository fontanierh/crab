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

### Claude

- process-oriented adapter
- resumable backend session id
- usage accounting extracted from Claude usage events and surfaced as run notes

### Codex

- persistent `app-server` managed by lifecycle manager
- protocol primitives include:
  - `thread/start`
  - `thread/resume`
  - `turn/start`
  - `turn/interrupt`
- event normalizer maps Codex notifications/requests into shared backend events
- unattended policy handles approval and user-input requests deterministically

### OpenCode

- persistent server lifecycle manager
- protocol primitives include create/send/interrupt/end
- event normalizer maps OpenCode stream events into shared backend events

## Skills Discovery Contract

Crab keeps a single canonical skills source of truth in the workspace:

- Canonical path: `.agents/skills`
- Claude compatibility path: `.claude/skills -> ../.agents/skills`

Backend expectations:

- Codex: reads `.agents/skills`
- OpenCode: reads `.agents/skills`
- Claude Code: reads `.claude/skills` (symlinked to canonical path by workspace bootstrap)

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

- Claude: map canonical reasoning levels to Claude thinking bands
- Codex: map reasoning directly to `effort`
- OpenCode:
  - reasoning maps directly to native OpenCode session/turn controls
  - no synthetic reasoning guidance suffix is appended to prompt text

## Operator Controls

Owner-authorized operator commands:

- `/backend <claude|codex|opencode>`
- `/model <value>`
- `/reasoning <none|minimal|low|medium|high|xhigh>`
- `/profile`

Changing backend requires rotation (`requires_rotation=true`) and clears active physical handle.

## Current Status

Implemented:

- adapter contract and normalized event shape
- Claude/Codex/OpenCode manager/protocol/normalization modules
- profile resolution, source telemetry, fallback mapping
- owner-only operator command authorization

Operational note:

- backend process launch flags and host runtime wiring should explicitly enforce harness-owned compaction policy during deployment hardening.
