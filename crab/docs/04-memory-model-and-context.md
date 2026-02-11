# Memory Model And Context Assembly

## Scope

This document defines memory data layout, retrieval surfaces, and context assembly rules.

## Decisions

- Decision: memory is file-based and human-inspectable.
  - Rationale: transparent debugging and simple durability.

- Decision: per-user scope by default, plus optional global context.
  - Rationale: reduce cross-user bleed while allowing shared durable facts.

- Decision: prompt-first memory recall with CLI support.
  - Rationale: backend-agnostic behavior and deterministic command surface.

- Decision: semantic/vector memory is deferred in v1.
  - Rationale: keep operational complexity low before deployment hardening.

## Workspace Memory Layout

```text
workspace/
  MEMORY.md
  .agents/skills/
  .claude/skills -> ../.agents/skills
  memory/
    users/<discord_user_id>/YYYY-MM-DD.md
    global/YYYY-MM-DD.md
```

`MEMORY.md` is curated long-term memory.

`memory/users/...` and `memory/global/...` are rolling memory logs.

## Memory Snippet Injection

Scoped memory snippet resolver (`crates/crab-core/src/memory_snippets.rs`) behavior:

- user scope files: newest first, limited count
- global scope files: recent window relative to reference date
- deterministic sort before context assembly
- snippets capture whole-file content with line bounds

Defaults:

- max user files: 3
- max global files: 2
- global recency window: 2 days

## Memory Search API

`search_memory` (`crates/crab-core/src/memory_search.rs`) returns ranked snippets with:

- path
- line range
- overall score
- match score
- recency score
- snippet text

Query behavior:

- tokenized keyword/phrase matching
- recency-aware ranking
- deterministic ordering/tie-breaks

## Memory Get API

`get_memory` (`crates/crab-core/src/memory_get.rs`) returns exact line ranges from allowed paths.

Safety invariants:

- path must be relative with `/`
- no `.` or `..` segments
- only allowed roots:
  - `MEMORY.md`
  - `memory/users/<caller_scope>/...`
  - `memory/global/...` when global enabled
- file and path components must not be symlinks

## CLI Surface

Backend-neutral CLI commands in `crab-app`:

- `crab-memory-search`
- `crab-memory-get`

CLI wrappers enforce argument validation and return structured JSON output for agent use.

## Context Injection Order

Context assembly order (`crates/crab-core/src/context_assembly.rs`):

1. `SOUL.md`
2. `IDENTITY.md`
3. `AGENTS.md`
4. `USER.md`
5. `MEMORY.md`
6. memory snippets
7. latest checkpoint summary
8. turn input

## Context Budgeting And Truncation

`render_budgeted_turn_context` (`crates/crab-core/src/context_budget.rs`) applies:

- per-section char budget
- turn-input budget
- per-snippet budget
- max snippet count
- total context budget

Truncation markers are explicit and persisted in diagnostics-friendly form.

## Citation And Disclosure Policy

Prompt contract compiler (`crates/crab-core/src/prompt_contract.rs`) injects memory recall policy:

- `citation_mode=auto|on|off`
- surface-aware policy (DM vs shared contexts)
- owner vs non-owner disclosure behavior
- same policy applies to CLI search/get and native grep/read fallback

Skill governance is injected in the same prompt contract:

- canonical skills root is `.agents/skills`
- `.claude/skills` is compatibility-only symlink path
- built-in required policy skill lives at `.agents/skills/skill-authoring-policy/SKILL.md`

## Current Status

Implemented:

- memory search/get core + CLI
- scoped snippet resolution
- deterministic context assembly and budgeting
- citation/disclosure prompt policy
- runtime context wiring in `DaemonTurnRuntime::build_turn_context`:
  - compiles prompt contract per run profile/surface
  - injects `SOUL.md`/`IDENTITY.md`/`AGENTS.md`/`USER.md`/`MEMORY.md`
  - resolves scoped memory snippets (`memory/users/<scope>` + recent global)
  - injects latest checkpoint summary from persistent checkpoint store
  - emits deterministic context diagnostics fixture to tracing

Deferred:

- embedding/vector semantic retrieval
