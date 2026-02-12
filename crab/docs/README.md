# Crab Architecture Docs

This folder is the comprehensive architecture handbook for Crab.

Use `crab/DESIGN.md` as the canonical v1 spec, and use this folder for detailed explanations of decisions, flow semantics, invariants, and implementation notes.

## Reading Order

1. `crab/docs/01-initial-turn-and-onboarding.md`
2. `crab/docs/02-sessions-lanes-and-turn-lifecycle.md`
3. `crab/docs/03-rotation-checkpoint-and-compaction.md`
4. `crab/docs/04-memory-model-and-context.md`
5. `crab/docs/05-reliability-delivery-and-recovery.md`
6. `crab/docs/06-backend-contract-and-inference-profiles.md`
7. `crab/docs/07-storage-and-state-model.md`
8. `crab/docs/08-deployment-readiness-gaps.md`
9. `crab/docs/09-discord-provisioning-and-secrets.md`
10. `crab/docs/10-target-machine-operations.md`
11. `crab/docs/11-feature-integration-matrix.md`
12. `crab/docs/12-snapshot-restore-design-note.md`

## Doc Conventions

- "Decision" sections describe intentional design choices and why they exist.
- "Sequence" sections describe runtime order and side effects.
- "Invariants" sections describe conditions that must always hold.
- "Current status" sections call out behavior that is implemented today versus behavior that is specified but still needs integration.

## Scope

These docs focus on:

- first interaction and onboarding
- logical/physical session model
- queue and run orchestration semantics
- rotation/checkpoint/memory semantics
- reliability, replay, and crash recovery behavior
- backend adapter contract and profile resolution
- persistent state model and deployment hardening gaps
- Discord provisioning and token operations
- target-machine runtime operations and service lifecycle

## Quality Workflow Notes

- Canonical strict gate: `make quality` (coverage gate + duplication gate are blocking).
- Fast local preflight: `make quick` (non-gating).
- Coverage failure diagnostics: `make coverage-diagnostics` (writes actionable uncovered
  line/function details to `coverage/uncovered_locations.txt`).
- Baseline capture/trend inputs: `make quality-baseline` and `make quality-report`.
