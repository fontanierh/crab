# Crab Factory Live Controls Implementation Report

## Scope and provenance

This uncommitted implementation is based on
`bd08e0f266e7010233e6932f27e06956ec0d1fef`. It changes repository workflow tooling,
`crab-factory`, deterministic tests, and documentation only. No Crab production runtime crate or
external service was changed.

## Implemented behavior

### Workflow findings

1. LCOV validation bounds positive `DA` rows by `LH`, zero rows by `LF-LH`, and requires exact
   agreement when `DA` covers the complete `LF` universe. Contradictory duplicate records cannot be
   merged into plausible evidence.
2. Policy-rejected LCOV is removed from the authoritative name and quarantined as `*.rejected`.
   Shared LCOV and patch-coverage consumers independently reject `test_support.rs` evidence.
3. `coverage-quick` exits 2 when Git cannot determine changed scope instead of reporting a
   successful no-change skip.
4. Managed workflow paths reject every case-insensitive `.git` component. Report publication also
   requires a Markdown destination.
5. `quality-status` validates exact JSON scalar types. Boolean/float integer lookalikes and
   negative, NaN, or infinite durations fail closed.
6. Committed-report absence requires a successful single regular-blob tree lookup. Git lookup,
   object-shape, decoding, or `git show` failures preserve the existing report and exit 2.
7. Agent-workflow documentation now distinguishes directory validation before stale-status removal
   from individual log-leaf validation before any gate execution or log write.

### Factory configuration

- Defaults are effort `high`, two Codex plan critics, and two Codex normal reviewers plus the
  mandatory Fable reviewer.
- Run/start accepts `--effort high|max`, `--plan-critics 1..8`, and
  `--codex-reviewers 1..8`. Launch metadata and manifest schema-version-1 additive optional fields
  record prepared and effective configuration.
- Worker construction uses the effective effort for every Claude and Codex role while preserving
  unrestricted permissions/network, disabled nested fan-out, and advisory mutation checks.
- Cohort-size-aware prompts retain count-independent role markers and identical bytes for every
  parallel member. The mandatory Fable and thermonuclear reviewers remain unconditional.
- Verdict parsing retains the bootstrap recovery: exactly one exact trimmed verdict line may
  appear anywhere; missing, duplicate, or conflicting lines fail closed.

### Audited live controls

- `status`, `steer`, and `configure` operate on typed prepared run directories.
- Private immutable sequence records bind run/request identity and payload hashes. The authenticated
  `controls/state.json` ledger records per-knob accepted/applied/rejected dispositions and is the
  commit point before manifest projection.
- Control publication uses a private staged file, file fsync, atomic rename, and directory fsync.
  Reads use `O_NOFOLLOW` and opened-file ownership/mode checks. Writers and terminalization share a
  bounded controls lock.
- Steering and effort apply at the next prompt boundary. Count knobs apply only at their own cohort
  boundaries; too-late plan-critic changes are rejected honestly. Terminal sweep resolves remaining
  accepted knobs.
- Applied steering is explicitly delimited before prompt materialization, so prompt artifacts,
  cohort hashes, and worker stdin include the same accumulated bytes while `00-request.md` remains
  unchanged.
- Manifest events are idempotently projected by sequence/knob. Prepared configuration remains
  immutable; effective configuration and model effort follow the ordered applied ledger.
- Status reports lifecycle, launch and active worker PIDs where available, configurations, control
  dispositions, and the last applied sequence in human or JSON form. Legacy manifests remain
  status-readable but reject execution/control writes as predating live controls.

The schema decision is additive-only: manifest schema version remains 1 and no runtime-state
migration is required. Persisted trust fields are validated; permissions/network/nested-agent and
thermonuclear policies are structurally immutable and argv-tested.

## Validation completed in this worktree

The authoritative coverage policy remains `99.5%` functions, `98.93%` regions, `99.4%` lines,
and `95%` changed executable production lines (with the documented small-patch floor).

- `make gate-tests` — 161 deterministic offline tests passed.
- `cargo test -p crab-factory --locked` and focused reruns — 54 unit tests, 5 live-control
  integration tests, 4 end-to-end tests (including count bounds 1 and 8), 13 failure tests, and 3
  launch tests passed. The final full workspace test run in `make quick` includes the same suites.
- `make quick` (the deprecated alias for full-scope `make check`) — fmt passed in 2.34s, Clippy
  passed in 5.10s, and workspace tests passed in 136.44s.

Per the review-round execution contract, the factory runs the fresh authoritative `make quality`
coverage/attestation gate after final review. No fresh aggregate-coverage or `quality-status` pass is
claimed by this round-1 remediation report.

## Operator-side home-document follow-up

This worktree deliberately does not edit `/Users/jim/AGENTS.md`. Its “Crab code factory” section
must be updated outside this worktree: replace `max`/four-critic wording with the `high`, two-plan-
critic, two-Codex-reviewer defaults; document the three override flags; and add the
`status`/`steer`/`configure` commands.
