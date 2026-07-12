# Crab Factory Live Controls Implementation Report

## Scope and provenance

This uncommitted implementation is based on
`60b7244b3ff100172c8775359e0cdc861931cc9a`. It changes `crab-factory`, deterministic tests,
generated quality evidence, and factory documentation only. No Crab production runtime crate or
external service was changed, and the worktree remains uncommitted.

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
  Reads use `O_NOFOLLOW` and opened-file ownership/mode checks. Writers use a bounded controls-lock
  deadline; terminalization blocks behind a live writer and holds the same lock continuously through
  sweep and terminal manifest/status publication. Unsafe or failed sweeps produce idempotent audit
  evidence without preventing an honest terminal result.
- Steering and effort apply at the next prompt boundary. Count knobs apply only at their own cohort
  boundaries; acceptance warns for both too-late plan-critic and reviewer changes, and later
  disposition records reject them honestly. Terminal sweep resolves remaining accepted knobs.
- Applied steering is explicitly delimited before prompt materialization, so prompt artifacts,
  cohort hashes, and worker stdin include the same accumulated bytes while `00-request.md` remains
  unchanged.
- Manifest events are idempotently projected by sequence/knob. Prepared configuration remains
  immutable; effective configuration and model effort follow the ordered applied ledger.
- One shared prepared-run validator serves executor startup and every live-control entry point. It
  rejects symlinked final run-directory components, validates private managed leaves, exact request
  bytes and hashes, immutable root/identity derivations and tool metadata, strict ledger
  timestamps/reasons/transitions, and the effective configuration reconstructed from exact record
  bytes. Startup rejection occurs before worktree creation and emits one idempotent
  `control_invalid` event.
- Status reports lifecycle, launch and active worker PIDs where available, configurations, control
  dispositions, and the last applied sequence in human or JSON form. Legacy manifests remain
  status-readable but reject execution/control writes as predating live controls.

The schema decision is additive-only: manifest schema version remains 1 and no runtime-state
migration is required. Persisted trust fields are validated; permissions/network/nested-agent and
thermonuclear policies are structurally immutable and argv-tested.

Ownership mismatch is enforced in the same fd-based validator as type, symlink, and mode checks,
but an unprivileged deterministic test cannot change a file to another owner. The test matrix
therefore exercises symlink, wrong-type, and wrong-mode failures and does not fake an ownership
transition. A few impossible-after-validation defense-in-depth error regions intentionally remain;
they fail closed and are not exercised through pre-rejected private states.

## Validation completed in this worktree

The authoritative coverage policy is `95%` for functions, regions, lines, and changed executable
production lines (with the documented small-patch floor).

Fresh final-command results, timings, test counts, aggregate/patch percentages, and schema-3
attestation identity are recorded here only after the exact final validation chain completes.

## Operator-side home-document follow-up

This worktree deliberately does not edit `/Users/jim/AGENTS.md`. Its “Crab code factory” section
must be updated outside this worktree: replace `max`/four-critic wording with the `high`, two-plan-
critic, two-Codex-reviewer defaults; document the three override flags; and add the
`status`/`steer`/`configure` commands.
