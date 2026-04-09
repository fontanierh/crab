#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$({
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
})"

cd "$ROOT_DIR"

OUT_PATH="${1:-CODE_QUALITY_REPORT.md}"
BASELINE_LATEST_PATH="$ROOT_DIR/quality/baselines/latest.json"
BASELINE_HISTORY_PATH="$ROOT_DIR/quality/baselines/history.jsonl"
LCOV_PATH="$ROOT_DIR/coverage/lcov.info"

have() { command -v "$1" >/dev/null 2>&1; }

git_sha="$(git rev-parse HEAD)"
git_commit_date="$(git show -s --format=%cs HEAD)"

rustc_version="$(rustc -V)"
cargo_version="$(cargo -V)"

node_version="(missing: node)"
if have node; then
  node_version="$(node --version)"
fi

llvm_cov_version="(missing: cargo-llvm-cov)"
if have cargo-llvm-cov || cargo llvm-cov --version >/dev/null 2>&1; then
  llvm_cov_version="$(cargo llvm-cov --version 2>/dev/null | head -n 1 || true)"
fi

cargo_audit_version="(missing: cargo-audit)"
cargo_audit_output="(cargo-audit not installed)"
if have cargo-audit || cargo audit --version >/dev/null 2>&1; then
  cargo_audit_version="$(cargo audit --version 2>/dev/null | head -n 1 || true)"
  set +e
  cargo_audit_output="$(cargo audit -q 2>&1)"
  audit_exit="$?"
  set -e
  if [ "$audit_exit" -ne 0 ]; then
    cargo_audit_output="$cargo_audit_output"$'\n'"(exit code: $audit_exit)"
  fi
fi

scc_version="(missing: scc)"
sloc_table_md="(missing: install scc)"
if have scc; then
  scc_version="$(scc --version)"
  sloc_table_md="$(printf '```text\n%s\n```' "$(scc crates)")"
fi

production_loc="$(find crates -type f -path '*/src/*.rs' -print0 | xargs -0 wc -l | tail -n 1 | awk '{print $1}')"
test_attribute_count="$(rg -n '^\s*#\[(tokio::)?test\]' crates | wc -l | tr -d ' ')"
tests_per_kloc="$(python3 - "$production_loc" "$test_attribute_count" <<'PY'
import sys
loc = float(sys.argv[1])
tests = float(sys.argv[2])
if loc <= 0:
    print('0.00')
else:
    print(f"{(tests / (loc / 1000.0)):.2f}")
PY
)"

coverage_cfg_not_count="$(rg -n 'cfg\(not\(coverage\)\)' crates | wc -l | tr -d ' ')"
quality_churn_30d="$(git log --since='30 days ago' --oneline --grep='coverage\|quality[- ]gate' | wc -l | tr -d ' ')"
quality_churn_90d="$(git log --since='90 days ago' --oneline --grep='coverage\|quality[- ]gate' | wc -l | tr -d ' ')"

tests_total="$({
  cargo test --workspace --all-features --locked -- --list | python3 -c '
import sys
count = 0
for raw in sys.stdin:
    line = raw.strip()
    if line.endswith(": test"):
        count += 1
print(count)
'
})"

coverage_hotspots_md='Coverage hotspot view unavailable: run `make coverage` first to generate `coverage/lcov.info`.'
if [ -f "$LCOV_PATH" ]; then
  coverage_hotspots_md="$(python3 - "$LCOV_PATH" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
current = None
counts = {}
for raw in path.read_text(encoding="utf-8", errors="replace").splitlines():
    if raw.startswith("SF:"):
        current = raw[3:]
        counts.setdefault(current, 0)
    elif raw.startswith("DA:") and current is not None:
        _, hits = raw[3:].split(",", 1)
        if int(hits) == 0:
            counts[current] = counts.get(current, 0) + 1

rows = [(name, value) for name, value in counts.items() if value > 0]
rows.sort(key=lambda item: (-item[1], item[0]))
if not rows:
    print("No uncovered lines reported in coverage/lcov.info.")
else:
    print("| File | Uncovered lines |")
    print("|---|---:|")
    for name, value in rows[:15]:
        print(f"| `{name}` | {value} |")
PY
)"
fi

baseline_latest_md="No recorded baseline yet. Run `make quality-baseline` to capture one."
baseline_trend_md="No baseline trend yet. Capture at least two baselines with `make quality-baseline`."

if [ -f "$BASELINE_LATEST_PATH" ]; then
  baseline_latest_md="$(python3 - "$BASELINE_LATEST_PATH" <<'PY'
import json
import pathlib
import sys

payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8'))
runtime = payload.get('runtime_seconds', {})
repo = payload.get('repo_metrics', {})
churn = payload.get('churn', {})
print(f"- Captured at: `{payload.get('timestamp_utc', 'unknown')}`")
print(f"- Baseline commit: `{payload.get('git_sha', 'unknown')}`")
print(f"- `make test`: `{runtime.get('make_test', 'n/a')}s`")
print(f"- `make coverage-gate`: `{runtime.get('make_coverage_gate', 'n/a')}s`")
print(f"- Production LOC: `{repo.get('production_loc', 'n/a')}`")
print(f"- Tests/KLOC: `{repo.get('tests_per_kloc', 'n/a')}`")
print(f"- `cfg(not(coverage))` occurrences: `{repo.get('cfg_not_coverage_count', 'n/a')}`")
print(f"- Uncovered lines (lcov snapshot): `{repo.get('uncovered_lines', 'n/a')}`")
print(f"- Files with uncovered lines (lcov snapshot): `{repo.get('uncovered_files', 'n/a')}`")
print(f"- Quality-fix-like commits (30d): `{churn.get('quality_fix_like_commits_last_30d', 'n/a')}`")
print(f"- Quality-fix-like commits (90d): `{churn.get('quality_fix_like_commits_last_90d', 'n/a')}`")
PY
)"
fi

if [ -f "$BASELINE_HISTORY_PATH" ]; then
  baseline_trend_md="$(python3 - "$BASELINE_HISTORY_PATH" <<'PY'
import json
import pathlib
import sys

entries = []
for line in pathlib.Path(sys.argv[1]).read_text(encoding='utf-8').splitlines():
    line = line.strip()
    if not line:
        continue
    try:
        entries.append(json.loads(line))
    except json.JSONDecodeError:
        continue

if len(entries) < 2:
    print("No baseline trend yet. Capture at least two baselines with `make quality-baseline`.")
    raise SystemExit(0)

prev = entries[-2]
last = entries[-1]

prev_rt = prev.get('runtime_seconds', {})
last_rt = last.get('runtime_seconds', {})
prev_repo = prev.get('repo_metrics', {})
last_repo = last.get('repo_metrics', {})

def delta(name: str) -> str:
    a = prev_rt.get(name)
    b = last_rt.get(name)
    if a is None or b is None:
        return "n/a"
    change = b - a
    sign = "+" if change >= 0 else ""
    return f"{sign}{change}s (from {a}s to {b}s)"

print(f"- Previous baseline: `{prev.get('timestamp_utc', 'unknown')}` @ `{prev.get('git_sha', 'unknown')}`")
print(f"- Latest baseline: `{last.get('timestamp_utc', 'unknown')}` @ `{last.get('git_sha', 'unknown')}`")
print(f"- `make test` delta: `{delta('make_test')}`")
print(f"- `make coverage-gate` delta: `{delta('make_coverage_gate')}`")
print(
    f"- Uncovered lines delta: "
    f"`{last_repo.get('uncovered_lines', 'n/a')} - {prev_repo.get('uncovered_lines', 'n/a')}`"
)
print(
    f"- Uncovered files delta: "
    f"`{last_repo.get('uncovered_files', 'n/a')} - {prev_repo.get('uncovered_files', 'n/a')}`"
)
PY
)"
fi

cat >"$OUT_PATH" <<REPORT
# Code Quality Report — Crab Project

Generated for commit \`$git_sha\` (commit date: $git_commit_date).

This report is generated by \`scripts/gen_code_quality_report.sh\`.

## Tool Versions

- $rustc_version
- $cargo_version
- $llvm_cov_version
- $cargo_audit_version
- node $node_version
- $scc_version
- jscpd (via \`npx\`)

## Enforced Quality Gates

All required gates are defined in \`Makefile\` and enforced by CI in \`.github/workflows/quality.yml\`:

- \`cargo fmt --check\`
- \`cargo clippy -D warnings\`
- \`cargo check\` (dead code / unused warnings denied at workspace lint level)
- \`bash scripts/public_api_usage_check.sh\`
- \`cargo llvm-cov\` gate: 100% functions, 99% regions, and changed-line coverage on PRs
- \`jscpd\` duplication gate (threshold 0)

Canonical local command:

- \`make quality\`

Fast local command (non-gating):

- \`make quick\`

## Size (SLOC)

$sloc_table_md

## Runtime + Density Metrics

- Production LOC (\`crates/*/src/*.rs\`): $production_loc
- Workspace test attributes (\`#[test]\`, \`#[tokio::test]\`): $test_attribute_count
- Tests/KLOC: $tests_per_kloc
- \`cfg(not(coverage))\` occurrences: $coverage_cfg_not_count
- Quality-fix-like commits (30d): $quality_churn_30d
- Quality-fix-like commits (90d): $quality_churn_90d

## Baseline Snapshot

$baseline_latest_md

## Baseline Trend

$baseline_trend_md

## Coverage Hotspots (from \`coverage/lcov.info\` when present)

$coverage_hotspots_md

## Tests

- Total tests (workspace): $tests_total

## Dependency Health (cargo-audit)

    --- cargo-audit output ---
$cargo_audit_output
    --- end ---
REPORT

echo "Wrote $OUT_PATH"
