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
SUMMARY_PATH="$ROOT_DIR/coverage/summary.json"
PATCH_COVERAGE_PATH="$ROOT_DIR/coverage/patch-coverage.json"

have() { command -v "$1" >/dev/null 2>&1; }

rustc_version="$(rustc -V)"
cargo_version="$(cargo -V)"
python_version="$(python3 --version)"

node_version="(missing: node)"
if have node; then
  node_version="$(node --version)"
fi

llvm_cov_version="(missing: cargo-llvm-cov)"
if have cargo-llvm-cov; then
  llvm_cov_version="$(cargo-llvm-cov llvm-cov --version 2>/dev/null | head -n 1 || true)"
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

metrics_json="$(python3 scripts/quality_baseline.py --print-metrics)"
metric_values="$(python3 - "$metrics_json" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
repo = payload["repo_metrics"]
churn = payload["churn"]
print(
    repo["production_loc"],
    repo["test_attribute_count"],
    repo["tests_per_kloc"],
    repo["cfg_not_coverage_count"],
    churn["quality_fix_like_commits_last_30d"],
    churn["quality_fix_like_commits_last_90d"],
    sep="\t",
)
PY
)"
IFS=$'\t' read -r production_loc test_attribute_count tests_per_kloc coverage_cfg_not_count quality_churn_30d quality_churn_90d <<<"$metric_values"

workspace_member_count="$(python3 - <<'PY'
import pathlib
import sys
import tomllib

try:
    payload = tomllib.loads(pathlib.Path("Cargo.toml").read_text(encoding="utf-8"))
    print(len(payload["workspace"]["members"]))
except (OSError, UnicodeError, tomllib.TOMLDecodeError, KeyError, TypeError) as error:
    print(f"invalid workspace manifest while generating report: {error}", file=sys.stderr)
    raise SystemExit(2)
PY
)"

tests_total="$({
  cargo test --quiet --workspace --all-features --locked -- --list | python3 -c '
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
  coverage_hotspots_md="$(python3 scripts/lcov_stats.py hotspots --root "$ROOT_DIR" --lcov "$LCOV_PATH")"
fi

coverage_totals_md='Coverage totals unavailable: run `make coverage-gate` to generate fresh summary and patch artifacts.'
if [ -f "$SUMMARY_PATH" ] && [ -f "$PATCH_COVERAGE_PATH" ]; then
  coverage_totals_md="$(python3 - "$SUMMARY_PATH" "$PATCH_COVERAGE_PATH" <<'PY'
import json
import pathlib
import sys

try:
    summary = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    patch = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
    totals = summary["data"][0]["totals"]
    rows = []
    for name, floor in (("functions", "95%"), ("regions", "95%"), ("lines", "95%")):
        item = totals[name]
        rows.append(
            f"| {name.title()} | {item['covered']} / {item['count']} | "
            f"{item['percent']:.4f}% | {floor} |"
        )
    rows.append(
        "| Changed executable production lines | "
        f"{patch['covered_changed_lines']} / {patch['changed_executable_lines']} | "
        f"{patch['patch_percent']:.4f}% | 95% with small-patch floor |"
    )
except (
    OSError,
    UnicodeError,
    json.JSONDecodeError,
    KeyError,
    IndexError,
    TypeError,
    ValueError,
) as error:
    print(f"invalid coverage summary artifact: {error}", file=sys.stderr)
    raise SystemExit(2)

print("| Measure | Covered / total | Result | Floor |")
print("|---|---:|---:|---:|")
print("\n".join(rows))
PY
)"
fi

baseline_latest_md='No recorded baseline yet. Run `make quality-baseline` to capture one.'
baseline_trend_md='No baseline trend yet. Capture at least two baselines with `make quality-baseline`.'

if [ -f "$BASELINE_LATEST_PATH" ]; then
  baseline_latest_md="$(python3 - "$BASELINE_LATEST_PATH" <<'PY'
import json
import pathlib
import sys

try:
    payload = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8'))
except (OSError, UnicodeError, json.JSONDecodeError) as error:
    print(f"invalid baseline latest artifact: {error}", file=sys.stderr)
    raise SystemExit(2)
if not isinstance(payload, dict):
    print("invalid baseline latest artifact: root must be a JSON object", file=sys.stderr)
    raise SystemExit(2)
runtime = payload.get('runtime_seconds', {})
repo = payload.get('repo_metrics', {})
churn = payload.get('churn', {})
if not all(isinstance(value, dict) for value in (runtime, repo, churn)):
    print("invalid baseline latest artifact: metric groups must be objects", file=sys.stderr)
    raise SystemExit(2)
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

def render() -> None:
    entries = []
    for line in pathlib.Path(sys.argv[1]).read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line:
            continue
        payload = json.loads(line)
        if not isinstance(payload, dict):
            raise ValueError("each history line must be a JSON object")
        entries.append(payload)

    if len(entries) < 2:
        print("No baseline trend yet. Capture at least two baselines with `make quality-baseline`.")
        return

    prev = entries[-2]
    last = entries[-1]
    prev_rt = prev.get('runtime_seconds', {})
    last_rt = last.get('runtime_seconds', {})
    prev_repo = prev.get('repo_metrics', {})
    last_repo = last.get('repo_metrics', {})
    if not all(isinstance(value, dict) for value in (prev_rt, last_rt, prev_repo, last_repo)):
        raise ValueError("history metric groups must be objects")

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

try:
    render()
except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as error:
    print(f"invalid baseline history artifact: {error}", file=sys.stderr)
    raise SystemExit(2)
PY
)"
fi

python3 scripts/publish_report.py --out-path "$OUT_PATH" <<REPORT
This report is generated by \`scripts/gen_code_quality_report.sh\`.

## Tool Versions

- $rustc_version
- $cargo_version
- $python_version
- $llvm_cov_version
- $cargo_audit_version
- node $node_version
- $scc_version
- jscpd 4.0.5 (explicit prerequisite)

## Enforced Quality Gates

All required gates are defined in \`Makefile\` and enforced by CI in \`.github/workflows/quality.yml\`:

- \`fmt\`: \`cargo fmt --all -- --check\`
- \`clippy\`: repository policy with rustc warnings and correctness/suspicious/perf denied;
  style/complexity force-warned
- \`tests\`: full workspace tests in the normal build configuration (the only gate compiling
  \`cfg(not(any(test, coverage)))\` production paths)
- \`public-api\`: cross-file public API wiring check
- \`duplication\`: production Rust \`jscpd\` gate (threshold 0)
- \`gate-tests\`: deterministic offline workflow tests
- \`coverage\`: fresh \`cargo llvm-cov\` gate: 95% functions, regions, lines, and patch coverage
  (the workspace suite is re-executed under instrumentation and \`cfg(coverage)\`)

Canonical agent loop:

- \`make doctor\`
- \`make check\`
- \`make quality\`

The authoritative gate writes a fingerprinted \`quality/status.json\` and rejects required skips.

## Size (SLOC)

$sloc_table_md

## Runtime + Density Metrics

- Workspace crates: $workspace_member_count
- Production LOC (production Rust, excluding \`src/test_support.rs\`): $production_loc
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

$coverage_totals_md

Aggregate production coverage excludes every \`src/test_support.rs\` through the shared export
policy. Hotspots below use LCOV \`LF\`/\`LH\` totals; \`DA:0\` rows supply locations only.

$coverage_hotspots_md

## Tests

- Total tests (workspace): $tests_total

## Dependency Health (cargo-audit)

    --- cargo-audit output ---
$cargo_audit_output
    --- end ---
REPORT

echo "Wrote $OUT_PATH"
