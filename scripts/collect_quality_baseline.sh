#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

OUT_DIR="${1:-$ROOT_DIR/quality/baselines}"
mkdir -p "$OUT_DIR"

LATEST_PATH="$OUT_DIR/latest.json"
HISTORY_PATH="$OUT_DIR/history.jsonl"

cd "$ROOT_DIR"

timestamp_utc="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
git_sha="$(git rev-parse HEAD)"

run_timed() {
  local label="$1"
  shift
  local log_path="$OUT_DIR/${label}.log"
  local start_epoch
  local end_epoch
  start_epoch="$(date +%s)"
  "$@" >"$log_path" 2>&1
  end_epoch="$(date +%s)"
  echo $((end_epoch - start_epoch))
}

echo "Collecting baseline: make test"
test_seconds="$(run_timed test make test)"

echo "Collecting baseline: make coverage-gate"
coverage_gate_seconds="$(run_timed coverage-gate make coverage-gate)"

production_loc="$(find crates -type f -path '*/src/*.rs' -print0 | xargs -0 wc -l | tail -n 1 | awk '{print $1}')"
test_count="$(rg -n '^\s*#\[(tokio::)?test\]' crates | wc -l | tr -d ' ')"
coverage_cfg_not_count="$(rg -n 'cfg\(not\(coverage\)\)' crates | wc -l | tr -d ' ')"
quality_churn_30d="$(git log --since='30 days ago' --oneline --grep='coverage\|quality[- ]gate' | wc -l | tr -d ' ')"
quality_churn_90d="$(git log --since='90 days ago' --oneline --grep='coverage\|quality[- ]gate' | wc -l | tr -d ' ')"
coverage_lcov_path="$ROOT_DIR/coverage/lcov.info"

uncovered_summary="$(python3 - "$coverage_lcov_path" <<'PY'
from __future__ import annotations

import pathlib
import sys

lcov = pathlib.Path(sys.argv[1])
if not lcov.exists():
    print("0 0")
    raise SystemExit(0)

current_file = None
uncovered_lines = 0
uncovered_files = set()
for raw in lcov.read_text(encoding="utf-8", errors="replace").splitlines():
    if raw.startswith("SF:"):
        current_file = raw[3:]
    elif raw.startswith("DA:") and current_file is not None:
        payload = raw[3:]
        _, _, hits = payload.partition(",")
        try:
            if int(hits) == 0:
                uncovered_lines += 1
                uncovered_files.add(current_file)
        except ValueError:
            continue

print(f"{uncovered_lines} {len(uncovered_files)}")
PY
)"
uncovered_lines="$(awk '{print $1}' <<<"$uncovered_summary")"
uncovered_files="$(awk '{print $2}' <<<"$uncovered_summary")"

tests_per_kloc="$(python3 - "$production_loc" "$test_count" <<'PY'
import sys
loc = float(sys.argv[1])
tests = float(sys.argv[2])
if loc <= 0:
    print('0.00')
else:
    print(f"{(tests/(loc/1000.0)):.2f}")
PY
)"

python3 - "$timestamp_utc" "$git_sha" "$test_seconds" "$coverage_gate_seconds" "$production_loc" "$test_count" "$tests_per_kloc" "$coverage_cfg_not_count" "$quality_churn_30d" "$quality_churn_90d" "$uncovered_lines" "$uncovered_files" "$LATEST_PATH" <<'PY'
import json
import pathlib
import sys

payload = {
    "timestamp_utc": sys.argv[1],
    "git_sha": sys.argv[2],
    "runtime_seconds": {
        "make_test": int(sys.argv[3]),
        "make_coverage_gate": int(sys.argv[4]),
    },
    "repo_metrics": {
        "production_loc": int(sys.argv[5]),
        "test_attribute_count": int(sys.argv[6]),
        "tests_per_kloc": float(sys.argv[7]),
        "cfg_not_coverage_count": int(sys.argv[8]),
        "uncovered_lines": int(sys.argv[11]),
        "uncovered_files": int(sys.argv[12]),
    },
    "churn": {
        "quality_fix_like_commits_last_30d": int(sys.argv[9]),
        "quality_fix_like_commits_last_90d": int(sys.argv[10]),
    },
}
latest_path = pathlib.Path(sys.argv[13])
latest_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
print(json.dumps(payload, separators=(",", ":")))
PY

line_json="$(python3 - "$LATEST_PATH" <<'PY'
import json
import pathlib
import sys
print(json.dumps(json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8')), separators=(',', ':')))
PY
)"

touch "$HISTORY_PATH"
if ! grep -q "\"git_sha\":\"$git_sha\"" "$HISTORY_PATH"; then
  printf '%s\n' "$line_json" >>"$HISTORY_PATH"
fi

echo "wrote baseline latest: $LATEST_PATH"
echo "updated baseline history: $HISTORY_PATH"
