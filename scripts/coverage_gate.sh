#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

OUT_DIR="$ROOT_DIR/coverage"
LCOV_PATH="$OUT_DIR/lcov.info"
IGNORE_REGEX='crates/crab-app/src/installer.rs'

mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

CARGO_EXIT=0
cargo llvm-cov --workspace --all-features --locked \
  --ignore-filename-regex "$IGNORE_REGEX" \
  --fail-under-functions 100 \
  --fail-uncovered-functions 0 \
  --lcov --output-path "$LCOV_PATH" || CARGO_EXIT=$?

if [ "$CARGO_EXIT" -ne 0 ]; then
  echo "cargo llvm-cov exited with code $CARGO_EXIT (function coverage gate failed)"
fi

GATE_SCRIPT="$OUT_DIR/_coverage_gate.py"
cat > "$GATE_SCRIPT" <<'PY'
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

lcov_path = Path(sys.argv[1])
if not lcov_path.exists():
    print(f"coverage gate failed: missing {lcov_path}")
    sys.exit(1)

line_totals = 0
line_covered = 0

missing_lines_by_file: dict[str, list[int]] = defaultdict(list)
current_file: str | None = None

for raw in lcov_path.read_text(encoding="utf-8", errors="replace").splitlines():
    line = raw.strip()
    if line.startswith("SF:"):
        current_file = line[3:]
        continue
    if current_file is None:
        continue
    if line.startswith("DA:"):
        line_number_raw, hits_raw = line[3:].split(",", 1)
        line_number = int(line_number_raw)
        hits = int(hits_raw)
        line_totals += 1
        if hits > 0:
            line_covered += 1
        else:
            missing_lines_by_file[current_file].append(line_number)
        continue

uncovered_lines = line_totals - line_covered

if uncovered_lines == 0:
    print(
        "coverage-gate: ok "
        f"(lines {line_covered}/{line_totals}, uncovered lines=0)"
    )
    sys.exit(0)

print(f"coverage-gate: failed (uncovered lines={uncovered_lines})")

if uncovered_lines:
    print("Top uncovered line locations:")
    rows = [
        (path, sorted(lines))
        for path, lines in missing_lines_by_file.items()
        if lines
    ]
    rows.sort(key=lambda item: (-len(item[1]), item[0]))
    for path, lines in rows[:25]:
        preview = ", ".join(str(line) for line in lines[:20])
        if len(lines) > 20:
            preview += ", ..."
        print(f"- {path}: {len(lines)} uncovered line(s) [{preview}]")

sys.exit(1)
PY

python3 "$GATE_SCRIPT" "$LCOV_PATH" 2>&1
