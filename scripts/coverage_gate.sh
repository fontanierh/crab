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

cargo llvm-cov --workspace --all-features --locked \
  --ignore-filename-regex "$IGNORE_REGEX" \
  --fail-under-functions 100 \
  --fail-uncovered-functions 0 \
  --lcov --output-path "$LCOV_PATH"

python3 - "$LCOV_PATH" <<'PY'
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

lcov_path = Path(sys.argv[1])
if not lcov_path.exists():
    print(f"coverage gate failed: missing {lcov_path}", file=sys.stderr)
    raise SystemExit(1)

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
    raise SystemExit(0)

msg = "coverage-gate: failed " f"(uncovered lines={uncovered_lines})"
print(msg)
print(msg, file=sys.stderr)

if uncovered_lines:
    print("Top uncovered line locations:")
    print("Top uncovered line locations:", file=sys.stderr)
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
        detail = f"- {path}: {len(lines)} uncovered line(s) [{preview}]"
        print(detail)
        print(detail, file=sys.stderr)

raise SystemExit(1)
PY
