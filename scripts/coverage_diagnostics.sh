#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

OUT_DIR="$ROOT_DIR/coverage"
LCOV_OUT="$OUT_DIR/lcov.info"
RAW_OUT="$OUT_DIR/coverage_diagnostics.txt"
MISSING_OUT="$OUT_DIR/uncovered_locations.txt"
IGNORE_REGEX='crates/crab-app/src/installer.rs'

mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

cargo llvm-cov --workspace --all-features --locked \
  --ignore-filename-regex "$IGNORE_REGEX" \
  --lcov --output-path "$LCOV_OUT" \
  >"$RAW_OUT" 2>&1

python3 - "$LCOV_OUT" "$MISSING_OUT" <<'PY'
from __future__ import annotations

import pathlib
import sys

lcov_path = pathlib.Path(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])

missing: dict[str, list[int]] = {}
current_file: str | None = None

for raw_line in lcov_path.read_text(encoding="utf-8", errors="replace").splitlines():
    line = raw_line.strip()
    if line.startswith("SF:"):
        current_file = line[3:]
        missing.setdefault(current_file, [])
        continue
    if current_file is None:
        continue
    if line.startswith("DA:"):
        line_number_raw, hits_raw = line[3:].split(",", 1)
        if int(hits_raw) == 0:
            missing[current_file].append(int(line_number_raw))
        continue
    if line == "end_of_record":
        current_file = None

rows = [(path, sorted(set(lines))) for path, lines in missing.items() if lines]
rows.sort(key=lambda item: (-len(item[1]), item[0]))

with out_path.open("w", encoding="utf-8") as handle:
    if rows:
        handle.write("Top uncovered files (line hits with count=0):\n")
        for path, lines in rows[:25]:
            preview = ", ".join(str(line) for line in lines[:20])
            if len(lines) > 20:
                preview += ", ..."
            handle.write(f"- {path}: {len(lines)} uncovered line(s) [{preview}]\n")
    else:
        handle.write("No uncovered line locations parsed from lcov output.\n")

    handle.write(
        "\nFunction-level diagnostics are validated by `make coverage-gate` via llvm-cov's native checks.\n"
    )
PY

echo "coverage diagnostics raw output: $RAW_OUT"
echo "lcov output: $LCOV_OUT"
echo "uncovered line summary: $MISSING_OUT"
