#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

OUT_DIR="$ROOT_DIR/coverage"
RAW_OUT="$OUT_DIR/coverage_diagnostics.txt"
MISSING_OUT="$OUT_DIR/uncovered_locations.txt"

mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

set +e
cargo llvm-cov --workspace --all-features --locked \
  --ignore-filename-regex 'crates/crab-app/src/installer.rs' \
  --show-missing-lines \
  >"$RAW_OUT" 2>&1
status=$?
set -e

python3 - "$RAW_OUT" "$MISSING_OUT" <<'PY'
from __future__ import annotations

import pathlib
import re
import sys

raw_path = pathlib.Path(sys.argv[1])
out_path = pathlib.Path(sys.argv[2])

line_re = re.compile(r"^\s*(\d+)\|\s*0\|")
header_re = re.compile(r"^([\w./\\-]+\.rs):\s*$")
uncovered_summary_re = re.compile(r"^(.+\.rs):\s*(\d+)\s*$")
uncovered_function_re = re.compile(r"^(.+\.rs):\s*(.+)\s*$")

current_file: str | None = None
missing: dict[str, list[int]] = {}
in_uncovered_summary = False
in_uncovered_functions = False
uncovered_functions: dict[str, list[str]] = {}

for raw_line in raw_path.read_text(encoding="utf-8", errors="replace").splitlines():
    stripped = raw_line.strip()
    if stripped == "Uncovered Lines:":
        in_uncovered_functions = False
        in_uncovered_summary = True
        continue
    if stripped == "Uncovered Functions:":
        in_uncovered_summary = False
        in_uncovered_functions = True
        continue
    if in_uncovered_summary:
        if not stripped:
            continue
        summary_match = uncovered_summary_re.match(stripped)
        if summary_match:
            file_path = summary_match.group(1)
            line_number = int(summary_match.group(2))
            missing.setdefault(file_path, []).append(line_number)
            continue
        in_uncovered_summary = False
    if in_uncovered_functions:
        if not stripped:
            continue
        function_match = uncovered_function_re.match(stripped)
        if function_match:
            file_path = function_match.group(1)
            function_name = function_match.group(2)
            uncovered_functions.setdefault(file_path, []).append(function_name)
            continue
        in_uncovered_functions = False

    header_match = header_re.match(raw_line)
    if header_match:
        current_file = header_match.group(1)
        missing.setdefault(current_file, [])
        continue
    line_match = line_re.match(raw_line)
    if line_match and current_file is not None:
        missing.setdefault(current_file, []).append(int(line_match.group(1)))

rows = [(path, sorted(lines)) for path, lines in missing.items() if lines]
rows.sort(key=lambda item: (-len(item[1]), item[0]))
fn_rows = [(path, sorted(names)) for path, names in uncovered_functions.items() if names]
fn_rows.sort(key=lambda item: (-len(item[1]), item[0]))

with out_path.open("w", encoding="utf-8") as handle:
    if rows:
        handle.write("Top uncovered files (line hits with count=0):\n")
        for path, lines in rows[:25]:
            preview = ", ".join(str(line) for line in lines[:20])
            if len(lines) > 20:
                preview += ", ..."
            handle.write(f"- {path}: {len(lines)} uncovered line(s) [{preview}]\n")
    else:
        handle.write("No uncovered line locations parsed from llvm-cov output.\n")

    handle.write("\n")
    if fn_rows:
        handle.write("Top uncovered functions:\n")
        for path, names in fn_rows[:25]:
            handle.write(f"- {path}: {len(names)} uncovered function(s)\n")
    else:
        handle.write("No uncovered functions parsed from llvm-cov output.\n")
PY

echo "coverage diagnostics raw output: $RAW_OUT"
echo "uncovered line summary: $MISSING_OUT"

if [ "$status" -ne 0 ]; then
  echo "cargo llvm-cov exited with status $status (diagnostics still generated)."
  exit "$status"
fi
