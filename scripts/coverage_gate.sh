#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
)"

OUT_DIR="$ROOT_DIR/coverage"
LCOV_PATH="$OUT_DIR/lcov.info"
mkdir -p "$OUT_DIR"

cd "$ROOT_DIR"

LLVM_COV_ARGS=(
  --workspace
  --all-features
  --locked
)

cargo llvm-cov "${LLVM_COV_ARGS[@]}" \
  --fail-under-functions 100 \
  --fail-under-regions 99 \
  --show-missing-lines

if [ -z "${BASE_REF:-}" ]; then
  echo "coverage-gate: BASE_REF not set; skipping PR patch coverage check"
  exit 0
fi

echo "coverage-gate: checking changed production Rust lines against $BASE_REF"

cargo llvm-cov report --lcov --output-path "$LCOV_PATH"

python3 - "$ROOT_DIR" "$BASE_REF" "$LCOV_PATH" <<'PY'
from __future__ import annotations

import subprocess
import sys
from collections import defaultdict
from pathlib import Path, PurePosixPath

root_dir = Path(sys.argv[1]).resolve()
base_ref = sys.argv[2]
lcov_path = Path(sys.argv[3]).resolve()

if not lcov_path.exists():
    print(f"coverage-gate: failed (missing LCOV report at {lcov_path})")
    sys.exit(1)

def is_production_rust(path: str) -> bool:
    candidate = PurePosixPath(path)
    if candidate.suffix != ".rs":
        return False
    if len(candidate.parts) < 3 or candidate.parts[0] != "crates":
        return False
    if "tests" in candidate.parts:
        return False
    return "src" in candidate.parts and candidate.name != "test_support.rs"

diff_cmd = [
    "git",
    "-C",
    str(root_dir),
    "diff",
    "--unified=0",
    "--no-color",
    "--diff-filter=AMCR",
    f"{base_ref}...HEAD",
    "--",
    "crates",
]
try:
    diff_output = subprocess.check_output(diff_cmd, text=True)
except subprocess.CalledProcessError as exc:
    print(
        f"coverage-gate: failed to diff against {base_ref} (git exited {exc.returncode})"
    )
    sys.exit(exc.returncode)

changed_lines_by_file: dict[str, set[int]] = defaultdict(set)
current_file: str | None = None
for raw in diff_output.splitlines():
    line = raw.rstrip()
    if line.startswith("+++ "):
        path = line[4:]
        if path == "/dev/null":
            current_file = None
            continue
        if path.startswith("b/"):
            path = path[2:]
        current_file = path if is_production_rust(path) else None
        continue
    if current_file is None or not line.startswith("@@ "):
        continue
    hunk = line.split(" +", 1)[1].split(" @@", 1)[0]
    if "," in hunk:
        start_raw, count_raw = hunk.split(",", 1)
        start = int(start_raw)
        count = int(count_raw)
    else:
        start = int(hunk)
        count = 1
    if count == 0:
        continue
    changed_lines_by_file[current_file].update(range(start, start + count))

if not changed_lines_by_file:
    print("coverage-gate: no changed production Rust lines detected for patch coverage")
    sys.exit(0)

missing_lines_by_file: dict[str, set[int]] = defaultdict(set)
current_file = None
for raw in lcov_path.read_text(encoding="utf-8", errors="replace").splitlines():
    line = raw.strip()
    if line.startswith("SF:"):
        source = Path(line[3:]).resolve()
        try:
            relative_path = source.relative_to(root_dir).as_posix()
        except ValueError:
            current_file = None
            continue
        current_file = relative_path if is_production_rust(relative_path) else None
        continue
    if current_file is None or not line.startswith("DA:"):
        continue
    line_number_raw, hits_raw = line[3:].split(",", 1)
    if int(hits_raw) == 0:
        missing_lines_by_file[current_file].add(int(line_number_raw))

uncovered_changed_lines = []
for path, changed_lines in sorted(changed_lines_by_file.items()):
    uncovered = sorted(changed_lines & missing_lines_by_file.get(path, set()))
    if uncovered:
        uncovered_changed_lines.append((path, uncovered))

if uncovered_changed_lines:
    total_uncovered = sum(len(lines) for _, lines in uncovered_changed_lines)
    print("coverage-gate: failed (uncovered changed production lines detected)")
    print(
        "coverage-gate: "
        f"{total_uncovered} changed line(s) uncovered across "
        f"{len(uncovered_changed_lines)} file(s)"
    )
    for path, lines in uncovered_changed_lines:
        preview = ", ".join(str(line) for line in lines[:20])
        if len(lines) > 20:
            preview += ", ..."
        print(f"- {path}: {preview}")
    sys.exit(1)

changed_line_count = sum(len(lines) for lines in changed_lines_by_file.values())
print(
    "coverage-gate: patch coverage ok "
    f"({changed_line_count} changed line(s) across {len(changed_lines_by_file)} file(s))"
)
PY
