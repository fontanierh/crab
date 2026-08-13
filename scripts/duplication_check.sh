#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$({
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
})"

cd "$ROOT_DIR"

if ! command -v jscpd >/dev/null 2>&1; then
  echo "duplication-check: environment error: jscpd 4.0.5 is required" >&2
  echo "next: npm install --global jscpd@4.0.5" >&2
  exit 2
fi

if [[ "$(jscpd --version 2>/dev/null)" != "4.0.5" ]]; then
  echo "duplication-check: environment error: expected jscpd 4.0.5" >&2
  echo "next: npm install --global jscpd@4.0.5" >&2
  exit 2
fi

RUST_FILES=()
while IFS= read -r file; do
  RUST_FILES+=("$file")
done < <(find crates -type f -name '*.rs' | sort)

PRODUCTION_FILES=()
for file in "${RUST_FILES[@]}"; do
  case "$file" in
    */tests/*|*/src/test_support.rs)
      ;;
    */src/*.rs)
      PRODUCTION_FILES+=("$file")
      ;;
  esac
done

if [ "${#PRODUCTION_FILES[@]}" -eq 0 ]; then
  echo "duplication-check: failed (no production Rust files found)"
  exit 1
fi

echo "duplication-check: informational production scan (10% threshold, tests excluded)"
set +e
jscpd --config .jscpd.json "${PRODUCTION_FILES[@]}"
SCAN_EXIT=$?
set -e

if [ "$SCAN_EXIT" -eq 0 ]; then
  echo "duplication-check: production duplication is below the reporting threshold"
  exit 0
fi

if [ "$SCAN_EXIT" -eq 1 ]; then
  echo "duplication-check: duplication reported above (informational only)"
  exit 0
fi

echo "duplication-check: jscpd failed with exit $SCAN_EXIT" >&2
exit 2
