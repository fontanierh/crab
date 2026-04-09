#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$({
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
})"

cd "$ROOT_DIR"

RUST_FILES=()
while IFS= read -r file; do
  RUST_FILES+=("$file")
done < <(find crates -type f -name '*.rs' | sort)

PRODUCTION_FILES=()
TEST_FILES=()
for file in "${RUST_FILES[@]}"; do
  case "$file" in
    */tests/*|*/src/test_support.rs)
      TEST_FILES+=("$file")
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

echo "duplication-check: strict production scan"
npx --yes jscpd@4.0.5 --config .jscpd.json "${PRODUCTION_FILES[@]}"

if [ "${#TEST_FILES[@]}" -eq 0 ]; then
  echo "duplication-check: no test code files found for informational scan"
  exit 0
fi

echo "duplication-check: informational test-code scan"
set +e
npx --yes jscpd@4.0.5 --config .jscpd.json "${TEST_FILES[@]}"
TEST_SCAN_EXIT=$?
set -e

if [ "$TEST_SCAN_EXIT" -ne 0 ]; then
  echo "duplication-check: test-code duplication reported above (informational only)"
else
  echo "duplication-check: no test-code duplication detected"
fi
