#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$({
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
})"

args=(gate --mode "${PATCH_MODE:-worktree}")
if [[ -n "${BASE_SHA:-${BASE_REF:-}}" ]]; then
  args+=(--base-sha "${BASE_SHA:-${BASE_REF}}")
fi

exec python3 "$ROOT_DIR/scripts/coverage_workflow.py" "${args[@]}"
