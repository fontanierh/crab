#!/usr/bin/env bash
set -euo pipefail

if ! command -v rg >/dev/null 2>&1; then
  echo "error: rg (ripgrep) is required for public API usage check" >&2
  exit 2
fi

mapfile -t defs < <(rg -n '^[[:space:]]*pub (async )?fn ([A-Za-z0-9_]+)' crates --glob '*.rs')

if [[ ${#defs[@]} -eq 0 ]]; then
  echo "public-api-check: no pub functions found"
  exit 0
fi

failures=()
for def in "${defs[@]}"; do
  file=${def%%:*}
  rest=${def#*:}
  line=${rest%%:*}
  src=${rest#*:}
  name=$(sed -E 's/^[[:space:]]*pub (async )?fn ([A-Za-z0-9_]+).*/\2/' <<<"$src")

  refs=$(rg -n "\\b${name}\\b" crates --glob '*.rs' \
    | grep -v "^${file}:${line}:" \
    | grep -v "^${file}:" \
    || true)

  if [[ -z "$refs" ]]; then
    failures+=("${file}:${line}:${name}")
  fi
done

if [[ ${#failures[@]} -gt 0 ]]; then
  echo "public-api-check: found public functions without cross-file usage:" >&2
  printf '  - %s\n' "${failures[@]}" >&2
  echo "hint: reduce visibility (pub(crate)/private) or wire callsites" >&2
  exit 1
fi

echo "public-api-check: ok (${#defs[@]} pub functions checked)"
