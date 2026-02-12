#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$({
  cd "$(dirname "${BASH_SOURCE[0]}")/.."
  pwd
})"

cd "$ROOT_DIR"

# Production-focused duplication gate:
# - scan crate source roots (`crates/*/src` from .jscpd.json)
# - ignore explicit support-only files configured in .jscpd.json
npx --yes jscpd@4.0.5 --config .jscpd.json
