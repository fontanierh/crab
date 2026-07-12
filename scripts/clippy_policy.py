#!/usr/bin/env python3
"""Run Clippy with Crab's ordered, stable lint tri-state."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import WorkflowError, repository_root


# `rust.warnings = deny` also raises tool-lint warnings. These later, specific flags
# deliberately restore style/complexity to warning level while retaining rustc failures.
CLIPPY_POLICY_ARGS = (
    "-D",
    "warnings",
    "-D",
    "clippy::correctness",
    "-D",
    "clippy::suspicious",
    "-D",
    "clippy::perf",
    "--force-warn",
    "clippy::style",
    "--force-warn",
    "clippy::complexity",
)


def main(arguments: list[str] | None = None) -> int:
    cargo_arguments = list(sys.argv[1:] if arguments is None else arguments)
    if "--" in cargo_arguments:
        print("clippy-policy: do not pass raw rustc arguments; policy flags are repository-owned", file=sys.stderr)
        return 2
    try:
        root = repository_root(Path(__file__).parent)
    except WorkflowError as error:
        print(f"clippy-policy: environment error: {error}", file=sys.stderr)
        return 2
    command = [
        sys.executable,
        str(root / "scripts" / "cargo_target.py"),
        "build",
        "--",
        "cargo",
        "clippy",
        *cargo_arguments,
        "--",
        *CLIPPY_POLICY_ARGS,
    ]
    try:
        result = subprocess.run(command, cwd=root, check=False)
    except OSError as error:
        print(f"clippy-policy: could not run Clippy: {error}", file=sys.stderr)
        return 2
    return 0 if result.returncode == 0 else (2 if result.returncode == 2 else 1)


if __name__ == "__main__":
    raise SystemExit(main())
