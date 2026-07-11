#!/usr/bin/env python3
"""Run Cargo with Crab's validated build or isolated coverage target policy."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import (
    WorkflowError,
    build_target_environment,
    coverage_target_environment,
    repository_root,
    shell_join,
)


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "coverage"))
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(arguments)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")
    return args


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).resolve().parent)
        if args.mode == "coverage":
            environment = coverage_target_environment(root, os.environ)
        else:
            environment = build_target_environment(root, os.environ)
    except WorkflowError as error:
        print(f"cargo-target: environment error: {error}", file=sys.stderr)
        return 2

    if args.dry_run:
        target = environment.get("CARGO_TARGET_DIR", "<cargo default>")
        print(f"CARGO_TARGET_DIR={target} {shell_join(args.command)}")
        return 0

    try:
        result = subprocess.run(args.command, cwd=root, env=environment, check=False)
    except KeyboardInterrupt:
        print("cargo-target: interrupted", file=sys.stderr)
        return 1
    except OSError as error:
        print(f"cargo-target: could not execute {args.command[0]}: {error}", file=sys.stderr)
        return 2
    return 0 if result.returncode == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
