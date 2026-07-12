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
    raw_arguments = list(sys.argv[1:] if arguments is None else arguments)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("build", "coverage"))
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true")
    if "--" not in raw_arguments:
        parser.error("a command is required after --")
    separator = raw_arguments.index("--")
    command = raw_arguments[separator + 1 :]
    if not command:
        parser.error("a command is required after --")
    args = parser.parse_args(raw_arguments[:separator])
    args.command = command
    return args


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).resolve().parent)
        if args.mode == "coverage":
            environment = coverage_target_environment(
                root, os.environ, create=not args.dry_run
            )
        else:
            environment = build_target_environment(
                root, os.environ, create=not args.dry_run
            )
    except WorkflowError as error:
        print(f"cargo-target: environment error: {error}", file=sys.stderr)
        return 2

    if args.dry_run:
        target = environment.get("CARGO_TARGET_DIR", "<cargo default>")
        variables = [f"CARGO_TARGET_DIR={target}"]
        if args.mode == "coverage":
            variables.extend(
                (
                    "CARGO_LLVM_COV_TARGET_DIR="
                    + environment["CARGO_LLVM_COV_TARGET_DIR"],
                    "CARGO_LLVM_COV_BUILD_DIR="
                    + environment["CARGO_LLVM_COV_BUILD_DIR"],
                )
            )
        print(f"{' '.join(variables)} {shell_join(args.command)}")
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
