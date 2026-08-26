#!/usr/bin/env python3
"""Run one pragmatic gate across the selected Crab workspaces."""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import WorkflowError, repository_root, shell_join


@dataclass(frozen=True)
class PlannedCommand:
    label: str
    command: tuple[str, ...]
    working_directory: Path


Runner = Callable[[Sequence[str], Path], int]


def planned_commands(
    root: Path,
    gate: str,
    *,
    include_root: bool,
    include_v2: bool,
    root_packages: Sequence[str] = (),
    write: bool = False,
) -> list[PlannedCommand]:
    if not include_root and not include_v2:
        raise WorkflowError("at least one workspace must be selected")
    if root_packages and not include_root:
        raise WorkflowError("root packages require the root workspace")
    if len(set(root_packages)) != len(root_packages) or any(
        not package.strip() for package in root_packages
    ):
        raise WorkflowError("root package selection is invalid")
    if write and gate != "fmt":
        raise WorkflowError("only the format gate supports write mode")

    package_arguments: list[str] = []
    for package in root_packages:
        package_arguments.extend(("--package", package))
    if include_root and not package_arguments:
        package_arguments.append("--workspace")

    commands: list[PlannedCommand] = []
    if gate == "fmt":
        if include_root:
            commands.append(
                PlannedCommand(
                    "Crab v1 Rust",
                    ("cargo", "fmt", "--all")
                    if write
                    else ("cargo", "fmt", "--all", "--", "--check"),
                    root,
                )
            )
        if include_v2:
            commands.append(
                PlannedCommand(
                    "Crab v2 Rust",
                    ("cargo", "fmt", "--all")
                    if write
                    else ("cargo", "fmt", "--all", "--", "--check"),
                    root / "v2",
                )
            )
    elif gate == "clippy":
        if include_root:
            commands.append(
                PlannedCommand(
                    "Crab v1 Rust",
                    (
                        sys.executable,
                        str(root / "scripts" / "clippy_policy.py"),
                        *package_arguments,
                        "--all-targets",
                        "--all-features",
                        "--locked",
                    ),
                    root,
                )
            )
        if include_v2:
            commands.append(
                PlannedCommand(
                    "Crab v2 Rust",
                    (
                        sys.executable,
                        str(root / "scripts" / "cargo_target.py"),
                        "build",
                        "--working-directory",
                        "v2",
                        "--",
                        "cargo",
                        "clippy",
                        "--workspace",
                        "--all-targets",
                        "--all-features",
                        "--locked",
                        "--",
                        "-D",
                        "warnings",
                    ),
                    root,
                )
            )
    elif gate == "tests":
        if include_root:
            commands.append(
                PlannedCommand(
                    "Crab v1 Rust",
                    (
                        sys.executable,
                        str(root / "scripts" / "cargo_target.py"),
                        "build",
                        "--",
                        "cargo",
                        "test",
                        *package_arguments,
                        "--all-features",
                        "--locked",
                    ),
                    root,
                )
            )
        if include_v2:
            commands.append(
                PlannedCommand(
                    "Crab v2 Rust",
                    (
                        sys.executable,
                        str(root / "scripts" / "cargo_target.py"),
                        "build",
                        "--working-directory",
                        "v2",
                        "--",
                        "cargo",
                        "test",
                        "--workspace",
                        "--all-features",
                        "--locked",
                    ),
                    root,
                )
            )
            bridge_tests = tuple(
                str(path.relative_to(root))
                for path in sorted((root / "v2" / "bridges").glob("*/test/*.test.js"))
            )
            if not bridge_tests:
                raise WorkflowError("no first-party v2 bridge tests were found")
            commands.append(
                PlannedCommand(
                    "Crab v2 bridge Node preflight",
                    (
                        "node",
                        "-e",
                        "if (Number(process.versions.node.split('.')[0]) < 20) { "
                        "console.error('Crab v2 bridge tests require Node 20+'); "
                        "process.exit(2); }",
                    ),
                    root,
                )
            )
            commands.append(
                PlannedCommand(
                    "Crab v2 first-party bridges",
                    ("node", "--test", *bridge_tests),
                    root,
                )
            )
    else:
        raise WorkflowError(f"unsupported workspace gate: {gate}")
    return commands


def default_runner(command: Sequence[str], working_directory: Path) -> int:
    return subprocess.run(command, cwd=working_directory, check=False).returncode


def run_commands(root: Path, commands: Sequence[PlannedCommand], runner: Runner) -> int:
    for planned in commands:
        print(
            f"workspace-gate: {planned.label}: {shell_join(planned.command)}",
            flush=True,
        )
        try:
            returncode = runner(planned.command, planned.working_directory)
        except KeyboardInterrupt:
            print("workspace-gate: interrupted", file=sys.stderr)
            return 1
        except OSError as error:
            print(
                f"workspace-gate: could not execute {planned.command[0]}: {error}",
                file=sys.stderr,
            )
            return 2
        if returncode != 0:
            return 2 if returncode == 2 else 1
    return 0


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("gate", choices=("fmt", "clippy", "tests"))
    parser.add_argument("--root-workspace", action="store_true")
    parser.add_argument("--v2-workspace", action="store_true")
    parser.add_argument("--root-package", action="append", default=[])
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        commands = planned_commands(
            root,
            args.gate,
            include_root=args.root_workspace,
            include_v2=args.v2_workspace,
            root_packages=args.root_package,
            write=args.write,
        )
    except WorkflowError as error:
        print(f"workspace-gate: environment error: {error}", file=sys.stderr)
        return 2
    return run_commands(root, commands, default_runner)


if __name__ == "__main__":
    raise SystemExit(main())
