#!/usr/bin/env python3
"""Fresh, worktree-isolated aggregate and focused coverage workflows."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.changed_scope import select_scope
from scripts.patch_coverage import resolve_patch_base, validate_diff_mode
from scripts.workflow_common import (
    WorkflowError,
    repository_root,
    validate_local_directory,
    validate_managed_file,
)


FUNCTION_THRESHOLD = "99.5"
REGION_THRESHOLD = "99.0"
LINE_THRESHOLD = "99.4"


def _invalidate_outputs(root: Path, names: Sequence[str]) -> dict[str, Path]:
    validate_local_directory(root, "coverage", create=False)
    paths: dict[str, Path] = {}
    for name in names:
        path = validate_managed_file(root, Path("coverage") / name)
        try:
            path.unlink(missing_ok=True)
        except OSError as error:
            raise WorkflowError(f"could not invalidate stale coverage artifact {path}: {error}") from error
        paths[name] = path
    return paths


def _create_coverage_directory(root: Path) -> None:
    validate_local_directory(root, "coverage", create=True)


def run_local_coverage(root: Path, arguments: Sequence[str]) -> int:
    command = [
        sys.executable,
        str(root / "scripts" / "cargo_target.py"),
        "coverage",
        "--",
        "cargo",
        "llvm-cov",
        *arguments,
    ]
    try:
        result = subprocess.run(command, cwd=root, check=False)
    except OSError as error:
        print(f"coverage: could not execute local coverage wrapper: {error}", file=sys.stderr)
        return 2
    return result.returncode


def run_patch_gate(
    root: Path,
    *,
    mode: str,
    base_sha: str,
    lcov_path: Path,
    artifact_path: Path,
) -> int:
    command = [
        sys.executable,
        str(root / "scripts" / "patch_coverage.py"),
        "--mode",
        mode,
        "--base-sha",
        base_sha,
        "--fresh-lcov",
        str(lcov_path),
        "--output-json",
        str(artifact_path),
    ]
    try:
        return subprocess.run(command, cwd=root, check=False).returncode
    except OSError as error:
        print(f"coverage: could not execute patch gate: {error}", file=sys.stderr)
        return 2


def coverage_arguments(scope_packages: Sequence[str], full_workspace: bool) -> list[str]:
    if full_workspace:
        return ["--workspace"]
    output: list[str] = []
    for package in scope_packages:
        output.extend(("--package", package))
    return output


def run_report(root: Path) -> int:
    try:
        output = _invalidate_outputs(root, ("lcov.info",))["lcov.info"]
        _create_coverage_directory(root)
    except WorkflowError as error:
        print(f"coverage-report: {error}", file=sys.stderr)
        return 2
    result = run_local_coverage(
        root,
        [
            "--workspace",
            "--all-features",
            "--locked",
            "--lcov",
            "--output-path",
            str(output),
        ],
    )
    if result == 0:
        print(f"coverage: fresh LCOV report: {output}")
    return result


def run_gate(root: Path, mode: str, explicit_base: str | None) -> int:
    try:
        validate_diff_mode(root, mode)
        base_sha = resolve_patch_base(root, explicit_base)
        outputs = _invalidate_outputs(
            root, ("lcov.info", "summary.json", "patch-coverage.json")
        )
        _create_coverage_directory(root)
    except WorkflowError as error:
        print(f"coverage-gate: {error}", file=sys.stderr)
        return 2

    output = outputs["lcov.info"]
    artifact = outputs["patch-coverage.json"]
    print(
        "coverage-gate: fresh aggregate thresholds "
        f"functions {FUNCTION_THRESHOLD}%, regions {REGION_THRESHOLD}%, lines {LINE_THRESHOLD}%",
        flush=True,
    )
    result = run_local_coverage(
        root,
        [
            "--workspace",
            "--all-features",
            "--locked",
            "--lcov",
            "--output-path",
            str(output),
            "--fail-under-functions",
            FUNCTION_THRESHOLD,
            "--fail-under-regions",
            REGION_THRESHOLD,
            "--fail-under-lines",
            LINE_THRESHOLD,
            "--show-missing-lines",
        ],
    )
    if result != 0:
        return result
    summary_path = outputs["summary.json"]
    summary_result = run_local_coverage(
        root,
        [
            "report",
            "--json",
            "--summary-only",
            "--output-path",
            str(summary_path),
        ],
    )
    if summary_result != 0:
        return summary_result
    try:
        _print_quick_summary(summary_path, label="coverage-gate", qualifier="enforced")
    except (OSError, KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"coverage-gate: invalid aggregate summary: {error}", file=sys.stderr)
        return 2
    return run_patch_gate(
        root,
        mode=mode,
        base_sha=base_sha,
        lcov_path=output,
        artifact_path=artifact,
    )


def _print_quick_summary(
    path: Path, *, label: str = "coverage-quick", qualifier: str = "report only"
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    totals = payload["data"][0]["totals"]
    values = []
    for key, threshold in (
        ("functions", FUNCTION_THRESHOLD),
        ("regions", REGION_THRESHOLD),
        ("lines", LINE_THRESHOLD),
    ):
        values.append(f"{key} {totals[key]['percent']:.2f}% (final floor {threshold}%)")
    print(f"{label}: aggregate {qualifier}: " + ", ".join(values))


def run_quick(root: Path, explicit_base: str | None) -> int:
    try:
        base_sha = resolve_patch_base(root, explicit_base)
        scope = select_scope(root, mode="worktree", explicit_base=base_sha)
        outputs = _invalidate_outputs(
            root,
            ("quick-lcov.info", "quick-summary.json", "quick-patch-coverage.json"),
        )
    except WorkflowError as error:
        print(f"coverage-quick: {error}", file=sys.stderr)
        return 2
    rust_changes = [path for path in scope.changed_files if path.endswith(".rs")]
    if not rust_changes:
        print("coverage-quick: skipped: no Rust changes")
        return 0
    if scope.fallback_reason:
        print(f"coverage-quick: scope fallback: {scope.fallback_reason}")
    if scope.full_workspace:
        print("coverage-quick: scope is the whole workspace; coverage-gate remains authoritative")
    else:
        print("coverage-quick: packages: " + ", ".join(scope.selected_packages))

    try:
        _create_coverage_directory(root)
    except WorkflowError as error:
        print(f"coverage-quick: {error}", file=sys.stderr)
        return 2
    lcov_path = outputs["quick-lcov.info"]
    summary_path = outputs["quick-summary.json"]
    artifact_path = outputs["quick-patch-coverage.json"]
    package_arguments = coverage_arguments(scope.selected_packages, scope.full_workspace)
    result = run_local_coverage(
        root,
        [
            *package_arguments,
            "--all-features",
            "--locked",
            "--lcov",
            "--output-path",
            str(lcov_path),
        ],
    )
    if result != 0:
        return result
    summary_result = run_local_coverage(
        root,
        [
            "report",
            "--json",
            "--summary-only",
            "--output-path",
            str(summary_path),
        ],
    )
    if summary_result != 0:
        return summary_result
    try:
        _print_quick_summary(summary_path)
    except (OSError, KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"coverage-quick: invalid aggregate summary: {error}", file=sys.stderr)
        return 2
    return run_patch_gate(
        root,
        mode="worktree",
        base_sha=base_sha,
        lcov_path=lcov_path,
        artifact_path=artifact_path,
    )


def run_diagnostics(root: Path) -> int:
    try:
        outputs = _invalidate_outputs(root, ("lcov.info", "uncovered_locations.txt"))
        _create_coverage_directory(root)
    except WorkflowError as error:
        print(f"coverage-diagnostics: {error}", file=sys.stderr)
        return 2
    lcov_path = outputs["lcov.info"]
    summary_path = outputs["uncovered_locations.txt"]
    result = run_local_coverage(
        root,
        [
            "--workspace",
            "--all-features",
            "--locked",
            "--lcov",
            "--output-path",
            str(lcov_path),
        ],
    )
    if result != 0:
        return result

    current: str | None = None
    missing: dict[str, list[int]] = {}
    try:
        lines = lcov_path.read_text(encoding="utf-8", errors="replace").splitlines()
        for raw in lines:
            if raw.startswith("SF:"):
                current = raw[3:]
                missing.setdefault(current, [])
            elif raw.startswith("DA:") and current is not None:
                fields = raw[3:].split(",")
                if len(fields) >= 2 and int(fields[1]) == 0:
                    missing[current].append(int(fields[0]))
            elif raw == "end_of_record":
                current = None
        rows = sorted(
            ((path, sorted(set(values))) for path, values in missing.items() if values),
            key=lambda value: (-len(value[1]), value[0]),
        )
        with summary_path.open("w", encoding="utf-8") as output:
            if not rows:
                output.write("No uncovered line locations parsed from fresh LCOV output.\n")
            else:
                output.write("Top uncovered files (fresh LCOV DA hit count = 0):\n")
                for path, values in rows[:25]:
                    preview = ", ".join(str(value) for value in values[:20])
                    if len(values) > 20:
                        preview += ", ..."
                    output.write(f"- {path}: {len(values)} line(s) [{preview}]\n")
    except (OSError, ValueError) as error:
        print(f"coverage-diagnostics: could not parse fresh LCOV: {error}", file=sys.stderr)
        return 2
    print(f"coverage-diagnostics: fresh LCOV: {lcov_path}")
    print(f"coverage-diagnostics: uncovered-line summary: {summary_path}")
    return 0


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("report")
    subparsers.add_parser("diagnostics")
    gate = subparsers.add_parser("gate")
    gate.add_argument("--mode", choices=("worktree", "staged", "committed"), default="worktree")
    gate.add_argument("--base-sha")
    quick = subparsers.add_parser("quick")
    quick.add_argument("--base-sha")
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
    except WorkflowError as error:
        print(f"coverage: environment error: {error}", file=sys.stderr)
        return 2
    if args.command == "report":
        return run_report(root)
    if args.command == "diagnostics":
        return run_diagnostics(root)
    if args.command == "gate":
        return run_gate(root, args.mode, args.base_sha)
    return run_quick(root, args.base_sha)


if __name__ == "__main__":
    raise SystemExit(main())
