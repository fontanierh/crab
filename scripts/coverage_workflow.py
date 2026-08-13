#!/usr/bin/env python3
"""Fresh, worktree-isolated aggregate and focused coverage workflows."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.changed_scope import select_scope
from scripts.lcov_stats import parse_lcov
from scripts.patch_coverage import resolve_patch_base, validate_diff_mode
from scripts.workflow_common import (
    COVERAGE_IGNORE_FILENAME_REGEX,
    WorkflowError,
    repository_root,
    validate_local_directory,
    validate_managed_file,
)


AUTHORITATIVE_ARTIFACTS = (
    "lcov.info",
    "lcov.info.rejected",
    "summary.json",
    "patch-coverage.json",
    "uncovered_locations.txt",
)
QUICK_ARTIFACTS = (
    "quick-lcov.info",
    "quick-lcov.info.rejected",
    "quick-summary.json",
    "quick-patch-coverage.json",
)
IGNORE_ARGUMENTS = ("--ignore-filename-regex", COVERAGE_IGNORE_FILENAME_REGEX)


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


def _validate_lcov_policy(path: Path) -> None:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise WorkflowError(f"could not read fresh LCOV artifact {path}: {error}") from error
    excluded = [
        raw[3:]
        for raw in lines
        if raw.startswith("SF:")
        and re.search(COVERAGE_IGNORE_FILENAME_REGEX, raw[3:].replace("\\", "/"))
    ]
    if excluded:
        preview = ", ".join(excluded[:5])
        raise WorkflowError(
            "coverage export contains policy-excluded files "
            f"({preview}); rerun after fixing the ignore policy"
        )


def _quarantine_rejected_lcov(path: Path) -> Path:
    rejected = path.with_name(f"{path.name}.rejected")
    try:
        rejected.unlink(missing_ok=True)
        path.replace(rejected)
    except OSError as replace_error:
        try:
            path.unlink()
        except OSError as unlink_error:
            raise WorkflowError(
                f"could not quarantine rejected LCOV {path}: {replace_error}; "
                f"could not remove authoritative artifact: {unlink_error}"
            ) from unlink_error
    return rejected


def _reject_lcov(label: str, path: Path, error: WorkflowError) -> int:
    try:
        rejected = _quarantine_rejected_lcov(path)
    except WorkflowError as quarantine_error:
        print(f"{label}: {error}; {quarantine_error}", file=sys.stderr)
        return 2
    print(f"{label}: {error}; quarantined at {rejected}", file=sys.stderr)
    return 2


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
        output = _invalidate_outputs(root, AUTHORITATIVE_ARTIFACTS)["lcov.info"]
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
            *IGNORE_ARGUMENTS,
        ],
    )
    if result == 0:
        try:
            _validate_lcov_policy(output)
        except WorkflowError as error:
            return _reject_lcov("coverage-report", output, error)
        print(f"coverage: fresh LCOV report: {output}")
    return result


def run_gate(root: Path, mode: str, explicit_base: str | None) -> int:
    # Compatibility entry point: coverage is intentionally a developer-owned report,
    # not a merge gate. A diff baseline is therefore neither resolved nor required.
    del mode, explicit_base
    print("coverage-gate: deprecated; generating the non-blocking coverage report")
    return run_report(root)


def _print_quick_summary(
    path: Path, *, label: str = "coverage-quick", qualifier: str = "report only"
) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    totals = payload["data"][0]["totals"]
    values = []
    for key in ("functions", "regions", "lines"):
        values.append(f"{key} {totals[key]['percent']:.2f}%")
    print(f"{label}: aggregate {qualifier}: " + ", ".join(values))


def run_quick(root: Path, explicit_base: str | None) -> int:
    try:
        base_sha = resolve_patch_base(root, explicit_base)
        scope = select_scope(root, mode="worktree", explicit_base=base_sha)
        outputs = _invalidate_outputs(root, QUICK_ARTIFACTS)
    except WorkflowError as error:
        print(f"coverage-quick: {error}", file=sys.stderr)
        return 2
    rust_changes = [path for path in scope.changed_files if path.endswith(".rs")]
    if scope.base_sha is None or (scope.fallback_reason and not scope.changed_files):
        reason = scope.fallback_reason or "merge base is unavailable"
        print(f"coverage-quick: cannot determine changed scope: {reason}", file=sys.stderr)
        return 2
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
            *IGNORE_ARGUMENTS,
        ],
    )
    if result != 0:
        return result
    try:
        _validate_lcov_policy(lcov_path)
    except WorkflowError as error:
        return _reject_lcov("coverage-quick", lcov_path, error)
    summary_result = run_local_coverage(
        root,
        [
            "report",
            "--json",
            "--summary-only",
            "--output-path",
            str(summary_path),
            *IGNORE_ARGUMENTS,
        ],
    )
    if summary_result != 0:
        return summary_result
    try:
        _print_quick_summary(summary_path)
    except (OSError, KeyError, IndexError, TypeError, ValueError, json.JSONDecodeError) as error:
        print(f"coverage-quick: invalid aggregate summary: {error}", file=sys.stderr)
        return 2
    del artifact_path
    print("coverage-quick: report complete; no coverage threshold is enforced")
    return 0


def run_diagnostics(root: Path) -> int:
    try:
        outputs = _invalidate_outputs(root, AUTHORITATIVE_ARTIFACTS)
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
            *IGNORE_ARGUMENTS,
        ],
    )
    if result != 0:
        return result

    try:
        _validate_lcov_policy(lcov_path)
        stats = parse_lcov(root, lcov_path)
        rows = sorted(
            (item for item in stats.files if item.uncovered_lines > 0),
            key=lambda item: (-item.uncovered_lines, item.path),
        )
        with summary_path.open("w", encoding="utf-8") as output:
            if not rows:
                output.write("No uncovered lines reported by fresh LCOV LF/LH totals.\n")
            else:
                output.write("Top uncovered files (fresh LCOV LF/LH totals):\n")
                for item in rows[:25]:
                    values = item.zero_hit_lines
                    preview = ", ".join(str(value) for value in values[:20])
                    if len(values) > 20:
                        preview += ", ..."
                    if not preview:
                        preview = "no DA:0 locations"
                    missing_locations = item.uncovered_lines - len(values)
                    annotation = ""
                    if missing_locations:
                        annotation = (
                            f" (+{missing_locations} uncovered line(s) without DA:0 rows; "
                            "totals from LF/LH)"
                        )
                    output.write(
                        f"- {item.path}: {item.uncovered_lines} line(s) "
                        f"[{preview}]{annotation}\n"
                    )
            output.write(
                f"Totals: {stats.uncovered_lines} uncovered of {stats.lines_found} line(s) "
                f"across {stats.uncovered_files} file(s) with gaps.\n"
            )
    except WorkflowError as error:
        if lcov_path.exists():
            return _reject_lcov("coverage-diagnostics", lcov_path, error)
        print(f"coverage-diagnostics: could not parse fresh LCOV: {error}", file=sys.stderr)
        return 2
    except OSError as error:
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
