#!/usr/bin/env python3
"""Compact gate orchestration with full logs and exact-tree status attestation."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.changed_scope import ScopeResult, select_scope
from scripts.patch_coverage import resolve_patch_base, validate_diff_mode
from scripts.workflow_common import (
    Fingerprint,
    WorkflowError,
    atomic_write_json,
    command_version,
    git_output,
    repository_root,
    shell_join,
    tree_fingerprint,
    validate_local_directory,
    validate_managed_file,
)


STATUS_SCHEMA_VERSION = 2
QUALITY_GATE_NAMES = (
    "fmt",
    "clippy",
    "tests",
    "public-api",
    "duplication",
    "gate-tests",
    "coverage",
)
LOG_TAIL_LINES = 30


@dataclass(frozen=True)
class GateSpec:
    name: str
    command: tuple[str, ...]
    rerun_command: str
    skip_reason: str | None = None


@dataclass(frozen=True)
class GateRecord:
    name: str
    status: str
    reason: str | None
    duration_seconds: float
    exit_code: int | None
    log_path: str | None
    rerun_command: str


Executor = Callable[[Path, GateSpec, Path, bool], tuple[int, float]]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def execute_command(root: Path, spec: GateSpec, log_path: Path, verbose: bool) -> tuple[int, float]:
    started = time.monotonic()
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    try:
        with log_path.open("w", encoding="utf-8") as log:
            log.write(f"$ {shell_join(spec.command)}\n")
            log.flush()
            if verbose:
                process = subprocess.Popen(
                    spec.command,
                    cwd=root,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=environment,
                )
                assert process.stdout is not None
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    print(line, end="")
                returncode = process.wait()
            else:
                result = subprocess.run(
                    spec.command,
                    cwd=root,
                    stdout=log,
                    stderr=subprocess.STDOUT,
                    env=environment,
                    check=False,
                )
                returncode = result.returncode
    except OSError as error:
        try:
            log_path.write_text(f"could not execute command: {error}\n", encoding="utf-8")
        except OSError:
            pass
        returncode = 2
    return returncode, time.monotonic() - started


def _print_log_tail(log_path: Path) -> None:
    try:
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as error:
        print(f"  log unavailable: {error}")
        return
    print(f"  last {min(LOG_TAIL_LINES, len(lines))} log line(s):")
    for line in lines[-LOG_TAIL_LINES:]:
        print(f"    {line}")


def run_specs(
    root: Path,
    specs: Sequence[GateSpec],
    log_directory: Path,
    *,
    verbose: bool = False,
    executor: Executor = execute_command,
) -> list[GateRecord]:
    resolved_root = root.resolve()
    try:
        relative_log_directory = log_directory.resolve(strict=False).relative_to(
            resolved_root
        )
    except ValueError as error:
        raise WorkflowError(
            f"gate log directory must remain inside the repository: {log_directory}"
        ) from error
    root = resolved_root
    log_directory = validate_local_directory(
        root, relative_log_directory, create=True
    )
    records: list[GateRecord] = []
    blocked_reason: str | None = None
    for index, spec in enumerate(specs, start=1):
        if spec.skip_reason or blocked_reason:
            reason = spec.skip_reason or blocked_reason
            record = GateRecord(
                name=spec.name,
                status="skipped",
                reason=reason,
                duration_seconds=0.0,
                exit_code=None,
                log_path=None,
                rerun_command=spec.rerun_command,
            )
            records.append(record)
            print(f"gates: SKIP {spec.name}: {reason}")
            continue
        log_path = validate_managed_file(
            root,
            (log_directory / f"{index:02d}-{spec.name}.log").relative_to(root),
        )
        returncode, duration = executor(root, spec, log_path, verbose)
        status = "passed" if returncode == 0 else "failed"
        record = GateRecord(
            name=spec.name,
            status=status,
            reason=None,
            duration_seconds=round(duration, 3),
            exit_code=returncode,
            log_path=log_path.relative_to(root).as_posix(),
            rerun_command=spec.rerun_command,
        )
        records.append(record)
        print(f"gates: {'PASS' if returncode == 0 else 'FAIL'} {spec.name} ({duration:.2f}s)")
        if returncode != 0:
            _print_log_tail(log_path)
            print(f"  rerun: {spec.rerun_command}")
            blocked_reason = f"blocked by failed gate {spec.name}"
    return records


def _cargo_wrapper(root: Path, *command: str) -> tuple[str, ...]:
    return (
        sys.executable,
        str(root / "scripts" / "cargo_target.py"),
        "build",
        "--",
        *command,
    )


def _clippy_wrapper(root: Path, *arguments: str) -> tuple[str, ...]:
    return (
        sys.executable,
        str(root / "scripts" / "clippy_policy.py"),
        *arguments,
    )


def _package_arguments(scope: ScopeResult) -> list[str]:
    if scope.full_workspace:
        return ["--workspace"]
    output: list[str] = []
    for package in scope.selected_packages:
        output.extend(("--package", package))
    return output


def check_specs(root: Path, scope: ScopeResult) -> list[GateSpec]:
    skip_reason: str | None = None
    if scope.docs_only:
        skip_reason = "docs-only change"
    elif not scope.changed_files:
        skip_reason = "no changed files"
    package_args = _package_arguments(scope)
    return [
        GateSpec(
            "fmt",
            ("cargo", "fmt", "--all", "--", "--check"),
            "make fmt-check",
            skip_reason,
        ),
        GateSpec(
            "clippy",
            _clippy_wrapper(
                root,
                *package_args,
                "--all-targets",
                "--all-features",
                "--locked",
            ),
            "make check",
            skip_reason,
        ),
        GateSpec(
            "tests",
            _cargo_wrapper(
                root,
                "cargo",
                "test",
                *package_args,
                "--all-features",
                "--locked",
            ),
            "make check",
            skip_reason,
        ),
    ]


def quality_specs(root: Path, mode: str, base_sha: str) -> list[GateSpec]:
    by_name = {
        "fmt": GateSpec("fmt", ("cargo", "fmt", "--all", "--", "--check"), "make fmt-check"),
        "clippy": GateSpec(
            "clippy",
            _clippy_wrapper(
                root,
                "--workspace",
                "--all-targets",
                "--all-features",
                "--locked",
            ),
            "make clippy",
        ),
        "tests": GateSpec(
            "tests",
            _cargo_wrapper(
                root,
                "cargo",
                "test",
                "--workspace",
                "--all-features",
                "--locked",
            ),
            "make test",
        ),
        "public-api": GateSpec(
            "public-api",
            ("bash", str(root / "scripts" / "public_api_usage_check.sh")),
            "make public-api-check",
        ),
        "duplication": GateSpec(
            "duplication",
            ("bash", str(root / "scripts" / "duplication_check.sh")),
            "make duplication-check",
        ),
        "gate-tests": GateSpec(
            "gate-tests",
            (
                sys.executable,
                "-m",
                "unittest",
                "discover",
                "-s",
                "scripts/tests",
                "-p",
                "test_*.py",
            ),
            "make gate-tests",
        ),
        "coverage": GateSpec(
            "coverage",
            (
                sys.executable,
                str(root / "scripts" / "coverage_workflow.py"),
                "gate",
                "--mode",
                mode,
                "--base-sha",
                base_sha,
            ),
            f"make coverage-gate PATCH_MODE={mode} BASE_SHA={base_sha}",
        ),
    }
    return [by_name[name] for name in QUALITY_GATE_NAMES]


def fingerprint_payload(value: Fingerprint) -> dict[str, object]:
    return {
        "sha256": value.digest,
        "dirty": value.dirty,
        "entry_count": len(value.entries),
    }


def tool_versions(root: Path) -> dict[str, str]:
    return {
        "rustc": command_version(root, ["rustc", "-V"]),
        "clippy": command_version(root, ["cargo", "clippy", "-V"]),
        "cargo_llvm_cov": command_version(
            root, ["cargo-llvm-cov", "llvm-cov", "--version"]
        ),
    }


def _branch(root: Path) -> str:
    try:
        result = subprocess.run(
            ["git", "symbolic-ref", "--quiet", "--short", "HEAD"],
            cwd=root,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        )
    except OSError as error:
        raise WorkflowError(f"could not resolve current Git branch: {error}") from error
    return result.stdout.strip() if result.returncode == 0 else "(detached)"


def prepare_status_path(root: Path) -> Path:
    path = validate_managed_file(
        root, Path("quality") / "status.json", create_parent=True
    )
    try:
        path.unlink(missing_ok=True)
    except OSError as error:
        raise WorkflowError(f"could not invalidate stale status {path}: {error}") from error
    return path


def build_status(
    root: Path,
    *,
    started_at: str,
    ended_at: str,
    mode: str,
    base_sha: str | None,
    start: Fingerprint,
    end: Fingerprint,
    records: Sequence[GateRecord],
    versions: dict[str, str],
    setup_error: str | None = None,
) -> tuple[dict[str, object], int]:
    fingerprint_matches = start.digest == end.digest
    failed = [record for record in records if record.status == "failed"]
    skipped = [record for record in records if record.status == "skipped"]
    environment_failure = setup_error is not None or any(
        record.exit_code == 2 for record in failed
    )
    if not fingerprint_matches or environment_failure:
        result = "invalid"
        exit_code = 2
    elif failed or skipped:
        result = "failed"
        exit_code = 1
    else:
        result = "passed"
        exit_code = 0

    if not fingerprint_matches:
        next_actions = [
            "The tree changed during validation; inspect the diff and rerun make quality."
        ]
    elif setup_error:
        next_actions = [setup_error, "Resolve the environment/baseline error and rerun make quality."]
    elif failed:
        first = failed[0]
        next_actions = [
            f"Rerun `{first.rerun_command}`.",
            f"Inspect `{first.log_path}` for the complete diagnostic log.",
        ]
    elif skipped:
        next_actions = ["A required gate was skipped; resolve its blocker and rerun make quality."]
    else:
        next_actions = ["The exact current tree is ready for handoff."]

    payload: dict[str, object] = {
        "schema_version": STATUS_SCHEMA_VERSION,
        "started_at": started_at,
        "ended_at": ended_at,
        "git_sha": git_output(root, "rev-parse", "HEAD"),
        "branch": _branch(root),
        "resolved_base_sha": base_sha,
        "diff_mode": mode,
        "start_fingerprint": fingerprint_payload(start),
        "end_fingerprint": fingerprint_payload(end),
        "dirty": end.dirty,
        "tool_versions": versions,
        "checks": [asdict(record) for record in records],
        "result": result,
        "setup_error": setup_error,
        "next_actions": next_actions,
    }
    return payload, exit_code


def orchestrate_quality(
    root: Path,
    *,
    mode: str,
    explicit_base: str | None,
    executor: Executor = execute_command,
    specs_override: Sequence[GateSpec] | None = None,
    versions_override: dict[str, str] | None = None,
) -> int:
    status_path = prepare_status_path(root)
    started_at = utc_now()
    start = tree_fingerprint(root)
    versions = versions_override if versions_override is not None else tool_versions(root)
    base_sha: str | None = None
    setup_error: str | None = None
    try:
        validate_diff_mode(root, mode)
        base_sha = resolve_patch_base(root, explicit_base)
    except WorkflowError as error:
        setup_error = str(error)

    if setup_error:
        placeholder_specs = specs_override or quality_specs(root, mode, base_sha or "<unresolved>")
        records = [
            GateRecord(
                name=spec.name,
                status="skipped",
                reason=f"setup failed: {setup_error}",
                duration_seconds=0.0,
                exit_code=None,
                log_path=None,
                rerun_command=spec.rerun_command,
            )
            for spec in placeholder_specs
        ]
        for record in records:
            print(f"gates: SKIP {record.name}: {record.reason}")
    else:
        specs = list(specs_override or quality_specs(root, mode, base_sha))
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        records = run_specs(
            root,
            specs,
            root / "quality" / "logs" / stamp,
            verbose=os.environ.get("VERBOSE") == "1",
            executor=executor,
        )

    end = tree_fingerprint(root)
    payload, exit_code = build_status(
        root,
        started_at=started_at,
        ended_at=utc_now(),
        mode=mode,
        base_sha=base_sha,
        start=start,
        end=end,
        records=records,
        versions=versions,
        setup_error=setup_error,
    )
    atomic_write_json(status_path, payload)
    print(f"gates: {payload['result']}; status: {status_path}")
    for action in payload["next_actions"]:
        print(f"gates: next: {action}")
    return exit_code


def run_check(root: Path, mode: str, explicit_base: str | None, dry_run: bool) -> int:
    scope = select_scope(root, mode=mode, explicit_base=explicit_base)
    if scope.docs_only:
        print("check: scope docs-only")
    elif scope.full_workspace:
        print("check: scope workspace")
    else:
        packages = ", ".join(scope.selected_packages) or "none"
        print(f"check: scope packages: {packages}")
    print("check: changed files: " + (", ".join(scope.changed_files) or "none"))
    if scope.fallback_reason:
        print(f"check: fallback: {scope.fallback_reason}")
    specs = check_specs(root, scope)
    if dry_run:
        for spec in specs:
            if spec.skip_reason:
                print(f"check: skip {spec.name}: {spec.skip_reason}")
            else:
                print(f"check: run {spec.name}: {shell_join(spec.command)}")
        return 0
    stamp = datetime.now(timezone.utc).strftime("check-%Y%m%dT%H%M%SZ")
    records = run_specs(
        root,
        specs,
        root / "quality" / "logs" / stamp,
        verbose=os.environ.get("VERBOSE") == "1",
    )
    failed = [record for record in records if record.status == "failed"]
    if any(record.exit_code == 2 for record in failed):
        return 2
    return 1 if failed else 0


def verify_status(root: Path) -> int:
    try:
        path = validate_managed_file(root, Path("quality") / "status.json")
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print("quality-status: missing quality/status.json; run make quality", file=sys.stderr)
        return 2
    except (OSError, json.JSONDecodeError) as error:
        print(f"quality-status: invalid status artifact: {error}", file=sys.stderr)
        return 2
    except WorkflowError as error:
        print(f"quality-status: unsafe status artifact: {error}", file=sys.stderr)
        return 2
    if not isinstance(payload, Mapping):
        print("quality-status: status artifact must be a JSON object", file=sys.stderr)
        return 2
    if payload.get("schema_version") != STATUS_SCHEMA_VERSION:
        print(
            "quality-status: status artifact schema is stale; rerun make quality",
            file=sys.stderr,
        )
        return 2
    if payload.get("result") != "passed":
        print(f"quality-status: last result is {payload.get('result', 'unknown')}", file=sys.stderr)
        return 1

    checks = payload.get("checks")
    if not isinstance(checks, list):
        print("quality-status: checks must be a list", file=sys.stderr)
        return 2
    if not all(isinstance(check, Mapping) for check in checks):
        print("quality-status: every check must be an object", file=sys.stderr)
        return 2
    names = tuple(check.get("name") for check in checks)
    if names != QUALITY_GATE_NAMES:
        print(
            "quality-status: check names/order do not match the required seven-gate policy",
            file=sys.stderr,
        )
        return 2
    if any(
        check.get("status") != "passed" or check.get("exit_code") != 0
        for check in checks
    ):
        print("quality-status: passed artifact contains a non-passing check", file=sys.stderr)
        return 2
    if payload.get("setup_error") is not None:
        print("quality-status: passed artifact contains a setup error", file=sys.stderr)
        return 2

    start = payload.get("start_fingerprint")
    end = payload.get("end_fingerprint")
    if not isinstance(start, Mapping) or not isinstance(end, Mapping):
        print("quality-status: fingerprint records are malformed", file=sys.stderr)
        return 2
    if start != end:
        print("quality-status: validation fingerprints do not match", file=sys.stderr)
        return 2
    expected = end.get("sha256")
    if not isinstance(expected, str) or not expected:
        print("quality-status: validated fingerprint digest is missing", file=sys.stderr)
        return 2
    try:
        current = tree_fingerprint(root)
    except WorkflowError as error:
        print(f"quality-status: current tree cannot be attested: {error}", file=sys.stderr)
        return 2
    if dict(end) != fingerprint_payload(current):
        print("quality-status: tree differs from the validated fingerprint; rerun make quality", file=sys.stderr)
        return 2
    print("quality-status: passed artifact matches the exact current tree")
    return 0


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    check = subparsers.add_parser("check")
    check.add_argument("--mode", choices=("worktree", "committed"), default="worktree")
    check.add_argument("--base-sha")
    check.add_argument("--dry-run", action="store_true")
    quality = subparsers.add_parser("quality")
    quality.add_argument("--mode", choices=("worktree", "staged", "committed"), default="worktree")
    quality.add_argument("--base-sha")
    subparsers.add_parser("verify-status")
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        if args.command == "check":
            return run_check(root, args.mode, args.base_sha, args.dry_run)
        if args.command == "quality":
            return orchestrate_quality(root, mode=args.mode, explicit_base=args.base_sha)
        return verify_status(root)
    except WorkflowError as error:
        print(f"gates: environment error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
