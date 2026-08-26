#!/usr/bin/env python3
"""Compact gate orchestration with full logs and exact-tree status attestation."""

from __future__ import annotations

import argparse
import json
import math
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


STATUS_SCHEMA_VERSION = 3
QUALITY_GATE_NAMES = (
    "fmt",
    "clippy",
    "tests",
)
LOG_TAIL_LINES = 30
CHECK_STATUSES = ("passed", "failed", "skipped")


def _wrong_type(name: str) -> WorkflowError:
    return WorkflowError(
        f"status artifact field {name} has the wrong type; rerun make quality"
    )


def _exact_string(value: object, name: str, *, nonempty: bool = False) -> None:
    if type(value) is not str or (nonempty and not value):
        raise _wrong_type(name)


def _optional_string(value: object, name: str) -> None:
    if value is not None and type(value) is not str:
        raise _wrong_type(name)


def _validate_fingerprint_types(value: object, name: str) -> None:
    if not isinstance(value, Mapping) or set(value) != {"sha256", "dirty", "entry_count"}:
        raise _wrong_type(name)
    _exact_string(value.get("sha256"), f"{name}.sha256", nonempty=True)
    if type(value.get("dirty")) is not bool:
        raise _wrong_type(f"{name}.dirty")
    count = value.get("entry_count")
    if type(count) is not int or count < 0:
        raise _wrong_type(f"{name}.entry_count")


def _validate_status_types(payload: Mapping[str, object]) -> None:
    if type(payload.get("schema_version")) is not int:
        raise _wrong_type("schema_version")
    for name in ("result", "started_at", "ended_at", "git_sha", "branch", "diff_mode"):
        _exact_string(payload.get(name), name, nonempty=True)
    for name in ("resolved_base_sha", "setup_error"):
        _optional_string(payload.get(name), name)
    if type(payload.get("dirty")) is not bool:
        raise _wrong_type("dirty")
    versions = payload.get("tool_versions")
    if not isinstance(versions, Mapping) or any(
        type(key) is not str or type(value) is not str for key, value in versions.items()
    ):
        raise _wrong_type("tool_versions")
    actions = payload.get("next_actions")
    if type(actions) is not list or any(type(action) is not str for action in actions):
        raise _wrong_type("next_actions")
    _validate_fingerprint_types(payload.get("start_fingerprint"), "start_fingerprint")
    _validate_fingerprint_types(payload.get("end_fingerprint"), "end_fingerprint")
    checks = payload.get("checks")
    if type(checks) is not list:
        raise _wrong_type("checks")
    for index, check in enumerate(checks):
        prefix = f"checks[{index}]"
        if not isinstance(check, Mapping):
            raise _wrong_type(prefix)
        for name in ("name", "rerun_command"):
            _exact_string(check.get(name), f"{prefix}.{name}")
        _exact_string(check.get("status"), f"{prefix}.status")
        if check.get("status") not in CHECK_STATUSES:
            raise _wrong_type(f"{prefix}.status")
        for name in ("reason", "log_path"):
            _optional_string(check.get(name), f"{prefix}.{name}")
        duration = check.get("duration_seconds")
        if (
            type(duration) not in (int, float)
            or not math.isfinite(duration)
            or duration < 0
        ):
            raise _wrong_type(f"{prefix}.duration_seconds")
        exit_code = check.get("exit_code")
        if exit_code is not None and type(exit_code) is not int:
            raise _wrong_type(f"{prefix}.exit_code")
        if check.get("status") == "passed" and (type(exit_code) is not int or exit_code != 0):
            raise _wrong_type(f"{prefix}.exit_code")


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
    lexical_root = root if root.is_absolute() else root.absolute()
    resolved_root = root.resolve()
    if log_directory.is_absolute():
        try:
            relative_log_directory = log_directory.relative_to(lexical_root)
        except ValueError as error:
            try:
                relative_log_directory = log_directory.relative_to(resolved_root)
            except ValueError:
                raise WorkflowError(
                    f"gate log directory must remain inside the repository: {log_directory}"
                ) from error
    else:
        relative_log_directory = log_directory
    root = resolved_root
    log_directory = validate_local_directory(
        root, relative_log_directory, create=True
    )
    planned_logs: dict[int, Path] = {}
    for index, spec in enumerate(specs, start=1):
        if spec.skip_reason:
            continue
        planned_logs[index] = validate_managed_file(
            root,
            (log_directory / f"{index:02d}-{spec.name}.log").relative_to(root),
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
        log_path = planned_logs[index]
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
    elif (
        not scope.changed_files
        and not scope.full_workspace
        and scope.fallback_reason is None
    ):
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


def quality_specs(root: Path, mode: str = "worktree", base_sha: str = "") -> list[GateSpec]:
    # ``mode`` and ``base_sha`` remain accepted for callers using the old API, but the
    # core handoff gate no longer depends on a Git diff or a coverage baseline.
    del mode, base_sha
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
    validate_local_directory(root, "quality", create=False)
    validate_local_directory(root, Path("quality") / "logs", create=False)
    status_path = prepare_status_path(root)
    started_at = utc_now()
    start = tree_fingerprint(root)
    versions = versions_override if versions_override is not None else tool_versions(root)
    # Full-workspace correctness does not need a merge base. Keeping the caller's
    # optional base in the status file is useful provenance without making a fetch a
    # prerequisite for a local handoff.
    base_sha: str | None = explicit_base
    setup_error: str | None = None
    specs = list(specs_override or quality_specs(root))
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


def run_check(
    root: Path,
    mode: str,
    explicit_base: str | None,
    dry_run: bool,
    executor: Executor = execute_command,
) -> int:
    validate_local_directory(root, "quality", create=False)
    validate_local_directory(root, Path("quality") / "logs", create=False)
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
        executor=executor,
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
    except (OSError, ValueError) as error:
        print(
            f"quality-status: status artifact is not valid UTF-8/JSON: {error}",
            file=sys.stderr,
        )
        return 2
    except WorkflowError as error:
        print(f"quality-status: unsafe status artifact: {error}", file=sys.stderr)
        return 2
    if not isinstance(payload, Mapping):
        print("quality-status: status artifact must be a JSON object", file=sys.stderr)
        return 2
    try:
        _validate_status_types(payload)
    except WorkflowError as error:
        print(f"quality-status: {error}", file=sys.stderr)
        return 2
    if payload.get("schema_version") != STATUS_SCHEMA_VERSION:
        print(
            "quality-status: status artifact schema is stale; rerun make quality",
            file=sys.stderr,
        )
        return 2
    result = payload.get("result")
    if result not in ("passed", "failed", "invalid"):
        print(
            f"quality-status: unknown result value {result!r}; rerun make quality",
            file=sys.stderr,
        )
        return 2
    checks = payload.get("checks")
    if not isinstance(checks, list):
        print("quality-status: checks must be a list", file=sys.stderr)
        return 2
    if not all(isinstance(check, Mapping) for check in checks):
        print("quality-status: every check must be an object", file=sys.stderr)
        return 2
    if "setup_error" not in payload:
        print("quality-status: setup_error key is missing; rerun make quality", file=sys.stderr)
        return 2
    if result == "invalid":
        print("quality-status: last result is invalid", file=sys.stderr)
        actions = payload.get("next_actions")
        if isinstance(actions, list):
            for action in actions:
                if isinstance(action, str):
                    print(f"quality-status: next: {action}", file=sys.stderr)
        return 2

    names = tuple(check.get("name") for check in checks)
    if names != QUALITY_GATE_NAMES:
        print(
            "quality-status: check names/order do not match the required gate policy",
            file=sys.stderr,
        )
        return 2
    if payload.get("setup_error") is not None:
        print(
            f"quality-status: {result} artifact contains a setup error",
            file=sys.stderr,
        )
        return 2

    if result == "passed":
        if any(
            check.get("status") != "passed" or check.get("exit_code") != 0
            for check in checks
        ):
            print("quality-status: passed artifact contains a non-passing check", file=sys.stderr)
            return 2
    else:
        failed_seen = False
        failed_shape_valid = False
        for check in checks:
            status = check.get("status")
            exit_code = check.get("exit_code")
            if not failed_seen and status == "passed" and exit_code == 0:
                continue
            if not failed_seen and status == "failed":
                if (
                    isinstance(exit_code, bool)
                    or not isinstance(exit_code, int)
                    or exit_code in (0, 2)
                    or not isinstance(check.get("log_path"), str)
                    or not check.get("log_path")
                    or not isinstance(check.get("rerun_command"), str)
                    or not check.get("rerun_command")
                ):
                    break
                failed_seen = True
                continue
            if failed_seen and status == "skipped" and exit_code is None:
                continue
            break
        else:
            failed_shape_valid = failed_seen
        if not failed_shape_valid:
            print(
                "quality-status: artifact claims failure but its checks are inconsistent; rerun make quality",
                file=sys.stderr,
            )
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
        qualifier = "stale failed artifact; " if result == "failed" else ""
        print(
            f"quality-status: {qualifier}tree differs from the validated fingerprint; rerun make quality",
            file=sys.stderr,
        )
        return 2
    if result == "failed":
        print("quality-status: last result is failed", file=sys.stderr)
        actions = payload.get("next_actions")
        if isinstance(actions, list):
            for action in actions:
                if isinstance(action, str):
                    print(f"quality-status: next: {action}", file=sys.stderr)
        return 1
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
