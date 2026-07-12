#!/usr/bin/env python3
"""Capture validated runtime and density baselines for the repository workflow."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.lcov_stats import parse_lcov
from scripts.patch_coverage import is_production_rust
from scripts.workflow_common import (
    WorkflowError,
    atomic_write_json,
    git_output,
    repository_root,
    run_text,
    validate_local_directory,
    validate_managed_file,
)


Runner = Callable[[Path, Sequence[str], Path, Mapping[str, str]], tuple[int, float]]
Replacer = Callable[[str | bytes | os.PathLike[str] | os.PathLike[bytes], str | bytes | os.PathLike[str] | os.PathLike[bytes]], None]
TEST_ATTRIBUTE = re.compile(r"^\s*#\[(?:tokio::)?test\]", re.MULTILINE)
CFG_NOT_COVERAGE = re.compile(r"cfg\(not\(coverage\)\)")


def run_timed_command(
    root: Path,
    command: Sequence[str],
    log_path: Path,
    environment: Mapping[str, str],
) -> tuple[int, float]:
    started = time.monotonic()
    try:
        with log_path.open("w", encoding="utf-8") as log:
            result = subprocess.run(
                list(command),
                cwd=root,
                env=dict(environment),
                stdout=log,
                stderr=subprocess.STDOUT,
                check=False,
            )
    except OSError as error:
        raise WorkflowError(f"could not execute {' '.join(command)}: {error}") from error
    return result.returncode, time.monotonic() - started


def _history_entries(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as error:
        raise WorkflowError(f"could not read baseline history {path}: {error}") from error
    entries: list[dict[str, object]] = []
    for line_number, raw in enumerate(lines, start=1):
        if not raw.strip():
            continue
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as error:
            raise WorkflowError(
                f"baseline history {path} line {line_number} is invalid JSON: {error}"
            ) from error
        if not isinstance(payload, dict):
            raise WorkflowError(
                f"baseline history {path} line {line_number} must be a JSON object"
            )
        entries.append(payload)
    return entries


def _source_metrics(root: Path) -> tuple[int, int, int]:
    production_loc = 0
    test_attributes = 0
    cfg_not_coverage = 0
    crates = root / "crates"
    try:
        candidates = sorted(path for path in crates.rglob("*.rs") if path.is_file())
        for path in candidates:
            relative = path.relative_to(root).as_posix()
            text = path.read_text(encoding="utf-8")
            if is_production_rust(relative):
                production_loc += len(text.splitlines())
            test_attributes += len(TEST_ATTRIBUTE.findall(text))
            cfg_not_coverage += len(CFG_NOT_COVERAGE.findall(text))
    except (OSError, UnicodeError) as error:
        raise WorkflowError(f"could not collect repository source metrics: {error}") from error
    return production_loc, test_attributes, cfg_not_coverage


def _churn_count(root: Path, days: int) -> int:
    result = run_text(
        [
            "git",
            "log",
            f"--since={days} days ago",
            "--oneline",
            "--grep=coverage\\|quality[- ]gate",
        ],
        cwd=root,
    )
    return len(result.stdout.splitlines())


def collect_metrics(root: Path, *, require_lcov: bool) -> dict[str, object]:
    production_loc, test_attributes, cfg_not_coverage = _source_metrics(root)
    lcov_path = root / "coverage" / "lcov.info"
    uncovered_lines: int | None = None
    uncovered_files: int | None = None
    if lcov_path.exists():
        coverage = parse_lcov(root, lcov_path)
        uncovered_lines = coverage.uncovered_lines
        uncovered_files = coverage.uncovered_files
    elif require_lcov:
        raise WorkflowError(
            "coverage gate succeeded but coverage/lcov.info is missing; rerun after fixing coverage evidence generation"
        )
    tests_per_kloc = (
        round(test_attributes / (production_loc / 1000.0), 2)
        if production_loc > 0
        else 0.0
    )
    return {
        "repo_metrics": {
            "production_loc": production_loc,
            "test_attribute_count": test_attributes,
            "tests_per_kloc": tests_per_kloc,
            "cfg_not_coverage_count": cfg_not_coverage,
            "uncovered_lines": uncovered_lines,
            "uncovered_files": uncovered_files,
        },
        "churn": {
            "quality_fix_like_commits_last_30d": _churn_count(root, 30),
            "quality_fix_like_commits_last_90d": _churn_count(root, 90),
        },
    }


def _atomic_write_text(path: Path, content: str, *, replace: Replacer) -> None:
    existing_mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    try:
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
    except OSError as error:
        raise WorkflowError(f"could not prepare atomic write for {path}: {error}") from error
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, existing_mode)
        replace(temporary, path)
    except OSError as error:
        raise WorkflowError(f"could not atomically write baseline history {path}: {error}") from error
    finally:
        if temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--print-metrics", action="store_true")
    return parser.parse_args(arguments)


def main(
    arguments: list[str] | None = None,
    *,
    runner: Runner = run_timed_command,
    replace: Replacer = os.replace,
) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        if args.print_metrics:
            print(json.dumps(collect_metrics(root, require_lcov=False), sort_keys=True))
            return 0

        validate_local_directory(root, Path("quality") / "baselines", create=True)
        leaves = {
            name: validate_managed_file(
                root, Path("quality") / "baselines" / name
            )
            for name in ("test.log", "coverage-gate.log", "latest.json", "history.jsonl")
        }
        history = _history_entries(leaves["history.jsonl"])
        environment = dict(os.environ)
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        commands = (
            (
                "test",
                (
                    sys.executable,
                    str(root / "scripts" / "cargo_target.py"),
                    "build",
                    "--",
                    "cargo",
                    "test",
                    "--workspace",
                    "--all-features",
                    "--locked",
                ),
                leaves["test.log"],
            ),
            (
                "coverage-gate",
                ("bash", str(root / "scripts" / "coverage_gate.sh")),
                leaves["coverage-gate.log"],
            ),
        )
        durations: dict[str, int] = {}
        for label, command, log_path in commands:
            print(f"Collecting baseline: {label}")
            exit_code, duration = runner(root, command, log_path, environment)
            if exit_code != 0:
                classification = 2 if exit_code == 2 else 1
                print(
                    f"quality-baseline: {label} failed with exit {exit_code}; inspect {log_path}",
                    file=sys.stderr,
                )
                return classification
            durations[label] = int(duration)

        metrics = collect_metrics(root, require_lcov=True)
        git_sha = git_output(root, "rev-parse", "HEAD")
        payload: dict[str, object] = {
            "timestamp_utc": datetime.now(timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "Z"),
            "git_sha": git_sha,
            "runtime_seconds": {
                "make_test": durations["test"],
                "make_coverage_gate": durations["coverage-gate"],
            },
            **metrics,
        }
        if not any(entry.get("git_sha") == git_sha for entry in history):
            history.append(payload)
        history_text = "".join(
            json.dumps(entry, separators=(",", ":"), sort_keys=True) + "\n"
            for entry in history
        )
        _atomic_write_text(leaves["history.jsonl"], history_text, replace=replace)
        atomic_write_json(leaves["latest.json"], payload)
        print(f"wrote baseline latest: {leaves['latest.json']}")
        print(f"updated baseline history: {leaves['history.jsonl']}")
        return 0
    except WorkflowError as error:
        print(f"quality-baseline: environment error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
