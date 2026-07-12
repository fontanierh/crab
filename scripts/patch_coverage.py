#!/usr/bin/env python3
"""Enforce changed executable-line coverage against a deliberate Git diff mode."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Mapping, Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.changed_scope import is_docs_path
from scripts.workflow_common import (
    WorkflowError,
    atomic_write_json,
    compact_reason,
    repository_root,
    run_text,
    validate_managed_file,
)


PATCH_PERCENT = 95
HUMAN_RANGE_LIMIT = 50
HUNK_PATTERN = re.compile(r"^@@ -\d+(?:,\d+)? \+(\d+)(?:,(\d+))? @@")


@dataclass(frozen=True)
class CoverageResult:
    mode: str
    base_sha: str
    authoritative_lcov: bool
    changed_executable_lines: int
    covered_changed_lines: int
    uncovered_changed_lines: int
    allowed_uncovered_lines: int
    patch_percent: float
    threshold_percent: int
    passed: bool
    uncovered_lines: list[dict[str, object]]
    unrepresented_files: list[str]


def is_production_rust(path: str) -> bool:
    candidate = PurePosixPath(path)
    if candidate.suffix != ".rs":
        return False
    if len(candidate.parts) < 4 or candidate.parts[0] != "crates":
        return False
    if "tests" in candidate.parts or "src" not in candidate.parts:
        return False
    return candidate.name != "test_support.rs"


def allowed_uncovered(changed_executable_lines: int) -> int:
    return (changed_executable_lines * (100 - PATCH_PERCENT)) // 100


def resolve_patch_base(root: Path, explicit_base: str | None) -> str:
    reference = explicit_base or "origin/main"
    exists = run_text(
        ["git", "cat-file", "-e", f"{reference}^{{commit}}"],
        cwd=root,
        check=False,
    )
    if exists.returncode != 0:
        raise WorkflowError(
            f"baseline {reference!r} is not available; fetch the base commit "
            "(`git fetch origin main`) or pass --base-sha <commit>"
        )
    merge_base = run_text(
        ["git", "merge-base", reference, "HEAD"], cwd=root, check=False
    )
    if merge_base.returncode != 0 or not merge_base.stdout.strip():
        raise WorkflowError(
            f"no merge base exists with {reference!r}; fetch history or pass a resolvable --base-sha"
        )
    return merge_base.stdout.strip()


def _git_paths(root: Path, arguments: Sequence[str]) -> list[str]:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as error:
        raise WorkflowError(f"could not execute git {' '.join(arguments)}: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(f"git {' '.join(arguments)} failed: {detail}")
    return [
        value.decode("utf-8", errors="surrogateescape")
        for value in result.stdout.split(b"\0")
        if value
    ]


def _non_doc_paths(paths: Sequence[str]) -> list[str]:
    return sorted({path for path in paths if not is_docs_path(path)})


def _render_snapshot_differences(kinds: Mapping[str, Sequence[str]]) -> str:
    rendered: list[str] = []
    for kind, paths in kinds.items():
        if not paths:
            continue
        preview = ", ".join(paths[:8])
        if len(paths) > 8:
            preview += f", ... ({len(paths) - 8} more)"
        rendered.append(f"{kind}: {preview}")
    return "; ".join(rendered)


def validate_diff_mode(root: Path, mode: str) -> None:
    if mode == "worktree":
        return
    if mode == "staged":
        # Guard the whole non-doc tree because the working copies of the gate tooling,
        # manifests, fixtures, and configuration determine what the staged snapshot tests.
        differences = {
            "unstaged": _non_doc_paths(
                _git_paths(root, ["diff", "--name-only", "--no-renames", "-z", "--"])
            ),
            "untracked": _non_doc_paths(
                _git_paths(root, ["ls-files", "--others", "--exclude-standard", "-z"])
            ),
        }
        if any(differences.values()):
            raise WorkflowError(
                "staged mode requires every non-documentation working-tree input to match "
                f"the index; {_render_snapshot_differences(differences)}. Stage/restore it or use --mode worktree"
            )
        return
    if mode == "committed":
        differences = {
            "working tree vs HEAD": _non_doc_paths(
                _git_paths(
                    root,
                    ["diff", "--name-only", "--no-renames", "-z", "HEAD", "--"],
                )
            ),
            "index vs HEAD": _non_doc_paths(
                _git_paths(
                    root,
                    [
                        "diff",
                        "--cached",
                        "--name-only",
                        "--no-renames",
                        "-z",
                        "HEAD",
                        "--",
                    ],
                )
            ),
            "untracked": _non_doc_paths(
                _git_paths(root, ["ls-files", "--others", "--exclude-standard", "-z"])
            ),
        }
        if any(differences.values()):
            raise WorkflowError(
                "committed mode requires every non-documentation input to match HEAD; "
                f"{_render_snapshot_differences(differences)}. Clean the tree or use --mode worktree"
            )
        return
    raise WorkflowError(f"unsupported patch mode: {mode}")


def _diff_arguments(mode: str, base_sha: str) -> list[str]:
    if mode == "committed":
        return [base_sha, "HEAD"]
    if mode == "staged":
        return ["--cached", base_sha]
    if mode == "worktree":
        return [base_sha]
    raise WorkflowError(f"unsupported patch mode: {mode}")


def _added_lines_from_diff(text: str) -> dict[int, str]:
    additions: dict[int, str] = {}
    next_new_line: int | None = None
    for raw_line in text.splitlines():
        if raw_line.startswith("diff "):
            next_new_line = None
            continue
        match = HUNK_PATTERN.match(raw_line)
        if match:
            next_new_line = int(match.group(1))
            continue
        if next_new_line is None:
            continue
        if raw_line.startswith("+"):
            additions[next_new_line] = raw_line[1:]
            next_new_line += 1
        elif raw_line.startswith("-") or raw_line.startswith("\\"):
            continue
        else:
            next_new_line += 1
    return additions


def collect_added_production_lines(
    root: Path, mode: str, base_sha: str
) -> dict[str, dict[int, str]]:
    diff_arguments = _diff_arguments(mode, base_sha)
    changed = _git_paths(
        root,
        [
            "diff",
            "--name-only",
            "-z",
            "--no-renames",
            *diff_arguments,
            "--",
            "crates",
        ],
    )
    additions: dict[str, dict[int, str]] = {}
    for path in sorted(set(changed)):
        if not is_production_rust(path):
            continue
        result = run_text(
            [
                "git",
                "diff",
                "--unified=0",
                "--no-color",
                "--no-ext-diff",
                "--no-renames",
                *diff_arguments,
                "--",
                path,
            ],
            cwd=root,
        )
        additions[path] = _added_lines_from_diff(result.stdout)

    if mode == "worktree":
        untracked = _git_paths(
            root, ["ls-files", "--others", "--exclude-standard", "-z", "--", "crates"]
        )
        for path in untracked:
            if not is_production_rust(path):
                continue
            try:
                lines = (root / path).read_text(encoding="utf-8").splitlines()
            except (OSError, UnicodeError) as error:
                raise WorkflowError(f"could not read untracked Rust file {path}: {error}") from error
            additions[path] = {index: line for index, line in enumerate(lines, start=1)}
    return additions


def parse_lcov(root: Path, lcov_path: Path) -> tuple[set[str], dict[str, dict[int, int]]]:
    root = root.resolve()
    if not lcov_path.is_file():
        raise WorkflowError(f"LCOV input does not exist: {lcov_path}")
    represented: set[str] = set()
    hits: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    current: str | None = None
    try:
        raw_lines = lcov_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as error:
        raise WorkflowError(f"could not read LCOV input {lcov_path}: {error}") from error
    for raw in raw_lines:
        line = raw.strip()
        if line.startswith("SF:"):
            source = Path(line[3:])
            if not source.is_absolute():
                source = root / source
            try:
                relative = source.resolve(strict=False).relative_to(root).as_posix()
            except ValueError:
                current = None
                continue
            current = relative if is_production_rust(relative) else None
            if current is not None:
                represented.add(current)
            continue
        if line == "end_of_record":
            current = None
            continue
        if current is None or not line.startswith("DA:"):
            continue
        fields = line[3:].split(",")
        if len(fields) < 2:
            continue
        try:
            line_number = int(fields[0])
            line_hits = int(fields[1])
        except ValueError:
            continue
        hits[current][line_number] += line_hits
    return represented, {path: dict(values) for path, values in hits.items()}


def _has_meaningful_added_line(lines: Mapping[int, str]) -> bool:
    in_block_comment = False
    for _, raw in sorted(lines.items()):
        text = raw.strip()
        if not text:
            continue
        if in_block_comment:
            if "*/" in text:
                text = text.split("*/", 1)[1].strip()
                in_block_comment = False
            else:
                continue
        while text.startswith("/*"):
            if "*/" not in text[2:]:
                in_block_comment = True
                text = ""
                break
            text = text.split("*/", 1)[1].strip()
        if text and not text.startswith("//"):
            return True
    return False


def evaluate_patch(
    *,
    mode: str,
    base_sha: str,
    additions: Mapping[str, Mapping[int, str]],
    represented: set[str],
    hits: Mapping[str, Mapping[int, int]],
    authoritative_lcov: bool,
) -> CoverageResult:
    unrepresented = sorted(
        path
        for path, lines in additions.items()
        if path not in represented and _has_meaningful_added_line(lines)
    )
    uncovered: list[dict[str, object]] = []
    executable_count = 0
    for path, changed_lines in sorted(additions.items()):
        file_hits = hits.get(path, {})
        for line_number in sorted(set(changed_lines) & set(file_hits)):
            executable_count += 1
            if file_hits[line_number] <= 0:
                uncovered.append({"path": path, "line": line_number})
    uncovered_count = len(uncovered)
    allowed = allowed_uncovered(executable_count)
    covered = executable_count - uncovered_count
    percent = 100.0 if executable_count == 0 else (covered * 100.0 / executable_count)
    passed = not unrepresented and uncovered_count <= allowed
    return CoverageResult(
        mode=mode,
        base_sha=base_sha,
        authoritative_lcov=authoritative_lcov,
        changed_executable_lines=executable_count,
        covered_changed_lines=covered,
        uncovered_changed_lines=uncovered_count,
        allowed_uncovered_lines=allowed,
        patch_percent=round(percent, 4),
        threshold_percent=PATCH_PERCENT,
        passed=passed,
        uncovered_lines=uncovered,
        unrepresented_files=unrepresented,
    )


def compact_line_ranges(uncovered: Sequence[Mapping[str, object]]) -> list[str]:
    by_file: dict[str, list[int]] = defaultdict(list)
    for item in uncovered:
        by_file[str(item["path"])].append(int(item["line"]))
    output: list[str] = []
    for path, numbers in sorted(by_file.items()):
        ordered = sorted(set(numbers))
        if not ordered:
            continue
        start = previous = ordered[0]
        for number in ordered[1:] + [ordered[-1] + 2]:
            if number == previous + 1:
                previous = number
                continue
            suffix = str(start) if start == previous else f"{start}-{previous}"
            output.append(f"{path}:{suffix}")
            start = previous = number
    return output


def write_result(path: Path, result: CoverageResult, lcov_path: Path) -> None:
    payload = asdict(result)
    payload["schema_version"] = 1
    payload["lcov_path"] = str(lcov_path)
    atomic_write_json(path, payload)


def render_result(result: CoverageResult, artifact: Path) -> None:
    if result.unrepresented_files:
        print("patch-coverage: failed; changed production files are absent from LCOV:")
        for path in result.unrepresented_files:
            print(f"  - {path}")
        print("These files contain added code but were not compiled by the coverage run.")
    if result.uncovered_lines:
        ranges = compact_line_ranges(result.uncovered_lines)
        print(
            "patch-coverage: uncovered changed executable lines "
            f"({result.uncovered_changed_lines}/{result.changed_executable_lines}; "
            f"allowed {result.allowed_uncovered_lines})"
        )
        for value in ranges[:HUMAN_RANGE_LIMIT]:
            print(f"  - {value}")
        if len(ranges) > HUMAN_RANGE_LIMIT:
            print(
                f"  ... {len(ranges) - HUMAN_RANGE_LIMIT} more range(s); "
                "the JSON artifact is complete"
            )
    if result.passed:
        print(
            "patch-coverage: passed "
            f"({result.covered_changed_lines}/{result.changed_executable_lines} executable "
            f"changed lines covered; {result.patch_percent:.2f}%; "
            f"allowed uncovered {result.allowed_uncovered_lines})"
        )
    else:
        print(
            "patch-coverage: failed "
            f"(requires {PATCH_PERCENT}% with floor(5% × changed lines) allowance)"
        )
    print(f"patch-coverage: complete artifact: {artifact}")


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("worktree", "staged", "committed"), default="worktree")
    parser.add_argument("--base-sha")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--lcov",
        type=Path,
        help="diagnostic/test LCOV injection; never an authoritative freshness attestation",
    )
    source.add_argument("--fresh-lcov", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        validate_diff_mode(root, args.mode)
        base_sha = resolve_patch_base(root, args.base_sha)
        lcov_path = (args.fresh_lcov or args.lcov).resolve()
        if args.output_json:
            artifact = args.output_json.resolve()
            try:
                relative_artifact = artifact.relative_to(root)
            except ValueError:
                relative_artifact = None
            if relative_artifact and relative_artifact.parts[:1] == ("coverage",):
                artifact = validate_managed_file(
                    root, relative_artifact, create_parent=True
                )
        else:
            artifact = validate_managed_file(
                root,
                Path("coverage") / "patch-coverage.json",
                create_parent=True,
            )
        additions = collect_added_production_lines(root, args.mode, base_sha)
        represented, hits = parse_lcov(root, lcov_path)
        result = evaluate_patch(
            mode=args.mode,
            base_sha=base_sha,
            additions=additions,
            represented=represented,
            hits=hits,
            authoritative_lcov=args.fresh_lcov is not None,
        )
        write_result(artifact, result, lcov_path)
    except WorkflowError as error:
        print(f"patch-coverage: environment/baseline error: {compact_reason(str(error))}", file=sys.stderr)
        return 2
    render_result(result, artifact)
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
