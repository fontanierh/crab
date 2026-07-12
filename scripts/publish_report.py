#!/usr/bin/env python3
"""Publish the generated code-quality report with content-derived provenance."""

from __future__ import annotations

import argparse
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import WorkflowError, repository_root, validate_managed_file


Runner = Callable[[Path, Sequence[str]], subprocess.CompletedProcess[bytes]]
Replacer = Callable[[Path, Path], None]
TITLE = "# Code Quality Report — Crab Project"
HEADER = re.compile(
    rb"\A# Code Quality Report \xe2\x80\x94 Crab Project\n\n"
    rb"Generated for commit `[^`]+`(?: plus uncommitted worktree changes)? "
    rb"\(commit date: [^)]+\)\.\n\n"
)


def run_git(root: Path, command: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    try:
        return subprocess.run(
            list(command),
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as error:
        raise WorkflowError(f"could not execute {' '.join(command)}: {error}") from error


def _required_git_output(root: Path, arguments: Sequence[str], runner: Runner) -> str:
    result = runner(root, ["git", *arguments])
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(
            f"git {' '.join(arguments)} failed with exit {result.returncode}: {detail}"
        )
    try:
        return result.stdout.decode("utf-8").strip()
    except UnicodeError as error:
        raise WorkflowError(f"git {' '.join(arguments)} returned non-UTF-8 output") from error


def _committed_report(root: Path, relative: Path, runner: Runner) -> bytes | None:
    path = relative.as_posix()
    lookup = runner(root, ["git", "ls-tree", "-z", "HEAD", "--", path])
    if lookup.returncode != 0:
        detail = lookup.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(
            f"git ls-tree failed with exit {lookup.returncode}: {detail}"
        )
    if not lookup.stdout:
        return None
    entries = lookup.stdout.split(b"\0")
    if entries[-1] != b"" or len(entries) != 2:
        raise WorkflowError("git ls-tree returned malformed or multiple report entries")
    try:
        metadata, recorded_path = entries[0].split(b"\t", 1)
        mode, object_type, _object_id = metadata.split(b" ", 2)
        decoded_path = recorded_path.decode("utf-8")
    except (ValueError, UnicodeError) as error:
        raise WorkflowError("git ls-tree returned an unparseable report entry") from error
    if object_type != b"blob" or not mode.startswith(b"100") or decoded_path != path:
        raise WorkflowError("git ls-tree report entry is not one regular blob")
    result = runner(root, ["git", "show", f"HEAD:{path}"])
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(f"git show failed with exit {result.returncode}: {detail}")
    return result.stdout


def _atomic_publish(path: Path, content: bytes, *, replace: Replacer) -> None:
    try:
        mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
    except OSError as error:
        raise WorkflowError(f"could not prepare atomic report publication for {path}: {error}") from error
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        replace(temporary, path)
    except OSError as error:
        raise WorkflowError(f"could not atomically publish report {path}: {error}") from error
    finally:
        if temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--out-path", default="CODE_QUALITY_REPORT.md")
    return parser.parse_args(arguments)


def main(
    arguments: list[str] | None = None,
    *,
    body: bytes | None = None,
    runner: Runner = run_git,
    replace: Replacer = os.replace,
) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        relative = Path(args.out_path)
        output = validate_managed_file(root, relative)
        if relative.suffix.lower() != ".md":
            raise WorkflowError("report destination must be a Markdown (.md) worktree file")
        rendered_body = sys.stdin.buffer.read() if body is None else body
        committed = _committed_report(root, relative, runner)
        match = HEADER.match(committed) if committed is not None else None
        committed_body = committed[match.end() :] if match is not None else None
        if committed is not None and committed_body == rendered_body:
            target = committed
            provenance = "preserved committed provenance"
        else:
            sha = _required_git_output(root, ["rev-parse", "HEAD"], runner)
            commit_date = _required_git_output(
                root, ["show", "-s", "--format=%cs", "HEAD"], runner
            )
            header = (
                f"{TITLE}\n\nGenerated for commit `{sha}` plus uncommitted worktree changes "
                f"(commit date: {commit_date}).\n\n"
            ).encode("utf-8")
            target = header + rendered_body
            provenance = "uncommitted-worktree provenance"
        try:
            current = output.read_bytes() if output.exists() else None
        except OSError as error:
            raise WorkflowError(f"could not read existing report {output}: {error}") from error
        if current != target:
            _atomic_publish(output, target, replace=replace)
        print(f"report-publisher: {provenance}: {output}")
        return 0
    except WorkflowError as error:
        print(f"report-publisher: environment error: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
