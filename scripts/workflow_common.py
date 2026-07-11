#!/usr/bin/env python3
"""Shared, stdlib-only helpers for Crab's repository workflow."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


class WorkflowError(RuntimeError):
    """An environment or repository error that should exit with status 2."""


@dataclass(frozen=True)
class Fingerprint:
    digest: str
    dirty: bool
    entries: tuple[tuple[str, str], ...]


def run_text(
    command: Sequence[str],
    *,
    cwd: Path,
    env: Mapping[str, str] | None = None,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(command),
        cwd=cwd,
        env=dict(env) if env is not None else None,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "no diagnostic output"
        raise WorkflowError(
            f"{' '.join(command)} failed with exit {result.returncode}: {detail}"
        )
    return result


def repository_root(start: Path | None = None) -> Path:
    cwd = (start or Path.cwd()).resolve()
    result = run_text(["git", "rev-parse", "--show-toplevel"], cwd=cwd)
    return Path(result.stdout.strip()).resolve()


def git_output(root: Path, *arguments: str, check: bool = True) -> str:
    return run_text(["git", *arguments], cwd=root, check=check).stdout.strip()


def path_is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def git_common_dir(root: Path) -> Path:
    raw = git_output(root, "rev-parse", "--git-common-dir")
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = root / candidate
    return candidate.resolve()


def worktree_roots(root: Path) -> tuple[Path, ...]:
    result = run_text(["git", "worktree", "list", "--porcelain"], cwd=root)
    roots = []
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            roots.append(Path(line.removeprefix("worktree ")).resolve())
    return tuple(roots)


def repository_namespace(root: Path) -> str:
    common_path = git_common_dir(root)
    common = str(common_path).encode("utf-8", errors="surrogateescape")
    digest = hashlib.sha256(common).hexdigest()[:16]
    identity_name = common_path.parent.name if common_path.name == ".git" else common_path.name
    slug = identity_name.removesuffix(".git").lower()
    safe_slug = "".join(character if character.isalnum() else "-" for character in slug)
    safe_slug = safe_slug.strip("-") or "repo"
    return f"{safe_slug}-{digest}"


def _nearest_existing_parent(path: Path) -> Path:
    candidate = path
    while not candidate.exists() and candidate != candidate.parent:
        candidate = candidate.parent
    return candidate


def validate_shared_target_base(root: Path, raw_value: str) -> Path:
    """Validate an opt-in shared base without creating it."""
    root = root.resolve()
    if not raw_value:
        raise WorkflowError("CRAB_SHARED_TARGET_DIR is set but empty")
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        raise WorkflowError(
            "CRAB_SHARED_TARGET_DIR must be an absolute path outside every Git worktree"
        )
    resolved = candidate.resolve(strict=False)
    forbidden = set(worktree_roots(root))
    forbidden.add(git_common_dir(root))
    for location in forbidden:
        if path_is_within(resolved, location):
            raise WorkflowError(
                "CRAB_SHARED_TARGET_DIR resolves inside a worktree or Git directory: "
                f"{resolved} (inside {location})"
            )
    existing_parent = _nearest_existing_parent(resolved)
    if not existing_parent.exists() or not existing_parent.is_dir():
        raise WorkflowError(
            f"no existing directory can contain CRAB_SHARED_TARGET_DIR: {resolved}"
        )
    if not os.access(existing_parent, os.W_OK):
        raise WorkflowError(
            f"CRAB_SHARED_TARGET_DIR is not writable via {existing_parent}"
        )
    if resolved.exists() and not resolved.is_dir():
        raise WorkflowError(f"CRAB_SHARED_TARGET_DIR is not a directory: {resolved}")
    return resolved / repository_namespace(root)


def validate_ambient_target_dir(root: Path, raw_value: str | None) -> Path | None:
    root = root.resolve()
    if raw_value is None:
        return None
    if not raw_value:
        raise WorkflowError("CARGO_TARGET_DIR is set but empty")
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    resolved = candidate.resolve(strict=False)
    checkout_target = (root / "target").resolve(strict=False)
    for worktree in worktree_roots(root):
        if not path_is_within(resolved, worktree):
            continue
        if worktree == root and path_is_within(resolved, checkout_target):
            return resolved
        raise WorkflowError(
            "ambient CARGO_TARGET_DIR points inside a repository worktree outside this "
            f"checkout's ignored target/: {resolved}; unset it or use "
            "CRAB_SHARED_TARGET_DIR with an external absolute path"
        )
    common = git_common_dir(root)
    if path_is_within(resolved, common):
        raise WorkflowError(
            f"ambient CARGO_TARGET_DIR points inside the Git common directory: {resolved}"
        )
    return resolved


def build_target_environment(root: Path, environ: Mapping[str, str]) -> dict[str, str]:
    root = root.resolve()
    result = dict(environ)
    shared = environ.get("CRAB_SHARED_TARGET_DIR")
    if shared is not None:
        namespaced = validate_shared_target_base(root, shared)
        namespaced.mkdir(parents=True, exist_ok=True)
        result["CARGO_TARGET_DIR"] = str(namespaced)
    else:
        validate_ambient_target_dir(root, environ.get("CARGO_TARGET_DIR"))
    return result


def coverage_target_environment(root: Path, environ: Mapping[str, str]) -> dict[str, str]:
    root = root.resolve()
    result = dict(environ)
    local_target = (root / "target" / "llvm-cov-worktree").resolve()
    local_target.mkdir(parents=True, exist_ok=True)
    result["CARGO_TARGET_DIR"] = str(local_target)
    result["CARGO_LLVM_COV_TARGET_DIR"] = str(local_target / "instrumented")
    result.pop("CRAB_SHARED_TARGET_DIR", None)
    return result


def shell_join(command: Sequence[str]) -> str:
    return shlex.join(command)


def _nul_paths(root: Path, command: Sequence[str]) -> set[str]:
    result = subprocess.run(
        list(command),
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(f"{' '.join(command)} failed: {detail}")
    return {
        item.decode("utf-8", errors="surrogateescape")
        for item in result.stdout.split(b"\0")
        if item
    }


def _working_blob_hash(path: Path) -> str:
    if path.is_symlink():
        payload = b"symlink\0" + os.readlink(path).encode(
            "utf-8", errors="surrogateescape"
        )
    elif path.is_file():
        payload = b"file\0" + path.read_bytes()
    elif path.exists():
        payload = b"other\0"
    else:
        payload = b"deleted\0"
    return hashlib.sha256(payload).hexdigest()


def tree_fingerprint(root: Path) -> Fingerprint:
    modified = _nul_paths(
        root,
        ["git", "diff", "--name-only", "-z", "HEAD", "--"],
    )
    untracked = _nul_paths(
        root,
        ["git", "ls-files", "--others", "--exclude-standard", "-z"],
    )
    paths = sorted(modified | untracked)
    entries = tuple((path, _working_blob_hash(root / path)) for path in paths)
    head = git_output(root, "rev-parse", "HEAD")
    digest_input = json.dumps(
        {"head": head, "entries": entries},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8", errors="surrogateescape")
    return Fingerprint(
        digest=hashlib.sha256(digest_input).hexdigest(),
        dirty=bool(entries),
        entries=entries,
    )


def atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def command_version(root: Path, command: Sequence[str]) -> str:
    result = run_text(command, cwd=root, check=False)
    output = (result.stdout or result.stderr).strip().splitlines()
    if result.returncode != 0 or not output:
        return "unavailable"
    return output[0]


def compact_reason(value: str, limit: int = 240) -> str:
    one_line = " ".join(value.split())
    if len(one_line) <= limit:
        return one_line
    return one_line[: limit - 3] + "..."


def unique_sorted(values: Iterable[str]) -> list[str]:
    return sorted(set(values))
