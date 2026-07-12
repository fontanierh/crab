#!/usr/bin/env python3
"""Shared, stdlib-only helpers for Crab's repository workflow."""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import stat
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence


COVERAGE_EXCLUDED_BASENAME = "test_support.rs"
COVERAGE_IGNORE_FILENAME_REGEX = r"(^|/)test_support\.rs$"


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
    try:
        result = subprocess.run(
            list(command),
            cwd=cwd,
            env=dict(env) if env is not None else None,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as error:
        if check:
            raise WorkflowError(f"could not execute {' '.join(command)}: {error}") from error
        return subprocess.CompletedProcess(list(command), 127, "", str(error))
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


def _path_exists(path: Path) -> bool:
    return os.path.lexists(path)


def validate_local_directory(
    root: Path, relative: str | Path, *, create: bool = False
) -> Path:
    """Validate a workflow-owned directory without following repository symlinks."""
    root = root.resolve()
    relative_path = Path(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise WorkflowError(f"managed directory must be relative to the repository: {relative}")
    if any(part.lower() == ".git" for part in relative_path.parts):
        raise WorkflowError(f"managed path must not intersect Git metadata: {relative}")
    candidate = root / relative_path
    if not path_is_within(candidate, root) or not path_is_within(
        candidate.resolve(strict=False), root
    ):
        raise WorkflowError(f"managed directory escapes the repository: {candidate}")

    current = root
    for component in relative_path.parts:
        current /= component
        if not _path_exists(current):
            continue
        try:
            mode = current.lstat().st_mode
        except OSError as error:
            raise WorkflowError(f"could not inspect managed directory {current}: {error}") from error
        if not stat.S_ISDIR(mode):
            raise WorkflowError(
                f"managed directory component must be a real directory, not a symlink or file: {current}"
            )

    if create:
        try:
            candidate.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise WorkflowError(f"could not create managed directory {candidate}: {error}") from error
        return validate_local_directory(root, relative_path, create=False)
    return candidate


def validate_managed_file(
    root: Path,
    relative: str | Path,
    *,
    create_parent: bool = False,
) -> Path:
    """Validate a workflow-owned file and its repository-local directory chain."""
    root = root.resolve()
    relative_path = Path(relative)
    if relative_path.is_absolute() or relative_path.name in ("", ".", ".."):
        raise WorkflowError(f"managed file must be relative to the repository: {relative}")
    if any(part.lower() == ".git" for part in relative_path.parts):
        raise WorkflowError(f"managed path must not intersect Git metadata: {relative}")
    parent = validate_local_directory(
        root, relative_path.parent, create=create_parent
    )
    candidate = parent / relative_path.name
    if _path_exists(candidate):
        try:
            mode = candidate.lstat().st_mode
        except OSError as error:
            raise WorkflowError(f"could not inspect managed file {candidate}: {error}") from error
        if not stat.S_ISREG(mode):
            raise WorkflowError(
                f"managed file must be a regular file, not a symlink or directory: {candidate}"
            )
    return candidate


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
    namespaced = resolved / repository_namespace(root)
    resolved_namespaced = namespaced.resolve(strict=False)
    if not path_is_within(resolved_namespaced, resolved):
        raise WorkflowError(
            f"shared Cargo target namespace escapes its validated base: {namespaced}"
        )
    for candidate_path, label in (
        (resolved, "CRAB_SHARED_TARGET_DIR"),
        (resolved_namespaced, "shared Cargo target namespace"),
    ):
        for location in forbidden:
            if path_is_within(candidate_path, location) or path_is_within(
                location, candidate_path
            ):
                raise WorkflowError(
                    f"{label} must be disjoint from every worktree and Git directory: "
                    f"{candidate_path} is inside or contains {location}"
                )
    if _path_exists(namespaced):
        try:
            mode = namespaced.lstat().st_mode
        except OSError as error:
            raise WorkflowError(
                f"could not inspect shared Cargo target namespace {namespaced}: {error}"
            ) from error
        if not stat.S_ISDIR(mode):
            raise WorkflowError(
                "shared Cargo target namespace must be a real directory, not a symlink "
                f"or file: {namespaced}"
            )
    return namespaced


def validate_ambient_target_dir(root: Path, raw_value: str | None) -> Path | None:
    root = root.resolve()
    if raw_value is None:
        return None
    if not raw_value:
        raise WorkflowError("CARGO_TARGET_DIR is set but empty")
    candidate = Path(raw_value).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    validate_local_directory(root, "target", create=False)
    resolved = candidate.resolve(strict=False)
    checkout_target = root / "target"
    if path_is_within(resolved, checkout_target):
        relative = resolved.relative_to(root)
        validate_local_directory(root, relative, create=False)
        return resolved
    raise WorkflowError(
        "ambient CARGO_TARGET_DIR must resolve inside this checkout's target/: "
        f"{resolved}; unset it or opt in to an external cache with CRAB_SHARED_TARGET_DIR"
    )


def build_target_environment(
    root: Path, environ: Mapping[str, str], *, create: bool = True
) -> dict[str, str]:
    root = root.resolve()
    result = dict(environ)
    shared = environ.get("CRAB_SHARED_TARGET_DIR")
    if shared is not None:
        namespaced = validate_shared_target_base(root, shared)
        if create:
            try:
                namespaced.mkdir(parents=True, exist_ok=True)
            except OSError as error:
                raise WorkflowError(
                    f"could not create shared Cargo target namespace {namespaced}: {error}"
                ) from error
            validate_shared_target_base(root, shared)
        result["CARGO_TARGET_DIR"] = str(namespaced)
    else:
        ambient = validate_ambient_target_dir(root, environ.get("CARGO_TARGET_DIR"))
        if ambient is None:
            validate_local_directory(root, "target", create=False)
    return result


def coverage_target_environment(
    root: Path, environ: Mapping[str, str], *, create: bool = True
) -> dict[str, str]:
    root = root.resolve()
    result = dict(environ)
    local_target = validate_local_directory(
        root, Path("target") / "llvm-cov-worktree", create=create
    )
    instrumented = validate_local_directory(
        root,
        Path("target") / "llvm-cov-worktree" / "instrumented",
        create=create,
    )
    result["CARGO_TARGET_DIR"] = str(local_target)
    result["CARGO_LLVM_COV_TARGET_DIR"] = str(instrumented)
    result["CARGO_LLVM_COV_BUILD_DIR"] = str(instrumented)
    result.pop("CRAB_SHARED_TARGET_DIR", None)
    return result


def shell_join(command: Sequence[str]) -> str:
    return shlex.join(command)


def _working_blob_hash(path: Path) -> str:
    try:
        if path.is_symlink():
            payload = b"symlink\0" + os.readlink(path).encode(
                "utf-8", errors="surrogateescape"
            )
        elif path.is_file():
            executable = bool(path.stat().st_mode & stat.S_IXUSR)
            mode = b"100755" if executable else b"100644"
            payload = b"file:" + mode + b"\0" + path.read_bytes()
        elif path.exists():
            payload = b"other\0"
        else:
            payload = b"deleted\0"
    except OSError as error:
        raise WorkflowError(f"could not hash working-tree path {path}: {error}") from error
    return hashlib.sha256(payload).hexdigest()


def _preview_paths(paths: Sequence[str]) -> str:
    ordered = sorted(set(paths))
    preview = ", ".join(ordered[:8])
    if len(ordered) > 8:
        preview += f", ... ({len(ordered) - 8} more)"
    return preview


def _index_flags(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-v", "-z"],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as error:
        raise WorkflowError(f"could not inspect Git index flags: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(f"git ls-files -v -z failed: {detail}")
    flagged: list[str] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        decoded = raw.decode("utf-8", errors="surrogateescape")
        if len(decoded) < 3 or decoded[1] != " ":
            raise WorkflowError("git ls-files returned an unrecognized index entry")
        marker, path = decoded[0], decoded[2:]
        if marker.islower() or marker in ("S", "s"):
            flagged.append(path)
    return flagged


def _status_paths(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            [
                "git",
                "status",
                "--porcelain=v2",
                "-z",
                "--untracked-files=all",
                "--no-renames",
            ],
            cwd=root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except OSError as error:
        raise WorkflowError(f"could not inspect Git status: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise WorkflowError(f"git status --porcelain=v2 failed: {detail}")

    paths: list[str] = []
    split_paths: list[str] = []
    restored_deletions: list[str] = []
    unmerged_paths: list[str] = []
    intent_to_add_paths: list[str] = []
    staged_paths: list[str] = []
    unstaged_paths: list[str] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        record = raw.decode("utf-8", errors="surrogateescape")
        kind = record[0]
        if kind == "1" and record.startswith("1 "):
            fields = record.split(" ", 8)
            if len(fields) != 9 or len(fields[1]) != 2:
                raise WorkflowError("git status returned a malformed tracked-file record")
            index_state, worktree_state = fields[1]
            path = fields[8]
            paths.append(path)
            if index_state != ".":
                staged_paths.append(path)
            if worktree_state != ".":
                unstaged_paths.append(path)
            if index_state != "." and worktree_state != ".":
                split_paths.append(path)
            if index_state == "D" and _path_exists(root / path):
                restored_deletions.append(path)
            if worktree_state == "A":
                intent_to_add_paths.append(path)
        elif kind == "?" and record.startswith("? "):
            path = record[2:]
            paths.append(path)
            unstaged_paths.append(path)
        elif kind == "u" and record.startswith("u "):
            fields = record.split(" ", 10)
            unmerged_paths.append(fields[-1])
        else:
            preview = record[2:] if len(record) > 2 else record
            raise WorkflowError(
                "git status returned an unsupported record for "
                f"{preview!r}; resolve renames/conflicts and rerun"
            )

    if split_paths:
        raise WorkflowError(
            "index and working tree contain split content for: "
            f"{_preview_paths(split_paths)}; stage or restore so the index matches the working tree"
        )
    if restored_deletions:
        raise WorkflowError(
            "staged deletions have paths restored in the working tree: "
            f"{_preview_paths(restored_deletions)}; stage or restore so the index matches the working tree"
        )
    if unmerged_paths:
        raise WorkflowError(
            f"unmerged paths prevent attestation: {_preview_paths(unmerged_paths)}; resolve conflicts and rerun"
        )
    if intent_to_add_paths:
        raise WorkflowError(
            "intent-to-add entries prevent attestation: "
            f"{_preview_paths(intent_to_add_paths)}; fully git add the files or git reset them, then rerun"
        )
    if staged_paths and unstaged_paths:
        raise WorkflowError(
            "the Git index must be entirely HEAD or the fully staged validated worktree; "
            f"staged: {_preview_paths(staged_paths)}; unstaged/untracked: {_preview_paths(unstaged_paths)}. "
            "Run git add -A to stage the exact validated tree, or git reset to unstage everything, then rerun"
        )
    return paths


def attestation_preflight(root: Path) -> None:
    """Reject Git settings and index flags that hide snapshot inputs."""
    filemode = run_text(
        ["git", "config", "--bool", "--get", "core.filemode"],
        cwd=root,
        check=False,
    )
    if filemode.returncode == 0 and filemode.stdout.strip().lower() == "false":
        raise WorkflowError(
            "git core.filemode=false prevents executable-mode attestation; set core.filemode=true and rerun"
        )
    flagged = _index_flags(root)
    if flagged:
        raise WorkflowError(
            "assume-unchanged or skip-worktree flags hide paths from attestation: "
            f"{_preview_paths(flagged)}; clear assume-unchanged/skip-worktree flags and rerun"
        )


def tree_fingerprint(root: Path) -> Fingerprint:
    attestation_preflight(root)
    paths = sorted(set(_status_paths(root)))
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
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
        )
    except OSError as error:
        raise WorkflowError(f"could not prepare atomic JSON write for {path}: {error}") from error
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except OSError as error:
        raise WorkflowError(f"could not atomically write JSON artifact {path}: {error}") from error
    finally:
        if temporary.exists():
            try:
                temporary.unlink()
            except OSError:
                pass


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
