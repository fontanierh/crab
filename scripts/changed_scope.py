#!/usr/bin/env python3
"""Select conservative changed-scope Rust checks using Git before Cargo metadata."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping, Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import WorkflowError, compact_reason, repository_root, run_text


FULL_SCOPE_NAMES = {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml", "Makefile"}
FULL_SCOPE_PREFIXES = ("scripts/", ".github/workflows/", ".cargo/")
DOC_DIRECTORY_PREFIXES = ("docs/", "crab/docs/", "notes/", "design/")
DOC_FILES = {
    "README.md",
    "AGENTS.md",
    "CLAUDE.md",
    "CONTRIBUTING.md",
    "PHILOSOPHY.md",
    "CODE_QUALITY_REPORT.md",
    "crab/DESIGN.md",
    "crab/WORKSTREAMS.md",
    ".github/pull_request_template.md",
}
DOC_SUFFIXES = {".md", ".mdx", ".rst", ".txt"}


@dataclass(frozen=True)
class ScopeResult:
    mode: str
    base_sha: str | None
    changed_files: list[str]
    selected_packages: list[str]
    full_workspace: bool
    docs_only: bool
    fallback_reason: str | None


def is_docs_path(path: str) -> bool:
    normalized = PurePosixPath(path).as_posix()
    has_document_suffix = PurePosixPath(normalized.lower()).suffix in DOC_SUFFIXES
    return has_document_suffix and (
        normalized.startswith(DOC_DIRECTORY_PREFIXES) or normalized in DOC_FILES
    )


def is_full_scope_trigger(path: str) -> bool:
    normalized = PurePosixPath(path).as_posix()
    return (
        PurePosixPath(normalized).name == "Cargo.toml"
        or normalized in FULL_SCOPE_NAMES
        or normalized.startswith(FULL_SCOPE_PREFIXES)
    )


def classify_paths(paths: Sequence[str]) -> tuple[bool, bool]:
    if not paths:
        return False, False
    full_scope = any(is_full_scope_trigger(path) for path in paths)
    docs_only = not full_scope and all(is_docs_path(path) for path in paths)
    return docs_only, full_scope


def _git_bytes(root: Path, arguments: Sequence[str]) -> list[str]:
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
        item.decode("utf-8", errors="surrogateescape")
        for item in result.stdout.split(b"\0")
        if item
    ]


def resolve_merge_base(root: Path, explicit_base: str | None) -> tuple[str | None, str | None]:
    reference = explicit_base or "origin/main"
    exists = run_text(
        ["git", "cat-file", "-e", f"{reference}^{{commit}}"],
        cwd=root,
        check=False,
    )
    if exists.returncode != 0:
        return None, f"baseline {reference!r} is unavailable; selecting the full workspace"
    merge_base = run_text(
        ["git", "merge-base", reference, "HEAD"], cwd=root, check=False
    )
    if merge_base.returncode != 0 or not merge_base.stdout.strip():
        return None, f"no merge base with {reference!r}; selecting the full workspace"
    return merge_base.stdout.strip(), None


def collect_changed_files(root: Path, mode: str, base_sha: str) -> list[str]:
    if mode == "committed":
        paths = _git_bytes(
            root,
            ["diff", "--name-only", "-z", "--no-renames", base_sha, "HEAD", "--"],
        )
    elif mode == "worktree":
        paths = _git_bytes(
            root,
            ["diff", "--name-only", "-z", "--no-renames", base_sha, "--"],
        )
        paths.extend(
            _git_bytes(root, ["ls-files", "--others", "--exclude-standard", "-z"])
        )
    else:
        raise WorkflowError(f"unsupported diff mode: {mode}")
    return sorted(set(paths))


def workspace_package_names(root: Path) -> list[str]:
    try:
        workspace = tomllib.loads((root / "Cargo.toml").read_text(encoding="utf-8"))
        names = []
        for member in workspace.get("workspace", {}).get("members", []):
            manifest = tomllib.loads(
                (root / member / "Cargo.toml").read_text(encoding="utf-8")
            )
            names.append(manifest["package"]["name"])
        return sorted(names)
    except (OSError, KeyError, tomllib.TOMLDecodeError) as error:
        raise WorkflowError(f"could not read workspace package names: {error}") from error


def load_metadata(root: Path) -> Mapping[str, Any]:
    result = run_text(
        [
            "cargo",
            "metadata",
            "--no-deps",
            "--locked",
            "--offline",
            "--format-version",
            "1",
        ],
        cwd=root,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise WorkflowError(compact_reason(detail or "cargo metadata failed"))
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise WorkflowError(f"cargo metadata returned invalid JSON: {error}") from error


def select_packages_from_metadata(
    root: Path, changed_files: Sequence[str], metadata: Mapping[str, Any]
) -> tuple[list[str], str | None]:
    packages = list(metadata.get("packages", []))
    workspace_names = {str(package["name"]) for package in packages}
    directories: dict[str, Path] = {}
    for package in packages:
        manifest = Path(str(package["manifest_path"])).resolve()
        directories[str(package["name"])] = manifest.parent

    directly_changed: set[str] = set()
    unknown_code_paths: list[str] = []
    for raw_path in changed_files:
        absolute = (root / raw_path).resolve(strict=False)
        matches = [
            name
            for name, directory in directories.items()
            if absolute == directory or directory in absolute.parents
        ]
        if matches:
            directly_changed.update(matches)
        elif not is_docs_path(raw_path):
            unknown_code_paths.append(raw_path)

    if unknown_code_paths:
        preview = ", ".join(sorted(unknown_code_paths)[:5])
        return sorted(workspace_names), f"unmapped code path(s) ({preview}); selecting full workspace"

    reverse: dict[str, set[str]] = {name: set() for name in workspace_names}
    for package in packages:
        dependent = str(package["name"])
        for dependency in package.get("dependencies", []):
            dependency_name = str(dependency.get("name", ""))
            if dependency_name in workspace_names:
                reverse[dependency_name].add(dependent)

    selected = set(directly_changed)
    pending = list(directly_changed)
    while pending:
        dependency = pending.pop()
        for dependent in reverse.get(dependency, set()):
            if dependent not in selected:
                selected.add(dependent)
                pending.append(dependent)
    return sorted(selected), None


def select_scope(
    root: Path,
    *,
    mode: str = "worktree",
    explicit_base: str | None = None,
    metadata_loader: Callable[[Path], Mapping[str, Any]] = load_metadata,
    docs_only_check: bool = False,
) -> ScopeResult:
    base_sha, fallback = resolve_merge_base(root, explicit_base)
    if base_sha is None:
        return ScopeResult(
            mode=mode,
            base_sha=None,
            changed_files=[],
            selected_packages=workspace_package_names(root),
            full_workspace=True,
            docs_only=False,
            fallback_reason=fallback,
        )

    try:
        changed_files = collect_changed_files(root, mode, base_sha)
    except WorkflowError as error:
        return ScopeResult(
            mode=mode,
            base_sha=base_sha,
            changed_files=[],
            selected_packages=workspace_package_names(root),
            full_workspace=True,
            docs_only=False,
            fallback_reason=f"{compact_reason(str(error))}; selecting full workspace",
        )

    docs_only, full_trigger = classify_paths(changed_files)
    if docs_only_check:
        return ScopeResult(
            mode=mode,
            base_sha=base_sha,
            changed_files=changed_files,
            selected_packages=[],
            full_workspace=full_trigger,
            docs_only=docs_only,
            fallback_reason=None,
        )
    if full_trigger:
        return ScopeResult(
            mode=mode,
            base_sha=base_sha,
            changed_files=changed_files,
            selected_packages=workspace_package_names(root),
            full_workspace=True,
            docs_only=False,
            fallback_reason="workspace workflow/configuration change requires full scope",
        )
    if docs_only or not changed_files:
        return ScopeResult(
            mode=mode,
            base_sha=base_sha,
            changed_files=changed_files,
            selected_packages=[],
            full_workspace=False,
            docs_only=docs_only,
            fallback_reason=None,
        )
    try:
        metadata = metadata_loader(root)
        packages, selection_fallback = select_packages_from_metadata(
            root, changed_files, metadata
        )
    except WorkflowError as error:
        return ScopeResult(
            mode=mode,
            base_sha=base_sha,
            changed_files=changed_files,
            selected_packages=workspace_package_names(root),
            full_workspace=True,
            docs_only=False,
            fallback_reason=f"metadata unavailable ({compact_reason(str(error))}); selecting full workspace",
        )
    return ScopeResult(
        mode=mode,
        base_sha=base_sha,
        changed_files=changed_files,
        selected_packages=packages,
        full_workspace=selection_fallback is not None,
        docs_only=False,
        fallback_reason=selection_fallback,
    )


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("committed", "worktree"), default="worktree")
    parser.add_argument("--base-sha")
    parser.add_argument("--docs-only-check", action="store_true")
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        result = select_scope(
            root,
            mode=args.mode,
            explicit_base=args.base_sha,
            docs_only_check=args.docs_only_check,
        )
    except WorkflowError as error:
        print(f"changed-scope: environment error: {error}", file=sys.stderr)
        return 2

    payload = asdict(result)
    print(json.dumps(payload, sort_keys=True))
    if args.github_output:
        try:
            with args.github_output.open("a", encoding="utf-8") as output:
                output.write(f"docs_only={'true' if result.docs_only else 'false'}\n")
                output.write(f"base_sha={result.base_sha or ''}\n")
        except OSError as error:
            print(f"changed-scope: could not write GitHub output: {error}", file=sys.stderr)
            return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
