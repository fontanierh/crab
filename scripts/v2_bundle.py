#!/usr/bin/env python3
"""Build and verify a locked, self-contained Crab v2 runtime directory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import posixpath
import shutil
import stat
import subprocess
import sys
import tempfile
from pathlib import Path, PurePosixPath
from typing import Any, Sequence


SCHEMA_VERSION = 1
MINIMUM_NODE_MAJOR = 22
CLAUDE_ADAPTER_VERSION = "0.70.0"
RUNTIME_BINARIES = (
    "crab-v2",
    "crab-v2-acp-channel",
    "crab-v2-bridge",
    "crab-v2-bridge-mcp",
    "crab-v2-sub-agent",
    "crab-v2-sub-agent-mcp",
    "crab-v2-trigger",
    "crab-v2-claude-authority-probe",
)
PUBLIC_HELP_BINARIES = RUNTIME_BINARIES[:-1]
MANIFEST_NAME = "bundle-manifest.json"


class BundleError(RuntimeError):
    """A safe bundle build or verification failure."""


def run(
    command: Sequence[str],
    *,
    cwd: Path,
    capture: bool = False,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            cwd=cwd,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
        )
    except OSError as error:
        raise BundleError(f"could not execute {command[0]}: {error}") from error
    if result.returncode != 0:
        detail = ""
        if capture:
            detail = (result.stderr or result.stdout).strip()
        suffix = f": {detail}" if detail else ""
        raise BundleError(
            f"{' '.join(command)} failed with exit {result.returncode}{suffix}"
        )
    return result


def git_text(root: Path, *arguments: str) -> str:
    return run(("git", *arguments), cwd=root, capture=True).stdout.strip()


def repository_root(script: Path | None = None) -> Path:
    start = (script or Path(__file__)).resolve().parent
    return Path(git_text(start, "rev-parse", "--show-toplevel")).resolve()


def source_identity(root: Path, *, allow_dirty: bool) -> dict[str, Any]:
    commit = git_text(root, "rev-parse", "HEAD")
    dirty = bool(git_text(root, "status", "--porcelain", "--untracked-files=all"))
    if dirty and not allow_dirty:
        raise BundleError(
            "source tree is dirty; commit the release or pass --allow-dirty with an explicit "
            "development output"
        )
    repository = git_text(root, "remote", "get-url", "origin")
    return {"repository": repository, "commit": commit, "dirty": dirty}


def platform_slug() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    aliases = {"aarch64": "arm64", "amd64": "x86_64"}
    return f"{system}-{aliases.get(machine, machine)}"


def default_output(root: Path, source: dict[str, Any]) -> Path:
    return root / "v2" / "dist" / (
        f"crab-v2-{str(source['commit'])[:12]}-{platform_slug()}"
    )


def select_output(
    root: Path,
    source: dict[str, Any],
    requested: Path | None,
    *,
    allow_dirty: bool,
) -> Path:
    if source["dirty"] and allow_dirty and requested is None:
        raise BundleError("--allow-dirty requires an explicit --output path")
    output = requested if requested is not None else default_output(root, source)
    return output.expanduser().resolve()


def require_node(root: Path) -> tuple[str, str]:
    node = run(("node", "--version"), cwd=root, capture=True).stdout.strip()
    npm = run(("npm", "--version"), cwd=root, capture=True).stdout.strip()
    try:
        major = int(node.removeprefix("v").split(".", 1)[0])
    except ValueError as error:
        raise BundleError(f"could not parse Node version: {node}") from error
    if major < MINIMUM_NODE_MAJOR:
        raise BundleError(f"Crab v2 runtime bundles require Node {MINIMUM_NODE_MAJOR}+")
    return node, npm


def cargo_target_directory(root: Path) -> Path:
    output = run(
        ("cargo", "metadata", "--locked", "--no-deps", "--format-version", "1"),
        cwd=root / "v2",
        capture=True,
    ).stdout
    try:
        target = json.loads(output)["target_directory"]
    except (json.JSONDecodeError, KeyError, TypeError) as error:
        raise BundleError("cargo metadata did not report a target directory") from error
    return Path(target).resolve()


def copy_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination, follow_symlinks=False)


def install_production_package(source: Path, destination: Path) -> None:
    destination.mkdir(parents=True)
    copy_file(source / "package.json", destination / "package.json")
    copy_file(source / "package-lock.json", destination / "package-lock.json")
    run(
        ("npm", "ci", "--omit=dev", "--no-audit", "--no-fund"),
        cwd=destination,
    )


def build_artifacts(root: Path) -> Path:
    v2 = root / "v2"
    run(
        ("cargo", "build", "--release", "--locked", "-p", "crab-v2-runtime", "--bins"),
        cwd=v2,
    )
    run(
        (
            "cargo",
            "build",
            "--release",
            "--locked",
            "-p",
            "agent-host-implementation",
            "--bin",
            "crab-v2-claude-authority-probe",
        ),
        cwd=v2,
    )
    return cargo_target_directory(root) / "release"


def stage_bundle(root: Path, staging: Path, artifacts: Path) -> None:
    for binary in RUNTIME_BINARIES:
        source = artifacts / binary
        if not source.is_file():
            raise BundleError(f"release build did not produce {binary}")
        copy_file(source, staging / "bin" / binary)

    claude_source = root / "v2" / "runtime" / "agents" / "claude"
    install_production_package(claude_source, staging / "agents" / "claude")

    whatsapp_source = root / "v2" / "bridges" / "whatsapp"
    whatsapp = staging / "bridges" / "whatsapp"
    install_production_package(whatsapp_source, whatsapp)
    shutil.copytree(whatsapp_source / "src", whatsapp / "src", symlinks=True)

    runtime = root / "v2" / "runtime"
    copy_file(
        runtime / "runtime.bundle.example.json",
        staging / "config" / "runtime.bundle.example.json",
    )
    copy_file(
        runtime / "runtime.example.json",
        staging / "config" / "runtime.example.json",
    )
    copy_file(runtime / "BUNDLE_README.md", staging / "README.md")
    copy_file(Path(__file__).resolve(), staging / "libexec" / "v2_bundle.py")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_relative(raw: str) -> PurePosixPath:
    if not isinstance(raw, str):
        raise BundleError("manifest path is not a string")
    path = PurePosixPath(raw)
    if raw in ("", ".") or path.is_absolute() or ".." in path.parts:
        raise BundleError(f"unsafe manifest path: {raw}")
    if path.as_posix() != raw:
        raise BundleError(f"manifest path is not canonical: {raw}")
    return path


def symlink_stays_inside(link_path: PurePosixPath, target: str) -> bool:
    if not isinstance(target, str):
        return False
    target_path = PurePosixPath(target)
    if target_path.is_absolute() or target == "":
        return False
    combined = posixpath.normpath((link_path.parent / target_path).as_posix())
    return combined != ".." and not combined.startswith("../") and combined != "."


def filesystem_entries(root: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        relative = path.relative_to(root).as_posix()
        if relative == MANIFEST_NAME:
            continue
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            target = os.readlink(path)
            if not symlink_stays_inside(PurePosixPath(relative), target):
                raise BundleError(f"symlink escapes bundle: {relative} -> {target}")
            try:
                resolved = path.resolve(strict=True)
                resolved.relative_to(root.resolve())
            except (OSError, ValueError) as error:
                raise BundleError(
                    f"symlink target is missing or escapes bundle: {relative} -> {target}"
                ) from error
            entries.append({"path": relative, "kind": "symlink", "target": target})
        elif stat.S_ISDIR(mode):
            entries.append(
                {
                    "path": relative,
                    "kind": "directory",
                    "mode": stat.S_IMODE(mode),
                }
            )
        elif stat.S_ISREG(mode):
            entries.append(
                {
                    "path": relative,
                    "kind": "file",
                    "sha256": sha256(path),
                    "size": path.stat().st_size,
                    "mode": stat.S_IMODE(mode),
                }
            )
        else:
            raise BundleError(f"special filesystem entry is not allowed: {relative}")
    return entries


def write_manifest(
    root: Path,
    *,
    source: dict[str, Any],
    node: str,
    npm: str,
    rustc: str,
) -> None:
    manifest = {
        "schemaVersion": SCHEMA_VERSION,
        "bundleName": "crab-v2-runtime",
        "source": source,
        "platform": {"system": platform.system(), "machine": platform.machine()},
        "tools": {"rustc": rustc, "node": node, "npm": npm},
        "files": filesystem_entries(root),
    }
    (root / MANIFEST_NAME).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def require_exact_keys(value: Any, keys: set[str], label: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != keys:
        raise BundleError(f"{label} has an invalid schema")
    return value


def parse_manifest(root: Path) -> dict[str, Any]:
    manifest_path = root / MANIFEST_NAME
    try:
        if not stat.S_ISREG(manifest_path.lstat().st_mode):
            raise BundleError("bundle manifest must be a regular file")
        raw = manifest_path.read_text(encoding="utf-8")
        manifest = json.loads(raw)
    except BundleError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise BundleError("bundle manifest is unavailable or invalid") from error
    manifest = require_exact_keys(
        manifest,
        {"schemaVersion", "bundleName", "source", "platform", "tools", "files"},
        "bundle manifest",
    )
    if (
        manifest["schemaVersion"] != SCHEMA_VERSION
        or manifest["bundleName"] != "crab-v2-runtime"
    ):
        raise BundleError("bundle manifest identity is unsupported")
    source = require_exact_keys(
        manifest["source"], {"repository", "commit", "dirty"}, "manifest source"
    )
    if (
        not isinstance(source["repository"], str)
        or not isinstance(source["commit"], str)
        or len(source["commit"]) != 40
        or not isinstance(source["dirty"], bool)
    ):
        raise BundleError("manifest source values are invalid")
    platform_value = require_exact_keys(
        manifest["platform"], {"system", "machine"}, "manifest platform"
    )
    tools = require_exact_keys(
        manifest["tools"], {"rustc", "node", "npm"}, "manifest tools"
    )
    if not all(isinstance(value, str) and value for value in platform_value.values()):
        raise BundleError("manifest platform values are invalid")
    if not all(isinstance(value, str) and value for value in tools.values()):
        raise BundleError("manifest tool values are invalid")
    if not isinstance(manifest["files"], list):
        raise BundleError("manifest files are invalid")
    return manifest


def verify_bundle(root: Path) -> dict[str, Any]:
    root = root.expanduser().resolve()
    if not root.is_dir():
        raise BundleError(f"bundle directory is unavailable: {root}")
    manifest = parse_manifest(root)
    expected: dict[str, dict[str, Any]] = {}
    for entry in manifest["files"]:
        if not isinstance(entry, dict) or entry.get("kind") not in {
            "directory",
            "file",
            "symlink",
        }:
            raise BundleError("manifest contains an invalid file entry")
        kind = entry["kind"]
        keys = {
            "directory": {"path", "kind", "mode"},
            "symlink": {"path", "kind", "target"},
            "file": {"path", "kind", "sha256", "size", "mode"},
        }[kind]
        require_exact_keys(entry, keys, "manifest file entry")
        path = safe_relative(entry["path"])
        if path.as_posix() in expected or path.as_posix() == MANIFEST_NAME:
            raise BundleError(f"duplicate or reserved manifest path: {path}")
        if kind == "symlink" and not symlink_stays_inside(path, entry["target"]):
            raise BundleError(f"manifest symlink escapes bundle: {path}")
        if kind == "file" and (
            not isinstance(entry["sha256"], str)
            or len(entry["sha256"]) != 64
            or not isinstance(entry["size"], int)
            or isinstance(entry["size"], bool)
            or entry["size"] < 0
        ):
            raise BundleError(f"manifest file metadata is invalid: {path}")
        if kind in {"directory", "file"} and (
            not isinstance(entry["mode"], int)
            or isinstance(entry["mode"], bool)
            or not 0 <= entry["mode"] <= 0o7777
        ):
            raise BundleError(f"manifest entry mode is invalid: {path}")
        expected[path.as_posix()] = entry

    actual = {entry["path"]: entry for entry in filesystem_entries(root)}
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    if missing:
        raise BundleError(f"bundle entries are missing: {', '.join(missing[:5])}")
    if extra:
        raise BundleError(f"bundle contains extra entries: {', '.join(extra[:5])}")
    for path, wanted in expected.items():
        observed = actual[path]
        if observed != wanted:
            raise BundleError(f"bundle entry changed: {path}")
    return manifest


def smoke_test(root: Path) -> None:
    for binary in PUBLIC_HELP_BINARIES:
        run((str(root / "bin" / binary), "--help"), cwd=root, capture=True)
    adapter = root / "agents" / "claude" / "node_modules" / ".bin" / "claude-agent-acp"
    version = run((str(adapter), "--version"), cwd=root, capture=True).stdout.strip()
    if version != CLAUDE_ADAPTER_VERSION:
        raise BundleError(f"Claude ACP adapter version is not {CLAUDE_ADAPTER_VERSION}")
    run(
        (
            "node",
            "--input-type=module",
            "-e",
            "await import('./src/baileys-adapter.js')",
        ),
        cwd=root / "bridges" / "whatsapp",
        capture=True,
    )
    config_path = root / "config" / "runtime.bundle.example.json"
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BundleError("bundle runtime config is invalid") from error
    paths = (
        config["agents"][0]["executable"],
        config["agents"][0]["authorityProbe"]["executable"],
        *(server["executable"] for server in config["agents"][0]["sessionMcpServers"]),
        config["bridges"][0]["executable"],
        config["bridges"][0]["workingDirectory"],
    )
    for relative in paths:
        resolved = (config_path.parent / relative).resolve()
        try:
            resolved.relative_to(root)
        except ValueError as error:
            raise BundleError(f"runtime config path escapes bundle: {relative}") from error
        if not resolved.exists():
            raise BundleError(f"runtime config path is unavailable: {relative}")


def build_bundle(root: Path, output: Path | None, *, allow_dirty: bool) -> Path:
    source = source_identity(root, allow_dirty=allow_dirty)
    destination = select_output(root, source, output, allow_dirty=allow_dirty)
    if destination.exists():
        raise BundleError(f"output already exists; refusing to overwrite: {destination}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    node, npm = require_node(root)
    rustc = run(("rustc", "--version"), cwd=root / "v2", capture=True).stdout.strip()
    artifacts = build_artifacts(root)
    staging = Path(
        tempfile.mkdtemp(prefix=f".{destination.name}.staging-", dir=destination.parent)
    )
    try:
        stage_bundle(root, staging, artifacts)
        write_manifest(staging, source=source, node=node, npm=npm, rustc=rustc)
        verify_bundle(staging)
        smoke_test(staging)
        staging.rename(destination)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return destination


def parser() -> argparse.ArgumentParser:
    command = argparse.ArgumentParser(description=__doc__)
    subcommands = command.add_subparsers(dest="command", required=True)
    build = subcommands.add_parser("build", help="build and verify a locked runtime bundle")
    build.add_argument("--output", type=Path)
    build.add_argument(
        "--allow-dirty",
        action="store_true",
        help="development only; requires --output and records dirty provenance",
    )
    verify = subcommands.add_parser("verify", help="verify one existing bundle offline")
    verify.add_argument("bundle", type=Path)
    return command


def main(arguments: Sequence[str] | None = None) -> int:
    options = parser().parse_args(arguments)
    try:
        if options.command == "verify":
            manifest = verify_bundle(options.bundle)
            print(
                "v2-bundle: verified "
                f"{len(manifest['files'])} entries from {manifest['source']['commit'][:12]}"
            )
        else:
            output = build_bundle(
                repository_root(), options.output, allow_dirty=options.allow_dirty
            )
            print(f"v2-bundle: built and verified {output}")
    except BundleError as error:
        print(f"v2-bundle: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
