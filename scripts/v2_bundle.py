#!/usr/bin/env python3
"""Build and verify a locked, self-contained Crab v2 runtime directory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import plistlib
import posixpath
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence


SCHEMA_VERSION = 1
MINIMUM_NODE_MAJOR = 22
CLAUDE_ADAPTER_VERSION = "0.70.0"
CODEX_ADAPTER_VERSION = "1.7.0-crab.2"
CODEX_CLI_VERSION = "0.150.1"
PUBLIC_HELP_BINARIES = (
    "crab-v2",
    "crab-v2-acp-channel",
    "crab-v2-agent",
    "crab-v2-bridge",
    "crab-v2-bridge-mcp",
    "crab-v2-channel",
    "crab-v2-health",
    "crab-v2-sub-agent",
    "crab-v2-sub-agent-mcp",
    "crab-v2-trigger",
)
AUTHORITY_PROBE_BINARIES = (
    "crab-v2-claude-authority-probe",
    "crab-v2-codex-authority-probe",
)
RUNTIME_BINARIES = PUBLIC_HELP_BINARIES + AUTHORITY_PROBE_BINARIES
BUNDLE_AGENT_PRESETS = {
    "claude-opus": "runtime.bundle.example.json",
    "codex": "runtime.bundle.codex.example.json",
}
DEFAULT_BUNDLE_AGENT_PRESET = "claude-opus"
MANIFEST_NAME = "bundle-manifest.json"
SERVICE_SCHEMA_VERSION = 1
HEALTH_SCHEMA_VERSION = 2
SERVICE_LABEL = "com.crab.v2.runtime"
SERVICE_LINKS = ("bin", "agents", "bridges", "libexec")
DEFAULT_READINESS_TIMEOUT_SECONDS = 30.0
STATUS_HEALTH_TIMEOUT_SECONDS = 10.0
MAX_ENVIRONMENT_FILE_BYTES = 1024 * 1024


class BundleError(RuntimeError):
    """A safe bundle build or verification failure."""


@dataclass(frozen=True)
class ServicePaths:
    """One owner-private Crab v2 installation."""

    root: Path
    releases: Path
    current: Path
    config: Path
    state: Path
    logs: Path
    deployment: Path
    lock: Path
    launch_agent: Path


@dataclass(frozen=True)
class LaunchdState:
    """The observable state of the one Crab v2 launchd job."""

    loaded: bool
    running: bool
    pid: int | None


class LaunchdController(Protocol):
    """Small launchd boundary so deployment can be tested without a live service."""

    def inspect(self) -> LaunchdState: ...

    def stop(self) -> None: ...

    def start(self, launch_agent: Path) -> None: ...


def run(
    command: Sequence[str],
    *,
    cwd: Path,
    capture: bool = False,
    timeout_seconds: float | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            cwd=cwd,
            check=False,
            text=True,
            stdout=subprocess.PIPE if capture else None,
            stderr=subprocess.PIPE if capture else None,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        raise BundleError(
            f"{Path(command[0]).name} timed out after {timeout_seconds:g} seconds"
        ) from error
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
            "--bin",
            "crab-v2-codex-authority-probe",
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
    codex_source = root / "v2" / "runtime" / "agents" / "codex"
    install_production_package(codex_source, staging / "agents" / "codex")

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
        runtime / "runtime.bundle.codex.example.json",
        staging / "config" / "runtime.bundle.codex.example.json",
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
        or not re.fullmatch(r"[0-9a-f]{40}", source["commit"])
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
    if not all(
        re.fullmatch(r"[A-Za-z0-9_.-]+", value) for value in platform_value.values()
    ):
        raise BundleError("manifest platform values are unsafe")
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
    adapters = (
        (
            "Claude",
            root
            / "agents"
            / "claude"
            / "node_modules"
            / ".bin"
            / "claude-agent-acp",
            CLAUDE_ADAPTER_VERSION,
        ),
        (
            "Codex",
            root / "agents" / "codex" / "node_modules" / ".bin" / "codex-acp",
            f"@agentclientprotocol/codex-acp {CODEX_ADAPTER_VERSION}",
        ),
    )
    for name, adapter, expected_version in adapters:
        version = run((str(adapter), "--version"), cwd=root, capture=True).stdout.strip()
        if version != expected_version:
            raise BundleError(f"{name} ACP adapter version is not {expected_version}")
    codex_version = run(
        (
            str(root / "agents" / "codex" / "node_modules" / ".bin" / "codex"),
            "--version",
        ),
        cwd=root,
        capture=True,
    ).stdout.strip()
    expected_codex_version = f"codex-cli {CODEX_CLI_VERSION}"
    if codex_version != expected_codex_version:
        raise BundleError(f"Codex CLI version is not {expected_codex_version}")
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
    for filename in BUNDLE_AGENT_PRESETS.values():
        smoke_test_config(root, root / "config" / filename)


def smoke_test_config(root: Path, config_path: Path) -> None:
    try:
        config = json.loads(config_path.read_text(encoding="utf-8"))
        agents = config["agents"]
        bridges = config["bridges"]
        paths = [
            *(agent["executable"] for agent in agents),
            *(agent["authorityProbe"]["executable"] for agent in agents),
            *(
                server["executable"]
                for agent in agents
                for server in agent["sessionMcpServers"]
            ),
            *(bridge["executable"] for bridge in bridges),
            *(bridge["workingDirectory"] for bridge in bridges),
        ]
    except (KeyError, OSError, TypeError, json.JSONDecodeError) as error:
        raise BundleError(f"bundle runtime config is invalid: {config_path.name}") from error
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


class SystemLaunchd:
    """The fixed per-user launchd service used by Crab v2."""

    def __init__(self, *, uid: int | None = None, label: str = SERVICE_LABEL) -> None:
        self.uid = os.getuid() if uid is None else uid
        self.label = label
        self.target = f"gui/{self.uid}/{self.label}"
        self.domain = f"gui/{self.uid}"

    def inspect(self) -> LaunchdState:
        try:
            result = subprocess.run(
                ("launchctl", "print", self.target),
                check=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as error:
            raise BundleError(f"could not inspect {self.label}: {error}") from error
        if result.returncode != 0:
            detail = f"{result.stdout}\n{result.stderr}".lower()
            if "could not find service" in detail or "service not found" in detail:
                return LaunchdState(loaded=False, running=False, pid=None)
            raise BundleError(f"could not inspect {self.label}: launchctl failed")
        state: str | None = None
        pid: int | None = None
        for line in result.stdout.splitlines():
            key, separator, value = line.strip().partition(" = ")
            if not separator:
                continue
            if key == "state":
                state = value
            elif key == "pid":
                try:
                    pid = int(value)
                except ValueError:
                    pid = None
        return LaunchdState(loaded=True, running=state == "running", pid=pid)

    def stop(self) -> None:
        if not self.inspect().loaded:
            return
        run(("launchctl", "bootout", self.target), cwd=Path.home(), capture=True)

    def start(self, launch_agent: Path) -> None:
        run(
            ("launchctl", "bootstrap", self.domain, str(launch_agent)),
            cwd=Path.home(),
            capture=True,
        )


def require_macos() -> None:
    if platform.system() != "Darwin":
        raise BundleError("Crab v2 launchd deployment requires macOS")


def service_paths(root: Path, *, launch_agents: Path | None = None) -> ServicePaths:
    raw = root.expanduser()
    if not raw.is_absolute():
        raise BundleError("service root must be an absolute path")
    if raw.exists() and raw.is_symlink():
        raise BundleError("service root must not be a symlink")
    resolved = raw.resolve()
    if resolved in {Path("/"), Path.home().resolve()}:
        raise BundleError("service root is too broad")
    launch_directory = (
        launch_agents.expanduser().resolve()
        if launch_agents is not None
        else Path.home() / "Library" / "LaunchAgents"
    )
    return ServicePaths(
        root=resolved,
        releases=resolved / "releases",
        current=resolved / "current",
        config=resolved / "config" / "runtime.json",
        state=resolved / "state",
        logs=resolved / "logs",
        deployment=resolved / "deployment.json",
        lock=resolved / "deploy.lock",
        launch_agent=launch_directory / f"{SERVICE_LABEL}.plist",
    )


def require_owned_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    details = path.lstat()
    if not stat.S_ISDIR(details.st_mode) or stat.S_ISLNK(details.st_mode):
        raise BundleError(f"service path is not a real directory: {path}")
    if details.st_uid != os.getuid():
        raise BundleError(f"service path is not owned by the current user: {path}")
    if stat.S_IMODE(details.st_mode) & 0o077:
        path.chmod(0o700)


def prepare_service_directories(paths: ServicePaths) -> None:
    for path in (
        paths.root,
        paths.releases,
        paths.config.parent,
        paths.state,
        paths.logs,
    ):
        require_owned_directory(path)
    paths.launch_agent.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def deployment_lock(path: Path) -> Iterator[None]:
    try:
        import fcntl
    except ImportError as error:
        raise BundleError("Crab v2 deployment locking requires macOS") from error
    try:
        descriptor = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    except OSError as error:
        raise BundleError(f"could not open deployment lock: {error}") from error
    try:
        os.fchmod(descriptor, 0o600)
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BundleError("another Crab v2 deployment is in progress") from error
        yield
    finally:
        os.close(descriptor)


def atomic_bytes(path: Path, content: bytes, *, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_raw = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_raw)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        temporary.unlink(missing_ok=True)
        raise


def atomic_json(path: Path, value: Any, *, mode: int = 0o600) -> None:
    atomic_bytes(
        path,
        (json.dumps(value, indent=2, sort_keys=True) + "\n").encode(),
        mode=mode,
    )


def atomic_symlink(path: Path, target: str) -> None:
    temporary = path.parent / f".{path.name}.{os.getpid()}.tmp"
    temporary.unlink(missing_ok=True)
    try:
        os.symlink(target, temporary)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def snapshot_regular_file(path: Path, label: str) -> tuple[bytes, int] | None:
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
        raise BundleError(f"{label} must be a regular file")
    try:
        return path.read_bytes(), stat.S_IMODE(path.stat().st_mode)
    except OSError as error:
        raise BundleError(f"could not snapshot {label}") from error


def restore_snapshot(path: Path, snapshot: tuple[bytes, int] | None) -> None:
    if snapshot is None:
        path.unlink(missing_ok=True)
    else:
        atomic_bytes(path, snapshot[0], mode=snapshot[1])


def host_matches_manifest(manifest: Mapping[str, Any]) -> bool:
    wanted = manifest["platform"]
    aliases = {"aarch64": "arm64", "amd64": "x86_64"}
    host_machine = aliases.get(platform.machine().lower(), platform.machine().lower())
    bundle_machine = aliases.get(str(wanted["machine"]).lower(), str(wanted["machine"]).lower())
    return str(wanted["system"]).lower() == platform.system().lower() and (
        bundle_machine == host_machine
    )


def release_name(manifest: Mapping[str, Any]) -> str:
    source = manifest["source"]
    wanted = manifest["platform"]
    system = str(wanted["system"]).lower()
    machine = str(wanted["machine"]).lower()
    aliases = {"aarch64": "arm64", "amd64": "x86_64"}
    return f"{source['commit']}-{system}-{aliases.get(machine, machine)}"


def install_release(bundle: Path, paths: ServicePaths) -> tuple[Path, dict[str, Any]]:
    bundle = bundle.expanduser().resolve()
    manifest = verify_bundle(bundle)
    if manifest["source"]["dirty"]:
        raise BundleError("dirty development bundles cannot be deployed")
    if not host_matches_manifest(manifest):
        raise BundleError("bundle platform does not match this host")
    release = paths.releases / release_name(manifest)
    if release.exists() or release.is_symlink():
        if release.is_symlink():
            raise BundleError(f"installed release must not be a symlink: {release}")
        existing = verify_bundle(release)
        if existing != manifest:
            raise BundleError(f"installed release does not match its bundle: {release}")
        return release, manifest
    staging = Path(tempfile.mkdtemp(prefix=".release-", dir=paths.releases))
    try:
        shutil.rmtree(staging)
        shutil.copytree(bundle, staging, symlinks=True)
        copied = verify_bundle(staging)
        if copied != manifest:
            raise BundleError("copied release manifest changed")
        staging.rename(release)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    return release, manifest


def load_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
            raise BundleError(f"{label} must be a regular file")
        value = json.loads(path.read_text(encoding="utf-8"))
    except BundleError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise BundleError(f"{label} is unavailable or invalid") from error
    if not isinstance(value, dict):
        raise BundleError(f"{label} must contain a JSON object")
    return value


def materialize_config(
    release: Path,
    paths: ServicePaths,
    workspace: Path | None,
    agent_preset: str | None,
) -> bool:
    if paths.config.exists() or paths.config.is_symlink():
        if agent_preset is not None:
            raise BundleError("--agent is only valid for the first deployment")
        config = load_json_object(paths.config, "runtime config")
        if workspace is not None:
            wanted = workspace.expanduser().resolve()
            configured = {
                Path(channel["workingDirectory"]).expanduser().resolve()
                for channel in config.get("channels", [])
                if isinstance(channel, dict)
                and isinstance(channel.get("workingDirectory"), str)
            }
            if configured != {wanted}:
                raise BundleError("--workspace does not match the existing runtime config")
        return False
    if workspace is None:
        raise BundleError("first deployment requires --workspace")
    workspace = workspace.expanduser().resolve()
    if not workspace.is_dir():
        raise BundleError(f"workspace is not an existing directory: {workspace}")
    selected_preset = agent_preset or DEFAULT_BUNDLE_AGENT_PRESET
    try:
        preset_filename = BUNDLE_AGENT_PRESETS[selected_preset]
    except KeyError as error:
        raise BundleError(f"unknown bundled agent preset: {selected_preset}") from error
    example = load_json_object(
        release / "config" / preset_filename,
        f"bundle {selected_preset} runtime config",
    )
    channels = example.get("channels")
    if not isinstance(channels, list) or not channels:
        raise BundleError("bundle runtime config does not define a channel")
    replaced = 0
    for channel in channels:
        if not isinstance(channel, dict):
            raise BundleError("bundle runtime config contains an invalid channel")
        if channel.get("workingDirectory") == "/absolute/path/to/agent-workspace":
            channel["workingDirectory"] = str(workspace)
            replaced += 1
    if replaced == 0:
        raise BundleError("bundle runtime config has no workspace placeholder")
    atomic_json(paths.config, example)
    return True


def environment_names(config: Mapping[str, Any]) -> set[str]:
    names: set[str] = set()

    def add(value: Any, label: str) -> None:
        if value is None:
            return
        if not isinstance(value, list) or not all(
            isinstance(item, str)
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item)
            for item in value
        ):
            raise BundleError(f"{label} environmentFrom is invalid")
        names.update(value)

    agents = config.get("agents")
    bridges = config.get("bridges", [])
    if not isinstance(agents, list) or not agents or not isinstance(bridges, list):
        raise BundleError("runtime config agents or bridges are invalid")
    for index, agent in enumerate(agents):
        if not isinstance(agent, dict):
            raise BundleError("runtime config contains an invalid agent")
        add(agent.get("environmentFrom"), f"agent {index}")
        probe = agent.get("authorityProbe")
        if isinstance(probe, dict):
            add(probe.get("environmentFrom"), f"agent {index} authority probe")
        servers = agent.get("sessionMcpServers", [])
        if not isinstance(servers, list):
            raise BundleError(f"agent {index} sessionMcpServers is invalid")
        for server_index, server in enumerate(servers):
            if not isinstance(server, dict):
                raise BundleError(f"agent {index} has an invalid MCP server")
            add(
                server.get("environmentFrom"),
                f"agent {index} MCP server {server_index}",
            )
    for index, bridge in enumerate(bridges):
        if not isinstance(bridge, dict):
            raise BundleError("runtime config contains an invalid bridge")
        add(bridge.get("environmentFrom"), f"bridge {index}")
    return names


def load_launch_agent(path: Path) -> dict[str, Any]:
    if not path.exists() and not path.is_symlink():
        raise BundleError("Crab v2 launch agent is unavailable")
    if path.is_symlink():
        raise BundleError("existing Crab v2 launch agent must not be a symlink")
    try:
        with path.open("rb") as handle:
            plist = plistlib.load(handle)
    except (OSError, plistlib.InvalidFileException) as error:
        raise BundleError("existing Crab v2 launch agent is invalid") from error
    if not isinstance(plist, dict):
        raise BundleError("existing Crab v2 launch agent is invalid")
    return plist


def existing_launch_environment(path: Path) -> dict[str, str]:
    if not path.exists() and not path.is_symlink():
        return {}
    plist = load_launch_agent(path)
    environment = plist.get("EnvironmentVariables", {})
    if not isinstance(environment, dict) or not all(
        isinstance(key, str) and isinstance(value, str)
        for key, value in environment.items()
    ):
        raise BundleError("existing Crab v2 launch environment is invalid")
    return environment


def deployment_environment(
    names: set[str],
    current: Mapping[str, str],
    previous: Mapping[str, str],
) -> dict[str, str]:
    environment: dict[str, str] = {}
    missing: list[str] = []
    for name in sorted(names):
        if name in current:
            environment[name] = current[name]
        elif name in previous:
            environment[name] = previous[name]
        else:
            missing.append(name)
    if missing:
        raise BundleError(
            "required runtime environment is unavailable: " + ", ".join(missing)
        )
    return environment


def load_environment_file(path: Path) -> dict[str, str]:
    """Read one owner-only dotenv file without evaluating shell syntax."""
    path = path.expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise BundleError("environment file is unavailable") from error
    try:
        details = os.fstat(descriptor)
        if (
            not stat.S_ISREG(details.st_mode)
            or details.st_uid != os.getuid()
            or stat.S_IMODE(details.st_mode) & 0o077
            or details.st_size > MAX_ENVIRONMENT_FILE_BYTES
        ):
            raise BundleError("environment file must be an owner-only regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(MAX_ENVIRONMENT_FILE_BYTES + 1)
    finally:
        os.close(descriptor)
    if len(raw) > MAX_ENVIRONMENT_FILE_BYTES:
        raise BundleError("environment file is too large")
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as error:
        raise BundleError("environment file must be UTF-8") from error
    environment: dict[str, str] = {}
    for line_number, line in enumerate(text.splitlines(), start=1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        name, separator, value = line.partition("=")
        name = name.strip()
        value = value.strip()
        if (
            not separator
            or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name)
            or name in environment
        ):
            raise BundleError(f"environment file line {line_number} is invalid")
        if value.startswith('"'):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError as error:
                raise BundleError(
                    f"environment file line {line_number} is invalid"
                ) from error
            if not isinstance(decoded, str):
                raise BundleError(f"environment file line {line_number} is invalid")
            value = decoded
        elif value.startswith("'"):
            if len(value) < 2 or not value.endswith("'") or "'" in value[1:-1]:
                raise BundleError(f"environment file line {line_number} is invalid")
            value = value[1:-1]
        elif any(character in value for character in "'\"`\\$"):
            raise BundleError(f"environment file line {line_number} is invalid")
        if "\0" in value:
            raise BundleError(f"environment file line {line_number} is invalid")
        environment[name] = value
    return environment


def merged_deployment_environment(
    ambient: Mapping[str, str], environment_file: Path | None
) -> dict[str, str]:
    environment = dict(ambient)
    if environment_file is not None:
        environment.update(load_environment_file(environment_file))
    return environment


def require_runtime_node(environment: Mapping[str, str]) -> None:
    path = environment.get("PATH")
    if path is None:
        raise BundleError("runtime config must import PATH for the bundled Node agent")
    executable = shutil.which("node", path=path)
    if executable is None:
        raise BundleError("runtime PATH does not provide Node.js")
    try:
        result = subprocess.run(
            (executable, "--version"),
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"PATH": path},
        )
    except OSError as error:
        raise BundleError(f"could not inspect runtime Node.js: {error}") from error
    version = result.stdout.strip()
    try:
        major = int(version.removeprefix("v").split(".", 1)[0])
    except ValueError as error:
        raise BundleError("runtime Node.js version is invalid") from error
    if result.returncode != 0 or major < MINIMUM_NODE_MAJOR:
        raise BundleError(f"runtime requires Node.js {MINIMUM_NODE_MAJOR}+")


def launch_agent_payload(
    paths: ServicePaths, environment: Mapping[str, str]
) -> dict[str, Any]:
    return {
        "Label": SERVICE_LABEL,
        "ProgramArguments": [
            str(paths.root / "bin" / "crab-v2"),
            "--config",
            str(paths.config),
            "--state-dir",
            str(paths.state),
        ],
        "WorkingDirectory": str(paths.root),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "StandardOutPath": str(paths.logs / "runtime.stdout.log"),
        "StandardErrorPath": str(paths.logs / "runtime.stderr.log"),
        "EnvironmentVariables": dict(sorted(environment.items())),
    }


def write_launch_agent(paths: ServicePaths, environment: Mapping[str, str]) -> None:
    content = plistlib.dumps(
        launch_agent_payload(paths, environment),
        fmt=plistlib.FMT_XML,
        sort_keys=True,
    )
    atomic_bytes(paths.launch_agent, content, mode=0o600)


def current_target(paths: ServicePaths) -> str | None:
    if paths.current.is_symlink():
        target = os.readlink(paths.current)
        relative = PurePosixPath(target)
        if (
            relative.is_absolute()
            or len(relative.parts) != 2
            or relative.parts[0] != "releases"
            or relative.as_posix() != target
        ):
            raise BundleError("current release symlink is not canonical")
        resolved = (paths.current.parent / target).resolve()
        try:
            resolved.relative_to(paths.releases)
        except ValueError as error:
            raise BundleError("current release symlink escapes the release directory") from error
        return target
    if paths.current.exists():
        raise BundleError("current release path must be a symlink")
    return None


def runtime_processes() -> list[int]:
    output = run(("ps", "-axo", "pid=,comm="), cwd=Path.home(), capture=True).stdout
    processes: list[int] = []
    for line in output.splitlines():
        pid_raw, separator, command = line.strip().partition(" ")
        if not separator or Path(command.strip()).name != "crab-v2":
            continue
        try:
            processes.append(int(pid_raw))
        except ValueError:
            continue
    return sorted(processes)


def ensure_stable_links(paths: ServicePaths) -> list[Path]:
    created: list[Path] = []
    try:
        for name in SERVICE_LINKS:
            path = paths.root / name
            wanted = f"current/{name}"
            if path.is_symlink() and os.readlink(path) == wanted:
                continue
            if path.exists() or path.is_symlink():
                raise BundleError(f"stable service path is not managed by Crab: {path}")
            atomic_symlink(path, wanted)
            created.append(path)
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise
    return created


def parse_runtime_health(raw: str) -> dict[str, Any]:
    try:
        report = json.loads(raw)
    except json.JSONDecodeError as error:
        raise BundleError("runtime health output is invalid") from error
    report = require_exact_keys(
        report,
        {
            "schemaVersion",
            "observedAtMs",
            "ready",
            "healthy",
            "runtime",
            "channels",
            "bridges",
            "errors",
            "needsAction",
        },
        "runtime health output",
    )
    runtime = require_exact_keys(
        report["runtime"],
        {
            "expectedConfigurationFingerprint",
            "loadedConfigurationFingerprint",
            "startedAtMs",
            "processId",
            "ready",
            "error",
        },
        "runtime health attestation",
    )
    expected_fingerprint = runtime["expectedConfigurationFingerprint"]
    loaded_fingerprint = runtime["loadedConfigurationFingerprint"]
    valid_expected_fingerprint = isinstance(expected_fingerprint, str) and bool(
        re.fullmatch(r"[0-9a-f]{64}", expected_fingerprint)
    )
    valid_loaded_fingerprint = loaded_fingerprint is None or (
        isinstance(loaded_fingerprint, str)
        and bool(re.fullmatch(r"[0-9a-f]{64}", loaded_fingerprint))
    )
    if (
        report["schemaVersion"] != HEALTH_SCHEMA_VERSION
        or isinstance(report["observedAtMs"], bool)
        or not isinstance(report["observedAtMs"], int)
        or report["observedAtMs"] < 0
        or not isinstance(report["ready"], bool)
        or not isinstance(report["healthy"], bool)
        or not valid_expected_fingerprint
        or not valid_loaded_fingerprint
        or isinstance(runtime["startedAtMs"], bool)
        or not isinstance(runtime["startedAtMs"], int)
        or runtime["startedAtMs"] < 0
        or isinstance(runtime["processId"], bool)
        or not isinstance(runtime["processId"], int)
        or runtime["processId"] <= 0
        or not isinstance(runtime["ready"], bool)
        or (runtime["error"] is not None and not isinstance(runtime["error"], str))
        or runtime["ready"] != (runtime["error"] is None)
        or runtime["ready"] != (loaded_fingerprint == expected_fingerprint)
        or not isinstance(report["channels"], list)
        or not all(isinstance(channel, dict) for channel in report["channels"])
        or not isinstance(report["bridges"], list)
        or not all(isinstance(bridge, dict) for bridge in report["bridges"])
        or not isinstance(report["errors"], list)
        or not all(isinstance(error, str) for error in report["errors"])
        or not isinstance(report["needsAction"], list)
        or not all(isinstance(action, str) for action in report["needsAction"])
        or (report["healthy"] and not report["ready"])
        or (report["ready"] and not runtime["ready"])
    ):
        raise BundleError("runtime health output has invalid values")
    return report


def production_runtime_health(
    paths: ServicePaths, timeout_seconds: float
) -> dict[str, Any]:
    result = run(
        (
            str(paths.root / "bin" / "crab-v2-health"),
            "--config",
            str(paths.config),
            "--state-dir",
            str(paths.state),
        ),
        cwd=paths.root,
        capture=True,
        timeout_seconds=timeout_seconds,
    )
    return parse_runtime_health(result.stdout)


RuntimeHealthProbe = Callable[[ServicePaths, float], dict[str, Any]]


def production_readiness(
    paths: ServicePaths,
    launchd: LaunchdController,
    *,
    timeout_seconds: float,
    health: RuntimeHealthProbe = production_runtime_health,
) -> int:
    deadline = time.monotonic() + timeout_seconds
    last_error = "runtime did not start"
    while True:
        state = launchd.inspect()
        if state.running and state.pid is not None:
            try:
                processes = runtime_processes()
                if processes != [state.pid]:
                    raise BundleError(
                        "expected one launchd-owned crab-v2 process, found "
                        + ", ".join(str(pid) for pid in processes)
                    )
                verify_bundle(paths.current)
                remaining = max(0.0, deadline - time.monotonic())
                topology = health(paths, remaining)
                if topology["runtime"]["processId"] != state.pid:
                    raise BundleError(
                        "runtime health attestation does not match launchd process"
                    )
                if not topology["ready"]:
                    detail = (
                        "; ".join(topology["errors"][:3])
                        or "configured topology is not ready"
                    )
                    raise BundleError(detail)
                return state.pid
            except BundleError as error:
                last_error = str(error)
        elif state.loaded:
            last_error = "launchd job is loaded but not running"
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise BundleError(f"runtime readiness failed: {last_error}")
        time.sleep(min(0.2, remaining))


ReadinessProbe = Callable[[ServicePaths, LaunchdController, float], int]


def deploy_service(
    bundle: Path,
    root: Path,
    *,
    workspace: Path | None,
    launchd: LaunchdController,
    agent_preset: str | None = None,
    launch_agents: Path | None = None,
    environ: Mapping[str, str] = os.environ,
    timeout_seconds: float = DEFAULT_READINESS_TIMEOUT_SECONDS,
    readiness: ReadinessProbe = lambda paths, launchd, timeout: production_readiness(
        paths, launchd, timeout_seconds=timeout
    ),
) -> dict[str, Any]:
    paths = service_paths(root, launch_agents=launch_agents)
    prepare_service_directories(paths)
    with deployment_lock(paths.lock):
        release, manifest = install_release(bundle, paths)
        old_target = current_target(paths)
        if old_target is not None:
            verify_bundle(paths.current)
        old_plist = snapshot_regular_file(paths.launch_agent, "Crab v2 launch agent")
        old_deployment = snapshot_regular_file(
            paths.deployment, "Crab v2 deployment record"
        )
        was_loaded = launchd.inspect().loaded
        if was_loaded and (old_target is None or old_plist is None):
            raise BundleError(
                "loaded Crab v2 launchd job has no complete managed deployment"
            )
        created_config = False
        created_links: list[Path] = []
        try:
            created_config = materialize_config(
                release, paths, workspace, agent_preset
            )
            config = load_json_object(paths.config, "runtime config")
            environment = deployment_environment(
                environment_names(config),
                environ,
                existing_launch_environment(paths.launch_agent),
            )
            require_runtime_node(environment)
            created_links = ensure_stable_links(paths)
        except BaseException:
            if created_config:
                paths.config.unlink(missing_ok=True)
            raise
        new_target = release.relative_to(paths.root).as_posix()
        try:
            launchd.stop()
            atomic_symlink(paths.current, new_target)
            write_launch_agent(paths, environment)
            launchd.start(paths.launch_agent)
            pid = readiness(paths, launchd, timeout_seconds)
            result = {
                "schemaVersion": SERVICE_SCHEMA_VERSION,
                "label": SERVICE_LABEL,
                "release": new_target,
                "sourceCommit": manifest["source"]["commit"],
                "pid": pid,
            }
            atomic_json(paths.deployment, result)
        except BaseException as deployment_error:
            rollback_error: BaseException | None = None
            try:
                launchd.stop()
                if old_target is None:
                    paths.current.unlink(missing_ok=True)
                else:
                    atomic_symlink(paths.current, old_target)
                restore_snapshot(paths.launch_agent, old_plist)
                restore_snapshot(paths.deployment, old_deployment)
                if created_config:
                    paths.config.unlink(missing_ok=True)
                if old_target is None:
                    for path in created_links:
                        path.unlink(missing_ok=True)
                if was_loaded and old_target is not None and old_plist is not None:
                    launchd.start(paths.launch_agent)
                    readiness(paths, launchd, timeout_seconds)
            except BaseException as error:
                rollback_error = error
            if rollback_error is not None:
                raise BundleError(
                    f"deployment failed ({deployment_error}); rollback failed ({rollback_error})"
                ) from deployment_error
            if isinstance(deployment_error, BundleError):
                raise deployment_error
            raise BundleError(f"deployment failed: {deployment_error}") from deployment_error
        return result


def service_status(
    root: Path,
    *,
    launchd: LaunchdController,
    launch_agents: Path | None = None,
    processes: Callable[[], list[int]] = runtime_processes,
    health: RuntimeHealthProbe = production_runtime_health,
) -> dict[str, Any]:
    paths = service_paths(root, launch_agents=launch_agents)
    errors: list[str] = []
    target: str | None = None
    commit: str | None = None
    bundle_verified = False
    try:
        target = current_target(paths)
        if target is None:
            raise BundleError("no current release")
        manifest = verify_bundle(paths.current)
        commit = manifest["source"]["commit"]
        bundle_verified = True
        for name in SERVICE_LINKS:
            path = paths.root / name
            if not path.is_symlink() or os.readlink(path) != f"current/{name}":
                raise BundleError(f"stable {name} link is invalid")
        launch_agent = load_launch_agent(paths.launch_agent)
        environment = existing_launch_environment(paths.launch_agent)
        config = load_json_object(paths.config, "runtime config")
        if set(environment) != environment_names(config):
            raise BundleError("launch environment does not match the runtime config")
        if launch_agent != launch_agent_payload(paths, environment):
            raise BundleError("Crab v2 launch agent does not match the managed service")
        deployment = require_exact_keys(
            load_json_object(paths.deployment, "deployment record"),
            {"schemaVersion", "label", "release", "sourceCommit", "pid"},
            "deployment record",
        )
        if (
            deployment["schemaVersion"] != SERVICE_SCHEMA_VERSION
            or deployment["label"] != SERVICE_LABEL
            or deployment["release"] != target
            or deployment["sourceCommit"] != commit
        ):
            raise BundleError("deployment record does not match the current release")
    except (BundleError, OSError) as error:
        errors.append(str(error))
    state = launchd.inspect()
    process_ids: list[int] = []
    try:
        process_ids = processes()
        if state.running and state.pid is not None and process_ids != [state.pid]:
            raise BundleError("runtime process set does not match the launchd-owned pid")
    except BundleError as error:
        errors.append(str(error))
    ipc_ready = False
    topology: dict[str, Any] | None = None
    runtime_pid_matches = False
    if state.running and state.pid is not None and bundle_verified:
        try:
            topology = health(paths, STATUS_HEALTH_TIMEOUT_SECONDS)
            ipc_ready = True
            runtime_pid_matches = topology["runtime"]["processId"] == state.pid
            if not runtime_pid_matches:
                errors.append("runtime health attestation does not match launchd process")
            if not topology["ready"]:
                errors.append("configured runtime topology is not ready")
        except BundleError as error:
            errors.append(str(error))
    elif not state.running:
        errors.append("launchd job is not running")
    return {
        "schemaVersion": SERVICE_SCHEMA_VERSION,
        "label": SERVICE_LABEL,
        "root": str(paths.root),
        "release": target,
        "sourceCommit": commit,
        "bundleVerified": bundle_verified,
        "launchdLoaded": state.loaded,
        "launchdRunning": state.running,
        "pid": state.pid,
        "processCount": len(process_ids),
        "ipcReady": ipc_ready,
        "topologyReady": topology is not None
        and topology["ready"]
        and runtime_pid_matches,
        "topologyHealthy": topology is not None
        and topology["healthy"]
        and runtime_pid_matches,
        "needsAction": topology["needsAction"] if topology is not None else [],
        "topology": topology,
        "healthy": not errors and ipc_ready and topology is not None and topology["healthy"],
        "errors": errors,
    }


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
    deploy = subcommands.add_parser(
        "deploy", help="atomically install, supervise, and verify a runtime bundle"
    )
    deploy.add_argument("bundle", type=Path)
    deploy.add_argument(
        "--root", type=Path, default=Path.home() / ".crab-v2", help="service root"
    )
    deploy.add_argument(
        "--workspace",
        type=Path,
        help="agent workspace; required only for the first deployment",
    )
    deploy.add_argument(
        "--agent",
        choices=tuple(BUNDLE_AGENT_PRESETS),
        help=(
            "bundled agent preset for the first deployment; "
            f"defaults to {DEFAULT_BUNDLE_AGENT_PRESET}"
        ),
    )
    deploy.add_argument(
        "--environment-file",
        type=Path,
        help="owner-only dotenv file; only config-declared names are captured",
    )
    deploy.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_READINESS_TIMEOUT_SECONDS,
        help="readiness and rollback timeout in seconds",
    )
    status = subcommands.add_parser(
        "status", help="verify the installed release, launchd job, and local IPC"
    )
    status.add_argument(
        "--root", type=Path, default=Path.home() / ".crab-v2", help="service root"
    )
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
        elif options.command == "build":
            output = build_bundle(
                repository_root(), options.output, allow_dirty=options.allow_dirty
            )
            print(f"v2-bundle: built and verified {output}")
        elif options.command == "deploy":
            require_macos()
            if options.timeout <= 0:
                raise BundleError("--timeout must be positive")
            result = deploy_service(
                options.bundle,
                options.root,
                workspace=options.workspace,
                launchd=SystemLaunchd(),
                agent_preset=options.agent,
                environ=merged_deployment_environment(
                    os.environ, options.environment_file
                ),
                timeout_seconds=options.timeout,
            )
            print(
                "v2-bundle: deployed "
                f"{result['sourceCommit'][:12]} as pid {result['pid']}"
            )
        else:
            require_macos()
            result = service_status(options.root, launchd=SystemLaunchd())
            print(json.dumps(result, indent=2, sort_keys=True))
            if not result["healthy"]:
                return 1
    except BundleError as error:
        print(f"v2-bundle: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
