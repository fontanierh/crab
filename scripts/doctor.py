#!/usr/bin/env python3
"""Read-only prerequisite and version-skew checks for Crab development."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

sys.dont_write_bytecode = True

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.workflow_common import (
    WorkflowError,
    git_common_dir,
    repository_root,
    validate_ambient_target_dir,
    validate_shared_target_base,
)


LLVM_COV_VERSION = "0.6.21"
JSCPD_VERSION = "4.0.5"


@dataclass(frozen=True)
class Check:
    name: str
    status: str
    detail: str
    remediation: str | None = None


Runner = Callable[[Sequence[str], Path], subprocess.CompletedProcess[str]]


def default_runner(command: Sequence[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(command),
        cwd=cwd,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )


def parse_version(output: str, tool: str) -> str | None:
    patterns = {
        "rustc": r"rustc\s+(\d+\.\d+\.\d+)",
        "cargo-llvm-cov": r"cargo-llvm-cov\s+(\d+\.\d+\.\d+)",
        "python": r"Python\s+(\d+\.\d+\.\d+)",
    }
    match = re.search(patterns[tool], output)
    return match.group(1) if match else None


def pinned_toolchain(root: Path) -> str:
    try:
        payload = tomllib.loads((root / "rust-toolchain.toml").read_text(encoding="utf-8"))
        return str(payload["toolchain"]["channel"])
    except (OSError, KeyError, tomllib.TOMLDecodeError) as error:
        raise WorkflowError(f"could not read rust-toolchain.toml: {error}") from error


def _run_check(
    runner: Runner, root: Path, command: Sequence[str]
) -> tuple[int, str]:
    try:
        result = runner(command, root)
    except OSError as error:
        return 127, str(error)
    output = (result.stdout or result.stderr).strip()
    return result.returncode, output


def collect_checks(
    root: Path,
    *,
    environ: Mapping[str, str] | None = None,
    runner: Runner = default_runner,
    which: Callable[[str], str | None] = shutil.which,
) -> list[Check]:
    environment = dict(os.environ if environ is None else environ)
    checks: list[Check] = []

    for executable in ("git", "python3", "rustup"):
        location = which(executable)
        if location:
            checks.append(Check(executable, "passed", location))
        else:
            remediation = {
                "git": "install Git and retry",
                "python3": "install Python 3.11 or newer and retry",
                "rustup": "install rustup from https://rustup.rs and retry",
            }[executable]
            checks.append(Check(executable, "failed", "not found on PATH", remediation))

    if which("python3"):
        returncode, output = _run_check(runner, root, ["python3", "--version"])
        version = parse_version(output, "python")
        supported = False
        if version:
            major, minor, _ = (int(value) for value in version.split("."))
            supported = (major, minor) >= (3, 11)
        checks.append(
            Check(
                "python-version",
                "passed" if returncode == 0 and supported else "failed",
                output or "could not determine Python version",
                None if supported else "install Python 3.11 or newer and retry",
            )
        )

    pin = pinned_toolchain(root)
    rustup_available = which("rustup") is not None
    toolchain_installed = False
    if rustup_available:
        returncode, output = _run_check(runner, root, ["rustup", "toolchain", "list"])
        toolchain_installed = returncode == 0 and any(
            line.split()[0] == pin or line.split()[0].startswith(f"{pin}-")
            for line in output.splitlines()
            if line.split()
        )
    if toolchain_installed:
        checks.append(Check("rust-toolchain", "passed", f"exact pin {pin} is installed"))
        returncode, output = _run_check(runner, root, ["rustc", f"+{pin}", "-V"])
        version = parse_version(output, "rustc")
        if returncode == 0 and version == pin:
            checks.append(Check("rustc", "passed", output.splitlines()[0]))
        else:
            checks.append(
                Check(
                    "rustc",
                    "failed",
                    output or "could not resolve pinned rustc",
                    f"rustup toolchain install {pin} --component rustfmt --component clippy --component llvm-tools-preview",
                )
            )
        component_code, component_output = _run_check(
            runner,
            root,
            ["rustup", "component", "list", "--toolchain", pin, "--installed"],
        )
        installed_components = set(component_output.splitlines()) if component_code == 0 else set()
        for component in ("rustfmt", "clippy", "llvm-tools"):
            present = any(line.startswith(f"{component}-") for line in installed_components)
            checks.append(
                Check(
                    component,
                    "passed" if present else "failed",
                    "installed for pinned toolchain" if present else f"missing for {pin}",
                    None
                    if present
                    else f"rustup component add {component if component != 'llvm-tools' else 'llvm-tools-preview'} --toolchain {pin}",
                )
            )
        for name, command in (
            ("cargo", ["cargo", f"+{pin}", "-V"]),
            ("clippy", ["cargo", f"+{pin}", "clippy", "-V"]),
            ("rustfmt", ["cargo", f"+{pin}", "fmt", "--version"]),
        ):
            returncode, output = _run_check(runner, root, command)
            if returncode == 0:
                checks.append(Check(f"{name}-resolution", "passed", output.splitlines()[0]))
            else:
                checks.append(
                    Check(
                        f"{name}-resolution",
                        "failed",
                        output or f"could not resolve {name}",
                        f"rustup component add {name} --toolchain {pin}",
                    )
                )
    else:
        remediation = (
            f"rustup toolchain install {pin} --component rustfmt --component clippy "
            "--component llvm-tools-preview"
        )
        checks.append(
            Check(
                "rust-toolchain",
                "failed",
                f"exact pin {pin} is not installed (rustup normally installs it on first Cargo use)",
                remediation,
            )
        )
        for name in ("rustc", "cargo", "rustfmt", "clippy", "llvm-tools"):
            checks.append(Check(name, "info", f"blocked until exact toolchain {pin} is installed"))

    if which("cargo") and which("cargo-llvm-cov"):
        returncode, output = _run_check(
            runner, root, ["cargo-llvm-cov", "llvm-cov", "--version"]
        )
        version = parse_version(output, "cargo-llvm-cov")
        if returncode == 0 and version == LLVM_COV_VERSION:
            checks.append(Check("cargo-llvm-cov", "passed", output.splitlines()[0]))
        else:
            checks.append(
                Check(
                    "cargo-llvm-cov",
                    "failed",
                    f"expected {LLVM_COV_VERSION}, found {version or output or 'unavailable'}",
                    f"cargo install cargo-llvm-cov --version {LLVM_COV_VERSION} --locked --force",
                )
            )
    else:
        checks.append(
            Check(
                "cargo-llvm-cov",
                "failed",
                "not found on PATH",
                f"cargo install cargo-llvm-cov --version {LLVM_COV_VERSION} --locked",
            )
        )

    jscpd_exact = False
    if which("jscpd"):
        returncode, output = _run_check(runner, root, ["jscpd", "--version"])
        found = output.strip().lstrip("v")
        if returncode == 0 and found == JSCPD_VERSION:
            jscpd_exact = True
            checks.append(Check("jscpd", "passed", f"jscpd {found}"))
        else:
            checks.append(
                Check(
                    "jscpd",
                    "failed",
                    f"expected {JSCPD_VERSION}, found {found or 'unavailable'}",
                    f"npm install --global jscpd@{JSCPD_VERSION}",
                )
            )
    else:
        checks.append(
            Check(
                "jscpd",
                "failed",
                "not found on PATH; quality gates never install it implicitly",
                f"npm install --global jscpd@{JSCPD_VERSION}",
            )
        )

    for executable, purpose in (
        ("node", "Node runtime used to install or change jscpd"),
        ("npm", f"npm installer for pinned jscpd@{JSCPD_VERSION}"),
    ):
        location = which(executable)
        if location:
            checks.append(Check(executable, "passed", f"{purpose}: {location}"))
        elif jscpd_exact:
            checks.append(
                Check(
                    executable,
                    "info",
                    f"{purpose}: not found; exact runnable jscpd {JSCPD_VERSION} is already present",
                )
            )
        else:
            checks.append(
                Check(
                    executable,
                    "failed",
                    f"{purpose}: not found on PATH and jscpd must be installed or corrected",
                    f"install Node.js/npm, then run npm install --global jscpd@{JSCPD_VERSION}",
                )
            )

    rg_location = which("rg")
    checks.append(
        Check(
            "rg",
            "passed" if rg_location else "failed",
            f"ripgrep for public API checks: {rg_location}"
            if rg_location
            else "ripgrep for public API checks: not found on PATH",
            None if rg_location else "install rg and retry",
        )
    )

    baseline_reference = (
        environment.get("BASE_SHA")
        or environment.get("BASE_REF")
        or "origin/main"
    )
    baseline_code, baseline_output = _run_check(
        runner,
        root,
        ["git", "cat-file", "-e", f"{baseline_reference}^{{commit}}"],
    )
    if baseline_code == 0:
        merge_code, merge_output = _run_check(
            runner, root, ["git", "merge-base", baseline_reference, "HEAD"]
        )
    else:
        merge_code, merge_output = 1, baseline_output
    if merge_code == 0 and merge_output.strip():
        checks.append(
            Check(
                "patch-baseline",
                "passed",
                f"{baseline_reference} resolves to merge base {merge_output.strip()}",
            )
        )
    else:
        detail = merge_output.strip() or baseline_output.strip() or "reference is unavailable"
        checks.append(
            Check(
                "patch-baseline",
                "failed",
                f"could not resolve patch baseline {baseline_reference!r}: {detail}",
                "git fetch origin main or pass BASE_SHA=<commit>",
            )
        )

    shared = environment.get("CRAB_SHARED_TARGET_DIR")
    if shared is None:
        checks.append(Check("shared-target", "info", "disabled (opt in with CRAB_SHARED_TARGET_DIR)"))
    else:
        try:
            namespaced = validate_shared_target_base(root, shared)
            checks.append(Check("shared-target", "passed", f"validated namespace {namespaced}"))
        except WorkflowError as error:
            checks.append(Check("shared-target", "failed", str(error), "unset or correct CRAB_SHARED_TARGET_DIR"))
    try:
        ambient = validate_ambient_target_dir(root, environment.get("CARGO_TARGET_DIR"))
        checks.append(
            Check(
                "ambient-target",
                "passed" if ambient else "info",
                str(ambient) if ambient else "CARGO_TARGET_DIR is unset",
            )
        )
    except WorkflowError as error:
        checks.append(Check("ambient-target", "failed", str(error), "unset CARGO_TARGET_DIR"))

    try:
        common = git_common_dir(root)
        git_dir_code, git_dir_output = _run_check(runner, root, ["git", "rev-parse", "--git-dir"])
        git_dir = Path(git_dir_output)
        if not git_dir.is_absolute():
            git_dir = root / git_dir
        is_worktree = git_dir_code == 0 and git_dir.resolve() != common
        detail = f"{'linked worktree' if is_worktree else 'ordinary checkout'}; git common dir {common}"
        checks.append(Check("checkout", "info", detail))
    except WorkflowError as error:
        checks.append(Check("checkout", "failed", str(error), "run doctor inside a Git worktree"))
    return checks


def parse_args(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--root", type=Path, help=argparse.SUPPRESS)
    return parser.parse_args(arguments)


def main(arguments: list[str] | None = None) -> int:
    args = parse_args(arguments)
    try:
        root = args.root.resolve() if args.root else repository_root(Path(__file__).parent)
        checks = collect_checks(root)
    except WorkflowError as error:
        print(f"doctor: environment error: {error}", file=sys.stderr)
        return 2
    failures = [check for check in checks if check.status == "failed"]
    if args.json:
        print(json.dumps({"checks": [asdict(check) for check in checks], "healthy": not failures}, indent=2))
    else:
        for check in checks:
            symbol = {"passed": "ok", "failed": "FAIL", "info": "info"}[check.status]
            print(f"doctor: {symbol:4} {check.name}: {check.detail}")
            if check.remediation:
                print(f"         next: {check.remediation}")
        print(f"doctor: {'healthy' if not failures else f'{len(failures)} problem(s) found'}")
    return 0 if not failures else 1


if __name__ == "__main__":
    raise SystemExit(main())
