from __future__ import annotations

import subprocess
from pathlib import Path


def run_git(root: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if result.returncode != 0:
        raise AssertionError(
            f"git {' '.join(arguments)} failed ({result.returncode}): {result.stderr}"
        )
    return result.stdout.strip()


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def init_repo(root: Path, *, packages: tuple[str, ...] = ("alpha",)) -> str:
    root.mkdir(parents=True, exist_ok=True)
    run_git(root, "init", "-b", "main")
    run_git(root, "config", "user.name", "Workflow Tests")
    run_git(root, "config", "user.email", "workflow@example.invalid")
    members = ", ".join(f'"crates/{name}"' for name in packages)
    write(
        root / "Cargo.toml",
        f"[workspace]\nmembers = [{members}]\nresolver = \"2\"\n",
    )
    write(root / "Cargo.lock", "# deterministic fixture\n")
    write(
        root / ".gitignore",
        "/quality/status.json\n/quality/logs/\n/coverage/\ntarget/\n",
    )
    for package in packages:
        write(
            root / "crates" / package / "Cargo.toml",
            f"[package]\nname = \"{package}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        )
        write(
            root / "crates" / package / "src" / "lib.rs",
            f"pub fn {package.replace('-', '_')}() {{}}\n",
        )
    run_git(root, "add", ".")
    run_git(root, "commit", "-m", "fixture base")
    head = run_git(root, "rev-parse", "HEAD")
    run_git(root, "update-ref", "refs/remotes/origin/main", head)
    return head


def file_snapshot(root: Path) -> dict[str, bytes]:
    output: dict[str, bytes] = {}
    result = subprocess.run(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"],
        cwd=root,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    )
    paths = sorted(
        item.decode("utf-8", errors="surrogateescape")
        for item in result.stdout.split(b"\0")
        if item
    )
    for relative in paths:
        path = root / relative
        if not path.is_file():
            continue
        output[relative] = path.read_bytes()
    return output
