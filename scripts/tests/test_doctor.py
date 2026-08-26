from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path

from scripts.doctor import LLVM_COV_VERSION, collect_checks, parse_version
from scripts.workflow_common import (
    WorkflowError,
    repository_namespace,
    validate_ambient_target_dir,
    validate_shared_target_base,
)
from scripts.tests.helpers import file_snapshot, init_repo, run_git, write


def completed(command: tuple[str, ...], output: str, returncode: int = 0) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(command, returncode, stdout=output, stderr="")


def fake_runner_factory(
    pin: str,
    *,
    llvm_version: str = LLVM_COV_VERSION,
    jscpd_version: str = "4.0.5",
    installed: bool = True,
):
    def runner(command, cwd):
        key = tuple(command)
        if key == ("rustup", "toolchain", "list"):
            output = f"{pin}-fixture-host\n" if installed else "stable-fixture-host\n"
            return completed(key, output)
        if key == ("python3", "--version"):
            return completed(key, "Python 3.11.9\n")
        if key == ("rustc", f"+{pin}", "-V"):
            return completed(key, f"rustc {pin} (fixture)\n")
        if key[:4] == ("rustup", "component", "list", "--toolchain"):
            return completed(
                key,
                "rustfmt-fixture-host\nclippy-fixture-host\nllvm-tools-fixture-host\n",
            )
        if key == ("cargo", f"+{pin}", "clippy", "-V"):
            return completed(key, "clippy fixture\n")
        if key == ("cargo", f"+{pin}", "-V"):
            return completed(key, f"cargo {pin} (fixture)\n")
        if key == ("cargo", f"+{pin}", "fmt", "--version"):
            return completed(key, "rustfmt fixture\n")
        if key == ("cargo-llvm-cov", "llvm-cov", "--version"):
            return completed(key, f"cargo-llvm-cov {llvm_version}\n")
        if key == ("jscpd", "--version"):
            return completed(key, f"{jscpd_version}\n")
        if key == ("git", "rev-parse", "--git-dir"):
            return completed(key, ".git\n")
        if key[:2] in (("git", "cat-file"), ("git", "merge-base")):
            return subprocess.run(
                list(command),
                cwd=cwd,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
        return completed(key, "", 1)

    return runner


def all_tools(_: str) -> str:
    return "/fixture/bin/tool"


class DoctorTests(unittest.TestCase):
    def test_version_parser(self) -> None:
        self.assertEqual(parse_version("rustc 1.93.0 (hash)", "rustc"), "1.93.0")
        self.assertEqual(
            parse_version("cargo-llvm-cov 0.6.21", "cargo-llvm-cov"), "0.6.21"
        )
        self.assertIsNone(parse_version("unknown", "rustc"))

    def test_llvm_cov_skew_is_optional_with_exact_remediation(self) -> None:
        root = Path(__file__).resolve().parents[2]
        checks = collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0", llvm_version="0.6.20"),
            which=all_tools,
        )
        check = next(item for item in checks if item.name == "cargo-llvm-cov")
        self.assertEqual(check.status, "info")
        self.assertIn("--version 0.6.21", check.remediation or "")

    def test_missing_toolchain_reports_install_command_without_probing_it(self) -> None:
        root = Path(__file__).resolve().parents[2]
        checks = collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0", installed=False),
            which=all_tools,
        )
        check = next(item for item in checks if item.name == "rust-toolchain")
        self.assertEqual(check.status, "failed")
        self.assertIn("rustup toolchain install 1.93.0", check.remediation or "")

    def test_jscpd_skew_is_optional_with_install_guidance(self) -> None:
        root = Path(__file__).resolve().parents[2]
        checks = collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0", jscpd_version="4.0.4"),
            which=all_tools,
        )
        check = next(item for item in checks if item.name == "jscpd")
        self.assertEqual(check.status, "info")
        self.assertIn("jscpd@4.0.5", check.remediation or "")

    def test_missing_python_is_reported(self) -> None:
        root = Path(__file__).resolve().parents[2]

        def without_python(name: str) -> str | None:
            return None if name == "python3" else "/fixture/bin/tool"

        checks = collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0"),
            which=without_python,
        )
        check = next(item for item in checks if item.name == "python3")
        self.assertEqual(check.status, "failed")

    def test_doctor_collection_is_read_only(self) -> None:
        root = Path(__file__).resolve().parents[2]
        before = file_snapshot(root)
        collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0"),
            which=all_tools,
        )
        self.assertEqual(file_snapshot(root), before)

    def test_exact_jscpd_makes_missing_node_and_npm_informational(self) -> None:
        root = Path(__file__).resolve().parents[2]

        def without_node_installers(name: str) -> str | None:
            return None if name in ("node", "npm") else "/fixture/bin/tool"

        checks = collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0"),
            which=without_node_installers,
        )
        statuses = {item.name: item.status for item in checks}
        self.assertEqual(statuses["jscpd"], "info")
        self.assertEqual(statuses["node"], "info")
        self.assertEqual(statuses["npm"], "info")
        self.assertNotIn("failed", statuses.values())

    def test_missing_jscpd_and_npm_are_informational(self) -> None:
        root = Path(__file__).resolve().parents[2]

        def without_jscpd_or_npm(name: str) -> str | None:
            return None if name in ("jscpd", "npm") else "/fixture/bin/tool"

        checks = collect_checks(
            root,
            environ={},
            runner=fake_runner_factory("1.93.0"),
            which=without_jscpd_or_npm,
        )
        by_name = {item.name: item for item in checks}
        self.assertEqual(by_name["jscpd"].status, "info")
        self.assertEqual(by_name["npm"].status, "info")

    def test_patch_baseline_resolution_order_and_failure_remediation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(
                root / "rust-toolchain.toml",
                '[toolchain]\nchannel = "1.93.0"\n',
            )
            runner = fake_runner_factory("1.93.0")
            origin = collect_checks(root, environ={}, runner=runner, which=all_tools)
            origin_check = next(item for item in origin if item.name == "patch-baseline")
            self.assertEqual(origin_check.status, "passed")
            self.assertIn(base, origin_check.detail)

            run_git(root, "update-ref", "-d", "refs/remotes/origin/main")
            explicit = collect_checks(
                root,
                environ={"BASE_SHA": base},
                runner=runner,
                which=all_tools,
            )
            explicit_check = next(item for item in explicit if item.name == "patch-baseline")
            self.assertEqual(explicit_check.status, "passed")

            run_git(root, "update-ref", "refs/heads/baseline", base)
            referenced = collect_checks(
                root,
                environ={"BASE_REF": "baseline"},
                runner=runner,
                which=all_tools,
            )
            referenced_check = next(item for item in referenced if item.name == "patch-baseline")
            self.assertEqual(referenced_check.status, "passed")
            self.assertIn("baseline resolves", referenced_check.detail)

            missing = collect_checks(
                root,
                environ={"BASE_SHA": "missing"},
                runner=runner,
                which=all_tools,
            )
            missing_check = next(item for item in missing if item.name == "patch-baseline")
            self.assertEqual(missing_check.status, "info")
            self.assertIn("git fetch origin main", missing_check.remediation or "")
            self.assertIn("BASE_SHA=<commit>", missing_check.remediation or "")


class SharedTargetValidationTests(unittest.TestCase):
    def test_linked_worktrees_receive_the_same_repository_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            linked = parent / "linked"
            run = subprocess.run(
                ["git", "worktree", "add", "--detach", str(linked), "HEAD"],
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(run.returncode, 0, run.stderr)
            self.assertEqual(repository_namespace(root), repository_namespace(linked))

    def test_relative_inside_symlink_and_external_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            external = parent / "external-cache"
            external.mkdir()
            inside = root / "custom-target"
            inside.mkdir()
            link = parent / "cache-link"
            link.symlink_to(inside, target_is_directory=True)

            with self.assertRaisesRegex(WorkflowError, "absolute"):
                validate_shared_target_base(root, "relative/cache")
            with self.assertRaisesRegex(WorkflowError, "inside"):
                validate_shared_target_base(root, str(inside))
            with self.assertRaisesRegex(WorkflowError, "inside"):
                validate_shared_target_base(root, str(link))
            namespaced = validate_shared_target_base(root, str(external))

        self.assertEqual(namespaced.parent, external.resolve())
        self.assertIn("repo-", namespaced.name)

    def test_ambient_checkout_path_is_rejected_except_ignored_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"
            init_repo(root)
            with self.assertRaisesRegex(WorkflowError, "inside this checkout's target"):
                validate_ambient_target_dir(root, str(root / "build-cache"))
            allowed = validate_ambient_target_dir(root, str(root / "target" / "custom"))
        self.assertEqual(allowed, (root / "target" / "custom").resolve())

    def test_ambient_target_in_another_linked_worktree_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            linked = parent / "linked"
            result = subprocess.run(
                ["git", "worktree", "add", "--detach", str(linked), "HEAD"],
                cwd=root,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            with self.assertRaisesRegex(WorkflowError, "inside this checkout's target"):
                validate_ambient_target_dir(root, str(linked / "target"))

    def test_external_ambient_target_is_rejected_by_doctor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            write(root / "rust-toolchain.toml", '[toolchain]\nchannel = "1.93.0"\n')
            checks = collect_checks(
                root,
                environ={"CARGO_TARGET_DIR": str(parent / "external")},
                runner=fake_runner_factory("1.93.0"),
                which=all_tools,
            )
            ambient = next(item for item in checks if item.name == "ambient-target")
        self.assertEqual(ambient.status, "failed")
        self.assertEqual(ambient.remediation, "unset CARGO_TARGET_DIR")


if __name__ == "__main__":
    unittest.main()
