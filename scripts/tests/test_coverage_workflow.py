from __future__ import annotations

import contextlib
import io
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.changed_scope import ScopeResult
from scripts.lcov_stats import main as lcov_stats_main, parse_lcov
from scripts.quality_baseline import collect_metrics
from scripts.workflow_common import WorkflowError

from scripts.coverage_workflow import (
    FUNCTION_THRESHOLD,
    LINE_THRESHOLD,
    REGION_THRESHOLD,
    coverage_arguments,
    main as coverage_main,
    run_diagnostics,
    run_gate,
    run_quick,
    run_report,
)
from scripts.tests.helpers import init_repo, run_git, write


def coverage_side_effect(root: Path, calls: list[list[str]], *, excluded: bool = False):
    def run(_: Path, arguments: list[str]) -> int:
        calls.append(list(arguments))
        output = Path(arguments[arguments.index("--output-path") + 1])
        if output.suffix == ".info":
            source = (
                root / "crates" / "alpha" / "src" / "test_support.rs"
                if excluded
                else root / "crates" / "alpha" / "src" / "lib.rs"
            )
            write(
                output,
                f"SF:{source}\nDA:1,1\nLF:1\nLH:1\nend_of_record\n",
            )
        elif output.suffix == ".json":
            write(
                output,
                json.dumps(
                    {
                        "data": [
                            {
                                "totals": {
                                    "functions": {"percent": 100.0},
                                    "regions": {"percent": 100.0},
                                    "lines": {"percent": 100.0},
                                }
                            }
                        ]
                    }
                ),
            )
        return 0

    return run


class CoverageWorkflowTests(unittest.TestCase):
    def test_quick_explicitly_skips_without_rust_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "README.md", "documentation only\n")
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = run_quick(root, base)
            self.assertFalse((root / "coverage").exists())
        self.assertEqual(code, 0)
        self.assertIn("skipped: no Rust changes", output.getvalue())

    def test_package_arguments_are_stable(self) -> None:
        self.assertEqual(
            coverage_arguments(["alpha", "beta"], False),
            ["--package", "alpha", "--package", "beta"],
        )
        self.assertEqual(coverage_arguments(["alpha"], True), ["--workspace"])

    def test_documented_aggregate_thresholds_are_exact(self) -> None:
        self.assertEqual(
            (FUNCTION_THRESHOLD, REGION_THRESHOLD, LINE_THRESHOLD),
            ("99.5", "98.93", "99.4"),
        )

    def test_failed_gate_invalidates_all_prior_authoritative_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            coverage = root / "coverage"
            coverage.mkdir()
            paths = [
                coverage / "lcov.info",
                coverage / "summary.json",
                coverage / "patch-coverage.json",
                coverage / "uncovered_locations.txt",
            ]
            for path in paths:
                write(path, "stale green evidence\n")
            with patch(
                "scripts.coverage_workflow.run_local_coverage", return_value=1
            ):
                code = run_gate(root, "worktree", base)
            self.assertEqual(code, 1)
            self.assertTrue(all(not path.exists() for path in paths))

    def test_every_coverage_command_uses_shared_ignore_policy(self) -> None:
        runners = ("report", "gate", "quick", "diagnostics")
        for command in runners:
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                base = init_repo(root)
                if command == "quick":
                    write(root / "Cargo.lock", "changed config\n")
                    write(root / "crates" / "alpha" / "src" / "lib.rs", "pub fn changed() {}\n")
                calls: list[list[str]] = []
                side_effect = coverage_side_effect(root, calls)
                with (
                    patch("scripts.coverage_workflow.run_local_coverage", side_effect=side_effect),
                    patch("scripts.coverage_workflow.run_patch_gate", return_value=0),
                    contextlib.redirect_stdout(io.StringIO()),
                ):
                    if command == "report":
                        code = run_report(root)
                    elif command == "gate":
                        code = run_gate(root, "worktree", base)
                    elif command == "quick":
                        code = run_quick(root, base)
                    else:
                        code = run_diagnostics(root)
                self.assertEqual(code, 0)
                self.assertGreaterEqual(len(calls), 1)
                for arguments in calls:
                    index = arguments.index("--ignore-filename-regex")
                    self.assertEqual(arguments[index + 1], r"(^|/)test_support\.rs$")

    def test_generated_lcov_with_policy_excluded_source_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            calls: list[list[str]] = []
            stderr = io.StringIO()
            with (
                patch(
                    "scripts.coverage_workflow.run_local_coverage",
                    side_effect=coverage_side_effect(root, calls, excluded=True),
                ),
                contextlib.redirect_stderr(stderr),
            ):
                code = run_report(root)
            self.assertEqual(code, 2)
            self.assertIn("policy-excluded files", stderr.getvalue())
            self.assertFalse((root / "coverage/lcov.info").exists())
            self.assertTrue((root / "coverage/lcov.info.rejected").exists())
            poison = (root / "coverage/lcov.info.rejected").read_bytes()
            (root / "coverage/lcov.info").write_bytes(poison)
            with self.assertRaisesRegex(WorkflowError, "policy-excluded"):
                parse_lcov(root, root / "coverage/lcov.info")
            with self.assertRaisesRegex(WorkflowError, "policy-excluded"):
                collect_metrics(root, require_lcov=True)
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(
                    lcov_stats_main(
                        [
                            "hotspots",
                            "--root",
                            str(root),
                            "--lcov",
                            str(root / "coverage/lcov.info"),
                        ]
                    ),
                    2,
                )

    def test_quick_quarantines_policy_excluded_lcov(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "crates/alpha/src/lib.rs", "pub fn changed() {}\n")
            calls: list[list[str]] = []
            with (
                patch(
                    "scripts.coverage_workflow.run_local_coverage",
                    side_effect=coverage_side_effect(root, calls, excluded=True),
                ),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(run_quick(root, base), 2)
            self.assertFalse((root / "coverage/quick-lcov.info").exists())
            self.assertTrue((root / "coverage/quick-lcov.info.rejected").exists())

    def test_quick_diff_failure_is_not_misreported_as_no_changes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            scope = ScopeResult(
                mode="worktree",
                base_sha=base,
                changed_files=[],
                selected_packages=[],
                full_workspace=True,
                docs_only=False,
                fallback_reason="git diff failed with exit 128",
            )
            stderr = io.StringIO()
            with (
                patch("scripts.coverage_workflow.select_scope", return_value=scope),
                patch("scripts.coverage_workflow.run_local_coverage") as coverage,
                contextlib.redirect_stderr(stderr),
            ):
                code = run_quick(root, base)
            self.assertEqual(code, 2)
            self.assertIn("cannot determine changed scope", stderr.getvalue())
            coverage.assert_not_called()

    def test_quick_injected_git_diff_failure_stops_before_coverage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            real_run = subprocess.run

            def git_boundary(command: object, *args: object, **kwargs: object):
                if isinstance(command, list) and "diff" in command:
                    return subprocess.CompletedProcess(
                        command, 128, stdout=b"", stderr=b"fatal: injected diff failure\n"
                    )
                return real_run(command, *args, **kwargs)

            with (
                patch("scripts.workflow_common.subprocess.run", side_effect=git_boundary),
                patch("scripts.coverage_workflow.run_local_coverage") as coverage,
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(run_quick(root, base), 2)
            coverage.assert_not_called()
            with (
                patch("scripts.workflow_common.subprocess.run", side_effect=git_boundary),
                patch("scripts.coverage_workflow.run_local_coverage") as coverage,
                contextlib.redirect_stderr(io.StringIO()),
            ):
                self.assertEqual(
                    coverage_main(["--root", str(root), "quick", "--base-sha", base]),
                    2,
                )
            coverage.assert_not_called()

    def test_diagnostics_reports_lf_lh_gap_without_da_zero_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)

            def generate(_: Path, arguments: list[str]) -> int:
                output = Path(arguments[arguments.index("--output-path") + 1])
                write(
                    output,
                    f"SF:{root / 'crates/alpha/src/lib.rs'}\n"
                    "DA:1,3\nDA:2,1\nLF:10\nLH:8\nend_of_record\n",
                )
                return 0

            with patch("scripts.coverage_workflow.run_local_coverage", side_effect=generate):
                code = run_diagnostics(root)
            summary = (root / "coverage" / "uncovered_locations.txt").read_text(
                encoding="utf-8"
            )
            self.assertEqual(code, 0)
            self.assertIn("crates/alpha/src/lib.rs: 2 line(s)", summary)
            self.assertIn("+2 uncovered line(s) without DA:0 rows", summary)

    def test_authoritative_replacement_invalidates_all_companions(self) -> None:
        for command in ("report", "diagnostics"):
            with self.subTest(command=command), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                coverage = root / "coverage"
                coverage.mkdir()
                for name in (
                    "lcov.info",
                    "summary.json",
                    "patch-coverage.json",
                    "uncovered_locations.txt",
                ):
                    write(coverage / name, "stale\n")
                calls: list[list[str]] = []
                with patch(
                    "scripts.coverage_workflow.run_local_coverage",
                    side_effect=coverage_side_effect(root, calls),
                ):
                    code = run_report(root) if command == "report" else run_diagnostics(root)
                self.assertEqual(code, 0)
                self.assertTrue((coverage / "lcov.info").exists())
                self.assertFalse((coverage / "summary.json").exists())
                self.assertFalse((coverage / "patch-coverage.json").exists())
                if command == "report":
                    self.assertFalse((coverage / "uncovered_locations.txt").exists())

    def test_gate_then_report_cannot_leave_stale_green_companions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            calls: list[list[str]] = []

            def patch_gate(*_: object, artifact_path: Path, **__: object) -> int:
                write(artifact_path, '{"passed":true}\n')
                return 0

            with (
                patch(
                    "scripts.coverage_workflow.run_local_coverage",
                    side_effect=coverage_side_effect(root, calls),
                ),
                patch("scripts.coverage_workflow.run_patch_gate", side_effect=patch_gate),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(run_gate(root, "worktree", base), 0)
                self.assertTrue((root / "coverage" / "summary.json").exists())
                self.assertTrue((root / "coverage" / "patch-coverage.json").exists())
                self.assertEqual(run_report(root), 0)
            self.assertFalse((root / "coverage" / "summary.json").exists())
            self.assertFalse((root / "coverage" / "patch-coverage.json").exists())

    def test_guarded_gate_preflight_stops_coverage_before_spawn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            run_git(root, "update-index", "--assume-unchanged", "Cargo.lock")
            with patch("scripts.coverage_workflow.run_local_coverage") as coverage:
                code = run_gate(root, "staged", base)
            self.assertEqual(code, 2)
            coverage.assert_not_called()

    def test_coverage_directory_symlink_is_rejected_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            external = parent / "external"
            external.mkdir()
            sentinel = external / "lcov.info"
            write(sentinel, "sentinel\n")
            (root / "coverage").symlink_to(external, target_is_directory=True)
            code = run_report(root)
            self.assertEqual(code, 2)
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "sentinel\n")


if __name__ == "__main__":
    unittest.main()
