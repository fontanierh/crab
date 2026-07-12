from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.coverage_workflow import (
    FUNCTION_THRESHOLD,
    LINE_THRESHOLD,
    REGION_THRESHOLD,
    coverage_arguments,
    run_gate,
    run_quick,
    run_report,
)
from scripts.tests.helpers import init_repo, write


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
            ("99.5", "99.0", "99.4"),
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
            ]
            for path in paths:
                write(path, "stale green evidence\n")
            with patch(
                "scripts.coverage_workflow.run_local_coverage", return_value=1
            ):
                code = run_gate(root, "worktree", base)
            self.assertEqual(code, 1)
            self.assertTrue(all(not path.exists() for path in paths))

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
