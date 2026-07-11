from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path

from scripts.coverage_workflow import (
    FUNCTION_THRESHOLD,
    LINE_THRESHOLD,
    REGION_THRESHOLD,
    coverage_arguments,
    run_quick,
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


if __name__ == "__main__":
    unittest.main()
