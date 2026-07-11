from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts.patch_coverage import (
    HUMAN_RANGE_LIMIT,
    allowed_uncovered,
    evaluate_patch,
    main,
    parse_lcov,
    validate_diff_mode,
)
from scripts.workflow_common import WorkflowError
from scripts.tests.helpers import init_repo, run_git, write


def lcov_record(root: Path, path: str, values: list[tuple[int, int]]) -> str:
    rows = [f"SF:{root / path}"]
    rows.extend(f"DA:{line},{hits}" for line, hits in values)
    rows.append("end_of_record")
    return "\n".join(rows) + "\n"


class PatchCoverageAccountingTests(unittest.TestCase):
    def test_small_patch_floor_boundaries(self) -> None:
        self.assertEqual(allowed_uncovered(19), 0)
        self.assertEqual(allowed_uncovered(20), 1)
        self.assertEqual(allowed_uncovered(39), 1)
        self.assertEqual(allowed_uncovered(40), 2)

    def test_evaluation_enforces_boundary(self) -> None:
        path = "crates/alpha/src/lib.rs"
        nineteen = {line: "code" for line in range(1, 20)}
        hits_19 = {line: (0 if line == 19 else 1) for line in nineteen}
        failed = evaluate_patch(
            mode="worktree",
            base_sha="base",
            additions={path: nineteen},
            represented={path},
            hits={path: hits_19},
            authoritative_lcov=True,
        )
        twenty = {line: "code" for line in range(1, 21)}
        hits_20 = {line: (0 if line == 20 else 1) for line in twenty}
        passed = evaluate_patch(
            mode="worktree",
            base_sha="base",
            additions={path: twenty},
            represented={path},
            hits={path: hits_20},
            authoritative_lcov=True,
        )
        self.assertFalse(failed.passed)
        self.assertTrue(passed.passed)

    def test_duplicate_sf_and_da_records_are_aggregated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = "crates/alpha/src/lib.rs"
            report = root / "report.info"
            report.write_text(
                lcov_record(root, path, [(4, 0)]) + lcov_record(root, path, [(4, 2)]),
                encoding="utf-8",
            )
            represented, hits = parse_lcov(root, report)
        self.assertEqual(represented, {path})
        self.assertEqual(hits[path][4], 2)

    def test_unrepresented_changed_code_fails_closed(self) -> None:
        result = evaluate_patch(
            mode="worktree",
            base_sha="base",
            additions={"crates/alpha/src/new.rs": {1: "pub fn new() {}"}},
            represented=set(),
            hits={},
            authoritative_lcov=True,
        )
        self.assertFalse(result.passed)
        self.assertEqual(result.unrepresented_files, ["crates/alpha/src/new.rs"])

    def test_comment_only_unrepresented_file_is_not_treated_as_code(self) -> None:
        result = evaluate_patch(
            mode="worktree",
            base_sha="base",
            additions={"crates/alpha/src/comments.rs": {1: "// docs", 2: ""}},
            represented=set(),
            hits={},
            authoritative_lcov=True,
        )
        self.assertTrue(result.passed)


class PatchCoverageGitModeTests(unittest.TestCase):
    def test_staged_mode_rejects_unstaged_rust_even_when_production_is_staged(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            test_file = root / "crates" / "alpha" / "tests" / "flow.rs"
            write(test_file, "#[test]\nfn base() {}\n")
            run_git(root, "add", str(test_file.relative_to(root)))
            run_git(root, "commit", "-m", "tracked test")
            production = root / "crates" / "alpha" / "src" / "lib.rs"
            write(production, "pub fn staged() {}\n")
            run_git(root, "add", str(production.relative_to(root)))
            write(test_file, "#[test]\nfn unstaged_support() {}\n")

            with self.assertRaisesRegex(WorkflowError, "index and working tree"):
                validate_diff_mode(root, "staged")

    def test_committed_mode_rejects_dirty_rust(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            write(root / "crates" / "alpha" / "src" / "lib.rs", "pub fn dirty() {}\n")
            with self.assertRaisesRegex(WorkflowError, "match HEAD"):
                validate_diff_mode(root, "committed")

    def test_no_base_exits_two_with_remediation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            run_git(root, "update-ref", "-d", "refs/remotes/origin/main")
            report = root / "empty.info"
            report.write_text("", encoding="utf-8")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = main(["--root", str(root), "--lcov", str(report)])
        self.assertEqual(code, 2)
        self.assertIn("git fetch", stderr.getvalue())

    def test_human_output_is_capped_but_json_is_complete(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            path = "crates/alpha/src/lib.rs"
            source = root / path
            base_lines = source.read_text(encoding="utf-8").splitlines()
            additions: list[str] = []
            for index in range(HUMAN_RANGE_LIMIT + 10):
                additions.extend((f"pub fn uncovered_{index}() {{}}", ""))
            source.write_text("\n".join(base_lines + additions) + "\n", encoding="utf-8")
            first_added = len(base_lines) + 1
            values = [(first_added + index * 2, 0) for index in range(HUMAN_RANGE_LIMIT + 10)]
            report = root / "report.info"
            report.write_text(lcov_record(root, path, values), encoding="utf-8")
            artifact = root / "result.json"
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = main(
                    [
                        "--root",
                        str(root),
                        "--base-sha",
                        base,
                        "--lcov",
                        str(report),
                        "--output-json",
                        str(artifact),
                    ]
                )
            payload = json.loads(artifact.read_text(encoding="utf-8"))
        self.assertEqual(code, 1)
        self.assertEqual(len(payload["uncovered_lines"]), HUMAN_RANGE_LIMIT + 10)
        self.assertIn("JSON artifact is complete", stdout.getvalue())
        self.assertFalse(payload["authoritative_lcov"])

    def test_fresh_input_marks_authoritative_and_unrepresented_file_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            new_file = root / "crates" / "alpha" / "src" / "new.rs"
            write(new_file, "pub fn not_compiled() {}\n")
            report = root / "empty.info"
            report.write_text("", encoding="utf-8")
            artifact = root / "result.json"
            with contextlib.redirect_stdout(io.StringIO()):
                code = main(
                    [
                        "--root",
                        str(root),
                        "--base-sha",
                        base,
                        "--fresh-lcov",
                        str(report),
                        "--output-json",
                        str(artifact),
                    ]
                )
            payload = json.loads(artifact.read_text(encoding="utf-8"))
        self.assertEqual(code, 1)
        self.assertTrue(payload["authoritative_lcov"])
        self.assertEqual(payload["unrepresented_files"], ["crates/alpha/src/new.rs"])


if __name__ == "__main__":
    unittest.main()
