from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts.patch_coverage import (
    HUMAN_RANGE_LIMIT,
    _added_lines_from_diff,
    allowed_uncovered,
    collect_added_production_lines,
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
    def test_diff_parser_maps_plus_prefixed_content_without_shifting(self) -> None:
        diff = """diff --git a/file b/file
index 111..222 100644
--- a/file
+++ b/file
@@ -1 +1,3 @@
 original
+++literal
+successor
"""
        self.assertEqual(
            _added_lines_from_diff(diff),
            {2: "++literal", 3: "successor"},
        )

    def test_removed_dash_content_and_multiple_hunks_map_exactly(self) -> None:
        diff = """diff --git a/file b/file
--- a/file
+++ b/file
@@ -1,2 +1,2 @@
 keep
---removed
+++added
@@ -10 +10,2 @@
 ten
+eleven
"""
        self.assertEqual(_added_lines_from_diff(diff), {2: "++added", 11: "eleven"})

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

            with self.assertRaisesRegex(WorkflowError, "match the index"):
                validate_diff_mode(root, "staged")

    def test_committed_mode_rejects_dirty_rust(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            write(root / "crates" / "alpha" / "src" / "lib.rs", "pub fn dirty() {}\n")
            with self.assertRaisesRegex(WorkflowError, "match HEAD"):
                validate_diff_mode(root, "committed")

    def test_guarded_modes_reject_all_non_doc_snapshot_differences(self) -> None:
        staged_cases = (
            ("Cargo.toml", "[workspace]\nmembers=[]\n"),
            ("scripts/patch_coverage.py", "changed tooling\n"),
            ("crates/alpha/tests/fixtures/data.json", "{}\n"),
        )
        for path, content in staged_cases:
            with self.subTest(mode="staged", path=path), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                if path == "scripts/patch_coverage.py":
                    write(root / path, "base tooling\n")
                    run_git(root, "add", path)
                    run_git(root, "commit", "-m", "add tooling")
                write(root / path, content)
                with self.assertRaisesRegex(WorkflowError, "use --mode worktree"):
                    validate_diff_mode(root, "staged")

        committed_cases = (
            ("Cargo.lock", "staged lock change\n", True),
            ("crates/alpha/fixture.json", "dirty fixture\n", False),
        )
        for path, content, stage in committed_cases:
            with self.subTest(mode="committed", path=path), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                if path != "Cargo.lock":
                    write(root / path, "base fixture\n")
                    run_git(root, "add", path)
                    run_git(root, "commit", "-m", "add fixture")
                write(root / path, content)
                if stage:
                    run_git(root, "add", path)
                with self.assertRaisesRegex(WorkflowError, "use --mode worktree"):
                    validate_diff_mode(root, "committed")

    def test_guarded_modes_tolerate_allowlisted_documentation(self) -> None:
        for mode in ("staged", "committed"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                write(root / "README.md", "dirty documentation\n")
                validate_diff_mode(root, mode)

    def test_git_diff_maps_added_content_beginning_with_double_plus(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            source = root / "crates" / "alpha" / "src" / "lib.rs"
            source.write_text(
                source.read_text(encoding="utf-8") + "++literal\nsuccessor\n",
                encoding="utf-8",
            )
            additions = collect_added_production_lines(root, "worktree", base)
        self.assertEqual(additions["crates/alpha/src/lib.rs"], {2: "++literal", 3: "successor"})

    def test_symlink_to_regular_type_change_counts_full_new_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            source = root / "crates" / "alpha" / "src" / "lib.rs"
            source.unlink()
            source.symlink_to("elsewhere.rs")
            run_git(root, "add", "crates/alpha/src/lib.rs")
            run_git(root, "commit", "-m", "make source a symlink")
            base = run_git(root, "rev-parse", "HEAD")
            source.unlink()
            write(source, "pub fn first() {}\npub fn second() {}\n")
            additions = collect_added_production_lines(root, "worktree", base)
        self.assertEqual(
            additions["crates/alpha/src/lib.rs"],
            {1: "pub fn first() {}", 2: "pub fn second() {}"},
        )

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
