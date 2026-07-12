from __future__ import annotations

import contextlib
import io
import os
import stat
import tempfile
import unittest
from pathlib import Path

from scripts.publish_report import main
from scripts.tests.helpers import init_repo, run_git, write


def report(body: bytes, *, dirty: bool = False) -> bytes:
    suffix = " plus uncommitted worktree changes" if dirty else ""
    header = (
        "# Code Quality Report — Crab Project\n\n"
        f"Generated for commit `fixture`{suffix} (commit date: 2026-07-12).\n\n"
    ).encode("utf-8")
    return header + body


def commit_report(root: Path, body: bytes, *, dirty: bool = False) -> bytes:
    content = report(body, dirty=dirty)
    (root / "CODE_QUALITY_REPORT.md").write_bytes(content)
    run_git(root, "add", "CODE_QUALITY_REPORT.md")
    run_git(root, "commit", "-m", "add generated report")
    return content


class PublishReportTests(unittest.TestCase):
    def test_clean_checkout_with_changed_body_gets_dirty_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            commit_report(root, b"old body\n")
            code = main(["--root", str(root)], body=b"new body\n")
            published = (root / "CODE_QUALITY_REPORT.md").read_text(encoding="utf-8")
        self.assertEqual(code, 0)
        self.assertIn("plus uncommitted worktree changes", published)
        self.assertTrue(published.endswith("new body\n"))

    def test_identical_body_preserves_committed_bytes_and_clean_tree(self) -> None:
        for dirty_header in (False, True):
            with self.subTest(dirty_header=dirty_header), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                expected = commit_report(root, b"same body\n", dirty=dirty_header)
                code = main(["--root", str(root)], body=b"same body\n")
                self.assertEqual(code, 0)
                self.assertEqual((root / "CODE_QUALITY_REPORT.md").read_bytes(), expected)
                self.assertEqual(run_git(root, "status", "--porcelain"), "")
                self.assertEqual(list(root.glob(".CODE_QUALITY_REPORT.md.*.tmp")), [])

    def test_other_dirty_file_does_not_rewrite_unchanged_historical_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            expected = commit_report(root, b"same body\n")
            write(root / "Cargo.lock", "dirty elsewhere\n")
            self.assertEqual(main(["--root", str(root)], body=b"same body\n"), 0)
            self.assertEqual((root / "CODE_QUALITY_REPORT.md").read_bytes(), expected)

    def test_uncommitted_path_and_unexpected_committed_header_use_dirty_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            self.assertEqual(
                main(
                    ["--root", str(root), "--out-path", "NEW_REPORT.md"],
                    body=b"new\n",
                ),
                0,
            )
            self.assertIn(
                b"plus uncommitted worktree changes",
                (root / "NEW_REPORT.md").read_bytes(),
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            write(root / "CODE_QUALITY_REPORT.md", "unexpected header\nold body\n")
            run_git(root, "add", "CODE_QUALITY_REPORT.md")
            run_git(root, "commit", "-m", "malformed historical report")
            self.assertEqual(main(["--root", str(root)], body=b"old body\n"), 0)
            self.assertIn(
                b"plus uncommitted worktree changes",
                (root / "CODE_QUALITY_REPORT.md").read_bytes(),
            )

    def test_symlinked_output_or_parent_is_rejected_without_touching_sentinel(self) -> None:
        for parent_link in (False, True):
            with self.subTest(parent_link=parent_link), tempfile.TemporaryDirectory() as directory:
                parent = Path(directory)
                root = parent / "repo"
                init_repo(root)
                external = parent / "external"
                external.mkdir()
                sentinel = external / "report.md"
                write(sentinel, "sentinel\n")
                if parent_link:
                    (root / "reports").symlink_to(external, target_is_directory=True)
                    output = "reports/report.md"
                else:
                    (root / "CODE_QUALITY_REPORT.md").symlink_to(sentinel)
                    output = "CODE_QUALITY_REPORT.md"
                with contextlib.redirect_stderr(io.StringIO()):
                    code = main(
                        ["--root", str(root), "--out-path", output], body=b"body\n"
                    )
                self.assertEqual(code, 2)
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "sentinel\n")

    def test_atomic_publish_preserves_existing_mode_and_new_files_use_0644(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            commit_report(root, b"old\n")
            path = root / "CODE_QUALITY_REPORT.md"
            os.chmod(path, 0o640)
            self.assertEqual(main(["--root", str(root)], body=b"new\n"), 0)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)
            self.assertEqual(
                main(
                    ["--root", str(root), "--out-path", "NEW.md"], body=b"body\n"
                ),
                0,
            )
            self.assertEqual(stat.S_IMODE((root / "NEW.md").stat().st_mode), 0o644)

    def test_replace_failure_preserves_original_and_cleans_temporary_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            original = commit_report(root, b"old\n")
            path = root / "CODE_QUALITY_REPORT.md"
            os.chmod(path, 0o640)

            def fail_replace(_: Path, __: Path) -> None:
                raise OSError("injected replace failure")

            with contextlib.redirect_stderr(io.StringIO()):
                code = main(
                    ["--root", str(root)], body=b"new\n", replace=fail_replace
                )
            self.assertEqual(code, 2)
            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)
            self.assertEqual(list(root.glob(".CODE_QUALITY_REPORT.md.*.tmp")), [])


if __name__ == "__main__":
    unittest.main()
