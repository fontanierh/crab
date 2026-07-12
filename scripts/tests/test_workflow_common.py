from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

from scripts.run_gates import main as run_gates_main
from scripts.workflow_common import WorkflowError, tree_fingerprint
from scripts.tests.helpers import init_repo, run_git, write


class TreeFingerprintTests(unittest.TestCase):
    def test_digest_is_stable_and_tracks_content_mode_and_untracked_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            clean = tree_fingerprint(root)
            repeated = tree_fingerprint(root)
            self.assertEqual(clean, repeated)
            self.assertFalse(clean.dirty)

            lockfile = root / "Cargo.lock"
            write(lockfile, "content change\n")
            content = tree_fingerprint(root)
            self.assertTrue(content.dirty)
            self.assertNotEqual(content.digest, clean.digest)

            os.chmod(lockfile, 0o755)
            executable = tree_fingerprint(root)
            self.assertNotEqual(executable.digest, content.digest)

            write(root / "untracked.txt", "new\n")
            untracked = tree_fingerprint(root)
            self.assertNotEqual(untracked.digest, executable.digest)
            self.assertEqual(len(untracked.entries), 2)

    def test_split_index_state_prevents_quality_from_starting(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "Cargo.lock", "staged version\n")
            run_git(root, "add", "Cargo.lock")
            write(root / "Cargo.lock", "# deterministic fixture\n")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = run_gates_main(
                    ["--root", str(root), "quality", "--base-sha", base]
                )
        self.assertEqual(code, 2)
        self.assertIn("index and working tree contain split content", stderr.getvalue())

    def test_staged_deletion_restored_on_disk_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            run_git(root, "rm", "Cargo.lock")
            write(root / "Cargo.lock", "restored but not staged\n")
            with self.assertRaisesRegex(WorkflowError, "staged deletions"):
                tree_fingerprint(root)

    def test_hidden_index_flags_are_rejected(self) -> None:
        for flag, wording in (
            ("--assume-unchanged", "assume-unchanged"),
            ("--skip-worktree", "skip-worktree"),
        ):
            with self.subTest(flag=flag), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                run_git(root, "update-index", flag, "Cargo.lock")
                with self.assertRaisesRegex(WorkflowError, wording):
                    tree_fingerprint(root)

    def test_core_filemode_false_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            run_git(root, "config", "core.filemode", "false")
            with self.assertRaisesRegex(WorkflowError, "core.filemode=false"):
                tree_fingerprint(root)


if __name__ == "__main__":
    unittest.main()
