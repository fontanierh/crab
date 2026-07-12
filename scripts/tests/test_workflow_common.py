from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

from scripts.run_gates import main as run_gates_main
from scripts.workflow_common import (
    WorkflowError,
    coverage_target_environment,
    repository_namespace,
    tree_fingerprint,
    validate_shared_target_base,
)
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

    def test_cross_file_partial_staging_and_staged_untracked_mix_are_rejected(self) -> None:
        for untracked in (False, True):
            with self.subTest(untracked=untracked), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)
                first = root / "Cargo.lock"
                second = root / "Cargo.toml"
                write(first, "staged\n")
                run_git(root, "add", "Cargo.lock")
                if untracked:
                    write(root / "new.txt", "untracked\n")
                else:
                    write(second, "unstaged\n")
                with self.assertRaisesRegex(WorkflowError, "entirely HEAD"):
                    tree_fingerprint(root)

    def test_intent_to_add_is_rejected_explicitly(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            write(root / "new.txt", "new\n")
            run_git(root, "add", "-N", "new.txt")
            with self.assertRaisesRegex(WorkflowError, "intent-to-add"):
                tree_fingerprint(root)

    def test_owner_execute_matches_git_while_group_execute_is_ignored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            lockfile = root / "Cargo.lock"
            write(lockfile, "dirty\n")
            os.chmod(lockfile, 0o775)
            owner_executable = tree_fingerprint(root)
            os.chmod(lockfile, 0o675)
            owner_cleared = tree_fingerprint(root)
            self.assertNotEqual(owner_executable.digest, owner_cleared.digest)
            os.chmod(lockfile, 0o644)
            regular = tree_fingerprint(root)
            os.chmod(lockfile, 0o654)
            group_only = tree_fingerprint(root)
            self.assertEqual(regular.digest, group_only.digest)


class TargetPolicyTests(unittest.TestCase):
    def test_coverage_environment_overrides_both_llvm_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            environment = coverage_target_environment(
                root,
                {
                    "CARGO_LLVM_COV_TARGET_DIR": "/external/target",
                    "CARGO_LLVM_COV_BUILD_DIR": "/external/build",
                    "CRAB_SHARED_TARGET_DIR": "/external/shared",
                },
                create=False,
            )
            instrumented = root.resolve() / "target" / "llvm-cov-worktree" / "instrumented"
            self.assertEqual(environment["CARGO_TARGET_DIR"], str(instrumented.parent))
            self.assertEqual(environment["CARGO_LLVM_COV_TARGET_DIR"], str(instrumented))
            self.assertEqual(environment["CARGO_LLVM_COV_BUILD_DIR"], str(instrumented))
            self.assertNotIn("CRAB_SHARED_TARGET_DIR", environment)

    def test_shared_base_rejects_real_linked_worktree_at_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            shared = parent / "shared"
            shared.mkdir()
            namespace = shared / repository_namespace(root)
            run_git(root, "worktree", "add", "--detach", str(namespace), "HEAD")
            with self.assertRaisesRegex(WorkflowError, "disjoint"):
                validate_shared_target_base(root, str(shared))

    def test_shared_base_rejects_reverse_containment_of_any_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            shared = parent / "shared"
            shared.mkdir()
            linked = shared / "some-worktree"
            run_git(root, "worktree", "add", "--detach", str(linked), "HEAD")
            with self.assertRaisesRegex(WorkflowError, "disjoint"):
                validate_shared_target_base(root, str(shared))

    def test_shared_base_cannot_contain_the_main_repository_or_common_dir(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            with self.assertRaisesRegex(WorkflowError, "disjoint"):
                validate_shared_target_base(root, str(parent))


if __name__ == "__main__":
    unittest.main()
