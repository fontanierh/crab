from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.changed_scope import (
    ScopeResult,
    classify_paths,
    collect_changed_files,
    is_docs_path,
    select_packages_from_metadata,
    select_scope,
)
from scripts.workflow_common import WorkflowError
from scripts.tests.helpers import init_repo, run_git, write


FIXTURE = Path(__file__).parent / "fixtures" / "metadata-no-deps.json"


def fixture_metadata(root: Path) -> dict[str, object]:
    return json.loads(FIXTURE.read_text(encoding="utf-8").replace("$ROOT", str(root)))


class ChangedScopeUnitTests(unittest.TestCase):
    def test_docs_unknown_and_full_trigger_classification_is_conservative(self) -> None:
        self.assertEqual(classify_paths(["README.md", "crab/docs/guide.md"]), (True, False))
        self.assertEqual(classify_paths(["Makefile"]), (False, True))
        self.assertEqual(classify_paths(["assets/logo.png"]), (False, False))

    def test_docs_allowlist_is_location_and_suffix_exact(self) -> None:
        positives = [
            "docs/x.md",
            "crab/docs/x.txt",
            "notes/x.md",
            "design/x.rst",
            "README.md",
            "AGENTS.md",
            "CLAUDE.md",
            "CONTRIBUTING.md",
            "PHILOSOPHY.md",
            "CODE_QUALITY_REPORT.md",
            "crab/DESIGN.md",
            "crab/WORKSTREAMS.md",
            ".github/pull_request_template.md",
        ]
        negatives = [
            "crates/crab-core/tests/fixtures/data.txt",
            "crates/crab-app/README.md",
            "scripts/x.md",
            "crab/config/README.md",
            "crab/config/crab.env.example",
            "settings.txt",
            "LICENSE",
            "docs/logo.png",
        ]
        for path in positives:
            with self.subTest(path=path):
                self.assertTrue(is_docs_path(path))
        for path in negatives:
            with self.subTest(path=path):
                self.assertFalse(is_docs_path(path))
        self.assertEqual(classify_paths(["scripts/x.md"]), (False, True))

    def test_core_edit_selects_actual_reverse_dependents_but_not_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            selected, fallback = select_packages_from_metadata(
                root,
                ["crates/crab-core/src/lib.rs"],
                fixture_metadata(root),
            )
        self.assertIsNone(fallback)
        self.assertEqual(
            selected,
            [
                "crab-app",
                "crab-backends",
                "crab-core",
                "crab-discord",
                "crab-discord-connector",
                "crab-scheduler",
                "crab-store",
            ],
        )
        self.assertNotIn("crab-telemetry", selected)

    def test_unknown_code_path_falls_back_to_all_packages(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            selected, fallback = select_packages_from_metadata(
                root, ["tools/custom.rs"], fixture_metadata(root)
            )
        self.assertEqual(len(selected), 8)
        self.assertIn("unmapped code path", fallback or "")

    def test_full_trigger_is_classified_before_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "Makefile", "help:\n\t@true\n")

            def forbidden_loader(_: Path) -> dict[str, object]:
                raise AssertionError("metadata must not run for a full-scope trigger")

            result = select_scope(
                root, explicit_base=base, metadata_loader=forbidden_loader
            )
        self.assertTrue(result.full_workspace)
        self.assertIn("workflow/configuration", result.fallback_reason or "")

    def test_metadata_failure_falls_back_to_full_scope_with_reason(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "crates" / "alpha" / "src" / "lib.rs", "pub fn changed() {}\n")

            def broken_loader(_: Path) -> dict[str, object]:
                raise WorkflowError("offline metadata fixture failure")

            result = select_scope(root, explicit_base=base, metadata_loader=broken_loader)
        self.assertTrue(result.full_workspace)
        self.assertIn("offline metadata fixture failure", result.fallback_reason or "")


class ChangedScopeGitIntegrationTests(unittest.TestCase):
    @staticmethod
    def two_package_metadata(root: Path) -> dict[str, object]:
        return {
            "packages": [
                {
                    "name": "alpha",
                    "manifest_path": str(root / "crates" / "alpha" / "Cargo.toml"),
                    "dependencies": [],
                },
                {
                    "name": "beta",
                    "manifest_path": str(root / "crates" / "beta" / "Cargo.toml"),
                    "dependencies": [{"name": "alpha"}],
                },
            ]
        }

    def test_committed_and_worktree_modes_collect_the_deliberate_sets(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            tracked = root / "crates" / "alpha" / "src" / "lib.rs"
            write(tracked, "pub fn committed() {}\n")
            run_git(root, "add", str(tracked.relative_to(root)))
            run_git(root, "commit", "-m", "feature commit")
            write(root / "README.md", "staged\n")
            run_git(root, "add", "README.md")
            write(root / "notes.txt", "unstaged\n")
            write(root / "untracked.rs", "fn untracked() {}\n")

            committed = collect_changed_files(root, "committed", base)
            worktree = collect_changed_files(root, "worktree", base)

        self.assertEqual(committed, ["crates/alpha/src/lib.rs"])
        self.assertEqual(
            worktree,
            ["README.md", "crates/alpha/src/lib.rs", "notes.txt", "untracked.rs"],
        )

    def test_diverged_origin_uses_merge_base(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            run_git(root, "checkout", "-b", "feature")
            write(root / "feature.txt", "feature\n")
            run_git(root, "add", "feature.txt")
            run_git(root, "commit", "-m", "feature")
            feature_head = run_git(root, "rev-parse", "HEAD")
            run_git(root, "checkout", "main")
            write(root / "main.txt", "main\n")
            run_git(root, "add", "main.txt")
            run_git(root, "commit", "-m", "main advances")
            run_git(root, "update-ref", "refs/remotes/origin/main", "HEAD")
            run_git(root, "checkout", "feature")

            result = select_scope(root, mode="committed", docs_only_check=True)
            self.assertEqual(run_git(root, "rev-parse", "HEAD"), feature_head)

        self.assertEqual(result.base_sha, base)
        self.assertEqual(result.changed_files, ["feature.txt"])

    def test_missing_base_records_full_scope_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            run_git(root, "update-ref", "-d", "refs/remotes/origin/main")
            result = select_scope(root, explicit_base="missing-ref")
        self.assertTrue(result.full_workspace)
        self.assertIsNone(result.base_sha)
        self.assertIn("unavailable", result.fallback_reason or "")

    def test_deletion_only_committed_change_selects_package_and_reverse_dependent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root, packages=("alpha", "beta"))
            run_git(root, "rm", "crates/alpha/src/lib.rs")
            run_git(root, "commit", "-m", "delete alpha source")
            result = select_scope(
                root,
                mode="committed",
                explicit_base=base,
                metadata_loader=self.two_package_metadata,
            )
        self.assertEqual(result.changed_files, ["crates/alpha/src/lib.rs"])
        self.assertEqual(result.selected_packages, ["alpha", "beta"])

    def test_worktree_deletion_is_collected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            (root / "crates" / "alpha" / "src" / "lib.rs").unlink()
            changed = collect_changed_files(root, "worktree", base)
        self.assertEqual(changed, ["crates/alpha/src/lib.rs"])

    def test_deleted_full_scope_file_selects_whole_workspace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root, packages=("alpha", "beta"))
            run_git(root, "rm", "Cargo.lock")
            run_git(root, "commit", "-m", "delete lockfile")
            result = select_scope(root, mode="committed", explicit_base=base)
        self.assertTrue(result.full_workspace)
        self.assertEqual(result.selected_packages, ["alpha", "beta"])

    def test_cross_package_rename_accounts_for_both_sides(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root, packages=("alpha", "beta"))
            run_git(
                root,
                "mv",
                "crates/alpha/src/lib.rs",
                "crates/beta/src/moved.rs",
            )
            run_git(root, "commit", "-m", "move source across packages")
            changed = collect_changed_files(root, "committed", base)
            selected, fallback = select_packages_from_metadata(
                root, changed, self.two_package_metadata(root)
            )
        self.assertEqual(
            changed,
            ["crates/alpha/src/lib.rs", "crates/beta/src/moved.rs"],
        )
        self.assertIsNone(fallback)
        self.assertEqual(selected, ["alpha", "beta"])


if __name__ == "__main__":
    unittest.main()
