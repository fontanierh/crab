from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path

from scripts.quality_baseline import main
from scripts.tests.helpers import init_repo, write


def fresh_lcov(root: Path) -> str:
    source = root / "crates" / "alpha" / "src" / "lib.rs"
    return f"SF:{source}\nDA:1,1\nLF:1\nLH:1\nend_of_record\n"


class QualityBaselineTests(unittest.TestCase):
    def test_symlinked_managed_directories_fail_before_runner_and_preserve_sentinel(self) -> None:
        for component, internal in (
            ("quality", False),
            ("baselines", False),
            ("quality", True),
            ("baselines", True),
        ):
            with self.subTest(component=component, internal=internal), tempfile.TemporaryDirectory() as directory:
                parent = Path(directory)
                root = parent / "repo"
                init_repo(root)
                target = (root / "internal") if internal else (parent / "external")
                target.mkdir()
                sentinel = target / "sentinel"
                write(sentinel, "untouched\n")
                if component == "quality":
                    (root / "quality").symlink_to(target, target_is_directory=True)
                else:
                    (root / "quality").mkdir()
                    (root / "quality" / "baselines").symlink_to(
                        target, target_is_directory=True
                    )
                calls: list[str] = []

                def runner(*_: object) -> tuple[int, float]:
                    calls.append("called")
                    return 0, 1.0

                with contextlib.redirect_stderr(io.StringIO()):
                    code = main(["--root", str(root)], runner=runner)
                self.assertEqual(code, 2)
                self.assertEqual(calls, [])
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "untouched\n")

    def test_symlinked_leaf_fails_before_runner(self) -> None:
        for name in ("latest.json", "history.jsonl", "test.log", "coverage-gate.log"):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                parent = Path(directory)
                root = parent / "repo"
                init_repo(root)
                managed = root / "quality" / "baselines"
                managed.mkdir(parents=True)
                sentinel = parent / f"{name}.sentinel"
                write(sentinel, "safe\n")
                (managed / name).symlink_to(sentinel)
                called = False

                def runner(*_: object) -> tuple[int, float]:
                    nonlocal called
                    called = True
                    return 0, 1.0

                with contextlib.redirect_stderr(io.StringIO()):
                    code = main(["--root", str(root)], runner=runner)
                self.assertEqual(code, 2)
                self.assertFalse(called)
                self.assertEqual(sentinel.read_text(encoding="utf-8"), "safe\n")

    def test_malformed_history_fails_before_commands_and_preserves_latest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            managed = root / "quality" / "baselines"
            managed.mkdir(parents=True)
            latest = managed / "latest.json"
            write(latest, '{"old":true}\n')
            write(managed / "history.jsonl", "not json\n")
            called = False

            def runner(*_: object) -> tuple[int, float]:
                nonlocal called
                called = True
                return 0, 1.0

            with contextlib.redirect_stderr(io.StringIO()):
                code = main(["--root", str(root)], runner=runner)
            self.assertEqual(code, 2)
            self.assertFalse(called)
            self.assertEqual(latest.read_text(encoding="utf-8"), '{"old":true}\n')

    def test_runner_exit_classification_never_publishes_json(self) -> None:
        for child_exit, expected in ((1, 1), (101, 1), (2, 2)):
            with self.subTest(child_exit=child_exit), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                init_repo(root)

                def runner(_: Path, __: object, log: Path, ___: object) -> tuple[int, float]:
                    write(log, "failed\n")
                    return child_exit, 0.25

                with contextlib.redirect_stderr(io.StringIO()):
                    code = main(["--root", str(root)], runner=runner)
                managed = root / "quality" / "baselines"
                self.assertEqual(code, expected)
                self.assertFalse((managed / "latest.json").exists())
                self.assertFalse((managed / "history.jsonl").exists())

    def test_missing_lcov_after_success_is_setup_error_without_publication(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)

            def runner(_: Path, __: object, log: Path, ___: object) -> tuple[int, float]:
                write(log, "ok\n")
                return 0, 1.0

            with contextlib.redirect_stderr(io.StringIO()):
                code = main(["--root", str(root)], runner=runner)
            managed = root / "quality" / "baselines"
            self.assertEqual(code, 2)
            self.assertFalse((managed / "latest.json").exists())
            self.assertFalse((managed / "history.jsonl").exists())

    def test_success_publishes_exact_schema_atomically_and_deduplicates_sha(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)

            def runner(root_path: Path, command: object, log: Path, _: object) -> tuple[int, float]:
                write(log, "ok\n")
                if "coverage_gate.sh" in " ".join(command):
                    write(root_path / "coverage" / "lcov.info", fresh_lcov(root_path))
                return 0, 3.75

            self.assertEqual(main(["--root", str(root)], runner=runner), 0)
            self.assertEqual(main(["--root", str(root)], runner=runner), 0)
            managed = root / "quality" / "baselines"
            payload = json.loads((managed / "latest.json").read_text(encoding="utf-8"))
            history = (managed / "history.jsonl").read_text(encoding="utf-8").splitlines()
            self.assertEqual(
                set(payload),
                {"timestamp_utc", "git_sha", "runtime_seconds", "repo_metrics", "churn"},
            )
            self.assertEqual(
                set(payload["repo_metrics"]),
                {
                    "production_loc",
                    "test_attribute_count",
                    "tests_per_kloc",
                    "cfg_not_coverage_count",
                    "uncovered_lines",
                    "uncovered_files",
                },
            )
            self.assertEqual(len(history), 1)
            self.assertEqual(list(managed.glob(".*.tmp")), [])

    def test_history_replace_failure_preserves_previous_artifacts_and_cleans_temp(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            managed = root / "quality" / "baselines"
            managed.mkdir(parents=True)
            history = managed / "history.jsonl"
            latest = managed / "latest.json"
            write(history, '{"git_sha":"old"}\n')
            write(latest, '{"old":true}\n')

            def runner(root_path: Path, command: object, log: Path, _: object) -> tuple[int, float]:
                write(log, "ok\n")
                if "coverage_gate.sh" in " ".join(command):
                    write(root_path / "coverage" / "lcov.info", fresh_lcov(root_path))
                return 0, 1.0

            def fail_replace(_: object, __: object) -> None:
                raise OSError("injected replace failure")

            with contextlib.redirect_stderr(io.StringIO()):
                code = main(
                    ["--root", str(root)], runner=runner, replace=fail_replace
                )
            self.assertEqual(code, 2)
            self.assertEqual(history.read_text(encoding="utf-8"), '{"git_sha":"old"}\n')
            self.assertEqual(latest.read_text(encoding="utf-8"), '{"old":true}\n')
            self.assertEqual(list(managed.glob(".*.tmp")), [])

    def test_print_metrics_is_read_only_and_does_not_require_lcov(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                code = main(["--root", str(root), "--print-metrics"])
            payload = json.loads(output.getvalue())
            self.assertEqual(code, 0)
            self.assertIsNone(payload["repo_metrics"]["uncovered_lines"])
            self.assertFalse((root / "quality").exists())


if __name__ == "__main__":
    unittest.main()
