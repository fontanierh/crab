from __future__ import annotations

import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.cargo_target import main, parse_args
from scripts.workflow_common import repository_namespace
from scripts.tests.helpers import init_repo


class CargoTargetTests(unittest.TestCase):
    def environment(self, **values: str) -> dict[str, str]:
        return {"PATH": os.environ.get("PATH", ""), **values}

    def test_dry_run_is_non_mutating_in_both_modes_and_argument_orders(self) -> None:
        for mode in ("build", "coverage"):
            for order in ("before", "after"):
                with (
                    self.subTest(mode=mode, order=order),
                    tempfile.TemporaryDirectory() as directory,
                ):
                    parent = Path(directory)
                    root = parent / "repo"
                    init_repo(root)
                    shared = parent / "shared"
                    shared.mkdir()
                    arguments = ["--root", str(root)]
                    if order == "before":
                        arguments.extend(("--dry-run", mode))
                    else:
                        arguments.extend((mode, "--dry-run"))
                    arguments.extend(("--", "/usr/bin/true"))
                    output = io.StringIO()
                    with (
                        patch.dict(
                            os.environ,
                            self.environment(CRAB_SHARED_TARGET_DIR=str(shared)),
                            clear=True,
                        ),
                        contextlib.redirect_stdout(output),
                    ):
                        code = main(arguments)
                    self.assertEqual(code, 0)
                    self.assertFalse((root / "target").exists())
                    self.assertFalse((shared / repository_namespace(root)).exists())
                    self.assertIn("CARGO_TARGET_DIR=", output.getvalue())

    def test_dry_run_still_rejects_invalid_shared_base(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "repo"
            init_repo(root)
            with patch.dict(
                os.environ,
                self.environment(CRAB_SHARED_TARGET_DIR=str(root / "inside")),
                clear=True,
            ):
                code = main(
                    ["--root", str(root), "build", "--dry-run", "--", "/usr/bin/true"]
                )
        self.assertEqual(code, 2)

    def test_real_run_creates_validated_shared_namespace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            shared = parent / "shared"
            shared.mkdir()
            with patch.dict(
                os.environ,
                self.environment(CRAB_SHARED_TARGET_DIR=str(shared)),
                clear=True,
            ):
                code = main(["--root", str(root), "build", "--", "/usr/bin/true"])
            namespace = shared / repository_namespace(root)
            self.assertEqual(code, 0)
            self.assertTrue(namespace.is_dir())

    def test_missing_separator_is_a_usage_error(self) -> None:
        with self.assertRaises(SystemExit) as raised:
            parse_args(["build", "/usr/bin/true"])
        self.assertEqual(raised.exception.code, 2)

    def test_target_symlink_is_rejected_even_in_coverage_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            external = parent / "external"
            external.mkdir()
            (root / "target").symlink_to(external, target_is_directory=True)
            with patch.dict(os.environ, self.environment(), clear=True):
                code = main(
                    ["--root", str(root), "coverage", "--dry-run", "--", "/usr/bin/true"]
                )
        self.assertEqual(code, 2)

    def test_shared_namespace_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            shared = parent / "shared"
            shared.mkdir()
            inside = root / "inside"
            inside.mkdir()
            (shared / repository_namespace(root)).symlink_to(
                inside, target_is_directory=True
            )
            with patch.dict(
                os.environ,
                self.environment(CRAB_SHARED_TARGET_DIR=str(shared)),
                clear=True,
            ):
                code = main(
                    ["--root", str(root), "build", "--dry-run", "--", "/usr/bin/true"]
                )
        self.assertEqual(code, 2)

    def test_external_ambient_target_is_rejected_but_local_target_is_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            external = parent / "external"
            with patch.dict(
                os.environ,
                self.environment(CARGO_TARGET_DIR=str(external)),
                clear=True,
            ):
                rejected = main(
                    ["--root", str(root), "build", "--dry-run", "--", "/usr/bin/true"]
                )
            with patch.dict(
                os.environ,
                self.environment(CARGO_TARGET_DIR=str(root / "target" / "custom")),
                clear=True,
            ):
                accepted = main(
                    ["--root", str(root), "build", "--dry-run", "--", "/usr/bin/true"]
                )
        self.assertEqual(rejected, 2)
        self.assertEqual(accepted, 0)


if __name__ == "__main__":
    unittest.main()
