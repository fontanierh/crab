from __future__ import annotations

import unittest
from pathlib import Path

from scripts.workspace_gate import PlannedCommand, planned_commands, run_commands
from scripts.workflow_common import WorkflowError


class WorkspaceGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[2]

    def test_v2_tests_include_rust_and_every_first_party_bridge_test(self) -> None:
        commands = planned_commands(
            self.root, "tests", include_root=False, include_v2=True
        )
        self.assertEqual(
            [command.label for command in commands],
            [
                "Crab v2 Rust",
                "Crab v2 bridge Node preflight",
                "Crab v2 first-party bridges",
            ],
        )
        rust = " ".join(commands[0].command)
        bridge = commands[2].command
        self.assertIn("--working-directory v2", rust)
        self.assertIn("cargo test --workspace", rust)
        self.assertEqual(bridge[:2], ("node", "--test"))
        self.assertTrue(any(path.endswith("bridge-service.test.js") for path in bridge))

    def test_root_package_selection_does_not_expand_to_v2(self) -> None:
        commands = planned_commands(
            self.root,
            "clippy",
            include_root=True,
            include_v2=False,
            root_packages=("crab-core",),
        )
        self.assertEqual(len(commands), 1)
        rendered = " ".join(commands[0].command)
        self.assertIn("--package crab-core", rendered)
        self.assertNotIn("--working-directory v2", rendered)

    def test_selection_fails_closed_and_execution_stops_on_first_failure(self) -> None:
        with self.assertRaises(WorkflowError):
            planned_commands(
                self.root, "fmt", include_root=False, include_v2=False
            )
        calls: list[str] = []

        def runner(command, working_directory):
            self.assertEqual(working_directory, self.root)
            calls.append(command[0])
            return 1

        result = run_commands(
            self.root,
            [
                PlannedCommand("first", ("first",), self.root),
                PlannedCommand("second", ("second",), self.root),
            ],
            runner,
        )
        self.assertEqual(result, 1)
        self.assertEqual(calls, ["first"])

    def test_format_write_mode_preserves_each_workspace_toolchain_directory(self) -> None:
        commands = planned_commands(
            self.root,
            "fmt",
            include_root=True,
            include_v2=True,
            write=True,
        )
        self.assertEqual(commands[0].command, ("cargo", "fmt", "--all"))
        self.assertEqual(commands[0].working_directory, self.root)
        self.assertEqual(commands[1].command, ("cargo", "fmt", "--all"))
        self.assertEqual(commands[1].working_directory, self.root / "v2")


if __name__ == "__main__":
    unittest.main()
