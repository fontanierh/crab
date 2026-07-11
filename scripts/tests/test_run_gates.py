from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from scripts.run_gates import GateSpec, orchestrate_quality, verify_status
from scripts.tests.helpers import init_repo, write


def spec(name: str, *, skip_reason: str | None = None) -> GateSpec:
    return GateSpec(
        name=name,
        command=("fixture-command", name),
        rerun_command=f"make {name}",
        skip_reason=skip_reason,
    )


def write_log(log_path: Path, content: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(content, encoding="utf-8")


class OrchestratorTests(unittest.TestCase):
    def test_pass_writes_atomic_attestation_with_rerun_and_log_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(root_path: Path, gate: GateSpec, log: Path, verbose: bool) -> tuple[int, float]:
                self.assertFalse(verbose)
                write_log(log, f"passed {gate.name}\n")
                return 0, 0.125

            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=[spec("one"), spec("two")],
                versions_override={"rustc": "fixture", "clippy": "fixture", "cargo_llvm_cov": "fixture"},
            )
            status_path = root / "quality" / "status.json"
            payload = json.loads(status_path.read_text(encoding="utf-8"))
            temporary_files = list(status_path.parent.glob(".status.json.*.tmp"))

        self.assertEqual(code, 0)
        self.assertEqual(payload["result"], "passed")
        self.assertEqual(payload["start_fingerprint"], payload["end_fingerprint"])
        self.assertEqual([item["status"] for item in payload["checks"]], ["passed", "passed"])
        self.assertEqual(payload["checks"][0]["rerun_command"], "make one")
        self.assertTrue(payload["checks"][0]["log_path"].endswith("01-one.log"))
        self.assertEqual(temporary_files, [])

    def test_failure_blocks_later_gate_and_propagates_skip_reason(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, gate: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                write_log(log, "deliberate failure\n")
                return (1 if gate.name == "first" else 0), 0.01

            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=[spec("first"), spec("second")],
                versions_override={},
            )
            payload = json.loads((root / "quality" / "status.json").read_text(encoding="utf-8"))

        self.assertEqual(code, 1)
        self.assertEqual(payload["result"], "failed")
        self.assertEqual(payload["checks"][1]["status"], "skipped")
        self.assertIn("blocked by failed gate first", payload["checks"][1]["reason"])

    def test_environment_exit_two_makes_result_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, __: GateSpec, log: Path, ___: bool) -> tuple[int, float]:
                write_log(log, "environment failure\n")
                return 2, 0.01

            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=[spec("environment")],
                versions_override={},
            )
            payload = json.loads((root / "quality" / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(code, 2)
        self.assertEqual(payload["result"], "invalid")

    def test_explicit_skip_prevents_authoritative_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                specs_override=[spec("required", skip_reason="fixture skip")],
                versions_override={},
            )
            payload = json.loads((root / "quality" / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(code, 1)
        self.assertEqual(payload["result"], "failed")
        self.assertEqual(payload["checks"][0]["reason"], "fixture skip")

    def test_stale_status_is_removed_before_first_gate(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            status = root / "quality" / "status.json"
            write(status, '{"result":"passed","stale":true}\n')

            def executor(root_path: Path, _: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                self.assertFalse((root_path / "quality" / "status.json").exists())
                write_log(log, "ok\n")
                return 0, 0.01

            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=[spec("one")],
                versions_override={},
            )
        self.assertEqual(code, 0)

    def test_edit_during_run_invalidates_otherwise_green_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(root_path: Path, _: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                write_log(log, "ok\n")
                write(root_path / "Cargo.lock", "changed during gate\n")
                return 0, 0.01

            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=[spec("one")],
                versions_override={},
            )
            payload = json.loads((root / "quality" / "status.json").read_text(encoding="utf-8"))
        self.assertEqual(code, 2)
        self.assertEqual(payload["result"], "invalid")
        self.assertNotEqual(payload["start_fingerprint"], payload["end_fingerprint"])

    def test_edit_after_success_is_rejected_by_status_verifier(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, __: GateSpec, log: Path, ___: bool) -> tuple[int, float]:
                write_log(log, "ok\n")
                return 0, 0.01

            self.assertEqual(
                orchestrate_quality(
                    root,
                    mode="worktree",
                    explicit_base=base,
                    executor=executor,
                    specs_override=[spec("one")],
                    versions_override={},
                ),
                0,
            )
            self.assertEqual(verify_status(root), 0)
            write(root / "Cargo.lock", "edited after success\n")
            self.assertEqual(verify_status(root), 2)


if __name__ == "__main__":
    unittest.main()
