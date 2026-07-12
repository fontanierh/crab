from __future__ import annotations

import copy
import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path

from scripts.run_gates import (
    QUALITY_GATE_NAMES,
    STATUS_SCHEMA_VERSION,
    GateSpec,
    orchestrate_quality,
    quality_specs,
    verify_status,
)
from scripts.workflow_common import WorkflowError
from scripts.tests.helpers import init_repo, run_git, write


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


def required_specs() -> list[GateSpec]:
    return [spec(name) for name in QUALITY_GATE_NAMES]


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
        self.assertEqual(payload["schema_version"], STATUS_SCHEMA_VERSION)
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
                    specs_override=required_specs(),
                    versions_override={},
                ),
                0,
            )
            self.assertEqual(verify_status(root), 0)
            write(root / "Cargo.lock", "edited after success\n")
            self.assertEqual(verify_status(root), 2)

    def test_quality_gate_set_and_normal_configuration_test_command_are_fixed(self) -> None:
        root = Path("/fixture/repo")
        specs = quality_specs(root, "worktree", "base")
        self.assertEqual(tuple(item.name for item in specs), QUALITY_GATE_NAMES)
        tests = next(item for item in specs if item.name == "tests")
        self.assertIn("cargo_target.py", " ".join(tests.command))
        self.assertIn("cargo test --workspace", " ".join(tests.command))

    def test_coverage_failure_propagates_to_quality_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, gate: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                write_log(log, f"{gate.name}\n")
                return (1 if gate.name == "coverage" else 0), 0.01

            code = orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=required_specs(),
                versions_override={},
            )
            payload = json.loads((root / "quality" / "status.json").read_text())
        self.assertEqual(code, 1)
        coverage = next(item for item in payload["checks"] if item["name"] == "coverage")
        self.assertEqual(coverage["status"], "failed")
        self.assertEqual(coverage["exit_code"], 1)

    def test_verify_status_rejects_malformed_passing_artifacts_without_traceback(self) -> None:
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
                    specs_override=required_specs(),
                    versions_override={},
                ),
                0,
            )
            status_path = root / "quality" / "status.json"
            valid = json.loads(status_path.read_text(encoding="utf-8"))
            cases: dict[str, object] = {}
            schema_one = copy.deepcopy(valid)
            schema_one["schema_version"] = 1
            cases["schema-v1"] = schema_one
            empty = copy.deepcopy(valid)
            empty["checks"] = []
            cases["empty"] = empty
            missing = copy.deepcopy(valid)
            missing["checks"] = missing["checks"][:-1]
            cases["missing"] = missing
            duplicate = copy.deepcopy(valid)
            duplicate["checks"][-1] = copy.deepcopy(duplicate["checks"][0])
            cases["duplicate"] = duplicate
            extra = copy.deepcopy(valid)
            extra["checks"].append(copy.deepcopy(extra["checks"][0]))
            cases["extra"] = extra
            failed = copy.deepcopy(valid)
            failed["checks"][0]["status"] = "failed"
            failed["checks"][0]["exit_code"] = 1
            cases["failed-check"] = failed
            non_list = copy.deepcopy(valid)
            non_list["checks"] = {}
            cases["non-list"] = non_list
            non_dict = copy.deepcopy(valid)
            non_dict["checks"][0] = "bad"
            cases["non-dict"] = non_dict
            wrong_fingerprint_shape = copy.deepcopy(valid)
            wrong_fingerprint_shape["start_fingerprint"]["entry_count"] += 1
            wrong_fingerprint_shape["end_fingerprint"]["entry_count"] += 1
            cases["wrong-fingerprint-shape"] = wrong_fingerprint_shape

            for name, payload in cases.items():
                with self.subTest(name=name):
                    status_path.write_text(json.dumps(payload), encoding="utf-8")
                    stderr = io.StringIO()
                    with contextlib.redirect_stderr(stderr):
                        self.assertEqual(verify_status(root), 2)
                    self.assertNotIn("Traceback", stderr.getvalue())

    def test_staging_exact_validated_content_preserves_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "Cargo.lock", "validated dirty content\n")

            def executor(_: Path, __: GateSpec, log: Path, ___: bool) -> tuple[int, float]:
                write_log(log, "ok\n")
                return 0, 0.01

            self.assertEqual(
                orchestrate_quality(
                    root,
                    mode="worktree",
                    explicit_base=base,
                    executor=executor,
                    specs_override=required_specs(),
                    versions_override={},
                ),
                0,
            )
            run_git(root, "add", "-A")
            self.assertEqual(verify_status(root), 0)

    def test_quality_status_rejects_post_validation_content_mode_and_path_changes(self) -> None:
        mutations = {
            "staged-only": lambda root: (
                write(root / "Cargo.lock", "staged content\n"),
                run_git(root, "add", "Cargo.lock"),
                write(root / "Cargo.lock", "# deterministic fixture\n"),
            ),
            "mode": lambda root: os.chmod(root / "Cargo.lock", 0o755),
            "rename": lambda root: run_git(root, "mv", "Cargo.lock", "Cargo.lock.moved"),
            "untracked": lambda root: write(root / "new.txt", "new\n"),
        }
        for name, mutate in mutations.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
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
                        specs_override=required_specs(),
                        versions_override={},
                    ),
                    0,
                )
                mutate(root)
                self.assertEqual(verify_status(root), 2)

    def test_mode_change_on_already_dirty_validated_file_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            lockfile = root / "Cargo.lock"
            write(lockfile, "dirty before validation\n")

            def executor(_: Path, __: GateSpec, log: Path, ___: bool) -> tuple[int, float]:
                write_log(log, "ok\n")
                return 0, 0.01

            self.assertEqual(
                orchestrate_quality(
                    root,
                    mode="worktree",
                    explicit_base=base,
                    executor=executor,
                    specs_override=required_specs(),
                    versions_override={},
                ),
                0,
            )
            os.chmod(lockfile, 0o755)
            self.assertEqual(verify_status(root), 2)

    def test_quality_directory_symlink_is_rejected_without_touching_target(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            base = init_repo(root)
            external = parent / "external"
            external.mkdir()
            sentinel = external / "status.json"
            write(sentinel, "sentinel\n")
            (root / "quality").symlink_to(external, target_is_directory=True)
            with self.assertRaisesRegex(WorkflowError, "escapes|real directory"):
                orchestrate_quality(
                    root,
                    mode="worktree",
                    explicit_base=base,
                    specs_override=required_specs(),
                    versions_override={},
                )
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "sentinel\n")


if __name__ == "__main__":
    unittest.main()
