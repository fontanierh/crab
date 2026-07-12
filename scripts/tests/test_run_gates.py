from __future__ import annotations

import copy
import contextlib
import io
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_gates import (
    QUALITY_GATE_NAMES,
    STATUS_SCHEMA_VERSION,
    GateSpec,
    main as run_gates_main,
    orchestrate_quality,
    quality_specs,
    run_check,
    run_specs,
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
    def test_quality_status_rejects_boolean_float_and_nonfinite_type_tampering(self) -> None:
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
            path = root / "quality/status.json"
            original = json.loads(path.read_text(encoding="utf-8"))
            mutations = (
                lambda value: value.__setitem__("schema_version", True),
                lambda value: value.__setitem__("schema_version", 3.0),
                lambda value: value.__setitem__("dirty", 1),
                lambda value: (
                    value["start_fingerprint"].__setitem__("dirty", 1),
                    value["end_fingerprint"].__setitem__("dirty", 1),
                ),
                lambda value: value["end_fingerprint"].__setitem__("entry_count", True),
                lambda value: value["checks"][0].__setitem__("exit_code", False),
                lambda value: value["checks"][0].__setitem__("exit_code", 0.0),
                lambda value: value["checks"][0].__setitem__("duration_seconds", True),
                lambda value: value["checks"][0].__setitem__("duration_seconds", float("nan")),
                lambda value: value["checks"][0].__setitem__("duration_seconds", float("inf")),
                lambda value: value["checks"][0].__setitem__("duration_seconds", -1),
                lambda value: value.__setitem__("result", 1),
            )
            for mutate in mutations:
                payload = copy.deepcopy(original)
                mutate(payload)
                path.write_text(json.dumps(payload), encoding="utf-8")
                with contextlib.redirect_stderr(io.StringIO()):
                    self.assertEqual(verify_status(root), 2)
            failed = copy.deepcopy(original)
            failed["result"] = "failed"
            failed["checks"][0]["status"] = "failed"
            failed["checks"][0]["exit_code"] = 1.0
            path.write_text(json.dumps(failed), encoding="utf-8")
            with contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(verify_status(root), 2)
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

    def test_missing_baseline_fallback_runs_full_workspace_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            run_git(root, "update-ref", "-d", "refs/remotes/origin/main")
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = run_gates_main(
                    ["--root", str(root), "check", "--dry-run"]
                )
            rendered = stdout.getvalue()
        self.assertEqual(code, 0)
        self.assertIn("check: fallback:", rendered)
        self.assertIn("check: run fmt", rendered)
        self.assertIn("check: run clippy", rendered)
        self.assertIn("check: run tests", rendered)
        self.assertIn("--workspace", rendered)
        self.assertNotIn("check: skip", rendered)

    def test_injected_diff_failure_runs_full_workspace_in_dry_and_real_modes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            real_run = subprocess.run

            def git_boundary(command: object, *args: object, **kwargs: object):
                if isinstance(command, list) and "diff" in command:
                    return subprocess.CompletedProcess(
                        command, 128, b"", b"fatal: injected diff failure\n"
                    )
                return real_run(command, *args, **kwargs)

            stdout = io.StringIO()
            with (
                patch("scripts.changed_scope.subprocess.run", side_effect=git_boundary),
                contextlib.redirect_stdout(stdout),
            ):
                dry_code = run_gates_main(
                    ["--root", str(root), "check", "--base-sha", base, "--dry-run"]
                )
            executed: list[GateSpec] = []

            def executor(_: Path, gate: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                executed.append(gate)
                write_log(log, "ok\n")
                return 0, 0.01

            with patch("scripts.changed_scope.subprocess.run", side_effect=git_boundary):
                real_code = run_check(
                    root, "worktree", base, False, executor=executor
                )
            rendered = stdout.getvalue()
        self.assertEqual((dry_code, real_code), (0, 0))
        self.assertIn("check: fallback:", rendered)
        self.assertEqual([item.name for item in executed], ["fmt", "clippy", "tests"])
        self.assertTrue(all(item.skip_reason is None for item in executed))
        self.assertIn("--workspace", executed[1].command)
        self.assertIn("--workspace", executed[2].command)

    def test_resolved_clean_baseline_still_skips_changed_scope_checks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                code = run_gates_main(
                    ["--root", str(root), "check", "--base-sha", base, "--dry-run"]
                )
            self.assertEqual(code, 0)
            self.assertEqual(stdout.getvalue().count("check: skip"), 3)

    def test_partial_staging_at_quality_start_fails_before_status_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "Cargo.lock", "staged\n")
            write(root / "Cargo.toml", "unstaged\n")
            run_git(root, "add", "Cargo.lock")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = run_gates_main(
                    ["--root", str(root), "quality", "--base-sha", base]
                )
            self.assertEqual(code, 2)
            self.assertIn("entirely HEAD", stderr.getvalue())
            self.assertFalse((root / "quality" / "status.json").exists())

    def test_post_pass_partial_stage_partial_reset_and_intent_to_add_are_rejected(self) -> None:
        scenarios = ("partial-stage", "partial-reset", "intent-to-add")
        for scenario in scenarios:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                base = init_repo(root)
                write(root / "Cargo.lock", "dirty lock\n")
                write(root / "Cargo.toml", "dirty manifest\n")
                if scenario == "intent-to-add":
                    write(root / "new.txt", "new\n")

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
                if scenario == "partial-stage":
                    run_git(root, "add", "Cargo.lock")
                elif scenario == "partial-reset":
                    run_git(root, "add", "-A")
                    self.assertEqual(verify_status(root), 0)
                    run_git(root, "reset", "--", "Cargo.lock")
                else:
                    run_git(root, "add", "-N", "new.txt")
                self.assertEqual(verify_status(root), 2)

    def test_complete_multifile_staging_preserves_attestation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)
            write(root / "Cargo.lock", "dirty lock\n")
            write(root / "Cargo.toml", "dirty manifest\n")
            write(root / "new.txt", "new\n")

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

    def test_owner_execute_mode_attestation_matches_git(self) -> None:
        for initial, changed, expected in ((0o775, 0o675, 2), (0o644, 0o654, 0)):
            with self.subTest(initial=oct(initial), changed=oct(changed)), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                base = init_repo(root)
                lockfile = root / "Cargo.lock"
                write(lockfile, "dirty\n")
                os.chmod(lockfile, initial)

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
                os.chmod(lockfile, changed)
                self.assertEqual(verify_status(root), expected)

    def test_log_root_symlinks_fail_before_quality_or_check_side_effects(self) -> None:
        for entry in ("quality", "logs"):
            for internal in (False, True):
                for command in ("quality", "check"):
                    with self.subTest(entry=entry, internal=internal, command=command), tempfile.TemporaryDirectory() as directory:
                        parent = Path(directory)
                        root = parent / "repo"
                        base = init_repo(root)
                        write(root / "Cargo.lock", "changed\n")
                        target = root / "internal" if internal else parent / "external"
                        target.mkdir()
                        sentinel = target / "sentinel"
                        write(sentinel, "safe\n")
                        status = root / "quality" / "status.json"
                        if entry == "quality":
                            (root / "quality").symlink_to(target, target_is_directory=True)
                            stale_expected = False
                        else:
                            (root / "quality").mkdir()
                            write(status, '{"stale":true}\n')
                            (root / "quality" / "logs").symlink_to(
                                target, target_is_directory=True
                            )
                            stale_expected = True
                        calls: list[str] = []

                        def executor(_: Path, gate: GateSpec, __: Path, ___: bool) -> tuple[int, float]:
                            calls.append(gate.name)
                            return 0, 0.01

                        with self.assertRaises(WorkflowError):
                            if command == "quality":
                                orchestrate_quality(
                                    root,
                                    mode="worktree",
                                    explicit_base=base,
                                    executor=executor,
                                    specs_override=required_specs(),
                                    versions_override={},
                                )
                            else:
                                run_check(
                                    root,
                                    "worktree",
                                    base,
                                    False,
                                    executor=executor,
                                )
                        self.assertEqual(calls, [])
                        self.assertEqual(sentinel.read_text(encoding="utf-8"), "safe\n")
                        if stale_expected:
                            self.assertEqual(status.read_text(encoding="utf-8"), '{"stale":true}\n')

    def test_every_planned_log_is_validated_before_first_gate_executes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            init_repo(root)
            logs = root / "quality" / "logs" / "fixed"
            logs.mkdir(parents=True)
            sentinel = root / "sentinel"
            write(sentinel, "safe\n")
            (logs / "02-second.log").symlink_to(sentinel)
            calls: list[str] = []

            def executor(_: Path, gate: GateSpec, __: Path, ___: bool) -> tuple[int, float]:
                calls.append(gate.name)
                return 0, 0.01

            with self.assertRaises(WorkflowError):
                run_specs(
                    root,
                    [spec("first"), spec("second")],
                    logs,
                    executor=executor,
                )
            self.assertEqual(calls, [])
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "safe\n")

    def test_log_preflight_precedes_missing_baseline_setup_and_status_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            init_repo(root)
            run_git(root, "update-ref", "-d", "refs/remotes/origin/main")
            (root / "quality").mkdir()
            external = parent / "external"
            external.mkdir()
            (root / "quality" / "logs").symlink_to(external, target_is_directory=True)
            with self.assertRaises(WorkflowError):
                orchestrate_quality(
                    root,
                    mode="worktree",
                    explicit_base=None,
                    specs_override=required_specs(),
                    versions_override={},
                )
            self.assertFalse((external / "status.json").exists())
            self.assertFalse((root / "quality" / "status.json").exists())

    def test_check_main_reports_log_symlink_as_exit_two_even_in_dry_run(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            root = parent / "repo"
            base = init_repo(root)
            (root / "quality").mkdir()
            external = parent / "external"
            external.mkdir()
            sentinel = external / "sentinel"
            write(sentinel, "safe\n")
            (root / "quality" / "logs").symlink_to(external, target_is_directory=True)
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                code = run_gates_main(
                    [
                        "--root",
                        str(root),
                        "check",
                        "--base-sha",
                        base,
                        "--dry-run",
                    ]
                )
            self.assertEqual(code, 2)
            self.assertIn("environment error", stderr.getvalue())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "safe\n")

    def test_verify_status_exit_codes_distinguish_failed_from_invalid_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, gate: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                write_log(log, f"{gate.name}\n")
                return (101 if gate.name == "tests" else 0), 0.01

            self.assertEqual(
                orchestrate_quality(
                    root,
                    mode="worktree",
                    explicit_base=base,
                    executor=executor,
                    specs_override=required_specs(),
                    versions_override={},
                ),
                1,
            )
            self.assertEqual(verify_status(root), 1)
            write(root / "Cargo.lock", "tree changed\n")
            self.assertEqual(verify_status(root), 2)

    def test_failed_artifact_tampering_is_malformed_not_a_gate_failure(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, gate: GateSpec, log: Path, __: bool) -> tuple[int, float]:
                write_log(log, f"{gate.name}\n")
                return (1 if gate.name == "tests" else 0), 0.01

            orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=required_specs(),
                versions_override={},
            )
            status = root / "quality" / "status.json"
            valid = json.loads(status.read_text(encoding="utf-8"))
            cases: dict[str, object] = {}
            all_skipped = copy.deepcopy(valid)
            for check in all_skipped["checks"]:
                check["status"] = "skipped"
                check["exit_code"] = None
            cases["all-skipped"] = all_skipped
            two_failed = copy.deepcopy(valid)
            two_failed["checks"][3].update(
                status="failed", exit_code=1, log_path="quality/logs/second.log"
            )
            cases["two-failed"] = two_failed
            for exit_code in (0, 2):
                item = copy.deepcopy(valid)
                item["checks"][2]["exit_code"] = exit_code
                cases[f"failed-exit-{exit_code}"] = item
            passed_nonzero = copy.deepcopy(valid)
            passed_nonzero["checks"][0]["exit_code"] = 1
            cases["passed-nonzero"] = passed_nonzero
            skipped_nonnull = copy.deepcopy(valid)
            skipped_nonnull["checks"][3]["exit_code"] = 1
            cases["skipped-nonnull"] = skipped_nonnull
            unequal = copy.deepcopy(valid)
            unequal["start_fingerprint"]["sha256"] = "different"
            cases["unequal-fingerprints"] = unequal
            missing_fingerprint = copy.deepcopy(valid)
            del missing_fingerprint["end_fingerprint"]
            cases["missing-fingerprint"] = missing_fingerprint
            for name, payload in cases.items():
                with self.subTest(name=name):
                    status.write_text(json.dumps(payload), encoding="utf-8")
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(verify_status(root), 2)

    def test_invalid_unknown_missing_setup_and_non_utf8_status_are_exit_two(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            base = init_repo(root)

            def executor(_: Path, __: GateSpec, log: Path, ___: bool) -> tuple[int, float]:
                write_log(log, "ok\n")
                return 0, 0.01

            orchestrate_quality(
                root,
                mode="worktree",
                explicit_base=base,
                executor=executor,
                specs_override=required_specs(),
                versions_override={},
            )
            status = root / "quality" / "status.json"
            valid = json.loads(status.read_text(encoding="utf-8"))
            cases: dict[str, object] = {}
            for result in ("invalid", "unknown", 42):
                item = copy.deepcopy(valid)
                item["result"] = result
                cases[f"result-{result}"] = item
            missing_result = copy.deepcopy(valid)
            del missing_result["result"]
            cases["missing-result"] = missing_result
            missing_setup = copy.deepcopy(valid)
            del missing_setup["setup_error"]
            cases["missing-setup"] = missing_setup
            for name, payload in cases.items():
                with self.subTest(name=name):
                    status.write_text(json.dumps(payload), encoding="utf-8")
                    with contextlib.redirect_stderr(io.StringIO()):
                        self.assertEqual(verify_status(root), 2)
            status.write_bytes(b"\xff\xfebroken")
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                self.assertEqual(verify_status(root), 2)
            self.assertNotIn("Traceback", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
