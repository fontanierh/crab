from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from scripts import v2_bundle as bundle_tool
from scripts.v2_bundle import (
    CODEX_ADAPTER_VERSION,
    CODEX_CLI_VERSION,
    MANIFEST_NAME,
    RUNTIME_BINARIES,
    BundleError,
    LaunchdState,
    deploy_service,
    select_output,
    service_paths,
    service_status,
    verify_bundle,
    write_manifest,
)


SOURCE = {
    "repository": "https://example.test/crab.git",
    "commit": "a" * 40,
    "dirty": False,
}


def runtime_health_report(
    *,
    ready: bool = True,
    healthy: bool = True,
    errors: list[str] | None = None,
    needs_action: list[str] | None = None,
    process_id: int = 7001,
) -> dict[str, object]:
    return {
        "schemaVersion": 2,
        "observedAtMs": 42,
        "ready": ready,
        "healthy": healthy,
        "runtime": {
            "expectedConfigurationFingerprint": "a" * 64,
            "loadedConfigurationFingerprint": "a" * 64 if ready else "b" * 64,
            "startedAtMs": 40,
            "processId": process_id,
            "ready": ready,
            "error": None if ready else "configuration does not match",
        },
        "channels": [],
        "bridges": [],
        "errors": errors or [],
        "needsAction": needs_action or [],
    }


def fixture_bundle(parent: Path) -> Path:
    bundle = parent / "bundle"
    (bundle / "bin").mkdir(parents=True)
    (bundle / "bin" / "crab-v2").write_text("runtime\n", encoding="utf-8")
    write_manifest(
        bundle,
        source=SOURCE,
        node="v22.0.0",
        npm="11.0.0",
        rustc="rustc 1.97.1",
    )
    return bundle


def deployable_bundle(parent: Path, commit: str, *, token: bool = False) -> Path:
    bundle = parent / f"bundle-{commit[0]}"
    for directory in ("bin", "agents", "bridges", "libexec", "config"):
        (bundle / directory).mkdir(parents=True)
    (bundle / "bin" / "crab-v2").write_text("runtime\n", encoding="utf-8")
    bridge = bundle / "bin" / "crab-v2-bridge"
    bridge.write_text("#!/bin/sh\nprintf '[]\\n'\n", encoding="utf-8")
    bridge.chmod(0o755)
    environment = ["PATH", "CRAB_TOKEN"] if token else ["PATH"]
    config = {
        "schema": 1,
        "agents": [
            {
                "agentId": "test",
                "environmentFrom": environment,
                "authorityProbe": {"environmentFrom": []},
                "sessionMcpServers": [{"environmentFrom": []}],
            }
        ],
        "channels": [
            {
                "channelId": "primary",
                "workingDirectory": "/absolute/path/to/agent-workspace",
            }
        ],
        "bridges": [{"environmentFrom": []}],
    }
    (bundle / "config" / "runtime.bundle.example.json").write_text(
        json.dumps(config), encoding="utf-8"
    )
    codex_config = json.loads(json.dumps(config))
    codex_config["agents"][0]["agentId"] = "codex"
    (bundle / "config" / "runtime.bundle.codex.example.json").write_text(
        json.dumps(codex_config), encoding="utf-8"
    )
    write_manifest(
        bundle,
        source={**SOURCE, "commit": commit},
        node="v22.0.0",
        npm="11.0.0",
        rustc="rustc 1.97.1",
    )
    return bundle


class FakeLaunchd:
    def __init__(self) -> None:
        self.loaded = False
        self.pid = 7000
        self.starts = 0
        self.stops = 0

    def inspect(self) -> LaunchdState:
        return LaunchdState(self.loaded, self.loaded, self.pid if self.loaded else None)

    def stop(self) -> None:
        self.stops += 1
        self.loaded = False

    def start(self, launch_agent: Path) -> None:
        self.assert_launch_agent(launch_agent)
        self.starts += 1
        self.pid += 1
        self.loaded = True

    @staticmethod
    def assert_launch_agent(path: Path) -> None:
        if not path.is_file():
            raise AssertionError("launch agent was not written before bootstrap")


class BundleVerifierTests(unittest.TestCase):
    @mock.patch(
        "scripts.v2_bundle.subprocess.run",
        side_effect=subprocess.TimeoutExpired(cmd=["health-probe"], timeout=0.25),
    )
    def test_process_timeout_becomes_a_safe_bundle_error(self, _run: mock.Mock) -> None:
        with self.assertRaisesRegex(
            BundleError, "health-probe timed out after 0.25 seconds"
        ):
            bundle_tool.run(
                ("health-probe",),
                cwd=Path("/"),
                capture=True,
                timeout_seconds=0.25,
            )

    def test_manifest_round_trip(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            bundle = fixture_bundle(Path(raw))
            manifest = verify_bundle(bundle)
            self.assertEqual(manifest["source"], SOURCE)
            self.assertEqual(
                [entry["path"] for entry in manifest["files"]],
                ["bin", "bin/crab-v2"],
            )

    def test_permission_changes_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            bundle = fixture_bundle(Path(raw))
            binary = bundle / "bin" / "crab-v2"
            binary.chmod(0o755)
            with self.assertRaises(BundleError):
                verify_bundle(bundle)

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_manifest_itself_cannot_be_a_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = fixture_bundle(parent)
            manifest = bundle / MANIFEST_NAME
            relocated = parent / "relocated-manifest.json"
            manifest.rename(relocated)
            os.symlink(relocated, manifest)
            with self.assertRaises(BundleError):
                verify_bundle(bundle)

    def test_changed_missing_and_extra_entries_are_rejected(self) -> None:
        for mutation in ("changed", "missing", "extra"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as raw:
                bundle = fixture_bundle(Path(raw))
                binary = bundle / "bin" / "crab-v2"
                if mutation == "changed":
                    binary.write_text("changed\n", encoding="utf-8")
                elif mutation == "missing":
                    binary.unlink()
                else:
                    (bundle / "extra").write_text("unexpected\n", encoding="utf-8")
                with self.assertRaises(BundleError):
                    verify_bundle(bundle)

    def test_runtime_health_output_is_strict_and_preserves_ready_vs_healthy(self) -> None:
        waiting_for_auth = runtime_health_report(
            healthy=False,
            needs_action=["authenticate bridge whatsapp"],
        )
        parsed = bundle_tool.parse_runtime_health(json.dumps(waiting_for_auth))
        self.assertTrue(parsed["ready"])
        self.assertFalse(parsed["healthy"])
        self.assertEqual(parsed["needsAction"], ["authenticate bridge whatsapp"])

        invalid_reports = [
            {**waiting_for_auth, "ready": 1},
            {**waiting_for_auth, "healthy": True, "ready": False},
            {**waiting_for_auth, "observedAtMs": True},
            {**waiting_for_auth, "errors": [1]},
            {**waiting_for_auth, "channels": ["not an object"]},
            {**waiting_for_auth, "bridges": [42]},
            {key: value for key, value in waiting_for_auth.items() if key != "channels"},
            {
                **waiting_for_auth,
                "runtime": {
                    **waiting_for_auth["runtime"],
                    "processId": True,
                },
            },
            {
                **waiting_for_auth,
                "runtime": {
                    **waiting_for_auth["runtime"],
                    "loadedConfigurationFingerprint": "b" * 64,
                },
            },
        ]
        for invalid in invalid_reports:
            with self.subTest(invalid=invalid), self.assertRaises(BundleError):
                bundle_tool.parse_runtime_health(json.dumps(invalid))

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_absolute_and_escaping_symlinks_are_rejected(self) -> None:
        for target in ("/private/tmp/outside", "../../outside"):
            with self.subTest(target=target), tempfile.TemporaryDirectory() as raw:
                bundle = Path(raw) / "bundle"
                bundle.mkdir()
                os.symlink(target, bundle / "escape")
                manifest = {
                    "schemaVersion": 1,
                    "bundleName": "crab-v2-runtime",
                    "source": SOURCE,
                    "platform": {"system": "Darwin", "machine": "arm64"},
                    "tools": {"rustc": "rustc", "node": "node", "npm": "npm"},
                    "files": [
                        {"path": "escape", "kind": "symlink", "target": target}
                    ],
                }
                (bundle / MANIFEST_NAME).write_text(
                    json.dumps(manifest), encoding="utf-8"
                )
                with self.assertRaises(BundleError):
                    verify_bundle(bundle)


class EnvironmentFileTests(unittest.TestCase):
    def test_owner_private_dotenv_is_parsed_without_shell_evaluation(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "runtime.env"
            path.write_text(
                "# deployment credentials\n"
                "export CLAUDE_CODE_OAUTH_TOKEN=service-token\n"
                'QUOTED="value with spaces"\n'
                "SINGLE='literal value'\n",
                encoding="utf-8",
            )
            path.chmod(0o600)
            self.assertEqual(
                bundle_tool.load_environment_file(path),
                {
                    "CLAUDE_CODE_OAUTH_TOKEN": "service-token",
                    "QUOTED": "value with spaces",
                    "SINGLE": "literal value",
                },
            )
            self.assertEqual(
                bundle_tool.merged_deployment_environment(
                    {"PATH": "/ambient", "QUOTED": "old"}, path
                ),
                {
                    "PATH": "/ambient",
                    "CLAUDE_CODE_OAUTH_TOKEN": "service-token",
                    "QUOTED": "value with spaces",
                    "SINGLE": "literal value",
                },
            )

    def test_environment_file_rejects_unsafe_shape_and_never_reports_values(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            for name, content, mode in (
                ("duplicate", "TOKEN=secret\nTOKEN=other\n", 0o600),
                ("syntax", "TOKEN=$(unsafe)\n", 0o600),
                ("permissions", "TOKEN=secret\n", 0o644),
            ):
                with self.subTest(name=name):
                    path = parent / name
                    path.write_text(content, encoding="utf-8")
                    path.chmod(mode)
                    with self.assertRaises(BundleError) as raised:
                        bundle_tool.load_environment_file(path)
                    self.assertNotIn("secret", str(raised.exception))
                    self.assertNotIn("other", str(raised.exception))

    @unittest.skipUnless(hasattr(os, "symlink"), "symlinks unavailable")
    def test_environment_file_rejects_a_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            target = parent / "target.env"
            target.write_text("TOKEN=secret\n", encoding="utf-8")
            target.chmod(0o600)
            link = parent / "runtime.env"
            link.symlink_to(target)
            with self.assertRaises(BundleError):
                bundle_tool.load_environment_file(link)


class ServiceDeploymentTests(unittest.TestCase):
    @staticmethod
    def ready(_paths: object, launchd: FakeLaunchd, _timeout: float) -> int:
        state = launchd.inspect()
        if state.pid is None:
            raise BundleError("not running")
        return state.pid

    @mock.patch("scripts.v2_bundle.runtime_processes", return_value=[7000])
    def test_production_readiness_accepts_actionable_health_but_rejects_unready(
        self, _processes: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = fixture_bundle(parent)
            root = parent / "service"
            root.mkdir()
            paths = service_paths(root, launch_agents=parent / "LaunchAgents")
            paths.current.symlink_to(bundle)
            launchd = FakeLaunchd()
            launchd.loaded = True

            pid = bundle_tool.production_readiness(
                paths,
                launchd,
                timeout_seconds=0,
                health=lambda _paths, _timeout: runtime_health_report(
                    healthy=False,
                    needs_action=["authenticate bridge whatsapp"],
                    process_id=7000,
                ),
            )
            self.assertEqual(pid, 7000)

            with self.assertRaisesRegex(
                BundleError, "attestation does not match launchd process"
            ):
                bundle_tool.production_readiness(
                    paths,
                    launchd,
                    timeout_seconds=0,
                    health=lambda _paths, _timeout: runtime_health_report(process_id=9999),
                )

            with self.assertRaisesRegex(
                BundleError, "configured channel primary is missing"
            ):
                bundle_tool.production_readiness(
                    paths,
                    launchd,
                    timeout_seconds=0,
                    health=lambda _paths, _timeout: runtime_health_report(
                        ready=False,
                        healthy=False,
                        errors=["configured channel primary is missing"],
                        process_id=7000,
                    ),
                )

    @mock.patch("scripts.v2_bundle.runtime_processes", return_value=[7000])
    def test_readiness_passes_only_its_remaining_budget_to_health(
        self, _processes: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = fixture_bundle(parent)
            root = parent / "service"
            root.mkdir()
            paths = service_paths(root, launch_agents=parent / "LaunchAgents")
            paths.current.symlink_to(bundle)
            launchd = FakeLaunchd()
            launchd.loaded = True
            budgets: list[float] = []

            def unavailable(_paths: object, timeout_seconds: float) -> dict[str, object]:
                budgets.append(timeout_seconds)
                raise BundleError("health unavailable")

            with self.assertRaisesRegex(
                BundleError, "runtime readiness failed: health unavailable"
            ):
                bundle_tool.production_readiness(
                    paths,
                    launchd,
                    timeout_seconds=0.01,
                    health=unavailable,
                )
            self.assertTrue(budgets)
            self.assertTrue(all(0.0 <= budget <= 0.01 for budget in budgets))
            self.assertEqual(budgets, sorted(budgets, reverse=True))

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_first_install_selects_the_codex_bundle_preset(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = deployable_bundle(parent, "a" * 40)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launch_agents = parent / "LaunchAgents"
            launchd = FakeLaunchd()

            deploy_service(
                bundle,
                root,
                workspace=workspace,
                launchd=launchd,
                agent_preset="codex",
                launch_agents=launch_agents,
                environ={"PATH": "/runtime/bin"},
                readiness=self.ready,
            )

            config = json.loads(
                service_paths(root, launch_agents=launch_agents).config.read_text()
            )
            self.assertEqual(config["agents"][0]["agentId"], "codex")
            stops_before_rejected_update = launchd.stops

            with self.assertRaisesRegex(
                BundleError, "only valid for the first deployment"
            ):
                deploy_service(
                    bundle,
                    root,
                    workspace=None,
                    launchd=launchd,
                    agent_preset="claude-opus",
                    launch_agents=launch_agents,
                    environ={"PATH": "/runtime/bin"},
                    readiness=self.ready,
                )
            self.assertEqual(launchd.stops, stops_before_rejected_update)

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_first_install_creates_one_verified_supervised_layout(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = deployable_bundle(parent, "a" * 40)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launch_agents = parent / "LaunchAgents"
            launchd = FakeLaunchd()

            result = deploy_service(
                bundle,
                root,
                workspace=workspace,
                launchd=launchd,
                launch_agents=launch_agents,
                environ={"PATH": "/runtime/bin"},
                readiness=self.ready,
            )

            paths = service_paths(root, launch_agents=launch_agents)
            self.assertEqual(result["sourceCommit"], "a" * 40)
            self.assertEqual(result["pid"], launchd.pid)
            self.assertEqual(os.readlink(paths.current), result["release"])
            self.assertEqual(verify_bundle(paths.current)["source"]["commit"], "a" * 40)
            for name in ("bin", "agents", "bridges", "libexec"):
                self.assertEqual(os.readlink(root / name), f"current/{name}")
            config = json.loads(paths.config.read_text())
            self.assertEqual(config["agents"][0]["agentId"], "test")
            self.assertEqual(
                config["channels"][0]["workingDirectory"], str(workspace.resolve())
            )
            self.assertEqual(paths.config.stat().st_mode & 0o777, 0o600)
            self.assertEqual(paths.launch_agent.stat().st_mode & 0o777, 0o600)

            import plistlib

            with paths.launch_agent.open("rb") as handle:
                launch_agent = plistlib.load(handle)
            self.assertEqual(launch_agent["Label"], "com.crab.v2.runtime")
            self.assertEqual(
                launch_agent["ProgramArguments"][0],
                str(root.resolve() / "bin" / "crab-v2"),
            )
            self.assertEqual(
                launch_agent["EnvironmentVariables"], {"PATH": "/runtime/bin"}
            )
            self.assertEqual(json.loads(paths.deployment.read_text()), result)

            status_timeouts: list[float] = []
            status = service_status(
                root,
                launchd=launchd,
                launch_agents=launch_agents,
                processes=lambda: [launchd.pid],
                health=lambda _paths, timeout: status_timeouts.append(timeout)
                or runtime_health_report(),
            )
            self.assertTrue(status["healthy"])
            self.assertTrue(status["ipcReady"])
            self.assertTrue(status["topologyReady"])
            self.assertTrue(status["topologyHealthy"])
            self.assertNotIn("EnvironmentVariables", json.dumps(status))
            self.assertEqual(
                status_timeouts, [bundle_tool.STATUS_HEALTH_TIMEOUT_SECONDS]
            )

            degraded = service_status(
                root,
                launchd=launchd,
                launch_agents=launch_agents,
                processes=lambda: [launchd.pid],
                health=lambda _paths, _timeout: runtime_health_report(
                    healthy=False,
                    needs_action=["authenticate bridge whatsapp"],
                ),
            )
            self.assertTrue(degraded["ipcReady"])
            self.assertTrue(degraded["topologyReady"])
            self.assertFalse(degraded["topologyHealthy"])
            self.assertFalse(degraded["healthy"])
            self.assertEqual(
                degraded["needsAction"], ["authenticate bridge whatsapp"]
            )

            wrong_process = service_status(
                root,
                launchd=launchd,
                launch_agents=launch_agents,
                processes=lambda: [launchd.pid],
                health=lambda _paths, _timeout: runtime_health_report(process_id=9999),
            )
            self.assertTrue(wrong_process["ipcReady"])
            self.assertFalse(wrong_process["topologyReady"])
            self.assertFalse(wrong_process["topologyHealthy"])
            self.assertFalse(wrong_process["healthy"])
            self.assertIn(
                "runtime health attestation does not match launchd process",
                wrong_process["errors"],
            )

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_update_preserves_unavailable_credential_environment(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            first = deployable_bundle(parent, "a" * 40, token=True)
            second = deployable_bundle(parent, "b" * 40, token=True)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launch_agents = parent / "LaunchAgents"
            launchd = FakeLaunchd()
            arguments = {
                "root": root,
                "launchd": launchd,
                "launch_agents": launch_agents,
                "readiness": self.ready,
            }
            deploy_service(
                first,
                workspace=workspace,
                environ={"PATH": "/first/bin", "CRAB_TOKEN": "keep-me"},
                **arguments,
            )
            deploy_service(
                second,
                workspace=None,
                environ={"PATH": "/second/bin"},
                **arguments,
            )

            paths = service_paths(root, launch_agents=launch_agents)
            import plistlib

            with paths.launch_agent.open("rb") as handle:
                environment = plistlib.load(handle)["EnvironmentVariables"]
            self.assertEqual(
                environment, {"PATH": "/second/bin", "CRAB_TOKEN": "keep-me"}
            )
            self.assertEqual(verify_bundle(paths.current)["source"]["commit"], "b" * 40)

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_failed_update_rolls_back_release_plist_and_process(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            first = deployable_bundle(parent, "a" * 40)
            second = deployable_bundle(parent, "b" * 40)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launch_agents = parent / "LaunchAgents"
            launchd = FakeLaunchd()
            deploy_service(
                first,
                root,
                workspace=workspace,
                launchd=launchd,
                launch_agents=launch_agents,
                environ={"PATH": "/runtime/bin"},
                readiness=self.ready,
            )
            paths = service_paths(root, launch_agents=launch_agents)
            old_target = os.readlink(paths.current)
            old_plist = paths.launch_agent.read_bytes()

            def fail_new_release(
                probe_paths: object, controller: FakeLaunchd, _timeout: float
            ) -> int:
                assert hasattr(probe_paths, "current")
                if "b" * 40 in os.readlink(probe_paths.current):
                    raise BundleError("new release is not ready")
                return self.ready(probe_paths, controller, _timeout)

            with self.assertRaisesRegex(BundleError, "new release is not ready"):
                deploy_service(
                    second,
                    root,
                    workspace=None,
                    launchd=launchd,
                    launch_agents=launch_agents,
                    environ={"PATH": "/runtime/bin"},
                    readiness=fail_new_release,
                )

            self.assertEqual(os.readlink(paths.current), old_target)
            self.assertEqual(paths.launch_agent.read_bytes(), old_plist)
            self.assertTrue(launchd.inspect().running)
            self.assertEqual(verify_bundle(paths.current)["source"]["commit"], "a" * 40)
            self.assertEqual(
                json.loads(paths.deployment.read_text())["sourceCommit"], "a" * 40
            )

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_failed_first_install_leaves_no_active_release_or_config(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = deployable_bundle(parent, "a" * 40)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launchd = FakeLaunchd()

            def fail(
                _paths: object, _launchd: FakeLaunchd, _timeout: float
            ) -> int:
                raise BundleError("not ready")

            with self.assertRaisesRegex(BundleError, "not ready"):
                deploy_service(
                    bundle,
                    root,
                    workspace=workspace,
                    launchd=launchd,
                    launch_agents=parent / "LaunchAgents",
                    environ={"PATH": "/runtime/bin"},
                    readiness=fail,
                )
            paths = service_paths(root, launch_agents=parent / "LaunchAgents")
            self.assertFalse(paths.current.exists())
            self.assertFalse(paths.config.exists())
            self.assertFalse(paths.launch_agent.exists())
            self.assertFalse(launchd.inspect().loaded)

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_preflight_failure_removes_new_config_without_stopping_launchd(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = deployable_bundle(parent, "a" * 40, token=True)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launchd = FakeLaunchd()
            with self.assertRaisesRegex(BundleError, "CRAB_TOKEN"):
                deploy_service(
                    bundle,
                    root,
                    workspace=workspace,
                    launchd=launchd,
                    launch_agents=parent / "LaunchAgents",
                    environ={"PATH": "/runtime/bin"},
                    readiness=self.ready,
                )
            paths = service_paths(root, launch_agents=parent / "LaunchAgents")
            self.assertFalse(paths.config.exists())
            self.assertEqual(launchd.stops, 0)

    @mock.patch("scripts.v2_bundle.require_runtime_node")
    def test_provenance_write_failure_rolls_back_the_first_install(
        self, _node: mock.Mock
    ) -> None:
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            bundle = deployable_bundle(parent, "a" * 40)
            root = parent / "service"
            workspace = parent / "workspace"
            workspace.mkdir()
            launchd = FakeLaunchd()
            original = bundle_tool.atomic_json

            def fail_provenance(path: Path, value: object, *, mode: int = 0o600) -> None:
                if path.name == "deployment.json":
                    raise BundleError("provenance unavailable")
                original(path, value, mode=mode)

            with mock.patch("scripts.v2_bundle.atomic_json", side_effect=fail_provenance):
                with self.assertRaisesRegex(BundleError, "provenance unavailable"):
                    deploy_service(
                        bundle,
                        root,
                        workspace=workspace,
                        launchd=launchd,
                        launch_agents=parent / "LaunchAgents",
                        environ={"PATH": "/runtime/bin"},
                        readiness=self.ready,
                    )
            paths = service_paths(root, launch_agents=parent / "LaunchAgents")
            self.assertFalse(paths.current.exists())
            self.assertFalse(paths.config.exists())
            self.assertFalse(paths.deployment.exists())
            self.assertFalse(paths.launch_agent.exists())
            self.assertFalse(launchd.inspect().loaded)

    def test_service_root_rejects_relative_home_and_symlink_paths(self) -> None:
        with self.assertRaises(BundleError):
            service_paths(Path("relative"))
        with self.assertRaises(BundleError):
            service_paths(Path.home())
        with tempfile.TemporaryDirectory() as raw:
            parent = Path(raw)
            target = parent / "target"
            target.mkdir()
            link = parent / "link"
            link.symlink_to(target)
            with self.assertRaises(BundleError):
                service_paths(link)


class BundleBuildPolicyTests(unittest.TestCase):
    def test_dirty_build_requires_explicit_development_output(self) -> None:
        root = Path("/repo")
        source = {**SOURCE, "dirty": True}
        with self.assertRaises(BundleError):
            select_output(root, source, None, allow_dirty=True)
        selected = select_output(
            root, source, Path("/private/tmp/dev-bundle"), allow_dirty=True
        )
        self.assertEqual(selected, Path("/private/tmp/dev-bundle"))

    def test_runtime_binary_allowlist_excludes_fixtures(self) -> None:
        self.assertEqual(
            RUNTIME_BINARIES,
            (
                "crab-v2",
                "crab-v2-acp-channel",
                "crab-v2-agent",
                "crab-v2-bridge",
                "crab-v2-bridge-mcp",
                "crab-v2-channel",
                "crab-v2-health",
                "crab-v2-sub-agent",
                "crab-v2-sub-agent-mcp",
                "crab-v2-trigger",
                "crab-v2-claude-authority-probe",
                "crab-v2-codex-authority-probe",
            ),
        )
        self.assertNotIn("acp_fixture", RUNTIME_BINARIES)
        self.assertNotIn("bridge_fixture", RUNTIME_BINARIES)

    def test_agent_packages_and_locks_use_the_exact_adapters(self) -> None:
        root = Path(__file__).resolve().parents[2]
        claude_directory = root / "v2" / "runtime" / "agents" / "claude"
        package = json.loads((claude_directory / "package.json").read_text())
        lock = json.loads((claude_directory / "package-lock.json").read_text())
        self.assertEqual(
            package["dependencies"]["@agentclientprotocol/claude-agent-acp"],
            "0.70.0",
        )
        resolved = lock["packages"][
            "node_modules/@agentclientprotocol/claude-agent-acp"
        ]
        self.assertEqual(resolved["version"], "0.70.0")
        self.assertTrue(resolved["integrity"].startswith("sha512-"))

        codex_directory = root / "v2" / "runtime" / "agents" / "codex"
        package = json.loads((codex_directory / "package.json").read_text())
        lock = json.loads((codex_directory / "package-lock.json").read_text())
        codex_adapter_url = (
            "https://github.com/fontanierh/codex-acp/releases/download/"
            "crab-v1.7.0-2/agentclientprotocol-codex-acp-1.7.0-crab.2.tgz"
        )
        self.assertEqual(
            package["dependencies"]["@agentclientprotocol/codex-acp"],
            codex_adapter_url,
        )
        self.assertEqual(
            package["dependencies"]["@openai/codex"], CODEX_CLI_VERSION
        )
        resolved = lock["packages"]["node_modules/@agentclientprotocol/codex-acp"]
        self.assertEqual(resolved["version"], CODEX_ADAPTER_VERSION)
        self.assertEqual(resolved["resolved"], codex_adapter_url)
        self.assertTrue(resolved["integrity"].startswith("sha512-"))
        codex = lock["packages"]["node_modules/@openai/codex"]
        self.assertEqual(codex["version"], CODEX_CLI_VERSION)
        self.assertTrue(codex["integrity"].startswith("sha512-"))


if __name__ == "__main__":
    unittest.main()
