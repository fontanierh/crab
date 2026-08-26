from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path

from scripts.v2_bundle import (
    MANIFEST_NAME,
    RUNTIME_BINARIES,
    BundleError,
    select_output,
    verify_bundle,
    write_manifest,
)


SOURCE = {
    "repository": "https://example.test/crab.git",
    "commit": "a" * 40,
    "dirty": False,
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


class BundleVerifierTests(unittest.TestCase):
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
                "crab-v2-bridge",
                "crab-v2-bridge-mcp",
                "crab-v2-sub-agent",
                "crab-v2-sub-agent-mcp",
                "crab-v2-trigger",
                "crab-v2-claude-authority-probe",
            ),
        )
        self.assertNotIn("acp_fixture", RUNTIME_BINARIES)
        self.assertNotIn("bridge_fixture", RUNTIME_BINARIES)

    def test_agent_package_and_lock_use_the_exact_adapter(self) -> None:
        root = Path(__file__).resolve().parents[2]
        package_directory = root / "v2" / "runtime" / "agents" / "claude"
        package = json.loads((package_directory / "package.json").read_text())
        lock = json.loads((package_directory / "package-lock.json").read_text())
        self.assertEqual(
            package["dependencies"]["@agentclientprotocol/claude-agent-acp"],
            "0.70.0",
        )
        resolved = lock["packages"][
            "node_modules/@agentclientprotocol/claude-agent-acp"
        ]
        self.assertEqual(resolved["version"], "0.70.0")
        self.assertTrue(resolved["integrity"].startswith("sha512-"))


if __name__ == "__main__":
    unittest.main()
