from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path

from scripts.clippy_policy import CLIPPY_POLICY_ARGS
from scripts.coverage_workflow import FUNCTION_THRESHOLD, LINE_THRESHOLD, REGION_THRESHOLD
from scripts.doctor import JSCPD_VERSION, LLVM_COV_VERSION
from scripts.run_gates import QUALITY_GATE_NAMES


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = Path(__file__).parent / "fixtures" / "lint-policy"


class LintPolicyFixtureTests(unittest.TestCase):
    def fixture_toolchain(self, pin: str) -> str:
        installed = subprocess.run(
            ["rustup", "toolchain", "list"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        if installed.returncode == 0 and any(
            line.split()[0] == pin or line.split()[0].startswith(f"{pin}-")
            for line in installed.stdout.splitlines()
            if line.split()
        ):
            return pin
        stable = subprocess.run(
            ["rustc", "+stable", "-V"],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(stable.returncode, 0, stable.stderr)
        self.assertRegex(stable.stdout, rf"^rustc {re.escape(pin)}\b")
        return "stable"

    def run_case(self, case: str) -> subprocess.CompletedProcess[str]:
        pin = tomllib.loads((ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"))[
            "toolchain"
        ]["channel"]
        toolchain = self.fixture_toolchain(pin)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        fixture_root = Path(temporary.name) / "fixture"
        fixture_root.mkdir()
        shutil.copy2(FIXTURE / "Cargo.toml", fixture_root / "Cargo.toml")
        (fixture_root / "src").mkdir()
        shutil.copy2(FIXTURE / "cases" / f"{case}.rs", fixture_root / "src" / "lib.rs")
        environment = dict(os.environ)
        environment["CARGO_TARGET_DIR"] = str(Path(temporary.name) / "target")
        environment["CARGO_NET_OFFLINE"] = "true"
        environment["CARGO_TERM_COLOR"] = "never"
        return subprocess.run(
            [
                "cargo",
                f"+{toolchain}",
                "clippy",
                "--offline",
                "--",
                *CLIPPY_POLICY_ARGS,
            ],
            cwd=fixture_root,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )

    def test_style_and_complexity_are_visible_but_nonfatal(self) -> None:
        result = self.run_case("style_complexity")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("warning:", result.stdout)
        self.assertRegex(result.stdout, r"clippy::(bool-comparison|needless-bool|needless-return)")

    def test_correctness_is_fatal(self) -> None:
        result = self.run_case("correctness")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("clippy::erasing_op", result.stdout)

    def test_suspicious_is_fatal(self) -> None:
        result = self.run_case("suspicious")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("clippy::almost_swapped", result.stdout)

    def test_perf_is_fatal(self) -> None:
        result = self.run_case("perf")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("clippy::manual_memcpy", result.stdout)

    def test_rustc_warning_is_fatal(self) -> None:
        result = self.run_case("rust_warning")
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("unused variable", result.stdout)


class RepositoryPolicyTests(unittest.TestCase):
    def test_bare_make_is_read_only_help(self) -> None:
        before = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout
        result = subprocess.run(
            ["make"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
        )
        after = subprocess.run(
            ["git", "status", "--porcelain=v1", "--untracked-files=all"],
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        ).stdout
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(before, after)
        self.assertIn("make doctor", result.stdout)
        self.assertIn("make check", result.stdout)
        self.assertIn("make quality", result.stdout)

    def test_every_workspace_member_inherits_workspace_lints(self) -> None:
        workspace = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
        for member in workspace["workspace"]["members"]:
            with self.subTest(member=member):
                manifest = tomllib.loads(
                    (ROOT / member / "Cargo.toml").read_text(encoding="utf-8")
                )
                self.assertTrue(manifest.get("lints", {}).get("workspace"))

    def test_ci_toolchain_pin_matches_rust_toolchain_file(self) -> None:
        pin = tomllib.loads((ROOT / "rust-toolchain.toml").read_text(encoding="utf-8"))[
            "toolchain"
        ]["channel"]
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        references = re.findall(r"toolchain:\s*[\"']?([^\s\"']+)", workflow)
        self.assertTrue(references, "CI must state its Rust toolchain explicitly")
        self.assertEqual(set(references), {pin})

    def test_ci_actions_runner_and_node_are_immutable(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        uses_lines = re.findall(r"^\s*uses:\s*(.+)$", workflow, flags=re.MULTILINE)
        self.assertTrue(uses_lines)
        for line in uses_lines:
            with self.subTest(line=line):
                self.assertRegex(line, r"^[^@\s]+@[0-9a-f]{40}\s+#\s+\S+")
        runners = re.findall(r"^\s*runs-on:\s*(\S+)", workflow, flags=re.MULTILINE)
        self.assertEqual(runners, ["ubuntu-24.04", "ubuntu-24.04"])
        node_versions = re.findall(r"node-version:\s*[\"']?(\d+\.\d+\.\d+)", workflow)
        self.assertEqual(node_versions, ["20.20.2"])

    def test_ci_pinned_tool_literals_match_repository_policy(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        duplication = (ROOT / "scripts" / "duplication_check.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn(f"jscpd@{JSCPD_VERSION}", workflow)
        self.assertIn(f"jscpd {JSCPD_VERSION}", duplication)
        self.assertIn(f"cargo-llvm-cov@{LLVM_COV_VERSION}", workflow)

    def test_ci_docs_classifier_precedes_every_setup_step(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        job_sections = workflow.split("      - name: Checkout")[1:]
        self.assertEqual(len(job_sections), 2)
        for section in job_sections:
            classifier = section.index("- name: Classify changed paths")
            setup = section.index("- name: Setup Rust toolchain")
            self.assertLess(classifier, setup)

    def test_ci_declares_read_only_contents_and_checkout_drops_credentials(self) -> None:
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        permission = re.search(r"(?m)^permissions:\n((?:  .+\n)+)\njobs:", workflow)
        self.assertIsNotNone(permission)
        self.assertEqual(permission.group(1), "  contents: read\n")
        checkout_sections = workflow.split(
            "uses: actions/checkout@08eba0b27e820071cde6df949e0beb9ba4906955"
        )[1:]
        self.assertEqual(len(checkout_sections), 2)
        for section in checkout_sections:
            checkout_step = section.split("\n      - name:", 1)[0]
            self.assertEqual(checkout_step.count("persist-credentials: false"), 1)
            self.assertIn("fetch-depth: 0", checkout_step)
        self.assertEqual(workflow.count("persist-credentials: false"), 2)

    def test_agents_ci_section_matches_authoritative_gate_order_once(self) -> None:
        agents = (ROOT / "AGENTS.md").read_text(encoding="utf-8")
        section = agents.split("## 6. CI Gates (Must All Pass)", 1)[1].split(
            "## 7.", 1
        )[0]
        names = tuple(
            re.findall(r"(?m)^\d+\. `([^`]+)`", section)
            or re.findall(r"(?m)^- `([^`]+)`", section)
        )
        self.assertEqual(names, QUALITY_GATE_NAMES)
        for name in QUALITY_GATE_NAMES:
            self.assertEqual(names.count(name), 1)

    def test_documented_threshold_literals_match_runtime_policy(self) -> None:
        expected = (
            f"{FUNCTION_THRESHOLD}%",
            f"{REGION_THRESHOLD}%",
            f"{LINE_THRESHOLD}%",
        )
        paths = (
            "AGENTS.md",
            "CLAUDE.md",
            "CONTRIBUTING.md",
            ".github/pull_request_template.md",
            "docs/agent-workflow.md",
            "crab/DESIGN.md",
            "crab/docs/16-code-factory.md",
            "scripts/gen_code_quality_report.sh",
            "CODE_QUALITY_REPORT.md",
            "quality/WORKFLOW_IMPLEMENTATION_REPORT.md",
        )
        for relative in paths:
            text = (ROOT / relative).read_text(encoding="utf-8")
            with self.subTest(path=relative):
                for literal in expected:
                    self.assertIn(literal, text)

    def test_coverage_entry_points_use_the_local_target_wrapper(self) -> None:
        workflow = (ROOT / "scripts" / "coverage_workflow.py").read_text(encoding="utf-8")
        diagnostics = (ROOT / "scripts" / "coverage_diagnostics.sh").read_text(encoding="utf-8")
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        self.assertIn("cargo_target.py", workflow)
        self.assertIn("coverage_workflow.py", diagnostics)
        self.assertNotIn("cargo llvm-cov", makefile)
        self.assertIn(
            '["cargo-llvm-cov", "llvm-cov", "--version"]',
            (ROOT / "scripts" / "run_gates.py").read_text(encoding="utf-8"),
        )

    def test_make_clippy_uses_the_ordered_policy_wrapper(self) -> None:
        makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
        orchestrator = (ROOT / "scripts" / "run_gates.py").read_text(encoding="utf-8")
        self.assertIn("scripts/clippy_policy.py", makefile)
        self.assertIn("clippy_policy.py", orchestrator)
        self.assertIn(
            "scripts/clippy_policy.py",
            (ROOT / "Cargo.toml").read_text(encoding="utf-8"),
        )

    def test_report_fallback_text_cannot_execute_markdown_backticks(self) -> None:
        generator = (ROOT / "scripts" / "gen_code_quality_report.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("baseline_latest_md='No recorded baseline", generator)
        self.assertIn("baseline_trend_md='No baseline trend", generator)

    def test_duplication_gate_never_installs_implicitly(self) -> None:
        gate = (ROOT / "scripts" / "duplication_check.sh").read_text(encoding="utf-8")
        workflow = (ROOT / ".github" / "workflows" / "quality.yml").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("npx --yes", gate)
        self.assertIn("jscpd --version", gate)
        self.assertIn("npm install --global jscpd@4.0.5", workflow)

    def test_standard_generated_workflow_artifacts_are_ignored(self) -> None:
        candidates = [
            "scripts/__pycache__/x.pyc",
            "scripts/tests/__pycache__/x.pyc",
            "quality/status.json",
            "quality/logs/x.log",
            "quality/baselines/x.json",
            "coverage/lcov.info",
            "target/llvm-cov-worktree/x",
        ]
        result = subprocess.run(
            ["git", "check-ignore", "--stdin"],
            cwd=ROOT,
            input="\n".join(candidates) + "\n",
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.splitlines(), candidates)


if __name__ == "__main__":
    unittest.main()
