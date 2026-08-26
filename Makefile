SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
export PYTHONDONTWRITEBYTECODE := 1

.PHONY: help doctor check quick quality quality-status gate-tests fmt fmt-check clippy test v2-bundle v2-bundle-verify public-api-check coverage coverage-gate coverage-quick coverage-diagnostics duplication-check quality-report quality-baseline

help: ## Show the repository-owned agent workflow (read-only).
	@printf '%s\n' \
	  'Crab agent workflow:' \
	  '  make doctor              Verify pinned, read-only prerequisites.' \
	  '  make check               Run changed-scope format, Clippy, and tests across v1/v2.' \
	  '  make quality             Run full v1/v2 format, Clippy, and tests.' \
	  '  make v2-bundle           Build and verify one locked Crab v2 runtime bundle.' \
	  '' \
	  'Optional diagnostics (never merge gates):' \
	  '  make coverage-quick      Generate focused coverage totals.' \
	  '  make coverage            Generate a fresh full LCOV report.' \
	  '  make coverage-gate       Deprecated compatibility alias for make coverage.' \
	  '  make coverage-diagnostics Generate fresh concise uncovered-line diagnostics.' \
	  '  make duplication-check   Report substantial production duplication.' \
	  '  make public-api-check    Report cross-file public API wiring problems.' \
	  '  make gate-tests          Run deterministic offline workflow-tool tests.' \
	  '' \
	  'Utilities:' \
	  '  make quality-status      Verify quality/status.json still attests this tree.' \
	  '  make fmt                  Format Rust sources (the only mutating workflow target).' \
	  '  make fmt-check           Check formatting without editing.' \
	  '  make clippy              Run full-workspace Clippy.' \
	  '  make test                Run full-workspace tests.' \
	  '  make v2-bundle-verify V2_BUNDLE=<path>' \
	  '                           Verify an existing Crab v2 runtime bundle.' \
	  '  make quality-report      Regenerate CODE_QUALITY_REPORT.md.' \
	  '  make quality-baseline    Capture local runtime/density baselines.' \
	  '' \
	  'Options: DRY_RUN=1, VERBOSE=1, BASE_SHA=<commit>, DIFF_MODE=worktree|committed,' \
	  '         CRAB_SHARED_TARGET_DIR=<absolute-dir>'

doctor: ## Verify prerequisites without installing or mutating anything.
	@command -v "$(PYTHON)" >/dev/null 2>&1 || { \
	  echo 'doctor: python3 is required; install Python 3 and retry' >&2; \
	  exit 2; \
	}
	@$(PYTHON) scripts/doctor.py

check: ## Run changed-scope development checks.
	@args=(check --mode "$${DIFF_MODE:-worktree}"); \
	if [[ -n "$${BASE_SHA:-}" ]]; then args+=(--base-sha "$$BASE_SHA"); fi; \
	if [[ "$${DRY_RUN:-0}" == "1" ]]; then args+=(--dry-run); fi; \
	$(PYTHON) scripts/run_gates.py "$${args[@]}"

quick: ## Deprecated alias for make check.
	@echo 'make quick is deprecated; running make check'
	@$(MAKE) --no-print-directory check

quality: ## Run the authoritative handoff gate and write quality/status.json.
	@$(PYTHON) scripts/run_gates.py quality

quality-status: ## Verify that the last green status still matches this tree.
	@$(PYTHON) scripts/run_gates.py verify-status

gate-tests: ## Run workflow/gate regression tests offline.
	@$(PYTHON) -m unittest discover -s scripts/tests -p 'test_*.py'

fmt: ## Format Rust sources (mutating).
	@$(PYTHON) scripts/workspace_gate.py fmt --root-workspace --v2-workspace --write

fmt-check: ## Check Rust formatting.
	@$(PYTHON) scripts/workspace_gate.py fmt --root-workspace --v2-workspace

clippy: ## Run full-workspace Clippy under the manifest lint policy.
	@$(PYTHON) scripts/workspace_gate.py clippy --root-workspace --v2-workspace

test: ## Run full-workspace tests.
	@$(PYTHON) scripts/workspace_gate.py tests --root-workspace --v2-workspace

v2-bundle: ## Build and verify one locked Crab v2 runtime bundle.
	@$(PYTHON) scripts/v2_bundle.py build $(if $(V2_BUNDLE),--output "$(V2_BUNDLE)",)

v2-bundle-verify: ## Verify V2_BUNDLE without needing Rust, npm, or the source checkout.
	@test -n "$(V2_BUNDLE)" || { echo 'V2_BUNDLE=<path> is required' >&2; exit 2; }
	@$(PYTHON) scripts/v2_bundle.py verify "$(V2_BUNDLE)"

public-api-check: ## Check public functions have cross-file use.
	@bash scripts/public_api_usage_check.sh

coverage: ## Generate a fresh full LCOV report in a local coverage target.
	@$(PYTHON) scripts/coverage_workflow.py report

coverage-gate: ## Deprecated compatibility alias for the non-blocking coverage report.
	@bash scripts/coverage_gate.sh

coverage-quick: ## Run fresh changed-package coverage and the worktree patch gate.
	@args=(quick); \
	if [[ -n "$${BASE_SHA:-}" ]]; then args+=(--base-sha "$$BASE_SHA"); fi; \
	$(PYTHON) scripts/coverage_workflow.py "$${args[@]}"

coverage-diagnostics: ## Generate a fresh LCOV report and concise missing-line summary.
	@bash scripts/coverage_diagnostics.sh

duplication-check: ## Report substantial production duplication (non-blocking findings).
	@bash scripts/duplication_check.sh

quality-report: ## Regenerate the tracked code-quality report.
	@bash scripts/gen_code_quality_report.sh

quality-baseline: ## Capture local performance/density data.
	@$(PYTHON) scripts/quality_baseline.py
