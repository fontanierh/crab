SHELL := /bin/bash
.DEFAULT_GOAL := help

PYTHON ?= python3
export PYTHONDONTWRITEBYTECODE := 1

.PHONY: help doctor check quick quality quality-status gate-tests fmt fmt-check clippy test public-api-check coverage coverage-gate coverage-quick coverage-diagnostics duplication-check quality-report quality-baseline

help: ## Show the repository-owned agent workflow (read-only).
	@printf '%s\n' \
	  'Crab agent workflow:' \
	  '  make doctor              Verify pinned, read-only prerequisites.' \
	  '  make check               Run changed-scope format, Clippy, and tests.' \
	  '  make quality             Run every authoritative handoff gate.' \
	  '' \
	  'Coverage:' \
	  '  make coverage-quick      Fresh focused coverage plus worktree patch gate.' \
	  '  make coverage-gate       Fresh full aggregate and patch coverage gates.' \
	  '  make coverage            Generate a fresh full LCOV report.' \
	  '  make coverage-diagnostics Generate fresh concise uncovered-line diagnostics.' \
	  '' \
	  'Utilities:' \
	  '  make gate-tests          Run deterministic offline workflow-tool tests.' \
	  '  make quality-status      Verify quality/status.json still attests this tree.' \
	  '  make fmt                  Format Rust sources (the only mutating workflow target).' \
	  '  make fmt-check           Check formatting without editing.' \
	  '  make clippy              Run full-workspace Clippy.' \
	  '  make test                Run full-workspace tests.' \
	  '  make public-api-check    Check cross-file public API wiring.' \
	  '  make duplication-check   Check production Rust duplication.' \
	  '  make quality-report      Regenerate CODE_QUALITY_REPORT.md.' \
	  '  make quality-baseline    Capture local runtime/density baselines.' \
	  '' \
	  'Options: DRY_RUN=1, VERBOSE=1, BASE_SHA=<commit>, DIFF_MODE=worktree|committed,' \
	  '         PATCH_MODE=worktree|staged|committed, CRAB_SHARED_TARGET_DIR=<absolute-dir>'

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
	@args=(quality --mode "$${PATCH_MODE:-worktree}"); \
	if [[ -n "$${BASE_SHA:-}" ]]; then args+=(--base-sha "$$BASE_SHA"); fi; \
	$(PYTHON) scripts/run_gates.py "$${args[@]}"

quality-status: ## Verify that the last green status still matches this tree.
	@$(PYTHON) scripts/run_gates.py verify-status

gate-tests: ## Run workflow/gate regression tests offline.
	@$(PYTHON) -m unittest discover -s scripts/tests -p 'test_*.py'

fmt: ## Format Rust sources (mutating).
	cargo fmt --all

fmt-check: ## Check Rust formatting.
	cargo fmt --all -- --check

clippy: ## Run full-workspace Clippy under the manifest lint policy.
	@$(PYTHON) scripts/clippy_policy.py --workspace --all-targets --all-features --locked

test: ## Run full-workspace tests.
	@$(PYTHON) scripts/cargo_target.py build -- cargo test --workspace --all-features --locked

public-api-check: ## Check public functions have cross-file use.
	@bash scripts/public_api_usage_check.sh

coverage: ## Generate a fresh full LCOV report in a local coverage target.
	@$(PYTHON) scripts/coverage_workflow.py report

coverage-gate: ## Enforce fresh aggregate and patch coverage.
	@bash scripts/coverage_gate.sh

coverage-quick: ## Run fresh changed-package coverage and the worktree patch gate.
	@args=(quick); \
	if [[ -n "$${BASE_SHA:-}" ]]; then args+=(--base-sha "$$BASE_SHA"); fi; \
	$(PYTHON) scripts/coverage_workflow.py "$${args[@]}"

coverage-diagnostics: ## Generate a fresh LCOV report and concise missing-line summary.
	@bash scripts/coverage_diagnostics.sh

duplication-check: ## Enforce production duplication policy.
	@bash scripts/duplication_check.sh

quality-report: ## Regenerate the tracked code-quality report.
	@bash scripts/gen_code_quality_report.sh

quality-baseline: ## Capture local performance/density data.
	@$(PYTHON) scripts/quality_baseline.py
