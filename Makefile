SHELL := /bin/bash

.PHONY: fmt fmt-check clippy test deadcode-check public-api-check coverage coverage-gate coverage-diagnostics duplication-check quick quality quality-report quality-baseline

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features --locked

deadcode-check:
	cargo check --workspace --all-targets --all-features --locked

public-api-check:
	bash scripts/public_api_usage_check.sh

coverage:
	cargo llvm-cov --workspace --all-features --locked --lcov --output-path coverage/lcov.info

coverage-gate:
	bash scripts/coverage_gate.sh

coverage-diagnostics:
	bash scripts/coverage_diagnostics.sh

duplication-check:
	bash scripts/duplication_check.sh

quick: fmt-check clippy deadcode-check public-api-check test

quality: fmt-check clippy deadcode-check public-api-check coverage-gate duplication-check

quality-report:
	bash scripts/gen_code_quality_report.sh

quality-baseline:
	bash scripts/collect_quality_baseline.sh
