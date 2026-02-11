SHELL := /bin/bash

.PHONY: fmt fmt-check clippy test deadcode-check public-api-check coverage coverage-gate duplication-check quality

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
	cargo llvm-cov --workspace --all-features --locked \
		--ignore-filename-regex 'crates/crab-app/src/installer.rs' \
		--fail-under-lines 100 \
		--fail-under-functions 100 \
		--fail-uncovered-lines 0 \
		--fail-uncovered-functions 0

duplication-check:
	npx --yes jscpd@4.0.5 --config .jscpd.json

quality: fmt-check clippy deadcode-check public-api-check test coverage-gate duplication-check
