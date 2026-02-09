SHELL := /bin/bash

.PHONY: fmt fmt-check clippy test deadcode-check coverage coverage-gate duplication-check quality

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

coverage:
	cargo llvm-cov --workspace --all-features --locked --lcov --output-path coverage/lcov.info

coverage-gate:
	cargo llvm-cov --workspace --all-features --locked \
		--fail-under-lines 100 \
		--fail-under-functions 100 \
		--fail-under-regions 100

duplication-check:
	npx --yes jscpd@4.0.5 --config .jscpd.json

quality: fmt-check clippy deadcode-check test coverage-gate duplication-check
