.PHONY: install-hooks check fmt clippy test

## Install git hooks from .githooks/ into the local repo.
## Run this once after cloning: make install-hooks
install-hooks:
	git config core.hooksPath .githooks
	@echo "Git hooks installed from .githooks/"

## Run the same checks the pre-commit hook enforces.
check: fmt clippy test

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings

test:
	cargo test --all
