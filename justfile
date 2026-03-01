# rfs-rs Justfile
# Development tasks for the Rust filesystem project

# Default task
default:
    @just --list

# Build the project
build:
    cargo build

# Build the project in release mode
build-release:
    cargo build --release

# Run tests
test:
    cargo test

# Run tests with output
test-verbose:
    cargo test -- --nocapture

# Format code
fmt:
    cargo fmt

# Lint code
lint:
    cargo clippy -- -D warnings

# Clean build artifacts
clean:
    cargo clean

# Check code without building
check:
    cargo check

# Run the project
run *ARGS:
    cargo run {{ ARGS }}

# Run in release mode
run-release *ARGS:
    cargo run --release {{ ARGS }}

# Build documentation
docs:
    cargo doc --no-deps --open

# Run all checks (fmt, clippy, test)
verify: fmt lint test

# Install the project locally
install:
    cargo install --path .