set shell := ["sh", "-eu", "-c"]

# List available recipes
default:
    @just --list

# Build the optimized release binary
build:
    cargo build --release

# Build the debug/dev binary
build-dev:
    cargo build

# Run the full test suite
test:
    cargo test

# Type-check all targets without producing binaries
check:
    cargo check --all-targets

# Run clippy with warnings denied
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Format the workspace
fmt:
    cargo fmt --all

# Check formatting without changing files
fmt-check:
    cargo fmt --all -- --check

# Validate one filter config; override with `just validate examples/foo.yaml`
validate file="examples/pois_simple.yaml":
    cargo run -- validate "{{file}}"

# Validate all checked-in example filter configs
validate-examples:
    for file in examples/*.yaml examples/guide/*.yaml examples/maproulette/*.yml; do \
        cargo run -- validate "$file"; \
    done

# Run a fixture-backed conversion smoke test; output streams to stdout
smoke filter="examples/pois_simple.yaml" format="geojsonl":
    cargo run -- convert \
        --input fixture/library_square.osm.pbf \
        --filters "{{filter}}" \
        --format "{{format}}" \
        --output -

# Run the CLI with arbitrary args, e.g. `just run --help` or `just run validate examples/pois_simple.yaml`
run *args:
    cargo run -- {{args}}

# Local CI gate
ci: fmt-check lint test validate-examples

# Remove build artifacts
clean:
    cargo clean
