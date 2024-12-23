# WCN Justfile
wcn-binary-crate            := "."

export WCN_JUST_ROOT        := justfile_directory()

# Default to listing recipes
_default:
  @just --list --list-prefix '  > '

# Open project documentation in your local browser
docs: (_build-docs "open" "nodeps")
  @echo '==> Opening documentation in system browser'

# Fast check project for errors
check:
  @echo '==> Checking project for compile errors'
  cargo check --workspace --all-features

# Build service for development
build:
  @echo '==> Building project'
  cargo build

# Build project documentation
build-docs: (_build-docs "" "nodeps")

# Run the service
run: build
  @echo '==> Running project (ctrl+c to exit)'
  cargo run

# Run project test suite, skipping storage tests
test:
  @echo '==> Testing project (default)'
  cargo nextest run --workspace # --features=relay-tests

# Run project test suite, including storage tests (requires storage docker services to be running)
test-all:
  @echo '==> Testing project (all features)'
  cargo nextest run --workspace --all-features

# Run test from project documentation
test-doc:
  @echo '==> Testing project docs'
  cargo test --workspace --doc --all-features

# Clean build artifacts
clean:
  @echo '==> Cleaning project target/*'
  cargo clean

# Clean /tmp test folder
clean-tmp:
  @echo '==> Cleaning /tmp/wcn'
  rm -rf /tmp/wcn

# Build WCN docker image
build-docker:
  @echo '=> Build WCN docker image'
  docker compose -f ./docker-compose.yml build

# Start WCN sandbox cluster on docker
run-docker:
  @echo '==> Start WCN sandbox cluster on docker'
  docker compose -f ./docker-compose.yml up -d

# Stop WCN sandbox cluster on docker
stop-docker:
  @echo '==> Stop WCN sandbox cluster on docker'
  docker compose -f ./docker-compose.yml down


run-docker-all:
  @echo '==> Start WCN sandbox cluster on docker'
  docker compose -f ./docker-compose.yml --profile oracle up -d
  @sleep 2
  @sh ./infra/load_anvil_state.sh


stop-docker-all:
  @echo '==> Stop WCN sandbox cluster on docker'
  docker compose -f ./docker-compose.yml --profile oracle down


# Clean up docker WCN sandbox cluster
clean-docker:
  @echo '==> Clean WCN sandbox cluster on docker'
  docker compose  -f ./docker-compose.yml stop
  docker compose -f ./docker-compose.yml rm -f

# List services running on docker
ps-docker:
  @echo '==> List services on docker'
  docker compose -f ./docker-compose.yml ps

# Bumps the binary version to the given version
bump-version to: (_bump-cargo-version to wcn-binary-crate + "/Cargo.toml")

# Lint the project for any quality issues
lint: check fmt clippy commit-check clean-tmp

devloop: lint test-doc test

# Run project linter
clippy:
  #!/bin/bash
  set -euo pipefail

  if command -v cargo-clippy >/dev/null; then
    echo '==> Running clippy'
    cargo +nightly clippy --workspace --all-targets --all-features --tests -- -D warnings
  else
    echo '==> clippy not found in PATH, skipping'
    echo '    ^^^^^^ To install `rustup component add clippy`, see https://github.com/rust-lang/rust-clippy for details'
  fi

# Run code formatting check
fmt:
  #!/bin/bash
  set -euo pipefail

  if command -v cargo-fmt >/dev/null; then
    echo '==> Running rustfmt'
    cargo +nightly fmt --all
  else
    echo '==> rustfmt not found in PATH, skipping'
    echo '    ^^^^^^ To install `rustup component add rustfmt`, see https://github.com/rust-lang/rustfmt for details'
  fi

# Run commit checker
commit-check:
  #!/bin/bash
  set -euo pipefail

  if command -v cog >/dev/null; then
    echo '==> Running cog check'
    cog check --from-latest-tag
  else
    echo '==> cog not found in PATH, skipping'
    echo '    ^^^ To install `cargo install --locked cocogitto`, see https://github.com/cocogitto/cocogitto for details'
  fi

# Update documentation with any changes detected
update-docs: (_regenerate-metrics "docs/Metrics.md")

# Build project documentation
_build-docs $open="" $nodeps="":
  @echo "==> Building project documentation @$WCN_JUST_ROOT/target/doc"
  @cargo doc --all-features --document-private-items ${nodeps:+--no-deps} ${open:+--open}

# Update the metrics documentation with current metrics
_regenerate-metrics file temp=`mktemp`: build
  @echo '==> Regenerating metrics to @{{file}}'
  @cd scripts && ./metrics-apply.awk <(./metrics-fetch.sh | ./metrics-doc.pl | ./metrics-format.pl) < $WCN_JUST_ROOT/{{file}} > {{temp}}
  @mv -f {{temp}} {{file}}

# Bump the version field of a given Cargo.toml file
_bump-cargo-version version file temp=`mktemp`:
  @echo '==> Bumping {{file}} version to {{version}}'
  @perl -spe 'if (/^version/) { s/("[\w.]+")/"$version"/ }' -- -version={{version}} < {{file}} > {{temp}}
  @mv -f {{temp}} {{file}}
