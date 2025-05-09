# Contributing

Thank you for taking an interest in the project! Described below is what you need
to get up to speed with this program.

## Table of contents

<details>
<summary>Expand</summary>

- [Build](#build)
- [Test](#test)
- [Local Dev](#local-dev)
- [Documentation](#documentation)
- [Style](#style)
  - [Format](#format)
  - [Linting](#linting)
  - [Code documentation](#code-documentation)
  - [Git hygiene](#git-hygiene)
    - [Conventional commits](#conventional-commits)

</details>

## Build

Building the project locally is as easy as `cargo build`, and you can just as easily
run the project via `cargo run` (ctrl+c to exit).

## Test

Testing the entire project can be done via `cargo test --all-features` from the
repo root.

## Documentation

You can generate and open this repo's full documentation via:

```
cargo +nightly doc --all-features --document-private-items --open
```

_NOTE:_ You must have a nightly toolchain installed (`rustup install nightly`).

## Style

### Format

This project uses [`rustfmt`](https://github.com/rust-lang/rustfmt) for code formatting.
You can format your code via:

```bash
cargo +nightly fmt --all
```

### Linting

This project uses [`clippy`](https://github.com/rust-lang/rust-clippy) for linting, and
it's enforced by CI. You can run it locally via:

```bash
cargo clippy
```

### Code documentation

All of a crate's public items are expected to be properly documented via Rust
[doc comments](https://doc.rust-lang.org/rust-by-example/meta/doc.html#documentation).
Private items are more flexible, but should still be documented unless the item's
purpose is obvious.

### Git hygiene

This repo expects expressive commits where 1 commit = 1 logical change, with a
clear status message and body explaining the purpose of the commit.

We use [conventional commits](https://www.conventionalcommits.org).
