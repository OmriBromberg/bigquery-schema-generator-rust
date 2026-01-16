# Developer Guide

This document provides information for developers who want to build, test, and contribute to the BigQuery Schema Generator.

## Prerequisites

- **Rust**: Version 1.92 or later (MSRV)
  ```bash
  # Install Rust via rustup
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
  ```
- **Git**: For version control

## Building from Source

### Debug Build

```bash
cargo build
```

The binary will be at `./target/debug/bq-schema-gen`.

### Release Build

```bash
cargo build --release
```

The optimized binary will be at `./target/release/bq-schema-gen`.

## Running Tests

### All Tests

```bash
cargo test
```

### Specific Test File

```bash
cargo test --test cli_tests
cargo test --test diff_cli_tests
cargo test --test validation_cli_tests
```

### Single Test

```bash
cargo test test_name
```

### Tests with Output

```bash
cargo test -- --nocapture
```

### Property-Based Tests

```bash
cargo test --test proptest_tests
```

## Running Benchmarks

```bash
cargo bench
```

Benchmark results are saved in `target/criterion/`.

## Code Quality

### Formatting

Check formatting:
```bash
cargo fmt --all -- --check
```

Fix formatting:
```bash
cargo fmt
```

### Linting

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### Documentation

Build documentation:
```bash
cargo doc --no-deps --document-private-items
```

Open documentation:
```bash
cargo doc --open
```

## Project Structure

```
.
├── src/
│   ├── main.rs           # CLI entry point
│   ├── lib.rs            # Library entry point
│   ├── cli/              # CLI subcommand implementations
│   │   ├── mod.rs        # CLI argument parsing
│   │   ├── generate.rs   # Schema generation command
│   │   ├── diff.rs       # Schema diff command
│   │   └── validate.rs   # Data validation command
│   ├── diff/             # Schema comparison module
│   ├── error.rs          # Error types
│   ├── inference/        # Type inference logic
│   ├── input/            # Input parsing (JSON, CSV)
│   ├── output/           # Output formatting
│   ├── schema/           # Schema types and generation
│   ├── validate/         # Data validation
│   └── watch/            # File watching
├── tests/                # Integration tests
├── examples/             # Example usage
└── benches/              # Benchmarks
```

## Debugging Tips

### Enable Debug Output

Use the `--debugging-interval` flag to see progress:
```bash
./target/debug/bq-schema-gen --debugging-interval 100 input.json
```

### Verbose Schema Output

Use `debug-map` output format to see internal schema representation:
```bash
echo '{"a": 1}' | ./target/debug/bq-schema-gen --output-format debug-map
```

### Debug Build with Backtrace

```bash
RUST_BACKTRACE=1 cargo run -- input.json
```

### Running with LLDB/GDB

```bash
# With LLDB (macOS)
lldb -- ./target/debug/bq-schema-gen input.json

# With GDB (Linux)
gdb --args ./target/debug/bq-schema-gen input.json
```

## Common Development Tasks

### Adding a New CLI Flag

1. Add the argument to `src/cli/mod.rs` in the `Cli` struct
2. Update the relevant subcommand in `src/cli/generate.rs`, `diff.rs`, or `validate.rs`
3. Add tests in the corresponding `tests/*_cli_tests.rs` file
4. Update documentation

### Adding a New Output Format

1. Add the format variant to `OutputFormat` in `src/output/mod.rs`
2. Implement the writer function in `src/output/mod.rs`
3. Add CLI handling in `src/cli/generate.rs`
4. Add tests and documentation

### Adding a New Subcommand

1. Create a new file in `src/cli/` (e.g., `src/cli/mycommand.rs`)
2. Add the subcommand variant to `Commands` in `src/cli/mod.rs`
3. Add the `pub mod mycommand;` declaration
4. Add dispatch in `cli::run()`
5. Create integration tests

## Pre-commit Hooks

Install pre-commit hooks to automatically check code before committing:

```bash
pip install pre-commit
pre-commit install
```

Run hooks manually:
```bash
pre-commit run --all-files
```

## Continuous Integration

The CI pipeline runs:

1. **Build**: Compiles on Ubuntu, macOS, and Windows
2. **Test**: Runs all tests on multiple Rust versions (stable, 1.92)
3. **Lint**: Checks formatting and runs clippy
4. **Docs**: Builds documentation

## Release Process

1. Update version in `Cargo.toml`
2. Update `CHANGELOG.md`
3. Create a git tag: `git tag -a v0.x.x -m "Release v0.x.x"`
4. Push the tag: `git push origin v0.x.x`
5. The release workflow will build and publish binaries

## Troubleshooting

### Build Fails with "linker not found"

Install build essentials:
```bash
# Ubuntu/Debian
sudo apt-get install build-essential

# macOS
xcode-select --install
```

### Tests Fail on Windows

Some tests use Unix-specific paths. Tests should skip gracefully on Windows, but if issues persist, run:
```bash
cargo test --test integration_tests
```

### Clippy Warnings

Fix warnings before committing:
```bash
cargo clippy --fix --allow-dirty
```
