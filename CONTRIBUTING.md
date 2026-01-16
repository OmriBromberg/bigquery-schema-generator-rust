# Contributing to BigQuery Schema Generator

Thank you for your interest in contributing to the BigQuery Schema Generator! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Development Setup](#development-setup)
- [Code Style](#code-style)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Commit Message Guidelines](#commit-message-guidelines)
- [Reporting Issues](#reporting-issues)

## Development Setup

### Prerequisites

- Rust 1.92 or later (MSRV)
- Git

### Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/bigquery-schema-generator-rust.git
   cd bigquery-schema-generator-rust
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/omribromberg/bigquery-schema-generator-rust.git
   ```
4. Build the project:
   ```bash
   cargo build
   ```
5. Run tests to ensure everything works:
   ```bash
   cargo test
   ```

## Code Style

We use standard Rust tooling for code formatting and linting.

### Formatting

All code must be formatted with `rustfmt`:

```bash
cargo fmt
```

Before submitting a PR, ensure your code is properly formatted:

```bash
cargo fmt --all -- --check
```

### Linting

All code must pass `clippy` without warnings:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

### General Guidelines

- Follow the [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Write documentation for all public items
- Use meaningful variable and function names
- Keep functions focused and reasonably sized
- Prefer explicit error handling over panics

## Testing

### Running Tests

Run all tests:
```bash
cargo test
```

Run tests with output:
```bash
cargo test -- --nocapture
```

Run a specific test:
```bash
cargo test test_name
```

### Writing Tests

- Write unit tests in the same file as the code being tested (in a `#[cfg(test)]` module)
- Write integration tests in the `tests/` directory
- Aim for comprehensive coverage of edge cases
- Use descriptive test names that explain what is being tested

### Test Categories

- **Unit tests**: Test individual functions and modules
- **Integration tests**: Test CLI behavior and file processing
- **Property-based tests**: Use `proptest` for invariant testing

### Running Benchmarks

```bash
cargo bench
```

## Pull Request Process

1. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write code following the style guidelines
   - Add tests for new functionality
   - Update documentation as needed

3. **Verify your changes**:
   ```bash
   cargo fmt --all -- --check
   cargo clippy --all-targets --all-features -- -D warnings
   cargo test
   ```

4. **Commit your changes** following the commit message guidelines

5. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

6. **Open a Pull Request** against the `main` branch

7. **Address review feedback** if any changes are requested

### PR Requirements

- All CI checks must pass
- Tests must be added for new functionality
- Documentation must be updated if applicable
- The PR description should clearly explain the changes

## Commit Message Guidelines

We follow conventional commit style for commit messages:

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks
- `perf`: Performance improvements

### Examples

```
feat(diff): add SQL output format for schema differences

fix(csv): handle empty values correctly

docs: update README with new CLI options

test(validation): add tests for type coercion
```

### Guidelines

- Use the imperative mood ("add" not "added")
- Keep the first line under 72 characters
- Reference issues when applicable (e.g., "Fixes #123")

## Reporting Issues

### Bug Reports

When reporting a bug, please include:

- A clear, descriptive title
- Steps to reproduce the issue
- Expected behavior
- Actual behavior
- Your environment (OS, Rust version)
- Sample input data (if applicable)

### Feature Requests

When requesting a feature, please include:

- A clear description of the feature
- Use cases and motivation
- Any relevant examples or references

## Questions?

If you have questions about contributing, feel free to:

- Open a GitHub Discussion
- Ask in an issue (tagged with `question`)

Thank you for contributing!
