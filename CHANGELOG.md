# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2025-01-19

### Fixed
- Shell completions now include `diff` and `validate` subcommands
- Unified CLI definition between build.rs and main crate to prevent completion drift

### Changed
- Improved security: cargo publish token now passed via env var only

## [0.1.0] - 2024-01-01

### Added
- Initial release
- Generate BigQuery schema from JSON or CSV data files
- Process all records (not just first 500 like BigQuery auto-detect)
- Support for nested records and arrays
- Type inference with automatic type widening
- Multiple output formats: JSON, DDL, Debug Map, JSON Schema
- Existing schema merging support
- Field name sanitization option
- Configurable NULL handling
- Quoted value type inference
- REQUIRED/NULLABLE mode inference
- Preserve input field order option
- Streaming processing for large files
- 6-11x faster than Python implementation
- Library API for programmatic use

[Unreleased]: https://github.com/omribromberg/bigquery-schema-generator-rust/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/omribromberg/bigquery-schema-generator-rust/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/omribromberg/bigquery-schema-generator-rust/releases/tag/v0.1.0
