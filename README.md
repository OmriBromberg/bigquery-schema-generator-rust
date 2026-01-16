# bq-schema-gen

[![Crates.io](https://img.shields.io/crates/v/bq-schema-gen.svg)](https://crates.io/crates/bq-schema-gen)
[![License](https://img.shields.io/crates/l/bq-schema-gen.svg)](https://github.com/omribromberg/bigquery-schema-generator-rust/blob/main/LICENSE)
[![CI](https://github.com/omribromberg/bigquery-schema-generator-rust/actions/workflows/ci.yml/badge.svg)](https://github.com/omribromberg/bigquery-schema-generator-rust/actions/workflows/ci.yml)
[![docs.rs](https://img.shields.io/docsrs/bq-schema-gen)](https://docs.rs/bq-schema-gen)

Generate BigQuery schemas from JSON or CSV data. Unlike BigQuery's built-in auto-detect which only examines the first 500 records, this tool processes **all records** to generate complete and accurate schemas.

## Quick Start

```bash
# Install
cargo install bq-schema-gen

# Generate a schema
echo '{"name": "Alice", "age": 30}' | bq-schema-gen
```

## Features

- **Schema Generation** - Infer BigQuery schemas from JSON or CSV files
- **Schema Diff** - Compare schemas and detect breaking changes
- **Data Validation** - Validate data against existing schemas
- **Watch Mode** - Auto-regenerate schemas when files change
- **Parallel Processing** - Fast processing of large datasets
- **Multiple Output Formats** - JSON, DDL, JSON Schema

## Installation

### From crates.io

```bash
cargo install bq-schema-gen
```

### Using Homebrew

```bash
brew tap omribromberg/bigquery-schema-generator-rust
brew install bq-schema-gen
```

### From Binary

Download pre-built binaries from [GitHub Releases](https://github.com/omribromberg/bigquery-schema-generator-rust/releases).

### From Source

```bash
git clone https://github.com/omribromberg/bigquery-schema-generator-rust
cd bigquery-schema-generator-rust
cargo install --path .
```

## Usage

### Generate Schema

From stdin:

```bash
echo '{"name": "Alice", "age": 30}' | bq-schema-gen
```

From a file:

```bash
bq-schema-gen data.json --output schema.json
```

Multiple files with glob patterns:

```bash
bq-schema-gen "data/*.json"
```

Output separate schemas per file:

```bash
bq-schema-gen "data/*.json" --per-file --output-dir schemas/
```

CSV input:

```bash
bq-schema-gen --input-format csv data.csv
```

### Compare Schemas (diff)

Compare two schemas to identify changes:

```bash
bq-schema-gen diff old_schema.json new_schema.json
```

Example output:

```
Schema Diff Report
==================

Summary: 1 added, 1 removed, 1 modified (2 breaking)

Added Fields:
  + email (STRING, NULLABLE)

Removed Fields:
  - legacy_id (INTEGER, NULLABLE)  [BREAKING]

Modified Fields:
  ~ name: Mode changed: NULLABLE -> REQUIRED  [BREAKING]
```

Output formats: `text` (default), `json`, `json-patch`, `sql`

```bash
bq-schema-gen diff old.json new.json --format json-patch
```

### Validate Data

Validate data against an existing schema:

```bash
bq-schema-gen data.json --existing-schema-path schema.json
```

### Watch Mode

Auto-regenerate schemas when files change:

```bash
bq-schema-gen watch data.json --output schema.json
```

## CLI Reference

| Flag | Description |
|------|-------------|
| `--input-format <FORMAT>` | Input format: `json` (default) or `csv` |
| `--output-format <FORMAT>` | Output format: `json`, `ddl`, `debug-map`, or `json-schema` |
| `--table-name <NAME>` | Table name for DDL output |
| `-o, --output <FILE>` | Output file (stdout if not provided) |
| `-q, --quiet` | Suppress progress messages |
| `--per-file` | Output separate schema for each input file |
| `--output-dir <DIR>` | Output directory for per-file schemas |
| `--keep-nulls` | Include null values and empty containers in schema |
| `--quoted-values-are-strings` | Treat quoted values as strings |
| `--infer-mode` | Infer REQUIRED mode for CSV fields |
| `--sanitize-names` | Replace invalid characters in field names |
| `--preserve-input-sort-order` | Preserve field order from input |
| `--existing-schema-path <FILE>` | Merge with an existing schema |
| `--ignore-invalid-lines` | Skip unparseable lines |

> All flags support both kebab-case (`--keep-nulls`) and underscore (`--keep_nulls`) syntax.

### Diff Options

| Flag | Description |
|------|-------------|
| `--format <FORMAT>` | Output: `text`, `json`, `json-patch`, `sql` |
| `--color <WHEN>` | Color output: `auto`, `always`, `never` |
| `--strict` | Flag ALL changes as breaking |
| `-o, --output <FILE>` | Output file |

## Output Formats

### JSON (default)

Standard BigQuery schema format:

```bash
echo '{"name": "Alice", "age": 30}' | bq-schema-gen
```

```json
[
  {"mode": "NULLABLE", "name": "age", "type": "INTEGER"},
  {"mode": "NULLABLE", "name": "name", "type": "STRING"}
]
```

### DDL

BigQuery CREATE TABLE statement:

```bash
echo '{"name": "Alice", "age": 30}' | bq-schema-gen --output-format ddl --table-name myproject.users
```

```sql
CREATE TABLE `myproject.users` (
  age INT64,
  name STRING
);
```

### JSON Schema

JSON Schema draft-07 format:

```bash
echo '{"name": "Alice", "age": 30}' | bq-schema-gen --output-format json-schema
```

## Type Inference

The tool automatically infers BigQuery types:

| JSON Type | BigQuery Type |
|-----------|---------------|
| string | STRING, DATE, TIME, or TIMESTAMP (auto-detected) |
| number (integer) | INTEGER |
| number (float) | FLOAT |
| boolean | BOOLEAN |
| object | RECORD |
| array | REPEATED |

### Type Evolution

Types evolve as more data is processed:

- INTEGER + FLOAT = FLOAT
- DATE/TIME/TIMESTAMP combinations = STRING
- Type widening is automatic (INTEGER -> FLOAT, anything -> STRING)

## Shell Completions

Shell completions for bash, zsh, fish, and PowerShell are generated during build. If installed via Homebrew, completions are automatically installed.

## Library Usage

The crate can be used as a Rust library:

```rust
use bq_schema_gen::{SchemaGenerator, GeneratorConfig, SchemaMap};
use serde_json::json;

let config = GeneratorConfig::default();
let mut generator = SchemaGenerator::new(config);
let mut schema_map = SchemaMap::new();

let record = json!({"name": "test", "count": 42});
generator.process_record(&record, &mut schema_map).unwrap();

let schema = generator.flatten_schema(&schema_map);
```

See [docs.rs](https://docs.rs/bq-schema-gen) for the full API documentation.

## License

Apache-2.0 (same as the original Python project)

## Credits

- Original Python implementation by [Brian T. Park](https://github.com/bxparks/bigquery-schema-generator)
- Rust port maintains compatibility with the original tool's behavior
