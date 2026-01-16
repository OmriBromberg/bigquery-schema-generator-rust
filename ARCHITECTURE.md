# BigQuery Schema Generator - Technical Overview

## Project Purpose

A Rust CLI tool and library that generates BigQuery schemas from JSON/CSV data by processing **all records** (unlike BigQuery's auto-detect which only samples 500 records). Supports schema merging, type inference, validation, parallel processing, and file watching.

---

## Architecture

```
src/
├── main.rs           # CLI entry point, clap parsing, command dispatch
├── lib.rs            # Public API exports
├── error.rs          # Error types (Error enum, ErrorLog, Result alias)
├── schema/
│   ├── mod.rs        # Module exports
│   ├── types.rs      # Core types: BqType, BqMode, SchemaEntry, SchemaMap, BqSchemaField
│   ├── generator.rs  # SchemaGenerator: record processing, type inference, merging
│   └── existing.rs   # Load existing schemas from files, type alias conversion
├── inference/
│   └── mod.rs        # Type inference from JSON values, regex patterns for DATE/TIME/TIMESTAMP
├── input/
│   ├── mod.rs        # Module exports
│   ├── json.rs       # JsonRecordIterator: line-by-line NDJSON parsing
│   └── csv.rs        # CsvRecordIterator: CSV to JSON object conversion
├── output/
│   └── mod.rs        # Output formatters: JSON, DDL, debug-map, JSON-Schema
├── diff/
│   ├── mod.rs        # Schema comparison: diff_schemas(), breaking change detection
│   └── output.rs     # Diff formatters: text, JSON, JSON-patch, SQL
├── validate/
│   ├── mod.rs        # SchemaValidator: validates data against existing schema
│   └── error.rs      # ValidationError, ValidationErrorType, ValidationResult
└── watch/
    └── mod.rs        # WatchState: file watching with incremental schema caching
```

---

## Core Types

### `SchemaMap` (schema/types.rs:181)
```rust
pub type SchemaMap = IndexMap<String, SchemaEntry>;
```
- Keys are **lowercase/canonical** field names
- Uses `IndexMap` to preserve insertion order (important for `--preserve-input-sort-order`)

### `SchemaEntry` (schema/types.rs:139-151)
```rust
pub struct SchemaEntry {
    pub status: EntryStatus,  // Hard | Soft | Ignore
    pub filled: bool,         // Present in all records?
    pub name: String,         // Original field name (pre-lowercase)
    pub bq_type: BqType,      // Inferred type
    pub mode: BqMode,         // NULLABLE | REQUIRED | REPEATED
}
```

### `BqType` (schema/types.rs:14-41)
```rust
pub enum BqType {
    // Output types
    Boolean, Integer, Float, String, Timestamp, Date, Time, Record(SchemaMap),
    // Internal tracking types (become STRING in output)
    Null, EmptyArray, EmptyRecord,
    // Quoted types (for CSV/quoted JSON strings)
    QBoolean, QInteger, QFloat,
}
```
- `Q*` types track values inferred from quoted strings (e.g., `"123"` → `QInteger`)
- Internal types (`Null`, `EmptyArray`, `EmptyRecord`) handled specially in output

### `BqSchemaField` (schema/types.rs:184-192)
```rust
pub struct BqSchemaField {
    pub fields: Option<Vec<BqSchemaField>>,  // Nested fields for RECORD
    pub mode: String,      // "NULLABLE", "REQUIRED", "REPEATED"
    pub name: String,
    pub field_type: String, // "STRING", "INTEGER", etc.
}
```
- JSON-serializable output format matching BigQuery schema JSON

### `EntryStatus` (schema/types.rs:128-136)
- `Hard`: Type definitively determined from non-null value
- `Soft`: Type provisional (from null/empty), can be overwritten
- `Ignore`: Conflicting types, excluded from output

---

## Data Flow

### Schema Generation
```
Input (JSON/CSV)
  → JsonRecordIterator/CsvRecordIterator (yields (line_num, serde_json::Value))
  → SchemaGenerator::process_record()
    → deduce_schema_for_record() - recursive field extraction
    → merge_schema_entry() - merges new entry with existing
  → SchemaMap (internal representation)
  → flatten_schema() - converts to Vec<BqSchemaField>
  → write_schema_json/ddl/etc. (output)
```

### Type Inference Priority (inference/mod.rs)
1. JSON null → `Null`
2. JSON boolean → `Boolean`
3. JSON number → `Integer` (if fits i64) or `Float`
4. JSON string:
   - Check TIMESTAMP regex first
   - Check DATE regex
   - Check TIME regex
   - If `!quoted_values_are_strings`: check INTEGER/FLOAT/BOOLEAN patterns → `Q*` types
   - Otherwise → `String`
5. JSON array → `(Repeated, element_type)` or `EmptyArray`
6. JSON object → `Record(SchemaMap)` or `EmptyRecord`

### Type Merging Rules (schema/generator.rs:300-432)
- Same type → same type
- `[Q]Boolean + [Q]Boolean` → `Boolean`
- `[Q]Integer + [Q]Integer` → `Integer`
- `[Q]Float + [Q]Float` → `Float`
- `QInteger + QFloat` → `QFloat`
- `[Q]Integer + [Q]Float` → `Float`
- String-compatible types (`String`, `Timestamp`, `Date`, `Time`, `Q*`) → `String`
- `Record + Record` → merged Record (recursive)
- `NULLABLE RECORD → REPEATED RECORD` allowed (logs warning)
- Incompatible types → `EntryStatus::Ignore`

---

## CLI Structure (main.rs)

### Main Struct
```rust
#[derive(Parser)]
struct Cli {
    files: Vec<String>,          // Positional, glob-expanded
    // ... many flags with aliases (--input_format and --input-format both work)
    threads: Option<usize>,      // Parallel processing
    watch: bool,                 // Watch mode
    debounce: u64,               // Watch debounce (ms)
    on_change: Option<String>,   // Watch on-change command
}
```

### Subcommands
```rust
enum Commands {
    Diff { old_schema, new_schema, format, color, strict, output },
    Validate { files, schema, allow_unknown, strict_types, max_errors, format, quiet },
}
```

### Command Dispatch (main.rs:161-197)
1. `Some(Commands::Diff {...})` → `run_diff()`
2. `Some(Commands::Validate {...})` → `run_validate()`
3. `None` → `run_generate()` (default schema generation)

### Processing Modes (run_generate)
1. **Watch mode** (`--watch`) → `run_watch_mode()`
2. **Per-file mode** (`--per-file`) → `process_per_file()`
3. **Stdin mode** (no files) → `process_single_input(None, ...)`
4. **Merged mode** (default) → `process_merged_files()`
   - If `threads > 1 && files > 1` → `process_files_parallel()` with rayon
   - Otherwise → `process_files_sequential()`

---

## Key Patterns

### Streaming Processing
- Files processed line-by-line via iterators
- Memory-efficient for large files
- `JsonRecordIterator` yields `(line_number, serde_json::Value)`

### Case-Insensitive Field Matching
- Fields stored with lowercase canonical keys
- Original name preserved in `SchemaEntry.name`
- Matching done via `key.to_lowercase()`

### Error Handling
- `Error` enum with `thiserror` derive
- Non-fatal errors logged to `SchemaGenerator.error_logs: Vec<ErrorLog>`
- `--ignore-invalid-lines` skips parse errors

### Field Name Sanitization (`--sanitize-names`)
- Regex replaces `[^a-zA-Z0-9_]` with `_`
- Truncates to 128 chars (BigQuery limit)

---

## Validation Module (validate/)

### SchemaValidator
```rust
pub struct SchemaValidator<'a> {
    schema: &'a [BqSchemaField],
    options: ValidationOptions,
    schema_map: HashMap<String, &'a BqSchemaField>,  // lowercase lookup
}
```

### ValidationOptions
```rust
pub struct ValidationOptions {
    pub allow_unknown: bool,   // Unknown fields → warnings instead of errors
    pub strict_types: bool,    // "123" fails INTEGER (JSON string ≠ number)
    pub max_errors: usize,     // Stop after N errors
}
```

### Type Coercion (lenient mode, default)
- Uses inference module's `is_*_string()` functions
- `"123"` valid for INTEGER, `"true"` valid for BOOLEAN, etc.

### Strict Mode (`--strict-types`)
- JSON type must match schema type exactly
- JSON string → only STRING
- JSON number → INTEGER or FLOAT
- JSON boolean → only BOOLEAN

---

## Parallel Processing (main.rs:715-935)

### Approach
1. Collect files from glob patterns
2. Create rayon thread pool (`ThreadPoolBuilder::new().num_threads(n)`)
3. Process files via `par_iter()`, each producing `SchemaMap`
4. Merge all `SchemaMap` results using `merge_schema_maps()`

### Progress Bar
```rust
ProgressBar::new(files.len() as u64)
    .set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} files | {msg}"))
```

### Schema Merging
`merge_schema_maps()` converts `SchemaEntry` back to JSON value and processes through generator to trigger merge logic.

---

## Watch Mode (watch/mod.rs)

### WatchState
```rust
pub struct WatchState {
    file_schemas: HashMap<PathBuf, SchemaMap>,  // Per-file cache
    current_schema: Vec<BqSchemaField>,         // Merged result
    config: GeneratorConfig,
    watch_config: WatchConfig,
}
```

### Incremental Updates
- `handle_file_change(path)`: Reprocess single file, rebuild merged schema, return diff
- `handle_file_delete(path)`: Remove from cache, rebuild, return diff

### Event Loop
- Uses `notify-debouncer-mini` for debounced file events
- Watches unique parent directories of matched files
- Only processes events for files matching original patterns
- Prints diff summary with colored output

---

## Diff Module (diff/)

### Breaking Change Rules (diff/mod.rs:247-282)
**Always Breaking:**
- Field removal
- `NULLABLE → REQUIRED`
- `REPEATED ↔ NULLABLE/REQUIRED`

**Not Breaking:**
- `INTEGER → FLOAT` (widening)
- Any type → `STRING`
- `REQUIRED → NULLABLE`

**Strict Mode (`--strict`):** All changes flagged as breaking

### Output Formats
- `text`: Colored human-readable
- `json`: Full structured diff
- `json-patch`: RFC 6902 format
- `sql`: Migration hints with comments

---

## Testing

### Test Organization
```
tests/
├── cli_tests.rs          # CLI argument handling, end-to-end
├── csv_tests.rs          # CSV parsing edge cases
├── edge_cases.rs         # Type coercion, conflicts, unicode
└── integration_tests.rs  # Full processing scenarios
```

### Test Helper Pattern
```rust
fn generate_schema(records: &[&str], config: GeneratorConfig) -> (Vec<BqSchemaField>, Vec<ErrorLog>) {
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = SchemaMap::new();
    for record in records {
        let value: Value = serde_json::from_str(record).unwrap();
        generator.process_record(&value, &mut schema_map).ok();
    }
    (generator.flatten_schema(&schema_map), generator.error_logs().to_vec())
}
```

---

## Dependencies

```toml
# Core
serde = { version = "1.0", features = ["derive"] }
serde_json = { version = "1.0", features = ["preserve_order"] }
clap = { version = "4.4", features = ["derive", "string"] }
indexmap = { version = "2.1", features = ["serde"] }
regex = "1.10"
once_cell = "1.19"  # Lazy statics for regex
glob = "0.3"
colored = "2"
thiserror = "1.0"
csv = "1.3"

# Parallel processing
rayon = "1.8"
indicatif = "0.17"
num_cpus = "1.16"

# Watch mode
notify = "6.1"
notify-debouncer-mini = "0.4"
```

---

## Important Gotchas

1. **Field order**: `IndexMap` preserves insertion order; `--preserve-input-sort-order` disables alphabetical sorting

2. **Case sensitivity**: BigQuery is case-insensitive for field names; canonical key is lowercase, original preserved in entry

3. **Quoted types**: `QInteger`, `QFloat`, `QBoolean` track inferred types from strings; merge with unquoted → unquoted type

4. **Empty arrays**: `[]` becomes `EmptyArray`, can later upgrade to typed array; `--keep-nulls` outputs as `STRING REPEATED`

5. **Nested arrays**: Not supported by BigQuery; returns `None` from type inference, logged as error

6. **NULLABLE RECORD → REPEATED**: Allowed with warning (BigQuery behavior)

7. **Integer overflow**: Numbers > `i64::MAX` become `FLOAT`

8. **Regex patterns**: Defined in `inference/mod.rs` with `once_cell::Lazy`; match Python implementation exactly

9. **Exit codes**:
   - Generation: 0 success
   - Diff: 1 if breaking changes
   - Validate: 0 valid, 1 invalid, 2 file error

10. **CLI flag aliases**: Both `--input_format` and `--input-format` work (Python compatibility)

---

## Future Enhancement Points

1. **Single-file chunking**: Parallel processing within a single large file
2. **CSV validation**: Currently JSON-only
3. **Incremental parallel watch**: Parallelize file reprocessing in watch mode
4. **Custom type mappings**: User-defined regex → type rules
5. **Schema migration generation**: DDL ALTER statements from diff
