//! Watch mode example.
//!
//! This example demonstrates the watch mode configuration options
//! for monitoring files and regenerating schemas automatically.
//!
//! Run with: cargo run --example watch_example
//!
//! Note: To actually use watch mode, use the CLI with the --watch flag.

use bq_schema_gen::watch::WatchConfig;
use bq_schema_gen::GeneratorConfig;

fn main() {
    println!("=== BigQuery Schema Generator Watch Mode Example ===\n");

    // Example 1: Basic WatchConfig
    println!("Example 1: Creating a WatchConfig");
    let config = WatchConfig {
        debounce_ms: 100,           // Wait 100ms after last change before processing
        on_change: None,            // No command to run after schema generation
        quiet: false,               // Show progress messages
        ignore_invalid_lines: true, // Skip malformed JSON lines
    };
    println!("  Debounce: {} ms", config.debounce_ms);
    println!("  Quiet mode: {}", config.quiet);
    println!("  Ignore invalid lines: {}", config.ignore_invalid_lines);
    println!();

    // Example 2: WatchConfig with on_change command
    println!("Example 2: WatchConfig with on_change command");
    let config_with_command = WatchConfig {
        debounce_ms: 200,
        on_change: Some("echo 'Schema updated!'".to_string()),
        quiet: true,
        ignore_invalid_lines: false,
    };
    println!("  On change: {:?}", config_with_command.on_change);
    println!();

    // Example 3: Default WatchConfig
    println!("Example 3: Default WatchConfig");
    let default_config = WatchConfig::default();
    println!("  Debounce: {} ms", default_config.debounce_ms);
    println!("  On change: {:?}", default_config.on_change);
    println!("  Quiet: {}", default_config.quiet);
    println!("  Ignore invalid lines: {}", default_config.ignore_invalid_lines);
    println!();

    // Example 4: GeneratorConfig for watch mode
    println!("Example 4: GeneratorConfig used in watch mode");
    let generator_config = GeneratorConfig::default();
    println!("  Infer mode: {}", generator_config.infer_mode);
    println!("  Keep nulls: {}", generator_config.keep_nulls);
    println!("  Sanitize names: {}", generator_config.sanitize_names);
    println!();

    // CLI usage examples
    println!("=== CLI Usage Examples ===\n");
    println!("Basic watch mode:");
    println!("  bq-schema-gen --watch data/*.json -o schema.json");
    println!();
    println!("With on-change command:");
    println!("  bq-schema-gen --watch data/*.json -o schema.json --on-change 'bq update-schema'");
    println!();
    println!("With custom debounce (500ms):");
    println!("  bq-schema-gen --watch data/*.json --debounce 500");
    println!();
    println!("Quiet mode (suppress progress messages):");
    println!("  bq-schema-gen --watch data/*.json -o schema.json --quiet");
    println!();
    println!("With ignore invalid lines:");
    println!("  bq-schema-gen --watch data/*.json --ignore-invalid-lines");
    println!();

    // Supported file patterns
    println!("=== Supported File Patterns ===\n");
    println!("  Single file:     data/input.json");
    println!("  Glob pattern:    data/*.json");
    println!("  Recursive:       data/**/*.json");
    println!("  Multiple files:  data/*.json logs/*.json");
    println!();

    // How watch mode works
    println!("=== How Watch Mode Works ===\n");
    println!("1. Initial processing: All matched files are processed to generate the schema");
    println!("2. File monitoring: The watcher monitors all matched files for changes");
    println!("3. Debouncing: Changes are debounced to avoid excessive regeneration");
    println!("4. Incremental update: Only changed files are reprocessed");
    println!("5. Schema merge: File schemas are merged into a single output");
    println!("6. On-change hook: Optional command is executed after each update");
}
