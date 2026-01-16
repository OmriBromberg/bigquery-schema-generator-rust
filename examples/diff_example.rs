//! Schema diff example.
//!
//! This example demonstrates how to compare two BigQuery schemas
//! and identify changes, including breaking changes.
//!
//! Run with: cargo run --example diff_example

use bq_schema_gen::diff::output::{write_diff, ColorMode, DiffFormat};
use bq_schema_gen::diff::{diff_schemas, DiffOptions};
use bq_schema_gen::BqSchemaField;

fn main() {
    // Define an "old" schema (e.g., currently deployed)
    let old_schema = vec![
        BqSchemaField {
            name: "id".to_string(),
            field_type: "INTEGER".to_string(),
            mode: "REQUIRED".to_string(),
            fields: None,
        },
        BqSchemaField {
            name: "name".to_string(),
            field_type: "STRING".to_string(),
            mode: "NULLABLE".to_string(),
            fields: None,
        },
        BqSchemaField {
            name: "email".to_string(),
            field_type: "STRING".to_string(),
            mode: "NULLABLE".to_string(),
            fields: None,
        },
        BqSchemaField {
            name: "count".to_string(),
            field_type: "INTEGER".to_string(),
            mode: "NULLABLE".to_string(),
            fields: None,
        },
    ];

    // Define a "new" schema (e.g., proposed changes)
    let new_schema = vec![
        BqSchemaField {
            name: "id".to_string(),
            field_type: "INTEGER".to_string(),
            mode: "REQUIRED".to_string(),
            fields: None,
        },
        BqSchemaField {
            name: "name".to_string(),
            field_type: "STRING".to_string(),
            mode: "REQUIRED".to_string(), // Changed: NULLABLE -> REQUIRED (BREAKING)
            fields: None,
        },
        // "email" field removed (BREAKING)
        BqSchemaField {
            name: "count".to_string(),
            field_type: "FLOAT".to_string(), // Changed: INTEGER -> FLOAT (safe widening)
            mode: "NULLABLE".to_string(),
            fields: None,
        },
        BqSchemaField {
            name: "created_at".to_string(), // New field added (safe)
            field_type: "TIMESTAMP".to_string(),
            mode: "NULLABLE".to_string(),
            fields: None,
        },
    ];

    println!("=== BigQuery Schema Diff Example ===\n");

    // Compare schemas with default options
    let options = DiffOptions::default();
    let diff = diff_schemas(&old_schema, &new_schema, &options);

    // Print summary
    println!("Summary:");
    println!("  - Added: {}", diff.summary.added);
    println!("  - Removed: {}", diff.summary.removed);
    println!("  - Modified: {}", diff.summary.modified);
    println!("  - Breaking: {}", diff.summary.breaking);
    println!();

    // Print text format
    println!("=== Text Format ===\n");
    let mut output = Vec::new();
    write_diff(&diff, DiffFormat::Text, ColorMode::Always, &mut output).unwrap();
    println!("{}", String::from_utf8_lossy(&output));

    // Print JSON format
    println!("=== JSON Format ===\n");
    let mut json_output = Vec::new();
    write_diff(&diff, DiffFormat::Json, ColorMode::Never, &mut json_output).unwrap();
    println!("{}", String::from_utf8_lossy(&json_output));

    // Print SQL migration hints
    println!("=== SQL Format ===\n");
    let mut sql_output = Vec::new();
    write_diff(&diff, DiffFormat::Sql, ColorMode::Never, &mut sql_output).unwrap();
    println!("{}", String::from_utf8_lossy(&sql_output));

    // Check for breaking changes
    if diff.has_breaking_changes() {
        println!(
            "\nWARNING: This schema change contains {} breaking change(s)!",
            diff.summary.breaking
        );
        println!("Breaking changes:");
        for change in diff.breaking_changes() {
            println!("  - {}: {}", change.path, change.description);
        }
    }

    // Example with strict mode (all changes are breaking)
    println!("\n=== Strict Mode Example ===\n");
    let strict_options = DiffOptions { strict: true };
    let strict_diff = diff_schemas(&old_schema, &new_schema, &strict_options);
    println!(
        "In strict mode, all {} changes are marked as breaking.",
        strict_diff.summary.breaking
    );
}
