//! Merging with an existing schema example.
//!
//! Run with: cargo run --example existing_schema

use bq_schema_gen::{bq_schema_to_map, GeneratorConfig, SchemaGenerator};
use serde_json::json;

fn main() {
    // Simulate an existing BigQuery schema (as JSON)
    let existing_schema = json!([
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
    ]);

    // Convert to internal SchemaMap
    let mut schema_map = bq_schema_to_map(&existing_schema).expect("Failed to parse schema");

    // Create generator and process new data with additional fields
    let config = GeneratorConfig::default();
    let mut generator = SchemaGenerator::new(config);

    let new_records = vec![
        json!({"id": 1, "name": "Alice", "email": "alice@example.com"}),
        json!({"id": 2, "name": "Bob", "active": true}),
    ];

    for record in &new_records {
        generator
            .process_record(record, &mut schema_map)
            .expect("Failed to process record");
    }

    // Get the merged schema
    let schema = generator.flatten_schema(&schema_map);

    println!("Merged Schema:");
    println!("(Original fields + new fields discovered from data)\n");

    for field in &schema {
        println!("  {}: {} ({})", field.name, field.field_type, field.mode);
    }

    // Note: created_at is preserved even though it's not in the new data
}
