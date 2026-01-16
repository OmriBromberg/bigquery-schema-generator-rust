//! Basic JSON schema generation example.
//!
//! Run with: cargo run --example basic_json

use bq_schema_gen::{GeneratorConfig, SchemaGenerator, SchemaMap};
use serde_json::json;

fn main() {
    // Create a generator with default configuration
    let config = GeneratorConfig::default();
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = SchemaMap::new();

    // Process some JSON records
    let records = vec![
        json!({"name": "Alice", "age": 30, "active": true}),
        json!({"name": "Bob", "age": 25, "email": "bob@example.com"}),
        json!({"name": "Charlie", "age": 35, "tags": ["rust", "bigquery"]}),
    ];

    for record in &records {
        generator
            .process_record(record, &mut schema_map)
            .expect("Failed to process record");
    }

    // Get the flattened BigQuery schema
    let schema = generator.flatten_schema(&schema_map);

    // Print the schema as JSON
    let json = serde_json::to_string_pretty(&schema).unwrap();
    println!("Generated BigQuery Schema:");
    println!("{}", json);
}
