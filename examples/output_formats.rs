//! Output format examples: JSON, DDL, JSON Schema.
//!
//! Run with: cargo run --example output_formats

use bq_schema_gen::{
    write_schema_ddl, write_schema_json, write_schema_json_schema, GeneratorConfig,
    SchemaGenerator, SchemaMap,
};
use serde_json::json;

fn main() {
    // Create some test data
    let config = GeneratorConfig::default();
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = SchemaMap::new();

    let records = vec![
        json!({
            "user": {
                "name": "Alice",
                "age": 30
            },
            "tags": ["rust", "bigquery"],
            "active": true
        }),
        json!({
            "user": {
                "name": "Bob",
                "age": 25,
                "email": "bob@example.com"
            },
            "tags": ["python"],
            "active": false
        }),
    ];

    for record in &records {
        generator
            .process_record(record, &mut schema_map)
            .expect("Failed to process record");
    }

    let schema = generator.flatten_schema(&schema_map);

    // 1. JSON Output (BigQuery native format)
    println!("=== JSON Output (BigQuery Schema) ===\n");
    let mut json_output = Vec::new();
    write_schema_json(&schema, &mut json_output).unwrap();
    println!("{}", String::from_utf8(json_output).unwrap());

    // 2. DDL Output (CREATE TABLE statement)
    println!("\n=== DDL Output (CREATE TABLE) ===\n");
    let mut ddl_output = Vec::new();
    write_schema_ddl(&schema, "myproject.mydataset.users", &mut ddl_output).unwrap();
    println!("{}", String::from_utf8(ddl_output).unwrap());

    // 3. JSON Schema Output (draft-07)
    println!("\n=== JSON Schema Output (draft-07) ===\n");
    let mut json_schema_output = Vec::new();
    write_schema_json_schema(&schema, &mut json_schema_output).unwrap();
    println!("{}", String::from_utf8(json_schema_output).unwrap());
}
