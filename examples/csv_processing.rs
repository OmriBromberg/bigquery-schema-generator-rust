//! CSV schema generation with infer_mode example.
//!
//! Run with: cargo run --example csv_processing

use bq_schema_gen::{CsvRecordIterator, GeneratorConfig, InputFormat, SchemaGenerator, SchemaMap};
use std::io::Cursor;

fn main() {
    // Sample CSV data
    let csv_data = r#"name,age,email,score
Alice,30,alice@example.com,4.5
Bob,25,bob@example.com,3.8
Charlie,35,charlie@example.com,4.9"#;

    // Configure for CSV with infer_mode enabled
    // infer_mode determines REQUIRED vs NULLABLE based on whether fields are always present
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        infer_mode: true, // Fields present in all rows become REQUIRED
        ..Default::default()
    };

    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = SchemaMap::new();

    // Process CSV records
    let cursor = Cursor::new(csv_data);
    let iter = CsvRecordIterator::new(cursor).expect("Failed to create CSV iterator");

    for result in iter {
        let (_line_num, record) = result.expect("Failed to read record");
        generator
            .process_record(&record, &mut schema_map)
            .expect("Failed to process record");
    }

    // Get the schema
    let schema = generator.flatten_schema(&schema_map);

    println!("CSV Schema (with infer_mode):");
    for field in &schema {
        println!("  {} {} ({})", field.name, field.field_type, field.mode);
    }
}
