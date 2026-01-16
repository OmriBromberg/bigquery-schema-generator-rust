//! BigQuery Schema Generator
//!
//! A Rust library and CLI tool for generating BigQuery schemas from JSON or CSV data.
//!
//! Unlike BigQuery's built-in auto-detect which only examines the first 500 records,
//! this tool processes all records in the input to generate a complete schema.
//!
//! # Example
//!
//! ```rust
//! use bq_schema_gen::{SchemaGenerator, GeneratorConfig, SchemaMap};
//! use serde_json::json;
//!
//! let config = GeneratorConfig::default();
//! let mut generator = SchemaGenerator::new(config);
//! let mut schema_map = SchemaMap::new();
//!
//! // Process records
//! let record = json!({"name": "test", "count": 42});
//! generator.process_record(&record, &mut schema_map).unwrap();
//!
//! // Get the BigQuery schema
//! let schema = generator.flatten_schema(&schema_map);
//! ```

pub mod diff;
pub mod error;
pub mod inference;
pub mod input;
pub mod output;
pub mod schema;
pub mod validate;
pub mod watch;

// Re-export commonly used types
pub use error::{Error, ErrorLog, Result};
pub use input::{CsvRecordIterator, JsonRecordIterator};
pub use output::{
    schema_to_json_string, write_schema_ddl, write_schema_debug_map, write_schema_json,
    write_schema_json_schema, OutputFormat,
};
pub use schema::{
    bq_schema_to_map, read_existing_schema_from_file, BqMode, BqSchemaField, BqType, EntryStatus,
    GeneratorConfig, InputFormat, SchemaEntry, SchemaGenerator, SchemaMap,
};
pub use validate::{
    validate_json_data, SchemaValidator, ValidationError, ValidationErrorType, ValidationOptions,
    ValidationResult,
};
pub use watch::{run_watch, WatchConfig, WatchState};

use std::io::{BufRead, Read, Write};

/// High-level function to generate schema from a JSON reader.
///
/// If `existing_schema` is provided, the generated schema will be merged with it.
pub fn generate_schema_from_json<R: BufRead, W: Write>(
    input: R,
    output: &mut W,
    config: GeneratorConfig,
    ignore_invalid_lines: bool,
    debugging_interval: Option<usize>,
    existing_schema: Option<SchemaMap>,
) -> Result<Vec<ErrorLog>> {
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = existing_schema.unwrap_or_default();

    let iter = JsonRecordIterator::new(input, ignore_invalid_lines);

    for result in iter {
        let (line_num, record) = result?;

        if let Some(interval) = debugging_interval {
            if line_num % interval == 0 {
                eprintln!("Processing line {}", line_num);
            }
        }

        if let Err(e) = generator.process_record(&record, &mut schema_map) {
            if !ignore_invalid_lines {
                return Err(e);
            }
        }
    }

    eprintln!("Processed {} lines", generator.line_number());

    let schema = generator.flatten_schema(&schema_map);
    write_schema_json(&schema, output)?;

    Ok(generator.error_logs().to_vec())
}

/// High-level function to generate schema from a CSV reader.
///
/// If `existing_schema` is provided, the generated schema will be merged with it.
pub fn generate_schema_from_csv<R: Read, W: Write>(
    input: R,
    output: &mut W,
    config: GeneratorConfig,
    debugging_interval: Option<usize>,
    existing_schema: Option<SchemaMap>,
) -> Result<Vec<ErrorLog>> {
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = existing_schema.unwrap_or_default();

    let iter = CsvRecordIterator::new(input)?;

    for result in iter {
        let (line_num, record) = result?;

        if let Some(interval) = debugging_interval {
            if line_num % interval == 0 {
                eprintln!("Processing line {}", line_num);
            }
        }

        generator.process_record(&record, &mut schema_map)?;
    }

    eprintln!("Processed {} lines", generator.line_number());

    let schema = generator.flatten_schema(&schema_map);
    write_schema_json(&schema, output)?;

    Ok(generator.error_logs().to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_generate_schema_from_json() {
        let input = r#"{"name": "test", "value": 42}
{"name": "foo", "value": 123, "active": true}"#;
        let cursor = Cursor::new(input);
        let mut output = Vec::new();

        let config = GeneratorConfig::default();
        let errors =
            generate_schema_from_json(cursor, &mut output, config, false, None, None).unwrap();

        assert!(errors.is_empty());

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("\"name\""));
        assert!(output_str.contains("\"value\""));
        assert!(output_str.contains("\"active\""));
    }

    #[test]
    fn test_generate_schema_from_csv() {
        let input = "name,value,active\ntest,42,true\nfoo,123,false";
        let cursor = Cursor::new(input);
        let mut output = Vec::new();

        let mut config = GeneratorConfig::default();
        config.input_format = InputFormat::Csv;

        let errors = generate_schema_from_csv(cursor, &mut output, config, None, None).unwrap();

        assert!(errors.is_empty());

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("\"name\""));
        assert!(output_str.contains("\"value\""));
        assert!(output_str.contains("\"active\""));
    }
}
