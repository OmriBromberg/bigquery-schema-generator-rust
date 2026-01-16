//! Functions for loading and converting existing BigQuery schemas.
//!
//! This module handles reading an existing BigQuery schema JSON file
//! and converting it to the internal SchemaMap representation.

use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use crate::error::{Error, Result};
use crate::schema::types::{BqMode, BqType, EntryStatus, SchemaEntry, SchemaMap};

/// BigQuery type aliases - maps standard SQL types to legacy types.
fn normalize_bq_type(type_name: &str) -> Result<&'static str> {
    match type_name.to_uppercase().as_str() {
        // Standard types
        "STRING" => Ok("STRING"),
        "BYTES" => Ok("BYTES"),
        "INTEGER" => Ok("INTEGER"),
        "FLOAT" => Ok("FLOAT"),
        "BOOLEAN" => Ok("BOOLEAN"),
        "TIMESTAMP" => Ok("TIMESTAMP"),
        "DATE" => Ok("DATE"),
        "TIME" => Ok("TIME"),
        "DATETIME" => Ok("DATETIME"),
        "RECORD" => Ok("RECORD"),
        // Type aliases (Standard SQL names)
        "INT64" => Ok("INTEGER"),
        "FLOAT64" => Ok("FLOAT"),
        "BOOL" => Ok("BOOLEAN"),
        "STRUCT" => Ok("RECORD"),
        other => Err(Error::SchemaFile(format!(
            "Unknown BigQuery type: {}",
            other
        ))),
    }
}

/// Parse mode string to BqMode.
fn parse_mode(mode: Option<&str>) -> BqMode {
    match mode.map(|s| s.to_uppercase()).as_deref() {
        Some("REQUIRED") => BqMode::Required,
        Some("REPEATED") => BqMode::Repeated,
        _ => BqMode::Nullable, // Default to NULLABLE if not specified
    }
}

/// Convert a BigQuery schema JSON to internal SchemaMap.
pub fn bq_schema_to_map(schema: &serde_json::Value) -> Result<SchemaMap> {
    let fields = match schema {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Object(obj) => {
            // Handle {"fields": [...]} format
            obj.get("fields")
                .and_then(|f| f.as_array())
                .ok_or_else(|| {
                    Error::SchemaFile("Expected 'fields' array in schema object".to_string())
                })?
        }
        _ => {
            return Err(Error::SchemaFile(
                "Schema must be an array or object with 'fields'".to_string(),
            ))
        }
    };

    let mut schema_map = SchemaMap::new();

    for field in fields {
        let field_obj = field
            .as_object()
            .ok_or_else(|| Error::SchemaFile("Each field must be an object".to_string()))?;

        let name = field_obj
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::SchemaFile("Field must have 'name'".to_string()))?;

        let type_str = field_obj
            .get("type")
            .and_then(|t| t.as_str())
            .ok_or_else(|| Error::SchemaFile("Field must have 'type'".to_string()))?;

        let normalized_type = normalize_bq_type(type_str)?;
        let mode = parse_mode(field_obj.get("mode").and_then(|m| m.as_str()));

        let bq_type = if normalized_type == "RECORD" {
            let nested_fields = field_obj
                .get("fields")
                .ok_or_else(|| Error::SchemaFile("RECORD field must have 'fields'".to_string()))?;
            let nested_map = bq_schema_to_map(nested_fields)?;
            BqType::Record(nested_map)
        } else {
            match normalized_type {
                "STRING" => BqType::String,
                "INTEGER" => BqType::Integer,
                "FLOAT" => BqType::Float,
                "BOOLEAN" => BqType::Boolean,
                "TIMESTAMP" => BqType::Timestamp,
                "DATE" => BqType::Date,
                "TIME" => BqType::Time,
                "DATETIME" => BqType::Timestamp, // DATETIME maps to TIMESTAMP
                "BYTES" => BqType::String,       // BYTES maps to STRING for our purposes
                _ => {
                    return Err(Error::SchemaFile(format!(
                        "Unsupported type: {}",
                        normalized_type
                    )))
                }
            }
        };

        // Existing schema entries are always "hard" and filled based on mode
        let entry = SchemaEntry {
            status: EntryStatus::Hard,
            filled: mode != BqMode::Nullable,
            name: name.to_string(),
            bq_type,
            mode,
        };

        schema_map.insert(name.to_lowercase(), entry);
    }

    Ok(schema_map)
}

/// Read an existing BigQuery schema from a JSON file.
pub fn read_existing_schema_from_file<P: AsRef<Path>>(path: P) -> Result<SchemaMap> {
    let file = File::open(path.as_ref())
        .map_err(|e| Error::SchemaFile(format!("Cannot open schema file: {}", e)))?;

    let reader = BufReader::new(file);
    let schema: serde_json::Value = serde_json::from_reader(reader)
        .map_err(|e| Error::SchemaFile(format!("Cannot parse schema JSON: {}", e)))?;

    bq_schema_to_map(&schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_schema() {
        let schema = json!([
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "score", "type": "FLOAT"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 3);
        assert!(map.contains_key("id"));
        assert!(map.contains_key("name"));
        assert!(map.contains_key("score"));
    }

    #[test]
    fn test_nested_record() {
        let schema = json!([
            {
                "name": "user",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "email", "type": "STRING"},
                    {"name": "age", "type": "INTEGER"}
                ]
            }
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 1);

        let user = map.get("user").unwrap();
        if let BqType::Record(fields) = &user.bq_type {
            assert_eq!(fields.len(), 2);
        } else {
            panic!("Expected RECORD type");
        }
    }

    #[test]
    fn test_type_aliases() {
        let schema = json!([
            {"name": "a", "type": "INT64"},
            {"name": "b", "type": "FLOAT64"},
            {"name": "c", "type": "BOOL"},
            {"name": "d", "type": "STRUCT", "fields": [{"name": "x", "type": "STRING"}]}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 4);

        assert!(matches!(map.get("a").unwrap().bq_type, BqType::Integer));
        assert!(matches!(map.get("b").unwrap().bq_type, BqType::Float));
        assert!(matches!(map.get("c").unwrap().bq_type, BqType::Boolean));
        assert!(matches!(map.get("d").unwrap().bq_type, BqType::Record(_)));
    }

    #[test]
    fn test_repeated_mode() {
        let schema = json!([
            {"name": "tags", "type": "STRING", "mode": "REPEATED"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        let tags = map.get("tags").unwrap();
        assert_eq!(tags.mode, BqMode::Repeated);
    }

    #[test]
    fn test_object_with_fields() {
        let schema = json!({
            "fields": [
                {"name": "id", "type": "INTEGER"}
            ]
        });

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 1);
    }
}
