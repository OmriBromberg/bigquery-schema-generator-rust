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

    // ===== Additional Coverage Tests =====

    #[test]
    fn test_unknown_bq_type_handling() {
        let schema = json!([
            {"name": "field", "type": "UNKNOWN_TYPE", "mode": "NULLABLE"}
        ]);

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Unknown BigQuery type"));
    }

    #[test]
    fn test_record_without_fields_error() {
        let schema = json!([
            {"name": "record_field", "type": "RECORD", "mode": "NULLABLE"}
        ]);

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must have 'fields'"));
    }

    #[test]
    fn test_type_alias_normalization_all() {
        // Test all type aliases
        let schema = json!([
            {"name": "a", "type": "STRING"},
            {"name": "b", "type": "BYTES"},
            {"name": "c", "type": "INTEGER"},
            {"name": "d", "type": "FLOAT"},
            {"name": "e", "type": "BOOLEAN"},
            {"name": "f", "type": "TIMESTAMP"},
            {"name": "g", "type": "DATE"},
            {"name": "h", "type": "TIME"},
            {"name": "i", "type": "DATETIME"},
            {"name": "j", "type": "INT64"},
            {"name": "k", "type": "FLOAT64"},
            {"name": "l", "type": "BOOL"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 12);

        // Check type mappings
        assert!(matches!(map.get("a").unwrap().bq_type, BqType::String));
        assert!(matches!(map.get("b").unwrap().bq_type, BqType::String)); // BYTES -> STRING
        assert!(matches!(map.get("c").unwrap().bq_type, BqType::Integer));
        assert!(matches!(map.get("d").unwrap().bq_type, BqType::Float));
        assert!(matches!(map.get("e").unwrap().bq_type, BqType::Boolean));
        assert!(matches!(map.get("f").unwrap().bq_type, BqType::Timestamp));
        assert!(matches!(map.get("g").unwrap().bq_type, BqType::Date));
        assert!(matches!(map.get("h").unwrap().bq_type, BqType::Time));
        assert!(matches!(map.get("i").unwrap().bq_type, BqType::Timestamp)); // DATETIME -> TIMESTAMP
        assert!(matches!(map.get("j").unwrap().bq_type, BqType::Integer)); // INT64 -> INTEGER
        assert!(matches!(map.get("k").unwrap().bq_type, BqType::Float)); // FLOAT64 -> FLOAT
        assert!(matches!(map.get("l").unwrap().bq_type, BqType::Boolean)); // BOOL -> BOOLEAN
    }

    #[test]
    fn test_missing_name_error() {
        let schema = json!([
            {"type": "STRING", "mode": "NULLABLE"}
        ]);

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must have 'name'"));
    }

    #[test]
    fn test_missing_type_error() {
        let schema = json!([
            {"name": "field", "mode": "NULLABLE"}
        ]);

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must have 'type'"));
    }

    #[test]
    fn test_field_not_object_error() {
        let schema = json!(["not an object"]);

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("must be an object"));
    }

    #[test]
    fn test_invalid_schema_root_type() {
        let schema = json!("just a string");

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("array or object"));
    }

    #[test]
    fn test_object_without_fields_key() {
        let schema = json!({
            "other": "data"
        });

        let result = bq_schema_to_map(&schema);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("fields"));
    }

    #[test]
    fn test_deeply_nested_records() {
        let schema = json!([
            {
                "name": "level1",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {
                        "name": "level2",
                        "type": "RECORD",
                        "mode": "NULLABLE",
                        "fields": [
                            {
                                "name": "level3",
                                "type": "RECORD",
                                "mode": "NULLABLE",
                                "fields": [
                                    {"name": "value", "type": "STRING"}
                                ]
                            }
                        ]
                    }
                ]
            }
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 1);

        let level1 = map.get("level1").unwrap();
        if let BqType::Record(fields1) = &level1.bq_type {
            let level2 = fields1.get("level2").unwrap();
            if let BqType::Record(fields2) = &level2.bq_type {
                let level3 = fields2.get("level3").unwrap();
                if let BqType::Record(fields3) = &level3.bq_type {
                    assert!(fields3.contains_key("value"));
                } else {
                    panic!("Expected RECORD at level3");
                }
            } else {
                panic!("Expected RECORD at level2");
            }
        } else {
            panic!("Expected RECORD at level1");
        }
    }

    #[test]
    fn test_case_insensitive_type_names() {
        let schema = json!([
            {"name": "a", "type": "string"},
            {"name": "b", "type": "Integer"},
            {"name": "c", "type": "BOOLEAN"},
            {"name": "d", "type": "TiMeStAmP"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.len(), 4);

        assert!(matches!(map.get("a").unwrap().bq_type, BqType::String));
        assert!(matches!(map.get("b").unwrap().bq_type, BqType::Integer));
        assert!(matches!(map.get("c").unwrap().bq_type, BqType::Boolean));
        assert!(matches!(map.get("d").unwrap().bq_type, BqType::Timestamp));
    }

    #[test]
    fn test_case_insensitive_mode() {
        let schema = json!([
            {"name": "a", "type": "STRING", "mode": "required"},
            {"name": "b", "type": "STRING", "mode": "REPEATED"},
            {"name": "c", "type": "STRING", "mode": "Nullable"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();

        assert_eq!(map.get("a").unwrap().mode, BqMode::Required);
        assert_eq!(map.get("b").unwrap().mode, BqMode::Repeated);
        assert_eq!(map.get("c").unwrap().mode, BqMode::Nullable);
    }

    #[test]
    fn test_default_mode_is_nullable() {
        let schema = json!([
            {"name": "field", "type": "STRING"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.get("field").unwrap().mode, BqMode::Nullable);
    }

    #[test]
    fn test_filled_based_on_mode() {
        let schema = json!([
            {"name": "required_field", "type": "STRING", "mode": "REQUIRED"},
            {"name": "repeated_field", "type": "STRING", "mode": "REPEATED"},
            {"name": "nullable_field", "type": "STRING", "mode": "NULLABLE"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();

        // REQUIRED and REPEATED are filled, NULLABLE is not
        assert!(map.get("required_field").unwrap().filled);
        assert!(map.get("repeated_field").unwrap().filled);
        assert!(!map.get("nullable_field").unwrap().filled);
    }

    #[test]
    fn test_entry_status_is_hard() {
        let schema = json!([
            {"name": "field", "type": "STRING"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert_eq!(map.get("field").unwrap().status, EntryStatus::Hard);
    }

    #[test]
    fn test_name_stored_lowercase() {
        let schema = json!([
            {"name": "MixedCaseField", "type": "STRING"}
        ]);

        let map = bq_schema_to_map(&schema).unwrap();

        // Key is lowercase
        assert!(map.contains_key("mixedcasefield"));
        assert!(!map.contains_key("MixedCaseField"));

        // But original name is preserved
        assert_eq!(map.get("mixedcasefield").unwrap().name, "MixedCaseField");
    }

    #[test]
    fn test_empty_schema_array() {
        let schema = json!([]);

        let map = bq_schema_to_map(&schema).unwrap();
        assert!(map.is_empty());
    }

    #[test]
    fn test_repeated_record() {
        let schema = json!([
            {
                "name": "items",
                "type": "RECORD",
                "mode": "REPEATED",
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "STRING"}
                ]
            }
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        let items = map.get("items").unwrap();

        assert_eq!(items.mode, BqMode::Repeated);
        assert!(matches!(items.bq_type, BqType::Record(_)));
    }

    #[test]
    fn test_struct_alias_with_nested() {
        let schema = json!([
            {
                "name": "data",
                "type": "STRUCT",
                "fields": [
                    {"name": "x", "type": "INT64"},
                    {"name": "y", "type": "FLOAT64"}
                ]
            }
        ]);

        let map = bq_schema_to_map(&schema).unwrap();
        let data = map.get("data").unwrap();

        if let BqType::Record(fields) = &data.bq_type {
            assert!(matches!(fields.get("x").unwrap().bq_type, BqType::Integer));
            assert!(matches!(fields.get("y").unwrap().bq_type, BqType::Float));
        } else {
            panic!("Expected RECORD type for STRUCT alias");
        }
    }
}
