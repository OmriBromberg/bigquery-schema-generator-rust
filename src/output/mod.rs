//! Output formatting for BigQuery schema.
//!
//! Supports multiple output formats:
//! - JSON: Standard BigQuery schema format (default)
//! - DDL: BigQuery CREATE TABLE statement
//! - Debug Map: Internal schema representation for debugging
//! - JSON Schema: JSON Schema draft-07 format

use std::io::Write;

use serde::Serialize;

use crate::error::Result;
use crate::schema::types::{BqType, EntryStatus, SchemaEntry, SchemaMap};
use crate::schema::BqSchemaField;

/// Output format for the generated schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OutputFormat {
    /// Standard BigQuery JSON schema format (default)
    #[default]
    Json,
    /// BigQuery DDL (CREATE TABLE statement)
    Ddl,
    /// Debug map showing internal schema state
    DebugMap,
    /// JSON Schema draft-07 format
    JsonSchema,
}

impl std::str::FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "ddl" => Ok(OutputFormat::Ddl),
            "debug-map" | "debug_map" | "debugmap" => Ok(OutputFormat::DebugMap),
            "json-schema" | "json_schema" | "jsonschema" => Ok(OutputFormat::JsonSchema),
            _ => Err(format!("Unknown output format: {}", s)),
        }
    }
}

// =============================================================================
// JSON Output (Default)
// =============================================================================

/// Write the schema as pretty-printed JSON to the given writer.
pub fn write_schema_json<W: Write>(schema: &[BqSchemaField], writer: &mut W) -> Result<()> {
    let json = serde_json::to_string_pretty(schema)
        .map_err(|e| crate::error::Error::SchemaFile(e.to_string()))?;
    writeln!(writer, "{}", json)?;
    Ok(())
}

/// Convert the schema to a JSON string.
pub fn schema_to_json_string(schema: &[BqSchemaField]) -> Result<String> {
    serde_json::to_string_pretty(schema).map_err(|e| crate::error::Error::SchemaFile(e.to_string()))
}

// =============================================================================
// DDL Output
// =============================================================================

/// Write the schema as BigQuery DDL (CREATE TABLE statement).
///
/// Output format:
/// ```sql
/// CREATE TABLE `dataset.table_name` (
///   field_name STRING,
///   required_field INT64 NOT NULL,
///   array_field ARRAY<STRING>,
///   record_field STRUCT<nested STRING>
/// );
/// ```
pub fn write_schema_ddl<W: Write>(
    schema: &[BqSchemaField],
    table_name: &str,
    writer: &mut W,
) -> Result<()> {
    writeln!(writer, "CREATE TABLE `{}` (", table_name)?;

    let fields: Vec<String> = schema.iter().map(field_to_ddl).collect();

    for (i, field) in fields.iter().enumerate() {
        if i < fields.len() - 1 {
            writeln!(writer, "  {},", field)?;
        } else {
            writeln!(writer, "  {}", field)?;
        }
    }

    writeln!(writer, ");")?;
    Ok(())
}

/// Convert a single field to DDL format.
fn field_to_ddl(field: &BqSchemaField) -> String {
    let type_str = bq_type_to_standard_sql(&field.field_type);
    let mode = field.mode.as_str();

    match mode {
        "REPEATED" => {
            if field.field_type == "RECORD" {
                let nested = field
                    .fields
                    .as_ref()
                    .map(|f| fields_to_struct(f))
                    .unwrap_or_default();
                format!("{} ARRAY<STRUCT<{}>>", field.name, nested)
            } else {
                format!("{} ARRAY<{}>", field.name, type_str)
            }
        }
        "REQUIRED" => {
            if field.field_type == "RECORD" {
                let nested = field
                    .fields
                    .as_ref()
                    .map(|f| fields_to_struct(f))
                    .unwrap_or_default();
                format!("{} STRUCT<{}> NOT NULL", field.name, nested)
            } else {
                format!("{} {} NOT NULL", field.name, type_str)
            }
        }
        _ => {
            // NULLABLE
            if field.field_type == "RECORD" {
                let nested = field
                    .fields
                    .as_ref()
                    .map(|f| fields_to_struct(f))
                    .unwrap_or_default();
                format!("{} STRUCT<{}>", field.name, nested)
            } else {
                format!("{} {}", field.name, type_str)
            }
        }
    }
}

/// Convert nested fields to STRUCT notation.
fn fields_to_struct(fields: &[BqSchemaField]) -> String {
    fields
        .iter()
        .map(|f| {
            let type_str = bq_type_to_standard_sql(&f.field_type);
            if f.field_type == "RECORD" {
                let nested = f
                    .fields
                    .as_ref()
                    .map(|inner| fields_to_struct(inner))
                    .unwrap_or_default();
                if f.mode == "REPEATED" {
                    format!("{} ARRAY<STRUCT<{}>>", f.name, nested)
                } else {
                    format!("{} STRUCT<{}>", f.name, nested)
                }
            } else if f.mode == "REPEATED" {
                format!("{} ARRAY<{}>", f.name, type_str)
            } else {
                format!("{} {}", f.name, type_str)
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// Convert legacy BigQuery type names to Standard SQL types.
fn bq_type_to_standard_sql(legacy_type: &str) -> &'static str {
    match legacy_type {
        "INTEGER" => "INT64",
        "FLOAT" => "FLOAT64",
        "BOOLEAN" => "BOOL",
        "STRING" => "STRING",
        "BYTES" => "BYTES",
        "TIMESTAMP" => "TIMESTAMP",
        "DATE" => "DATE",
        "TIME" => "TIME",
        "DATETIME" => "DATETIME",
        "RECORD" => "STRUCT",
        _ => "STRING", // Fallback
    }
}

// =============================================================================
// Debug Map Output
// =============================================================================

/// Serializable representation of a schema entry for debug output.
#[derive(Debug, Serialize)]
struct DebugSchemaEntry {
    status: String,
    filled: bool,
    name: String,
    bq_type: String,
    mode: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<serde_json::Value>,
}

impl From<&SchemaEntry> for DebugSchemaEntry {
    fn from(entry: &SchemaEntry) -> Self {
        let status = match entry.status {
            EntryStatus::Hard => "Hard",
            EntryStatus::Soft => "Soft",
            EntryStatus::Ignore => "Ignore",
        };

        let fields = if let BqType::Record(map) = &entry.bq_type {
            Some(schema_map_to_debug_value(map))
        } else {
            None
        };

        DebugSchemaEntry {
            status: status.to_string(),
            filled: entry.filled,
            name: entry.name.clone(),
            bq_type: entry.bq_type.as_str().to_string(),
            mode: entry.mode.as_str().to_string(),
            fields,
        }
    }
}

/// Convert a SchemaMap to debug JSON value.
fn schema_map_to_debug_value(map: &SchemaMap) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    for (key, entry) in map {
        let debug_entry = DebugSchemaEntry::from(entry);
        obj.insert(
            key.clone(),
            serde_json::to_value(&debug_entry).unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::Value::Object(obj)
}

/// Write the internal schema map as debug JSON output.
///
/// This shows the internal representation including entry status (Hard/Soft/Ignore),
/// filled state, and other metadata useful for debugging.
pub fn write_schema_debug_map<W: Write>(schema_map: &SchemaMap, writer: &mut W) -> Result<()> {
    let debug_value = schema_map_to_debug_value(schema_map);
    let json = serde_json::to_string_pretty(&debug_value)
        .map_err(|e| crate::error::Error::SchemaFile(e.to_string()))?;
    writeln!(writer, "{}", json)?;
    Ok(())
}

// =============================================================================
// JSON Schema Output
// =============================================================================

/// Write the schema as JSON Schema draft-07 format.
pub fn write_schema_json_schema<W: Write>(
    schema: &[BqSchemaField],
    writer: &mut W,
) -> Result<()> {
    let json_schema = bq_schema_to_json_schema(schema);
    let json = serde_json::to_string_pretty(&json_schema)
        .map_err(|e| crate::error::Error::SchemaFile(e.to_string()))?;
    writeln!(writer, "{}", json)?;
    Ok(())
}

/// Convert BigQuery schema to JSON Schema format.
fn bq_schema_to_json_schema(schema: &[BqSchemaField]) -> serde_json::Value {
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();

    for field in schema {
        let (prop_schema, is_required) = field_to_json_schema(field);
        properties.insert(field.name.clone(), prop_schema);
        if is_required {
            required.push(serde_json::Value::String(field.name.clone()));
        }
    }

    let mut schema_obj = serde_json::Map::new();
    schema_obj.insert(
        "$schema".to_string(),
        serde_json::Value::String("http://json-schema.org/draft-07/schema#".to_string()),
    );
    schema_obj.insert(
        "type".to_string(),
        serde_json::Value::String("object".to_string()),
    );
    schema_obj.insert(
        "properties".to_string(),
        serde_json::Value::Object(properties),
    );
    if !required.is_empty() {
        schema_obj.insert("required".to_string(), serde_json::Value::Array(required));
    }

    serde_json::Value::Object(schema_obj)
}

/// Convert a single BigQuery field to JSON Schema property.
/// Returns (schema, is_required).
fn field_to_json_schema(field: &BqSchemaField) -> (serde_json::Value, bool) {
    let is_required = field.mode == "REQUIRED";
    let base_type = bq_type_to_json_schema_type(&field.field_type);

    let schema = if field.mode == "REPEATED" {
        // Array type
        let mut arr_schema = serde_json::Map::new();
        arr_schema.insert(
            "type".to_string(),
            serde_json::Value::String("array".to_string()),
        );

        if field.field_type == "RECORD" {
            arr_schema.insert("items".to_string(), record_to_json_schema(field));
        } else {
            let mut items = serde_json::Map::new();
            items.insert("type".to_string(), serde_json::Value::String(base_type));
            arr_schema.insert("items".to_string(), serde_json::Value::Object(items));
        }

        serde_json::Value::Object(arr_schema)
    } else if field.field_type == "RECORD" {
        record_to_json_schema(field)
    } else {
        // Simple type
        let mut prop = serde_json::Map::new();
        prop.insert("type".to_string(), serde_json::Value::String(base_type));
        serde_json::Value::Object(prop)
    };

    (schema, is_required)
}

/// Convert a RECORD field to JSON Schema object.
fn record_to_json_schema(field: &BqSchemaField) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert(
        "type".to_string(),
        serde_json::Value::String("object".to_string()),
    );

    if let Some(nested_fields) = &field.fields {
        let mut properties = serde_json::Map::new();
        let mut required = Vec::new();

        for nested in nested_fields {
            let (nested_schema, is_required) = field_to_json_schema(nested);
            properties.insert(nested.name.clone(), nested_schema);
            if is_required {
                required.push(serde_json::Value::String(nested.name.clone()));
            }
        }

        obj.insert(
            "properties".to_string(),
            serde_json::Value::Object(properties),
        );
        if !required.is_empty() {
            obj.insert("required".to_string(), serde_json::Value::Array(required));
        }
    }

    serde_json::Value::Object(obj)
}

/// Convert BigQuery type to JSON Schema type.
fn bq_type_to_json_schema_type(bq_type: &str) -> String {
    match bq_type {
        "STRING" => "string",
        "INTEGER" => "integer",
        "FLOAT" => "number",
        "BOOLEAN" => "boolean",
        "TIMESTAMP" | "DATE" | "TIME" | "DATETIME" => "string", // DateTime types as strings
        "BYTES" => "string",
        "RECORD" => "object",
        _ => "string",
    }
    .to_string()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_schema_json() {
        let schema = vec![BqSchemaField::new(
            "test".to_string(),
            "STRING".to_string(),
            "NULLABLE".to_string(),
        )];

        let mut output = Vec::new();
        write_schema_json(&schema, &mut output).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("\"name\": \"test\""));
        assert!(output_str.contains("\"type\": \"STRING\""));
        assert!(output_str.contains("\"mode\": \"NULLABLE\""));
    }

    #[test]
    fn test_write_schema_ddl_simple() {
        let schema = vec![
            BqSchemaField::new("name".to_string(), "STRING".to_string(), "NULLABLE".to_string()),
            BqSchemaField::new("age".to_string(), "INTEGER".to_string(), "REQUIRED".to_string()),
        ];

        let mut output = Vec::new();
        write_schema_ddl(&schema, "my_dataset.my_table", &mut output).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("CREATE TABLE `my_dataset.my_table`"));
        assert!(output_str.contains("name STRING"));
        assert!(output_str.contains("age INT64 NOT NULL"));
    }

    #[test]
    fn test_write_schema_ddl_array() {
        let schema = vec![BqSchemaField::new(
            "tags".to_string(),
            "STRING".to_string(),
            "REPEATED".to_string(),
        )];

        let mut output = Vec::new();
        write_schema_ddl(&schema, "test.table", &mut output).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("tags ARRAY<STRING>"));
    }

    #[test]
    fn test_write_schema_ddl_nested() {
        let schema = vec![BqSchemaField::record(
            "user".to_string(),
            "NULLABLE".to_string(),
            vec![
                BqSchemaField::new("email".to_string(), "STRING".to_string(), "NULLABLE".to_string()),
                BqSchemaField::new("age".to_string(), "INTEGER".to_string(), "NULLABLE".to_string()),
            ],
        )];

        let mut output = Vec::new();
        write_schema_ddl(&schema, "test.table", &mut output).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("user STRUCT<"));
        assert!(output_str.contains("email STRING"));
        assert!(output_str.contains("age INT64"));
    }

    #[test]
    fn test_write_schema_json_schema() {
        let schema = vec![
            BqSchemaField::new("name".to_string(), "STRING".to_string(), "NULLABLE".to_string()),
            BqSchemaField::new("count".to_string(), "INTEGER".to_string(), "REQUIRED".to_string()),
        ];

        let mut output = Vec::new();
        write_schema_json_schema(&schema, &mut output).unwrap();

        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("\"$schema\""));
        assert!(output_str.contains("draft-07"));
        assert!(output_str.contains("\"properties\""));
        assert!(output_str.contains("\"name\""));
        assert!(output_str.contains("\"type\": \"string\""));
        assert!(output_str.contains("\"type\": \"integer\""));
        assert!(output_str.contains("\"required\""));
        assert!(output_str.contains("\"count\""));
    }

    #[test]
    fn test_output_format_from_str() {
        assert_eq!("json".parse::<OutputFormat>().unwrap(), OutputFormat::Json);
        assert_eq!("ddl".parse::<OutputFormat>().unwrap(), OutputFormat::Ddl);
        assert_eq!(
            "debug-map".parse::<OutputFormat>().unwrap(),
            OutputFormat::DebugMap
        );
        assert_eq!(
            "json-schema".parse::<OutputFormat>().unwrap(),
            OutputFormat::JsonSchema
        );
        assert!("invalid".parse::<OutputFormat>().is_err());
    }
}
