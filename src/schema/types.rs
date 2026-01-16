//! Core types for BigQuery schema representation.
//!
//! This module defines the fundamental types used to represent BigQuery schemas,
//! matching the behavior of the `bq load` command.

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::fmt;

/// BigQuery data types.
///
/// These correspond to the Legacy SQL types used by BigQuery's schema system.
/// See: <https://cloud.google.com/bigquery/docs/schemas>
#[derive(Debug, Clone, PartialEq)]
pub enum BqType {
    // Standard BigQuery types
    Boolean,
    Integer,
    Float,
    String,
    Timestamp,
    Date,
    Time,
    Record(SchemaMap),

    // Internal types for tracking inference state
    /// Null value - will become STRING if keep_nulls is enabled
    Null,
    /// Empty array `[]` - will become REPEATED STRING if keep_nulls is enabled
    EmptyArray,
    /// Empty record `{}` - will become RECORD with placeholder field
    EmptyRecord,

    // Quoted types - used internally to track type inference from quoted strings
    /// Quoted boolean like `"true"` or `"false"`
    QBoolean,
    /// Quoted integer like `"123"`
    QInteger,
    /// Quoted float like `"1.5"` or `"1e10"`
    QFloat,
}

impl BqType {
    /// Returns the BigQuery type name as used in schema JSON output.
    pub fn as_str(&self) -> &'static str {
        match self {
            BqType::Boolean | BqType::QBoolean => "BOOLEAN",
            BqType::Integer | BqType::QInteger => "INTEGER",
            BqType::Float | BqType::QFloat => "FLOAT",
            BqType::String => "STRING",
            BqType::Timestamp => "TIMESTAMP",
            BqType::Date => "DATE",
            BqType::Time => "TIME",
            BqType::Record(_) | BqType::EmptyRecord => "RECORD",
            BqType::Null | BqType::EmptyArray => "STRING",
        }
    }

    /// Returns true if this is an internal/special type (starts with __ in Python version).
    pub fn is_internal(&self) -> bool {
        matches!(
            self,
            BqType::Null | BqType::EmptyArray | BqType::EmptyRecord
        )
    }

    /// Returns true if this is a quoted type.
    pub fn is_quoted(&self) -> bool {
        matches!(self, BqType::QBoolean | BqType::QInteger | BqType::QFloat)
    }

    /// Returns true if this type can be represented as a string in BigQuery.
    /// Used for type coercion when types conflict.
    pub fn is_string_compatible(&self) -> bool {
        matches!(
            self,
            BqType::String
                | BqType::Timestamp
                | BqType::Date
                | BqType::Time
                | BqType::QInteger
                | BqType::QFloat
                | BqType::QBoolean
        )
    }
}

impl fmt::Display for BqType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// BigQuery field mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum BqMode {
    /// Field can be null
    Nullable,
    /// Field must have a value (only inferred from CSV with --infer_mode)
    Required,
    /// Field is an array
    Repeated,
}

impl BqMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            BqMode::Nullable => "NULLABLE",
            BqMode::Required => "REQUIRED",
            BqMode::Repeated => "REPEATED",
        }
    }
}

impl fmt::Display for BqMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Status of a schema entry's type inference.
///
/// This tracks how confident we are about a field's type:
/// - `Hard`: The type has been definitively determined from a non-null, non-empty value
/// - `Soft`: The type is provisional (from null or empty values) and can be overwritten
/// - `Ignore`: The field has conflicting types across records and should be excluded
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryStatus {
    /// Type definitively determined from actual values
    Hard,
    /// Type is provisional (from null/empty), can be overwritten
    Soft,
    /// Field has conflicting types, should be ignored in output
    Ignore,
}

/// A single field entry in the schema, tracking both the schema info and inference state.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaEntry {
    /// How confident we are about this field's type
    pub status: EntryStatus,
    /// Whether all records so far have had a value for this field
    pub filled: bool,
    /// The field name (possibly sanitized)
    pub name: String,
    /// The inferred BigQuery type
    pub bq_type: BqType,
    /// The field mode (NULLABLE, REQUIRED, REPEATED)
    pub mode: BqMode,
}

impl SchemaEntry {
    /// Create a new schema entry with hard status.
    pub fn new(name: String, bq_type: BqType, mode: BqMode) -> Self {
        Self {
            status: EntryStatus::Hard,
            filled: true,
            name,
            bq_type,
            mode,
        }
    }

    /// Create a soft entry (from null or empty value).
    pub fn soft(name: String, bq_type: BqType, mode: BqMode) -> Self {
        Self {
            status: EntryStatus::Soft,
            filled: false,
            name,
            bq_type,
            mode,
        }
    }
}

/// A map of field names (lowercase/canonical) to their schema entries.
///
/// We use IndexMap to preserve insertion order, which is important for
/// the `--preserve_input_sort_order` flag.
pub type SchemaMap = IndexMap<String, SchemaEntry>;

/// BigQuery schema output format - a single field in the schema array.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BqSchemaField {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fields: Option<Vec<BqSchemaField>>,
    pub mode: String,
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
}

impl BqSchemaField {
    /// Create a new schema field for JSON output.
    pub fn new(name: String, field_type: String, mode: String) -> Self {
        Self {
            fields: None,
            mode,
            name,
            field_type,
        }
    }

    /// Create a RECORD field with nested fields.
    pub fn record(name: String, mode: String, fields: Vec<BqSchemaField>) -> Self {
        Self {
            fields: Some(fields),
            mode,
            name,
            field_type: "RECORD".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bq_type_as_str() {
        assert_eq!(BqType::Boolean.as_str(), "BOOLEAN");
        assert_eq!(BqType::QBoolean.as_str(), "BOOLEAN");
        assert_eq!(BqType::Integer.as_str(), "INTEGER");
        assert_eq!(BqType::QInteger.as_str(), "INTEGER");
        assert_eq!(BqType::Float.as_str(), "FLOAT");
        assert_eq!(BqType::QFloat.as_str(), "FLOAT");
        assert_eq!(BqType::String.as_str(), "STRING");
        assert_eq!(BqType::Timestamp.as_str(), "TIMESTAMP");
        assert_eq!(BqType::Date.as_str(), "DATE");
        assert_eq!(BqType::Time.as_str(), "TIME");
        assert_eq!(BqType::Null.as_str(), "STRING");
        assert_eq!(BqType::EmptyArray.as_str(), "STRING");
    }

    #[test]
    fn test_bq_mode_as_str() {
        assert_eq!(BqMode::Nullable.as_str(), "NULLABLE");
        assert_eq!(BqMode::Required.as_str(), "REQUIRED");
        assert_eq!(BqMode::Repeated.as_str(), "REPEATED");
    }

    #[test]
    fn test_is_string_compatible() {
        assert!(BqType::String.is_string_compatible());
        assert!(BqType::Timestamp.is_string_compatible());
        assert!(BqType::Date.is_string_compatible());
        assert!(BqType::Time.is_string_compatible());
        assert!(BqType::QInteger.is_string_compatible());
        assert!(BqType::QFloat.is_string_compatible());
        assert!(BqType::QBoolean.is_string_compatible());

        assert!(!BqType::Boolean.is_string_compatible());
        assert!(!BqType::Integer.is_string_compatible());
        assert!(!BqType::Float.is_string_compatible());
    }

    // ===== Additional Coverage Tests =====

    #[test]
    fn test_is_internal_all_types() {
        // Internal types
        assert!(BqType::Null.is_internal());
        assert!(BqType::EmptyArray.is_internal());
        assert!(BqType::EmptyRecord.is_internal());

        // Non-internal types
        assert!(!BqType::String.is_internal());
        assert!(!BqType::Integer.is_internal());
        assert!(!BqType::Float.is_internal());
        assert!(!BqType::Boolean.is_internal());
        assert!(!BqType::Timestamp.is_internal());
        assert!(!BqType::Date.is_internal());
        assert!(!BqType::Time.is_internal());
        assert!(!BqType::Record(SchemaMap::new()).is_internal());

        // Quoted types
        assert!(!BqType::QBoolean.is_internal());
        assert!(!BqType::QInteger.is_internal());
        assert!(!BqType::QFloat.is_internal());
    }

    #[test]
    fn test_is_quoted_all_types() {
        // Quoted types
        assert!(BqType::QBoolean.is_quoted());
        assert!(BqType::QInteger.is_quoted());
        assert!(BqType::QFloat.is_quoted());

        // Non-quoted types
        assert!(!BqType::String.is_quoted());
        assert!(!BqType::Integer.is_quoted());
        assert!(!BqType::Float.is_quoted());
        assert!(!BqType::Boolean.is_quoted());
        assert!(!BqType::Timestamp.is_quoted());
        assert!(!BqType::Date.is_quoted());
        assert!(!BqType::Time.is_quoted());
        assert!(!BqType::Null.is_quoted());
        assert!(!BqType::EmptyArray.is_quoted());
        assert!(!BqType::EmptyRecord.is_quoted());
        assert!(!BqType::Record(SchemaMap::new()).is_quoted());
    }

    #[test]
    fn test_is_string_compatible_matrix() {
        // String compatible types (can be merged with String to produce String)
        let string_compatible_types = vec![
            BqType::QBoolean,
            BqType::QInteger,
            BqType::QFloat,
            BqType::String,
            BqType::Timestamp,
            BqType::Date,
            BqType::Time,
        ];

        for t in &string_compatible_types {
            assert!(
                t.is_string_compatible(),
                "{:?} should be string compatible",
                t
            );
        }

        // Non-string compatible types
        let non_string_compatible = vec![
            BqType::Boolean,
            BqType::Integer,
            BqType::Float,
            BqType::Null,
            BqType::EmptyArray,
            BqType::EmptyRecord,
            BqType::Record(SchemaMap::new()),
        ];

        for t in &non_string_compatible {
            assert!(
                !t.is_string_compatible(),
                "{:?} should NOT be string compatible",
                t
            );
        }
    }

    #[test]
    fn test_bq_type_display_all_types() {
        assert_eq!(BqType::Boolean.to_string(), "BOOLEAN");
        assert_eq!(BqType::QBoolean.to_string(), "BOOLEAN");
        assert_eq!(BqType::Integer.to_string(), "INTEGER");
        assert_eq!(BqType::QInteger.to_string(), "INTEGER");
        assert_eq!(BqType::Float.to_string(), "FLOAT");
        assert_eq!(BqType::QFloat.to_string(), "FLOAT");
        assert_eq!(BqType::String.to_string(), "STRING");
        assert_eq!(BqType::Timestamp.to_string(), "TIMESTAMP");
        assert_eq!(BqType::Date.to_string(), "DATE");
        assert_eq!(BqType::Time.to_string(), "TIME");
        assert_eq!(BqType::Record(SchemaMap::new()).to_string(), "RECORD");
        assert_eq!(BqType::Null.to_string(), "STRING");
        assert_eq!(BqType::EmptyArray.to_string(), "STRING");
        assert_eq!(BqType::EmptyRecord.to_string(), "RECORD");
    }

    #[test]
    fn test_bq_mode_display() {
        assert_eq!(BqMode::Nullable.to_string(), "NULLABLE");
        assert_eq!(BqMode::Required.to_string(), "REQUIRED");
        assert_eq!(BqMode::Repeated.to_string(), "REPEATED");
    }

    #[test]
    fn test_entry_status_variants() {
        // Test all variants
        let hard = EntryStatus::Hard;
        let soft = EntryStatus::Soft;
        let ignore = EntryStatus::Ignore;
        assert!(format!("{:?}", hard).contains("Hard"));
        assert!(format!("{:?}", soft).contains("Soft"));
        assert!(format!("{:?}", ignore).contains("Ignore"));

        // Test equality
        assert_eq!(EntryStatus::Hard, EntryStatus::Hard);
        assert_ne!(EntryStatus::Hard, EntryStatus::Soft);
        assert_ne!(EntryStatus::Soft, EntryStatus::Ignore);
    }

    #[test]
    fn test_schema_entry_new() {
        let entry = SchemaEntry::new("field".to_string(), BqType::String, BqMode::Nullable);
        assert_eq!(entry.name, "field");
        assert_eq!(entry.status, EntryStatus::Hard);
        assert!(entry.filled);
        assert_eq!(entry.mode, BqMode::Nullable);
    }

    #[test]
    fn test_schema_entry_soft() {
        let entry = SchemaEntry::soft("soft_field".to_string(), BqType::Null, BqMode::Nullable);
        assert_eq!(entry.name, "soft_field");
        assert_eq!(entry.status, EntryStatus::Soft);
        assert!(!entry.filled);
        assert_eq!(entry.bq_type, BqType::Null);
    }

    #[test]
    fn test_schema_entry_with_record() {
        let mut nested = SchemaMap::new();
        nested.insert(
            "child".to_string(),
            SchemaEntry::new("child".to_string(), BqType::Integer, BqMode::Nullable),
        );

        let entry = SchemaEntry::new(
            "parent".to_string(),
            BqType::Record(nested.clone()),
            BqMode::Nullable,
        );

        assert_eq!(entry.name, "parent");
        match &entry.bq_type {
            BqType::Record(fields) => {
                assert!(fields.contains_key("child"));
            }
            _ => panic!("Expected Record type"),
        }
    }

    #[test]
    fn test_bq_schema_field_new() {
        let field = BqSchemaField::new(
            "test".to_string(),
            "STRING".to_string(),
            "NULLABLE".to_string(),
        );
        assert_eq!(field.name, "test");
        assert_eq!(field.field_type, "STRING");
        assert_eq!(field.mode, "NULLABLE");
        assert!(field.fields.is_none());
    }

    #[test]
    fn test_bq_schema_field_record() {
        let children = vec![
            BqSchemaField::new(
                "a".to_string(),
                "INTEGER".to_string(),
                "NULLABLE".to_string(),
            ),
            BqSchemaField::new(
                "b".to_string(),
                "STRING".to_string(),
                "NULLABLE".to_string(),
            ),
        ];

        let record = BqSchemaField::record("parent".to_string(), "NULLABLE".to_string(), children);

        assert_eq!(record.name, "parent");
        assert_eq!(record.field_type, "RECORD");
        assert_eq!(record.mode, "NULLABLE");
        assert!(record.fields.is_some());
        assert_eq!(record.fields.unwrap().len(), 2);
    }

    #[test]
    fn test_bq_schema_field_clone() {
        let original = BqSchemaField::new(
            "test".to_string(),
            "STRING".to_string(),
            "NULLABLE".to_string(),
        );
        let cloned = original.clone();

        assert_eq!(original.name, cloned.name);
        assert_eq!(original.field_type, cloned.field_type);
        assert_eq!(original.mode, cloned.mode);
    }

    #[test]
    fn test_schema_map_operations() {
        let mut map = SchemaMap::new();
        assert!(map.is_empty());

        map.insert(
            "field1".to_string(),
            SchemaEntry::new("field1".to_string(), BqType::String, BqMode::Nullable),
        );
        assert_eq!(map.len(), 1);
        assert!(!map.is_empty());

        assert!(map.contains_key("field1"));
        assert!(!map.contains_key("field2"));

        let entry = map.get("field1").unwrap();
        assert_eq!(entry.name, "field1");
    }

    #[test]
    fn test_bq_type_record_as_str() {
        let empty_record = BqType::EmptyRecord;
        let record_with_fields = BqType::Record(SchemaMap::new());

        assert_eq!(empty_record.as_str(), "RECORD");
        assert_eq!(record_with_fields.as_str(), "RECORD");
    }

    #[test]
    fn test_bq_schema_field_serialization() {
        let field = BqSchemaField::new(
            "test".to_string(),
            "STRING".to_string(),
            "NULLABLE".to_string(),
        );

        let json = serde_json::to_string(&field).unwrap();
        assert!(json.contains("\"name\":\"test\""));
        assert!(json.contains("\"type\":\"STRING\""));
        assert!(json.contains("\"mode\":\"NULLABLE\""));
        assert!(!json.contains("\"fields\"")); // Should skip fields when None
    }

    #[test]
    fn test_bq_schema_field_deserialization() {
        let json = r#"{"name":"test","type":"STRING","mode":"NULLABLE"}"#;
        let field: BqSchemaField = serde_json::from_str(json).unwrap();

        assert_eq!(field.name, "test");
        assert_eq!(field.field_type, "STRING");
        assert_eq!(field.mode, "NULLABLE");
        assert!(field.fields.is_none());
    }

    #[test]
    fn test_bq_mode_serialization() {
        let nullable = BqMode::Nullable;
        let required = BqMode::Required;
        let repeated = BqMode::Repeated;

        assert_eq!(serde_json::to_string(&nullable).unwrap(), "\"NULLABLE\"");
        assert_eq!(serde_json::to_string(&required).unwrap(), "\"REQUIRED\"");
        assert_eq!(serde_json::to_string(&repeated).unwrap(), "\"REPEATED\"");
    }
}
