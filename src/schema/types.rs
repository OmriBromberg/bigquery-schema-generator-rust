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
/// See: https://cloud.google.com/bigquery/docs/schemas
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
}
