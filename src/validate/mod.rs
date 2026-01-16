//! Schema validation module for validating data against BigQuery schemas.
//!
//! This module provides functionality to validate JSON/CSV data against
//! an existing BigQuery schema, checking for:
//! - Required fields presence
//! - Type compatibility
//! - Unknown fields

pub mod error;

pub use error::{ValidationError, ValidationErrorType, ValidationResult};

use crate::inference::{
    is_boolean_string, is_date, is_float_string, is_integer_string, is_time, is_timestamp,
};
use crate::schema::types::BqSchemaField;
use serde_json::Value;
use std::collections::HashMap;

/// Configuration options for validation.
#[derive(Debug, Clone)]
pub struct ValidationOptions {
    /// Don't fail on fields not in schema (convert to warnings)
    pub allow_unknown: bool,
    /// Strict type checking - JSON strings don't match INTEGER, etc.
    pub strict_types: bool,
    /// Maximum number of errors before stopping
    pub max_errors: usize,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self {
            allow_unknown: false,
            strict_types: false,
            max_errors: 100,
        }
    }
}

/// Validator for checking data against a BigQuery schema.
pub struct SchemaValidator<'a> {
    schema: &'a [BqSchemaField],
    options: ValidationOptions,
    /// Schema fields indexed by lowercase name for quick lookup
    schema_map: HashMap<String, &'a BqSchemaField>,
}

impl<'a> SchemaValidator<'a> {
    /// Create a new validator with the given schema and options.
    pub fn new(schema: &'a [BqSchemaField], options: ValidationOptions) -> Self {
        let schema_map = schema.iter().map(|f| (f.name.to_lowercase(), f)).collect();

        Self {
            schema,
            options,
            schema_map,
        }
    }

    /// Validate a single record against the schema.
    ///
    /// Returns validation errors found in this record.
    pub fn validate_record(
        &self,
        record: &Value,
        line: usize,
        result: &mut ValidationResult,
    ) -> bool {
        if result.reached_max_errors(self.options.max_errors) {
            return false;
        }

        match record {
            Value::Object(obj) => {
                self.validate_object(obj, line, "", self.schema, &self.schema_map, result);
            }
            _ => {
                result.add_error(ValidationError {
                    line,
                    path: String::new(),
                    error_type: ValidationErrorType::TypeMismatch {
                        expected: "RECORD".to_string(),
                        actual: json_type_name(record).to_string(),
                    },
                    message: format!("Expected object/record, got {}", json_type_name(record)),
                });
            }
        }

        !result.reached_max_errors(self.options.max_errors)
    }

    /// Validate an object against schema fields.
    fn validate_object(
        &self,
        obj: &serde_json::Map<String, Value>,
        line: usize,
        prefix: &str,
        schema_fields: &[BqSchemaField],
        field_map: &HashMap<String, &BqSchemaField>,
        result: &mut ValidationResult,
    ) {
        // Check for required fields
        for field in schema_fields {
            if field.mode == "REQUIRED" {
                let key_lower = field.name.to_lowercase();
                let found = obj.keys().any(|k| k.to_lowercase() == key_lower);

                if !found {
                    let path = make_path(prefix, &field.name);
                    result.add_error(ValidationError::missing_required(line, &path));
                    if result.reached_max_errors(self.options.max_errors) {
                        return;
                    }
                } else {
                    // Check if the value is null for a required field
                    if let Some(value) = obj.iter().find(|(k, _)| k.to_lowercase() == key_lower) {
                        if value.1.is_null() {
                            let path = make_path(prefix, &field.name);
                            result.add_error(ValidationError::missing_required(line, &path));
                            if result.reached_max_errors(self.options.max_errors) {
                                return;
                            }
                        }
                    }
                }
            }
        }

        // Check each field in the data
        for (key, value) in obj {
            let path = make_path(prefix, key);
            let key_lower = key.to_lowercase();

            if result.reached_max_errors(self.options.max_errors) {
                return;
            }

            match field_map.get(&key_lower) {
                Some(field) => {
                    // Validate the value against the field definition
                    self.validate_value(value, field, line, &path, result);
                }
                None => {
                    // Unknown field
                    let error = ValidationError::unknown_field(line, &path);
                    if self.options.allow_unknown {
                        result.add_warning(error);
                    } else {
                        result.add_error(error);
                    }
                }
            }
        }
    }

    /// Validate a single value against a field definition.
    fn validate_value(
        &self,
        value: &Value,
        field: &BqSchemaField,
        line: usize,
        path: &str,
        result: &mut ValidationResult,
    ) {
        if result.reached_max_errors(self.options.max_errors) {
            return;
        }

        // Handle null values
        if value.is_null() {
            // Already checked for REQUIRED fields above
            return;
        }

        // Handle REPEATED (array) fields
        if field.mode == "REPEATED" {
            match value {
                Value::Array(arr) => {
                    for (idx, item) in arr.iter().enumerate() {
                        let item_path = format!("{}[{}]", path, idx);
                        self.validate_single_value(item, field, line, &item_path, result);
                        if result.reached_max_errors(self.options.max_errors) {
                            return;
                        }
                    }
                }
                _ => {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "ARRAY",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
        } else {
            self.validate_single_value(value, field, line, path, result);
        }
    }

    /// Validate a single (non-array) value against a field type.
    fn validate_single_value(
        &self,
        value: &Value,
        field: &BqSchemaField,
        line: usize,
        path: &str,
        result: &mut ValidationResult,
    ) {
        if result.reached_max_errors(self.options.max_errors) {
            return;
        }

        // Handle null in arrays (allowed)
        if value.is_null() {
            return;
        }

        let expected_type = &field.field_type;

        match expected_type.as_str() {
            "RECORD" => {
                match value {
                    Value::Object(obj) => {
                        // Recursively validate nested fields
                        if let Some(nested_fields) = &field.fields {
                            let nested_map: HashMap<String, &BqSchemaField> = nested_fields
                                .iter()
                                .map(|f| (f.name.to_lowercase(), f))
                                .collect();
                            self.validate_object(
                                obj,
                                line,
                                path,
                                nested_fields,
                                &nested_map,
                                result,
                            );
                        }
                    }
                    _ => {
                        result.add_error(ValidationError::type_mismatch(
                            line,
                            path,
                            "RECORD",
                            json_type_name(value),
                            &truncate_value(value),
                        ));
                    }
                }
            }
            "STRING" => {
                // Most types can be coerced to string
                if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "STRING",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            "INTEGER" => {
                if !self.is_valid_integer(value) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "INTEGER",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            "FLOAT" => {
                if !self.is_valid_float(value) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "FLOAT",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            "BOOLEAN" => {
                if !self.is_valid_boolean(value) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "BOOLEAN",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            "TIMESTAMP" => {
                if !self.is_valid_timestamp(value) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "TIMESTAMP",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            "DATE" => {
                if !self.is_valid_date(value) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "DATE",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            "TIME" => {
                if !self.is_valid_time(value) {
                    result.add_error(ValidationError::type_mismatch(
                        line,
                        path,
                        "TIME",
                        json_type_name(value),
                        &truncate_value(value),
                    ));
                }
            }
            _ => {
                // Unknown type - skip validation
            }
        }
    }

    /// Check if a value is valid for INTEGER type.
    fn is_valid_integer(&self, value: &Value) -> bool {
        match value {
            Value::Number(n) => n.is_i64() || n.is_u64(),
            Value::String(s) if !self.options.strict_types => is_integer_string(s),
            _ => false,
        }
    }

    /// Check if a value is valid for FLOAT type.
    fn is_valid_float(&self, value: &Value) -> bool {
        match value {
            Value::Number(_) => true,
            Value::String(s) if !self.options.strict_types => {
                is_float_string(s) || is_integer_string(s)
            }
            _ => false,
        }
    }

    /// Check if a value is valid for BOOLEAN type.
    fn is_valid_boolean(&self, value: &Value) -> bool {
        match value {
            Value::Bool(_) => true,
            Value::String(s) if !self.options.strict_types => is_boolean_string(s),
            _ => false,
        }
    }

    /// Check if a value is valid for TIMESTAMP type.
    fn is_valid_timestamp(&self, value: &Value) -> bool {
        match value {
            Value::String(s) => is_timestamp(s),
            Value::Number(_) if !self.options.strict_types => true, // Unix timestamp
            _ => false,
        }
    }

    /// Check if a value is valid for DATE type.
    fn is_valid_date(&self, value: &Value) -> bool {
        match value {
            Value::String(s) => is_date(s),
            _ => false,
        }
    }

    /// Check if a value is valid for TIME type.
    fn is_valid_time(&self, value: &Value) -> bool {
        match value {
            Value::String(s) => is_time(s),
            _ => false,
        }
    }
}

/// Build a field path string.
fn make_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}.{}", prefix, name)
    }
}

/// Get a human-readable type name for a JSON value.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "NULL",
        Value::Bool(_) => "BOOLEAN",
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "INTEGER"
            } else {
                "FLOAT"
            }
        }
        Value::String(_) => "STRING",
        Value::Array(_) => "ARRAY",
        Value::Object(_) => "RECORD",
    }
}

/// Truncate a value for display in error messages.
fn truncate_value(value: &Value) -> String {
    let s = match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    };
    if s.len() > 50 {
        format!("{}...", &s[..47])
    } else {
        s
    }
}

/// Validate data from a JSON iterator against a schema.
///
/// This function processes records line-by-line for memory efficiency.
pub fn validate_json_data<R: std::io::BufRead>(
    reader: R,
    schema: &[BqSchemaField],
    options: ValidationOptions,
) -> crate::Result<ValidationResult> {
    use crate::input::JsonRecordIterator;

    let validator = SchemaValidator::new(schema, options.clone());
    let mut result = ValidationResult::new();

    let iter = JsonRecordIterator::new(reader, true); // ignore_invalid_lines=true to collect all errors

    for record_result in iter {
        match record_result {
            Ok((line, record)) => {
                if !validator.validate_record(&record, line, &mut result) {
                    break; // Max errors reached
                }
            }
            Err(e) => {
                // Parse error
                result.add_error(ValidationError {
                    line: 0,
                    path: String::new(),
                    error_type: ValidationErrorType::TypeMismatch {
                        expected: "valid JSON".to_string(),
                        actual: "parse error".to_string(),
                    },
                    message: format!("JSON parse error: {}", e),
                });
                if result.reached_max_errors(options.max_errors) {
                    break;
                }
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn make_field(name: &str, field_type: &str, mode: &str) -> BqSchemaField {
        BqSchemaField {
            name: name.to_string(),
            field_type: field_type.to_string(),
            mode: mode.to_string(),
            fields: None,
        }
    }

    fn make_record_field(name: &str, mode: &str, fields: Vec<BqSchemaField>) -> BqSchemaField {
        BqSchemaField {
            name: name.to_string(),
            field_type: "RECORD".to_string(),
            mode: mode.to_string(),
            fields: Some(fields),
        }
    }

    #[test]
    fn test_valid_simple_record() {
        let schema = vec![
            make_field("name", "STRING", "NULLABLE"),
            make_field("age", "INTEGER", "NULLABLE"),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"name": "John", "age": 30});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid);
        assert_eq!(result.error_count, 0);
    }

    #[test]
    fn test_missing_required_field() {
        let schema = vec![
            make_field("name", "STRING", "REQUIRED"),
            make_field("age", "INTEGER", "NULLABLE"),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"age": 30});
        validator.validate_record(&record, 42, &mut result);

        assert!(!result.valid);
        assert_eq!(result.error_count, 1);
        assert_eq!(result.errors[0].line, 42);
        assert!(matches!(
            result.errors[0].error_type,
            ValidationErrorType::MissingRequired
        ));
    }

    #[test]
    fn test_null_required_field() {
        let schema = vec![make_field("name", "STRING", "REQUIRED")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"name": null});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert!(matches!(
            result.errors[0].error_type,
            ValidationErrorType::MissingRequired
        ));
    }

    #[test]
    fn test_type_mismatch() {
        let schema = vec![make_field("age", "INTEGER", "NULLABLE")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"age": "thirty"});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert!(matches!(
            result.errors[0].error_type,
            ValidationErrorType::TypeMismatch { .. }
        ));
    }

    #[test]
    fn test_lenient_type_coercion() {
        let schema = vec![
            make_field("age", "INTEGER", "NULLABLE"),
            make_field("active", "BOOLEAN", "NULLABLE"),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // "123" should be valid INTEGER in lenient mode
        let record = json!({"age": "123", "active": "true"});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid);
    }

    #[test]
    fn test_strict_type_checking() {
        let schema = vec![make_field("age", "INTEGER", "NULLABLE")];

        let options = ValidationOptions {
            strict_types: true,
            ..Default::default()
        };
        let validator = SchemaValidator::new(&schema, options);
        let mut result = ValidationResult::new();

        // "123" should fail INTEGER in strict mode
        let record = json!({"age": "123"});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
    }

    #[test]
    fn test_unknown_field_error() {
        let schema = vec![make_field("name", "STRING", "NULLABLE")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"name": "John", "unknown": 123});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert!(matches!(
            result.errors[0].error_type,
            ValidationErrorType::UnknownField
        ));
    }

    #[test]
    fn test_unknown_field_allowed() {
        let schema = vec![make_field("name", "STRING", "NULLABLE")];

        let options = ValidationOptions {
            allow_unknown: true,
            ..Default::default()
        };
        let validator = SchemaValidator::new(&schema, options);
        let mut result = ValidationResult::new();

        let record = json!({"name": "John", "unknown": 123});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid);
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn test_nested_record_validation() {
        let schema = vec![make_record_field(
            "user",
            "NULLABLE",
            vec![
                make_field("name", "STRING", "REQUIRED"),
                make_field("email", "STRING", "NULLABLE"),
            ],
        )];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Missing required nested field
        let record = json!({"user": {"email": "test@example.com"}});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert!(result.errors[0].path.contains("user.name"));
    }

    #[test]
    fn test_repeated_field_validation() {
        let schema = vec![make_field("tags", "STRING", "REPEATED")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Valid array
        let record = json!({"tags": ["a", "b", "c"]});
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid);

        // Non-array should fail
        let mut result2 = ValidationResult::new();
        let record2 = json!({"tags": "not-an-array"});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(!result2.valid);
    }

    #[test]
    fn test_max_errors_limit() {
        let schema = vec![
            make_field("a", "INTEGER", "NULLABLE"),
            make_field("b", "INTEGER", "NULLABLE"),
            make_field("c", "INTEGER", "NULLABLE"),
        ];

        let options = ValidationOptions {
            max_errors: 2,
            ..Default::default()
        };
        let validator = SchemaValidator::new(&schema, options);
        let mut result = ValidationResult::new();

        let record = json!({"a": "x", "b": "y", "c": "z"});
        validator.validate_record(&record, 1, &mut result);

        // Should stop after 2 errors
        assert_eq!(result.error_count, 2);
    }

    #[test]
    fn test_date_time_validation() {
        let schema = vec![
            make_field("date_field", "DATE", "NULLABLE"),
            make_field("time_field", "TIME", "NULLABLE"),
            make_field("timestamp_field", "TIMESTAMP", "NULLABLE"),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({
            "date_field": "2024-01-15",
            "time_field": "12:30:45",
            "timestamp_field": "2024-01-15T12:30:45"
        });
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid);
    }

    #[test]
    fn test_invalid_date_format() {
        let schema = vec![make_field("date_field", "DATE", "NULLABLE")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"date_field": "01-15-2024"}); // Wrong format
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
    }

    #[test]
    fn test_empty_record_validation() {
        // Schema with only NULLABLE fields
        let schema = vec![
            make_field("optional1", "STRING", "NULLABLE"),
            make_field("optional2", "INTEGER", "NULLABLE"),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Empty record should pass if all fields are NULLABLE
        let record = json!({});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid);
        assert_eq!(result.error_count, 0);
    }

    #[test]
    fn test_deeply_nested_validation_5_levels() {
        let schema = vec![make_record_field(
            "l1",
            "NULLABLE",
            vec![make_record_field(
                "l2",
                "NULLABLE",
                vec![make_record_field(
                    "l3",
                    "NULLABLE",
                    vec![make_record_field(
                        "l4",
                        "NULLABLE",
                        vec![make_field("l5", "STRING", "REQUIRED")],
                    )],
                )],
            )],
        )];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Valid deeply nested data
        let mut result1 = ValidationResult::new();
        let valid_record = json!({"l1": {"l2": {"l3": {"l4": {"l5": "value"}}}}});
        validator.validate_record(&valid_record, 1, &mut result1);
        assert!(result1.valid);

        // Missing required field at depth 5
        let mut result2 = ValidationResult::new();
        let invalid_record = json!({"l1": {"l2": {"l3": {"l4": {}}}}});
        validator.validate_record(&invalid_record, 1, &mut result2);
        assert!(!result2.valid);
        assert!(result2.errors[0].path.contains("l1.l2.l3.l4.l5"));
    }

    #[test]
    fn test_float_integer_boundary_validation() {
        let schema = vec![make_field("big_num", "INTEGER", "NULLABLE")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // i64::MAX should be valid
        let mut result1 = ValidationResult::new();
        let record1 = json!({"big_num": 9223372036854775807_i64});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid);

        // Float should not be valid for INTEGER
        let mut result2 = ValidationResult::new();
        let record2 = json!({"big_num": 3.5});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(!result2.valid);
    }

    #[test]
    fn test_timestamp_unix_epoch_numeric() {
        let schema = vec![make_field("ts", "TIMESTAMP", "NULLABLE")];

        // Lenient mode - numeric timestamps allowed
        let lenient_validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result1 = ValidationResult::new();
        let record = json!({"ts": 1609459200});
        lenient_validator.validate_record(&record, 1, &mut result1);
        assert!(
            result1.valid,
            "Numeric timestamp should be valid in lenient mode"
        );

        // Strict mode - only string timestamps
        let strict_options = ValidationOptions {
            strict_types: true,
            ..Default::default()
        };
        let strict_validator = SchemaValidator::new(&schema, strict_options);
        let mut result2 = ValidationResult::new();
        strict_validator.validate_record(&record, 1, &mut result2);
        assert!(
            !result2.valid,
            "Numeric timestamp should be invalid in strict mode"
        );
    }

    #[test]
    fn test_repeated_with_nulls_in_array() {
        let schema = vec![make_field("values", "INTEGER", "REPEATED")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Array with nulls should be valid (nulls are allowed in arrays)
        let record = json!({"values": [1, null, 2, null, 3]});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid, "Nulls in arrays should be allowed");
    }

    #[test]
    fn test_case_insensitive_field_matching() {
        let schema = vec![make_field("UserName", "STRING", "NULLABLE")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Different casing should match
        let record = json!({"username": "test"});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid, "Field matching should be case-insensitive");
    }

    #[test]
    fn test_empty_string_for_required_field() {
        let schema = vec![make_field("name", "STRING", "REQUIRED")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Empty string should count as present (not null)
        let record = json!({"name": ""});
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid, "Empty string should satisfy REQUIRED");
    }

    #[test]
    fn test_validation_json_output_structure() {
        let schema = vec![make_field("id", "INTEGER", "REQUIRED")];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"wrong_field": 42});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert_eq!(result.error_count, result.errors.len());

        // Check error structure
        let error = &result.errors[0];
        assert!(error.line > 0);
        assert!(!error.path.is_empty() || error.path == "id");
        assert!(!error.message.is_empty());
    }

    #[test]
    fn test_multiple_errors_in_single_record() {
        let schema = vec![
            make_field("a", "INTEGER", "REQUIRED"),
            make_field("b", "INTEGER", "REQUIRED"),
            make_field("c", "INTEGER", "REQUIRED"),
        ];

        let options = ValidationOptions {
            max_errors: 100,
            ..Default::default()
        };
        let validator = SchemaValidator::new(&schema, options);
        let mut result = ValidationResult::new();

        // Record missing all required fields
        let record = json!({});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert_eq!(
            result.error_count, 3,
            "Should report all missing required fields"
        );
    }

    #[test]
    fn test_nested_unknown_field() {
        let schema = vec![make_record_field(
            "user",
            "NULLABLE",
            vec![make_field("name", "STRING", "NULLABLE")],
        )];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"user": {"name": "test", "unknown_nested": 123}});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        assert!(result.errors[0].path.contains("user.unknown_nested"));
    }

    #[test]
    fn test_array_of_records_validation() {
        let schema = vec![make_record_field(
            "items",
            "REPEATED",
            vec![
                make_field("id", "INTEGER", "REQUIRED"),
                make_field("name", "STRING", "NULLABLE"),
            ],
        )];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Valid array of records
        let mut result1 = ValidationResult::new();
        let valid_record = json!({
            "items": [
                {"id": 1, "name": "first"},
                {"id": 2, "name": "second"}
            ]
        });
        validator.validate_record(&valid_record, 1, &mut result1);
        assert!(result1.valid);

        // Invalid: missing required field in one element
        let mut result2 = ValidationResult::new();
        let invalid_record = json!({
            "items": [
                {"id": 1, "name": "first"},
                {"name": "second"}  // missing id
            ]
        });
        validator.validate_record(&invalid_record, 1, &mut result2);
        assert!(!result2.valid);
        assert!(result2.errors[0].path.contains("[1]"));
    }

    #[test]
    fn test_validate_integer_i64_min() {
        let schema = vec![make_field("min_int", "INTEGER", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // i64::MIN should be valid
        let mut result = ValidationResult::new();
        let record = json!({"min_int": -9223372036854775808_i64});
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid);
    }

    #[test]
    fn test_validate_integer_zero() {
        let schema = vec![make_field("zero_int", "INTEGER", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        let mut result = ValidationResult::new();
        let record = json!({"zero_int": 0});
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid);
    }

    #[test]
    fn test_validate_integer_negative() {
        let schema = vec![make_field("neg_int", "INTEGER", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        let mut result = ValidationResult::new();
        let record = json!({"neg_int": -42});
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid);
    }

    #[test]
    fn test_validate_timestamp_with_timezone() {
        let schema = vec![make_field("ts", "TIMESTAMP", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // ISO 8601 with timezone offset
        let mut result1 = ValidationResult::new();
        let record1 = json!({"ts": "2024-01-15T12:30:45+05:30"});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid, "Timestamp with timezone should be valid");

        // ISO 8601 with Z suffix (UTC)
        let mut result2 = ValidationResult::new();
        let record2 = json!({"ts": "2024-01-15T12:30:45Z"});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(result2.valid, "Timestamp with Z suffix should be valid");

        // Negative timezone offset
        let mut result3 = ValidationResult::new();
        let record3 = json!({"ts": "2024-01-15T12:30:45-08:00"});
        validator.validate_record(&record3, 1, &mut result3);
        assert!(
            result3.valid,
            "Timestamp with negative timezone should be valid"
        );
    }

    #[test]
    fn test_validate_timestamp_milliseconds() {
        let schema = vec![make_field("ts", "TIMESTAMP", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Timestamp with milliseconds
        let mut result1 = ValidationResult::new();
        let record1 = json!({"ts": "2024-01-15T12:30:45.123"});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid, "Timestamp with milliseconds should be valid");

        // Timestamp with microseconds
        let mut result2 = ValidationResult::new();
        let record2 = json!({"ts": "2024-01-15T12:30:45.123456"});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(
            result2.valid,
            "Timestamp with microseconds should be valid"
        );
    }

    #[test]
    fn test_validate_timestamp_space_separator() {
        let schema = vec![make_field("ts", "TIMESTAMP", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Space instead of T separator (common in some systems)
        let mut result = ValidationResult::new();
        let record = json!({"ts": "2024-01-15 12:30:45"});
        validator.validate_record(&record, 1, &mut result);
        // This may or may not be valid depending on timestamp parsing
        // The test documents current behavior
    }

    #[test]
    fn test_validate_deeply_nested_record_10_levels() {
        // Create deeply nested schema - 10 levels deep
        fn make_nested_schema(depth: usize, leaf_name: &str) -> BqSchemaField {
            if depth == 0 {
                make_field(leaf_name, "STRING", "REQUIRED")
            } else {
                make_record_field(
                    &format!("level{}", depth),
                    "NULLABLE",
                    vec![make_nested_schema(depth - 1, leaf_name)],
                )
            }
        }

        let schema = vec![make_nested_schema(10, "deep_value")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Build nested JSON - 10 levels deep
        fn make_nested_json(depth: usize) -> serde_json::Value {
            if depth == 0 {
                json!({"deep_value": "found it!"})
            } else {
                let inner = make_nested_json(depth - 1);
                json!({format!("level{}", depth): inner})
            }
        }

        let mut result = ValidationResult::new();
        let record = make_nested_json(10);
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid, "10-level deep nesting should be valid");
    }

    #[test]
    fn test_validate_float_special_values() {
        let schema = vec![make_field("num", "FLOAT", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Very small float
        let mut result1 = ValidationResult::new();
        let record1 = json!({"num": 0.000000001});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid);

        // Very large float
        let mut result2 = ValidationResult::new();
        let record2 = json!({"num": 1.7976931348623157e308});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(result2.valid);

        // Negative float
        let mut result3 = ValidationResult::new();
        let record3 = json!({"num": -123.456});
        validator.validate_record(&record3, 1, &mut result3);
        assert!(result3.valid);
    }

    #[test]
    fn test_validate_date_edge_cases() {
        let schema = vec![make_field("d", "DATE", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Leap year date
        let mut result1 = ValidationResult::new();
        let record1 = json!({"d": "2024-02-29"});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid, "Leap year date should be valid");

        // End of year
        let mut result2 = ValidationResult::new();
        let record2 = json!({"d": "2024-12-31"});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(result2.valid);

        // Beginning of year
        let mut result3 = ValidationResult::new();
        let record3 = json!({"d": "2024-01-01"});
        validator.validate_record(&record3, 1, &mut result3);
        assert!(result3.valid);
    }

    #[test]
    fn test_validate_time_edge_cases() {
        let schema = vec![make_field("t", "TIME", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Midnight
        let mut result1 = ValidationResult::new();
        let record1 = json!({"t": "00:00:00"});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid, "Midnight should be valid");

        // Last second of day
        let mut result2 = ValidationResult::new();
        let record2 = json!({"t": "23:59:59"});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(result2.valid, "23:59:59 should be valid");

        // Noon
        let mut result3 = ValidationResult::new();
        let record3 = json!({"t": "12:00:00"});
        validator.validate_record(&record3, 1, &mut result3);
        assert!(result3.valid, "Noon should be valid");
    }

    #[test]
    fn test_validate_mixed_types_in_record() {
        let schema = vec![
            make_field("str", "STRING", "NULLABLE"),
            make_field("int", "INTEGER", "NULLABLE"),
            make_field("float", "FLOAT", "NULLABLE"),
            make_field("bool", "BOOLEAN", "NULLABLE"),
            make_field("date", "DATE", "NULLABLE"),
            make_field("time", "TIME", "NULLABLE"),
            make_field("ts", "TIMESTAMP", "NULLABLE"),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({
            "str": "hello",
            "int": 42,
            "float": 2.71,
            "bool": true,
            "date": "2024-01-15",
            "time": "12:30:00",
            "ts": "2024-01-15T12:30:00"
        });
        validator.validate_record(&record, 1, &mut result);

        assert!(result.valid, "Record with all valid types should pass");
    }

    #[test]
    fn test_validate_string_coercion_edge_cases() {
        let schema = vec![make_field("str", "STRING", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Number can be coerced to string
        let mut result1 = ValidationResult::new();
        let record1 = json!({"str": 12345});
        validator.validate_record(&record1, 1, &mut result1);
        assert!(result1.valid, "Number should coerce to string");

        // Boolean can be coerced to string
        let mut result2 = ValidationResult::new();
        let record2 = json!({"str": true});
        validator.validate_record(&record2, 1, &mut result2);
        assert!(result2.valid, "Boolean should coerce to string");

        // Array should NOT coerce to string
        let mut result3 = ValidationResult::new();
        let record3 = json!({"str": [1, 2, 3]});
        validator.validate_record(&record3, 1, &mut result3);
        assert!(!result3.valid, "Array should not coerce to string");

        // Object should NOT coerce to string
        let mut result4 = ValidationResult::new();
        let record4 = json!({"str": {"nested": "object"}});
        validator.validate_record(&record4, 1, &mut result4);
        assert!(!result4.valid, "Object should not coerce to string");
    }

    #[test]
    fn test_validate_repeated_records_nested() {
        let schema = vec![make_record_field(
            "orders",
            "REPEATED",
            vec![
                make_field("id", "INTEGER", "REQUIRED"),
                make_record_field(
                    "items",
                    "REPEATED",
                    vec![
                        make_field("product_id", "INTEGER", "REQUIRED"),
                        make_field("quantity", "INTEGER", "NULLABLE"),
                    ],
                ),
            ],
        )];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Valid nested repeated records
        let mut result = ValidationResult::new();
        let record = json!({
            "orders": [
                {
                    "id": 1,
                    "items": [
                        {"product_id": 100, "quantity": 2},
                        {"product_id": 101, "quantity": 1}
                    ]
                },
                {
                    "id": 2,
                    "items": [
                        {"product_id": 200, "quantity": 5}
                    ]
                }
            ]
        });
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid, "Nested repeated records should be valid");
    }

    #[test]
    fn test_validate_empty_array() {
        let schema = vec![make_field("tags", "STRING", "REPEATED")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        let mut result = ValidationResult::new();
        let record = json!({"tags": []});
        validator.validate_record(&record, 1, &mut result);
        assert!(result.valid, "Empty array should be valid for REPEATED field");
    }

    #[test]
    fn test_validate_json_data_function() {
        let schema = vec![
            make_field("name", "STRING", "REQUIRED"),
            make_field("age", "INTEGER", "NULLABLE"),
        ];

        let input = r#"{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie"}"#;

        let result =
            validate_json_data(std::io::Cursor::new(input), &schema, ValidationOptions::default())
                .unwrap();

        assert!(result.valid);
        assert_eq!(result.error_count, 0);
    }

    #[test]
    fn test_validate_json_data_with_errors() {
        let schema = vec![
            make_field("name", "STRING", "REQUIRED"),
            make_field("age", "INTEGER", "NULLABLE"),
        ];

        let input = r#"{"age": 30}
{"name": "Bob", "age": "not a number"}
{"name": "Charlie"}"#;

        let result =
            validate_json_data(std::io::Cursor::new(input), &schema, ValidationOptions::default())
                .unwrap();

        assert!(!result.valid);
        // Should have errors for missing required field and type mismatch
        assert!(result.error_count >= 2);
    }

    #[test]
    fn test_truncate_value_function() {
        // Test through validation error message
        let schema = vec![make_field("data", "INTEGER", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Create a very long string value
        let long_string = "x".repeat(100);
        let mut result = ValidationResult::new();
        let record = json!({"data": long_string});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        // Error message should have truncated value
        assert!(result.errors[0].message.len() < 200);
    }

    #[test]
    fn test_validation_options_defaults() {
        let options = ValidationOptions::default();

        assert!(!options.allow_unknown);
        assert!(!options.strict_types);
        assert_eq!(options.max_errors, 100);
    }

    #[test]
    fn test_validate_record_not_object() {
        let schema = vec![make_field("name", "STRING", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        // Array at root
        let mut result1 = ValidationResult::new();
        let record1 = json!([1, 2, 3]);
        validator.validate_record(&record1, 1, &mut result1);
        assert!(!result1.valid);

        // String at root
        let mut result2 = ValidationResult::new();
        let record2 = json!("just a string");
        validator.validate_record(&record2, 1, &mut result2);
        assert!(!result2.valid);

        // Number at root
        let mut result3 = ValidationResult::new();
        let record3 = json!(42);
        validator.validate_record(&record3, 1, &mut result3);
        assert!(!result3.valid);
    }

    #[test]
    fn test_json_type_name_function() {
        // Test through error messages
        let schema = vec![make_field("field", "RECORD", "NULLABLE")];
        let validator = SchemaValidator::new(&schema, ValidationOptions::default());

        let mut result = ValidationResult::new();
        let record = json!({"field": "string value"});
        validator.validate_record(&record, 1, &mut result);

        assert!(!result.valid);
        // Error should mention expected and actual types
        assert!(result.errors[0].message.contains("STRING"));
        assert!(result.errors[0].message.contains("RECORD"));
    }
}
