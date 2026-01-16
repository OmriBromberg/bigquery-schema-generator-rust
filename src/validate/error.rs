//! Validation error types for schema validation.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Types of validation errors that can occur.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationErrorType {
    /// A required field is missing or null
    MissingRequired,
    /// Value type doesn't match expected schema type
    TypeMismatch { expected: String, actual: String },
    /// Field exists in data but not in schema
    UnknownField,
}

impl fmt::Display for ValidationErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationErrorType::MissingRequired => write!(f, "missing required field"),
            ValidationErrorType::TypeMismatch { expected, actual } => {
                write!(f, "expected {}, got {}", expected, actual)
            }
            ValidationErrorType::UnknownField => write!(f, "unknown field"),
        }
    }
}

/// A single validation error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationError {
    /// Line number where the error occurred (1-indexed)
    pub line: usize,
    /// Path to the field (e.g., "user.address.city")
    pub path: String,
    /// Type of validation error
    pub error_type: ValidationErrorType,
    /// Human-readable error message
    pub message: String,
}

impl ValidationError {
    /// Create a new validation error for a missing required field.
    pub fn missing_required(line: usize, path: &str) -> Self {
        Self {
            line,
            path: path.to_string(),
            error_type: ValidationErrorType::MissingRequired,
            message: format!("Field '{}' is REQUIRED but missing", path),
        }
    }

    /// Create a new validation error for a type mismatch.
    pub fn type_mismatch(
        line: usize,
        path: &str,
        expected: &str,
        actual: &str,
        value: &str,
    ) -> Self {
        Self {
            line,
            path: path.to_string(),
            error_type: ValidationErrorType::TypeMismatch {
                expected: expected.to_string(),
                actual: actual.to_string(),
            },
            message: format!(
                "Field '{}' expected {}, got {} (\"{}\")",
                path, expected, actual, value
            ),
        }
    }

    /// Create a new validation error for an unknown field.
    pub fn unknown_field(line: usize, path: &str) -> Self {
        Self {
            line,
            path: path.to_string(),
            error_type: ValidationErrorType::UnknownField,
            message: format!("Unknown field '{}' not in schema", path),
        }
    }
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Line {}: {}", self.line, self.message)
    }
}

/// Result of validating data against a schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    /// Whether all data is valid
    pub valid: bool,
    /// Number of errors found
    pub error_count: usize,
    /// List of validation errors
    pub errors: Vec<ValidationError>,
    /// List of warnings (e.g., unknown fields when --allow-unknown is set)
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<ValidationError>,
}

impl ValidationResult {
    /// Create a new empty validation result (valid).
    pub fn new() -> Self {
        Self {
            valid: true,
            error_count: 0,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Add an error to the result.
    pub fn add_error(&mut self, error: ValidationError) {
        self.valid = false;
        self.error_count += 1;
        self.errors.push(error);
    }

    /// Add a warning to the result.
    pub fn add_warning(&mut self, warning: ValidationError) {
        self.warnings.push(warning);
    }

    /// Check if max errors has been reached.
    pub fn reached_max_errors(&self, max_errors: usize) -> bool {
        self.error_count >= max_errors
    }
}

impl Default for ValidationResult {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validation_error_display() {
        let err = ValidationError::missing_required(42, "user_id");
        assert!(err.to_string().contains("Line 42"));
        assert!(err.to_string().contains("REQUIRED"));

        let err = ValidationError::type_mismatch(10, "age", "INTEGER", "STRING", "thirty");
        assert!(err.to_string().contains("expected INTEGER"));
        assert!(err.to_string().contains("got STRING"));

        let err = ValidationError::unknown_field(5, "legacy_field");
        assert!(err.to_string().contains("Unknown field"));
    }

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::new();
        assert!(result.valid);
        assert_eq!(result.error_count, 0);

        result.add_error(ValidationError::missing_required(1, "id"));
        assert!(!result.valid);
        assert_eq!(result.error_count, 1);

        result.add_warning(ValidationError::unknown_field(2, "extra"));
        assert_eq!(result.warnings.len(), 1);
    }

    // ===== Additional Coverage Tests =====

    #[test]
    fn test_validation_error_type_display() {
        let missing = ValidationErrorType::MissingRequired;
        assert_eq!(missing.to_string(), "missing required field");

        let type_mismatch = ValidationErrorType::TypeMismatch {
            expected: "INTEGER".to_string(),
            actual: "STRING".to_string(),
        };
        assert_eq!(type_mismatch.to_string(), "expected INTEGER, got STRING");

        let unknown = ValidationErrorType::UnknownField;
        assert_eq!(unknown.to_string(), "unknown field");
    }

    #[test]
    fn test_validation_error_missing_required() {
        let err = ValidationError::missing_required(10, "user.name");

        assert_eq!(err.line, 10);
        assert_eq!(err.path, "user.name");
        assert_eq!(err.error_type, ValidationErrorType::MissingRequired);
        assert!(err.message.contains("REQUIRED"));
        assert!(err.message.contains("user.name"));
    }

    #[test]
    fn test_validation_error_type_mismatch() {
        let err = ValidationError::type_mismatch(5, "age", "INTEGER", "STRING", "twenty");

        assert_eq!(err.line, 5);
        assert_eq!(err.path, "age");
        match &err.error_type {
            ValidationErrorType::TypeMismatch { expected, actual } => {
                assert_eq!(expected, "INTEGER");
                assert_eq!(actual, "STRING");
            }
            _ => panic!("Expected TypeMismatch"),
        }
        assert!(err.message.contains("expected INTEGER"));
        assert!(err.message.contains("got STRING"));
        assert!(err.message.contains("twenty"));
    }

    #[test]
    fn test_validation_error_unknown_field() {
        let err = ValidationError::unknown_field(3, "legacy_data.old_field");

        assert_eq!(err.line, 3);
        assert_eq!(err.path, "legacy_data.old_field");
        assert_eq!(err.error_type, ValidationErrorType::UnknownField);
        assert!(err.message.contains("Unknown field"));
        assert!(err.message.contains("not in schema"));
    }

    #[test]
    fn test_validation_error_display_format() {
        let err = ValidationError::missing_required(42, "required_field");
        let display_str = err.to_string();

        assert!(display_str.contains("Line 42"));
        assert!(display_str.contains("required_field"));
    }

    #[test]
    fn test_validation_result_default() {
        let result = ValidationResult::default();

        assert!(result.valid);
        assert_eq!(result.error_count, 0);
        assert!(result.errors.is_empty());
        assert!(result.warnings.is_empty());
    }

    #[test]
    fn test_validation_result_reached_max_errors() {
        let mut result = ValidationResult::new();

        assert!(!result.reached_max_errors(5));

        for i in 0..5 {
            result.add_error(ValidationError::missing_required(i, "field"));
        }

        assert!(result.reached_max_errors(5));
        assert!(!result.reached_max_errors(6));
    }

    #[test]
    fn test_validation_result_multiple_errors() {
        let mut result = ValidationResult::new();

        result.add_error(ValidationError::missing_required(1, "a"));
        result.add_error(ValidationError::type_mismatch(2, "b", "INT", "STR", "x"));
        result.add_error(ValidationError::unknown_field(3, "c"));

        assert!(!result.valid);
        assert_eq!(result.error_count, 3);
        assert_eq!(result.errors.len(), 3);
    }

    #[test]
    fn test_validation_result_multiple_warnings() {
        let mut result = ValidationResult::new();

        result.add_warning(ValidationError::unknown_field(1, "extra1"));
        result.add_warning(ValidationError::unknown_field(2, "extra2"));

        // Warnings don't affect validity
        assert!(result.valid);
        assert_eq!(result.error_count, 0);
        assert_eq!(result.warnings.len(), 2);
    }

    #[test]
    fn test_validation_error_serialization() {
        let err = ValidationError::type_mismatch(1, "field", "INTEGER", "BOOLEAN", "true");
        let json = serde_json::to_string(&err).unwrap();

        assert!(json.contains("\"line\":1"));
        assert!(json.contains("\"path\":\"field\""));
        assert!(json.contains("type_mismatch"));
    }

    #[test]
    fn test_validation_result_serialization() {
        let mut result = ValidationResult::new();
        result.add_error(ValidationError::missing_required(1, "id"));

        let json = serde_json::to_string(&result).unwrap();

        assert!(json.contains("\"valid\":false"));
        assert!(json.contains("\"error_count\":1"));
        // warnings should be skipped if empty
    }

    #[test]
    fn test_validation_error_type_equality() {
        let a = ValidationErrorType::MissingRequired;
        let b = ValidationErrorType::MissingRequired;
        assert_eq!(a, b);

        let c = ValidationErrorType::TypeMismatch {
            expected: "A".to_string(),
            actual: "B".to_string(),
        };
        let d = ValidationErrorType::TypeMismatch {
            expected: "A".to_string(),
            actual: "B".to_string(),
        };
        assert_eq!(c, d);

        let e = ValidationErrorType::TypeMismatch {
            expected: "A".to_string(),
            actual: "C".to_string(),
        };
        assert_ne!(c, e);
    }
}
