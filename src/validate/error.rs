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
}
