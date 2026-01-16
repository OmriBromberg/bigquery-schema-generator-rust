//! Error types for the BigQuery schema generator.

use thiserror::Error;

/// Main error type for the schema generator.
#[derive(Error, Debug)]
pub enum Error {
    #[error("Invalid record: {0}")]
    InvalidRecord(String),

    #[error("JSON parse error on line {line}: {message}")]
    JsonParse { line: usize, message: String },

    #[error("CSV parse error: {0}")]
    CsvParse(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Schema file error: {0}")]
    SchemaFile(String),
}

/// Result type alias for this crate.
pub type Result<T> = std::result::Result<T, Error>;

/// An error log entry for non-fatal issues during schema generation.
#[derive(Debug, Clone)]
pub struct ErrorLog {
    /// Line number where the error occurred
    pub line_number: usize,
    /// Error message
    pub msg: String,
}

impl std::fmt::Display for ErrorLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Line {}: {}", self.line_number, self.msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_log_display() {
        let error = ErrorLog {
            line_number: 42,
            msg: "test error".to_string(),
        };
        assert_eq!(format!("{}", error), "Line 42: test error");
    }

    #[test]
    fn test_error_log_display_line_one() {
        let error = ErrorLog {
            line_number: 1,
            msg: "first line error".to_string(),
        };
        assert_eq!(format!("{}", error), "Line 1: first line error");
    }

    #[test]
    fn test_error_log_display_large_line_number() {
        let error = ErrorLog {
            line_number: 1_000_000,
            msg: "large line".to_string(),
        };
        assert_eq!(format!("{}", error), "Line 1000000: large line");
    }

    #[test]
    fn test_error_log_display_empty_message() {
        let error = ErrorLog {
            line_number: 10,
            msg: String::new(),
        };
        assert_eq!(format!("{}", error), "Line 10: ");
    }

    #[test]
    fn test_error_invalid_record_display() {
        let error = Error::InvalidRecord("not an object".to_string());
        assert_eq!(format!("{}", error), "Invalid record: not an object");
    }

    #[test]
    fn test_error_json_parse_display() {
        let error = Error::JsonParse {
            line: 5,
            message: "unexpected token".to_string(),
        };
        assert_eq!(
            format!("{}", error),
            "JSON parse error on line 5: unexpected token"
        );
    }

    #[test]
    fn test_error_csv_parse_display() {
        let error = Error::CsvParse("invalid CSV".to_string());
        assert_eq!(format!("{}", error), "CSV parse error: invalid CSV");
    }

    #[test]
    fn test_error_io_display() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error = Error::Io(io_error);
        assert!(format!("{}", error).contains("IO error"));
        assert!(format!("{}", error).contains("file not found"));
    }

    #[test]
    fn test_error_schema_file_display() {
        let error = Error::SchemaFile("invalid schema format".to_string());
        assert_eq!(
            format!("{}", error),
            "Schema file error: invalid schema format"
        );
    }

    #[test]
    fn test_error_from_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
        let error: Error = io_error.into();
        assert!(matches!(error, Error::Io(_)));
    }
}
