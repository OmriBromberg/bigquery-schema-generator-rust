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
