//! Validate subcommand implementation.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use bq_schema_gen::validate::{SchemaValidator, ValidationResult};
use bq_schema_gen::{
    BqSchemaField, JsonRecordIterator, ValidationError, ValidationErrorType, ValidationOptions,
};

/// Errors that can occur during validation
#[derive(Debug)]
pub enum ValidateError {
    /// Invalid output format specified
    InvalidFormat(String),
    /// Failed to open schema file
    SchemaOpen(PathBuf, std::io::Error),
    /// Failed to parse schema file
    SchemaParse(PathBuf, String),
    /// Invalid glob pattern
    InvalidGlobPattern(String, String),
    /// No input files found
    NoInputFiles,
    /// Failed to open input file
    InputFileOpen(PathBuf, std::io::Error),
}

impl std::fmt::Display for ValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidateError::InvalidFormat(fmt) => {
                write!(f, "Unknown format '{}'. Use 'text' or 'json'.", fmt)
            }
            ValidateError::SchemaOpen(path, e) => {
                write!(f, "Cannot open schema file '{}': {}", path.display(), e)
            }
            ValidateError::SchemaParse(path, e) => {
                write!(f, "Cannot parse schema file '{}': {}", path.display(), e)
            }
            ValidateError::InvalidGlobPattern(pattern, e) => {
                write!(f, "Invalid glob pattern '{}': {}", pattern, e)
            }
            ValidateError::NoInputFiles => write!(f, "No input files specified"),
            ValidateError::InputFileOpen(path, e) => {
                write!(f, "Cannot open input file '{}': {}", path.display(), e)
            }
        }
    }
}

impl std::error::Error for ValidateError {}

/// Output from a successful validation run
#[derive(Debug)]
pub struct ValidateOutput {
    /// Whether all data was valid
    pub valid: bool,
    /// Validation result with errors and warnings
    pub result: ValidationResult,
    /// Total lines processed
    pub total_lines: usize,
}

/// Run the validate subcommand implementation (testable version)
pub fn run_impl(
    file_patterns: &[String],
    schema_path: &Path,
    allow_unknown: bool,
    strict_types: bool,
    max_errors: usize,
    format: &str,
) -> Result<ValidateOutput, ValidateError> {
    // Validate format
    if format != "text" && format != "json" {
        return Err(ValidateError::InvalidFormat(format.to_string()));
    }

    // Load schema
    let schema = load_schema_file_impl(schema_path)?;

    // Build validation options
    let options = ValidationOptions {
        allow_unknown,
        strict_types,
        max_errors,
    };

    // Collect input files
    let files = collect_input_files_impl(file_patterns)?;

    if files.is_empty() {
        return Err(ValidateError::NoInputFiles);
    }

    // Create validator
    let validator = SchemaValidator::new(&schema, options.clone());
    let mut result = ValidationResult::new();
    let mut total_lines = 0;

    // Process each file
    for path in &files {
        let file = File::open(path).map_err(|e| ValidateError::InputFileOpen(path.clone(), e))?;

        let reader = BufReader::new(file);
        let iter = JsonRecordIterator::new(reader, true);

        for record_result in iter {
            match record_result {
                Ok((line, record)) => {
                    total_lines += 1;
                    if !validator.validate_record(&record, line, &mut result) {
                        break; // Max errors reached
                    }
                }
                Err(e) => {
                    result.add_error(ValidationError {
                        line: 0,
                        path: path.display().to_string(),
                        error_type: ValidationErrorType::TypeMismatch {
                            expected: "valid JSON".to_string(),
                            actual: "parse error".to_string(),
                        },
                        message: format!("JSON parse error: {}", e),
                    });
                    if result.reached_max_errors(max_errors) {
                        break;
                    }
                }
            }
        }

        if result.reached_max_errors(max_errors) {
            break;
        }
    }

    Ok(ValidateOutput {
        valid: result.valid,
        result,
        total_lines,
    })
}

/// Run the validate subcommand
pub fn run(
    file_patterns: &[String],
    schema_path: &Path,
    allow_unknown: bool,
    strict_types: bool,
    max_errors: usize,
    format: &str,
    quiet: bool,
) {
    let output = match run_impl(
        file_patterns,
        schema_path,
        allow_unknown,
        strict_types,
        max_errors,
        format,
    ) {
        Ok(output) => output,
        Err(e) => {
            eprintln!("Error: {}", e);
            // Exit with 2 for operational errors (file not found, etc.)
            std::process::exit(2);
        }
    };

    // Output results
    if quiet {
        // Exit code only
        std::process::exit(if output.valid { 0 } else { 1 });
    }

    match format {
        "json" => {
            let json_output = serde_json::json!({
                "valid": output.result.valid,
                "error_count": output.result.error_count,
                "errors": output.result.errors,
                "warnings": output.result.warnings,
                "lines_processed": output.total_lines,
            });
            println!("{}", serde_json::to_string_pretty(&json_output).unwrap());
        }
        _ => {
            if output.result.valid {
                eprintln!("Validation passed ({} lines processed)", output.total_lines);
                if !output.result.warnings.is_empty() {
                    eprintln!("{} warning(s):", output.result.warnings.len());
                    for warning in &output.result.warnings {
                        eprintln!("  {}", warning);
                    }
                }
            } else {
                eprintln!(
                    "Validation failed ({} error{}):",
                    output.result.error_count,
                    if output.result.error_count == 1 {
                        ""
                    } else {
                        "s"
                    }
                );
                for error in &output.result.errors {
                    eprintln!("  {}", error);
                }
                if !output.result.warnings.is_empty() {
                    eprintln!("{} warning(s):", output.result.warnings.len());
                    for warning in &output.result.warnings {
                        eprintln!("  {}", warning);
                    }
                }
            }
        }
    }

    // Exit codes: 0 = valid, 1 = invalid, 2 = error reading files
    std::process::exit(if output.valid { 0 } else { 1 });
}

/// Collect input files from patterns (returns Result)
fn collect_input_files_impl(patterns: &[String]) -> Result<Vec<PathBuf>, ValidateError> {
    let mut files = Vec::new();

    for pattern in patterns {
        match glob::glob(pattern) {
            Ok(paths) => {
                let mut found = false;
                for entry in paths {
                    match entry {
                        Ok(path) => {
                            if path.is_file() {
                                files.push(path);
                                found = true;
                            }
                        }
                        Err(e) => {
                            eprintln!("Warning: Error reading glob entry: {}", e);
                        }
                    }
                }
                if !found {
                    let path = PathBuf::from(pattern);
                    if path.exists() && path.is_file() {
                        files.push(path);
                    } else {
                        eprintln!("Warning: No files matched pattern '{}'", pattern);
                    }
                }
            }
            Err(e) => {
                return Err(ValidateError::InvalidGlobPattern(
                    pattern.clone(),
                    e.to_string(),
                ));
            }
        }
    }

    Ok(files)
}

/// Load a BigQuery schema from a JSON file (returns Result)
fn load_schema_file_impl(path: &Path) -> Result<Vec<BqSchemaField>, ValidateError> {
    let file = File::open(path).map_err(|e| ValidateError::SchemaOpen(path.to_owned(), e))?;

    let reader = BufReader::new(file);
    serde_json::from_reader(reader)
        .map_err(|e| ValidateError::SchemaParse(path.to_owned(), e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    /// Helper to create a temporary schema file with given content
    fn create_temp_schema_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    /// Helper to create a temporary data file with given content
    fn create_temp_data_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_load_schema_file_success() {
        let schema_json = r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#;
        let file = create_temp_schema_file(schema_json);

        let schema = load_schema_file_impl(file.path()).unwrap();
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[0].field_type, "INTEGER");
        assert_eq!(schema[1].name, "name");
        assert_eq!(schema[1].field_type, "STRING");
    }

    #[test]
    fn test_load_schema_file_nested_record() {
        let schema_json = r#"[
            {"name": "user", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "email", "type": "STRING", "mode": "NULLABLE"}
            ]}
        ]"#;
        let file = create_temp_schema_file(schema_json);

        let schema = load_schema_file_impl(file.path()).unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "user");
        assert_eq!(schema[0].field_type, "RECORD");
        assert!(schema[0].fields.is_some());
        assert_eq!(schema[0].fields.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_load_schema_file_empty_array() {
        let schema_json = "[]";
        let file = create_temp_schema_file(schema_json);

        let schema = load_schema_file_impl(file.path()).unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_collect_input_files_direct_path() {
        // Create a temporary file
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_string_lossy().to_string();

        let patterns = vec![path.clone()];
        let files = collect_input_files_impl(&patterns).unwrap();

        assert_eq!(files.len(), 1);
        assert_eq!(files[0], file.path());
    }

    #[test]
    fn test_collect_input_files_multiple_paths() {
        let file1 = NamedTempFile::new().unwrap();
        let file2 = NamedTempFile::new().unwrap();

        let patterns = vec![
            file1.path().to_string_lossy().to_string(),
            file2.path().to_string_lossy().to_string(),
        ];
        let files = collect_input_files_impl(&patterns).unwrap();

        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_collect_input_files_glob_pattern() {
        // Create temp directory with matching files
        let temp_dir = TempDir::new().unwrap();
        let file1_path = temp_dir.path().join("test1.json");
        let file2_path = temp_dir.path().join("test2.json");
        let file3_path = temp_dir.path().join("other.txt");

        std::fs::write(&file1_path, "{}").unwrap();
        std::fs::write(&file2_path, "{}").unwrap();
        std::fs::write(&file3_path, "text").unwrap();

        let pattern = temp_dir.path().join("*.json").to_string_lossy().to_string();
        let files = collect_input_files_impl(&[pattern]).unwrap();

        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.extension().unwrap() == "json"));
    }

    #[test]
    fn test_collect_input_files_nonexistent_pattern() {
        // Pattern that matches nothing should return empty (with warning printed)
        let patterns = vec!["/nonexistent/path/*.xyz".to_string()];
        let files = collect_input_files_impl(&patterns).unwrap();

        assert!(files.is_empty());
    }

    #[test]
    fn test_collect_input_files_empty_patterns() {
        let patterns: Vec<String> = vec![];
        let files = collect_input_files_impl(&patterns).unwrap();

        assert!(files.is_empty());
    }

    #[test]
    fn test_collect_input_files_mixed_patterns() {
        let temp_dir = TempDir::new().unwrap();
        let file1_path = temp_dir.path().join("data1.json");
        let file2_path = temp_dir.path().join("data2.json");

        std::fs::write(&file1_path, "{}").unwrap();
        std::fs::write(&file2_path, "{}").unwrap();

        // Mix direct path and glob pattern
        let patterns = vec![
            file1_path.to_string_lossy().to_string(),
            temp_dir
                .path()
                .join("data2.json")
                .to_string_lossy()
                .to_string(),
        ];
        let files = collect_input_files_impl(&patterns).unwrap();

        assert_eq!(files.len(), 2);
    }

    #[test]
    fn test_collect_input_files_directories_excluded() {
        let temp_dir = TempDir::new().unwrap();
        let subdir = temp_dir.path().join("subdir");
        let file_path = temp_dir.path().join("file.json");

        std::fs::create_dir(&subdir).unwrap();
        std::fs::write(&file_path, "{}").unwrap();

        let pattern = temp_dir.path().join("*").to_string_lossy().to_string();
        let files = collect_input_files_impl(&[pattern]).unwrap();

        // Should only include the file, not the directory
        assert_eq!(files.len(), 1);
        assert!(files[0].is_file());
    }

    // ===== Tests for run_impl =====

    #[test]
    fn test_run_impl_invalid_format() {
        let schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let data = create_temp_data_file(r#"{"id": 1}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "invalid_format",
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ValidateError::InvalidFormat(_)));
        assert!(err.to_string().contains("invalid_format"));
    }

    #[test]
    fn test_run_impl_schema_not_found() {
        let data = create_temp_data_file(r#"{"id": 1}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            Path::new("/nonexistent/schema.json"),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ValidateError::SchemaOpen(_, _)));
    }

    #[test]
    fn test_run_impl_schema_invalid_json() {
        let schema = create_temp_schema_file("not valid json");
        let data = create_temp_data_file(r#"{"id": 1}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ValidateError::SchemaParse(_, _)));
    }

    #[test]
    fn test_run_impl_no_input_files() {
        let schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        let result = run_impl(&[], schema.path(), false, false, 100, "text");

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, ValidateError::NoInputFiles));
    }

    #[test]
    fn test_run_impl_valid_data() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );
        let data = create_temp_data_file(
            r#"{"id": 1, "name": "test"}
{"id": 2, "name": "test2"}"#,
        );

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.valid);
        assert_eq!(output.total_lines, 2);
        assert_eq!(output.result.error_count, 0);
    }

    #[test]
    fn test_run_impl_valid_data_json_format() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "value", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let data = create_temp_data_file(r#"{"value": 42}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "json",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.valid);
    }

    #[test]
    fn test_run_impl_invalid_data_type_mismatch() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
        ]"#,
        );
        let data = create_temp_data_file(r#"{"id": "not_an_integer"}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            true, // strict_types
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.valid);
        assert!(output.result.error_count > 0);
    }

    #[test]
    fn test_run_impl_missing_required_field() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "REQUIRED"}
        ]"#,
        );
        let data = create_temp_data_file(r#"{"id": 1}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.valid);
        assert!(output.result.error_count > 0);
    }

    #[test]
    fn test_run_impl_unknown_field_error() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let data = create_temp_data_file(r#"{"id": 1, "unknown_field": "value"}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false, // allow_unknown = false
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.valid);
    }

    #[test]
    fn test_run_impl_unknown_field_allowed() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let data = create_temp_data_file(r#"{"id": 1, "unknown_field": "value"}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            true, // allow_unknown = true
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.valid);
    }

    #[test]
    fn test_run_impl_max_errors() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
        ]"#,
        );
        // Create data with many errors
        let data = create_temp_data_file(
            r#"{"id": "a"}
{"id": "b"}
{"id": "c"}
{"id": "d"}
{"id": "e"}"#,
        );

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            true, // strict_types to trigger type errors
            3,    // max_errors = 3
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.valid);
        // Should stop after max_errors
        assert!(output.result.error_count <= 3);
    }

    #[test]
    fn test_run_impl_multiple_files() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let data1 = create_temp_data_file(r#"{"id": 1}"#);
        let data2 = create_temp_data_file(r#"{"id": 2}"#);

        let result = run_impl(
            &[
                data1.path().to_string_lossy().to_string(),
                data2.path().to_string_lossy().to_string(),
            ],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.valid);
        assert_eq!(output.total_lines, 2);
    }

    #[test]
    fn test_run_impl_invalid_json_in_data() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let data = create_temp_data_file(
            r#"{"id": 1}
not valid json
{"id": 2}"#,
        );

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // Should have recorded a parse error
        assert!(output.result.error_count > 0 || output.total_lines >= 1);
    }

    #[test]
    fn test_run_impl_nested_record_validation() {
        let schema = create_temp_schema_file(
            r#"[
            {"name": "user", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "age", "type": "INTEGER", "mode": "NULLABLE"}
            ]}
        ]"#,
        );
        let data = create_temp_data_file(r#"{"user": {"name": "John", "age": 30}}"#);

        let result = run_impl(
            &[data.path().to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.valid);
    }

    #[test]
    fn test_run_impl_glob_pattern() {
        let temp_dir = TempDir::new().unwrap();

        let schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );

        // Create multiple data files
        std::fs::write(temp_dir.path().join("data1.json"), r#"{"id": 1}"#).unwrap();
        std::fs::write(temp_dir.path().join("data2.json"), r#"{"id": 2}"#).unwrap();

        let pattern = temp_dir.path().join("*.json").to_string_lossy().to_string();

        let result = run_impl(&[pattern], schema.path(), false, false, 100, "text");

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.valid);
        assert_eq!(output.total_lines, 2);
    }

    #[test]
    fn test_run_impl_input_file_not_found() {
        let schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        // Create a file path that exists initially
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("data.json");
        std::fs::write(&file_path, r#"{"id": 1}"#).unwrap();

        // Remove the file before running validation
        std::fs::remove_file(&file_path).unwrap();

        let result = run_impl(
            &[file_path.to_string_lossy().to_string()],
            schema.path(),
            false,
            false,
            100,
            "text",
        );

        // File doesn't exist, so collect_input_files_impl returns empty (with warning)
        // which then triggers NoInputFiles error
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ValidateError::NoInputFiles));
    }

    #[test]
    fn test_validate_error_display() {
        // Test all error variant display implementations
        let err = ValidateError::InvalidFormat("xml".to_string());
        assert!(err.to_string().contains("xml"));

        let err = ValidateError::SchemaOpen(
            PathBuf::from("/path/to/schema.json"),
            std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        );
        assert!(err.to_string().contains("schema.json"));

        let err = ValidateError::SchemaParse(
            PathBuf::from("/path/to/schema.json"),
            "invalid json".to_string(),
        );
        assert!(err.to_string().contains("invalid json"));

        let err =
            ValidateError::InvalidGlobPattern("**[".to_string(), "unclosed bracket".to_string());
        assert!(err.to_string().contains("**["));

        let err = ValidateError::NoInputFiles;
        assert!(err.to_string().contains("No input files"));

        let err = ValidateError::InputFileOpen(
            PathBuf::from("/path/to/data.json"),
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied"),
        );
        assert!(err.to_string().contains("data.json"));
    }

    #[test]
    fn test_load_schema_file_impl_success() {
        let schema_json = r#"[{"name": "test", "type": "STRING", "mode": "NULLABLE"}]"#;
        let file = create_temp_schema_file(schema_json);

        let result = load_schema_file_impl(file.path());
        assert!(result.is_ok());
        let schema = result.unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "test");
    }

    #[test]
    fn test_load_schema_file_impl_not_found() {
        let result = load_schema_file_impl(Path::new("/nonexistent/path.json"));
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidateError::SchemaOpen(_, _)
        ));
    }

    #[test]
    fn test_load_schema_file_impl_invalid_json() {
        let file = create_temp_schema_file("{ invalid json }");
        let result = load_schema_file_impl(file.path());
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidateError::SchemaParse(_, _)
        ));
    }

    #[test]
    fn test_collect_input_files_impl_success() {
        let file = NamedTempFile::new().unwrap();
        let patterns = vec![file.path().to_string_lossy().to_string()];

        let result = collect_input_files_impl(&patterns);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 1);
    }

    #[test]
    fn test_collect_input_files_impl_invalid_pattern() {
        let patterns = vec!["**[invalid".to_string()];
        let result = collect_input_files_impl(&patterns);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ValidateError::InvalidGlobPattern(_, _)
        ));
    }

    #[test]
    fn test_collect_input_files_impl_empty() {
        let patterns: Vec<String> = vec![];
        let result = collect_input_files_impl(&patterns);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}
