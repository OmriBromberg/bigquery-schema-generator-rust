//! Diff subcommand implementation.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use bq_schema_gen::diff::output::{write_diff, ColorMode, DiffFormat};
use bq_schema_gen::diff::{diff_schemas, DiffOptions, SchemaDiff};
use bq_schema_gen::BqSchemaField;

/// Errors that can occur during diff operation
#[derive(Debug)]
pub enum DiffError {
    /// Invalid output format specified
    InvalidFormat(String),
    /// Invalid color mode specified
    InvalidColorMode(String),
    /// Failed to open schema file
    SchemaOpen(PathBuf, std::io::Error),
    /// Failed to parse schema file
    SchemaParse(PathBuf, String),
    /// Failed to create output file
    OutputCreate(PathBuf, std::io::Error),
    /// Failed to write diff output
    WriteDiff(std::io::Error),
}

impl std::fmt::Display for DiffError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiffError::InvalidFormat(fmt) => {
                write!(
                    f,
                    "Invalid format '{}'. Valid formats: text, json, json-patch, sql",
                    fmt
                )
            }
            DiffError::InvalidColorMode(mode) => {
                write!(
                    f,
                    "Invalid color mode '{}'. Valid modes: auto, always, never",
                    mode
                )
            }
            DiffError::SchemaOpen(path, e) => {
                write!(f, "Cannot open schema file '{}': {}", path.display(), e)
            }
            DiffError::SchemaParse(path, e) => {
                write!(f, "Cannot parse schema file '{}': {}", path.display(), e)
            }
            DiffError::OutputCreate(path, e) => {
                write!(f, "Cannot create output file '{}': {}", path.display(), e)
            }
            DiffError::WriteDiff(e) => {
                write!(f, "Error writing diff: {}", e)
            }
        }
    }
}

impl std::error::Error for DiffError {}

/// Output from a successful diff operation
#[derive(Debug)]
#[allow(dead_code)] // Fields are part of public API, used by tests
pub struct DiffOutput {
    /// The computed schema diff
    pub diff: SchemaDiff,
    /// Whether there are breaking changes
    pub has_breaking_changes: bool,
}

/// Run the diff subcommand implementation (testable version)
pub fn run_impl(
    old_schema_path: &Path,
    new_schema_path: &Path,
    format: &str,
    color: &str,
    strict: bool,
    output_path: Option<&PathBuf>,
) -> Result<DiffOutput, DiffError> {
    // Parse format
    let diff_format: DiffFormat = format
        .parse()
        .map_err(|_| DiffError::InvalidFormat(format.to_string()))?;

    // Parse color mode
    let color_mode: ColorMode = color
        .parse()
        .map_err(|_| DiffError::InvalidColorMode(color.to_string()))?;

    // Load old schema
    let old_schema = load_schema_file_impl(old_schema_path)?;

    // Load new schema
    let new_schema = load_schema_file_impl(new_schema_path)?;

    // Run diff
    let options = DiffOptions { strict };
    let diff = diff_schemas(&old_schema, &new_schema, &options);

    // Set up output
    let mut output: Box<dyn io::Write> = match output_path {
        Some(path) => {
            let file = File::create(path).map_err(|e| DiffError::OutputCreate(path.clone(), e))?;
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    // Write diff
    write_diff(&diff, diff_format, color_mode, &mut output).map_err(DiffError::WriteDiff)?;

    let has_breaking_changes = diff.has_breaking_changes();

    Ok(DiffOutput {
        diff,
        has_breaking_changes,
    })
}

/// Run the diff subcommand
pub fn run(
    old_schema_path: &Path,
    new_schema_path: &Path,
    format: &str,
    color: &str,
    strict: bool,
    output_path: Option<&PathBuf>,
) {
    match run_impl(
        old_schema_path,
        new_schema_path,
        format,
        color,
        strict,
        output_path,
    ) {
        Ok(output) => {
            // Exit with non-zero status if there are breaking changes
            if output.has_breaking_changes {
                std::process::exit(1);
            }
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

/// Load a BigQuery schema from a JSON file (returns Result)
fn load_schema_file_impl(path: &Path) -> Result<Vec<BqSchemaField>, DiffError> {
    let file = File::open(path).map_err(|e| DiffError::SchemaOpen(path.to_owned(), e))?;

    let reader = BufReader::new(file);
    serde_json::from_reader(reader)
        .map_err(|e| DiffError::SchemaParse(path.to_owned(), e.to_string()))
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
        assert_eq!(schema[0].mode, "REQUIRED");
    }

    #[test]
    fn test_load_schema_file_empty_schema() {
        let schema_json = "[]";
        let file = create_temp_schema_file(schema_json);

        let schema = load_schema_file_impl(file.path()).unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_load_schema_file_with_nested_fields() {
        let schema_json = r#"[
            {
                "name": "metadata",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
                    {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
                ]
            }
        ]"#;
        let file = create_temp_schema_file(schema_json);

        let schema = load_schema_file_impl(file.path()).unwrap();
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].name, "metadata");
        assert_eq!(schema[0].field_type, "RECORD");
        let nested = schema[0].fields.as_ref().unwrap();
        assert_eq!(nested.len(), 2);
        assert_eq!(nested[0].name, "created_at");
    }

    #[test]
    fn test_diff_format_parsing() {
        assert_eq!("text".parse::<DiffFormat>().unwrap(), DiffFormat::Text);
        assert_eq!("json".parse::<DiffFormat>().unwrap(), DiffFormat::Json);
        assert_eq!(
            "json-patch".parse::<DiffFormat>().unwrap(),
            DiffFormat::JsonPatch
        );
        assert_eq!("sql".parse::<DiffFormat>().unwrap(), DiffFormat::Sql);
        assert!("invalid".parse::<DiffFormat>().is_err());
    }

    #[test]
    fn test_color_mode_parsing() {
        assert_eq!("auto".parse::<ColorMode>().unwrap(), ColorMode::Auto);
        assert_eq!("always".parse::<ColorMode>().unwrap(), ColorMode::Always);
        assert_eq!("never".parse::<ColorMode>().unwrap(), ColorMode::Never);
        assert!("invalid".parse::<ColorMode>().is_err());
    }

    #[test]
    fn test_diff_format_case_insensitive() {
        assert_eq!("TEXT".parse::<DiffFormat>().unwrap(), DiffFormat::Text);
        assert_eq!("Json".parse::<DiffFormat>().unwrap(), DiffFormat::Json);
        assert_eq!(
            "JSON-PATCH".parse::<DiffFormat>().unwrap(),
            DiffFormat::JsonPatch
        );
    }

    #[test]
    fn test_color_mode_case_insensitive() {
        assert_eq!("AUTO".parse::<ColorMode>().unwrap(), ColorMode::Auto);
        assert_eq!("Always".parse::<ColorMode>().unwrap(), ColorMode::Always);
        assert_eq!("NEVER".parse::<ColorMode>().unwrap(), ColorMode::Never);
    }

    // ===== Tests for run_impl =====

    #[test]
    fn test_run_impl_invalid_format() {
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "invalid_format",
            "auto",
            false,
            None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::InvalidFormat(_)));
        assert!(err.to_string().contains("invalid_format"));
    }

    #[test]
    fn test_run_impl_invalid_color_mode() {
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "invalid_color",
            false,
            None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::InvalidColorMode(_)));
        assert!(err.to_string().contains("invalid_color"));
    }

    #[test]
    fn test_run_impl_old_schema_not_found() {
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        let result = run_impl(
            Path::new("/nonexistent/old_schema.json"),
            new_schema.path(),
            "text",
            "auto",
            false,
            None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::SchemaOpen(_, _)));
    }

    #[test]
    fn test_run_impl_new_schema_not_found() {
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        let result = run_impl(
            old_schema.path(),
            Path::new("/nonexistent/new_schema.json"),
            "text",
            "auto",
            false,
            None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::SchemaOpen(_, _)));
    }

    #[test]
    fn test_run_impl_old_schema_invalid_json() {
        let old_schema = create_temp_schema_file("not valid json");
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "auto",
            false,
            None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::SchemaParse(_, _)));
    }

    #[test]
    fn test_run_impl_new_schema_invalid_json() {
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let new_schema = create_temp_schema_file("not valid json");

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "auto",
            false,
            None,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::SchemaParse(_, _)));
    }

    #[test]
    fn test_run_impl_no_changes() {
        let schema = r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#;
        let old_schema = create_temp_schema_file(schema);
        let new_schema = create_temp_schema_file(schema);

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.has_breaking_changes);
        assert!(!output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_breaking_changes() {
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
        ]"#,
        ); // name field removed

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // Removing a field is a breaking change
        assert!(output.has_breaking_changes);
        assert!(output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_non_breaking_changes() {
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "new_field", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        ); // new_field added

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // Adding a NULLABLE field is not breaking
        assert!(!output.has_breaking_changes);
        assert!(output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_with_output_file() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("diff_output.txt");

        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            Some(&output_path),
        );

        assert!(result.is_ok());
        assert!(output_path.exists());

        // Verify output file has content
        let content = std::fs::read_to_string(&output_path).unwrap();
        assert!(!content.is_empty());
    }

    #[test]
    fn test_run_impl_json_format() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("diff_output.json");

        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "json",
            "never",
            false,
            Some(&output_path),
        );

        assert!(result.is_ok());

        // Verify output is valid JSON
        let content = std::fs::read_to_string(&output_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed.is_object() || parsed.is_array());
    }

    #[test]
    fn test_run_impl_json_patch_format() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("diff_output.json");

        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "json-patch",
            "never",
            false,
            Some(&output_path),
        );

        assert!(result.is_ok());

        // Verify output is valid JSON
        let content = std::fs::read_to_string(&output_path).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&content).unwrap();
        assert!(parsed.is_array());
    }

    #[test]
    fn test_run_impl_sql_format() {
        let temp_dir = TempDir::new().unwrap();
        let output_path = temp_dir.path().join("diff_output.sql");

        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "sql",
            "never",
            false,
            Some(&output_path),
        );

        assert!(result.is_ok());

        // Verify output has SQL-like content
        let content = std::fs::read_to_string(&output_path).unwrap();
        // SQL output might contain ALTER TABLE or other SQL keywords
        assert!(
            content.is_empty()
                || content.contains("ALTER")
                || content.contains("ADD")
                || content.contains("--")
        );
    }

    #[test]
    fn test_run_impl_strict_mode() {
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            true, // strict mode
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // In strict mode, even adding fields may be flagged as breaking
        assert!(output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_empty_schemas() {
        let old_schema = create_temp_schema_file("[]");
        let new_schema = create_temp_schema_file("[]");

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(!output.has_breaking_changes);
        assert!(!output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_nested_record_changes() {
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "user", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}
            ]}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "user", "type": "RECORD", "mode": "NULLABLE", "fields": [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                {"name": "email", "type": "STRING", "mode": "NULLABLE"}
            ]}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        assert!(output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_type_change_widening() {
        // Type widening to STRING is considered safe (non-breaking)
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "value", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "value", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // Type widening to STRING is considered safe (non-breaking)
        assert!(!output.has_breaking_changes);
        assert!(output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_type_change_narrowing() {
        // Type narrowing from STRING to INTEGER is breaking
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "value", "type": "STRING", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "value", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // Narrowing from STRING is breaking
        assert!(output.has_breaking_changes);
        assert!(output.diff.has_changes());
    }

    #[test]
    fn test_run_impl_mode_change_nullable_to_required() {
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // NULLABLE to REQUIRED is a breaking change
        assert!(output.has_breaking_changes);
    }

    #[test]
    fn test_run_impl_mode_change_required_to_nullable() {
        let old_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
        ]"#,
        );
        let new_schema = create_temp_schema_file(
            r#"[
            {"name": "id", "type": "INTEGER", "mode": "NULLABLE"}
        ]"#,
        );

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            None,
        );

        assert!(result.is_ok());
        let output = result.unwrap();
        // REQUIRED to NULLABLE is not a breaking change
        assert!(!output.has_breaking_changes);
    }

    #[test]
    fn test_run_impl_output_file_creation_error() {
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

        // Try to write to a directory that doesn't exist
        let invalid_path = PathBuf::from("/nonexistent/directory/output.txt");

        let result = run_impl(
            old_schema.path(),
            new_schema.path(),
            "text",
            "never",
            false,
            Some(&invalid_path),
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, DiffError::OutputCreate(_, _)));
    }

    #[test]
    fn test_diff_error_display() {
        // Test all error variant display implementations
        let err = DiffError::InvalidFormat("xml".to_string());
        assert!(err.to_string().contains("xml"));
        assert!(err.to_string().contains("text, json"));

        let err = DiffError::InvalidColorMode("rainbow".to_string());
        assert!(err.to_string().contains("rainbow"));
        assert!(err.to_string().contains("auto, always, never"));

        let err = DiffError::SchemaOpen(
            PathBuf::from("/path/to/schema.json"),
            std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        );
        assert!(err.to_string().contains("schema.json"));

        let err = DiffError::SchemaParse(
            PathBuf::from("/path/to/schema.json"),
            "invalid json".to_string(),
        );
        assert!(err.to_string().contains("invalid json"));

        let err = DiffError::OutputCreate(
            PathBuf::from("/path/to/output.txt"),
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied"),
        );
        assert!(err.to_string().contains("output.txt"));

        let err = DiffError::WriteDiff(std::io::Error::other("write error"));
        assert!(err.to_string().contains("write error"));
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
        assert!(matches!(result.unwrap_err(), DiffError::SchemaOpen(_, _)));
    }

    #[test]
    fn test_load_schema_file_impl_invalid_json() {
        let file = create_temp_schema_file("{ invalid json }");
        let result = load_schema_file_impl(file.path());
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), DiffError::SchemaParse(_, _)));
    }

    #[test]
    fn test_run_impl_all_color_modes() {
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "STRING", "mode": "NULLABLE"}]"#);

        // Test all valid color modes
        for color in &["auto", "always", "never"] {
            let result = run_impl(
                old_schema.path(),
                new_schema.path(),
                "text",
                color,
                false,
                None,
            );
            assert!(result.is_ok(), "Color mode '{}' should be valid", color);
        }
    }

    #[test]
    fn test_run_impl_all_formats() {
        let temp_dir = TempDir::new().unwrap();
        let old_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let new_schema =
            create_temp_schema_file(r#"[{"name": "id", "type": "STRING", "mode": "NULLABLE"}]"#);

        // Test all valid formats
        for format in &["text", "json", "json-patch", "sql"] {
            let output_path = temp_dir.path().join(format!("output.{}", format));
            let result = run_impl(
                old_schema.path(),
                new_schema.path(),
                format,
                "never",
                false,
                Some(&output_path),
            );
            assert!(result.is_ok(), "Format '{}' should be valid", format);
        }
    }
}
