//! Diff subcommand implementation.

use std::fs::File;
use std::io::{self, BufReader};
use std::path::{Path, PathBuf};

use bq_schema_gen::diff::output::{write_diff, ColorMode, DiffFormat};
use bq_schema_gen::diff::{diff_schemas, DiffOptions};
use bq_schema_gen::BqSchemaField;

/// Run the diff subcommand
pub fn run(
    old_schema_path: &Path,
    new_schema_path: &Path,
    format: &str,
    color: &str,
    strict: bool,
    output_path: Option<&PathBuf>,
) {
    // Parse format
    let diff_format: DiffFormat = format.parse().unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        eprintln!("Valid formats: text, json, json-patch, sql");
        std::process::exit(1);
    });

    // Parse color mode
    let color_mode: ColorMode = color.parse().unwrap_or_else(|e| {
        eprintln!("Error: {}", e);
        eprintln!("Valid color modes: auto, always, never");
        std::process::exit(1);
    });

    // Load old schema
    let old_schema = load_schema_file(old_schema_path);

    // Load new schema
    let new_schema = load_schema_file(new_schema_path);

    // Run diff
    let options = DiffOptions { strict };
    let diff = diff_schemas(&old_schema, &new_schema, &options);

    // Set up output
    let mut output: Box<dyn io::Write> = match output_path {
        Some(path) => {
            let file = File::create(path).unwrap_or_else(|e| {
                eprintln!(
                    "Error: Cannot create output file '{}': {}",
                    path.display(),
                    e
                );
                std::process::exit(1);
            });
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    // Write diff
    if let Err(e) = write_diff(&diff, diff_format, color_mode, &mut output) {
        eprintln!("Error writing diff: {}", e);
        std::process::exit(1);
    }

    // Exit with non-zero status if there are breaking changes
    if diff.has_breaking_changes() {
        std::process::exit(1);
    }
}

/// Load a BigQuery schema from a JSON file
fn load_schema_file(path: &Path) -> Vec<BqSchemaField> {
    let file = File::open(path).unwrap_or_else(|e| {
        eprintln!("Error: Cannot open schema file '{}': {}", path.display(), e);
        std::process::exit(1);
    });

    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap_or_else(|e| {
        eprintln!(
            "Error: Cannot parse schema file '{}': {}",
            path.display(),
            e
        );
        std::process::exit(1);
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

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

        let schema = load_schema_file(file.path());
        assert_eq!(schema.len(), 2);
        assert_eq!(schema[0].name, "id");
        assert_eq!(schema[0].field_type, "INTEGER");
        assert_eq!(schema[0].mode, "REQUIRED");
    }

    #[test]
    fn test_load_schema_file_empty_schema() {
        let schema_json = "[]";
        let file = create_temp_schema_file(schema_json);

        let schema = load_schema_file(file.path());
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

        let schema = load_schema_file(file.path());
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
}
