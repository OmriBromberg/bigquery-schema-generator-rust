//! Validate subcommand implementation.

use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use bq_schema_gen::validate::{SchemaValidator, ValidationResult};
use bq_schema_gen::{BqSchemaField, JsonRecordIterator, ValidationError, ValidationErrorType, ValidationOptions};

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
    // Validate format
    if format != "text" && format != "json" {
        eprintln!("Error: Unknown format '{}'. Use 'text' or 'json'.", format);
        std::process::exit(1);
    }

    // Load schema
    let schema = load_schema_file(schema_path);

    // Build validation options
    let options = ValidationOptions {
        allow_unknown,
        strict_types,
        max_errors,
    };

    // Collect input files
    let files = collect_input_files(file_patterns);

    if files.is_empty() {
        eprintln!("Error: No input files specified");
        std::process::exit(2);
    }

    // Create validator
    let validator = SchemaValidator::new(&schema, options.clone());
    let mut result = ValidationResult::new();
    let mut total_lines = 0;

    // Process each file
    for path in &files {
        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("Error: Cannot open input file '{}': {}", path.display(), e);
            std::process::exit(2);
        });

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

    // Output results
    if quiet {
        // Exit code only
        std::process::exit(if result.valid { 0 } else { 1 });
    }

    match format {
        "json" => {
            let json_output = serde_json::json!({
                "valid": result.valid,
                "error_count": result.error_count,
                "errors": result.errors,
                "warnings": result.warnings,
                "lines_processed": total_lines,
            });
            println!("{}", serde_json::to_string_pretty(&json_output).unwrap());
        }
        _ => {
            if result.valid {
                eprintln!("Validation passed ({} lines processed)", total_lines);
                if !result.warnings.is_empty() {
                    eprintln!("{} warning(s):", result.warnings.len());
                    for warning in &result.warnings {
                        eprintln!("  {}", warning);
                    }
                }
            } else {
                eprintln!(
                    "Validation failed ({} error{}):",
                    result.error_count,
                    if result.error_count == 1 { "" } else { "s" }
                );
                for error in &result.errors {
                    eprintln!("  {}", error);
                }
                if !result.warnings.is_empty() {
                    eprintln!("{} warning(s):", result.warnings.len());
                    for warning in &result.warnings {
                        eprintln!("  {}", warning);
                    }
                }
            }
        }
    }

    // Exit codes: 0 = valid, 1 = invalid, 2 = error reading files
    std::process::exit(if result.valid { 0 } else { 1 });
}

/// Collect input files from patterns
fn collect_input_files(patterns: &[String]) -> Vec<PathBuf> {
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
                eprintln!("Error: Invalid glob pattern '{}': {}", pattern, e);
                std::process::exit(2);
            }
        }
    }

    files
}

/// Load a BigQuery schema from a JSON file
fn load_schema_file(path: &Path) -> Vec<BqSchemaField> {
    let file = File::open(path).unwrap_or_else(|e| {
        eprintln!("Error: Cannot open schema file '{}': {}", path.display(), e);
        std::process::exit(1);
    });

    let reader = BufReader::new(file);
    serde_json::from_reader(reader).unwrap_or_else(|e| {
        eprintln!("Error: Cannot parse schema file '{}': {}", path.display(), e);
        std::process::exit(1);
    })
}
