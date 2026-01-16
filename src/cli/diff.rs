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
                eprintln!("Error: Cannot create output file '{}': {}", path.display(), e);
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
        eprintln!("Error: Cannot parse schema file '{}': {}", path.display(), e);
        std::process::exit(1);
    })
}
