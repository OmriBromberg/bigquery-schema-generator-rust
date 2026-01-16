//! Output formatters for schema diff results.

use super::{ChangeType, SchemaDiff};
use colored::Colorize;
use std::io::Write;

/// Output format for diff results
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DiffFormat {
    #[default]
    Text,
    Json,
    JsonPatch,
    Sql,
}

impl std::str::FromStr for DiffFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "text" => Ok(DiffFormat::Text),
            "json" => Ok(DiffFormat::Json),
            "json-patch" => Ok(DiffFormat::JsonPatch),
            "sql" => Ok(DiffFormat::Sql),
            _ => Err(format!("Unknown diff format: {}", s)),
        }
    }
}

/// Color mode for text output
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

impl std::str::FromStr for ColorMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "auto" => Ok(ColorMode::Auto),
            "always" => Ok(ColorMode::Always),
            "never" => Ok(ColorMode::Never),
            _ => Err(format!("Unknown color mode: {}", s)),
        }
    }
}

/// Write diff result in the specified format
pub fn write_diff<W: Write>(
    diff: &SchemaDiff,
    format: DiffFormat,
    color_mode: ColorMode,
    writer: &mut W,
) -> std::io::Result<()> {
    // Configure colored output
    match color_mode {
        ColorMode::Always => colored::control::set_override(true),
        ColorMode::Never => colored::control::set_override(false),
        ColorMode::Auto => colored::control::unset_override(),
    }

    match format {
        DiffFormat::Text => write_text_diff(diff, writer),
        DiffFormat::Json => write_json_diff(diff, writer),
        DiffFormat::JsonPatch => write_json_patch_diff(diff, writer),
        DiffFormat::Sql => write_sql_diff(diff, writer),
    }
}

/// Write human-readable text diff
fn write_text_diff<W: Write>(diff: &SchemaDiff, writer: &mut W) -> std::io::Result<()> {
    writeln!(writer, "{}", "Schema Diff Report".bold())?;
    writeln!(writer, "{}", "==================".bold())?;
    writeln!(writer)?;

    if !diff.has_changes() {
        writeln!(writer, "{}", "No changes detected.".green())?;
        return Ok(());
    }

    // Summary
    writeln!(
        writer,
        "Summary: {} added, {} removed, {} modified ({} breaking)",
        diff.summary.added.to_string().green(),
        diff.summary.removed.to_string().red(),
        diff.summary.modified.to_string().yellow(),
        if diff.summary.breaking > 0 {
            diff.summary.breaking.to_string().red().bold()
        } else {
            diff.summary.breaking.to_string().normal()
        }
    )?;
    writeln!(writer)?;

    // Added fields
    let added: Vec<_> = diff
        .changes
        .iter()
        .filter(|c| c.change_type == ChangeType::Added)
        .collect();
    if !added.is_empty() {
        writeln!(writer, "{}", "Added Fields:".green().bold())?;
        for change in added {
            let field_info = change
                .new_field
                .as_ref()
                .map(|f| format!("({}, {})", f.field_type, f.mode))
                .unwrap_or_default();
            writeln!(
                writer,
                "  {} {} {}",
                "+".green(),
                change.path.green(),
                field_info.dimmed()
            )?;
        }
        writeln!(writer)?;
    }

    // Removed fields
    let removed: Vec<_> = diff
        .changes
        .iter()
        .filter(|c| c.change_type == ChangeType::Removed)
        .collect();
    if !removed.is_empty() {
        writeln!(writer, "{}", "Removed Fields:".red().bold())?;
        for change in removed {
            let field_info = change
                .old_field
                .as_ref()
                .map(|f| format!("({}, {})", f.field_type, f.mode))
                .unwrap_or_default();
            let breaking_tag = if change.breaking {
                " [BREAKING]".red().bold()
            } else {
                "".normal()
            };
            writeln!(
                writer,
                "  {} {} {}{}",
                "-".red(),
                change.path.red(),
                field_info.dimmed(),
                breaking_tag
            )?;
        }
        writeln!(writer)?;
    }

    // Modified fields
    let modified: Vec<_> = diff
        .changes
        .iter()
        .filter(|c| c.change_type == ChangeType::Modified)
        .collect();
    if !modified.is_empty() {
        writeln!(writer, "{}", "Modified Fields:".yellow().bold())?;
        for change in modified {
            let breaking_tag = if change.breaking {
                " [BREAKING]".red().bold()
            } else {
                "".normal()
            };
            writeln!(
                writer,
                "  {} {}: {}{}",
                "~".yellow(),
                change.path.yellow(),
                change.description,
                breaking_tag
            )?;
        }
        writeln!(writer)?;
    }

    Ok(())
}

/// Write JSON format diff
fn write_json_diff<W: Write>(diff: &SchemaDiff, writer: &mut W) -> std::io::Result<()> {
    let json = serde_json::to_string_pretty(diff).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    })?;
    writeln!(writer, "{}", json)
}

/// Write RFC 6902 JSON Patch format
fn write_json_patch_diff<W: Write>(diff: &SchemaDiff, writer: &mut W) -> std::io::Result<()> {
    let mut patches: Vec<serde_json::Value> = Vec::new();

    for change in &diff.changes {
        let json_path = format!("/{}", change.path.replace('.', "/"));

        match change.change_type {
            ChangeType::Added => {
                if let Some(new_field) = &change.new_field {
                    patches.push(serde_json::json!({
                        "op": "add",
                        "path": json_path,
                        "value": {
                            "name": new_field.name,
                            "type": new_field.field_type,
                            "mode": new_field.mode
                        }
                    }));
                }
            }
            ChangeType::Removed => {
                patches.push(serde_json::json!({
                    "op": "remove",
                    "path": json_path
                }));
            }
            ChangeType::Modified => {
                if let Some(new_field) = &change.new_field {
                    patches.push(serde_json::json!({
                        "op": "replace",
                        "path": json_path,
                        "value": {
                            "name": new_field.name,
                            "type": new_field.field_type,
                            "mode": new_field.mode
                        }
                    }));
                }
            }
        }
    }

    let json = serde_json::to_string_pretty(&patches).map_err(|e| {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    })?;
    writeln!(writer, "{}", json)
}

/// Write SQL migration hints format
fn write_sql_diff<W: Write>(diff: &SchemaDiff, writer: &mut W) -> std::io::Result<()> {
    writeln!(writer, "-- BigQuery Schema Migration Hints")?;
    writeln!(writer, "-- Generated by bq-schema-gen diff")?;
    writeln!(writer)?;

    if !diff.has_changes() {
        writeln!(writer, "-- No changes detected")?;
        return Ok(());
    }

    if diff.has_breaking_changes() {
        writeln!(writer, "-- WARNING: {} breaking change(s) detected!", diff.summary.breaking)?;
        writeln!(writer)?;
    }

    for change in &diff.changes {
        match change.change_type {
            ChangeType::Added => {
                if let Some(new_field) = &change.new_field {
                    writeln!(writer, "-- ADD COLUMN: {}", change.path)?;
                    writeln!(
                        writer,
                        "-- ALTER TABLE <table> ADD COLUMN {} {} {};",
                        new_field.name,
                        new_field.field_type,
                        if new_field.mode == "REQUIRED" { "NOT NULL" } else { "" }
                    )?;
                    writeln!(writer)?;
                }
            }
            ChangeType::Removed => {
                writeln!(writer, "-- DROP COLUMN: {} [BREAKING]", change.path)?;
                writeln!(writer, "-- ALTER TABLE <table> DROP COLUMN {};", change.path)?;
                writeln!(writer, "-- Note: Ensure no queries depend on this column")?;
                writeln!(writer)?;
            }
            ChangeType::Modified => {
                let breaking_note = if change.breaking { " [BREAKING]" } else { "" };
                writeln!(writer, "-- MODIFY COLUMN: {}{}", change.path, breaking_note)?;
                writeln!(writer, "-- Change: {}", change.description)?;

                if let (Some(old), Some(new)) = (&change.old_field, &change.new_field) {
                    if old.field_type != new.field_type {
                        writeln!(
                            writer,
                            "-- Note: Type change {} -> {} may require data migration",
                            old.field_type, new.field_type
                        )?;
                    }
                    if old.mode != new.mode && new.mode == "REQUIRED" {
                        writeln!(
                            writer,
                            "-- Note: Changing to REQUIRED - ensure no NULL values exist"
                        )?;
                    }
                }
                writeln!(writer)?;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::{DiffOptions, diff_schemas};
    use crate::schema::types::BqSchemaField;

    fn make_field(name: &str, field_type: &str, mode: &str) -> BqSchemaField {
        BqSchemaField {
            name: name.to_string(),
            field_type: field_type.to_string(),
            mode: mode.to_string(),
            fields: None,
        }
    }

    #[test]
    fn test_text_format_output() {
        let old = vec![make_field("name", "STRING", "NULLABLE")];
        let new = vec![
            make_field("name", "STRING", "NULLABLE"),
            make_field("email", "STRING", "NULLABLE"),
        ];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Text, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("Added Fields:"));
        assert!(output_str.contains("email"));
    }

    #[test]
    fn test_json_format_output() {
        let old = vec![make_field("name", "STRING", "NULLABLE")];
        let new = vec![make_field("name", "STRING", "REQUIRED")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Json, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        // Should be valid JSON
        let _: serde_json::Value = serde_json::from_str(&output_str).unwrap();
        assert!(output_str.contains("\"breaking\""));
    }

    #[test]
    fn test_diff_format_from_str() {
        assert_eq!("text".parse::<DiffFormat>().unwrap(), DiffFormat::Text);
        assert_eq!("json".parse::<DiffFormat>().unwrap(), DiffFormat::Json);
        assert_eq!("json-patch".parse::<DiffFormat>().unwrap(), DiffFormat::JsonPatch);
        assert_eq!("sql".parse::<DiffFormat>().unwrap(), DiffFormat::Sql);
        assert!("invalid".parse::<DiffFormat>().is_err());
    }
}
