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
    let json =
        serde_json::to_string_pretty(diff).map_err(|e| std::io::Error::other(e.to_string()))?;
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

    let json =
        serde_json::to_string_pretty(&patches).map_err(|e| std::io::Error::other(e.to_string()))?;
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
        writeln!(
            writer,
            "-- WARNING: {} breaking change(s) detected!",
            diff.summary.breaking
        )?;
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
                        if new_field.mode == "REQUIRED" {
                            "NOT NULL"
                        } else {
                            ""
                        }
                    )?;
                    writeln!(writer)?;
                }
            }
            ChangeType::Removed => {
                writeln!(writer, "-- DROP COLUMN: {} [BREAKING]", change.path)?;
                writeln!(
                    writer,
                    "-- ALTER TABLE <table> DROP COLUMN {};",
                    change.path
                )?;
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
    use crate::diff::{diff_schemas, DiffOptions};
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
        assert_eq!(
            "json-patch".parse::<DiffFormat>().unwrap(),
            DiffFormat::JsonPatch
        );
        assert_eq!("sql".parse::<DiffFormat>().unwrap(), DiffFormat::Sql);
        assert!("invalid".parse::<DiffFormat>().is_err());
    }

    #[test]
    fn test_write_json_patch_added_field() {
        let old = vec![];
        let new = vec![make_field("new_field", "STRING", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::JsonPatch, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        // Should be valid JSON
        let patches: Vec<serde_json::Value> = serde_json::from_str(&output_str).unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0]["op"], "add");
        assert!(patches[0]["path"].as_str().unwrap().contains("new_field"));
    }

    #[test]
    fn test_write_json_patch_removed_field() {
        let old = vec![make_field("old_field", "STRING", "NULLABLE")];
        let new = vec![];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::JsonPatch, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        let patches: Vec<serde_json::Value> = serde_json::from_str(&output_str).unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0]["op"], "remove");
        assert!(patches[0]["path"].as_str().unwrap().contains("old_field"));
    }

    #[test]
    fn test_write_json_patch_modified_field() {
        let old = vec![make_field("field", "STRING", "NULLABLE")];
        let new = vec![make_field("field", "STRING", "REQUIRED")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::JsonPatch, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        let patches: Vec<serde_json::Value> = serde_json::from_str(&output_str).unwrap();
        assert_eq!(patches.len(), 1);
        assert_eq!(patches[0]["op"], "replace");
        assert_eq!(patches[0]["value"]["mode"], "REQUIRED");
    }

    #[test]
    fn test_write_sql_diff_breaking_changes() {
        let old = vec![make_field("removed_field", "STRING", "NULLABLE")];
        let new = vec![];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Sql, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("WARNING"));
        assert!(output_str.contains("breaking"));
        assert!(output_str.contains("DROP COLUMN"));
    }

    #[test]
    fn test_write_sql_diff_non_breaking_changes() {
        let old = vec![make_field("existing", "STRING", "NULLABLE")];
        let new = vec![
            make_field("existing", "STRING", "NULLABLE"),
            make_field("new_field", "INTEGER", "NULLABLE"),
        ];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Sql, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("ADD COLUMN"));
        assert!(output_str.contains("new_field"));
        // Should not have breaking warning for additions
        assert!(!output_str.contains("WARNING"));
    }

    #[test]
    fn test_write_sql_diff_type_change() {
        let old = vec![make_field("field", "STRING", "NULLABLE")];
        let new = vec![make_field("field", "INTEGER", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Sql, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("MODIFY COLUMN"));
        assert!(output_str.contains("data migration"));
        assert!(output_str.contains("STRING"));
        assert!(output_str.contains("INTEGER"));
    }

    #[test]
    fn test_write_sql_diff_mode_to_required() {
        let old = vec![make_field("field", "STRING", "NULLABLE")];
        let new = vec![make_field("field", "STRING", "REQUIRED")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Sql, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("REQUIRED"));
        assert!(output_str.contains("NULL values"));
    }

    #[test]
    fn test_write_text_diff_no_changes() {
        let schema = vec![make_field("field", "STRING", "NULLABLE")];

        let diff = diff_schemas(&schema, &schema, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Text, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("No changes detected"));
    }

    #[test]
    fn test_write_json_diff_structure() {
        let old = vec![make_field("a", "STRING", "NULLABLE")];
        let new = vec![make_field("b", "INTEGER", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Json, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        let json: serde_json::Value = serde_json::from_str(&output_str).unwrap();

        // Check structure
        assert!(json.get("summary").is_some());
        assert!(json.get("changes").is_some());
        assert!(json["summary"].get("added").is_some());
        assert!(json["summary"].get("removed").is_some());
        assert!(json["summary"].get("modified").is_some());
        assert!(json["summary"].get("breaking").is_some());
    }

    #[test]
    fn test_special_characters_in_field_names() {
        let old = vec![];
        let new = vec![make_field("field.with.dots", "STRING", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());

        // Test JSON Patch format handles dots correctly
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::JsonPatch, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();
        // Path should convert dots to slashes for JSON Pointer format
        assert!(output_str.contains("/field/with/dots"));
    }

    #[test]
    fn test_multiple_changes_combined() {
        let old = vec![
            make_field("unchanged", "STRING", "NULLABLE"),
            make_field("removed", "INTEGER", "NULLABLE"),
            make_field("modified", "STRING", "NULLABLE"),
        ];
        let new = vec![
            make_field("unchanged", "STRING", "NULLABLE"),
            make_field("modified", "STRING", "REQUIRED"),
            make_field("added", "BOOLEAN", "NULLABLE"),
        ];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());

        // Test text format
        let mut text_output = Vec::new();
        write_diff(&diff, DiffFormat::Text, ColorMode::Never, &mut text_output).unwrap();
        let text_str = String::from_utf8(text_output).unwrap();

        assert!(text_str.contains("Added Fields"));
        assert!(text_str.contains("Removed Fields"));
        assert!(text_str.contains("Modified Fields"));
        assert!(text_str.contains("added"));
        assert!(text_str.contains("removed"));
        assert!(text_str.contains("modified"));
    }

    #[test]
    fn test_color_mode_override() {
        let old = vec![];
        let new = vec![make_field("field", "STRING", "NULLABLE")];
        let diff = diff_schemas(&old, &new, &DiffOptions::default());

        // Test with different color modes - all should produce valid output
        for mode in [ColorMode::Auto, ColorMode::Always, ColorMode::Never] {
            let mut output = Vec::new();
            let result = write_diff(&diff, DiffFormat::Text, mode, &mut output);
            assert!(result.is_ok());
            assert!(!output.is_empty());
        }
    }

    #[test]
    fn test_json_patch_empty_diff() {
        let schema = vec![make_field("field", "STRING", "NULLABLE")];

        let diff = diff_schemas(&schema, &schema, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::JsonPatch, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        let patches: Vec<serde_json::Value> = serde_json::from_str(&output_str).unwrap();
        assert!(patches.is_empty());
    }

    #[test]
    fn test_sql_diff_no_changes() {
        let schema = vec![make_field("field", "STRING", "NULLABLE")];

        let diff = diff_schemas(&schema, &schema, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Sql, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("No changes detected"));
    }

    #[test]
    fn test_nested_record_diff_text() {
        let old = vec![BqSchemaField {
            name: "parent".to_string(),
            field_type: "RECORD".to_string(),
            mode: "NULLABLE".to_string(),
            fields: Some(vec![make_field("child", "STRING", "NULLABLE")]),
        }];
        let new = vec![BqSchemaField {
            name: "parent".to_string(),
            field_type: "RECORD".to_string(),
            mode: "NULLABLE".to_string(),
            fields: Some(vec![
                make_field("child", "STRING", "NULLABLE"),
                make_field("new_child", "INTEGER", "NULLABLE"),
            ]),
        }];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        let mut output = Vec::new();
        write_diff(&diff, DiffFormat::Text, ColorMode::Never, &mut output).unwrap();
        let output_str = String::from_utf8(output).unwrap();

        assert!(output_str.contains("parent.new_child") || output_str.contains("new_child"));
    }
}
