//! Schema diff module for comparing BigQuery schemas.
//!
//! This module provides functionality to compare two BigQuery schemas and
//! identify changes including additions, removals, and modifications.

pub mod output;

use crate::schema::types::BqSchemaField;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents the type of change detected between schemas
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeType {
    Added,
    Removed,
    Modified,
}

/// Represents a single change in the schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaChange {
    /// The path to the field (e.g., "user.address.city")
    pub path: String,
    /// The type of change
    pub change_type: ChangeType,
    /// Whether this is a breaking change
    pub breaking: bool,
    /// Description of the change
    pub description: String,
    /// Old field definition (for removed/modified)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_field: Option<FieldSnapshot>,
    /// New field definition (for added/modified)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_field: Option<FieldSnapshot>,
}

/// A snapshot of field properties for comparison
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldSnapshot {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub mode: String,
}

impl From<&BqSchemaField> for FieldSnapshot {
    fn from(field: &BqSchemaField) -> Self {
        FieldSnapshot {
            name: field.name.clone(),
            field_type: field.field_type.clone(),
            mode: field.mode.clone(),
        }
    }
}

/// Summary of changes between schemas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffSummary {
    pub added: usize,
    pub removed: usize,
    pub modified: usize,
    pub breaking: usize,
}

/// Result of comparing two schemas
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiff {
    pub summary: DiffSummary,
    pub changes: Vec<SchemaChange>,
}

impl SchemaDiff {
    /// Check if there are any changes
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Check if there are any breaking changes
    pub fn has_breaking_changes(&self) -> bool {
        self.summary.breaking > 0
    }

    /// Get only breaking changes
    pub fn breaking_changes(&self) -> Vec<&SchemaChange> {
        self.changes.iter().filter(|c| c.breaking).collect()
    }
}

/// Options for schema comparison
#[derive(Debug, Clone, Default)]
pub struct DiffOptions {
    /// Flag all changes as breaking (strict mode)
    pub strict: bool,
}

/// Compare two BigQuery schemas and return the differences
pub fn diff_schemas(
    old_schema: &[BqSchemaField],
    new_schema: &[BqSchemaField],
    options: &DiffOptions,
) -> SchemaDiff {
    let mut changes = Vec::new();
    diff_fields(old_schema, new_schema, "", &mut changes, options);

    let summary = DiffSummary {
        added: changes
            .iter()
            .filter(|c| c.change_type == ChangeType::Added)
            .count(),
        removed: changes
            .iter()
            .filter(|c| c.change_type == ChangeType::Removed)
            .count(),
        modified: changes
            .iter()
            .filter(|c| c.change_type == ChangeType::Modified)
            .count(),
        breaking: changes.iter().filter(|c| c.breaking).count(),
    };

    SchemaDiff { summary, changes }
}

/// Recursively compare fields between old and new schemas
fn diff_fields(
    old_fields: &[BqSchemaField],
    new_fields: &[BqSchemaField],
    prefix: &str,
    changes: &mut Vec<SchemaChange>,
    options: &DiffOptions,
) {
    // Build maps for quick lookup (case-insensitive)
    let old_map: HashMap<String, &BqSchemaField> = old_fields
        .iter()
        .map(|f| (f.name.to_lowercase(), f))
        .collect();
    let new_map: HashMap<String, &BqSchemaField> = new_fields
        .iter()
        .map(|f| (f.name.to_lowercase(), f))
        .collect();

    // Check for removed fields
    for old_field in old_fields {
        let key = old_field.name.to_lowercase();
        let path = if prefix.is_empty() {
            old_field.name.clone()
        } else {
            format!("{}.{}", prefix, old_field.name)
        };

        if !new_map.contains_key(&key) {
            changes.push(SchemaChange {
                path,
                change_type: ChangeType::Removed,
                breaking: true, // Field removal is always breaking
                description: format!(
                    "Field removed: {} ({}, {})",
                    old_field.name, old_field.field_type, old_field.mode
                ),
                old_field: Some(old_field.into()),
                new_field: None,
            });
        }
    }

    // Check for added and modified fields
    for new_field in new_fields {
        let key = new_field.name.to_lowercase();
        let path = if prefix.is_empty() {
            new_field.name.clone()
        } else {
            format!("{}.{}", prefix, new_field.name)
        };

        match old_map.get(&key) {
            None => {
                // New field added
                changes.push(SchemaChange {
                    path,
                    change_type: ChangeType::Added,
                    breaking: options.strict, // Not breaking unless strict mode
                    description: format!(
                        "Field added: {} ({}, {})",
                        new_field.name, new_field.field_type, new_field.mode
                    ),
                    old_field: None,
                    new_field: Some(new_field.into()),
                });
            }
            Some(old_field) => {
                // Check for modifications
                compare_fields(old_field, new_field, &path, changes, options);
            }
        }
    }
}

/// Compare two fields and record any changes
fn compare_fields(
    old_field: &BqSchemaField,
    new_field: &BqSchemaField,
    path: &str,
    changes: &mut Vec<SchemaChange>,
    options: &DiffOptions,
) {
    // Check type change
    if old_field.field_type != new_field.field_type {
        let breaking =
            is_type_change_breaking(&old_field.field_type, &new_field.field_type, options);
        changes.push(SchemaChange {
            path: path.to_string(),
            change_type: ChangeType::Modified,
            breaking,
            description: format!(
                "Type changed: {} -> {}",
                old_field.field_type, new_field.field_type
            ),
            old_field: Some(old_field.into()),
            new_field: Some(new_field.into()),
        });
    }

    // Check mode change
    if old_field.mode != new_field.mode {
        let breaking = is_mode_change_breaking(&old_field.mode, &new_field.mode, options);
        changes.push(SchemaChange {
            path: path.to_string(),
            change_type: ChangeType::Modified,
            breaking,
            description: format!("Mode changed: {} -> {}", old_field.mode, new_field.mode),
            old_field: Some(old_field.into()),
            new_field: Some(new_field.into()),
        });
    }

    // Recursively compare nested fields for RECORD types
    if old_field.field_type == "RECORD" && new_field.field_type == "RECORD" {
        if let (Some(old_fields), Some(new_fields)) = (&old_field.fields, &new_field.fields) {
            diff_fields(old_fields, new_fields, path, changes, options);
        }
    }
}

/// Determine if a type change is breaking
fn is_type_change_breaking(old_type: &str, new_type: &str, options: &DiffOptions) -> bool {
    if options.strict {
        return true;
    }

    // Type widening that's generally safe (not breaking)
    let safe_widening = matches!(
        (old_type, new_type),
        // Integer to Float is safe (widening)
        ("INTEGER", "FLOAT") |
        // Any type to String is generally safe
        (_, "STRING")
    );

    !safe_widening
}

/// Determine if a mode change is breaking
fn is_mode_change_breaking(old_mode: &str, new_mode: &str, options: &DiffOptions) -> bool {
    if options.strict {
        return true;
    }

    // NULLABLE -> REQUIRED is breaking (existing null values will fail)
    // REQUIRED -> NULLABLE is safe
    // REPEATED -> NULLABLE/REQUIRED is breaking
    // NULLABLE/REQUIRED -> REPEATED is breaking

    matches!(
        (old_mode, new_mode),
        ("NULLABLE", "REQUIRED")
            | ("REPEATED", "NULLABLE")
            | ("REPEATED", "REQUIRED")
            | ("NULLABLE", "REPEATED")
            | ("REQUIRED", "REPEATED")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_field(name: &str, field_type: &str, mode: &str) -> BqSchemaField {
        BqSchemaField {
            name: name.to_string(),
            field_type: field_type.to_string(),
            mode: mode.to_string(),
            fields: None,
        }
    }

    fn make_record(name: &str, mode: &str, fields: Vec<BqSchemaField>) -> BqSchemaField {
        BqSchemaField {
            name: name.to_string(),
            field_type: "RECORD".to_string(),
            mode: mode.to_string(),
            fields: Some(fields),
        }
    }

    #[test]
    fn test_no_changes() {
        let old = vec![make_field("name", "STRING", "NULLABLE")];
        let new = vec![make_field("name", "STRING", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(!diff.has_changes());
        assert_eq!(diff.summary.added, 0);
        assert_eq!(diff.summary.removed, 0);
        assert_eq!(diff.summary.modified, 0);
    }

    #[test]
    fn test_field_added() {
        let old = vec![make_field("name", "STRING", "NULLABLE")];
        let new = vec![
            make_field("name", "STRING", "NULLABLE"),
            make_field("email", "STRING", "NULLABLE"),
        ];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert_eq!(diff.summary.added, 1);
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_field_removed() {
        let old = vec![
            make_field("name", "STRING", "NULLABLE"),
            make_field("email", "STRING", "NULLABLE"),
        ];
        let new = vec![make_field("name", "STRING", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert_eq!(diff.summary.removed, 1);
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_type_changed_breaking() {
        let old = vec![make_field("count", "STRING", "NULLABLE")];
        let new = vec![make_field("count", "INTEGER", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert_eq!(diff.summary.modified, 1);
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_type_widening_not_breaking() {
        let old = vec![make_field("value", "INTEGER", "NULLABLE")];
        let new = vec![make_field("value", "FLOAT", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert_eq!(diff.summary.modified, 1);
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_mode_nullable_to_required_breaking() {
        let old = vec![make_field("name", "STRING", "NULLABLE")];
        let new = vec![make_field("name", "STRING", "REQUIRED")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert!(diff.has_breaking_changes());
    }

    #[test]
    fn test_mode_required_to_nullable_not_breaking() {
        let old = vec![make_field("name", "STRING", "REQUIRED")];
        let new = vec![make_field("name", "STRING", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert!(!diff.has_breaking_changes());
    }

    #[test]
    fn test_nested_field_change() {
        let old = vec![make_record(
            "user",
            "NULLABLE",
            vec![make_field("name", "STRING", "NULLABLE")],
        )];
        let new = vec![make_record(
            "user",
            "NULLABLE",
            vec![
                make_field("name", "STRING", "NULLABLE"),
                make_field("email", "STRING", "NULLABLE"),
            ],
        )];

        let diff = diff_schemas(&old, &new, &DiffOptions::default());
        assert!(diff.has_changes());
        assert_eq!(diff.summary.added, 1);
        assert!(diff.changes[0].path.contains("user.email"));
    }

    #[test]
    fn test_strict_mode() {
        let old = vec![make_field("value", "INTEGER", "NULLABLE")];
        let new = vec![make_field("value", "FLOAT", "NULLABLE")];

        let diff = diff_schemas(&old, &new, &DiffOptions { strict: true });
        assert!(diff.has_changes());
        assert!(diff.has_breaking_changes()); // Strict mode flags all changes
    }
}
