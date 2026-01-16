//! Schema generator - the core logic for deducing BigQuery schemas.
//!
//! This module contains the `SchemaGenerator` struct which processes
//! JSON/CSV records and builds a BigQuery-compatible schema.

use once_cell::sync::Lazy;
use regex::Regex;

use crate::error::{Error, ErrorLog, Result};
use crate::inference::{convert_type, infer_bigquery_type};
use crate::schema::types::{BqMode, BqSchemaField, BqType, EntryStatus, SchemaEntry, SchemaMap};

/// Valid BigQuery field name pattern.
static FIELD_NAME_SANITIZER: Lazy<Regex> = Lazy::new(|| Regex::new(r"[^a-zA-Z0-9_]").unwrap());

/// Configuration options for schema generation.
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Input format: "json", "csv"
    pub input_format: InputFormat,
    /// Infer REQUIRED mode for CSV fields that are always filled
    pub infer_mode: bool,
    /// Keep null/empty fields in output schema
    pub keep_nulls: bool,
    /// Treat quoted values as strings (don't infer types)
    pub quoted_values_are_strings: bool,
    /// Sanitize field names for BigQuery compatibility
    pub sanitize_names: bool,
    /// Preserve input field order instead of sorting alphabetically
    pub preserve_input_sort_order: bool,
}

impl Default for GeneratorConfig {
    fn default() -> Self {
        Self {
            input_format: InputFormat::Json,
            infer_mode: false,
            keep_nulls: false,
            quoted_values_are_strings: false,
            sanitize_names: false,
            preserve_input_sort_order: false,
        }
    }
}

/// Input format for the schema generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputFormat {
    Json,
    Csv,
}

/// Schema generator that processes records and builds a BigQuery schema.
pub struct SchemaGenerator {
    config: GeneratorConfig,
    line_number: usize,
    error_logs: Vec<ErrorLog>,
}

impl SchemaGenerator {
    /// Create a new schema generator with the given configuration.
    ///
    /// Note: For CSV input format, `keep_nulls` is automatically set to `true`
    /// to ensure positional column matching works correctly.
    pub fn new(mut config: GeneratorConfig) -> Self {
        // CSV requires keep_nulls to be true (matches Python behavior)
        if config.input_format == InputFormat::Csv {
            config.keep_nulls = true;
        }

        Self {
            config,
            line_number: 0,
            error_logs: Vec::new(),
        }
    }

    /// Create a new schema generator with default configuration.
    pub fn default_config() -> Self {
        Self::new(GeneratorConfig::default())
    }

    /// Get the current line number.
    pub fn line_number(&self) -> usize {
        self.line_number
    }

    /// Get the error logs.
    pub fn error_logs(&self) -> &[ErrorLog] {
        &self.error_logs
    }

    /// Log an error at the current line.
    fn log_error(&mut self, msg: String) {
        self.error_logs.push(ErrorLog {
            line_number: self.line_number,
            msg,
        });
    }

    /// Process a single JSON record and update the schema map.
    pub fn process_record(
        &mut self,
        record: &serde_json::Value,
        schema_map: &mut SchemaMap,
    ) -> Result<()> {
        self.line_number += 1;

        match record {
            serde_json::Value::Object(obj) => {
                self.deduce_schema_for_record(obj, schema_map, None);
                Ok(())
            }
            _ => {
                let msg = format!(
                    "Record should be a JSON Object but was a {:?}",
                    json_type_name(record)
                );
                self.log_error(msg.clone());
                Err(Error::InvalidRecord(msg))
            }
        }
    }

    /// Deduce schema for a single record (JSON object).
    fn deduce_schema_for_record(
        &mut self,
        obj: &serde_json::Map<String, serde_json::Value>,
        schema_map: &mut SchemaMap,
        base_path: Option<&str>,
    ) {
        for (key, value) in obj {
            let sanitized_key = self.sanitize_name(key);
            let canonical_key = sanitized_key.to_lowercase();

            let new_entry = match self.get_schema_entry(&sanitized_key, value, base_path) {
                Some(entry) => entry,
                None => continue, // Unsupported type, skip
            };

            // Check if entry exists - if so, merge in place to preserve order
            if let Some(existing_entry) = schema_map.get(&canonical_key).cloned() {
                let merged_entry =
                    self.merge_schema_entry(Some(existing_entry), new_entry, base_path);

                if let Some(entry) = merged_entry {
                    // Update in place to preserve order
                    if let Some(slot) = schema_map.get_mut(&canonical_key) {
                        *slot = entry;
                    }
                } else {
                    // Remove if merge resulted in None (shouldn't happen normally)
                    schema_map.shift_remove(&canonical_key);
                }
            } else {
                // New field - just insert
                let merged_entry = self.merge_schema_entry(None, new_entry, base_path);
                if let Some(entry) = merged_entry {
                    schema_map.insert(canonical_key, entry);
                }
            }
        }
    }

    /// Sanitize a field name for BigQuery compatibility.
    fn sanitize_name(&self, name: &str) -> String {
        if self.config.sanitize_names {
            let sanitized = FIELD_NAME_SANITIZER.replace_all(name, "_");
            // Truncate to 128 characters (BigQuery limit)
            if sanitized.len() > 128 {
                sanitized[..128].to_string()
            } else {
                sanitized.into_owned()
            }
        } else {
            name.to_string()
        }
    }

    /// Get a schema entry for a key-value pair.
    fn get_schema_entry(
        &mut self,
        key: &str,
        value: &serde_json::Value,
        base_path: Option<&str>,
    ) -> Option<SchemaEntry> {
        let result = infer_bigquery_type(value, self.config.quoted_values_are_strings);

        let (mode, bq_type) = match result {
            Some(r) => r,
            None => {
                // Log error for unsupported types
                if let serde_json::Value::Array(arr) = value {
                    // Check what kind of array error
                    if arr.iter().any(|v| matches!(v, serde_json::Value::Array(_))) {
                        if arr
                            .iter()
                            .all(|v| matches!(v, serde_json::Value::Array(a) if a.is_empty()))
                        {
                            self.log_error(
                                "Unsupported array element type: __empty_array__".to_string(),
                            );
                        } else {
                            self.log_error("Unsupported array element type: __array__".to_string());
                        }
                    } else {
                        self.log_error(format!(
                            "All array elements must be the same compatible type: {:?}",
                            arr
                        ));
                    }
                }
                return None;
            }
        };

        match &bq_type {
            BqType::Record(_) => {
                // Recursively process nested record
                let new_base_path = json_full_path(base_path, key);
                let mut fields = SchemaMap::new();

                if mode == BqMode::Nullable {
                    // Single object
                    if let serde_json::Value::Object(obj) = value {
                        self.deduce_schema_for_record(obj, &mut fields, Some(&new_base_path));
                    }
                } else {
                    // Array of objects (REPEATED)
                    if let serde_json::Value::Array(arr) = value {
                        for item in arr {
                            if let serde_json::Value::Object(obj) = item {
                                self.deduce_schema_for_record(
                                    obj,
                                    &mut fields,
                                    Some(&new_base_path),
                                );
                            }
                        }
                    }
                }

                Some(SchemaEntry {
                    status: EntryStatus::Hard,
                    filled: true,
                    name: key.to_string(),
                    bq_type: BqType::Record(fields),
                    mode,
                })
            }
            BqType::Null => Some(SchemaEntry {
                status: EntryStatus::Soft,
                filled: false,
                name: key.to_string(),
                bq_type: BqType::String,
                mode: BqMode::Nullable,
            }),
            BqType::EmptyArray => Some(SchemaEntry {
                status: EntryStatus::Soft,
                filled: false,
                name: key.to_string(),
                bq_type: BqType::String,
                mode: BqMode::Repeated,
            }),
            BqType::EmptyRecord => Some(SchemaEntry {
                status: EntryStatus::Soft,
                filled: false,
                name: key.to_string(),
                bq_type: BqType::Record(SchemaMap::new()),
                mode,
            }),
            _ => {
                // Check for empty string in CSV mode
                let (status, filled) = if self.config.input_format == InputFormat::Csv {
                    if let serde_json::Value::String(s) = value {
                        if s.is_empty() {
                            (EntryStatus::Soft, false)
                        } else {
                            (EntryStatus::Hard, true)
                        }
                    } else {
                        (EntryStatus::Hard, true)
                    }
                } else {
                    (EntryStatus::Hard, true)
                };

                Some(SchemaEntry {
                    status,
                    filled,
                    name: key.to_string(),
                    bq_type,
                    mode,
                })
            }
        }
    }

    /// Merge a new schema entry with an existing one.
    fn merge_schema_entry(
        &mut self,
        old_entry: Option<SchemaEntry>,
        new_entry: SchemaEntry,
        base_path: Option<&str>,
    ) -> Option<SchemaEntry> {
        let mut old_entry = match old_entry {
            Some(e) => e,
            None => return Some(new_entry),
        };

        // Track filled status
        if !new_entry.filled || !old_entry.filled {
            old_entry.filled = false;
        }

        // If old was ignored, keep ignoring
        if old_entry.status == EntryStatus::Ignore {
            return Some(old_entry);
        }

        // Hard -> Soft: keep old hard
        if old_entry.status == EntryStatus::Hard && new_entry.status == EntryStatus::Soft {
            if let Some(mode) = self.merge_mode(&old_entry, &new_entry, base_path) {
                old_entry.mode = mode;
                return Some(old_entry);
            } else {
                old_entry.status = EntryStatus::Ignore;
                return Some(old_entry);
            }
        }

        // Soft -> Hard: use new hard
        if old_entry.status == EntryStatus::Soft && new_entry.status == EntryStatus::Hard {
            let mut result = new_entry;
            result.filled = old_entry.filled;
            if let Some(mode) = self.merge_mode(&old_entry, &result, base_path) {
                result.mode = mode;
                return Some(result);
            } else {
                old_entry.status = EntryStatus::Ignore;
                return Some(old_entry);
            }
        }

        // Same status - merge types
        let old_type = &old_entry.bq_type;
        let new_type = &new_entry.bq_type;

        // Handle RECORD + RECORD merging
        if let (BqType::Record(old_fields), BqType::Record(new_fields)) = (old_type, new_type) {
            // Allow NULLABLE RECORD -> REPEATED RECORD
            if old_entry.mode == BqMode::Nullable && new_entry.mode == BqMode::Repeated {
                let full_name = json_full_path(base_path, &old_entry.name);
                self.log_error(format!(
                    "Converting schema for \"{}\" from NULLABLE RECORD into REPEATED RECORD",
                    full_name
                ));
                old_entry.mode = BqMode::Repeated;
            } else if old_entry.mode == BqMode::Repeated && new_entry.mode == BqMode::Nullable {
                let full_name = json_full_path(base_path, &old_entry.name);
                self.log_error(format!(
                    "Leaving schema for \"{}\" as REPEATED RECORD",
                    full_name
                ));
            }

            // Merge the record fields
            let mut merged_fields = old_fields.clone();
            let new_base_path = json_full_path(base_path, &old_entry.name);

            for (key, new_field_entry) in new_fields {
                if let Some(existing) = merged_fields.get(key).cloned() {
                    // Update existing field in place to preserve order
                    if let Some(merged) = self.merge_schema_entry(
                        Some(existing),
                        new_field_entry.clone(),
                        Some(&new_base_path),
                    ) {
                        if let Some(slot) = merged_fields.get_mut(key) {
                            *slot = merged;
                        }
                    } else {
                        merged_fields.shift_remove(key);
                    }
                } else {
                    // New field - insert at end
                    if let Some(merged) =
                        self.merge_schema_entry(None, new_field_entry.clone(), Some(&new_base_path))
                    {
                        merged_fields.insert(key.clone(), merged);
                    }
                }
            }

            old_entry.bq_type = BqType::Record(merged_fields);
            return Some(old_entry);
        }

        // Merge mode
        let merged_mode = match self.merge_mode(&old_entry, &new_entry, base_path) {
            Some(m) => m,
            None => {
                old_entry.status = EntryStatus::Ignore;
                return Some(old_entry);
            }
        };

        // Merge types
        if old_type != new_type {
            match convert_type(old_type, new_type) {
                Some(converted) => {
                    old_entry.bq_type = converted;
                    old_entry.mode = merged_mode;
                    Some(old_entry)
                }
                None => {
                    let full_old_name = json_full_path(base_path, &old_entry.name);
                    let full_new_name = json_full_path(base_path, &new_entry.name);
                    self.log_error(format!(
                        "Ignoring field with mismatched type: old=({:?},{},{},{:?}); new=({:?},{},{},{:?})",
                        old_entry.status, full_old_name, old_entry.mode, old_entry.bq_type,
                        new_entry.status, full_new_name, new_entry.mode, new_entry.bq_type
                    ));
                    old_entry.status = EntryStatus::Ignore;
                    Some(old_entry)
                }
            }
        } else {
            old_entry.mode = merged_mode;
            Some(old_entry)
        }
    }

    /// Merge field modes, returning None if incompatible.
    fn merge_mode(
        &mut self,
        old_entry: &SchemaEntry,
        new_entry: &SchemaEntry,
        base_path: Option<&str>,
    ) -> Option<BqMode> {
        let old_mode = old_entry.mode;
        let new_mode = new_entry.mode;

        // Same mode - no change needed
        if old_mode == new_mode {
            return Some(old_mode);
        }

        let full_old_name = json_full_path(base_path, &old_entry.name);
        let full_new_name = json_full_path(base_path, &new_entry.name);

        // REQUIRED -> NULLABLE transition
        if old_mode == BqMode::Required && new_mode == BqMode::Nullable {
            if new_entry.filled {
                return Some(old_mode); // Keep REQUIRED
            } else if self.config.infer_mode {
                return Some(new_mode); // Allow relaxation
            } else {
                self.log_error(format!(
                    "Ignoring non-RECORD field with mismatched mode. Cannot convert to NULLABLE because infer_schema not set: old=({:?},{},{},{:?}); new=({:?},{},{},{:?})",
                    old_entry.status, full_old_name, old_mode, old_entry.bq_type,
                    new_entry.status, full_new_name, new_mode, new_entry.bq_type
                ));
                return None;
            }
        }

        // NULLABLE(soft) -> REPEATED(hard)
        if old_mode == BqMode::Nullable && new_mode == BqMode::Repeated {
            if old_entry.status == EntryStatus::Soft && new_entry.status == EntryStatus::Hard {
                return Some(new_mode);
            }
            self.log_error(format!(
                "Cannot convert NULLABLE(hard) -> REPEATED: old=({:?},{},{},{:?}); new=({:?},{},{},{:?})",
                old_entry.status, full_old_name, old_mode, old_entry.bq_type,
                new_entry.status, full_new_name, new_mode, new_entry.bq_type
            ));
            return None;
        }

        // REPEATED -> NULLABLE(soft): keep REPEATED
        if old_mode == BqMode::Repeated && new_mode == BqMode::Nullable {
            if old_entry.status == EntryStatus::Hard && new_entry.status == EntryStatus::Soft {
                return Some(old_mode);
            }
            self.log_error(format!(
                "Cannot convert REPEATED -> NULLABLE(hard): old=({:?},{},{},{:?}); new=({:?},{},{},{:?})",
                old_entry.status, full_old_name, old_mode, old_entry.bq_type,
                new_entry.status, full_new_name, new_mode, new_entry.bq_type
            ));
            return None;
        }

        // Other mode mismatches
        self.log_error(format!(
            "Ignoring non-RECORD field with mismatched mode: old=({:?},{},{},{:?}); new=({:?},{},{},{:?})",
            old_entry.status, full_old_name, old_mode, old_entry.bq_type,
            new_entry.status, full_new_name, new_mode, new_entry.bq_type
        ));
        None
    }

    /// Convert the schema map to BigQuery JSON schema format.
    pub fn flatten_schema(&self, schema_map: &SchemaMap) -> Vec<BqSchemaField> {
        self.flatten_schema_map(schema_map)
    }

    fn flatten_schema_map(&self, schema_map: &SchemaMap) -> Vec<BqSchemaField> {
        let mut result = Vec::new();

        // Get items, optionally sorted
        let items: Vec<_> = if self.config.preserve_input_sort_order
            || self.config.input_format == InputFormat::Csv
        {
            schema_map.iter().collect()
        } else {
            let mut items: Vec<_> = schema_map.iter().collect();
            items.sort_by(|a, b| a.0.cmp(b.0));
            items
        };

        for (_canonical_name, entry) in items {
            // Skip ignored entries
            if entry.status == EntryStatus::Ignore {
                continue;
            }

            // Skip soft entries unless keep_nulls is enabled
            if entry.status == EntryStatus::Soft && !self.config.keep_nulls {
                continue;
            }

            let mode = self.determine_output_mode(entry);
            let field = self.entry_to_schema_field(entry, mode);
            result.push(field);
        }

        result
    }

    fn determine_output_mode(&self, entry: &SchemaEntry) -> BqMode {
        // Infer REQUIRED mode for CSV with infer_mode enabled
        if self.config.infer_mode
            && self.config.input_format == InputFormat::Csv
            && entry.mode == BqMode::Nullable
            && entry.filled
        {
            BqMode::Required
        } else {
            entry.mode
        }
    }

    fn entry_to_schema_field(&self, entry: &SchemaEntry, mode: BqMode) -> BqSchemaField {
        match &entry.bq_type {
            BqType::Record(fields) => {
                let nested_fields = if fields.is_empty() {
                    // Empty record needs a placeholder field for BigQuery
                    vec![BqSchemaField::new(
                        "__unknown__".to_string(),
                        "STRING".to_string(),
                        "NULLABLE".to_string(),
                    )]
                } else {
                    self.flatten_schema_map(fields)
                };
                BqSchemaField::record(entry.name.clone(), mode.as_str().to_string(), nested_fields)
            }
            _ => BqSchemaField::new(
                entry.name.clone(),
                entry.bq_type.as_str().to_string(),
                mode.as_str().to_string(),
            ),
        }
    }
}

/// Get the JSON type name for error messages.
fn json_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Build full JSON path for nested fields.
fn json_full_path(base_path: Option<&str>, key: &str) -> String {
    match base_path {
        Some(base) if !base.is_empty() => format!("{}.{}", base, key),
        _ => key.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_simple_schema() {
        let mut generator = SchemaGenerator::default_config();
        let mut schema_map = SchemaMap::new();

        let record = json!({"name": "test", "count": 42, "active": true});
        generator.process_record(&record, &mut schema_map).unwrap();

        let schema = generator.flatten_schema(&schema_map);
        assert_eq!(schema.len(), 3);
    }

    #[test]
    fn test_nested_record() {
        let mut generator = SchemaGenerator::default_config();
        let mut schema_map = SchemaMap::new();

        let record = json!({
            "user": {
                "name": "test",
                "age": 25
            }
        });
        generator.process_record(&record, &mut schema_map).unwrap();

        let schema = generator.flatten_schema(&schema_map);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].field_type, "RECORD");
        assert!(schema[0].fields.is_some());
    }

    #[test]
    fn test_array_type() {
        let mut generator = SchemaGenerator::default_config();
        let mut schema_map = SchemaMap::new();

        let record = json!({"tags": ["a", "b", "c"]});
        generator.process_record(&record, &mut schema_map).unwrap();

        let schema = generator.flatten_schema(&schema_map);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].mode, "REPEATED");
        assert_eq!(schema[0].field_type, "STRING");
    }

    #[test]
    fn test_type_coercion() {
        let mut generator = SchemaGenerator::default_config();
        let mut schema_map = SchemaMap::new();

        // First record has integer
        let record1 = json!({"value": 42});
        generator.process_record(&record1, &mut schema_map).unwrap();

        // Second record has float
        let record2 = json!({"value": 3.5});
        generator.process_record(&record2, &mut schema_map).unwrap();

        let schema = generator.flatten_schema(&schema_map);
        assert_eq!(schema.len(), 1);
        assert_eq!(schema[0].field_type, "FLOAT");
    }

    #[test]
    fn test_sanitize_names() {
        let config = GeneratorConfig {
            sanitize_names: true,
            ..Default::default()
        };
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let record = json!({"field-name": "test", "field.with.dots": 42});
        generator.process_record(&record, &mut schema_map).unwrap();

        let schema = generator.flatten_schema(&schema_map);
        assert_eq!(schema.len(), 2);
        // Check that names are sanitized
        for field in &schema {
            assert!(!field.name.contains('-'));
            assert!(!field.name.contains('.'));
        }
    }
}
