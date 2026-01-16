//! Watch mode for automatic schema regeneration on file changes.
//!
//! This module provides functionality to monitor files for changes and
//! automatically regenerate the schema when files are modified.

use crate::diff::{diff_schemas, DiffOptions};
use crate::input::JsonRecordIterator;
use crate::output::write_schema_json;
use crate::schema::{GeneratorConfig, SchemaGenerator, SchemaMap};
use crate::BqSchemaField;

use notify::RecursiveMode;
use notify_debouncer_mini::{new_debouncer, DebouncedEventKind};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::mpsc::channel;
use std::time::Duration;

/// Configuration for watch mode.
#[derive(Debug, Clone)]
pub struct WatchConfig {
    /// Debounce delay in milliseconds
    pub debounce_ms: u64,
    /// Command to run after schema regeneration
    pub on_change: Option<String>,
    /// Suppress progress messages
    pub quiet: bool,
    /// Ignore invalid JSON lines
    pub ignore_invalid_lines: bool,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            debounce_ms: 100,
            on_change: None,
            quiet: false,
            ignore_invalid_lines: false,
        }
    }
}

/// State for watch mode, maintaining per-file schema caches.
pub struct WatchState {
    /// Per-file schema maps for incremental updates
    file_schemas: HashMap<PathBuf, SchemaMap>,
    /// Current merged schema
    current_schema: Vec<BqSchemaField>,
    /// Generator configuration
    config: GeneratorConfig,
    /// Watch configuration
    watch_config: WatchConfig,
}

impl WatchState {
    /// Create a new watch state with initial file processing.
    pub fn new(
        files: &[PathBuf],
        config: GeneratorConfig,
        watch_config: WatchConfig,
    ) -> crate::Result<Self> {
        let mut state = Self {
            file_schemas: HashMap::new(),
            current_schema: Vec::new(),
            config,
            watch_config,
        };

        // Process all initial files
        for file in files {
            if let Err(e) = state.process_file(file) {
                if !state.watch_config.quiet {
                    eprintln!("Warning: Error processing '{}': {}", file.display(), e);
                }
            }
        }

        // Merge all schemas
        state.rebuild_schema();

        Ok(state)
    }

    /// Process a single file and update its cached schema.
    fn process_file(&mut self, path: &Path) -> crate::Result<()> {
        let file = File::open(path)?;
        let reader = BufReader::new(file);

        let mut generator = SchemaGenerator::new(self.config.clone());
        let mut schema_map = SchemaMap::new();

        let iter = JsonRecordIterator::new(reader, self.watch_config.ignore_invalid_lines);

        for record_result in iter {
            match record_result {
                Ok((_line, record)) => {
                    let _ = generator.process_record(&record, &mut schema_map);
                }
                Err(e) if !self.watch_config.ignore_invalid_lines => {
                    return Err(e);
                }
                Err(_) => continue,
            }
        }

        self.file_schemas.insert(path.to_owned(), schema_map);
        Ok(())
    }

    /// Rebuild the merged schema from all cached file schemas.
    fn rebuild_schema(&mut self) {
        let mut generator = SchemaGenerator::new(self.config.clone());
        let mut merged_schema = SchemaMap::new();

        for file_schema in self.file_schemas.values() {
            for (_key, entry) in file_schema {
                // Convert entry to JSON value and process through generator
                let json_value = entry_to_json(entry);
                let mut temp_map = serde_json::Map::new();
                temp_map.insert(entry.name.clone(), json_value);
                let record = serde_json::Value::Object(temp_map);
                let _ = generator.process_record(&record, &mut merged_schema);
            }
        }

        self.current_schema = generator.flatten_schema(&merged_schema);
    }

    /// Handle a file change event.
    pub fn handle_file_change(&mut self, path: &Path) -> Option<crate::diff::SchemaDiff> {
        let old_schema = self.current_schema.clone();

        // Reprocess the changed file
        if let Err(e) = self.process_file(path) {
            if !self.watch_config.quiet {
                eprintln!("Warning: Error processing '{}': {}", path.display(), e);
            }
            return None;
        }

        // Rebuild merged schema
        self.rebuild_schema();

        // Calculate diff
        let options = DiffOptions::default();
        let diff = diff_schemas(&old_schema, &self.current_schema, &options);

        if diff.has_changes() {
            Some(diff)
        } else {
            None
        }
    }

    /// Handle a file deletion event.
    pub fn handle_file_delete(&mut self, path: &Path) -> Option<crate::diff::SchemaDiff> {
        self.file_schemas.remove(path)?;

        let old_schema = self.current_schema.clone();

        // Rebuild merged schema without the deleted file
        self.rebuild_schema();

        // Calculate diff
        let options = DiffOptions::default();
        let diff = diff_schemas(&old_schema, &self.current_schema, &options);

        if diff.has_changes() {
            Some(diff)
        } else {
            None
        }
    }

    /// Get the current schema.
    pub fn current_schema(&self) -> &[BqSchemaField] {
        &self.current_schema
    }
}

/// Convert a SchemaEntry to a representative JSON value.
fn entry_to_json(entry: &crate::schema::SchemaEntry) -> serde_json::Value {
    use crate::schema::BqType;
    use serde_json::Value;

    match &entry.bq_type {
        BqType::Boolean | BqType::QBoolean => Value::Bool(true),
        BqType::Integer | BqType::QInteger => Value::Number(serde_json::Number::from(0i64)),
        BqType::Float | BqType::QFloat => {
            Value::Number(serde_json::Number::from_f64(0.0).unwrap_or(serde_json::Number::from(0)))
        }
        BqType::String => Value::String(String::new()),
        BqType::Timestamp => Value::String("2024-01-01T00:00:00".to_string()),
        BqType::Date => Value::String("2024-01-01".to_string()),
        BqType::Time => Value::String("00:00:00".to_string()),
        BqType::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (_, field_entry) in fields {
                obj.insert(field_entry.name.clone(), entry_to_json(field_entry));
            }
            if entry.mode == crate::schema::BqMode::Repeated {
                Value::Array(vec![Value::Object(obj)])
            } else {
                Value::Object(obj)
            }
        }
        BqType::Null => Value::Null,
        BqType::EmptyArray => Value::Array(vec![]),
        BqType::EmptyRecord => Value::Object(serde_json::Map::new()),
    }
}

/// Run watch mode.
pub fn run_watch(
    patterns: &[String],
    output_path: Option<&Path>,
    config: GeneratorConfig,
    watch_config: WatchConfig,
) -> crate::Result<()> {
    // Collect initial files
    let files = collect_files_from_patterns(patterns)?;

    if files.is_empty() {
        eprintln!("Error: No files matched the patterns");
        std::process::exit(1);
    }

    // Initialize state with initial file processing
    let mut state = WatchState::new(&files, config, watch_config.clone())?;

    // Write initial schema
    if let Some(path) = output_path {
        write_schema_to_file(path, state.current_schema())?;
        if !watch_config.quiet {
            eprintln!("Initial schema written to {}", path.display());
        }
    }

    // Set up debounced file watcher
    let (tx, rx) = channel();
    let mut debouncer = new_debouncer(
        Duration::from_millis(watch_config.debounce_ms),
        tx,
    ).map_err(|e| crate::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;

    // Get unique directories to watch
    let watch_dirs = get_unique_dirs(&files);

    // Watch directories
    for dir in &watch_dirs {
        debouncer
            .watcher()
            .watch(dir, RecursiveMode::NonRecursive)
            .map_err(|e| crate::Error::Io(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))?;
    }

    // Create a set of file paths we're interested in
    let watched_files: HashSet<PathBuf> = files.iter().cloned().collect();

    println!("[{}] Watching {} files...", format_time(), files.len());

    // Event loop
    for events in rx {
        match events {
            Ok(events) => {
                for event in events {
                    let path = event.path;

                    // Skip if not a file we're watching
                    if !watched_files.contains(&path) && !matches_any_pattern(&path, patterns) {
                        continue;
                    }

                    if event.kind == DebouncedEventKind::Any {
                        // File modified or created
                        if path.exists() && path.is_file() {
                            if !watch_config.quiet {
                                println!("[{}] File changed: {}", format_time(), path.display());
                                println!("[{}] Regenerating schema...", format_time());
                            }

                            if let Some(diff) = state.handle_file_change(&path) {
                                // Print diff
                                if !watch_config.quiet {
                                    println!("[{}] Schema updated:", format_time());
                                    print_diff_summary(&diff);
                                }

                                // Write output
                                if let Some(out_path) = output_path {
                                    if let Err(e) = write_schema_to_file(out_path, state.current_schema()) {
                                        eprintln!("Error writing schema: {}", e);
                                    } else if !watch_config.quiet {
                                        println!("[{}] Wrote {}", format_time(), out_path.display());
                                    }
                                }

                                // Run on-change command
                                if let Some(ref cmd) = watch_config.on_change {
                                    if !watch_config.quiet {
                                        println!("[{}] Running: {}", format_time(), cmd);
                                    }
                                    let _ = std::process::Command::new("sh")
                                        .arg("-c")
                                        .arg(cmd)
                                        .status();
                                }
                            } else if !watch_config.quiet {
                                println!("[{}] No schema changes", format_time());
                            }
                        } else if !path.exists() {
                            // File deleted
                            if !watch_config.quiet {
                                println!("[{}] File deleted: {}", format_time(), path.display());
                                println!("[{}] Regenerating schema...", format_time());
                            }

                            if let Some(diff) = state.handle_file_delete(&path) {
                                if !watch_config.quiet {
                                    println!("[{}] Schema updated:", format_time());
                                    print_diff_summary(&diff);
                                }

                                if let Some(out_path) = output_path {
                                    if let Err(e) = write_schema_to_file(out_path, state.current_schema()) {
                                        eprintln!("Error writing schema: {}", e);
                                    } else if !watch_config.quiet {
                                        println!("[{}] Wrote {}", format_time(), out_path.display());
                                    }
                                }

                                if let Some(ref cmd) = watch_config.on_change {
                                    if !watch_config.quiet {
                                        println!("[{}] Running: {}", format_time(), cmd);
                                    }
                                    let _ = std::process::Command::new("sh")
                                        .arg("-c")
                                        .arg(cmd)
                                        .status();
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                eprintln!("Watch error: {:?}", e);
            }
        }
    }

    Ok(())
}

/// Collect files matching glob patterns.
fn collect_files_from_patterns(patterns: &[String]) -> crate::Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for pattern in patterns {
        match glob::glob(pattern) {
            Ok(paths) => {
                for entry in paths.flatten() {
                    if entry.is_file() {
                        files.push(entry);
                    }
                }
            }
            Err(e) => {
                return Err(crate::Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid glob pattern '{}': {}", pattern, e),
                )));
            }
        }
    }

    Ok(files)
}

/// Get unique parent directories from a list of files.
fn get_unique_dirs(files: &[PathBuf]) -> Vec<PathBuf> {
    let mut dirs: HashSet<PathBuf> = HashSet::new();

    for file in files {
        if let Some(parent) = file.parent() {
            dirs.insert(parent.to_owned());
        }
    }

    dirs.into_iter().collect()
}

/// Check if a path matches any of the patterns.
fn matches_any_pattern(path: &Path, patterns: &[String]) -> bool {
    for pattern in patterns {
        if let Ok(glob_pattern) = glob::Pattern::new(pattern) {
            if glob_pattern.matches_path(path) {
                return true;
            }
        }
    }
    false
}

/// Write schema to a file.
fn write_schema_to_file(path: &Path, schema: &[BqSchemaField]) -> crate::Result<()> {
    let mut file = File::create(path)?;
    write_schema_json(schema, &mut file)?;
    Ok(())
}

/// Format current time for log messages.
fn format_time() -> String {
    use std::time::SystemTime;

    let now = SystemTime::now();
    let duration = now
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = duration.as_secs();

    // Simple formatting: HH:MM:SS
    let hours = (secs / 3600) % 24;
    let minutes = (secs / 60) % 60;
    let seconds = secs % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

/// Print a summary of schema diff.
fn print_diff_summary(diff: &crate::diff::SchemaDiff) {
    use colored::Colorize;

    for change in &diff.changes {
        match change.change_type {
            crate::diff::ChangeType::Added => {
                println!("  {} {}", "+".green(), change.path.green());
                if let Some(ref new_field) = change.new_field {
                    println!(
                        "    {} ({}, {})",
                        "Added:".dimmed(),
                        new_field.field_type,
                        new_field.mode
                    );
                }
            }
            crate::diff::ChangeType::Removed => {
                println!("  {} {}", "-".red(), change.path.red());
            }
            crate::diff::ChangeType::Modified => {
                println!("  {} {}", "~".yellow(), change.path.yellow());
                println!("    {}", change.description.dimmed());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_watch_state_creation() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.json");

        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"name": "test", "value": 42}}"#).unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();
        let files = vec![file_path];

        let state = WatchState::new(&files, config, watch_config).unwrap();

        assert!(!state.current_schema().is_empty());
        assert!(state.current_schema().iter().any(|f| f.name == "name"));
        assert!(state.current_schema().iter().any(|f| f.name == "value"));
    }

    #[test]
    fn test_file_change_detection() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.json");

        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"name": "test"}}"#).unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();
        let files = vec![file_path.clone()];

        let mut state = WatchState::new(&files, config, watch_config).unwrap();

        // Modify the file
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"name": "test", "new_field": true}}"#).unwrap();

        // Handle the change
        let diff = state.handle_file_change(&file_path);

        assert!(diff.is_some());
        let diff = diff.unwrap();
        assert!(diff.has_changes());
    }

    #[test]
    fn test_handle_file_delete_removes_contribution() {
        let dir = tempdir().unwrap();

        // Create two files
        let file1_path = dir.path().join("test1.json");
        let file2_path = dir.path().join("test2.json");

        let mut file1 = File::create(&file1_path).unwrap();
        writeln!(file1, r#"{{"field_a": 1}}"#).unwrap();

        let mut file2 = File::create(&file2_path).unwrap();
        writeln!(file2, r#"{{"field_b": 2}}"#).unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();
        let files = vec![file1_path.clone(), file2_path.clone()];

        let mut state = WatchState::new(&files, config, watch_config).unwrap();

        // Verify both fields exist initially
        assert!(state.current_schema().iter().any(|f| f.name == "field_a"));
        assert!(state.current_schema().iter().any(|f| f.name == "field_b"));

        // Delete file1
        std::fs::remove_file(&file1_path).unwrap();
        let diff = state.handle_file_delete(&file1_path);

        assert!(diff.is_some(), "Delete should produce a diff");
        let diff = diff.unwrap();
        assert!(diff.has_changes());

        // field_a should be removed, field_b should remain
        assert!(!state.current_schema().iter().any(|f| f.name == "field_a"));
        assert!(state.current_schema().iter().any(|f| f.name == "field_b"));
    }

    #[test]
    fn test_handle_file_delete_nonexistent_noop() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.json");

        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"name": "test"}}"#).unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();
        let files = vec![file_path.clone()];

        let mut state = WatchState::new(&files, config, watch_config).unwrap();

        // Try to delete a file that was never tracked
        let unknown_path = dir.path().join("unknown.json");
        let diff = state.handle_file_delete(&unknown_path);

        assert!(diff.is_none(), "Deleting unknown file should return None");
    }

    #[test]
    fn test_rebuild_schema_from_multiple_files() {
        let dir = tempdir().unwrap();

        // Create three files with different fields
        let file1_path = dir.path().join("data1.json");
        let file2_path = dir.path().join("data2.json");
        let file3_path = dir.path().join("data3.json");

        File::create(&file1_path)
            .unwrap()
            .write_all(r#"{"field_a": 1}"#.as_bytes())
            .unwrap();
        File::create(&file2_path)
            .unwrap()
            .write_all(r#"{"field_b": "hello"}"#.as_bytes())
            .unwrap();
        File::create(&file3_path)
            .unwrap()
            .write_all(r#"{"field_c": true}"#.as_bytes())
            .unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();
        let files = vec![file1_path, file2_path, file3_path];

        let state = WatchState::new(&files, config, watch_config).unwrap();

        // All three fields should be merged
        let schema = state.current_schema();
        assert_eq!(schema.len(), 3);
        assert!(schema.iter().any(|f| f.name == "field_a"));
        assert!(schema.iter().any(|f| f.name == "field_b"));
        assert!(schema.iter().any(|f| f.name == "field_c"));
    }

    #[test]
    fn test_collect_files_from_patterns_glob() {
        let dir = tempdir().unwrap();

        // Create multiple JSON files
        for i in 0..5 {
            let path = dir.path().join(format!("data{}.json", i));
            File::create(&path)
                .unwrap()
                .write_all(r#"{"id": 1}"#.as_bytes())
                .unwrap();
        }

        // Also create a non-JSON file
        File::create(dir.path().join("readme.txt"))
            .unwrap()
            .write_all(b"hello")
            .unwrap();

        let pattern = dir.path().join("*.json").to_string_lossy().to_string();
        let files = collect_files_from_patterns(&[pattern]).unwrap();

        assert_eq!(files.len(), 5, "Should match only JSON files");
    }

    #[test]
    fn test_collect_files_from_patterns_invalid_pattern() {
        // Invalid glob pattern with unmatched bracket
        let result = collect_files_from_patterns(&["[invalid".to_string()]);

        assert!(result.is_err(), "Invalid pattern should return error");
    }

    #[test]
    fn test_matches_any_pattern_true() {
        let patterns = vec!["*.json".to_string(), "data/*.csv".to_string()];

        assert!(matches_any_pattern(
            std::path::Path::new("test.json"),
            &patterns
        ));
        assert!(matches_any_pattern(
            std::path::Path::new("data/file.csv"),
            &patterns
        ));
    }

    #[test]
    fn test_matches_any_pattern_false() {
        let patterns = vec!["*.json".to_string()];

        assert!(!matches_any_pattern(
            std::path::Path::new("test.csv"),
            &patterns
        ));
        assert!(!matches_any_pattern(
            std::path::Path::new("data/file.txt"),
            &patterns
        ));
    }

    #[test]
    fn test_get_unique_dirs() {
        let files = vec![
            PathBuf::from("/data/a/file1.json"),
            PathBuf::from("/data/a/file2.json"),
            PathBuf::from("/data/b/file3.json"),
            PathBuf::from("/data/c/file4.json"),
        ];

        let dirs = get_unique_dirs(&files);

        // Should have 3 unique directories
        assert_eq!(dirs.len(), 3);
        assert!(dirs.contains(&PathBuf::from("/data/a")));
        assert!(dirs.contains(&PathBuf::from("/data/b")));
        assert!(dirs.contains(&PathBuf::from("/data/c")));
    }

    #[test]
    fn test_entry_to_json_all_types() {
        use crate::schema::{BqMode, BqType, SchemaEntry, SchemaMap};

        // Boolean
        let entry = SchemaEntry::new("bool".to_string(), BqType::Boolean, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_boolean());

        // QBoolean
        let entry = SchemaEntry::new("qbool".to_string(), BqType::QBoolean, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_boolean());

        // Integer
        let entry = SchemaEntry::new("int".to_string(), BqType::Integer, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_number());

        // QInteger
        let entry = SchemaEntry::new("qint".to_string(), BqType::QInteger, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_number());

        // Float
        let entry = SchemaEntry::new("float".to_string(), BqType::Float, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_number());

        // QFloat
        let entry = SchemaEntry::new("qfloat".to_string(), BqType::QFloat, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_number());

        // String
        let entry = SchemaEntry::new("str".to_string(), BqType::String, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_string());

        // Timestamp
        let entry = SchemaEntry::new("ts".to_string(), BqType::Timestamp, BqMode::Nullable);
        let json = entry_to_json(&entry);
        assert!(json.is_string());
        assert!(json.as_str().unwrap().contains("T"));

        // Date
        let entry = SchemaEntry::new("date".to_string(), BqType::Date, BqMode::Nullable);
        let json = entry_to_json(&entry);
        assert!(json.is_string());
        assert!(json.as_str().unwrap().contains("-"));

        // Time
        let entry = SchemaEntry::new("time".to_string(), BqType::Time, BqMode::Nullable);
        let json = entry_to_json(&entry);
        assert!(json.is_string());
        assert!(json.as_str().unwrap().contains(":"));

        // Null
        let entry = SchemaEntry::new("null".to_string(), BqType::Null, BqMode::Nullable);
        assert!(entry_to_json(&entry).is_null());

        // EmptyArray
        let entry = SchemaEntry::new("arr".to_string(), BqType::EmptyArray, BqMode::Nullable);
        let json = entry_to_json(&entry);
        assert!(json.is_array());
        assert!(json.as_array().unwrap().is_empty());

        // EmptyRecord
        let entry = SchemaEntry::new("rec".to_string(), BqType::EmptyRecord, BqMode::Nullable);
        let json = entry_to_json(&entry);
        assert!(json.is_object());
        assert!(json.as_object().unwrap().is_empty());

        // Record with nested fields
        let mut nested = SchemaMap::new();
        nested.insert(
            "name".to_string(),
            SchemaEntry::new("name".to_string(), BqType::String, BqMode::Nullable),
        );
        let entry = SchemaEntry::new("user".to_string(), BqType::Record(nested), BqMode::Nullable);
        let json = entry_to_json(&entry);
        assert!(json.is_object());
        assert!(json.as_object().unwrap().contains_key("name"));

        // Repeated Record
        let mut nested = SchemaMap::new();
        nested.insert(
            "id".to_string(),
            SchemaEntry::new("id".to_string(), BqType::Integer, BqMode::Nullable),
        );
        let entry = SchemaEntry::new("items".to_string(), BqType::Record(nested), BqMode::Repeated);
        let json = entry_to_json(&entry);
        assert!(json.is_array());
    }

    #[test]
    fn test_watch_config_defaults() {
        let config = WatchConfig::default();

        assert_eq!(config.debounce_ms, 100);
        assert!(config.on_change.is_none());
        assert!(!config.quiet);
        assert!(!config.ignore_invalid_lines);
    }

    #[test]
    fn test_format_time_output_format() {
        let time_str = format_time();

        // Should match HH:MM:SS format
        assert_eq!(time_str.len(), 8);
        assert_eq!(&time_str[2..3], ":");
        assert_eq!(&time_str[5..6], ":");

        // Hours, minutes, seconds should be numeric
        assert!(time_str[0..2].parse::<u32>().is_ok());
        assert!(time_str[3..5].parse::<u32>().is_ok());
        assert!(time_str[6..8].parse::<u32>().is_ok());
    }

    #[test]
    fn test_watch_state_with_ignore_invalid_lines() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.json");

        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"valid": 1}}"#).unwrap();
        writeln!(file, "invalid json line").unwrap();
        writeln!(file, r#"{{"also_valid": 2}}"#).unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig {
            ignore_invalid_lines: true,
            ..Default::default()
        };
        let files = vec![file_path];

        let state = WatchState::new(&files, config, watch_config).unwrap();

        // Should have processed the valid lines
        assert!(!state.current_schema().is_empty());
    }

    #[test]
    fn test_watch_state_type_widening_across_changes() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.json");

        // Start with integer
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"value": 42}}"#).unwrap();

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();
        let files = vec![file_path.clone()];

        let mut state = WatchState::new(&files, config, watch_config).unwrap();

        // Initial type should be INTEGER
        let field = state.current_schema().iter().find(|f| f.name == "value").unwrap();
        assert_eq!(field.field_type, "INTEGER");

        // Update file to have float
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, r#"{{"value": 3.14}}"#).unwrap();

        state.handle_file_change(&file_path);

        // Type should be widened to FLOAT
        let field = state.current_schema().iter().find(|f| f.name == "value").unwrap();
        assert_eq!(field.field_type, "FLOAT");
    }
}
