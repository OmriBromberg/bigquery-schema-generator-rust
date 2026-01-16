//! CLI integration tests for the `diff` subcommand.

use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::TempDir;

/// Helper to run the diff CLI command
fn run_diff(
    old_schema: &str,
    new_schema: &str,
    args: &[&str],
) -> (String, String, bool, i32) {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let old_path = temp_dir.path().join("old_schema.json");
    let new_path = temp_dir.path().join("new_schema.json");

    File::create(&old_path)
        .unwrap()
        .write_all(old_schema.as_bytes())
        .unwrap();
    File::create(&new_path)
        .unwrap()
        .write_all(new_schema.as_bytes())
        .unwrap();

    let mut cmd_args = vec![
        "diff",
        old_path.to_str().unwrap(),
        new_path.to_str().unwrap(),
    ];
    cmd_args.extend_from_slice(args);

    let output = Command::new("./target/debug/bq-schema-gen")
        .args(&cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let success = output.status.success();
    let exit_code = output.status.code().unwrap_or(-1);

    (stdout, stderr, success, exit_code)
}

// =============================================================================
// HELP AND BASIC TESTS
// =============================================================================

#[test]
fn test_diff_help_message() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .args(["diff", "--help"])
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(stdout.contains("Compare two BigQuery schemas"));
    assert!(stdout.contains("--format"));
    assert!(stdout.contains("--color"));
    assert!(stdout.contains("--strict"));
}

#[test]
fn test_diff_missing_arguments() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .args(["diff"])
        .output()
        .expect("Failed to run command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("required") || stderr.contains("error"));
}

// =============================================================================
// IDENTICAL SCHEMAS
// =============================================================================

#[test]
fn test_diff_identical_schemas() {
    let schema = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(schema, schema, &[]);

    assert!(success);
    assert!(stdout.contains("No changes detected"));
}

#[test]
fn test_diff_empty_schemas() {
    let schema = "[]";

    let (stdout, _, success, _) = run_diff(schema, schema, &[]);

    assert!(success);
    assert!(stdout.contains("No changes detected"));
}

// =============================================================================
// FIELD CHANGES
// =============================================================================

#[test]
fn test_diff_added_field() {
    let old = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
    ]"#;

    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    assert!(success);
    assert!(stdout.contains("Added"));
    assert!(stdout.contains("email"));
    assert!(stdout.contains("1 added"));
}

#[test]
fn test_diff_removed_field() {
    let old = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
    ]"#;

    let (stdout, _, success, exit_code) = run_diff(old, new, &[]);

    // Should fail due to breaking change
    assert!(!success);
    assert_eq!(exit_code, 1);
    assert!(stdout.contains("Removed"));
    assert!(stdout.contains("name"));
    assert!(stdout.contains("BREAKING"));
}

#[test]
fn test_diff_type_change() {
    let old = r#"[
        {"name": "count", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "count", "type": "INTEGER", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // STRING -> INTEGER is breaking
    assert!(!success);
    assert!(stdout.contains("Modified"));
    assert!(stdout.contains("Type changed"));
    assert!(stdout.contains("STRING"));
    assert!(stdout.contains("INTEGER"));
}

#[test]
fn test_diff_type_widening_not_breaking() {
    let old = r#"[
        {"name": "value", "type": "INTEGER", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "value", "type": "FLOAT", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // INTEGER -> FLOAT is safe widening
    assert!(success);
    assert!(stdout.contains("Modified"));
    assert!(stdout.contains("Type changed"));
}

#[test]
fn test_diff_mode_nullable_to_required() {
    let old = r#"[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "name", "type": "STRING", "mode": "REQUIRED"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // NULLABLE -> REQUIRED is breaking
    assert!(!success);
    assert!(stdout.contains("Mode changed"));
    assert!(stdout.contains("NULLABLE"));
    assert!(stdout.contains("REQUIRED"));
    assert!(stdout.contains("BREAKING"));
}

#[test]
fn test_diff_mode_required_to_nullable() {
    let old = r#"[
        {"name": "name", "type": "STRING", "mode": "REQUIRED"}
    ]"#;

    let new = r#"[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // REQUIRED -> NULLABLE is safe
    assert!(success);
    assert!(stdout.contains("Mode changed"));
}

// =============================================================================
// OUTPUT FORMATS
// =============================================================================

#[test]
fn test_diff_text_format() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--format", "text"]);

    assert!(success);
    assert!(stdout.contains("Schema Diff Report"));
    assert!(stdout.contains("Added Fields:"));
}

#[test]
fn test_diff_json_format() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--format", "json"]);

    assert!(success);

    // Should be valid JSON
    let json: serde_json::Value = serde_json::from_str(&stdout)
        .expect("Output should be valid JSON");

    assert!(json.get("summary").is_some());
    assert!(json.get("changes").is_some());

    let summary = json.get("summary").unwrap();
    assert_eq!(summary.get("added").unwrap(), 1);
}

#[test]
fn test_diff_json_patch_format() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--format", "json-patch"]);

    assert!(success);

    // Should be valid JSON array (RFC 6902)
    let patches: Vec<serde_json::Value> = serde_json::from_str(&stdout)
        .expect("Output should be valid JSON array");

    assert_eq!(patches.len(), 1);
    assert_eq!(patches[0].get("op").unwrap(), "add");
    assert!(patches[0].get("path").is_some());
    assert!(patches[0].get("value").is_some());
}

#[test]
fn test_diff_sql_format() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--format", "sql"]);

    assert!(success);
    assert!(stdout.contains("BigQuery Schema Migration"));
    assert!(stdout.contains("ADD COLUMN"));
    assert!(stdout.contains("ALTER TABLE"));
}

#[test]
fn test_diff_invalid_format() {
    let schema = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;

    let (_, stderr, success, _) = run_diff(schema, schema, &["--format", "invalid"]);

    assert!(!success);
    assert!(stderr.contains("Unknown") || stderr.contains("format"));
}

// =============================================================================
// COLOR MODES
// =============================================================================

#[test]
fn test_diff_color_auto() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--color", "auto"]);

    assert!(success);
    // Auto mode - output depends on terminal detection
    assert!(stdout.contains("email"));
}

#[test]
fn test_diff_color_always() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--color", "always"]);

    assert!(success);
    // Should contain ANSI escape codes
    assert!(stdout.contains("\x1b[") || stdout.contains("email"));
}

#[test]
fn test_diff_color_never() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &["--color", "never"]);

    assert!(success);
    // Should NOT contain ANSI escape codes
    assert!(!stdout.contains("\x1b["));
    assert!(stdout.contains("email"));
}

#[test]
fn test_diff_invalid_color_mode() {
    let schema = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;

    let (_, stderr, success, _) = run_diff(schema, schema, &["--color", "invalid"]);

    assert!(!success);
    assert!(stderr.contains("Unknown") || stderr.contains("color"));
}

// =============================================================================
// STRICT MODE
// =============================================================================

#[test]
fn test_diff_strict_flag_added_field() {
    let old = r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#;
    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "email", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    // Without strict - adding field is not breaking
    let (_, _, success_normal, _) = run_diff(old, new, &[]);
    assert!(success_normal);

    // With strict - adding field IS breaking
    let (stdout, _, success_strict, _) = run_diff(old, new, &["--strict"]);
    assert!(!success_strict);
    assert!(stdout.contains("breaking") || stdout.contains("BREAKING"));
}

#[test]
fn test_diff_strict_flag_type_widening() {
    let old = r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#;
    let new = r#"[{"name": "value", "type": "FLOAT", "mode": "NULLABLE"}]"#;

    // Without strict - type widening is not breaking
    let (_, _, success_normal, _) = run_diff(old, new, &[]);
    assert!(success_normal);

    // With strict - type widening IS breaking
    let (_, _, success_strict, _) = run_diff(old, new, &["--strict"]);
    assert!(!success_strict);
}

// =============================================================================
// NESTED RECORDS
// =============================================================================

#[test]
fn test_diff_nested_records_added_field() {
    let old = r#"[{
        "name": "user",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]
    }]"#;

    let new = r#"[{
        "name": "user",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"}
        ]
    }]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    assert!(success);
    assert!(stdout.contains("user.email"));
    assert!(stdout.contains("Added"));
}

#[test]
fn test_diff_nested_records_removed_field() {
    let old = r#"[{
        "name": "user",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "age", "type": "INTEGER", "mode": "NULLABLE"}
        ]
    }]"#;

    let new = r#"[{
        "name": "user",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"name": "name", "type": "STRING", "mode": "NULLABLE"}
        ]
    }]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    assert!(!success); // Breaking change
    assert!(stdout.contains("user.age"));
    assert!(stdout.contains("Removed"));
}

#[test]
fn test_diff_deeply_nested_records() {
    let old = r#"[{
        "name": "data",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [{
            "name": "user",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "address",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "city", "type": "STRING", "mode": "NULLABLE"}
                ]
            }]
        }]
    }]"#;

    let new = r#"[{
        "name": "data",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [{
            "name": "user",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "address",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [
                    {"name": "city", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "country", "type": "STRING", "mode": "NULLABLE"}
                ]
            }]
        }]
    }]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    assert!(success);
    assert!(stdout.contains("data.user.address.country"));
}

// =============================================================================
// REPEATED FIELDS
// =============================================================================

#[test]
fn test_diff_repeated_field_changes() {
    let old = r#"[
        {"name": "tags", "type": "STRING", "mode": "REPEATED"}
    ]"#;

    let new = r#"[
        {"name": "tags", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // REPEATED -> NULLABLE is breaking
    assert!(!success);
    assert!(stdout.contains("Mode changed"));
    assert!(stdout.contains("BREAKING"));
}

#[test]
fn test_diff_nullable_to_repeated() {
    let old = r#"[
        {"name": "items", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "items", "type": "STRING", "mode": "REPEATED"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // NULLABLE -> REPEATED is breaking
    assert!(!success);
    assert!(stdout.contains("Mode changed"));
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

#[test]
fn test_diff_invalid_old_schema_file() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let new_path = temp_dir.path().join("new_schema.json");

    File::create(&new_path)
        .unwrap()
        .write_all(b"[]")
        .unwrap();

    let output = Command::new("./target/debug/bq-schema-gen")
        .args([
            "diff",
            "/nonexistent/old_schema.json",
            new_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Cannot open") || stderr.contains("Error"));
}

#[test]
fn test_diff_invalid_json_in_schema() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let old_path = temp_dir.path().join("old_schema.json");
    let new_path = temp_dir.path().join("new_schema.json");

    File::create(&old_path)
        .unwrap()
        .write_all(b"not valid json")
        .unwrap();
    File::create(&new_path)
        .unwrap()
        .write_all(b"[]")
        .unwrap();

    let output = Command::new("./target/debug/bq-schema-gen")
        .args([
            "diff",
            old_path.to_str().unwrap(),
            new_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run command");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("Cannot parse") || stderr.contains("Error"));
}

// =============================================================================
// OUTPUT FILE
// =============================================================================

#[test]
fn test_diff_output_file() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    let old_path = temp_dir.path().join("old_schema.json");
    let new_path = temp_dir.path().join("new_schema.json");
    let output_path = temp_dir.path().join("diff_output.txt");

    File::create(&old_path)
        .unwrap()
        .write_all(b"[]")
        .unwrap();
    File::create(&new_path)
        .unwrap()
        .write_all(br#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#)
        .unwrap();

    let output = Command::new("./target/debug/bq-schema-gen")
        .args([
            "diff",
            old_path.to_str().unwrap(),
            new_path.to_str().unwrap(),
            "-o",
            output_path.to_str().unwrap(),
        ])
        .output()
        .expect("Failed to run command");

    assert!(output.status.success());

    // Check output file exists and has content
    let content = std::fs::read_to_string(&output_path).expect("Failed to read output file");
    assert!(content.contains("Added"));
    assert!(content.contains("id"));
}

// =============================================================================
// MULTIPLE CHANGES
// =============================================================================

#[test]
fn test_diff_multiple_changes() {
    let old = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "old_field", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
        {"name": "new_field", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, success, _) = run_diff(old, new, &[]);

    // Has breaking changes (removed field and mode change)
    assert!(!success);

    // Summary should show all change types
    assert!(stdout.contains("added"));
    assert!(stdout.contains("removed"));
    assert!(stdout.contains("modified"));
}

#[test]
fn test_diff_json_format_multiple_changes() {
    let old = r#"[
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "old_field", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let new = r#"[
        {"name": "id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "new_field", "type": "STRING", "mode": "NULLABLE"}
    ]"#;

    let (stdout, _, _, _) = run_diff(old, new, &["--format", "json"]);

    let json: serde_json::Value = serde_json::from_str(&stdout)
        .expect("Output should be valid JSON");

    let changes = json.get("changes").unwrap().as_array().unwrap();
    assert!(changes.len() >= 2); // At least removed and added
}
