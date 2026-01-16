//! CLI integration tests for BigQuery Schema Generator

use std::io::Write;
use std::process::{Command, Stdio};

/// Helper to run the CLI with JSON input
fn run_cli_json(input: &str, args: &[&str]) -> (String, String, bool) {
    let mut cmd = Command::new("./target/debug/bq-schema-gen")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    {
        let stdin = cmd.stdin.as_mut().expect("Failed to open stdin");
        stdin
            .write_all(input.as_bytes())
            .expect("Failed to write to stdin");
    }

    let output = cmd.wait_with_output().expect("Failed to read output");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let success = output.status.success();

    (stdout, stderr, success)
}

/// Parse JSON output from CLI
fn parse_schema(output: &str) -> serde_json::Value {
    serde_json::from_str(output).expect("Failed to parse JSON output")
}

// =============================================================================
// BASIC CLI TESTS
// =============================================================================

#[test]
fn test_cli_basic_json() {
    let input = r#"{"name": "test", "value": 42}"#;
    let (stdout, stderr, success) = run_cli_json(input, &[]);

    assert!(success);
    assert!(stderr.contains("Processed"));

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

#[test]
fn test_cli_multiple_records() {
    let input = r#"{"a": 1}
{"b": 2}
{"c": 3}"#;
    let (stdout, _, success) = run_cli_json(input, &[]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 3);
}

// =============================================================================
// CLI FLAG TESTS
// =============================================================================

#[test]
fn test_cli_keep_nulls_flag() {
    let input = r#"{"s": null, "a": [], "m": {}}"#;

    // Without --keep-nulls
    let (stdout, _, _) = run_cli_json(input, &[]);
    let schema = parse_schema(&stdout);
    assert!(schema.as_array().unwrap().is_empty());

    // With --keep-nulls
    let (stdout, _, _) = run_cli_json(input, &["--keep-nulls"]);
    let schema = parse_schema(&stdout);
    assert!(!schema.as_array().unwrap().is_empty());
}

#[test]
fn test_cli_quoted_values_are_strings_flag() {
    let input = r#"{"qi": "123", "qf": "3.14", "qb": "true"}"#;

    // Without flag - should infer types
    let (stdout, _, _) = run_cli_json(input, &[]);
    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert!(arr
        .iter()
        .any(|f| f["name"] == "qi" && f["type"] == "INTEGER"));

    // With flag - should be strings
    let (stdout, _, _) = run_cli_json(input, &["--quoted-values-are-strings"]);
    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert!(arr.iter().all(|f| f["type"] == "STRING"));
}

#[test]
fn test_cli_preserve_input_sort_order() {
    let input = r#"{"z": 1, "a": 2, "m": 3}"#;

    // Without flag - alphabetical
    let (stdout, _, _) = run_cli_json(input, &[]);
    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr[0]["name"], "a");

    // With flag - preserve order
    let (stdout, _, _) = run_cli_json(input, &["--preserve-input-sort-order"]);
    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr[0]["name"], "z");
}

#[test]
fn test_cli_sanitize_names() {
    let input = r#"{"field-name": "test", "field.dots": 42}"#;

    let (stdout, _, success) = run_cli_json(input, &["--sanitize-names"]);
    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    for field in arr {
        let name = field["name"].as_str().unwrap();
        assert!(!name.contains('-'));
        assert!(!name.contains('.'));
    }
}

#[test]
fn test_cli_ignore_invalid_lines() {
    let input = r#"{"valid": 1}
not valid json
{"also_valid": 2}"#;

    // Without flag - should fail
    let (_, _, success) = run_cli_json(input, &[]);
    assert!(!success);

    // With flag - should succeed with warning
    let (stdout, stderr, success) = run_cli_json(input, &["--ignore-invalid-lines"]);
    assert!(success);
    assert!(stderr.contains("Warning") || stderr.contains("Skipping"));

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

// =============================================================================
// CLI CSV TESTS
// =============================================================================

#[test]
fn test_cli_csv_format() {
    let input = "name,value\ntest,42\nfoo,123";
    let (stdout, _, success) = run_cli_json(input, &["--input-format", "csv"]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

#[test]
fn test_cli_csv_infer_mode() {
    let input = "a,b\n1,hello\n2,world";
    let (stdout, _, success) = run_cli_json(input, &["--input-format", "csv", "--infer-mode"]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    // All fields should be REQUIRED since they're all filled
    assert!(arr.iter().all(|f| f["mode"] == "REQUIRED"));
}

// =============================================================================
// CLI UNDERSCORE FLAG COMPATIBILITY
// =============================================================================

#[test]
fn test_cli_underscore_flag_compatibility() {
    let input = r#"{"test": 1}"#;

    // Test that underscore versions work
    let (_stdout, _, success) = run_cli_json(input, &["--input_format", "json"]);
    assert!(success);

    let (_stdout2, _, success2) = run_cli_json(input, &["--keep_nulls"]);
    assert!(success2);

    let (_stdout3, _, success3) = run_cli_json(input, &["--quoted_values_are_strings"]);
    assert!(success3);
}

// =============================================================================
// CLI ERROR HANDLING
// =============================================================================

#[test]
fn test_cli_invalid_json_input() {
    let input = "this is not json";
    let (_, stderr, success) = run_cli_json(input, &[]);

    assert!(!success);
    assert!(stderr.contains("Error") || stderr.contains("error"));
}

#[test]
fn test_cli_array_not_object() {
    let input = "[1, 2, 3]";
    let (_, stderr, success) = run_cli_json(input, &[]);

    assert!(!success);
    assert!(stderr.contains("Object") || stderr.contains("Problem"));
}

#[test]
fn test_cli_empty_input() {
    let input = "";
    let (stdout, _, success) = run_cli_json(input, &[]);

    assert!(success);

    let schema = parse_schema(&stdout);
    assert!(schema.as_array().unwrap().is_empty());
}

// =============================================================================
// CLI HELP AND VERSION
// =============================================================================

#[test]
fn test_cli_help() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .arg("--help")
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Generate BigQuery schema"));
    assert!(stdout.contains("--input-format"));
    assert!(stdout.contains("--keep-nulls"));
}

#[test]
fn test_cli_version() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .arg("--version")
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("bq-schema-gen") || stdout.contains("0.1"));
}

// =============================================================================
// CLI COMPLEX SCENARIOS
// =============================================================================

#[test]
fn test_cli_nested_records() {
    let input = r#"{"user": {"name": "test", "profile": {"age": 25, "city": "NYC"}}}"#;
    let (stdout, _, success) = run_cli_json(input, &[]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    let user = arr.iter().find(|f| f["name"] == "user").unwrap();
    assert_eq!(user["type"], "RECORD");

    let user_fields = user["fields"].as_array().unwrap();
    let profile = user_fields.iter().find(|f| f["name"] == "profile").unwrap();
    assert_eq!(profile["type"], "RECORD");
}

#[test]
fn test_cli_array_of_records() {
    let input = r#"{"items": [{"name": "a"}, {"name": "b"}]}"#;
    let (stdout, _, success) = run_cli_json(input, &[]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    let items = arr.iter().find(|f| f["name"] == "items").unwrap();
    assert_eq!(items["type"], "RECORD");
    assert_eq!(items["mode"], "REPEATED");
}

#[test]
fn test_cli_type_evolution() {
    let input = r#"{"value": 42}
{"value": 3.14}"#;
    let (stdout, _, success) = run_cli_json(input, &[]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    let value = arr.iter().find(|f| f["name"] == "value").unwrap();
    assert_eq!(value["type"], "FLOAT");
}

#[test]
fn test_cli_large_number_of_fields() {
    let mut input = String::from("{");
    for i in 0..100 {
        if i > 0 {
            input.push_str(", ");
        }
        input.push_str(&format!("\"field_{}\": {}", i, i));
    }
    input.push('}');

    let (stdout, _, success) = run_cli_json(&input, &[]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 100);
}

#[test]
fn test_cli_large_number_of_records() {
    let mut input = String::new();
    for i in 0..1000 {
        input.push_str(&format!("{{\"id\": {}, \"value\": {}}}\n", i, i * 2));
    }

    let (stdout, stderr, success) = run_cli_json(&input, &[]);

    assert!(success);
    assert!(stderr.contains("1000"));

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

// =============================================================================
// CLI EXISTING SCHEMA TESTS
// =============================================================================

#[test]
fn test_cli_existing_schema_path() {
    use tempfile::NamedTempFile;

    // Create a temporary file with an existing schema
    let mut schema_file = NamedTempFile::new().expect("Failed to create temp file");
    let existing_schema = r#"[
        {"name": "existing_field", "type": "STRING", "mode": "NULLABLE"},
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
    ]"#;
    schema_file
        .write_all(existing_schema.as_bytes())
        .expect("Failed to write schema");

    // Input JSON with a new field
    let input = r#"{"new_field": 42, "id": 123}"#;

    let (stdout, stderr, success) = run_cli_json(
        input,
        &[
            "--existing_schema_path",
            schema_file.path().to_str().unwrap(),
        ],
    );

    assert!(success, "CLI should succeed: stderr={}", stderr);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    // Should have 3 fields: existing_field, id, new_field
    assert_eq!(arr.len(), 3, "Expected 3 fields, got: {:?}", arr);

    // Check existing field is preserved
    assert!(arr
        .iter()
        .any(|f| f["name"] == "existing_field" && f["type"] == "STRING"));

    // Check id is preserved with INTEGER type
    assert!(arr
        .iter()
        .any(|f| f["name"] == "id" && f["type"] == "INTEGER"));

    // Check new field is added
    assert!(arr
        .iter()
        .any(|f| f["name"] == "new_field" && f["type"] == "INTEGER"));
}

#[test]
fn test_cli_existing_schema_path_invalid_file() {
    let input = r#"{"test": 1}"#;

    let (_, stderr, success) =
        run_cli_json(input, &["--existing_schema_path", "/nonexistent/file.json"]);

    assert!(!success);
    assert!(stderr.contains("Error") || stderr.contains("Cannot"));
}

#[test]
fn test_cli_existing_schema_merges_types() {
    use tempfile::NamedTempFile;

    // Create a temporary file with INTEGER type
    let mut schema_file = NamedTempFile::new().expect("Failed to create temp file");
    let existing_schema = r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#;
    schema_file
        .write_all(existing_schema.as_bytes())
        .expect("Failed to write schema");

    // Input JSON with FLOAT value - should upgrade to FLOAT
    let input = r#"{"value": 3.14}"#;

    let (stdout, _, success) = run_cli_json(
        input,
        &[
            "--existing_schema_path",
            schema_file.path().to_str().unwrap(),
        ],
    );

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    // Type should be upgraded to FLOAT
    assert!(arr
        .iter()
        .any(|f| f["name"] == "value" && f["type"] == "FLOAT"));
}

// =============================================================================
// CLI OUTPUT FORMAT TESTS
// =============================================================================

#[test]
fn test_cli_output_format_json() {
    let input = r#"{"name": "test", "value": 42}"#;
    let (stdout, _, success) = run_cli_json(input, &["--output-format", "json"]);

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert_eq!(arr.len(), 2);
}

#[test]
fn test_cli_output_format_ddl() {
    let input = r#"{"name": "test", "count": 42, "active": true}"#;
    let (stdout, _, success) = run_cli_json(
        input,
        &["--output-format", "ddl", "--table-name", "myproject.users"],
    );

    assert!(success);
    assert!(stdout.contains("CREATE TABLE `myproject.users`"));
    assert!(stdout.contains("name STRING"));
    assert!(stdout.contains("count INT64"));
    assert!(stdout.contains("active BOOL"));
}

#[test]
fn test_cli_output_format_ddl_with_nested() {
    let input = r#"{"user": {"name": "test"}, "tags": ["a", "b"]}"#;
    let (stdout, _, success) = run_cli_json(
        input,
        &["--output-format", "ddl", "--table-name", "db.table"],
    );

    assert!(success);
    assert!(stdout.contains("STRUCT<"));
    assert!(stdout.contains("ARRAY<STRING>"));
}

#[test]
fn test_cli_output_format_debug_map() {
    let input = r#"{"name": "test", "value": 42}"#;
    let (stdout, _, success) = run_cli_json(input, &["--output-format", "debug-map"]);

    assert!(success);

    let debug: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let obj = debug.as_object().unwrap();

    // Should have both fields
    assert!(obj.contains_key("name"));
    assert!(obj.contains_key("value"));

    // Check structure of debug output
    let name_entry = obj.get("name").unwrap();
    assert!(name_entry.get("status").is_some());
    assert!(name_entry.get("filled").is_some());
    assert!(name_entry.get("bq_type").is_some());
    assert!(name_entry.get("mode").is_some());
}

#[test]
fn test_cli_output_format_json_schema() {
    let input = r#"{"name": "test", "count": 42, "active": true}"#;
    let (stdout, _, success) = run_cli_json(input, &["--output-format", "json-schema"]);

    assert!(success);

    let json_schema: serde_json::Value = serde_json::from_str(&stdout).unwrap();

    // Check JSON Schema structure
    assert_eq!(
        json_schema["$schema"],
        "http://json-schema.org/draft-07/schema#"
    );
    assert_eq!(json_schema["type"], "object");
    assert!(json_schema["properties"].is_object());

    let props = json_schema["properties"].as_object().unwrap();
    assert!(props.contains_key("name"));
    assert!(props.contains_key("count"));
    assert!(props.contains_key("active"));

    // Check types
    assert_eq!(props["name"]["type"], "string");
    assert_eq!(props["count"]["type"], "integer");
    assert_eq!(props["active"]["type"], "boolean");
}

#[test]
fn test_cli_output_format_json_schema_with_required() {
    let input = r#"{"id": 1, "name": "test"}
{"id": 2, "name": "foo"}"#;
    let (stdout, _, success) =
        run_cli_json(input, &["--output-format", "json-schema", "--infer-mode"]);

    assert!(success);

    let json_schema: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    assert!(json_schema["properties"].is_object());
}

#[test]
fn test_cli_output_format_invalid() {
    let input = r#"{"name": "test"}"#;
    let (_, stderr, success) = run_cli_json(input, &["--output-format", "invalid"]);

    assert!(!success);
    assert!(stderr.contains("Unknown output format"));
}

#[test]
fn test_cli_output_format_underscore_alias() {
    let input = r#"{"name": "test"}"#;
    let (stdout, _, success) = run_cli_json(input, &["--output_format", "ddl"]);

    assert!(success);
    assert!(stdout.contains("CREATE TABLE"));
}
