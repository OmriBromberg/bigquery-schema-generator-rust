//! CLI integration tests for the validate subcommand.
//!
//! These tests verify the validation functionality through the command-line interface.

use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::{tempdir, NamedTempFile};

/// Helper to run the CLI validate subcommand
fn run_validate(data_file: &str, schema_file: &str, args: &[&str]) -> (String, String, i32) {
    let mut cmd_args = vec!["validate", data_file, "--schema", schema_file];
    cmd_args.extend_from_slice(args);

    let output = Command::new("./target/debug/bq-schema-gen")
        .args(&cmd_args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let exit_code = output.status.code().unwrap_or(-1);

    (stdout, stderr, exit_code)
}

/// Create a temporary data file with given content
fn create_data_file(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(content.as_bytes())
        .expect("Failed to write data");
    file
}

/// Create a temporary schema file with given content
fn create_schema_file(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("Failed to create temp file");
    file.write_all(content.as_bytes())
        .expect("Failed to write schema");
    file
}

// =============================================================================
// HELP AND BASIC CLI TESTS
// =============================================================================

#[test]
fn test_validate_help_message() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .args(["validate", "--help"])
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Validate data against"));
    assert!(stdout.contains("--schema"));
    assert!(stdout.contains("--allow-unknown"));
    assert!(stdout.contains("--strict-types"));
    assert!(stdout.contains("--max-errors"));
    assert!(stdout.contains("--format"));
    assert!(stdout.contains("--quiet"));
}

#[test]
fn test_validate_missing_schema_exits_error() {
    let data = create_data_file(r#"{"name": "test"}"#);

    let output = Command::new("./target/debug/bq-schema-gen")
        .args(["validate", data.path().to_str().unwrap()])
        .output()
        .expect("Failed to run command");

    // Should fail because --schema is required
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("required") || stderr.contains("--schema"),
        "Should mention missing schema argument"
    );
}

#[test]
fn test_validate_nonexistent_schema_file() {
    let data = create_data_file(r#"{"name": "test"}"#);

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        "/nonexistent/schema.json",
        &[],
    );

    assert_ne!(exit_code, 0);
    assert!(
        stderr.contains("Cannot open") || stderr.contains("Error"),
        "Should report error opening schema file"
    );
}

// =============================================================================
// VALID DATA TESTS
// =============================================================================

#[test]
fn test_validate_valid_data_exits_zero() {
    let schema = create_schema_file(
        r#"[
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "value", "type": "INTEGER", "mode": "NULLABLE"}
    ]"#,
    );

    let data = create_data_file(r#"{"name": "test", "value": 42}"#);

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(
        exit_code, 0,
        "Valid data should exit with code 0. stderr: {}",
        stderr
    );
    assert!(
        stderr.contains("passed") || stderr.contains("Validation"),
        "Should indicate success"
    );
}

#[test]
fn test_validate_multiple_valid_records() {
    let schema = create_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    let data = create_data_file(
        r#"{"id": 1}
{"id": 2}
{"id": 3}"#,
    );

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(exit_code, 0);
    assert!(stderr.contains("3 lines") || stderr.contains("passed"));
}

// =============================================================================
// INVALID DATA TESTS
// =============================================================================

#[test]
fn test_validate_invalid_data_exits_one() {
    let schema =
        create_schema_file(r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    let data = create_data_file(r#"{"value": "not an integer"}"#);

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--strict-types"],
    );

    assert_eq!(exit_code, 1, "Invalid data should exit with code 1");
    assert!(stderr.contains("failed") || stderr.contains("error"));
}

#[test]
fn test_validate_missing_required_field() {
    let schema = create_schema_file(
        r#"[
        {"name": "required_field", "type": "STRING", "mode": "REQUIRED"},
        {"name": "optional_field", "type": "INTEGER", "mode": "NULLABLE"}
    ]"#,
    );

    let data = create_data_file(r#"{"optional_field": 42}"#);

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(exit_code, 1);
    assert!(
        stderr.contains("REQUIRED") || stderr.contains("missing"),
        "Should report missing required field"
    );
}

#[test]
fn test_validate_null_required_field() {
    let schema =
        create_schema_file(r#"[{"name": "required_field", "type": "STRING", "mode": "REQUIRED"}]"#);

    let data = create_data_file(r#"{"required_field": null}"#);

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(exit_code, 1, "Null in required field should fail");
    assert!(stderr.contains("REQUIRED") || stderr.contains("missing"));
}

// =============================================================================
// OUTPUT FORMAT TESTS
// =============================================================================

#[test]
fn test_validate_text_output_format() {
    let schema =
        create_schema_file(r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    let data = create_data_file(r#"{"value": "not_int"}"#);

    let (_, stderr, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--format", "text", "--strict-types"],
    );

    assert_eq!(exit_code, 1);
    // Text format should have human-readable output
    assert!(stderr.contains("Line") || stderr.contains("error"));
}

#[test]
fn test_validate_json_output_format() {
    let schema =
        create_schema_file(r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    let data = create_data_file(r#"{"value": "not_int"}"#);

    let (stdout, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--format", "json", "--strict-types"],
    );

    assert_eq!(exit_code, 1);

    // JSON output should be valid JSON
    let json: serde_json::Value =
        serde_json::from_str(&stdout).expect("JSON format should produce valid JSON");

    assert!(json.get("valid").is_some());
    assert!(json.get("error_count").is_some());
    assert!(json.get("errors").is_some());
    assert_eq!(json["valid"], false);
}

#[test]
fn test_validate_json_output_structure() {
    let schema = create_schema_file(r#"[{"name": "id", "type": "INTEGER", "mode": "REQUIRED"}]"#);

    let data = create_data_file(r#"{"wrong_field": 42}"#);

    let (stdout, _, _) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--format", "json"],
    );

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();

    // Check structure
    assert!(json["errors"].is_array());
    let errors = json["errors"].as_array().unwrap();
    assert!(!errors.is_empty());

    // Check error structure
    let error = &errors[0];
    assert!(error.get("line").is_some());
    assert!(error.get("path").is_some());
    assert!(error.get("error_type").is_some());
    assert!(error.get("message").is_some());
}

// =============================================================================
// QUIET MODE TESTS
// =============================================================================

#[test]
fn test_validate_quiet_mode_exit_code_only() {
    let schema =
        create_schema_file(r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    // Valid data
    let valid_data = create_data_file(r#"{"value": 42}"#);
    let (stdout, stderr, exit_code) = run_validate(
        valid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["-q"],
    );
    assert_eq!(exit_code, 0);
    assert!(stdout.is_empty(), "Quiet mode should suppress stdout");
    assert!(stderr.is_empty(), "Quiet mode should suppress stderr");

    // Invalid data
    let invalid_data = create_data_file(r#"{"value": "not_int"}"#);
    let (stdout2, stderr2, exit_code2) = run_validate(
        invalid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--quiet", "--strict-types"],
    );
    assert_eq!(exit_code2, 1);
    assert!(stdout2.is_empty(), "Quiet mode should suppress stdout");
    assert!(stderr2.is_empty(), "Quiet mode should suppress stderr");
}

// =============================================================================
// FLAG TESTS
// =============================================================================

#[test]
fn test_validate_allow_unknown_flag() {
    let schema = create_schema_file(r#"[{"name": "known", "type": "STRING", "mode": "NULLABLE"}]"#);

    let data = create_data_file(r#"{"known": "test", "unknown": 123}"#);

    // Without --allow-unknown: should fail
    let (_, stderr1, exit_code1) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(exit_code1, 1, "Unknown field should fail by default");
    assert!(stderr1.contains("unknown") || stderr1.contains("Unknown"));

    // With --allow-unknown: should pass with warning
    let (_, stderr2, exit_code2) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--allow-unknown"],
    );
    assert_eq!(
        exit_code2, 0,
        "Unknown field should pass with --allow-unknown"
    );
    assert!(stderr2.contains("warning") || stderr2.contains("passed"));
}

#[test]
fn test_validate_strict_types_flag() {
    let schema =
        create_schema_file(r#"[{"name": "value", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    let data = create_data_file(r#"{"value": "123"}"#);

    // Without --strict-types: "123" should be accepted as INTEGER (lenient)
    let (_, _, exit_code1) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(
        exit_code1, 0,
        "String '123' should be accepted for INTEGER in lenient mode"
    );

    // With --strict-types: "123" should be rejected
    let (_, stderr2, exit_code2) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--strict-types"],
    );
    assert_eq!(
        exit_code2, 1,
        "String '123' should be rejected in strict mode"
    );
    assert!(stderr2.contains("INTEGER") || stderr2.contains("type"));
}

#[test]
fn test_validate_max_errors_limit() {
    let schema = create_schema_file(
        r#"[
        {"name": "a", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "b", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "c", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "d", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "e", "type": "INTEGER", "mode": "NULLABLE"}
    ]"#,
    );

    // Data with 5 type errors
    let data = create_data_file(r#"{"a": "x", "b": "y", "c": "z", "d": "w", "e": "v"}"#);

    // Limit to 2 errors
    let (stdout, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--max-errors", "2", "--format", "json", "--strict-types"],
    );

    assert_eq!(exit_code, 1);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let error_count = json["error_count"].as_u64().unwrap();
    assert!(error_count <= 2, "Should stop at max_errors limit");
}

// =============================================================================
// NESTED RECORD TESTS
// =============================================================================

#[test]
fn test_validate_nested_record_errors() {
    let schema = create_schema_file(
        r#"[{
        "name": "user",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [{
            "name": "address",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "city",
                "type": "STRING",
                "mode": "REQUIRED"
            }]
        }]
    }]"#,
    );

    // Missing required nested field
    let data = create_data_file(r#"{"user": {"address": {}}}"#);

    let (stdout, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--format", "json"],
    );

    assert_eq!(exit_code, 1);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let errors = json["errors"].as_array().unwrap();
    assert!(!errors.is_empty());

    // Error path should include full nested path
    let error_path = errors[0]["path"].as_str().unwrap();
    assert!(
        error_path.contains("user")
            && error_path.contains("address")
            && error_path.contains("city"),
        "Error path should show full nested path: {}",
        error_path
    );
}

#[test]
fn test_validate_deeply_nested_5_levels() {
    let schema = create_schema_file(
        r#"[{
        "name": "l1",
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [{
            "name": "l2",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [{
                "name": "l3",
                "type": "RECORD",
                "mode": "NULLABLE",
                "fields": [{
                    "name": "l4",
                    "type": "RECORD",
                    "mode": "NULLABLE",
                    "fields": [{
                        "name": "l5",
                        "type": "STRING",
                        "mode": "REQUIRED"
                    }]
                }]
            }]
        }]
    }]"#,
    );

    // Valid deeply nested data
    let valid_data = create_data_file(r#"{"l1": {"l2": {"l3": {"l4": {"l5": "value"}}}}}"#);
    let (_, _, exit_code1) = run_validate(
        valid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(exit_code1, 0, "Valid deeply nested data should pass");

    // Missing required field at depth 5
    let invalid_data = create_data_file(r#"{"l1": {"l2": {"l3": {"l4": {}}}}}"#);
    let (_, _, exit_code2) = run_validate(
        invalid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(
        exit_code2, 1,
        "Missing deeply nested required field should fail"
    );
}

// =============================================================================
// REPEATED FIELD TESTS
// =============================================================================

#[test]
fn test_validate_repeated_field_validation() {
    let schema = create_schema_file(r#"[{"name": "tags", "type": "STRING", "mode": "REPEATED"}]"#);

    // Valid array
    let valid_data = create_data_file(r#"{"tags": ["a", "b", "c"]}"#);
    let (_, _, exit_code1) = run_validate(
        valid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(exit_code1, 0);

    // Invalid: non-array value for REPEATED field
    let invalid_data = create_data_file(r#"{"tags": "not_an_array"}"#);
    let (_, stderr, exit_code2) = run_validate(
        invalid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(exit_code2, 1, "Non-array for REPEATED should fail");
    assert!(stderr.contains("ARRAY") || stderr.contains("type"));
}

#[test]
fn test_validate_repeated_with_nulls_in_array() {
    let schema =
        create_schema_file(r#"[{"name": "values", "type": "INTEGER", "mode": "REPEATED"}]"#);

    // Array with nulls - should be allowed
    let data = create_data_file(r#"{"values": [1, null, 2]}"#);
    let (_, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(exit_code, 0, "Nulls in arrays should be allowed");
}

#[test]
fn test_validate_repeated_with_wrong_element_type() {
    let schema =
        create_schema_file(r#"[{"name": "numbers", "type": "INTEGER", "mode": "REPEATED"}]"#);

    // Array with wrong type element
    let data = create_data_file(r#"{"numbers": [1, 2, "three"]}"#);
    let (stdout, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--format", "json", "--strict-types"],
    );

    assert_eq!(exit_code, 1);

    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
    let errors = json["errors"].as_array().unwrap();
    assert!(!errors.is_empty());

    // Error path should include array index
    let error_path = errors[0]["path"].as_str().unwrap();
    assert!(
        error_path.contains("[2]") || error_path.contains("numbers"),
        "Error path should include array index: {}",
        error_path
    );
}

// =============================================================================
// MULTIPLE FILES TESTS
// =============================================================================

#[test]
fn test_validate_multiple_files_glob() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create schema
    let schema_path = dir.path().join("schema.json");
    let mut schema_file = File::create(&schema_path).unwrap();
    schema_file
        .write_all(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#.as_bytes())
        .unwrap();

    // Create multiple data files
    for i in 1..=3 {
        let data_path = dir.path().join(format!("data{}.json", i));
        let mut data_file = File::create(&data_path).unwrap();
        data_file
            .write_all(format!(r#"{{"id": {}}}"#, i).as_bytes())
            .unwrap();
    }

    // Use glob pattern to validate all files
    let pattern = dir.path().join("data*.json");
    let (_, stderr, exit_code) = run_validate(
        pattern.to_str().unwrap(),
        schema_path.to_str().unwrap(),
        &[],
    );

    assert_eq!(
        exit_code, 0,
        "All valid files should pass. stderr: {}",
        stderr
    );
    assert!(
        stderr.contains("3") || stderr.contains("passed"),
        "Should process multiple files"
    );
}

// =============================================================================
// EDGE CASE TESTS
// =============================================================================

#[test]
fn test_validate_empty_record() {
    let schema = create_schema_file(
        r#"[
        {"name": "optional1", "type": "STRING", "mode": "NULLABLE"},
        {"name": "optional2", "type": "INTEGER", "mode": "NULLABLE"}
    ]"#,
    );

    // Empty record with all nullable fields
    let data = create_data_file(r#"{}"#);

    let (_, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(
        exit_code, 0,
        "Empty record with only nullable fields should pass"
    );
}

#[test]
fn test_validate_case_insensitive_field_matching() {
    let schema =
        create_schema_file(r#"[{"name": "UserName", "type": "STRING", "mode": "NULLABLE"}]"#);

    // Data with different casing
    let data = create_data_file(r#"{"username": "test"}"#);

    let (_, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    // BigQuery is case-insensitive for field names
    assert_eq!(exit_code, 0, "Field matching should be case-insensitive");
}

#[test]
fn test_validate_empty_string_for_required_field() {
    let schema = create_schema_file(r#"[{"name": "name", "type": "STRING", "mode": "REQUIRED"}]"#);

    // Empty string should count as present
    let data = create_data_file(r#"{"name": ""}"#);

    let (_, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(exit_code, 0, "Empty string should satisfy REQUIRED");
}

#[test]
fn test_validate_timestamp_unix_epoch_numeric() {
    let schema = create_schema_file(r#"[{"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"}]"#);

    // Numeric timestamp (Unix epoch)
    let data = create_data_file(r#"{"ts": 1609459200}"#);

    // In lenient mode, numeric values should be accepted as timestamps
    let (_, _, exit_code) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );

    assert_eq!(
        exit_code, 0,
        "Numeric timestamp should be accepted in lenient mode"
    );

    // In strict mode, should reject
    let (_, _, exit_code2) = run_validate(
        data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &["--strict-types"],
    );

    assert_eq!(
        exit_code2, 1,
        "Numeric timestamp should be rejected in strict mode"
    );
}

#[test]
fn test_validate_float_integer_boundary() {
    let schema =
        create_schema_file(r#"[{"name": "big_int", "type": "INTEGER", "mode": "NULLABLE"}]"#);

    // Value within i64 range
    let valid_data = create_data_file(r#"{"big_int": 9223372036854775807}"#);
    let (_, _, exit_code1) = run_validate(
        valid_data.path().to_str().unwrap(),
        schema.path().to_str().unwrap(),
        &[],
    );
    assert_eq!(exit_code1, 0, "Max i64 value should be valid INTEGER");
}
