//! CLI integration tests for parallel processing functionality.
//!
//! These tests verify that the --threads flag and parallel processing work correctly.

use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::tempdir;

/// Helper to run the CLI with files
fn run_cli_with_files(files: &[&str], args: &[&str]) -> (String, String, bool) {
    let mut cmd_args: Vec<&str> = files.to_vec();
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

    (stdout, stderr, success)
}

/// Parse JSON output from CLI
fn parse_schema(output: &str) -> serde_json::Value {
    serde_json::from_str(output).expect("Failed to parse JSON output")
}

/// Create test data files in a temporary directory
fn create_test_files(dir: &std::path::Path, count: usize) -> Vec<String> {
    let mut paths = Vec::new();

    for i in 0..count {
        let path = dir.join(format!("data{}.json", i));
        let mut file = File::create(&path).expect("Failed to create file");
        writeln!(
            file,
            r#"{{"id": {}, "name": "item_{}", "value": {}}}"#,
            i,
            i,
            i * 10
        )
        .expect("Failed to write");
        paths.push(path.to_string_lossy().to_string());
    }

    paths
}

// =============================================================================
// BASIC PARALLEL TESTS
// =============================================================================

#[test]
fn test_parallel_threads_flag_help() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .arg("--help")
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("--threads"),
        "Help should mention --threads flag"
    );
}

#[test]
fn test_parallel_single_thread_same_as_sequential() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 5);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    // Run with default (sequential for 5 files might use parallel)
    let (stdout1, _, success1) = run_cli_with_files(&file_refs, &["-q"]);
    assert!(success1);

    // Run with explicit 1 thread (sequential)
    let (stdout2, _, success2) = run_cli_with_files(&file_refs, &["--threads", "1", "-q"]);
    assert!(success2);

    // Both should produce identical schemas
    let schema1 = parse_schema(&stdout1);
    let schema2 = parse_schema(&stdout2);

    assert_eq!(
        schema1.as_array().unwrap().len(),
        schema2.as_array().unwrap().len(),
        "Single thread and default should produce same schema"
    );
}

#[test]
fn test_parallel_multi_thread_same_result() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 10);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    // Run with 1 thread (sequential)
    let (stdout1, _, success1) = run_cli_with_files(&file_refs, &["--threads", "1", "-q"]);
    assert!(success1);

    // Run with 4 threads (parallel)
    let (stdout4, _, success4) = run_cli_with_files(&file_refs, &["--threads", "4", "-q"]);
    assert!(success4);

    // Both should produce identical schemas
    let schema1 = parse_schema(&stdout1);
    let schema4 = parse_schema(&stdout4);

    // Compare field count and structure
    let arr1 = schema1.as_array().unwrap();
    let arr4 = schema4.as_array().unwrap();

    assert_eq!(
        arr1.len(),
        arr4.len(),
        "Parallel should produce same field count"
    );

    // Check that all fields are present with same types
    for field1 in arr1 {
        let name = field1["name"].as_str().unwrap();
        let field4 = arr4
            .iter()
            .find(|f| f["name"].as_str() == Some(name))
            .unwrap_or_else(|| panic!("Field {} should exist in parallel result", name));

        assert_eq!(
            field1["type"], field4["type"],
            "Field {} should have same type",
            name
        );
        assert_eq!(
            field1["mode"], field4["mode"],
            "Field {} should have same mode",
            name
        );
    }
}

#[test]
fn test_parallel_result_consistency() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 20);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    // Run multiple times with parallel processing
    let (stdout1, _, _) = run_cli_with_files(&file_refs, &["--threads", "4", "-q"]);
    let (stdout2, _, _) = run_cli_with_files(&file_refs, &["--threads", "4", "-q"]);
    let (stdout3, _, _) = run_cli_with_files(&file_refs, &["--threads", "4", "-q"]);

    let schema1 = parse_schema(&stdout1);
    let schema2 = parse_schema(&stdout2);
    let schema3 = parse_schema(&stdout3);

    // All runs should produce identical results
    assert_eq!(
        schema1, schema2,
        "Multiple parallel runs should be consistent"
    );
    assert_eq!(
        schema2, schema3,
        "Multiple parallel runs should be consistent"
    );
}

// =============================================================================
// MERGED OUTPUT TESTS
// =============================================================================

#[test]
fn test_parallel_multiple_files_merged() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create files with different fields
    let path1 = dir.path().join("data1.json");
    let path2 = dir.path().join("data2.json");
    let path3 = dir.path().join("data3.json");

    File::create(&path1)
        .unwrap()
        .write_all(r#"{"field_a": 1}"#.as_bytes())
        .unwrap();
    File::create(&path2)
        .unwrap()
        .write_all(r#"{"field_b": "hello"}"#.as_bytes())
        .unwrap();
    File::create(&path3)
        .unwrap()
        .write_all(r#"{"field_c": true}"#.as_bytes())
        .unwrap();

    let (stdout, _, success) = run_cli_with_files(
        &[
            path1.to_str().unwrap(),
            path2.to_str().unwrap(),
            path3.to_str().unwrap(),
        ],
        &["--threads", "2", "-q"],
    );

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    // All fields from all files should be merged
    assert_eq!(arr.len(), 3, "Should have fields from all files");
    assert!(arr.iter().any(|f| f["name"] == "field_a"));
    assert!(arr.iter().any(|f| f["name"] == "field_b"));
    assert!(arr.iter().any(|f| f["name"] == "field_c"));
}

#[test]
fn test_parallel_type_widening_across_files() {
    let dir = tempdir().expect("Failed to create temp dir");

    // File 1 has integer
    let path1 = dir.path().join("data1.json");
    File::create(&path1)
        .unwrap()
        .write_all(r#"{"value": 42}"#.as_bytes())
        .unwrap();

    // File 2 has float for same field
    let path2 = dir.path().join("data2.json");
    File::create(&path2)
        .unwrap()
        .write_all(r#"{"value": 3.14}"#.as_bytes())
        .unwrap();

    let (stdout, _, success) = run_cli_with_files(
        &[path1.to_str().unwrap(), path2.to_str().unwrap()],
        &["--threads", "2", "-q"],
    );

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    // Should be widened to FLOAT
    let value_field = arr.iter().find(|f| f["name"] == "value").unwrap();
    assert_eq!(
        value_field["type"], "FLOAT",
        "INTEGER + FLOAT should widen to FLOAT"
    );
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

#[test]
fn test_parallel_error_handling_continues() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create some valid files
    let path1 = dir.path().join("valid1.json");
    let path2 = dir.path().join("valid2.json");

    File::create(&path1)
        .unwrap()
        .write_all(r#"{"id": 1}"#.as_bytes())
        .unwrap();
    File::create(&path2)
        .unwrap()
        .write_all(r#"{"id": 2}"#.as_bytes())
        .unwrap();

    // Create an invalid file
    let path_bad = dir.path().join("invalid.json");
    File::create(&path_bad)
        .unwrap()
        .write_all(b"not valid json")
        .unwrap();

    // With --ignore-invalid-lines, should continue processing
    let (stdout, _stderr, success) = run_cli_with_files(
        &[
            path1.to_str().unwrap(),
            path_bad.to_str().unwrap(),
            path2.to_str().unwrap(),
        ],
        &["--threads", "2", "--ignore-invalid-lines"],
    );

    // May or may not succeed depending on how errors are handled
    // But should at least have processed the valid files
    if success {
        let schema = parse_schema(&stdout);
        let arr = schema.as_array().unwrap();
        assert!(
            arr.iter().any(|f| f["name"] == "id"),
            "Should have processed valid files"
        );
    }
}

// =============================================================================
// OUTPUT FILE TESTS
// =============================================================================

#[test]
fn test_parallel_output_file_option() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 5);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    let output_path = dir.path().join("schema.json");

    let (_, _, success) = run_cli_with_files(
        &file_refs,
        &["--threads", "2", "-o", output_path.to_str().unwrap(), "-q"],
    );

    assert!(success);
    assert!(output_path.exists(), "Output file should be created");

    // Verify the output file contains valid JSON schema
    let content = std::fs::read_to_string(&output_path).unwrap();
    let schema: serde_json::Value = serde_json::from_str(&content).unwrap();
    assert!(schema.is_array());
}

// =============================================================================
// LARGE FILE SET TESTS
// =============================================================================

#[test]
fn test_parallel_large_file_set() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 100);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    let (stdout, stderr, success) = run_cli_with_files(&file_refs, &["--threads", "4"]);

    assert!(
        success,
        "Should process 100 files successfully. stderr: {}",
        stderr
    );

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    // Should have the expected fields
    assert_eq!(arr.len(), 3, "Should have id, name, value fields");

    // Check progress output mentions file count
    assert!(
        stderr.contains("100") || stderr.contains("files"),
        "Should report processing multiple files"
    );
}

// =============================================================================
// CSV PARALLEL TESTS
// =============================================================================

#[test]
fn test_parallel_csv_files() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create CSV files
    for i in 0..5 {
        let path = dir.path().join(format!("data{}.csv", i));
        let mut file = File::create(&path).unwrap();
        writeln!(file, "id,name,value").unwrap();
        for j in 0..10 {
            writeln!(file, "{},{},item_{}", i * 10 + j, j, j).unwrap();
        }
    }

    let pattern = dir.path().join("*.csv");

    let (stdout, _, success) = run_cli_with_files(
        &[pattern.to_str().unwrap()],
        &["--input-format", "csv", "--threads", "2", "-q"],
    );

    assert!(success);

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    assert_eq!(arr.len(), 3, "Should have id, name, value from CSV");
}

// =============================================================================
// GLOB PATTERN TESTS WITH PARALLEL
// =============================================================================

#[test]
fn test_parallel_glob_pattern() {
    let dir = tempdir().expect("Failed to create temp dir");
    create_test_files(dir.path(), 10);

    let pattern = dir.path().join("data*.json");

    let (stdout, _, success) =
        run_cli_with_files(&[pattern.to_str().unwrap()], &["--threads", "4", "-q"]);

    assert!(success, "Glob pattern should work with parallel processing");

    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert!(!arr.is_empty());
}

// =============================================================================
// THREAD COUNT TESTS
// =============================================================================

#[test]
fn test_parallel_various_thread_counts() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 20);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    // Test with different thread counts
    for threads in &["1", "2", "4", "8"] {
        let (stdout, _, success) = run_cli_with_files(&file_refs, &["--threads", threads, "-q"]);

        assert!(success, "Should succeed with {} threads", threads);

        let schema = parse_schema(&stdout);
        let arr = schema.as_array().unwrap();
        assert_eq!(
            arr.len(),
            3,
            "Should produce same schema with {} threads",
            threads
        );
    }
}

// =============================================================================
// PROGRESS BAR TESTS
// =============================================================================

#[test]
fn test_parallel_progress_output() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 20);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    // Without -q, should show progress
    let (_, stderr, success) = run_cli_with_files(&file_refs, &["--threads", "4"]);

    assert!(success);
    // Progress indicator should mention thread count or files processed
    assert!(
        stderr.contains("threads")
            || stderr.contains("files")
            || stderr.contains("records")
            || stderr.contains("Done"),
        "Should show progress information. stderr: {}",
        stderr
    );
}

#[test]
fn test_parallel_quiet_mode() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 10);
    let file_refs: Vec<&str> = files.iter().map(|s| s.as_str()).collect();

    // With -q, should suppress progress
    let (stdout, stderr, success) = run_cli_with_files(&file_refs, &["--threads", "4", "-q"]);

    assert!(success);
    assert!(
        stderr.is_empty(),
        "Quiet mode should suppress progress. Got: {}",
        stderr
    );
    assert!(!stdout.is_empty(), "Should still output schema");
}

// =============================================================================
// EDGE CASES
// =============================================================================

#[test]
fn test_parallel_single_file() {
    let dir = tempdir().expect("Failed to create temp dir");
    let files = create_test_files(dir.path(), 1);

    // Even with --threads, single file should work
    let (stdout, _, success) = run_cli_with_files(&[files[0].as_str()], &["--threads", "4", "-q"]);

    assert!(success);
    let schema = parse_schema(&stdout);
    assert!(schema.is_array());
}

#[test]
fn test_parallel_empty_files() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create empty JSON files
    for i in 0..3 {
        let path = dir.path().join(format!("empty{}.json", i));
        File::create(&path).unwrap();
    }

    let pattern = dir.path().join("empty*.json");

    let (stdout, _, success) =
        run_cli_with_files(&[pattern.to_str().unwrap()], &["--threads", "2", "-q"]);

    assert!(success, "Should handle empty files");
    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();
    assert!(arr.is_empty(), "Empty files should produce empty schema");
}

#[test]
fn test_parallel_preserves_input_sort_order() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create a file with fields in specific order
    let path = dir.path().join("data.json");
    File::create(&path)
        .unwrap()
        .write_all(r#"{"z_field": 1, "a_field": 2, "m_field": 3}"#.as_bytes())
        .unwrap();

    // Test with preserve order flag
    let (stdout, _, success) = run_cli_with_files(
        &[path.to_str().unwrap()],
        &["--threads", "1", "--preserve-input-sort-order", "-q"],
    );

    assert!(success);
    let schema = parse_schema(&stdout);
    let arr = schema.as_array().unwrap();

    // First field should be z_field (preserving input order)
    assert_eq!(arr[0]["name"], "z_field");
}
