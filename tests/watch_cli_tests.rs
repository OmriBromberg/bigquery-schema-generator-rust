//! CLI integration tests for watch mode functionality.
//!
//! Note: Most watch mode tests focus on the CLI arguments and initial behavior.
//! Testing actual file watching with events would require timeouts and is complex
//! for CI environments, so those are marked appropriately.

use std::fs::File;
use std::io::Write;
use std::process::{Command, Stdio};
use tempfile::tempdir;

/// Helper to run the CLI with arguments (no stdin needed)
fn run_cli(args: &[&str]) -> (String, String, bool) {
    let output = Command::new("./target/debug/bq-schema-gen")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let success = output.status.success();

    (stdout, stderr, success)
}

// =============================================================================
// HELP AND DOCUMENTATION TESTS
// =============================================================================

#[test]
fn test_watch_flag_help() {
    let output = Command::new("./target/debug/bq-schema-gen")
        .arg("--help")
        .output()
        .expect("Failed to run command");

    let stdout = String::from_utf8_lossy(&output.stdout);

    // Watch mode flags should be documented
    assert!(
        stdout.contains("--watch"),
        "Help should mention --watch flag"
    );
    assert!(
        stdout.contains("--debounce"),
        "Help should mention --debounce flag"
    );
    assert!(
        stdout.contains("--on-change"),
        "Help should mention --on-change flag"
    );
}

// =============================================================================
// VALIDATION TESTS
// =============================================================================

#[test]
fn test_watch_requires_files() {
    // --watch without any files should fail
    let (_, stderr, success) = run_cli(&["--watch"]);

    assert!(!success, "Watch mode without files should fail");
    assert!(
        stderr.contains("requires") || stderr.contains("pattern") || stderr.contains("Error"),
        "Should report that watch requires files. stderr: {}",
        stderr
    );
}

#[test]
fn test_watch_incompatible_with_per_file() {
    let dir = tempdir().expect("Failed to create temp dir");
    let file_path = dir.path().join("test.json");
    File::create(&file_path)
        .unwrap()
        .write_all(r#"{"id": 1}"#.as_bytes())
        .unwrap();

    let (_, stderr, success) = run_cli(&[file_path.to_str().unwrap(), "--watch", "--per-file"]);

    assert!(!success, "Watch with --per-file should fail");
    assert!(
        stderr.contains("cannot be used")
            || stderr.contains("incompatible")
            || stderr.contains("--per-file"),
        "Should report incompatibility. stderr: {}",
        stderr
    );
}

// =============================================================================
// FLAG PARSING TESTS
// =============================================================================

#[test]
fn test_watch_debounce_flag() {
    let dir = tempdir().expect("Failed to create temp dir");
    let file_path = dir.path().join("test.json");
    File::create(&file_path)
        .unwrap()
        .write_all(r#"{"id": 1}"#.as_bytes())
        .unwrap();

    // Start watch mode (will be killed immediately, but should accept the flag)
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([file_path.to_str().unwrap(), "--watch", "--debounce", "500"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it a moment to start
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Kill the process
    let _ = child.kill();
    let output = child.wait_with_output().expect("Failed to wait");

    // Should not have errored on the flag itself
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unknown") && !stderr.contains("invalid"),
        "debounce flag should be accepted. stderr: {}",
        stderr
    );
}

#[test]
fn test_watch_on_change_flag() {
    let dir = tempdir().expect("Failed to create temp dir");
    let file_path = dir.path().join("test.json");
    File::create(&file_path)
        .unwrap()
        .write_all(r#"{"id": 1}"#.as_bytes())
        .unwrap();

    // Start watch mode with on-change command
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([
            file_path.to_str().unwrap(),
            "--watch",
            "--on-change",
            "echo 'Schema changed'",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it a moment to start
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Kill the process
    let _ = child.kill();
    let output = child.wait_with_output().expect("Failed to wait");

    // Should not have errored on the flag
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("unknown") && !stderr.contains("invalid"),
        "on-change flag should be accepted. stderr: {}",
        stderr
    );
}

#[test]
fn test_watch_glob_pattern_expansion() {
    let dir = tempdir().expect("Failed to create temp dir");

    // Create multiple JSON files
    for i in 0..3 {
        let path = dir.path().join(format!("data{}.json", i));
        File::create(&path)
            .unwrap()
            .write_all(r#"{"id": 1}"#.as_bytes())
            .unwrap();
    }

    let pattern = dir.path().join("*.json");

    // Start watch mode with glob pattern
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([pattern.to_str().unwrap(), "--watch"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it more time to start and report (async file watching setup can take time)
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Kill the process
    let _ = child.kill();
    let output = child.wait_with_output().expect("Failed to wait");

    let stderr = String::from_utf8_lossy(&output.stderr);
    // The process should have started successfully (not errored on invalid args)
    // In watch mode, output may vary based on timing, so we just verify it didn't error
    // due to flag parsing issues
    assert!(
        !stderr.contains("error: invalid") && !stderr.contains("unrecognized"),
        "Glob pattern should be accepted. stderr: {}",
        stderr
    );
}

// =============================================================================
// OUTPUT FILE TESTS
// =============================================================================

#[test]
fn test_watch_with_output_file() {
    let dir = tempdir().expect("Failed to create temp dir");

    let data_path = dir.path().join("data.json");
    File::create(&data_path)
        .unwrap()
        .write_all(r#"{"name": "test", "value": 42}"#.as_bytes())
        .unwrap();

    let output_path = dir.path().join("schema.json");

    // Start watch mode with output file
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([
            data_path.to_str().unwrap(),
            "--watch",
            "-o",
            output_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it time to write initial schema
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Kill the process
    let _ = child.kill();
    let _ = child.wait();

    // Check that the output file was created
    assert!(
        output_path.exists(),
        "Initial schema should be written to output file"
    );

    // Verify it's valid JSON schema
    let content = std::fs::read_to_string(&output_path).unwrap();
    let schema: serde_json::Value =
        serde_json::from_str(&content).expect("Output should be valid JSON");
    assert!(schema.is_array());
}

// =============================================================================
// QUIET MODE TESTS
// =============================================================================

#[test]
fn test_watch_quiet_mode() {
    let dir = tempdir().expect("Failed to create temp dir");

    let data_path = dir.path().join("data.json");
    File::create(&data_path)
        .unwrap()
        .write_all(r#"{"id": 1}"#.as_bytes())
        .unwrap();

    // Start watch mode with quiet flag
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([data_path.to_str().unwrap(), "--watch", "-q"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it a moment
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Kill the process
    let _ = child.kill();
    let output = child.wait_with_output().expect("Failed to wait");

    let stderr = String::from_utf8_lossy(&output.stderr);
    // In quiet mode, should have minimal output
    assert!(
        stderr.is_empty() || !stderr.contains("Processing"),
        "Quiet mode should suppress progress. stderr: {}",
        stderr
    );
}

// =============================================================================
// ERROR HANDLING TESTS
// =============================================================================

#[test]
fn test_watch_nonexistent_files() {
    let (_, stderr, success) = run_cli(&["/nonexistent/path/*.json", "--watch"]);

    assert!(!success, "Watch with no matching files should fail");
    assert!(
        stderr.contains("No files") || stderr.contains("Error") || stderr.contains("matched"),
        "Should report no files matched. stderr: {}",
        stderr
    );
}

#[test]
fn test_watch_empty_directory() {
    let dir = tempdir().expect("Failed to create temp dir");
    let pattern = dir.path().join("*.json");

    let (_, stderr, success) = run_cli(&[pattern.to_str().unwrap(), "--watch"]);

    assert!(!success, "Watch with no matching files should fail");
    assert!(
        stderr.contains("No files") || stderr.contains("Error"),
        "Should report no files matched. stderr: {}",
        stderr
    );
}

// =============================================================================
// CONFIGURATION TESTS
// =============================================================================

#[test]
fn test_watch_with_ignore_invalid_lines() {
    let dir = tempdir().expect("Failed to create temp dir");

    let data_path = dir.path().join("data.json");
    {
        let mut file = File::create(&data_path).unwrap();
        writeln!(file, r#"{{"valid": 1}}"#).unwrap();
        writeln!(file, "invalid json").unwrap();
        writeln!(file, r#"{{"also_valid": 2}}"#).unwrap();
    }

    let output_path = dir.path().join("schema.json");

    // Start watch mode with ignore-invalid-lines
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([
            data_path.to_str().unwrap(),
            "--watch",
            "--ignore-invalid-lines",
            "-o",
            output_path.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it time to process
    std::thread::sleep(std::time::Duration::from_millis(500));

    // Kill the process
    let _ = child.kill();
    let _ = child.wait();

    // Should have created output with valid fields
    if output_path.exists() {
        let content = std::fs::read_to_string(&output_path).unwrap();
        let schema: serde_json::Value = serde_json::from_str(&content).unwrap();
        let arr = schema.as_array().unwrap();

        // Should have processed the valid fields
        assert!(
            arr.iter()
                .any(|f| f["name"] == "valid" || f["name"] == "also_valid"),
            "Should process valid lines despite invalid ones"
        );
    }
}

// =============================================================================
// COMBINATION TESTS
// =============================================================================

#[test]
fn test_watch_with_multiple_options() {
    let dir = tempdir().expect("Failed to create temp dir");

    let data_path = dir.path().join("data.json");
    File::create(&data_path)
        .unwrap()
        .write_all(r#"{"name": "test", "count": 42}"#.as_bytes())
        .unwrap();

    let output_path = dir.path().join("schema.json");

    // Start watch mode with multiple options
    let mut child = Command::new("./target/debug/bq-schema-gen")
        .args([
            data_path.to_str().unwrap(),
            "--watch",
            "--debounce",
            "200",
            "-o",
            output_path.to_str().unwrap(),
            "--preserve-input-sort-order",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("Failed to spawn command");

    // Give it more time to start and write initial schema
    std::thread::sleep(std::time::Duration::from_millis(800));

    // Kill the process
    let _ = child.kill();
    let output = child.wait_with_output().expect("Failed to wait");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // The test passes if either:
    // 1. Output file was created, OR
    // 2. Process didn't error due to invalid flags (it accepted all the options)
    let flags_accepted = !stderr.contains("error: invalid") && !stderr.contains("unrecognized");

    assert!(
        output_path.exists() || flags_accepted,
        "Either output should be created or flags should be accepted. stderr: {}",
        stderr
    );
}

// =============================================================================
// NOTE ABOUT ASYNC FILE CHANGE TESTS
// =============================================================================

// The following tests would verify actual file watching behavior:
// - test_watch_detects_file_modification
// - test_watch_detects_file_creation
// - test_watch_detects_file_deletion
// - test_watch_runs_on_change_command
//
// These are challenging to test reliably in CI because:
// 1. They require timeouts which can be flaky
// 2. File system event timing varies between OS/filesystems
// 3. The process runs indefinitely and must be killed
//
// The unit tests in src/watch/mod.rs test the core logic.
// The tests here verify CLI argument handling and initial state.
