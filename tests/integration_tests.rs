//! Integration tests for BigQuery Schema Generator
//!
//! These tests verify the schema generation against various input scenarios,
//! matching the behavior of the Python bigquery-schema-generator.

use bq_schema_gen::{GeneratorConfig, InputFormat, SchemaGenerator, SchemaMap};
use serde_json::{json, Value};
use std::io::Cursor;

/// Helper to generate schema from JSON strings
fn generate_schema(records: &[&str], config: GeneratorConfig) -> (Vec<Value>, Vec<String>) {
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = SchemaMap::new();

    for record_str in records {
        let record: Value = serde_json::from_str(record_str).unwrap();
        let _ = generator.process_record(&record, &mut schema_map);
    }

    let schema = generator.flatten_schema(&schema_map);
    let schema_json: Vec<Value> = schema
        .iter()
        .map(|f| serde_json::to_value(f).unwrap())
        .collect();

    let errors: Vec<String> = generator
        .error_logs()
        .iter()
        .map(|e| format!("{}: {}", e.line_number, e.msg))
        .collect();

    (schema_json, errors)
}

/// Helper to check if schema contains a field with given properties
fn has_field(schema: &[Value], name: &str, field_type: &str, mode: &str) -> bool {
    schema
        .iter()
        .any(|f| f["name"] == name && f["type"] == field_type && f["mode"] == mode)
}

// =============================================================================
// BASIC TYPE INFERENCE TESTS
// =============================================================================

#[test]
fn test_primitive_types() {
    let (schema, errors) = generate_schema(
        &[
            r#"{ "s": "string", "b": true, "d": "2017-01-01", "i": 1, "t": "17:10:00", "ts": "2017-05-22T17:10:00-07:00", "x": 3.1 }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(errors.is_empty());
    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "b", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "d", "DATE", "NULLABLE"));
    assert!(has_field(&schema, "i", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "t", "TIME", "NULLABLE"));
    assert!(has_field(&schema, "ts", "TIMESTAMP", "NULLABLE"));
    assert!(has_field(&schema, "x", "FLOAT", "NULLABLE"));
}

#[test]
fn test_null_values_default() {
    // By default, null values should produce empty schema
    let (schema, _) = generate_schema(
        &[r#"{ "s": null, "a": [], "m": {} }"#],
        GeneratorConfig::default(),
    );

    assert!(schema.is_empty());
}

#[test]
fn test_null_values_with_keep_nulls() {
    let config = GeneratorConfig {
        keep_nulls: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(&[r#"{ "s": null, "a": [], "m": {} }"#], config);

    assert_eq!(schema.len(), 3);
    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "a", "STRING", "REPEATED"));
    assert!(has_field(&schema, "m", "RECORD", "NULLABLE"));
}

// =============================================================================
// TYPE COERCION TESTS
// =============================================================================

#[test]
fn test_integer_upgrades_to_float() {
    let (schema, errors) = generate_schema(
        &[r#"{ "x": 3 }"#, r#"{ "x": 3.1 }"#],
        GeneratorConfig::default(),
    );

    assert!(errors.is_empty());
    assert!(has_field(&schema, "x", "FLOAT", "NULLABLE"));
}

#[test]
fn test_float_does_not_downgrade_to_integer() {
    let (schema, errors) = generate_schema(
        &[r#"{ "x": 3.1 }"#, r#"{ "x": 3 }"#],
        GeneratorConfig::default(),
    );

    assert!(errors.is_empty());
    assert!(has_field(&schema, "x", "FLOAT", "NULLABLE"));
}

#[test]
fn test_timestamp_cannot_change_to_non_string() {
    let (schema, errors) = generate_schema(
        &[
            r#"{ "ts": "2017-05-22T17:10:00-07:00" }"#,
            r#"{ "ts": 1.0 }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(!errors.is_empty());
    assert!(schema.is_empty());
}

#[test]
fn test_date_cannot_change_to_non_string() {
    let (schema, errors) = generate_schema(
        &[r#"{ "d": "2017-01-01" }"#, r#"{ "d": 1.0 }"#],
        GeneratorConfig::default(),
    );

    assert!(!errors.is_empty());
    assert!(schema.is_empty());
}

#[test]
fn test_time_cannot_change_to_non_string() {
    let (schema, errors) = generate_schema(
        &[r#"{ "t": "17:10:01" }"#, r#"{ "t": 1.0 }"#],
        GeneratorConfig::default(),
    );

    assert!(!errors.is_empty());
    assert!(schema.is_empty());
}

#[test]
fn test_conflicting_time_date_reduces_to_string() {
    let (schema, _) = generate_schema(
        &[r#"{ "s": "17:10:02" }"#, r#"{ "s": "2017-01-01" }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
}

#[test]
fn test_conflicting_time_timestamp_reduces_to_string() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "s": "17:10:03" }"#,
            r#"{ "s": "2017-01-01T17:10:00" }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
}

#[test]
fn test_conflicting_date_timestamp_reduces_to_string() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "s": "2017-01-04" }"#,
            r#"{ "s": "2017-01-04T17:10:00" }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
}

// =============================================================================
// ARRAY AND RECORD TESTS
// =============================================================================

#[test]
fn test_arrays_and_records() {
    let (schema, _) = generate_schema(
        &[r#"{ "a": [1, 1], "r": { "r0": "r0", "r1": "r1" } }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "a", "INTEGER", "REPEATED"));
    assert!(has_field(&schema, "r", "RECORD", "NULLABLE"));
}

#[test]
fn test_empty_record_replaced_by_known_record() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "a": [1, 2], "r": {} }"#,
            r#"{ "a": [1, 2], "r": { "r0": "r0", "r1": "r1" } }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "r", "RECORD", "NULLABLE"));
    let record_field = schema.iter().find(|f| f["name"] == "r").unwrap();
    let fields = record_field["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 2);
}

#[test]
fn test_nullable_record_upgrades_to_repeated() {
    let (schema, errors) = generate_schema(
        &[r#"{ "r" : { "i": 3 } }"#, r#"{ "r" : [{ "i": 4 }] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "r", "RECORD", "REPEATED"));
    assert!(errors.iter().any(|e| e.contains("Converting schema")));
}

#[test]
fn test_repeated_record_not_downgraded_to_nullable() {
    let (schema, errors) = generate_schema(
        &[r#"{ "r" : [{ "i": 4 }] }"#, r#"{ "r" : { "i": 3 } }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "r", "RECORD", "REPEATED"));
    assert!(errors.iter().any(|e| e.contains("Leaving schema")));
}

#[test]
fn test_arrays_of_arrays_not_allowed() {
    let (schema, errors) = generate_schema(&[r#"{ "a": [[]] }"#], GeneratorConfig::default());

    assert!(schema.is_empty());
    assert!(!errors.is_empty());
}

#[test]
fn test_mixed_array_elements_not_allowed() {
    let (schema, errors) = generate_schema(
        &[r#"{ "s": "string", "x": 3.2, "i": 3, "b": true, "a": [ "a", 1] }"#],
        GeneratorConfig::default(),
    );

    // The "a" field should not be in schema, but others should
    assert!(!has_field(&schema, "a", "STRING", "REPEATED"));
    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "b", "BOOLEAN", "NULLABLE"));
    assert!(!errors.is_empty());
}

// =============================================================================
// NULL HANDLING TESTS
// =============================================================================

#[test]
fn test_null_does_not_clobber_previous_type() {
    let (schema, _) = generate_schema(
        &[r#"{ "i": 1 }"#, r#"{ "i": null }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "i", "INTEGER", "NULLABLE"));
}

#[test]
fn test_null_placeholder_upgrades_to_real_type() {
    let (schema, _) = generate_schema(
        &[r#"{ "i": null }"#, r#"{ "i": 3 }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "i", "INTEGER", "NULLABLE"));
}

// =============================================================================
// MODE TRANSITION TESTS
// =============================================================================

#[test]
fn test_no_upgrade_nullable_to_repeated_for_primitives() {
    let (schema, errors) = generate_schema(
        &[r#"{ "i": 3 }"#, r#"{ "i": [1, 2] }"#],
        GeneratorConfig::default(),
    );

    assert!(schema.is_empty());
    assert!(!errors.is_empty());
}

#[test]
fn test_no_downgrade_repeated_to_nullable() {
    let (schema, errors) = generate_schema(
        &[r#"{ "i": [1, 2] }"#, r#"{ "i": 3 }"#],
        GeneratorConfig::default(),
    );

    assert!(schema.is_empty());
    assert!(!errors.is_empty());
}

// =============================================================================
// QUOTED VALUE TESTS
// =============================================================================

#[test]
fn test_quoted_integer_float_boolean() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "qi" : "1", "qf": "1.0", "qb": "true" }"#,
            r#"{ "qi" : "2", "qf": "1.1", "qb": "True" }"#,
            r#"{ "qi" : "3", "qf": "2.0", "qb": "false" }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "qi", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "qf", "FLOAT", "NULLABLE"));
    assert!(has_field(&schema, "qb", "BOOLEAN", "NULLABLE"));
}

#[test]
fn test_quoted_values_mixed_with_unquoted() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "qi" : "1", "qf": "1.0", "qb": "true" }"#,
            r#"{ "qi" : 2, "qf": 2.0, "qb": false }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "qi", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "qf", "FLOAT", "NULLABLE"));
    assert!(has_field(&schema, "qb", "BOOLEAN", "NULLABLE"));
}

#[test]
fn test_qinteger_qfloat_to_float() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "qf_i" : "1.0", "qi_f": "2" }"#,
            r#"{ "qf_i" : 1.1, "qi_f": 2.1 }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "qf_i", "FLOAT", "NULLABLE"));
    assert!(has_field(&schema, "qi_f", "FLOAT", "NULLABLE"));
}

#[test]
fn test_string_plus_quoted_types_becomes_string() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "qi" : "foo", "qf": "bar", "qb": "foo2" }"#,
            r#"{ "qi" : "2", "qf": "1.1", "qb": "True" }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "qi", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "qf", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "qb", "STRING", "NULLABLE"));
}

#[test]
fn test_qinteger_to_qfloat_to_string() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "qn" : "1" }"#,
            r#"{ "qn" : "1.1" }"#,
            r#"{ "qn" : "test" }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "qn", "STRING", "NULLABLE"));
}

#[test]
fn test_quoted_values_are_strings_flag() {
    let config = GeneratorConfig {
        quoted_values_are_strings: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(
        &[
            r#"{ "qi" : "1", "qf": "1.0", "qb": "true" }"#,
            r#"{ "qi" : "2", "qf": "1.1", "qb": "True" }"#,
        ],
        config,
    );

    assert!(has_field(&schema, "qi", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "qf", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "qb", "STRING", "NULLABLE"));
}

// =============================================================================
// INTEGER OVERFLOW TESTS
// =============================================================================

#[test]
fn test_integer_at_max_boundary() {
    let (schema, _) = generate_schema(
        &[
            r#"{"name": "9223372036854775807"}"#,
            r#"{"name": "-9223372036854775808"}"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "name", "INTEGER", "NULLABLE"));
}

#[test]
fn test_integer_overflow_becomes_float() {
    let (schema, _) = generate_schema(
        &[
            r#"{"name": "9223372036854775808"}"#,
            r#"{"name": "-9223372036854775809"}"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "name", "FLOAT", "NULLABLE"));
}

#[test]
fn test_overflow_integer_plus_string_becomes_string() {
    let (schema, _) = generate_schema(
        &[r#"{"name": "9223372036854775808"}"#, r#"{"name": "hello"}"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
}

// =============================================================================
// TIMESTAMP FORMAT TESTS
// =============================================================================

#[test]
fn test_timestamp_with_various_suffixes() {
    let (schema, _) = generate_schema(
        &[
            r#"{"date": "2019-01-16T12:46:02Z"}"#,
            r#"{"date": "2019-01-16T12:46:03 -05:00"}"#,
            r#"{"date": "2019-01-16 12:46:01 UTC"}"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "date", "TIMESTAMP", "NULLABLE"));
}

#[test]
fn test_date_time_formats() {
    let (schema, _) = generate_schema(
        &[r#"{ "qd" : "2018-12-07", "qt": "21:52:00", "qdt": "2018-12-07T21:52:00-08:00" }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "qd", "DATE", "NULLABLE"));
    assert!(has_field(&schema, "qt", "TIME", "NULLABLE"));
    assert!(has_field(&schema, "qdt", "TIMESTAMP", "NULLABLE"));
}

// =============================================================================
// FIELD MERGING TESTS
// =============================================================================

#[test]
fn test_independent_fields_merged() {
    let (schema, _) = generate_schema(
        &[r#"{ "a": [1, 2] }"#, r#"{ "i": 3 }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "a", "INTEGER", "REPEATED"));
    assert!(has_field(&schema, "i", "INTEGER", "NULLABLE"));
}

#[test]
fn test_recursive_merging() {
    let (schema, _) = generate_schema(
        &[r#"{ "r" : { "a": [1, 2] } }"#, r#"{ "r" : { "i": 3 } }"#],
        GeneratorConfig::default(),
    );

    let record_field = schema.iter().find(|f| f["name"] == "r").unwrap();
    let fields = record_field["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 2);
}

// =============================================================================
// SANITIZE NAMES TESTS
// =============================================================================

#[test]
fn test_sanitize_names_replaces_invalid_chars() {
    let config = GeneratorConfig {
        sanitize_names: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(
        &[r#"{ "field-name": "test", "field.with.dots": 42 }"#],
        config,
    );

    assert!(schema.iter().all(|f| {
        let name = f["name"].as_str().unwrap();
        !name.contains('-') && !name.contains('.')
    }));
}

#[test]
fn test_sanitize_names_recursive() {
    let config = GeneratorConfig {
        sanitize_names: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(&[r#"{ "r" : { "a-name": [1, 2] } }"#], config);

    let record_field = schema.iter().find(|f| f["name"] == "r").unwrap();
    let fields = record_field["fields"].as_array().unwrap();
    assert!(fields[0]["name"] == "a_name");
}

// =============================================================================
// PRESERVE INPUT SORT ORDER TESTS
// =============================================================================

#[test]
fn test_preserve_input_sort_order() {
    let config = GeneratorConfig {
        preserve_input_sort_order: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(&[r#"{ "z": 1, "a": 2, "m": 3 }"#], config);

    // With preserve_input_sort_order, fields should be in insertion order
    assert_eq!(schema[0]["name"], "z");
    assert_eq!(schema[1]["name"], "a");
    assert_eq!(schema[2]["name"], "m");
}

#[test]
fn test_default_alphabetical_sort() {
    let (schema, _) = generate_schema(
        &[r#"{ "z": 1, "a": 2, "m": 3 }"#],
        GeneratorConfig::default(),
    );

    // Default is alphabetical sort
    assert_eq!(schema[0]["name"], "a");
    assert_eq!(schema[1]["name"], "m");
    assert_eq!(schema[2]["name"], "z");
}

// =============================================================================
// NESTED RECORD PATH TESTS
// =============================================================================

#[test]
fn test_nested_path_in_error_messages() {
    let (_, errors) = generate_schema(
        &[
            r#"{"source_machine":{"port":80},"dest_machine":{"port":80}}"#,
            r#"{"source_machine":{"port":80},"dest_machine":{"port":"http-port"}}"#,
        ],
        GeneratorConfig::default(),
    );

    // Error should contain the full path
    assert!(errors.iter().any(|e| e.contains("dest_machine.port")));
}

// =============================================================================
// DEEPLY NESTED STRUCTURES
// =============================================================================

#[test]
fn test_deeply_nested_records() {
    let (schema, _) = generate_schema(
        &[r#"{ "level1": { "level2": { "level3": { "value": 42 } } } }"#],
        GeneratorConfig::default(),
    );

    let l1 = schema.iter().find(|f| f["name"] == "level1").unwrap();
    let l2 = l1["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "level2")
        .unwrap();
    let l3 = l2["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "level3")
        .unwrap();
    let value = l3["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|f| f["name"] == "value")
        .unwrap();

    assert_eq!(value["type"], "INTEGER");
}

// =============================================================================
// ARRAY OF RECORDS TESTS
// =============================================================================

#[test]
fn test_array_of_records() {
    let (schema, _) = generate_schema(
        &[r#"{ "items": [{"name": "a", "value": 1}, {"name": "b", "value": 2}] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "items", "RECORD", "REPEATED"));
    let items_field = schema.iter().find(|f| f["name"] == "items").unwrap();
    let fields = items_field["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 2);
}

#[test]
fn test_array_of_records_with_different_fields() {
    let (schema, _) = generate_schema(
        &[r#"{ "items": [{"name": "a"}, {"value": 1}] }"#],
        GeneratorConfig::default(),
    );

    let items_field = schema.iter().find(|f| f["name"] == "items").unwrap();
    let fields = items_field["fields"].as_array().unwrap();
    // Both name and value should be in the schema
    assert_eq!(fields.len(), 2);
}

// =============================================================================
// EDGE CASES
// =============================================================================

#[test]
fn test_empty_string_value() {
    let (schema, _) = generate_schema(&[r#"{ "s": "" }"#], GeneratorConfig::default());

    assert!(has_field(&schema, "s", "STRING", "NULLABLE"));
}

#[test]
fn test_boolean_variations() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "b1": true, "b2": false }"#,
            r#"{ "b1": "True", "b2": "FALSE" }"#,
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "b1", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "b2", "BOOLEAN", "NULLABLE"));
}

#[test]
fn test_numeric_array() {
    let (schema, _) = generate_schema(
        &[r#"{ "nums": [1, 2, 3, 4, 5] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "nums", "INTEGER", "REPEATED"));
}

#[test]
fn test_float_array() {
    let (schema, _) = generate_schema(
        &[r#"{ "nums": [1.1, 2.2, 3.3] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "nums", "FLOAT", "REPEATED"));
}

#[test]
fn test_mixed_int_float_array_becomes_float() {
    let (schema, _) = generate_schema(&[r#"{ "nums": [1, 2.5, 3] }"#], GeneratorConfig::default());

    assert!(has_field(&schema, "nums", "FLOAT", "REPEATED"));
}

#[test]
fn test_string_array() {
    let (schema, _) = generate_schema(
        &[r#"{ "tags": ["a", "b", "c"] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "tags", "STRING", "REPEATED"));
}

#[test]
fn test_boolean_array() {
    let (schema, _) = generate_schema(
        &[r#"{ "flags": [true, false, true] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "flags", "BOOLEAN", "REPEATED"));
}

#[test]
fn test_date_array() {
    let (schema, _) = generate_schema(
        &[r#"{ "dates": ["2024-01-01", "2024-02-01", "2024-03-01"] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "dates", "DATE", "REPEATED"));
}

#[test]
fn test_timestamp_array() {
    let (schema, _) = generate_schema(
        &[r#"{ "times": ["2024-01-01T10:00:00Z", "2024-02-01T11:00:00Z"] }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "times", "TIMESTAMP", "REPEATED"));
}

// =============================================================================
// CASE SENSITIVITY TESTS
// =============================================================================

#[test]
fn test_case_insensitive_field_merging() {
    // BigQuery is case-insensitive for field names
    let (schema, _) = generate_schema(
        &[r#"{ "Name": "test1" }"#, r#"{ "name": "test2" }"#],
        GeneratorConfig::default(),
    );

    // Should have only one field (first seen name preserved)
    assert_eq!(schema.len(), 1);
}

// =============================================================================
// LARGE INTEGER TESTS
// =============================================================================

#[test]
fn test_large_positive_integer() {
    let (schema, _) = generate_schema(
        &[r#"{ "big": 9223372036854775807 }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "big", "INTEGER", "NULLABLE"));
}

#[test]
fn test_large_negative_integer() {
    let (schema, _) = generate_schema(
        &[r#"{ "big": -9223372036854775808 }"#],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "big", "INTEGER", "NULLABLE"));
}

// =============================================================================
// FLOAT SPECIAL VALUES
// =============================================================================

#[test]
fn test_float_with_exponent() {
    let (schema, _) = generate_schema(&[r#"{ "sci": 1.5e10 }"#], GeneratorConfig::default());

    assert!(has_field(&schema, "sci", "FLOAT", "NULLABLE"));
}

#[test]
fn test_quoted_float_with_exponent() {
    let (schema, _) = generate_schema(&[r#"{ "sci": "1.5e10" }"#], GeneratorConfig::default());

    assert!(has_field(&schema, "sci", "FLOAT", "NULLABLE"));
}

// =============================================================================
// MULTIPLE RECORDS WITH VARYING FIELDS
// =============================================================================

#[test]
fn test_sparse_records() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "a": 1 }"#,
            r#"{ "b": 2 }"#,
            r#"{ "c": 3 }"#,
            r#"{ "a": 4, "b": 5, "c": 6 }"#,
        ],
        GeneratorConfig::default(),
    );

    assert_eq!(schema.len(), 3);
    assert!(has_field(&schema, "a", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "b", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "c", "INTEGER", "NULLABLE"));
}

#[test]
fn test_evolving_schema() {
    let (schema, _) = generate_schema(
        &[
            r#"{ "v1_field": "old" }"#,
            r#"{ "v1_field": "old", "v2_field": 123 }"#,
            r#"{ "v1_field": "old", "v2_field": 456, "v3_field": true }"#,
        ],
        GeneratorConfig::default(),
    );

    assert_eq!(schema.len(), 3);
    assert!(has_field(&schema, "v1_field", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "v2_field", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "v3_field", "BOOLEAN", "NULLABLE"));
}
