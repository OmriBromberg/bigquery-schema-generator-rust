//! Edge case tests for BigQuery Schema Generator

use bq_schema_gen::{bq_schema_to_map, GeneratorConfig, SchemaGenerator, SchemaMap};
use serde_json::{json, Value};

/// Helper to generate schema from JSON values
fn generate_schema(records: &[Value], config: GeneratorConfig) -> (Vec<Value>, Vec<String>) {
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = SchemaMap::new();

    for record in records {
        let _ = generator.process_record(record, &mut schema_map);
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

fn has_field(schema: &[Value], name: &str, field_type: &str, mode: &str) -> bool {
    schema
        .iter()
        .any(|f| f["name"] == name && f["type"] == field_type && f["mode"] == mode)
}

// =============================================================================
// UNICODE AND SPECIAL CHARACTERS
// =============================================================================

#[test]
fn test_unicode_field_names() {
    let (schema, _) = generate_schema(
        &[json!({"日本語": "value", "emoji_🎉": 42})],
        GeneratorConfig::default(),
    );

    assert_eq!(schema.len(), 2);
}

#[test]
fn test_unicode_string_values() {
    let (schema, _) = generate_schema(
        &[json!({"text": "こんにちは世界", "emoji": "Hello 🌍!"})],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "text", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "emoji", "STRING", "NULLABLE"));
}

#[test]
fn test_special_characters_in_strings() {
    let (schema, _) = generate_schema(
        &[json!({"text": "Line1\nLine2\tTabbed", "escaped": "Quote: \"test\""})],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "text", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "escaped", "STRING", "NULLABLE"));
}

// =============================================================================
// EXTREME VALUES
// =============================================================================

#[test]
fn test_very_long_string() {
    let long_string = "a".repeat(100000);
    let (schema, _) = generate_schema(&[json!({"long": long_string})], GeneratorConfig::default());

    assert!(has_field(&schema, "long", "STRING", "NULLABLE"));
}

#[test]
fn test_very_deep_nesting() {
    let deep = json!({
        "l1": {
            "l2": {
                "l3": {
                    "l4": {
                        "l5": {
                            "l6": {
                                "l7": {
                                    "l8": {
                                        "l9": {
                                            "l10": {
                                                "value": 42
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    });

    let (schema, _) = generate_schema(&[deep], GeneratorConfig::default());

    assert_eq!(schema.len(), 1);
    assert!(has_field(&schema, "l1", "RECORD", "NULLABLE"));
}

#[test]
fn test_many_fields_in_one_record() {
    let mut obj = serde_json::Map::new();
    for i in 0..500 {
        obj.insert(format!("field_{}", i), json!(i));
    }

    let (schema, _) = generate_schema(&[Value::Object(obj)], GeneratorConfig::default());

    assert_eq!(schema.len(), 500);
}

#[test]
fn test_very_long_array() {
    let arr: Vec<i32> = (0..10000).collect();
    let (schema, _) = generate_schema(&[json!({"numbers": arr})], GeneratorConfig::default());

    assert!(has_field(&schema, "numbers", "INTEGER", "REPEATED"));
}

// =============================================================================
// FLOATING POINT EDGE CASES
// =============================================================================

#[test]
fn test_float_zero() {
    let (schema, _) = generate_schema(&[json!({"zero": 0.0})], GeneratorConfig::default());

    assert!(has_field(&schema, "zero", "FLOAT", "NULLABLE"));
}

#[test]
fn test_float_negative_zero() {
    let (schema, _) = generate_schema(&[json!({"negzero": -0.0})], GeneratorConfig::default());

    assert!(has_field(&schema, "negzero", "FLOAT", "NULLABLE"));
}

#[test]
fn test_float_very_small() {
    let (schema, _) = generate_schema(&[json!({"tiny": 1e-300})], GeneratorConfig::default());

    assert!(has_field(&schema, "tiny", "FLOAT", "NULLABLE"));
}

#[test]
fn test_float_very_large() {
    let (schema, _) = generate_schema(&[json!({"huge": 1e300})], GeneratorConfig::default());

    assert!(has_field(&schema, "huge", "FLOAT", "NULLABLE"));
}

// =============================================================================
// DATE/TIME EDGE CASES
// =============================================================================

#[test]
fn test_date_boundary_values() {
    let (schema, _) = generate_schema(
        &[json!({
            "min_date": "0001-01-01",
            "max_date": "9999-12-31",
            "leap_day": "2024-02-29"
        })],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "min_date", "DATE", "NULLABLE"));
    assert!(has_field(&schema, "max_date", "DATE", "NULLABLE"));
    assert!(has_field(&schema, "leap_day", "DATE", "NULLABLE"));
}

#[test]
fn test_time_boundary_values() {
    let (schema, _) = generate_schema(
        &[json!({
            "midnight": "00:00:00",
            "almost_midnight": "23:59:59",
            "with_micros": "12:34:56.789012"
        })],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "midnight", "TIME", "NULLABLE"));
    assert!(has_field(&schema, "almost_midnight", "TIME", "NULLABLE"));
    assert!(has_field(&schema, "with_micros", "TIME", "NULLABLE"));
}

#[test]
fn test_timestamp_various_formats() {
    let (schema, _) = generate_schema(
        &[json!({
            "ts_z": "2024-01-15T10:30:00Z",
            "ts_utc": "2024-01-15 10:30:00 UTC",
            "ts_offset_positive": "2024-01-15T10:30:00+05:30",
            "ts_offset_negative": "2024-01-15T10:30:00-08:00",
            "ts_with_micros": "2024-01-15T10:30:00.123456Z"
        })],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "ts_z", "TIMESTAMP", "NULLABLE"));
    assert!(has_field(&schema, "ts_utc", "TIMESTAMP", "NULLABLE"));
    assert!(has_field(
        &schema,
        "ts_offset_positive",
        "TIMESTAMP",
        "NULLABLE"
    ));
    assert!(has_field(
        &schema,
        "ts_offset_negative",
        "TIMESTAMP",
        "NULLABLE"
    ));
    assert!(has_field(
        &schema,
        "ts_with_micros",
        "TIMESTAMP",
        "NULLABLE"
    ));
}

// =============================================================================
// ARRAY EDGE CASES
// =============================================================================

#[test]
fn test_array_with_single_element() {
    let (schema, _) = generate_schema(&[json!({"single": [42]})], GeneratorConfig::default());

    assert!(has_field(&schema, "single", "INTEGER", "REPEATED"));
}

#[test]
fn test_array_with_nulls_only() {
    let (schema, _errors) = generate_schema(
        &[json!({"nulls": [null, null, null]})],
        GeneratorConfig::default(),
    );

    // Array of nulls is not supported
    assert!(!has_field(&schema, "nulls", "STRING", "REPEATED"));
}

#[test]
fn test_empty_array_then_populated() {
    let (schema, _) = generate_schema(
        &[json!({"arr": []}), json!({"arr": [1, 2, 3]})],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "arr", "INTEGER", "REPEATED"));
}

#[test]
fn test_populated_array_then_empty() {
    let (schema, _) = generate_schema(
        &[json!({"arr": [1, 2, 3]}), json!({"arr": []})],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "arr", "INTEGER", "REPEATED"));
}

// =============================================================================
// RECORD EDGE CASES
// =============================================================================

#[test]
fn test_empty_record_keeps_type() {
    let config = GeneratorConfig {
        keep_nulls: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(&[json!({"empty": {}})], config);

    assert!(has_field(&schema, "empty", "RECORD", "NULLABLE"));
}

#[test]
fn test_record_with_only_nulls() {
    let (schema, _) = generate_schema(
        &[json!({"rec": {"a": null, "b": null}})],
        GeneratorConfig::default(),
    );

    // Record with only nulls should be handled
    assert!(has_field(&schema, "rec", "RECORD", "NULLABLE"));
}

#[test]
fn test_array_of_empty_records() {
    let config = GeneratorConfig {
        keep_nulls: true,
        ..Default::default()
    };
    let (schema, _) = generate_schema(&[json!({"recs": [{}, {}, {}]})], config);

    assert!(has_field(&schema, "recs", "RECORD", "REPEATED"));
}

// =============================================================================
// MULTIPLE RECORD EDGE CASES
// =============================================================================

#[test]
fn test_first_record_empty_second_has_data() {
    let (schema, _) = generate_schema(
        &[json!({}), json!({"name": "test", "value": 42})],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "value", "INTEGER", "NULLABLE"));
}

#[test]
fn test_alternating_field_presence() {
    let (schema, _) = generate_schema(
        &[
            json!({"a": 1}),
            json!({"b": 2}),
            json!({"a": 3}),
            json!({"b": 4}),
            json!({"a": 5, "b": 6}),
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "a", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "b", "INTEGER", "NULLABLE"));
}

#[test]
fn test_type_conflict_then_continue() {
    let (schema, errors) = generate_schema(
        &[
            json!({"field": "string"}),
            json!({"field": 123}), // Conflict!
            json!({"other": "still works"}),
        ],
        GeneratorConfig::default(),
    );

    assert!(!errors.is_empty());
    assert!(has_field(&schema, "other", "STRING", "NULLABLE"));
}

// =============================================================================
// STRING EDGE CASES
// =============================================================================

#[test]
fn test_empty_string() {
    let (schema, _) = generate_schema(&[json!({"empty": ""})], GeneratorConfig::default());

    assert!(has_field(&schema, "empty", "STRING", "NULLABLE"));
}

#[test]
fn test_whitespace_only_string() {
    let (schema, _) = generate_schema(&[json!({"ws": "   \t\n  "})], GeneratorConfig::default());

    assert!(has_field(&schema, "ws", "STRING", "NULLABLE"));
}

#[test]
fn test_string_that_looks_like_date_but_invalid() {
    let (schema, _) = generate_schema(
        &[json!({
            "not_date_1": "2024-13-01",  // Invalid month
            "not_date_2": "2024-00-15",  // Invalid month
            "not_date_3": "2024-01-32",  // Invalid day
            "not_date_4": "24-01-15"     // Two-digit year
        })],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "not_date_1", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "not_date_2", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "not_date_3", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "not_date_4", "STRING", "NULLABLE"));
}

#[test]
fn test_string_that_looks_like_number_but_has_leading_zeros() {
    let (schema, _) = generate_schema(&[json!({"num": "007"})], GeneratorConfig::default());

    // "007" should still be inferred as INTEGER (leading zeros ignored in regex)
    // Actually let's check the Python behavior - leading zeros should be INTEGER
    assert!(has_field(&schema, "num", "INTEGER", "NULLABLE"));
}

// =============================================================================
// BOOLEAN EDGE CASES
// =============================================================================

#[test]
fn test_boolean_true_variations() {
    let (schema, _) = generate_schema(
        &[
            json!({"b1": true}),
            json!({"b1": "true"}),
            json!({"b1": "True"}),
            json!({"b1": "TRUE"}),
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "b1", "BOOLEAN", "NULLABLE"));
}

#[test]
fn test_boolean_false_variations() {
    let (schema, _) = generate_schema(
        &[
            json!({"b1": false}),
            json!({"b1": "false"}),
            json!({"b1": "False"}),
            json!({"b1": "FALSE"}),
        ],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "b1", "BOOLEAN", "NULLABLE"));
}

// =============================================================================
// MIXED TYPE ARRAYS
// =============================================================================

#[test]
fn test_array_int_and_float_mixed() {
    let (schema, _) = generate_schema(
        &[json!({"nums": [1, 2.5, 3, 4.5]})],
        GeneratorConfig::default(),
    );

    // Should upgrade to FLOAT
    assert!(has_field(&schema, "nums", "FLOAT", "REPEATED"));
}

#[test]
fn test_array_bool_and_string_incompatible() {
    let (schema, errors) = generate_schema(
        &[json!({"mixed": [true, "text"]})],
        GeneratorConfig::default(),
    );

    // Should produce error
    assert!(!errors.is_empty());
    assert!(!has_field(&schema, "mixed", "STRING", "REPEATED"));
}

#[test]
fn test_array_int_and_bool_incompatible() {
    let (_schema, errors) =
        generate_schema(&[json!({"mixed": [1, true]})], GeneratorConfig::default());

    assert!(!errors.is_empty());
}

// =============================================================================
// FIELD NAME EDGE CASES
// =============================================================================

#[test]
fn test_numeric_field_name() {
    let (schema, _) = generate_schema(&[json!({"123": "value"})], GeneratorConfig::default());

    assert!(schema.iter().any(|f| f["name"] == "123"));
}

#[test]
fn test_field_name_with_underscore() {
    let (schema, _) = generate_schema(
        &[json!({"_private": 1, "__dunder__": 2})],
        GeneratorConfig::default(),
    );

    assert!(schema.iter().any(|f| f["name"] == "_private"));
    assert!(schema.iter().any(|f| f["name"] == "__dunder__"));
}

#[test]
fn test_case_sensitivity_field_names() {
    let (schema, _) = generate_schema(
        &[
            json!({"Field": "value1"}),
            json!({"field": "value2"}), // Same field, different case
        ],
        GeneratorConfig::default(),
    );

    // Should merge into one field (case-insensitive matching)
    assert_eq!(schema.len(), 1);
}

// =============================================================================
// REGRESSION TESTS
// =============================================================================

#[test]
fn test_issue_empty_record_in_array_of_records() {
    let (schema, _) = generate_schema(
        &[json!({"items": [{"name": "a"}, {}, {"name": "b"}]})],
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "items", "RECORD", "REPEATED"));
    let items = schema.iter().find(|f| f["name"] == "items").unwrap();
    let fields = items["fields"].as_array().unwrap();
    assert!(!fields.is_empty());
}

#[test]
fn test_issue_type_conflict_only_logs_once() {
    let (_, errors) = generate_schema(
        &[
            json!({"ts": "2017-05-22T17:10:00-07:00"}),
            json!({"ts": 1.0}),
            json!({"ts": 2.0}),
            json!({"ts": "2017-05-22T17:10:00-07:00"}),
        ],
        GeneratorConfig::default(),
    );

    // Should only have one error for the first mismatch
    let ts_errors: Vec<_> = errors.iter().filter(|e| e.contains("ts")).collect();
    assert_eq!(ts_errors.len(), 1);
}

// =============================================================================
// EXISTING SCHEMA TESTS
// =============================================================================

/// Helper to generate schema from JSON values with an existing schema
fn generate_schema_with_existing(
    records: &[Value],
    existing_schema: SchemaMap,
    config: GeneratorConfig,
) -> (Vec<Value>, Vec<String>) {
    let mut generator = SchemaGenerator::new(config);
    let mut schema_map = existing_schema;

    for record in records {
        let _ = generator.process_record(record, &mut schema_map);
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

#[test]
fn test_existing_schema_preserves_fields() {
    // Create an existing schema with a field
    let existing = json!([
        {"name": "existing_field", "type": "STRING", "mode": "NULLABLE"}
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // Process records with a new field
    let (schema, _) = generate_schema_with_existing(
        &[json!({"new_field": 42})],
        existing_schema,
        GeneratorConfig::default(),
    );

    // Both fields should be present
    assert!(has_field(&schema, "existing_field", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "new_field", "INTEGER", "NULLABLE"));
}

#[test]
fn test_existing_schema_type_merge() {
    // Existing schema has INTEGER
    let existing = json!([
        {"name": "value", "type": "INTEGER", "mode": "NULLABLE"}
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // New data has FLOAT - should upgrade to FLOAT
    let (schema, _) = generate_schema_with_existing(
        &[json!({"value": 3.5})],
        existing_schema,
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "value", "FLOAT", "NULLABLE"));
}

#[test]
fn test_existing_schema_nested_record() {
    // Existing schema has a nested record
    let existing = json!([
        {
            "name": "user",
            "type": "RECORD",
            "mode": "NULLABLE",
            "fields": [
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}
            ]
        }
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // New data adds a field to the nested record
    let (schema, _) = generate_schema_with_existing(
        &[json!({"user": {"name": "test", "age": 25}})],
        existing_schema,
        GeneratorConfig::default(),
    );

    let user = schema.iter().find(|f| f["name"] == "user").unwrap();
    let fields = user["fields"].as_array().unwrap();

    // Both fields should be present
    assert!(fields
        .iter()
        .any(|f| f["name"] == "name" && f["type"] == "STRING"));
    assert!(fields
        .iter()
        .any(|f| f["name"] == "age" && f["type"] == "INTEGER"));
}

#[test]
fn test_existing_schema_required_mode() {
    // Existing schema has REQUIRED mode
    let existing = json!([
        {"name": "id", "type": "INTEGER", "mode": "REQUIRED"}
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // New data provides the field
    let (schema, _) = generate_schema_with_existing(
        &[json!({"id": 123})],
        existing_schema,
        GeneratorConfig::default(),
    );

    // Mode should be preserved (stays at whatever existing had)
    assert!(has_field(&schema, "id", "INTEGER", "REQUIRED"));
}

#[test]
fn test_existing_schema_repeated_mode() {
    // Existing schema has REPEATED mode
    let existing = json!([
        {"name": "tags", "type": "STRING", "mode": "REPEATED"}
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // New data provides an array
    let (schema, _) = generate_schema_with_existing(
        &[json!({"tags": ["a", "b", "c"]})],
        existing_schema,
        GeneratorConfig::default(),
    );

    assert!(has_field(&schema, "tags", "STRING", "REPEATED"));
}

#[test]
fn test_existing_schema_type_aliases() {
    // Existing schema uses type aliases (INT64, FLOAT64, BOOL, STRUCT)
    let existing = json!([
        {"name": "a", "type": "INT64", "mode": "NULLABLE"},
        {"name": "b", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "c", "type": "BOOL", "mode": "NULLABLE"},
        {"name": "d", "type": "STRUCT", "mode": "NULLABLE", "fields": [
            {"name": "x", "type": "STRING", "mode": "NULLABLE"}
        ]}
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // Process empty data, just to flatten the existing schema
    let (schema, _) =
        generate_schema_with_existing(&[], existing_schema, GeneratorConfig::default());

    // Types should be normalized to BigQuery legacy names
    assert!(has_field(&schema, "a", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "b", "FLOAT", "NULLABLE"));
    assert!(has_field(&schema, "c", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "d", "RECORD", "NULLABLE"));
}

#[test]
fn test_existing_schema_empty_records_get_data() {
    // Existing schema has only field definitions
    let existing = json!([
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "age", "type": "INTEGER", "mode": "NULLABLE"}
    ]);
    let existing_schema = bq_schema_to_map(&existing).unwrap();

    // Process records that add more fields
    let (schema, _) = generate_schema_with_existing(
        &[
            json!({"name": "Alice", "age": 30, "city": "NYC"}),
            json!({"name": "Bob", "city": "LA", "active": true}),
        ],
        existing_schema,
        GeneratorConfig::default(),
    );

    // All fields should be present
    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "age", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "city", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "active", "BOOLEAN", "NULLABLE"));
}
