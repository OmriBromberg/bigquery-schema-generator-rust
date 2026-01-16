//! CSV-specific tests for BigQuery Schema Generator

use bq_schema_gen::{generate_schema_from_csv, GeneratorConfig, InputFormat};
use serde_json::Value;
use std::io::Cursor;

/// Helper to generate schema from CSV string
fn generate_csv_schema(csv_data: &str, config: GeneratorConfig) -> Vec<Value> {
    let cursor = Cursor::new(csv_data);
    let mut output = Vec::new();

    generate_schema_from_csv(cursor, &mut output, config, None, None).unwrap();

    let output_str = String::from_utf8(output).unwrap();
    serde_json::from_str(&output_str).unwrap()
}

/// Helper to check if schema contains a field with given properties
fn has_field(schema: &[Value], name: &str, field_type: &str, mode: &str) -> bool {
    schema
        .iter()
        .any(|f| f["name"] == name && f["type"] == field_type && f["mode"] == mode)
}

// =============================================================================
// BASIC CSV TESTS
// =============================================================================

#[test]
fn test_simple_csv() {
    let csv = "name,surname,age\nJohn,Smith,23\nMichael,Johnson,27";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    assert_eq!(schema.len(), 3);
    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "surname", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "age", "INTEGER", "NULLABLE"));
}

#[test]
fn test_csv_with_empty_values() {
    let csv = "name,surname,age\nJohn\nMichael,,\nMaria,Smith,30";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "surname", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "age", "INTEGER", "NULLABLE"));
}

#[test]
fn test_csv_type_inference() {
    let csv = "str,int,float,bool,date,time,timestamp\n\
               hello,42,3.14,true,2024-01-15,10:30:00,2024-01-15T10:30:00Z";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    assert!(has_field(&schema, "str", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "int", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "float", "FLOAT", "NULLABLE"));
    assert!(has_field(&schema, "bool", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "date", "DATE", "NULLABLE"));
    assert!(has_field(&schema, "time", "TIME", "NULLABLE"));
    assert!(has_field(&schema, "timestamp", "TIMESTAMP", "NULLABLE"));
}

// =============================================================================
// CSV INFER MODE TESTS
// =============================================================================

#[test]
fn test_csv_infer_mode_required() {
    let csv = "a,b,c\n1,hello,true\n2,world,false";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        infer_mode: true,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // All fields are filled, so should be REQUIRED
    assert!(has_field(&schema, "a", "INTEGER", "REQUIRED"));
    assert!(has_field(&schema, "b", "STRING", "REQUIRED"));
    assert!(has_field(&schema, "c", "BOOLEAN", "REQUIRED"));
}

#[test]
fn test_csv_infer_mode_mixed() {
    let csv = "a,b,c\n,ho,hi\n3,hu,he";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        infer_mode: true,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // a has empty value, so NULLABLE; b and c are filled, so REQUIRED
    assert!(has_field(&schema, "a", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "b", "STRING", "REQUIRED"));
    assert!(has_field(&schema, "c", "STRING", "REQUIRED"));
}

// =============================================================================
// CSV PRESERVE ORDER TESTS
// =============================================================================

#[test]
fn test_csv_preserves_column_order() {
    let csv = "z_col,a_col,m_col\n1,2,3";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // CSV should preserve header order
    assert_eq!(schema[0]["name"], "z_col");
    assert_eq!(schema[1]["name"], "a_col");
    assert_eq!(schema[2]["name"], "m_col");
}

// =============================================================================
// CSV EDGE CASES
// =============================================================================

#[test]
fn test_csv_with_quoted_values() {
    let csv =
        "name,description\ntest,\"This is a, quoted value\"\nfoo,\"Another \"\"quoted\"\" value\"";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "description", "STRING", "NULLABLE"));
}

#[test]
fn test_csv_with_numeric_strings() {
    let csv = "id,count\n001,100\n002,200";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // "001" should be inferred as INTEGER (leading zeros stripped in inference)
    assert!(has_field(&schema, "id", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "count", "INTEGER", "NULLABLE"));
}

#[test]
fn test_csv_type_progression() {
    // First row has integer, second has float
    let csv = "value\n42\n3.14";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // Should upgrade to FLOAT
    assert!(has_field(&schema, "value", "FLOAT", "NULLABLE"));
}

#[test]
fn test_csv_all_empty_column() {
    let csv = "a,b,c\n1,,x\n2,,y\n3,,z";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // Empty column should still be in schema (as STRING due to keep_nulls being true for CSV)
    assert!(has_field(&schema, "b", "STRING", "NULLABLE"));
}

#[test]
fn test_csv_boolean_variations() {
    let csv = "b1,b2,b3,b4\ntrue,false,True,FALSE";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    assert!(has_field(&schema, "b1", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "b2", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "b3", "BOOLEAN", "NULLABLE"));
    assert!(has_field(&schema, "b4", "BOOLEAN", "NULLABLE"));
}

#[test]
fn test_csv_many_rows() {
    let mut csv = String::from("id,value\n");
    for i in 0..1000 {
        csv.push_str(&format!("{},{}\n", i, i * 2));
    }

    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(&csv, config);

    assert!(has_field(&schema, "id", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "value", "INTEGER", "NULLABLE"));
}

#[test]
fn test_csv_with_sanitize_names() {
    let csv = "field-name,field.with.dots,normal_field\n1,2,3";
    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        sanitize_names: true,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    // Names should be sanitized
    assert!(schema.iter().all(|f| {
        let name = f["name"].as_str().unwrap();
        !name.contains('-') && !name.contains('.')
    }));
}

#[test]
fn test_csv_complex_type_inference() {
    let csv = "name,surname,age,is_student,registration_date,score\n\
               John\n\
               Michael,Johnson,27,True,,2.0\n\
               Maria,\"\",,false,2019-02-26 13:22:00 UTC,\n\
               Joanna,Anders,21,\"False\",2019-02-26 13:23:00,4";

    let config = GeneratorConfig {
        input_format: InputFormat::Csv,
        ..Default::default()
    };

    let schema = generate_csv_schema(csv, config);

    assert!(has_field(&schema, "name", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "surname", "STRING", "NULLABLE"));
    assert!(has_field(&schema, "age", "INTEGER", "NULLABLE"));
    assert!(has_field(&schema, "is_student", "BOOLEAN", "NULLABLE"));
    assert!(has_field(
        &schema,
        "registration_date",
        "TIMESTAMP",
        "NULLABLE"
    ));
    assert!(has_field(&schema, "score", "FLOAT", "NULLABLE"));
}
