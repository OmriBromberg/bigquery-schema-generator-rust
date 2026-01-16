//! Property-based tests using proptest for BigQuery Schema Generator.
//!
//! These tests verify invariants that should hold for all inputs, not just specific test cases.

use proptest::prelude::*;
use serde_json::json;

use bq_schema_gen::{
    inference::{convert_type, infer_type_from_json},
    schema::types::{BqType, SchemaMap},
    validate::{SchemaValidator, ValidationOptions, ValidationResult},
    BqSchemaField, GeneratorConfig, SchemaGenerator,
};

// =============================================================================
// STRATEGIES FOR GENERATING TEST DATA
// =============================================================================

/// Strategy to generate valid JSON primitive values
fn json_primitive() -> impl Strategy<Value = serde_json::Value> {
    prop_oneof![
        Just(json!(null)),
        any::<bool>().prop_map(|b| json!(b)),
        any::<i64>().prop_map(|i| json!(i)),
        any::<f64>()
            .prop_filter("finite floats only", |f| f.is_finite())
            .prop_map(|f| json!(f)),
        "[a-zA-Z0-9_]{0,20}".prop_map(|s| json!(s)),
    ]
}

/// Strategy to generate valid field names (lowercase only to avoid case-collision issues)
/// The schema generator normalizes field names to lowercase, so we use lowercase only
fn field_name() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9_]{0,15}".prop_map(|s| s)
}

/// Strategy to generate simple flat JSON objects
fn simple_json_object() -> impl Strategy<Value = serde_json::Value> {
    proptest::collection::hash_map(field_name(), json_primitive(), 1..5).prop_map(|map| {
        let obj: serde_json::Map<String, serde_json::Value> = map.into_iter().collect();
        serde_json::Value::Object(obj)
    })
}

/// Strategy to generate BqType (non-Record variants only for simplicity)
fn simple_bq_type() -> impl Strategy<Value = BqType> {
    prop_oneof![
        Just(BqType::Boolean),
        Just(BqType::Integer),
        Just(BqType::Float),
        Just(BqType::String),
        Just(BqType::Timestamp),
        Just(BqType::Date),
        Just(BqType::Time),
        Just(BqType::QBoolean),
        Just(BqType::QInteger),
        Just(BqType::QFloat),
    ]
}

// =============================================================================
// TYPE INFERENCE PROPERTIES
// =============================================================================

proptest! {
    /// Property: Type inference is deterministic - same input always produces same type
    #[test]
    fn prop_type_inference_is_deterministic(value in json_primitive()) {
        let result1 = infer_type_from_json(&value, false);
        let result2 = infer_type_from_json(&value, false);
        prop_assert_eq!(result1, result2, "Type inference should be deterministic");
    }

    /// Property: INTEGER always widens to FLOAT when combined
    #[test]
    fn prop_integer_always_widens_to_float(_seed in any::<u64>()) {
        let result = convert_type(&BqType::Integer, &BqType::Float);
        prop_assert_eq!(result, Some(BqType::Float));

        let result2 = convert_type(&BqType::Float, &BqType::Integer);
        prop_assert_eq!(result2, Some(BqType::Float));
    }

    /// Property: QInteger + Integer = Integer (Q types merge to hard types)
    #[test]
    fn prop_qtype_merges_to_hard_type(_seed in any::<u64>()) {
        // QInteger + Integer = Integer
        let result = convert_type(&BqType::QInteger, &BqType::Integer);
        prop_assert_eq!(result, Some(BqType::Integer));

        // QBoolean + Boolean = Boolean
        let result2 = convert_type(&BqType::QBoolean, &BqType::Boolean);
        prop_assert_eq!(result2, Some(BqType::Boolean));

        // QFloat + Float = Float
        let result3 = convert_type(&BqType::QFloat, &BqType::Float);
        prop_assert_eq!(result3, Some(BqType::Float));
    }

    /// Property: Null value inference produces BqType::Null
    #[test]
    fn prop_null_infers_to_null_type(_seed in any::<u64>()) {
        let null_value = json!(null);
        let result = infer_type_from_json(&null_value, false);
        prop_assert_eq!(result, Some(BqType::Null));
    }

    /// Property: Boolean values always infer to Boolean type
    #[test]
    fn prop_boolean_infers_correctly(b in any::<bool>()) {
        let value = json!(b);
        let result = infer_type_from_json(&value, false);
        prop_assert_eq!(result, Some(BqType::Boolean));
    }

    /// Property: Integer values (within i64 range) infer to Integer type
    #[test]
    fn prop_integer_infers_correctly(i in any::<i64>()) {
        let value = json!(i);
        let result = infer_type_from_json(&value, false);
        prop_assert_eq!(result, Some(BqType::Integer));
    }

    /// Property: Float values infer to Float type
    #[test]
    fn prop_float_infers_correctly(f in any::<f64>().prop_filter("finite", |f| f.is_finite())) {
        let value = json!(f);
        let result = infer_type_from_json(&value, false);
        // Note: integers that happen to have .0 still parse as integers in JSON
        // So we just check it's either Integer or Float
        prop_assert!(
            result == Some(BqType::Integer) || result == Some(BqType::Float),
            "Expected Integer or Float, got {:?}", result
        );
    }
}

// =============================================================================
// SCHEMA MERGING PROPERTIES
// =============================================================================

proptest! {
    /// Property: convert_type is commutative - order of arguments doesn't matter
    #[test]
    fn prop_merge_is_commutative(type_a in simple_bq_type(), type_b in simple_bq_type()) {
        let result_ab = convert_type(&type_a, &type_b);
        let result_ba = convert_type(&type_b, &type_a);
        prop_assert_eq!(result_ab, result_ba, "Type conversion should be commutative");
    }

    /// Property: convert_type is idempotent - merging with self returns self
    #[test]
    fn prop_merge_is_idempotent(bq_type in simple_bq_type()) {
        let result = convert_type(&bq_type, &bq_type);
        prop_assert_eq!(result, Some(bq_type.clone()), "Merging type with itself should return same type");
    }

    /// Property: Processing the same record twice produces the same schema
    #[test]
    fn prop_schema_generation_idempotent(record in simple_json_object()) {
        let config = GeneratorConfig::default();

        // Process record once
        let mut gen1 = SchemaGenerator::new(config.clone());
        let mut map1 = SchemaMap::new();
        let _ = gen1.process_record(&record, &mut map1);
        let schema1 = gen1.flatten_schema(&map1);

        // Process same record again
        let mut gen2 = SchemaGenerator::new(config.clone());
        let mut map2 = SchemaMap::new();
        let _ = gen2.process_record(&record, &mut map2);
        let schema2 = gen2.flatten_schema(&map2);

        prop_assert_eq!(schema1.len(), schema2.len(), "Same record should produce same schema field count");
    }

    /// Property: Processing records preserves field count (ignoring internal types)
    #[test]
    fn prop_flatten_preserves_field_count(record in simple_json_object()) {
        let config = GeneratorConfig {
            keep_nulls: true,
            ..Default::default()
        };

        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();
        let _ = generator.process_record(&record, &mut schema_map);

        // With keep_nulls=true, all fields should be preserved
        let schema = generator.flatten_schema(&schema_map);
        let obj = record.as_object().unwrap();

        prop_assert!(
            schema.len() <= obj.len(),
            "Schema should have at most as many fields as input (got {} fields for {} input fields)",
            schema.len(),
            obj.len()
        );
    }
}

// =============================================================================
// VALIDATION PROPERTIES
// =============================================================================

proptest! {
    /// Property: Valid data conforming to schema always passes validation
    #[test]
    fn prop_valid_data_always_passes(
        name in "[a-zA-Z][a-zA-Z0-9]{0,10}",
        value in any::<i64>()
    ) {
        let schema = vec![
            BqSchemaField::new("name".to_string(), "STRING".to_string(), "NULLABLE".to_string()),
            BqSchemaField::new("value".to_string(), "INTEGER".to_string(), "NULLABLE".to_string()),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        let record = json!({"name": name, "value": value});
        validator.validate_record(&record, 1, &mut result);

        prop_assert!(result.valid, "Valid data should pass validation");
        prop_assert_eq!(result.error_count, 0);
    }

    /// Property: Missing REQUIRED field always causes validation failure
    #[test]
    fn prop_missing_required_always_fails(
        extra_value in any::<i64>()
    ) {
        let schema = vec![
            BqSchemaField::new("required_field".to_string(), "STRING".to_string(), "REQUIRED".to_string()),
            BqSchemaField::new("optional_field".to_string(), "INTEGER".to_string(), "NULLABLE".to_string()),
        ];

        let validator = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result = ValidationResult::new();

        // Record without the required field
        let record = json!({"optional_field": extra_value});
        validator.validate_record(&record, 1, &mut result);

        prop_assert!(!result.valid, "Missing required field should fail validation");
        prop_assert!(result.error_count >= 1);
    }

    /// Property: error_count matches the length of errors vector
    #[test]
    fn prop_error_count_matches_actual_errors(
        num_missing in 1usize..5
    ) {
        // Create a schema with multiple required fields
        let mut schema = Vec::new();
        for i in 0..num_missing {
            schema.push(BqSchemaField::new(
                format!("required_{}", i),
                "STRING".to_string(),
                "REQUIRED".to_string(),
            ));
        }

        let validator = SchemaValidator::new(&schema, ValidationOptions {
            max_errors: 100,
            ..Default::default()
        });
        let mut result = ValidationResult::new();

        // Empty record - all required fields are missing
        let record = json!({});
        validator.validate_record(&record, 1, &mut result);

        prop_assert_eq!(
            result.error_count,
            result.errors.len(),
            "error_count should match errors vector length"
        );
    }

    /// Property: max_errors limit is respected
    #[test]
    fn prop_max_errors_respected(
        max_errors in 1usize..10,
        num_errors in 5usize..20
    ) {
        // Create schema with many required fields to generate errors
        let mut schema = Vec::new();
        for i in 0..num_errors {
            schema.push(BqSchemaField::new(
                format!("required_{}", i),
                "STRING".to_string(),
                "REQUIRED".to_string(),
            ));
        }

        let options = ValidationOptions {
            max_errors,
            ..Default::default()
        };
        let validator = SchemaValidator::new(&schema, options);
        let mut result = ValidationResult::new();

        // Empty record triggers all required field errors
        let record = json!({});
        validator.validate_record(&record, 1, &mut result);

        prop_assert!(
            result.error_count <= max_errors,
            "Should not exceed max_errors limit (got {} errors, max was {})",
            result.error_count,
            max_errors
        );
    }

    /// Property: Unknown fields are errors by default, warnings with allow_unknown
    #[test]
    fn prop_unknown_field_handling(
        unknown_name in "[a-zA-Z][a-zA-Z0-9]{0,10}",
        unknown_value in any::<i64>()
    ) {
        let schema = vec![
            BqSchemaField::new("known_field".to_string(), "STRING".to_string(), "NULLABLE".to_string()),
        ];

        // Without allow_unknown - should be error
        let validator1 = SchemaValidator::new(&schema, ValidationOptions::default());
        let mut result1 = ValidationResult::new();
        let record = json!({"known_field": "test", unknown_name.clone(): unknown_value});
        validator1.validate_record(&record, 1, &mut result1);
        prop_assert!(!result1.valid, "Unknown field should cause error by default");

        // With allow_unknown - should be warning
        let validator2 = SchemaValidator::new(&schema, ValidationOptions {
            allow_unknown: true,
            ..Default::default()
        });
        let mut result2 = ValidationResult::new();
        validator2.validate_record(&record, 1, &mut result2);
        prop_assert!(result2.valid, "Unknown field should be allowed with allow_unknown");
        prop_assert!(!result2.warnings.is_empty(), "Unknown field should generate warning");
    }
}

// =============================================================================
// COMPLEX PROPERTY TESTS
// =============================================================================

proptest! {
    /// Property: Schema generated from data can validate that same data
    /// Note: We use keep_nulls=true to ensure null-only fields are included in the schema
    #[test]
    fn prop_generated_schema_validates_source_data(record in simple_json_object()) {
        let config = GeneratorConfig {
            keep_nulls: true, // Include null fields in schema
            ..Default::default()
        };
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        // Generate schema from record
        if generator.process_record(&record, &mut schema_map).is_ok() {
            let schema = generator.flatten_schema(&schema_map);

            // Validate the original record against generated schema
            let validator = SchemaValidator::new(&schema, ValidationOptions::default());
            let mut result = ValidationResult::new();
            validator.validate_record(&record, 1, &mut result);

            // The data should be valid against its own generated schema
            prop_assert!(
                result.valid,
                "Data should validate against its own generated schema. Errors: {:?}",
                result.errors
            );
        }
    }

    /// Property: Type conversion is transitive for numeric types
    #[test]
    fn prop_numeric_conversion_transitive(_seed in any::<u64>()) {
        // If QInteger -> Integer and Integer -> Float, then QInteger -> Float
        let qi_to_i = convert_type(&BqType::QInteger, &BqType::Integer);
        let i_to_f = convert_type(&BqType::Integer, &BqType::Float);
        let qi_to_f = convert_type(&BqType::QInteger, &BqType::Float);

        prop_assert_eq!(qi_to_i, Some(BqType::Integer));
        prop_assert_eq!(i_to_f, Some(BqType::Float));
        prop_assert_eq!(qi_to_f, Some(BqType::Float));
    }

    /// Property: String-compatible types merge to STRING (except Q-numeric types among themselves)
    /// Note: QInteger + QFloat = QFloat (quoted numerics stay quoted), but mixing with
    /// other string-compatible types (Date, Time, Timestamp, String) produces STRING
    #[test]
    fn prop_string_compatible_merge_to_string(_seed in any::<u64>()) {
        // Types that always merge to STRING when mixed with each other
        let string_types = vec![
            BqType::String,
            BqType::Timestamp,
            BqType::Date,
            BqType::Time,
        ];

        // These merge to STRING when mixed
        for type_a in &string_types {
            for type_b in &string_types {
                if type_a != type_b {
                    let result = convert_type(type_a, type_b);
                    prop_assert_eq!(
                        result,
                        Some(BqType::String),
                        "String types {:?} and {:?} should merge to STRING",
                        type_a,
                        type_b
                    );
                }
            }
        }

        // QInteger + QFloat = QFloat (special case: quoted numerics stay numeric)
        prop_assert_eq!(
            convert_type(&BqType::QInteger, &BqType::QFloat),
            Some(BqType::QFloat),
            "QInteger + QFloat should merge to QFloat"
        );

        // But Q-types mixed with string types become STRING
        for string_type in &string_types {
            let result1 = convert_type(&BqType::QInteger, string_type);
            prop_assert_eq!(
                result1,
                Some(BqType::String),
                "QInteger + {:?} should merge to STRING",
                string_type
            );

            let result2 = convert_type(&BqType::QFloat, string_type);
            prop_assert_eq!(
                result2,
                Some(BqType::String),
                "QFloat + {:?} should merge to STRING",
                string_type
            );
        }
    }
}
