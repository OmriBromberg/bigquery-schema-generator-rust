//! Type inference for BigQuery schema generation.
//!
//! This module handles inferring BigQuery types from JSON/CSV values,
//! matching the behavior of the Python `bigquery-schema-generator`.

use once_cell::sync::Lazy;
use regex::Regex;

use crate::schema::types::{BqMode, BqType};

/// Maximum signed 64-bit integer value supported by BigQuery.
pub const INTEGER_MAX_VALUE: i64 = i64::MAX; // 2^63 - 1 = 9223372036854775807

/// Minimum signed 64-bit integer value supported by BigQuery.
pub const INTEGER_MIN_VALUE: i64 = i64::MIN; // -2^63 = -9223372036854775808

// Regex patterns matching the Python implementation exactly

/// Detect a TIMESTAMP field of the form:
/// `YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]`
static TIMESTAMP_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        r"^\d{4}-\d{1,2}-\d{1,2}[T ]\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})? *(([+-]\d{1,2}(:\d{1,2})?)|Z|UTC)?$"
    ).unwrap()
});

/// Detect a DATE field of the form `YYYY-[M]M-[D]D`.
static DATE_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\d{4}-(?:[1-9]|0[1-9]|1[012])-(?:[1-9]|0[1-9]|[12][0-9]|3[01])$").unwrap()
});

/// Detect a TIME field of the form `[H]H:[M]M:[S]S[.DDDDDD]`
static TIME_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^\d{1,2}:\d{1,2}:\d{1,2}(\.\d{1,6})?$").unwrap());

/// Detect integers inside quotes.
static INTEGER_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"^[-+]?\d+$").unwrap());

/// Detect floats inside quotes.
static FLOAT_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[-+]?(?:\d+\.?\d*|\.\d+)(?:[eE][-+]?\d+)?$").unwrap());

/// Check if a string matches the TIMESTAMP pattern.
pub fn is_timestamp(s: &str) -> bool {
    TIMESTAMP_REGEX.is_match(s)
}

/// Check if a string matches the DATE pattern.
pub fn is_date(s: &str) -> bool {
    DATE_REGEX.is_match(s)
}

/// Check if a string matches the TIME pattern.
pub fn is_time(s: &str) -> bool {
    TIME_REGEX.is_match(s)
}

/// Check if a string matches the INTEGER pattern.
pub fn is_integer_string(s: &str) -> bool {
    INTEGER_REGEX.is_match(s)
}

/// Check if a string matches the FLOAT pattern.
pub fn is_float_string(s: &str) -> bool {
    FLOAT_REGEX.is_match(s)
}

/// Check if a string represents a boolean value.
pub fn is_boolean_string(s: &str) -> bool {
    let lower = s.to_lowercase();
    lower == "true" || lower == "false"
}

/// Infer the BigQuery type from a serde_json Value.
///
/// Returns the inferred type. For arrays, returns `None` if the array
/// contains incompatible types.
///
/// The `quoted_values_are_strings` parameter controls whether quoted values
/// like `"123"` should be inferred as their actual types or kept as STRING.
pub fn infer_type_from_json(
    value: &serde_json::Value,
    quoted_values_are_strings: bool,
) -> Option<BqType> {
    match value {
        serde_json::Value::Null => Some(BqType::Null),
        serde_json::Value::Bool(_) => Some(BqType::Boolean),
        serde_json::Value::Number(n) => {
            if n.is_i64() {
                // i64 values are always within BigQuery INTEGER range (INT64)
                Some(BqType::Integer)
            } else if n.is_u64() {
                let u = n.as_u64().unwrap();
                // u64 values > i64::MAX become FLOAT
                if u > INTEGER_MAX_VALUE as u64 {
                    Some(BqType::Float)
                } else {
                    Some(BqType::Integer)
                }
            } else {
                Some(BqType::Float)
            }
        }
        serde_json::Value::String(s) => Some(infer_type_from_string(s, quoted_values_are_strings)),
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                Some(BqType::EmptyArray)
            } else {
                // For arrays, we need to check element types elsewhere
                None
            }
        }
        serde_json::Value::Object(obj) => {
            if obj.is_empty() {
                Some(BqType::EmptyRecord)
            } else {
                // For non-empty objects, return a marker - actual fields handled elsewhere
                Some(BqType::Record(Default::default()))
            }
        }
    }
}

/// Infer the BigQuery type from a string value.
///
/// This handles type inference for:
/// - Date/time types (TIMESTAMP, DATE, TIME)
/// - Quoted primitives (when `quoted_values_are_strings` is false)
pub fn infer_type_from_string(s: &str, quoted_values_are_strings: bool) -> BqType {
    // Always check date/time patterns first
    if is_timestamp(s) {
        return BqType::Timestamp;
    }
    if is_date(s) {
        return BqType::Date;
    }
    if is_time(s) {
        return BqType::Time;
    }

    // If quoted_values_are_strings is true, don't infer numeric/boolean types
    if quoted_values_are_strings {
        return BqType::String;
    }

    // Try to infer type from quoted value
    if is_integer_string(s) {
        // If it parses as i64, it's within BigQuery INTEGER range (INT64)
        if s.parse::<i64>().is_ok() {
            return BqType::QInteger;
        }
        // Overflow (doesn't fit in i64) - treat as float
        return BqType::QFloat;
    }

    if is_float_string(s) {
        return BqType::QFloat;
    }

    if is_boolean_string(s) {
        return BqType::QBoolean;
    }

    BqType::String
}

/// Infer the BigQuery type and mode from a JSON value.
///
/// Returns `(mode, type)` tuple, or `None` if the type cannot be determined
/// (e.g., for arrays with incompatible element types).
pub fn infer_bigquery_type(
    value: &serde_json::Value,
    quoted_values_are_strings: bool,
) -> Option<(BqMode, BqType)> {
    match value {
        serde_json::Value::Array(arr) => {
            if arr.is_empty() {
                return Some((BqMode::Nullable, BqType::EmptyArray));
            }

            // Infer array element type
            let element_type = infer_array_type(arr, quoted_values_are_strings)?;

            // Disallow arrays of special types (except empty records which bq load allows)
            if element_type.is_internal() && element_type != BqType::EmptyRecord {
                return None;
            }

            Some((BqMode::Repeated, element_type))
        }
        _ => {
            let bq_type = infer_type_from_json(value, quoted_values_are_strings)?;
            Some((BqMode::Nullable, bq_type))
        }
    }
}

/// Infer the common type for all elements in an array.
///
/// Returns `None` if the array contains incompatible types.
pub fn infer_array_type(
    elements: &[serde_json::Value],
    quoted_values_are_strings: bool,
) -> Option<BqType> {
    if elements.is_empty() {
        return None; // Should not happen - caller should check
    }

    let mut candidate_type: Option<BqType> = None;

    for elem in elements {
        let elem_type = match elem {
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    BqType::EmptyArray
                } else {
                    // Nested arrays not supported
                    return None;
                }
            }
            serde_json::Value::Object(obj) => {
                if obj.is_empty() {
                    BqType::EmptyRecord
                } else {
                    BqType::Record(Default::default())
                }
            }
            _ => infer_type_from_json(elem, quoted_values_are_strings)?,
        };

        candidate_type = match candidate_type {
            None => Some(elem_type),
            Some(current) => convert_type(&current, &elem_type),
        };

        candidate_type.as_ref()?;
    }

    candidate_type
}

/// Convert/merge two types, returning the compatible type if one exists.
///
/// Type conversion rules (matching Python implementation):
/// - Same type -> same type
/// - `[Q]BOOLEAN + [Q]BOOLEAN` -> BOOLEAN
/// - `[Q]INTEGER + [Q]INTEGER` -> INTEGER
/// - `[Q]FLOAT + [Q]FLOAT` -> FLOAT
/// - QINTEGER + QFLOAT -> QFLOAT
/// - `[Q]INTEGER + [Q]FLOAT` -> FLOAT (except QINTEGER + QFLOAT)
/// - String-compatible types -> STRING
/// - Otherwise -> None (incompatible)
pub fn convert_type(atype: &BqType, btype: &BqType) -> Option<BqType> {
    // Same type
    if atype == btype {
        return Some(atype.clone());
    }

    // [Q]BOOLEAN + [Q]BOOLEAN -> BOOLEAN
    if matches!(atype, BqType::Boolean | BqType::QBoolean)
        && matches!(btype, BqType::Boolean | BqType::QBoolean)
    {
        return Some(BqType::Boolean);
    }

    // [Q]INTEGER + [Q]INTEGER -> INTEGER
    if matches!(atype, BqType::Integer | BqType::QInteger)
        && matches!(btype, BqType::Integer | BqType::QInteger)
    {
        return Some(BqType::Integer);
    }

    // [Q]FLOAT + [Q]FLOAT -> FLOAT
    if matches!(atype, BqType::Float | BqType::QFloat)
        && matches!(btype, BqType::Float | BqType::QFloat)
    {
        return Some(BqType::Float);
    }

    // QINTEGER + QFLOAT -> QFLOAT
    if matches!(
        (atype, btype),
        (BqType::QInteger, BqType::QFloat) | (BqType::QFloat, BqType::QInteger)
    ) {
        return Some(BqType::QFloat);
    }

    // [Q]INTEGER + [Q]FLOAT -> FLOAT (except the QINTEGER + QFLOAT case above)
    let is_int_like = |t: &BqType| matches!(t, BqType::Integer | BqType::QInteger);
    let is_float_like = |t: &BqType| matches!(t, BqType::Float | BqType::QFloat);

    if (is_int_like(atype) && is_float_like(btype)) || (is_float_like(atype) && is_int_like(btype))
    {
        return Some(BqType::Float);
    }

    // String-compatible types all convert to STRING
    if atype.is_string_compatible() && btype.is_string_compatible() {
        return Some(BqType::String);
    }

    // RECORD + RECORD (both non-empty) - compatible
    if matches!(atype, BqType::Record(_)) && matches!(btype, BqType::Record(_)) {
        return Some(BqType::Record(Default::default()));
    }

    // EmptyRecord + Record -> Record
    if matches!(
        (atype, btype),
        (BqType::EmptyRecord, BqType::Record(_)) | (BqType::Record(_), BqType::EmptyRecord)
    ) {
        return Some(BqType::Record(Default::default()));
    }

    // No compatible type
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_timestamp_matcher_valid() {
        assert!(is_timestamp("2017-05-22T12:33:01"));
        assert!(is_timestamp("2017-05-22 12:33:01"));
        assert!(is_timestamp("2017-05-22 12:33:01.123"));
        assert!(is_timestamp("2017-05-22 12:33:01.123456"));
        assert!(is_timestamp("2017-05-22T12:33:01Z"));
        assert!(is_timestamp("2017-05-22T12:33:01 Z"));
        assert!(is_timestamp("2017-05-22T12:33:01UTC"));
        assert!(is_timestamp("2017-05-22 12:33:01 UTC"));
        assert!(is_timestamp("2017-05-22 12:33:01-7:00"));
        assert!(is_timestamp("2017-05-22 12:33:01-07:30"));
        assert!(is_timestamp("2017-05-22T12:33:01-7"));
        assert!(is_timestamp("2017-05-22 12:33:01+7:00"));
        assert!(is_timestamp("2017-5-2T1:3:1"));
    }

    #[test]
    fn test_timestamp_matcher_invalid() {
        assert!(!is_timestamp("2017-05-22 12:33:01-123:445"));
        assert!(!is_timestamp("2017-05-22 12:33:01-0700"));
        assert!(!is_timestamp("2017-05-22 12:33:01.1234567"));
        assert!(!is_timestamp("2017-05-22T12:33"));
        assert!(!is_timestamp("2017-05-22A12:33:00"));
        assert!(!is_timestamp("2017-05-22T12:33:01X07:00"));
        assert!(!is_timestamp("2017-5-2A2:3:0"));
        assert!(!is_timestamp("17-05-22T12:33:01"));
        assert!(!is_timestamp("2017-05-22T12:33:01 UT"));
    }

    #[test]
    fn test_date_matcher_valid() {
        assert!(is_date("2017-05-22"));
        assert!(is_date("2017-1-1"));
    }

    #[test]
    fn test_date_matcher_invalid() {
        assert!(!is_date("17-05-22"));
        assert!(!is_date("2017-111-22"));
        assert!(!is_date("1988-00-00"));
    }

    #[test]
    fn test_time_matcher_valid() {
        assert!(is_time("12:33:01"));
        assert!(is_time("12:33:01.123"));
        assert!(is_time("12:33:01.123456"));
        assert!(is_time("1:3:1"));
    }

    #[test]
    fn test_time_matcher_invalid() {
        assert!(!is_time(":33:01"));
        assert!(!is_time("123:33:01"));
        assert!(!is_time("12:33:01.1234567"));
    }

    #[test]
    fn test_integer_matcher() {
        assert!(is_integer_string("1"));
        assert!(is_integer_string("-1"));
        assert!(is_integer_string("+1"));
        assert!(!is_integer_string(""));
        assert!(!is_integer_string("-"));
        assert!(!is_integer_string("+"));
    }

    #[test]
    fn test_float_matcher() {
        assert!(is_float_string("1.0"));
        assert!(is_float_string("-1.0"));
        assert!(is_float_string("+1.0"));
        assert!(is_float_string("1."));
        assert!(is_float_string(".1"));
        assert!(is_float_string("1e1"));
        assert!(is_float_string("-1e-1"));
        assert!(is_float_string("3.3e+1"));
        assert!(!is_float_string(".e1"));
        assert!(!is_float_string("+e1"));
        assert!(!is_float_string("1e.1"));
        assert!(!is_float_string("1e"));
    }

    #[test]
    fn test_infer_type_from_json() {
        // Primitives
        assert_eq!(
            infer_type_from_json(&json!(null), false),
            Some(BqType::Null)
        );
        assert_eq!(
            infer_type_from_json(&json!(true), false),
            Some(BqType::Boolean)
        );
        assert_eq!(
            infer_type_from_json(&json!(42), false),
            Some(BqType::Integer)
        );
        assert_eq!(
            infer_type_from_json(&json!(3.5), false),
            Some(BqType::Float)
        );

        // Strings
        assert_eq!(
            infer_type_from_json(&json!("hello"), false),
            Some(BqType::String)
        );
        assert_eq!(
            infer_type_from_json(&json!("2018-02-08"), false),
            Some(BqType::Date)
        );
        assert_eq!(
            infer_type_from_json(&json!("12:34:56"), false),
            Some(BqType::Time)
        );
        assert_eq!(
            infer_type_from_json(&json!("2018-02-08T12:34:56"), false),
            Some(BqType::Timestamp)
        );

        // Quoted values with inference
        assert_eq!(
            infer_type_from_json(&json!("123"), false),
            Some(BqType::QInteger)
        );
        assert_eq!(
            infer_type_from_json(&json!("3.14"), false),
            Some(BqType::QFloat)
        );
        assert_eq!(
            infer_type_from_json(&json!("true"), false),
            Some(BqType::QBoolean)
        );

        // Quoted values without inference
        assert_eq!(
            infer_type_from_json(&json!("123"), true),
            Some(BqType::String)
        );
        assert_eq!(
            infer_type_from_json(&json!("3.14"), true),
            Some(BqType::String)
        );
        assert_eq!(
            infer_type_from_json(&json!("true"), true),
            Some(BqType::String)
        );

        // Empty structures
        assert_eq!(
            infer_type_from_json(&json!([]), false),
            Some(BqType::EmptyArray)
        );
        assert_eq!(
            infer_type_from_json(&json!({}), false),
            Some(BqType::EmptyRecord)
        );
    }

    #[test]
    fn test_convert_type() {
        // Same types
        assert_eq!(
            convert_type(&BqType::Integer, &BqType::Integer),
            Some(BqType::Integer)
        );
        assert_eq!(
            convert_type(&BqType::String, &BqType::String),
            Some(BqType::String)
        );

        // Integer + Float -> Float
        assert_eq!(
            convert_type(&BqType::Integer, &BqType::Float),
            Some(BqType::Float)
        );
        assert_eq!(
            convert_type(&BqType::Float, &BqType::Integer),
            Some(BqType::Float)
        );

        // QInteger + QFloat -> QFloat
        assert_eq!(
            convert_type(&BqType::QInteger, &BqType::QFloat),
            Some(BqType::QFloat)
        );

        // String compatible types
        assert_eq!(
            convert_type(&BqType::Date, &BqType::Time),
            Some(BqType::String)
        );
        assert_eq!(
            convert_type(&BqType::Date, &BqType::Timestamp),
            Some(BqType::String)
        );

        // Incompatible
        assert_eq!(convert_type(&BqType::Integer, &BqType::Boolean), None);
        assert_eq!(convert_type(&BqType::Float, &BqType::String), None);
    }

    // ===== Additional Coverage Tests =====

    #[test]
    fn test_infer_u64_exceeds_i64_max() {
        // u64 value greater than i64::MAX should become FLOAT
        let big_u64 = (i64::MAX as u64) + 1;
        let json_val = serde_json::json!(big_u64);
        let result = infer_type_from_json(&json_val, false);
        assert_eq!(result, Some(BqType::Float));

        // u64 value equal to i64::MAX should be INTEGER
        let max_i64_as_u64 = i64::MAX as u64;
        let json_val2 = serde_json::json!(max_i64_as_u64);
        let result2 = infer_type_from_json(&json_val2, false);
        assert_eq!(result2, Some(BqType::Integer));
    }

    #[test]
    fn test_infer_nested_arrays_not_supported() {
        // Non-empty nested arrays should return None
        let nested_arr = json!([[1, 2], [3, 4]]);
        let result = infer_bigquery_type(&nested_arr, false);
        assert!(result.is_none(), "Nested arrays should not be supported");

        // Empty nested array should work (becomes EmptyArray in outer array)
        let arr_with_empty = json!([[], []]);
        let result2 = infer_bigquery_type(&arr_with_empty, false);
        // Empty arrays in array should also not be directly supported
        assert!(
            result2.is_none() || matches!(result2, Some((BqMode::Repeated, BqType::EmptyArray)))
        );
    }

    #[test]
    fn test_infer_empty_array_elements() {
        // Array containing only empty arrays
        let _arr = json!([[], [], []]);
        let result = infer_array_type(&[json!([]), json!([]), json!([])], false);
        assert!(result.is_some());
        assert_eq!(result, Some(BqType::EmptyArray));
    }

    #[test]
    fn test_infer_mixed_record_and_empty_record() {
        // Mix of Record and EmptyRecord should result in Record
        let record = json!({"field": "value"});
        let empty_record = json!({});

        // Create array to test
        let arr = vec![record.clone(), empty_record.clone(), record.clone()];
        let result = infer_array_type(&arr, false);
        assert!(result.is_some());
        // Should be Record (empty record merges with record)
        assert!(matches!(result, Some(BqType::Record(_))));
    }

    #[test]
    fn test_infer_type_from_string_quoted_integer_overflow() {
        // Integer string that overflows i64 should become QFloat
        let big_int_str = "99999999999999999999999999999999";
        let result = infer_type_from_string(big_int_str, false);
        assert_eq!(result, BqType::QFloat);

        // Negative overflow
        let neg_big_int_str = "-99999999999999999999999999999999";
        let result2 = infer_type_from_string(neg_big_int_str, false);
        assert_eq!(result2, BqType::QFloat);
    }

    #[test]
    fn test_infer_bigquery_type_empty_array() {
        let empty_arr = json!([]);
        let result = infer_bigquery_type(&empty_arr, false);
        assert!(result.is_some());
        let (mode, bq_type) = result.unwrap();
        assert_eq!(mode, BqMode::Nullable);
        assert_eq!(bq_type, BqType::EmptyArray);
    }

    #[test]
    fn test_infer_array_with_internal_type_rejected() {
        // Arrays with Null type elements (not EmptyRecord) should be rejected
        let arr = vec![json!(null), json!(null)];
        let result = infer_bigquery_type(&json!(arr), false);
        // Should be rejected since Null is internal type
        assert!(result.is_none());
    }

    #[test]
    fn test_convert_type_record_merging() {
        // Record + Record -> Record
        let record1 = BqType::Record(Default::default());
        let record2 = BqType::Record(Default::default());
        let result = convert_type(&record1, &record2);
        assert!(matches!(result, Some(BqType::Record(_))));

        // EmptyRecord + Record -> Record
        let result2 = convert_type(&BqType::EmptyRecord, &record1);
        assert!(matches!(result2, Some(BqType::Record(_))));

        // Record + EmptyRecord -> Record
        let result3 = convert_type(&record1, &BqType::EmptyRecord);
        assert!(matches!(result3, Some(BqType::Record(_))));
    }

    #[test]
    fn test_convert_type_qboolean_combinations() {
        // QBoolean + QBoolean -> QBoolean (stays quoted)
        assert_eq!(
            convert_type(&BqType::QBoolean, &BqType::QBoolean),
            Some(BqType::QBoolean)
        );

        // QBoolean + Boolean -> Boolean (upgrades to unquoted)
        assert_eq!(
            convert_type(&BqType::QBoolean, &BqType::Boolean),
            Some(BqType::Boolean)
        );

        // Boolean + QBoolean -> Boolean (upgrades to unquoted)
        assert_eq!(
            convert_type(&BqType::Boolean, &BqType::QBoolean),
            Some(BqType::Boolean)
        );
    }

    #[test]
    fn test_convert_type_qinteger_combinations() {
        // QInteger + QInteger -> QInteger (stays quoted)
        assert_eq!(
            convert_type(&BqType::QInteger, &BqType::QInteger),
            Some(BqType::QInteger)
        );

        // QInteger + Integer -> Integer (upgrades to unquoted)
        assert_eq!(
            convert_type(&BqType::QInteger, &BqType::Integer),
            Some(BqType::Integer)
        );

        // Integer + QInteger -> Integer (upgrades to unquoted)
        assert_eq!(
            convert_type(&BqType::Integer, &BqType::QInteger),
            Some(BqType::Integer)
        );
    }

    #[test]
    fn test_convert_type_qfloat_combinations() {
        // QFloat + QFloat -> QFloat (stays quoted)
        assert_eq!(
            convert_type(&BqType::QFloat, &BqType::QFloat),
            Some(BqType::QFloat)
        );

        // QFloat + Float -> Float (upgrades to unquoted)
        assert_eq!(
            convert_type(&BqType::QFloat, &BqType::Float),
            Some(BqType::Float)
        );

        // Float + QFloat -> Float (upgrades to unquoted)
        assert_eq!(
            convert_type(&BqType::Float, &BqType::QFloat),
            Some(BqType::Float)
        );
    }

    #[test]
    fn test_convert_type_int_float_combinations() {
        // Integer + Float -> Float
        assert_eq!(
            convert_type(&BqType::Integer, &BqType::Float),
            Some(BqType::Float)
        );

        // QInteger + Float -> Float
        assert_eq!(
            convert_type(&BqType::QInteger, &BqType::Float),
            Some(BqType::Float)
        );

        // Integer + QFloat -> Float
        assert_eq!(
            convert_type(&BqType::Integer, &BqType::QFloat),
            Some(BqType::Float)
        );
    }

    #[test]
    fn test_convert_type_string_compatible_merging() {
        // String + Date -> String
        assert_eq!(
            convert_type(&BqType::String, &BqType::Date),
            Some(BqType::String)
        );

        // Time + Timestamp -> String
        assert_eq!(
            convert_type(&BqType::Time, &BqType::Timestamp),
            Some(BqType::String)
        );

        // QInteger + Date -> String
        assert_eq!(
            convert_type(&BqType::QInteger, &BqType::Date),
            Some(BqType::String)
        );

        // QBoolean + String -> String
        assert_eq!(
            convert_type(&BqType::QBoolean, &BqType::String),
            Some(BqType::String)
        );
    }

    #[test]
    fn test_convert_type_incompatible_combinations() {
        // Boolean + String -> None (String is not compatible with Boolean)
        assert_eq!(convert_type(&BqType::Boolean, &BqType::String), None);

        // Integer + String -> None
        assert_eq!(convert_type(&BqType::Integer, &BqType::String), None);

        // Float + Boolean -> None
        assert_eq!(convert_type(&BqType::Float, &BqType::Boolean), None);

        // Record + Integer -> None
        assert_eq!(
            convert_type(&BqType::Record(Default::default()), &BqType::Integer),
            None
        );
    }

    #[test]
    fn test_is_boolean_string_case_insensitive() {
        assert!(is_boolean_string("true"));
        assert!(is_boolean_string("false"));
        assert!(is_boolean_string("TRUE"));
        assert!(is_boolean_string("FALSE"));
        assert!(is_boolean_string("True"));
        assert!(is_boolean_string("False"));

        assert!(!is_boolean_string("yes"));
        assert!(!is_boolean_string("no"));
        assert!(!is_boolean_string("1"));
        assert!(!is_boolean_string("0"));
    }

    #[test]
    fn test_infer_type_from_json_non_empty_object() {
        let obj = json!({"key": "value"});
        let result = infer_type_from_json(&obj, false);
        assert!(matches!(result, Some(BqType::Record(_))));
    }

    #[test]
    fn test_infer_bigquery_type_non_array() {
        // Non-array values should return Nullable mode
        let cases = vec![
            (json!(null), BqType::Null),
            (json!(true), BqType::Boolean),
            (json!(42), BqType::Integer),
            (json!(2.5), BqType::Float),
            (json!("hello"), BqType::String),
        ];

        for (value, expected_type) in cases {
            let result = infer_bigquery_type(&value, false);
            assert!(result.is_some());
            let (mode, bq_type) = result.unwrap();
            assert_eq!(mode, BqMode::Nullable);
            assert_eq!(bq_type, expected_type);
        }
    }

    #[test]
    fn test_infer_array_type_mixed_integers_floats() {
        // Array with integers and floats should become FLOAT
        let arr = vec![json!(1), json!(2.5), json!(3)];
        let result = infer_array_type(&arr, false);
        assert_eq!(result, Some(BqType::Float));
    }

    #[test]
    fn test_infer_type_datetime_patterns_priority() {
        // Timestamp pattern should take priority over date
        let ts = "2024-01-15T12:30:45";
        assert_eq!(infer_type_from_string(ts, false), BqType::Timestamp);

        // Date only
        let date = "2024-01-15";
        assert_eq!(infer_type_from_string(date, false), BqType::Date);

        // Time only
        let time = "12:30:45";
        assert_eq!(infer_type_from_string(time, false), BqType::Time);
    }
}
