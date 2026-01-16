//! JSON input reader for newline-delimited JSON files.
//!
//! This module provides streaming JSON parsing, reading one record per line.

use std::io::BufRead;

use crate::error::{Error, Result};

/// Result of reading a single JSON line.
pub enum JsonLineResult {
    /// Successfully parsed JSON object
    Record(serde_json::Value),
    /// Parse error on this line (can be skipped with ignore_invalid_lines)
    ParseError { line: usize, error: String },
    /// End of input
    EndOfInput,
}

/// Streaming JSON reader for newline-delimited JSON.
pub struct JsonReader<R: BufRead> {
    reader: R,
    line_number: usize,
    buffer: String,
}

impl<R: BufRead> JsonReader<R> {
    /// Create a new JSON reader.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            line_number: 0,
            buffer: String::new(),
        }
    }

    /// Get the current line number.
    pub fn line_number(&self) -> usize {
        self.line_number
    }

    /// Read the next JSON record.
    pub fn read_record(&mut self) -> Result<JsonLineResult> {
        self.buffer.clear();

        match self.reader.read_line(&mut self.buffer) {
            Ok(0) => Ok(JsonLineResult::EndOfInput),
            Ok(_) => {
                self.line_number += 1;
                let line = self.buffer.trim();

                if line.is_empty() {
                    // Skip empty lines, try next
                    return self.read_record();
                }

                match serde_json::from_str(line) {
                    Ok(value) => Ok(JsonLineResult::Record(value)),
                    Err(e) => Ok(JsonLineResult::ParseError {
                        line: self.line_number,
                        error: e.to_string(),
                    }),
                }
            }
            Err(e) => Err(Error::Io(e)),
        }
    }
}

/// Iterator adapter for JSON reader.
pub struct JsonRecordIterator<R: BufRead> {
    reader: JsonReader<R>,
    ignore_invalid_lines: bool,
}

impl<R: BufRead> JsonRecordIterator<R> {
    pub fn new(reader: R, ignore_invalid_lines: bool) -> Self {
        Self {
            reader: JsonReader::new(reader),
            ignore_invalid_lines,
        }
    }

    pub fn line_number(&self) -> usize {
        self.reader.line_number()
    }
}

impl<R: BufRead> Iterator for JsonRecordIterator<R> {
    type Item = Result<(usize, serde_json::Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.reader.read_record() {
                Ok(JsonLineResult::Record(value)) => {
                    return Some(Ok((self.reader.line_number(), value)));
                }
                Ok(JsonLineResult::ParseError { line, error }) => {
                    if self.ignore_invalid_lines {
                        // Log and continue
                        eprintln!("Warning: Skipping invalid JSON on line {}: {}", line, error);
                        continue;
                    } else {
                        return Some(Err(Error::JsonParse {
                            line,
                            message: error,
                        }));
                    }
                }
                Ok(JsonLineResult::EndOfInput) => return None,
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_single_record() {
        let input = r#"{"name": "test", "value": 42}"#;
        let cursor = Cursor::new(input);
        let mut reader = JsonReader::new(cursor);

        match reader.read_record().unwrap() {
            JsonLineResult::Record(value) => {
                assert_eq!(value["name"], "test");
                assert_eq!(value["value"], 42);
            }
            _ => panic!("Expected Record"),
        }
    }

    #[test]
    fn test_read_multiple_records() {
        let input = r#"{"a": 1}
{"b": 2}
{"c": 3}"#;
        let cursor = Cursor::new(input);
        let iter = JsonRecordIterator::new(cursor, false);
        let records: Vec<_> = iter.collect();

        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_skip_empty_lines() {
        let input = r#"{"a": 1}

{"b": 2}"#;
        let cursor = Cursor::new(input);
        let iter = JsonRecordIterator::new(cursor, false);
        let records: Vec<_> = iter.collect();

        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_invalid_json_error() {
        let input = "not valid json";
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_json_ignore() {
        let input = r#"{"a": 1}
not valid json
{"b": 2}"#;
        let cursor = Cursor::new(input);
        let iter = JsonRecordIterator::new(cursor, true);
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        assert_eq!(records.unwrap().len(), 2);
    }

    #[test]
    fn test_read_record_empty_file() {
        let input = "";
        let cursor = Cursor::new(input);
        let mut reader = JsonReader::new(cursor);

        match reader.read_record().unwrap() {
            JsonLineResult::EndOfInput => {}
            _ => panic!("Expected EndOfInput for empty file"),
        }
    }

    #[test]
    fn test_read_record_only_whitespace() {
        let input = "   \n\n   \n";
        let cursor = Cursor::new(input);
        let iter = JsonRecordIterator::new(cursor, false);
        let records: Vec<_> = iter.collect();

        assert!(records.is_empty());
    }

    #[test]
    fn test_read_record_unicode_content() {
        let input = r#"{"name": "日本語", "emoji": "🚀"}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["name"], "日本語");
        assert_eq!(result.1["emoji"], "🚀");
    }

    #[test]
    fn test_read_record_nested_objects() {
        let input = r#"{"outer": {"inner": {"deep": 123}}}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["outer"]["inner"]["deep"], 123);
    }

    #[test]
    fn test_read_record_arrays() {
        let input = r#"{"items": [1, 2, 3], "nested": [{"a": 1}, {"a": 2}]}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["items"].as_array().unwrap().len(), 3);
        assert_eq!(result.1["nested"].as_array().unwrap().len(), 2);
    }

    #[test]
    fn test_read_record_special_characters() {
        let input = r#"{"path": "C:\\Users\\test", "quote": "He said \"hello\""}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["path"], "C:\\Users\\test");
        assert!(result.1["quote"].as_str().unwrap().contains("hello"));
    }

    #[test]
    fn test_read_record_null_values() {
        let input = r#"{"a": null, "b": "not null", "c": null}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert!(result.1["a"].is_null());
        assert!(!result.1["b"].is_null());
    }

    #[test]
    fn test_read_record_boolean_values() {
        let input = r#"{"flag1": true, "flag2": false}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["flag1"], true);
        assert_eq!(result.1["flag2"], false);
    }

    #[test]
    fn test_read_record_numeric_precision() {
        let input = r#"{"int": 9223372036854775807, "float": 1.234567890123456}"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["int"], 9223372036854775807_i64);
        assert!((result.1["float"].as_f64().unwrap() - 1.234567890123456).abs() < 1e-10);
    }

    #[test]
    fn test_line_number_tracking() {
        let input = r#"{"a": 1}
{"b": 2}
{"c": 3}"#;
        let cursor = Cursor::new(input);
        let iter = JsonRecordIterator::new(cursor, false);
        let records: Vec<_> = iter.collect();

        assert_eq!(records[0].as_ref().unwrap().0, 1);
        assert_eq!(records[1].as_ref().unwrap().0, 2);
        assert_eq!(records[2].as_ref().unwrap().0, 3);
    }

    #[test]
    fn test_line_number_with_empty_lines() {
        let input = r#"{"a": 1}

{"b": 2}

{"c": 3}"#;
        let cursor = Cursor::new(input);
        let iter = JsonRecordIterator::new(cursor, false);
        let records: Vec<_> = iter.collect();

        // Empty lines are skipped but line numbers are still tracked
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].as_ref().unwrap().0, 1);
        assert_eq!(records[1].as_ref().unwrap().0, 3); // Line 2 was empty
        assert_eq!(records[2].as_ref().unwrap().0, 5); // Line 4 was empty
    }

    #[test]
    fn test_malformed_json_error_message() {
        let input = "{missing: quotes}";
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap();
        assert!(result.is_err());
        // Error should contain some indication of what went wrong
        let err = result.unwrap_err();
        assert!(err.to_string().contains("line"));
    }

    #[test]
    fn test_truncated_json() {
        let input = r#"{"incomplete": "#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn test_json_array_at_root() {
        // Arrays at root should be parsed (though may not be typical input)
        let input = r#"[1, 2, 3]"#;
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert!(result.1.is_array());
    }

    #[test]
    fn test_very_long_line() {
        // Create a JSON with a very long string value
        let long_string = "x".repeat(10000);
        let input = format!(r#"{{"data": "{}"}}"#, long_string);
        let cursor = Cursor::new(input);
        let mut iter = JsonRecordIterator::new(cursor, false);

        let result = iter.next().unwrap().unwrap();
        assert_eq!(result.1["data"].as_str().unwrap().len(), 10000);
    }
}
