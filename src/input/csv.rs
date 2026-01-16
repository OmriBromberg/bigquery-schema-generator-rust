//! CSV input reader.
//!
//! This module provides CSV parsing with header support.

use std::io::Read;

use crate::error::{Error, Result};

/// CSV reader that converts rows to JSON-like objects.
pub struct CsvReader<R: Read> {
    reader: csv::Reader<R>,
    headers: Vec<String>,
    line_number: usize,
}

impl<R: Read> CsvReader<R> {
    /// Create a new CSV reader.
    pub fn new(reader: R) -> Result<Self> {
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(reader);

        // Read headers
        let headers: Vec<String> = csv_reader
            .headers()
            .map_err(|e| Error::CsvParse(e.to_string()))?
            .iter()
            .map(|s| s.to_string())
            .collect();

        Ok(Self {
            reader: csv_reader,
            headers,
            line_number: 1, // Header is line 1
        })
    }

    /// Get the headers.
    pub fn headers(&self) -> &[String] {
        &self.headers
    }

    /// Get the current line number.
    pub fn line_number(&self) -> usize {
        self.line_number
    }

    /// Read the next record as a JSON object.
    pub fn read_record(&mut self) -> Result<Option<serde_json::Value>> {
        let mut record = csv::StringRecord::new();

        match self.reader.read_record(&mut record) {
            Ok(true) => {
                self.line_number += 1;

                // Convert to JSON object
                let mut obj = serde_json::Map::new();
                for (i, field) in record.iter().enumerate() {
                    if i < self.headers.len() {
                        let key = self.headers[i].clone();
                        obj.insert(key, serde_json::Value::String(field.to_string()));
                    }
                }

                Ok(Some(serde_json::Value::Object(obj)))
            }
            Ok(false) => Ok(None),
            Err(e) => Err(Error::CsvParse(e.to_string())),
        }
    }
}

/// Iterator adapter for CSV reader.
pub struct CsvRecordIterator<R: Read> {
    reader: CsvReader<R>,
}

impl<R: Read> CsvRecordIterator<R> {
    pub fn new(reader: R) -> Result<Self> {
        Ok(Self {
            reader: CsvReader::new(reader)?,
        })
    }

    pub fn line_number(&self) -> usize {
        self.reader.line_number()
    }

    pub fn headers(&self) -> &[String] {
        self.reader.headers()
    }
}

impl<R: Read> Iterator for CsvRecordIterator<R> {
    type Item = Result<(usize, serde_json::Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_record() {
            Ok(Some(value)) => Some(Ok((self.reader.line_number(), value))),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_csv() {
        let input = "name,value,active\ntest,42,true\nfoo,123,false";
        let cursor = Cursor::new(input);
        let mut reader = CsvReader::new(cursor).unwrap();

        assert_eq!(reader.headers(), &["name", "value", "active"]);

        let record1 = reader.read_record().unwrap().unwrap();
        assert_eq!(record1["name"], "test");
        assert_eq!(record1["value"], "42"); // CSV values are strings

        let record2 = reader.read_record().unwrap().unwrap();
        assert_eq!(record2["name"], "foo");

        let record3 = reader.read_record().unwrap();
        assert!(record3.is_none());
    }

    #[test]
    fn test_csv_iterator() {
        let input = "a,b\n1,2\n3,4\n5,6";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Vec<_> = iter.collect();

        assert_eq!(records.len(), 3);
    }

    #[test]
    fn test_csv_empty_values() {
        let input = "a,b,c\n1,,3\n,,";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records.len(), 2);

        // Check empty values
        assert_eq!(records[0].1["b"], "");
        assert_eq!(records[1].1["a"], "");
    }

    #[test]
    fn test_csv_headers_only() {
        let input = "header1,header2,header3";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();

        assert_eq!(iter.headers(), &["header1", "header2", "header3"]);

        let records: Vec<_> = iter.collect();
        assert!(records.is_empty());
    }

    #[test]
    fn test_csv_quoted_fields() {
        let input = r#"name,description
"John Doe","A person with a comma, here"
"Jane","Quotes ""inside"" the field""#;
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records[0].1["description"], "A person with a comma, here");
        assert!(records[1].1["description"]
            .as_str()
            .unwrap()
            .contains("inside"));
    }

    #[test]
    fn test_csv_unicode_content() {
        let input = "name,city\n日本語,東京\n한국어,서울";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records[0].1["name"], "日本語");
        assert_eq!(records[0].1["city"], "東京");
    }

    #[test]
    fn test_csv_more_fields_than_headers() {
        // CSV with more fields than headers - extra fields should be ignored due to flexible mode
        let input = "a,b\n1,2,3,4";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        // Only fields matching headers should be included
        assert_eq!(records[0].1["a"], "1");
        assert_eq!(records[0].1["b"], "2");
    }

    #[test]
    fn test_csv_fewer_fields_than_headers() {
        // CSV with fewer fields than headers
        let input = "a,b,c\n1,2";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        // Only provided fields should be in output
        assert_eq!(records[0].1["a"], "1");
        assert_eq!(records[0].1["b"], "2");
        assert!(records[0].1.get("c").is_none());
    }

    #[test]
    fn test_csv_line_number_tracking() {
        let input = "col\na\nb\nc";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Vec<_> = iter.collect();

        // Line 1 is header, data starts at line 2
        assert_eq!(records[0].as_ref().unwrap().0, 2);
        assert_eq!(records[1].as_ref().unwrap().0, 3);
        assert_eq!(records[2].as_ref().unwrap().0, 4);
    }

    #[test]
    fn test_csv_newlines_in_quoted_fields() {
        let input = "name,bio\nJohn,\"Line 1\nLine 2\"";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert!(records[0].1["bio"].as_str().unwrap().contains("\n"));
    }

    #[test]
    fn test_csv_whitespace_in_values() {
        let input = "a,b,c\n  leading,trailing  ,  both  ";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records[0].1["a"], "  leading");
        assert_eq!(records[0].1["b"], "trailing  ");
        assert_eq!(records[0].1["c"], "  both  ");
    }

    #[test]
    fn test_csv_single_column() {
        let input = "single\nvalue1\nvalue2";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1["single"], "value1");
    }

    #[test]
    fn test_csv_many_columns() {
        let headers: Vec<String> = (0..100).map(|i| format!("col{}", i)).collect();
        let values: Vec<String> = (0..100).map(|i| format!("val{}", i)).collect();

        let input = format!("{}\n{}", headers.join(","), values.join(","));
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();

        assert_eq!(iter.headers().len(), 100);

        let records: Result<Vec<_>> = iter.collect();
        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records[0].1["col0"], "val0");
        assert_eq!(records[0].1["col99"], "val99");
    }

    #[test]
    fn test_csv_numeric_looking_strings() {
        let input = "id,phone\n123,555-1234\n456,+1 (555) 987-6543";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        // All CSV values should be strings
        assert!(records[0].1["id"].is_string());
        assert!(records[0].1["phone"].is_string());
    }

    #[test]
    fn test_csv_special_header_names() {
        let input = "has space,has-dash,has.dot\n1,2,3";
        let cursor = Cursor::new(input);
        let iter = CsvRecordIterator::new(cursor).unwrap();
        let records: Result<Vec<_>> = iter.collect();

        assert!(records.is_ok());
        let records = records.unwrap();
        assert_eq!(records[0].1["has space"], "1");
        assert_eq!(records[0].1["has-dash"], "2");
        assert_eq!(records[0].1["has.dot"], "3");
    }

    #[test]
    fn test_csv_empty_file() {
        let input = "";
        let cursor = Cursor::new(input);
        let result = CsvRecordIterator::new(cursor);

        // CSV library handles empty files gracefully with empty headers
        // This documents the actual behavior
        assert!(result.is_ok());
        let iter = result.unwrap();
        assert!(iter.headers().is_empty());
    }
}
