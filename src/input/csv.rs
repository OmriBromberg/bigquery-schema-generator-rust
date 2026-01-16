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
}
