//! Input readers for JSON and CSV formats.

pub mod csv;
pub mod json;

pub use self::csv::{CsvReader, CsvRecordIterator};
pub use self::json::{JsonReader, JsonRecordIterator};
