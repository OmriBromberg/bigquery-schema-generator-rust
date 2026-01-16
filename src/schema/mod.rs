//! Schema generation and representation for BigQuery.

pub mod existing;
pub mod generator;
pub mod types;

pub use existing::{bq_schema_to_map, read_existing_schema_from_file};
pub use generator::{GeneratorConfig, InputFormat, SchemaGenerator};
pub use types::{BqMode, BqSchemaField, BqType, EntryStatus, SchemaEntry, SchemaMap};
