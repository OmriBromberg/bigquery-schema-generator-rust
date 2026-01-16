//! CLI module for BigQuery Schema Generator.
//!
//! This module handles command-line argument parsing and subcommand dispatch.

pub mod diff;
pub mod generate;
pub mod validate;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Version string with git hash
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), " (", env!("GIT_HASH"), ")");

/// BigQuery Schema Generator CLI
#[derive(Parser, Debug)]
#[command(
    name = "bq-schema-gen",
    about = "Generate BigQuery schema from JSON or CSV file",
    version = VERSION,
    author
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,

    /// Input files (supports glob patterns). Reads from stdin if not provided.
    #[arg(value_name = "FILE")]
    pub files: Vec<String>,

    /// Input format: 'json' or 'csv'
    #[arg(long, alias = "input_format", default_value = "json")]
    pub input_format: String,

    /// Output format: 'json' (default), 'ddl', 'debug-map', or 'json-schema'
    #[arg(long, alias = "output_format", default_value = "json")]
    pub output_format: String,

    /// Table name for DDL output (e.g., 'dataset.table_name')
    #[arg(long, alias = "table_name", default_value = "dataset.table_name")]
    pub table_name: String,

    /// Print the schema for null values, empty arrays or empty records
    #[arg(long, alias = "keep_nulls")]
    pub keep_nulls: bool,

    /// Quoted values should be interpreted as strings
    #[arg(long, alias = "quoted_values_are_strings")]
    pub quoted_values_are_strings: bool,

    /// Determine if mode can be 'NULLABLE' or 'REQUIRED'
    #[arg(long, alias = "infer_mode")]
    pub infer_mode: bool,

    /// Number of lines between heartbeat debugging messages
    #[arg(long, alias = "debugging_interval", default_value = "1000")]
    pub debugging_interval: usize,

    /// Forces schema name to comply with BigQuery naming standard
    #[arg(long, alias = "sanitize_names")]
    pub sanitize_names: bool,

    /// Ignore lines that cannot be parsed instead of stopping
    #[arg(long, alias = "ignore_invalid_lines")]
    pub ignore_invalid_lines: bool,

    /// File that contains the existing BigQuery schema for a table
    #[arg(long, alias = "existing_schema_path")]
    pub existing_schema_path: Option<PathBuf>,

    /// Preserve the original ordering of columns from input instead of sorting alphabetically
    #[arg(long, alias = "preserve_input_sort_order")]
    pub preserve_input_sort_order: bool,

    /// Suppress progress messages (only output schema and errors)
    #[arg(short, long)]
    pub quiet: bool,

    /// Input file (reads from stdin if not provided). Deprecated: use positional arguments instead.
    #[arg(short, long)]
    pub input: Option<PathBuf>,

    /// Output file (writes to stdout if not provided)
    #[arg(short, long)]
    pub output: Option<PathBuf>,

    /// Output separate schema for each input file instead of merging
    #[arg(long)]
    pub per_file: bool,

    /// Output directory for per-file schemas (used with --per-file)
    #[arg(long)]
    pub output_dir: Option<PathBuf>,

    /// Number of threads for parallel processing (default: auto-detect CPUs)
    #[arg(long)]
    pub threads: Option<usize>,

    /// Enable watch mode to automatically regenerate schema on file changes
    #[arg(long)]
    pub watch: bool,

    /// Debounce delay in milliseconds for watch mode (default: 100)
    #[arg(long, default_value = "100")]
    pub debounce: u64,

    /// Command to run after schema regeneration in watch mode
    #[arg(long)]
    pub on_change: Option<String>,
}

/// Available subcommands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Compare two BigQuery schemas and show differences
    Diff {
        /// Old schema file (JSON format)
        old_schema: PathBuf,

        /// New schema file (JSON format)
        new_schema: PathBuf,

        /// Output format: 'text' (default), 'json', 'json-patch', or 'sql'
        #[arg(long, default_value = "text")]
        format: String,

        /// Color output: 'auto' (default), 'always', or 'never'
        #[arg(long, default_value = "auto")]
        color: String,

        /// Flag ALL changes as breaking (not just risky ones)
        #[arg(long)]
        strict: bool,

        /// Output file (writes to stdout if not provided)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Validate data against an existing BigQuery schema
    Validate {
        /// Input data file(s) (supports glob patterns)
        #[arg(value_name = "FILE")]
        files: Vec<String>,

        /// Path to existing BigQuery schema file (JSON format)
        #[arg(long, required = true)]
        schema: PathBuf,

        /// Don't fail on fields not in schema (warn only)
        #[arg(long)]
        allow_unknown: bool,

        /// Strict type checking (JSON strings don't match INTEGER, etc.)
        #[arg(long)]
        strict_types: bool,

        /// Stop after N errors (default: 100)
        #[arg(long, default_value = "100")]
        max_errors: usize,

        /// Output format: 'text' (default) or 'json'
        #[arg(long, default_value = "text")]
        format: String,

        /// Only exit code, no error details
        #[arg(short, long)]
        quiet: bool,
    },
}

/// Run the CLI application
pub fn run() {
    let cli = Cli::parse();

    match cli.command {
        Some(Commands::Diff {
            old_schema,
            new_schema,
            format,
            color,
            strict,
            output,
        }) => {
            diff::run(
                &old_schema,
                &new_schema,
                &format,
                &color,
                strict,
                output.as_ref(),
            );
        }
        Some(Commands::Validate {
            files,
            schema,
            allow_unknown,
            strict_types,
            max_errors,
            format,
            quiet,
        }) => {
            validate::run(
                &files,
                &schema,
                allow_unknown,
                strict_types,
                max_errors,
                &format,
                quiet,
            );
        }
        None => {
            generate::run(&cli);
        }
    }
}
