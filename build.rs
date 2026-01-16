//! Build script for bq-schema-gen
//!
//! This script:
//! 1. Captures the git hash at compile time
//! 2. Generates shell completions for bash, zsh, fish, and PowerShell
//! 3. Generates man pages

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

use clap::{CommandFactory, Parser, ValueEnum};
use clap_complete::{generate_to, Shell};

// We need to define the Args struct here since build.rs can't import from the main crate
#[derive(Parser, Debug)]
#[command(
    name = "bq-schema-gen",
    about = "Generate BigQuery schema from JSON or CSV file",
    version,
    author
)]
struct Args {
    /// Input files (supports glob patterns). Reads from stdin if not provided.
    #[arg(value_name = "FILE")]
    files: Vec<String>,

    /// Input format: 'json' or 'csv'
    #[arg(long, alias = "input_format", default_value = "json")]
    input_format: String,

    /// Output format: 'json' (default), 'ddl', 'debug-map', or 'json-schema'
    #[arg(long, alias = "output_format", default_value = "json")]
    output_format: String,

    /// Table name for DDL output (e.g., 'dataset.table_name')
    #[arg(long, alias = "table_name", default_value = "dataset.table_name")]
    table_name: String,

    /// Print the schema for null values, empty arrays or empty records
    #[arg(long, alias = "keep_nulls")]
    keep_nulls: bool,

    /// Quoted values should be interpreted as strings
    #[arg(long, alias = "quoted_values_are_strings")]
    quoted_values_are_strings: bool,

    /// Determine if mode can be 'NULLABLE' or 'REQUIRED'
    #[arg(long, alias = "infer_mode")]
    infer_mode: bool,

    /// Number of lines between heartbeat debugging messages
    #[arg(long, alias = "debugging_interval", default_value = "1000")]
    debugging_interval: usize,

    /// Forces schema name to comply with BigQuery naming standard
    #[arg(long, alias = "sanitize_names")]
    sanitize_names: bool,

    /// Ignore lines that cannot be parsed instead of stopping
    #[arg(long, alias = "ignore_invalid_lines")]
    ignore_invalid_lines: bool,

    /// File that contains the existing BigQuery schema for a table
    #[arg(long, alias = "existing_schema_path")]
    existing_schema_path: Option<PathBuf>,

    /// Preserve the original ordering of columns from input instead of sorting alphabetically
    #[arg(long, alias = "preserve_input_sort_order")]
    preserve_input_sort_order: bool,

    /// Suppress progress messages (only output schema and errors)
    #[arg(short, long)]
    quiet: bool,

    /// Input file (reads from stdin if not provided). Deprecated: use positional arguments instead.
    #[arg(short, long)]
    input: Option<PathBuf>,

    /// Output file (writes to stdout if not provided)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Output separate schema for each input file instead of merging
    #[arg(long)]
    per_file: bool,

    /// Output directory for per-file schemas (used with --per-file)
    #[arg(long)]
    output_dir: Option<PathBuf>,
}

fn get_git_hash() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;

    if output.status.success() {
        let hash = String::from_utf8(output.stdout).ok()?;
        Some(hash.trim().to_string())
    } else {
        None
    }
}

fn get_git_dirty() -> bool {
    Command::new("git")
        .args(["diff", "--quiet", "HEAD"])
        .status()
        .map(|s| !s.success())
        .unwrap_or(false)
}

fn main() {
    // Emit git hash for version string
    if let Some(hash) = get_git_hash() {
        let dirty = if get_git_dirty() { "-dirty" } else { "" };
        println!("cargo:rustc-env=GIT_HASH={}{}", hash, dirty);
    } else {
        println!("cargo:rustc-env=GIT_HASH=unknown");
    }

    // Re-run if git HEAD changes
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=.git/index");

    // Get output directory
    let out_dir = match env::var_os("OUT_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => return,
    };

    // Create completions directory
    let completions_dir = out_dir.join("completions");
    fs::create_dir_all(&completions_dir).expect("Failed to create completions directory");

    // Generate shell completions
    let mut cmd = Args::command();
    for shell in Shell::value_variants() {
        generate_to(*shell, &mut cmd, "bq-schema-gen", &completions_dir)
            .expect("Failed to generate shell completions");
    }

    // Create man page directory
    let man_dir = out_dir.join("man");
    fs::create_dir_all(&man_dir).expect("Failed to create man directory");

    // Generate man page
    let cmd = Args::command();
    let man = clap_mangen::Man::new(cmd);
    let mut buffer: Vec<u8> = Vec::new();
    man.render(&mut buffer)
        .expect("Failed to generate man page");
    fs::write(man_dir.join("bq-schema-gen.1"), buffer).expect("Failed to write man page");

    // Print paths for reference
    println!(
        "cargo:warning=Shell completions generated at: {}",
        completions_dir.display()
    );
    println!(
        "cargo:warning=Man page generated at: {}",
        man_dir.display()
    );
}
