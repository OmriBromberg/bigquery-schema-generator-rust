//! CLI module for BigQuery Schema Generator.
//!
//! This module handles command-line argument parsing and subcommand dispatch.

pub mod diff;
pub mod generate;
pub mod validate;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

// Include the shared CLI definition
// This makes Cli and Commands available in this module
include!("definition.rs");

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
