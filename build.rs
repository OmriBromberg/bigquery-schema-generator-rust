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

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate_to, Shell};

// Include the CLI definition from the shared file
// This ensures completions stay in sync with the actual CLI
include!("src/cli/definition.rs");

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

    // Re-run if CLI definition changes
    println!("cargo:rerun-if-changed=src/cli/definition.rs");

    // Get output directory
    let out_dir = match env::var_os("OUT_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => return,
    };

    // Create completions directory
    let completions_dir = out_dir.join("completions");
    fs::create_dir_all(&completions_dir).expect("Failed to create completions directory");

    // Generate shell completions
    let mut cmd = Cli::command();
    for shell in Shell::value_variants() {
        generate_to(*shell, &mut cmd, "bq-schema-gen", &completions_dir)
            .expect("Failed to generate shell completions");
    }

    // Create man page directory
    let man_dir = out_dir.join("man");
    fs::create_dir_all(&man_dir).expect("Failed to create man directory");

    // Generate man page
    let cmd = Cli::command();
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
    println!("cargo:warning=Man page generated at: {}", man_dir.display());
}
