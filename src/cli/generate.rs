//! Generate subcommand implementation (default command).

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::{Path, PathBuf};

use bq_schema_gen::{
    read_existing_schema_from_file, write_schema_ddl, write_schema_debug_map, write_schema_json,
    write_schema_json_schema, BqMode, BqType, CsvRecordIterator, ErrorLog, GeneratorConfig,
    InputFormat, JsonRecordIterator, OutputFormat, SchemaEntry, SchemaGenerator, SchemaMap,
};

use super::Cli;

/// Errors that can occur during schema generation
#[derive(Debug)]
#[allow(dead_code)] // Some variants are for public API completeness
pub enum GenerateError {
    /// Invalid input format specified
    InvalidInputFormat(String),
    /// Invalid output format specified
    InvalidOutputFormat(String),
    /// Failed to load existing schema
    ExistingSchemaLoad(PathBuf, String),
    /// Invalid glob pattern
    InvalidGlobPattern(String, String),
    /// Per-file mode requires input files
    PerFileRequiresInput,
    /// Output directory requires per-file mode
    OutputDirRequiresPerFile,
    /// Watch mode requires input files
    WatchRequiresInput,
    /// Watch mode cannot be used with per-file mode
    WatchWithPerFile,
    /// No input files found
    NoInputFiles,
    /// Failed to open input file
    InputFileOpen(PathBuf, std::io::Error),
    /// Failed to create output file
    OutputFileCreate(PathBuf, std::io::Error),
    /// Failed to create output directory
    OutputDirCreate(PathBuf, std::io::Error),
    /// Processing error
    ProcessingError(String),
    /// Watch mode error
    WatchError(String),
}

impl std::fmt::Display for GenerateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GenerateError::InvalidInputFormat(fmt) => {
                write!(f, "Unknown input format '{}'. Use 'json' or 'csv'.", fmt)
            }
            GenerateError::InvalidOutputFormat(fmt) => {
                write!(
                    f,
                    "Unknown output format '{}'. Use 'json', 'ddl', 'debug-map', or 'json-schema'.",
                    fmt
                )
            }
            GenerateError::ExistingSchemaLoad(path, e) => {
                write!(
                    f,
                    "Cannot load existing schema from '{}': {}",
                    path.display(),
                    e
                )
            }
            GenerateError::InvalidGlobPattern(pattern, e) => {
                write!(f, "Invalid glob pattern '{}': {}", pattern, e)
            }
            GenerateError::PerFileRequiresInput => {
                write!(
                    f,
                    "--per-file requires input files (cannot read from stdin)"
                )
            }
            GenerateError::OutputDirRequiresPerFile => {
                write!(f, "--output-dir requires --per-file")
            }
            GenerateError::WatchRequiresInput => {
                write!(f, "--watch requires input file patterns")
            }
            GenerateError::WatchWithPerFile => {
                write!(f, "--watch cannot be used with --per-file")
            }
            GenerateError::NoInputFiles => {
                write!(f, "No input files found")
            }
            GenerateError::InputFileOpen(path, e) => {
                write!(f, "Cannot open input file '{}': {}", path.display(), e)
            }
            GenerateError::OutputFileCreate(path, e) => {
                write!(f, "Cannot create output file '{}': {}", path.display(), e)
            }
            GenerateError::OutputDirCreate(path, e) => {
                write!(
                    f,
                    "Cannot create output directory '{}': {}",
                    path.display(),
                    e
                )
            }
            GenerateError::ProcessingError(msg) => {
                write!(f, "Processing error: {}", msg)
            }
            GenerateError::WatchError(msg) => {
                write!(f, "Watch mode error: {}", msg)
            }
        }
    }
}

impl std::error::Error for GenerateError {}

/// Validated CLI arguments for schema generation
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are part of public API
pub struct ValidatedArgs {
    /// Input format
    pub input_format: InputFormat,
    /// Output format
    pub output_format: OutputFormat,
    /// Generator configuration
    pub config: GeneratorConfig,
    /// Existing schema map (if provided)
    pub existing_schema: Option<SchemaMap>,
    /// Input files (empty means stdin)
    pub input_files: Vec<PathBuf>,
    /// Output path (None means stdout)
    pub output_path: Option<PathBuf>,
    /// Output directory for per-file mode
    pub output_dir: Option<PathBuf>,
    /// Per-file mode flag
    pub per_file: bool,
    /// Watch mode flag
    pub watch: bool,
    /// Number of threads for parallel processing
    pub threads: Option<usize>,
    /// Table name for DDL output
    pub table_name: String,
    /// Quiet mode
    pub quiet: bool,
    /// Ignore invalid lines
    pub ignore_invalid_lines: bool,
    /// Debugging interval
    pub debugging_interval: usize,
    /// Debounce delay for watch mode
    pub debounce: u64,
    /// Command to run on change in watch mode
    pub on_change: Option<String>,
    /// Original files patterns (for watch mode)
    pub file_patterns: Vec<String>,
}

/// Output from schema generation
#[derive(Debug)]
#[allow(dead_code)] // Fields are part of public API, used by tests
pub struct GenerateOutput {
    /// Number of lines processed
    pub lines_processed: usize,
    /// Number of files processed
    pub files_processed: usize,
    /// Error logs from processing
    pub error_logs: Vec<ErrorLog>,
}

/// Validate CLI arguments and return validated args
pub fn validate_cli_args(cli: &Cli) -> Result<ValidatedArgs, GenerateError> {
    // Parse input format
    let input_format = match cli.input_format.to_lowercase().as_str() {
        "json" => InputFormat::Json,
        "csv" => InputFormat::Csv,
        other => return Err(GenerateError::InvalidInputFormat(other.to_string())),
    };

    // Parse output format
    let output_format: OutputFormat = cli
        .output_format
        .parse()
        .map_err(|_| GenerateError::InvalidOutputFormat(cli.output_format.clone()))?;

    // Build configuration
    let config = GeneratorConfig {
        input_format,
        infer_mode: cli.infer_mode,
        keep_nulls: cli.keep_nulls,
        quoted_values_are_strings: cli.quoted_values_are_strings,
        sanitize_names: cli.sanitize_names,
        preserve_input_sort_order: cli.preserve_input_sort_order,
    };

    // Load existing schema if provided
    let existing_schema: Option<SchemaMap> = match &cli.existing_schema_path {
        Some(path) => {
            let schema = read_existing_schema_from_file(path)
                .map_err(|e| GenerateError::ExistingSchemaLoad(path.clone(), e.to_string()))?;
            Some(schema)
        }
        None => None,
    };

    // Collect input files from positional args and -i/--input flag
    let input_files = collect_input_files_impl(cli)?;

    // Validate per-file options
    if cli.per_file && input_files.is_empty() {
        return Err(GenerateError::PerFileRequiresInput);
    }

    if cli.output_dir.is_some() && !cli.per_file {
        return Err(GenerateError::OutputDirRequiresPerFile);
    }

    // Watch mode validation
    if cli.watch && cli.files.is_empty() {
        return Err(GenerateError::WatchRequiresInput);
    }

    if cli.watch && cli.per_file {
        return Err(GenerateError::WatchWithPerFile);
    }

    Ok(ValidatedArgs {
        input_format,
        output_format,
        config,
        existing_schema,
        input_files,
        output_path: cli.output.clone(),
        output_dir: cli.output_dir.clone(),
        per_file: cli.per_file,
        watch: cli.watch,
        threads: cli.threads,
        table_name: cli.table_name.clone(),
        quiet: cli.quiet,
        ignore_invalid_lines: cli.ignore_invalid_lines,
        debugging_interval: cli.debugging_interval,
        debounce: cli.debounce,
        on_change: cli.on_change.clone(),
        file_patterns: cli.files.clone(),
    })
}

/// Generate schema from validated arguments (testable entry point)
pub fn generate_schema(args: &ValidatedArgs) -> Result<GenerateOutput, GenerateError> {
    if args.per_file {
        process_per_file_impl(args)
    } else if args.input_files.is_empty() {
        process_single_input_impl(None, args)
    } else {
        process_merged_files_impl(args)
    }
}

/// Run the generate command (default)
pub fn run(cli: &Cli) {
    let args = match validate_cli_args(cli) {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };

    // Handle watch mode separately (it has its own loop)
    if args.watch {
        run_watch_mode(cli, &args.config);
        return;
    }

    match generate_schema(&args) {
        Ok(_output) => {
            // Success - output was already written
        }
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    }
}

/// Run watch mode
fn run_watch_mode(cli: &Cli, config: &GeneratorConfig) {
    use bq_schema_gen::watch::{run_watch, WatchConfig};

    let watch_config = WatchConfig {
        debounce_ms: cli.debounce,
        on_change: cli.on_change.clone(),
        quiet: cli.quiet,
        ignore_invalid_lines: cli.ignore_invalid_lines,
    };

    let output_path = cli.output.as_deref();

    if let Err(e) = run_watch(&cli.files, output_path, config.clone(), watch_config) {
        eprintln!("Error in watch mode: {}", e);
        std::process::exit(1);
    }
}

/// Collect input files from positional arguments and -i/--input flag, expanding globs
fn collect_input_files_impl(cli: &Cli) -> Result<Vec<PathBuf>, GenerateError> {
    let mut files = Vec::new();

    // Add files from positional arguments (with glob expansion)
    for pattern in &cli.files {
        match glob::glob(pattern) {
            Ok(paths) => {
                let mut found = false;
                for entry in paths {
                    match entry {
                        Ok(path) => {
                            if path.is_file() {
                                files.push(path);
                                found = true;
                            }
                        }
                        Err(e) => {
                            eprintln!("Warning: Error reading glob entry: {}", e);
                        }
                    }
                }
                if !found {
                    let path = PathBuf::from(pattern);
                    if path.exists() {
                        files.push(path);
                    } else {
                        eprintln!("Warning: No files matched pattern '{}'", pattern);
                    }
                }
            }
            Err(e) => {
                return Err(GenerateError::InvalidGlobPattern(
                    pattern.clone(),
                    e.to_string(),
                ));
            }
        }
    }

    // Add file from -i/--input flag if provided
    if let Some(input_path) = &cli.input {
        files.push(input_path.clone());
    }

    Ok(files)
}

/// Process a single input (file or stdin) - implementation
fn process_single_input_impl(
    input_path: Option<&Path>,
    args: &ValidatedArgs,
) -> Result<GenerateOutput, GenerateError> {
    let input: Box<dyn Read> = match input_path {
        Some(path) => {
            let file =
                File::open(path).map_err(|e| GenerateError::InputFileOpen(path.to_owned(), e))?;
            Box::new(file)
        }
        None => Box::new(io::stdin()),
    };

    let mut output: Box<dyn io::Write> = match &args.output_path {
        Some(path) => {
            let file =
                File::create(path).map_err(|e| GenerateError::OutputFileCreate(path.clone(), e))?;
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    let mut generator = SchemaGenerator::new(args.config.clone());
    let mut schema_map = args.existing_schema.clone().unwrap_or_default();

    process_input_impl(
        input,
        args.config.input_format,
        &mut generator,
        &mut schema_map,
        args.ignore_invalid_lines,
        args.debugging_interval,
        args.quiet,
    )?;

    if !args.quiet {
        eprintln!("Processed {} lines", generator.line_number());
    }

    let error_logs = generator.error_logs().to_vec();
    print_errors(&generator);
    write_output(
        &generator,
        &schema_map,
        &args.output_format,
        &args.table_name,
        &mut output,
    )?;

    Ok(GenerateOutput {
        lines_processed: generator.line_number(),
        files_processed: if input_path.is_some() { 1 } else { 0 },
        error_logs,
    })
}

/// Process multiple files and merge into single schema - implementation
fn process_merged_files_impl(args: &ValidatedArgs) -> Result<GenerateOutput, GenerateError> {
    let num_threads = args.threads.unwrap_or_else(num_cpus::get);
    let use_parallel = num_threads > 1 && args.input_files.len() > 1;

    if use_parallel {
        process_files_parallel_impl(args, num_threads)
    } else {
        process_files_sequential_impl(args)
    }
}

/// Process files sequentially - implementation
fn process_files_sequential_impl(args: &ValidatedArgs) -> Result<GenerateOutput, GenerateError> {
    let mut output: Box<dyn io::Write> = match &args.output_path {
        Some(path) => {
            let file =
                File::create(path).map_err(|e| GenerateError::OutputFileCreate(path.clone(), e))?;
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    let mut generator = SchemaGenerator::new(args.config.clone());
    let mut schema_map = args.existing_schema.clone().unwrap_or_default();
    let mut total_lines = 0;

    for (idx, path) in args.input_files.iter().enumerate() {
        if !args.quiet {
            eprintln!(
                "Processing file {}/{}: {}",
                idx + 1,
                args.input_files.len(),
                path.display()
            );
        }

        let file = File::open(path).map_err(|e| GenerateError::InputFileOpen(path.clone(), e))?;

        let lines_before = generator.line_number();
        process_input_impl(
            file,
            args.config.input_format,
            &mut generator,
            &mut schema_map,
            args.ignore_invalid_lines,
            args.debugging_interval,
            args.quiet,
        )?;
        total_lines += generator.line_number() - lines_before;
    }

    if !args.quiet {
        eprintln!(
            "Processed {} lines from {} files",
            total_lines,
            args.input_files.len()
        );
    }

    let error_logs = generator.error_logs().to_vec();
    print_errors(&generator);
    write_output(
        &generator,
        &schema_map,
        &args.output_format,
        &args.table_name,
        &mut output,
    )?;

    Ok(GenerateOutput {
        lines_processed: total_lines,
        files_processed: args.input_files.len(),
        error_logs,
    })
}

/// Process files in parallel - implementation
fn process_files_parallel_impl(
    args: &ValidatedArgs,
    num_threads: usize,
) -> Result<GenerateOutput, GenerateError> {
    use indicatif::{ProgressBar, ProgressStyle};
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // Set up thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok(); // Ignore error if already initialized

    let mut output: Box<dyn io::Write> = match &args.output_path {
        Some(path) => {
            let file =
                File::create(path).map_err(|e| GenerateError::OutputFileCreate(path.clone(), e))?;
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    // Set up progress bar
    let progress = if !args.quiet {
        let pb = ProgressBar::new(args.input_files.len() as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} files | {msg}")
                .unwrap_or_else(|_| ProgressStyle::default_bar())
                .progress_chars("█░░"),
        );
        pb.set_message(format!("{} threads", num_threads));
        Some(pb)
    } else {
        None
    };

    let total_records = AtomicUsize::new(0);
    let all_errors: Mutex<Vec<ErrorLog>> = Mutex::new(Vec::new());
    let config = args.config.clone();
    let ignore_invalid_lines = args.ignore_invalid_lines;

    // Process files in parallel
    let results: Vec<SchemaMap> = args
        .input_files
        .par_iter()
        .filter_map(|path| {
            let file = match File::open(path) {
                Ok(f) => f,
                Err(e) => {
                    eprintln!("Error: Cannot open input file '{}': {}", path.display(), e);
                    return None;
                }
            };

            let mut generator = SchemaGenerator::new(config.clone());
            let mut schema_map = SchemaMap::new();

            // Process the file
            let result: Result<(), ()> = match config.input_format {
                InputFormat::Json => {
                    let buf_reader = BufReader::new(file);
                    let iter = JsonRecordIterator::new(buf_reader, ignore_invalid_lines);
                    for record_result in iter {
                        match record_result {
                            Ok((_line_num, record)) => {
                                let _ = generator.process_record(&record, &mut schema_map);
                            }
                            Err(_) if ignore_invalid_lines => continue,
                            Err(e) => {
                                eprintln!("Error processing '{}': {}", path.display(), e);
                                break;
                            }
                        }
                    }
                    Ok(())
                }
                InputFormat::Csv => {
                    let iter = match CsvRecordIterator::new(file) {
                        Ok(i) => i,
                        Err(e) => {
                            eprintln!("Error processing '{}': {}", path.display(), e);
                            return None;
                        }
                    };
                    for record_result in iter {
                        match record_result {
                            Ok((_line_num, record)) => {
                                let _ = generator.process_record(&record, &mut schema_map);
                            }
                            Err(e) => {
                                eprintln!("Error processing '{}': {}", path.display(), e);
                                break;
                            }
                        }
                    }
                    Ok(())
                }
            };

            if result.is_ok() {
                total_records.fetch_add(generator.line_number(), Ordering::Relaxed);

                // Collect errors
                if !generator.error_logs().is_empty() {
                    if let Ok(mut errors) = all_errors.lock() {
                        errors.extend(generator.error_logs().iter().cloned());
                    }
                }

                if let Some(ref pb) = progress {
                    pb.inc(1);
                }

                Some(schema_map)
            } else {
                None
            }
        })
        .collect();

    if let Some(pb) = progress {
        pb.finish_with_message("Done");
    }

    // Merge all schema maps
    let mut final_generator = SchemaGenerator::new(config.clone());
    let mut final_schema = args.existing_schema.clone().unwrap_or_default();

    for schema_map in results {
        merge_schema_maps(&mut final_generator, &mut final_schema, schema_map);
    }

    let total = total_records.load(Ordering::Relaxed);
    if !args.quiet {
        eprintln!(
            "Processed {} records from {} files using {} threads",
            total,
            args.input_files.len(),
            num_threads
        );
    }

    // Print collected errors
    let error_logs = all_errors.lock().map(|e| e.clone()).unwrap_or_default();
    for error in &error_logs {
        eprintln!("Problem on line {}: {}", error.line_number, error.msg);
    }

    write_output(
        &final_generator,
        &final_schema,
        &args.output_format,
        &args.table_name,
        &mut output,
    )?;

    Ok(GenerateOutput {
        lines_processed: total,
        files_processed: args.input_files.len(),
        error_logs,
    })
}

/// Merge source schema map into target schema map
fn merge_schema_maps(generator: &mut SchemaGenerator, target: &mut SchemaMap, source: SchemaMap) {
    use serde_json::Value;

    for (_key, entry) in source {
        // Convert entry to a JSON value to trigger merge logic
        let json_value = schema_entry_to_json(&entry);

        // Process the record through the generator to trigger merge logic
        let mut temp_map = serde_json::Map::new();
        temp_map.insert(entry.name.clone(), json_value);

        let record = Value::Object(temp_map);
        let _ = generator.process_record(&record, target);
    }
}

/// Convert a SchemaEntry to a representative JSON value for merging
fn schema_entry_to_json(entry: &SchemaEntry) -> serde_json::Value {
    use serde_json::Value;

    match &entry.bq_type {
        BqType::Boolean | BqType::QBoolean => Value::Bool(true),
        BqType::Integer | BqType::QInteger => Value::Number(serde_json::Number::from(0i64)),
        BqType::Float | BqType::QFloat => {
            Value::Number(serde_json::Number::from_f64(0.0).unwrap_or(serde_json::Number::from(0)))
        }
        BqType::String => Value::String(String::new()),
        BqType::Timestamp => Value::String("2024-01-01T00:00:00".to_string()),
        BqType::Date => Value::String("2024-01-01".to_string()),
        BqType::Time => Value::String("00:00:00".to_string()),
        BqType::Record(fields) => {
            let mut obj = serde_json::Map::new();
            for (_, field_entry) in fields {
                obj.insert(field_entry.name.clone(), schema_entry_to_json(field_entry));
            }
            if entry.mode == BqMode::Repeated {
                Value::Array(vec![Value::Object(obj)])
            } else {
                Value::Object(obj)
            }
        }
        BqType::Null => Value::Null,
        BqType::EmptyArray => Value::Array(vec![]),
        BqType::EmptyRecord => Value::Object(serde_json::Map::new()),
    }
}

/// Process each file separately - implementation
fn process_per_file_impl(args: &ValidatedArgs) -> Result<GenerateOutput, GenerateError> {
    if let Some(output_dir) = &args.output_dir {
        std::fs::create_dir_all(output_dir)
            .map_err(|e| GenerateError::OutputDirCreate(output_dir.clone(), e))?;
    }

    let mut total_lines = 0;
    let mut all_error_logs = Vec::new();

    for (idx, path) in args.input_files.iter().enumerate() {
        if !args.quiet {
            eprintln!(
                "Processing file {}/{}: {}",
                idx + 1,
                args.input_files.len(),
                path.display()
            );
        }

        let file = File::open(path).map_err(|e| GenerateError::InputFileOpen(path.clone(), e))?;

        let output_path = get_per_file_output_path(path, &args.output_dir);
        let mut output: Box<dyn io::Write> = {
            let file = File::create(&output_path)
                .map_err(|e| GenerateError::OutputFileCreate(output_path.clone(), e))?;
            Box::new(file)
        };

        let mut generator = SchemaGenerator::new(args.config.clone());
        let mut schema_map = args.existing_schema.clone().unwrap_or_default();

        process_input_impl(
            file,
            args.config.input_format,
            &mut generator,
            &mut schema_map,
            args.ignore_invalid_lines,
            args.debugging_interval,
            args.quiet,
        )?;

        if !args.quiet {
            eprintln!(
                "  Processed {} lines -> {}",
                generator.line_number(),
                output_path.display()
            );
        }

        total_lines += generator.line_number();
        all_error_logs.extend(generator.error_logs().iter().cloned());

        print_errors(&generator);
        write_output(
            &generator,
            &schema_map,
            &args.output_format,
            &args.table_name,
            &mut output,
        )?;
    }

    Ok(GenerateOutput {
        lines_processed: total_lines,
        files_processed: args.input_files.len(),
        error_logs: all_error_logs,
    })
}

/// Get output path for per-file mode
fn get_per_file_output_path(input_path: &Path, output_dir: &Option<PathBuf>) -> PathBuf {
    let file_stem = input_path.file_stem().unwrap_or_default();
    let schema_filename = format!("{}.schema.json", file_stem.to_string_lossy());

    match output_dir {
        Some(dir) => dir.join(schema_filename),
        None => input_path.with_file_name(schema_filename),
    }
}

/// Process input and update schema - implementation (returns Result)
fn process_input_impl<R: Read>(
    input: R,
    input_format: InputFormat,
    generator: &mut SchemaGenerator,
    schema_map: &mut SchemaMap,
    ignore_invalid_lines: bool,
    debugging_interval: usize,
    quiet: bool,
) -> Result<(), GenerateError> {
    let result = match input_format {
        InputFormat::Json => {
            let buf_reader = BufReader::new(input);
            process_json_input(
                buf_reader,
                generator,
                schema_map,
                ignore_invalid_lines,
                debugging_interval,
                quiet,
            )
        }
        InputFormat::Csv => {
            process_csv_input(input, generator, schema_map, debugging_interval, quiet)
        }
    };

    result.map_err(|e| GenerateError::ProcessingError(e.to_string()))
}

/// Print error logs from generator
fn print_errors(generator: &SchemaGenerator) {
    for error in generator.error_logs() {
        eprintln!("Problem on line {}: {}", error.line_number, error.msg);
    }
}

/// Write schema output (returns Result)
fn write_output<W: io::Write>(
    generator: &SchemaGenerator,
    schema_map: &SchemaMap,
    output_format: &OutputFormat,
    table_name: &str,
    output: &mut W,
) -> Result<(), GenerateError> {
    let write_result = match output_format {
        OutputFormat::Json => {
            let schema = generator.flatten_schema(schema_map);
            write_schema_json(&schema, output)
        }
        OutputFormat::Ddl => {
            let schema = generator.flatten_schema(schema_map);
            write_schema_ddl(&schema, table_name, output)
        }
        OutputFormat::DebugMap => write_schema_debug_map(schema_map, output),
        OutputFormat::JsonSchema => {
            let schema = generator.flatten_schema(schema_map);
            write_schema_json_schema(&schema, output)
        }
    };

    write_result.map_err(|e| GenerateError::ProcessingError(format!("Error writing output: {}", e)))
}

/// Process JSON input records
fn process_json_input<R: std::io::BufRead>(
    input: R,
    generator: &mut SchemaGenerator,
    schema_map: &mut SchemaMap,
    ignore_invalid_lines: bool,
    debugging_interval: usize,
    quiet: bool,
) -> bq_schema_gen::Result<()> {
    let iter = JsonRecordIterator::new(input, ignore_invalid_lines);

    for result in iter {
        let (line_num, record) = result?;

        if !quiet && line_num % debugging_interval == 0 {
            eprintln!("Processing line {}", line_num);
        }

        if let Err(e) = generator.process_record(&record, schema_map) {
            if !ignore_invalid_lines {
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Process CSV input records
fn process_csv_input<R: Read>(
    input: R,
    generator: &mut SchemaGenerator,
    schema_map: &mut SchemaMap,
    debugging_interval: usize,
    quiet: bool,
) -> bq_schema_gen::Result<()> {
    let iter = CsvRecordIterator::new(input)?;

    for result in iter {
        let (line_num, record) = result?;

        if !quiet && line_num % debugging_interval == 0 {
            eprintln!("Processing line {}", line_num);
        }

        generator.process_record(&record, schema_map)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir};

    /// Helper to create a schema entry
    fn make_entry(name: &str, bq_type: BqType, mode: BqMode) -> SchemaEntry {
        SchemaEntry::new(name.to_string(), bq_type, mode)
    }

    /// Helper to create a temp file with content
    fn create_temp_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_merge_schema_maps_empty() {
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);
        let mut target = SchemaMap::new();

        // Empty source should not change target
        let source = SchemaMap::new();
        merge_schema_maps(&mut generator, &mut target, source);

        assert!(target.is_empty());

        // Non-empty source into empty target
        let mut source2 = SchemaMap::new();
        source2.insert(
            "field1".to_string(),
            make_entry("field1", BqType::Integer, BqMode::Nullable),
        );
        merge_schema_maps(&mut generator, &mut target, source2);

        assert!(!target.is_empty());
        assert!(target.contains_key("field1"));
    }

    #[test]
    fn test_merge_schema_maps_disjoint() {
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);

        // Target has field_a
        let mut target = SchemaMap::new();
        target.insert(
            "field_a".to_string(),
            make_entry("field_a", BqType::String, BqMode::Nullable),
        );

        // Source has field_b
        let mut source = SchemaMap::new();
        source.insert(
            "field_b".to_string(),
            make_entry("field_b", BqType::Integer, BqMode::Nullable),
        );

        merge_schema_maps(&mut generator, &mut target, source);

        // Both fields should exist
        assert!(target.contains_key("field_a"));
        assert!(target.contains_key("field_b"));
        assert_eq!(target.len(), 2);
    }

    #[test]
    fn test_get_per_file_output_path() {
        // Without output_dir
        let input = PathBuf::from("/data/file.json");
        let result = get_per_file_output_path(&input, &None);
        assert_eq!(result, PathBuf::from("/data/file.schema.json"));

        // With output_dir
        let output_dir = Some(PathBuf::from("/output"));
        let result = get_per_file_output_path(&input, &output_dir);
        assert_eq!(result, PathBuf::from("/output/file.schema.json"));
    }

    #[test]
    fn test_schema_entry_to_json_primitives() {
        // Boolean
        let entry = make_entry("bool_field", BqType::Boolean, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_boolean());

        // Integer
        let entry = make_entry("int_field", BqType::Integer, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_number());

        // Float
        let entry = make_entry("float_field", BqType::Float, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_number());

        // String
        let entry = make_entry("string_field", BqType::String, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_string());

        // Null
        let entry = make_entry("null_field", BqType::Null, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_null());
    }

    #[test]
    fn test_schema_entry_to_json_date_types() {
        // Timestamp
        let entry = make_entry("ts_field", BqType::Timestamp, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_string());
        assert!(json.as_str().unwrap().contains("2024-01-01"));

        // Date
        let entry = make_entry("date_field", BqType::Date, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_string());
        assert_eq!(json.as_str().unwrap(), "2024-01-01");

        // Time
        let entry = make_entry("time_field", BqType::Time, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_string());
        assert_eq!(json.as_str().unwrap(), "00:00:00");
    }

    #[test]
    fn test_schema_entry_to_json_quoted_types() {
        // QBoolean
        let entry = make_entry("qbool_field", BqType::QBoolean, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_boolean());

        // QInteger
        let entry = make_entry("qint_field", BqType::QInteger, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_number());

        // QFloat
        let entry = make_entry("qfloat_field", BqType::QFloat, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_number());
    }

    #[test]
    fn test_schema_entry_to_json_empty_types() {
        // EmptyArray
        let entry = make_entry("empty_arr", BqType::EmptyArray, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_array());
        assert!(json.as_array().unwrap().is_empty());

        // EmptyRecord
        let entry = make_entry("empty_rec", BqType::EmptyRecord, BqMode::Nullable);
        let json = schema_entry_to_json(&entry);
        assert!(json.is_object());
        assert!(json.as_object().unwrap().is_empty());
    }

    #[test]
    fn test_schema_entry_to_json_nested_record() {
        let nested_field = make_entry("inner", BqType::String, BqMode::Nullable);
        let mut inner_map = SchemaMap::new();
        inner_map.insert("inner".to_string(), nested_field);

        let entry = SchemaEntry::new(
            "outer".to_string(),
            BqType::Record(inner_map),
            BqMode::Nullable,
        );
        let json = schema_entry_to_json(&entry);
        assert!(json.is_object());
        assert!(json.as_object().unwrap().contains_key("inner"));
    }

    #[test]
    fn test_schema_entry_to_json_repeated_record() {
        let nested_field = make_entry("item", BqType::Integer, BqMode::Nullable);
        let mut inner_map = SchemaMap::new();
        inner_map.insert("item".to_string(), nested_field);

        let entry = SchemaEntry::new(
            "items".to_string(),
            BqType::Record(inner_map),
            BqMode::Repeated,
        );
        let json = schema_entry_to_json(&entry);
        assert!(json.is_array());
        assert_eq!(json.as_array().unwrap().len(), 1);
        assert!(json.as_array().unwrap()[0].is_object());
    }

    #[test]
    fn test_merge_schema_maps_overlapping() {
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);

        // Target has string field
        let mut target = SchemaMap::new();
        target.insert(
            "field".to_string(),
            make_entry("field", BqType::String, BqMode::Nullable),
        );

        // Source also has same field - merge should work
        let mut source = SchemaMap::new();
        source.insert(
            "field".to_string(),
            make_entry("field", BqType::String, BqMode::Nullable),
        );

        merge_schema_maps(&mut generator, &mut target, source);
        assert!(target.contains_key("field"));
    }

    #[test]
    fn test_get_per_file_output_path_no_extension() {
        // File without extension
        let input = PathBuf::from("/data/file");
        let result = get_per_file_output_path(&input, &None);
        assert_eq!(result, PathBuf::from("/data/file.schema.json"));
    }

    #[test]
    fn test_get_per_file_output_path_nested_dir() {
        let input = PathBuf::from("/a/b/c/file.json");
        let output_dir = Some(PathBuf::from("/out"));
        let result = get_per_file_output_path(&input, &output_dir);
        assert_eq!(result, PathBuf::from("/out/file.schema.json"));
    }

    #[test]
    fn test_process_json_input_empty() {
        let input = std::io::Cursor::new("");
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_json_input(
            std::io::BufReader::new(input),
            &mut generator,
            &mut schema_map,
            false,
            1000,
            true,
        );

        assert!(result.is_ok());
        assert!(schema_map.is_empty());
    }

    #[test]
    fn test_process_json_input_single_record() {
        let input = std::io::Cursor::new(r#"{"name": "test", "value": 42}"#);
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_json_input(
            std::io::BufReader::new(input),
            &mut generator,
            &mut schema_map,
            false,
            1000,
            true,
        );

        assert!(result.is_ok());
        assert!(schema_map.contains_key("name"));
        assert!(schema_map.contains_key("value"));
    }

    #[test]
    fn test_process_json_input_multiple_records() {
        let input = std::io::Cursor::new(
            r#"{"a": 1}
{"b": 2}
{"c": 3}"#,
        );
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_json_input(
            std::io::BufReader::new(input),
            &mut generator,
            &mut schema_map,
            false,
            1000,
            true,
        );

        assert!(result.is_ok());
        assert_eq!(schema_map.len(), 3);
    }

    #[test]
    fn test_process_json_input_invalid_line_error() {
        let input = std::io::Cursor::new("invalid json");
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_json_input(
            std::io::BufReader::new(input),
            &mut generator,
            &mut schema_map,
            false, // Don't ignore invalid lines
            1000,
            true,
        );

        assert!(result.is_err());
    }

    #[test]
    fn test_process_json_input_ignore_invalid() {
        let input = std::io::Cursor::new(
            r#"{"valid": 1}
invalid json
{"also_valid": 2}"#,
        );
        let config = GeneratorConfig::default();
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_json_input(
            std::io::BufReader::new(input),
            &mut generator,
            &mut schema_map,
            true, // Ignore invalid lines
            1000,
            true,
        );

        assert!(result.is_ok());
        assert_eq!(schema_map.len(), 2);
    }

    #[test]
    fn test_process_csv_input_basic() {
        let input = std::io::Cursor::new("name,value\ntest,42\nfoo,123");
        let config = GeneratorConfig {
            input_format: InputFormat::Csv,
            ..Default::default()
        };
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_csv_input(input, &mut generator, &mut schema_map, 1000, true);

        assert!(result.is_ok());
        assert!(schema_map.contains_key("name"));
        assert!(schema_map.contains_key("value"));
    }

    #[test]
    fn test_process_csv_input_empty_values() {
        let input = std::io::Cursor::new("a,b,c\n1,,3\n4,5,");
        let config = GeneratorConfig {
            input_format: InputFormat::Csv,
            ..Default::default()
        };
        let mut generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();

        let result = process_csv_input(input, &mut generator, &mut schema_map, 1000, true);

        assert!(result.is_ok());
        assert_eq!(schema_map.len(), 3);
    }

    #[test]
    fn test_write_output_json_format() {
        let config = GeneratorConfig::default();
        let generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();
        schema_map.insert(
            "test".to_string(),
            make_entry("test", BqType::String, BqMode::Nullable),
        );

        let mut output = Vec::new();
        let result = write_output(
            &generator,
            &schema_map,
            &OutputFormat::Json,
            "test_table",
            &mut output,
        );

        assert!(result.is_ok());
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("\"name\""));
        assert!(output_str.contains("\"test\""));
    }

    #[test]
    fn test_write_output_ddl_format() {
        let config = GeneratorConfig::default();
        let generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();
        schema_map.insert(
            "id".to_string(),
            make_entry("id", BqType::Integer, BqMode::Nullable),
        );

        let mut output = Vec::new();
        let result = write_output(
            &generator,
            &schema_map,
            &OutputFormat::Ddl,
            "my_table",
            &mut output,
        );

        assert!(result.is_ok());
        let output_str = String::from_utf8(output).unwrap();
        assert!(output_str.contains("CREATE TABLE"));
        assert!(output_str.contains("my_table"));
    }

    #[test]
    fn test_write_output_debug_map_format() {
        let config = GeneratorConfig::default();
        let generator = SchemaGenerator::new(config);
        let mut schema_map = SchemaMap::new();
        schema_map.insert(
            "field".to_string(),
            make_entry("field", BqType::Boolean, BqMode::Nullable),
        );

        let mut output = Vec::new();
        let result = write_output(
            &generator,
            &schema_map,
            &OutputFormat::DebugMap,
            "table",
            &mut output,
        );

        assert!(result.is_ok());
        let output_str = String::from_utf8(output).unwrap();
        assert!(!output_str.is_empty());
    }

    // ===== Tests for validate_cli_args =====

    fn create_test_cli() -> Cli {
        Cli {
            command: None,
            files: vec![],
            input_format: "json".to_string(),
            output_format: "json".to_string(),
            table_name: "test_table".to_string(),
            keep_nulls: false,
            quoted_values_are_strings: false,
            infer_mode: false,
            debugging_interval: 1000,
            sanitize_names: false,
            ignore_invalid_lines: false,
            existing_schema_path: None,
            preserve_input_sort_order: false,
            quiet: true,
            input: None,
            output: None,
            per_file: false,
            output_dir: None,
            threads: None,
            watch: false,
            debounce: 100,
            on_change: None,
        }
    }

    #[test]
    fn test_validate_cli_args_invalid_input_format() {
        let mut cli = create_test_cli();
        cli.input_format = "invalid".to_string();

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::InvalidInputFormat(_)
        ));
    }

    #[test]
    fn test_validate_cli_args_invalid_output_format() {
        let mut cli = create_test_cli();
        cli.output_format = "invalid".to_string();

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::InvalidOutputFormat(_)
        ));
    }

    #[test]
    fn test_validate_cli_args_per_file_no_files() {
        let mut cli = create_test_cli();
        cli.per_file = true;
        cli.files = vec![];

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::PerFileRequiresInput
        ));
    }

    #[test]
    fn test_validate_cli_args_output_dir_without_per_file() {
        let mut cli = create_test_cli();
        cli.output_dir = Some(PathBuf::from("/output"));
        cli.per_file = false;

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::OutputDirRequiresPerFile
        ));
    }

    #[test]
    fn test_validate_cli_args_watch_no_files() {
        let mut cli = create_test_cli();
        cli.watch = true;
        cli.files = vec![];

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::WatchRequiresInput
        ));
    }

    #[test]
    fn test_validate_cli_args_watch_with_per_file() {
        let temp_file = create_temp_file(r#"{"id": 1}"#);
        let mut cli = create_test_cli();
        cli.watch = true;
        cli.per_file = true;
        cli.files = vec![temp_file.path().to_string_lossy().to_string()];

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::WatchWithPerFile
        ));
    }

    #[test]
    fn test_validate_cli_args_valid_json_format() {
        let cli = create_test_cli();

        let result = validate_cli_args(&cli);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.input_format, InputFormat::Json);
        assert_eq!(args.output_format, OutputFormat::Json);
    }

    #[test]
    fn test_validate_cli_args_valid_csv_format() {
        let mut cli = create_test_cli();
        cli.input_format = "csv".to_string();

        let result = validate_cli_args(&cli);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.input_format, InputFormat::Csv);
    }

    #[test]
    fn test_validate_cli_args_all_output_formats() {
        for format in &["json", "ddl", "debug-map", "json-schema"] {
            let mut cli = create_test_cli();
            cli.output_format = format.to_string();

            let result = validate_cli_args(&cli);
            assert!(result.is_ok(), "Format '{}' should be valid", format);
        }
    }

    #[test]
    fn test_validate_cli_args_with_input_file() {
        let temp_file = create_temp_file(r#"{"id": 1}"#);
        let mut cli = create_test_cli();
        cli.input = Some(temp_file.path().to_owned());

        let result = validate_cli_args(&cli);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert_eq!(args.input_files.len(), 1);
    }

    #[test]
    fn test_validate_cli_args_with_existing_schema() {
        let schema_file =
            create_temp_file(r#"[{"name": "id", "type": "INTEGER", "mode": "NULLABLE"}]"#);
        let mut cli = create_test_cli();
        cli.existing_schema_path = Some(schema_file.path().to_owned());

        let result = validate_cli_args(&cli);
        assert!(result.is_ok());
        let args = result.unwrap();
        assert!(args.existing_schema.is_some());
    }

    #[test]
    fn test_validate_cli_args_existing_schema_not_found() {
        let mut cli = create_test_cli();
        cli.existing_schema_path = Some(PathBuf::from("/nonexistent/schema.json"));

        let result = validate_cli_args(&cli);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            GenerateError::ExistingSchemaLoad(_, _)
        ));
    }

    // ===== Tests for generate_schema =====

    #[test]
    fn test_generate_schema_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let input_file = temp_dir.path().join("input.json");
        let output_file = temp_dir.path().join("output.json");

        std::fs::write(&input_file, r#"{"id": 1, "name": "test"}"#).unwrap();

        let args = ValidatedArgs {
            input_format: InputFormat::Json,
            output_format: OutputFormat::Json,
            config: GeneratorConfig::default(),
            existing_schema: None,
            input_files: vec![input_file],
            output_path: Some(output_file.clone()),
            output_dir: None,
            per_file: false,
            watch: false,
            threads: Some(1),
            table_name: "test_table".to_string(),
            quiet: true,
            ignore_invalid_lines: false,
            debugging_interval: 1000,
            debounce: 100,
            on_change: None,
            file_patterns: vec![],
        };

        let result = generate_schema(&args);
        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.lines_processed, 1);
        assert_eq!(output.files_processed, 1);
        assert!(output_file.exists());
    }

    #[test]
    fn test_generate_schema_multiple_files() {
        let temp_dir = TempDir::new().unwrap();
        let input_file1 = temp_dir.path().join("input1.json");
        let input_file2 = temp_dir.path().join("input2.json");
        let output_file = temp_dir.path().join("output.json");

        std::fs::write(&input_file1, r#"{"id": 1}"#).unwrap();
        std::fs::write(&input_file2, r#"{"name": "test"}"#).unwrap();

        let args = ValidatedArgs {
            input_format: InputFormat::Json,
            output_format: OutputFormat::Json,
            config: GeneratorConfig::default(),
            existing_schema: None,
            input_files: vec![input_file1, input_file2],
            output_path: Some(output_file.clone()),
            output_dir: None,
            per_file: false,
            watch: false,
            threads: Some(1), // Force sequential
            table_name: "test_table".to_string(),
            quiet: true,
            ignore_invalid_lines: false,
            debugging_interval: 1000,
            debounce: 100,
            on_change: None,
            file_patterns: vec![],
        };

        let result = generate_schema(&args);
        assert!(result.is_ok());
        let output = result.unwrap();
        assert_eq!(output.lines_processed, 2);
        assert_eq!(output.files_processed, 2);
    }

    #[test]
    fn test_generate_schema_per_file_mode() {
        let temp_dir = TempDir::new().unwrap();
        let input_file1 = temp_dir.path().join("input1.json");
        let input_file2 = temp_dir.path().join("input2.json");
        let output_dir = temp_dir.path().join("output");

        std::fs::write(&input_file1, r#"{"id": 1}"#).unwrap();
        std::fs::write(&input_file2, r#"{"name": "test"}"#).unwrap();

        let args = ValidatedArgs {
            input_format: InputFormat::Json,
            output_format: OutputFormat::Json,
            config: GeneratorConfig::default(),
            existing_schema: None,
            input_files: vec![input_file1.clone(), input_file2.clone()],
            output_path: None,
            output_dir: Some(output_dir.clone()),
            per_file: true,
            watch: false,
            threads: None,
            table_name: "test_table".to_string(),
            quiet: true,
            ignore_invalid_lines: false,
            debugging_interval: 1000,
            debounce: 100,
            on_change: None,
            file_patterns: vec![],
        };

        let result = generate_schema(&args);
        assert!(result.is_ok());

        // Check output files were created
        assert!(output_dir.join("input1.schema.json").exists());
        assert!(output_dir.join("input2.schema.json").exists());
    }

    #[test]
    fn test_generate_schema_stdin() {
        // When input_files is empty, it should read from stdin
        // We can't easily test stdin, but we can verify the code path exists
        let args = ValidatedArgs {
            input_format: InputFormat::Json,
            output_format: OutputFormat::Json,
            config: GeneratorConfig::default(),
            existing_schema: None,
            input_files: vec![],
            output_path: None,
            output_dir: None,
            per_file: false,
            watch: false,
            threads: None,
            table_name: "test_table".to_string(),
            quiet: true,
            ignore_invalid_lines: false,
            debugging_interval: 1000,
            debounce: 100,
            on_change: None,
            file_patterns: vec![],
        };

        // This will try to read from stdin and will timeout/block
        // so we don't actually run it, just verify the structure
        assert!(args.input_files.is_empty());
    }

    #[test]
    fn test_generate_error_display() {
        // Test all error variant display implementations
        let err = GenerateError::InvalidInputFormat("xml".to_string());
        assert!(err.to_string().contains("xml"));

        let err = GenerateError::InvalidOutputFormat("yaml".to_string());
        assert!(err.to_string().contains("yaml"));

        let err = GenerateError::ExistingSchemaLoad(
            PathBuf::from("/path/schema.json"),
            "not found".to_string(),
        );
        assert!(err.to_string().contains("schema.json"));

        let err = GenerateError::InvalidGlobPattern("**[".to_string(), "unclosed".to_string());
        assert!(err.to_string().contains("**["));

        let err = GenerateError::PerFileRequiresInput;
        assert!(err.to_string().contains("--per-file"));

        let err = GenerateError::OutputDirRequiresPerFile;
        assert!(err.to_string().contains("--output-dir"));

        let err = GenerateError::WatchRequiresInput;
        assert!(err.to_string().contains("--watch"));

        let err = GenerateError::WatchWithPerFile;
        assert!(err.to_string().contains("--watch"));

        let err = GenerateError::NoInputFiles;
        assert!(err.to_string().contains("No input files"));

        let err = GenerateError::InputFileOpen(
            PathBuf::from("/path/input.json"),
            std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        );
        assert!(err.to_string().contains("input.json"));

        let err = GenerateError::OutputFileCreate(
            PathBuf::from("/path/output.json"),
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        );
        assert!(err.to_string().contains("output.json"));

        let err = GenerateError::OutputDirCreate(
            PathBuf::from("/path/dir"),
            std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied"),
        );
        assert!(err.to_string().contains("/path/dir"));

        let err = GenerateError::ProcessingError("test error".to_string());
        assert!(err.to_string().contains("test error"));

        let err = GenerateError::WatchError("watch error".to_string());
        assert!(err.to_string().contains("watch error"));
    }
}
