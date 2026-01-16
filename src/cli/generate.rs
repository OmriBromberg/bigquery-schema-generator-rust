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

/// Run the generate command (default)
pub fn run(cli: &Cli) {
    // Parse input format
    let input_format = match cli.input_format.to_lowercase().as_str() {
        "json" => InputFormat::Json,
        "csv" => InputFormat::Csv,
        other => {
            eprintln!(
                "Error: Unknown input format '{}'. Use 'json' or 'csv'.",
                other
            );
            std::process::exit(1);
        }
    };

    // Parse output format
    let output_format: OutputFormat = cli.output_format.parse().unwrap_or_else(|_| {
        eprintln!(
            "Error: Unknown output format '{}'. Use 'json', 'ddl', 'debug-map', or 'json-schema'.",
            cli.output_format
        );
        std::process::exit(1);
    });

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
            let schema = read_existing_schema_from_file(path).unwrap_or_else(|e| {
                eprintln!(
                    "Error: Cannot load existing schema from '{}': {}",
                    path.display(),
                    e
                );
                std::process::exit(1);
            });
            Some(schema)
        }
        None => None,
    };

    // Collect input files from positional args and -i/--input flag
    let input_files = collect_input_files(cli);

    // Validate per-file options
    if cli.per_file && input_files.is_empty() {
        eprintln!("Error: --per-file requires input files (cannot read from stdin)");
        std::process::exit(1);
    }

    if cli.output_dir.is_some() && !cli.per_file {
        eprintln!("Error: --output-dir requires --per-file");
        std::process::exit(1);
    }

    // Watch mode validation
    if cli.watch && cli.files.is_empty() {
        eprintln!("Error: --watch requires input file patterns");
        std::process::exit(1);
    }

    if cli.watch && cli.per_file {
        eprintln!("Error: --watch cannot be used with --per-file");
        std::process::exit(1);
    }

    // Handle watch mode
    if cli.watch {
        run_watch_mode(cli, &config);
        return;
    }

    if cli.per_file {
        process_per_file(&input_files, &config, &output_format, cli, existing_schema);
    } else if input_files.is_empty() {
        process_single_input(None, &config, &output_format, cli, existing_schema);
    } else {
        process_merged_files(&input_files, &config, &output_format, cli, existing_schema);
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
fn collect_input_files(cli: &Cli) -> Vec<PathBuf> {
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
                eprintln!("Error: Invalid glob pattern '{}': {}", pattern, e);
                std::process::exit(1);
            }
        }
    }

    // Add file from -i/--input flag if provided
    if let Some(input_path) = &cli.input {
        files.push(input_path.clone());
    }

    files
}

/// Process a single input (file or stdin)
fn process_single_input(
    input_path: Option<&Path>,
    config: &GeneratorConfig,
    output_format: &OutputFormat,
    cli: &Cli,
    existing_schema: Option<SchemaMap>,
) {
    let input: Box<dyn Read> = match input_path {
        Some(path) => {
            let file = File::open(path).unwrap_or_else(|e| {
                eprintln!("Error: Cannot open input file '{}': {}", path.display(), e);
                std::process::exit(1);
            });
            Box::new(file)
        }
        None => Box::new(io::stdin()),
    };

    let mut output: Box<dyn io::Write> = match &cli.output {
        Some(path) => {
            let file = File::create(path).unwrap_or_else(|e| {
                eprintln!(
                    "Error: Cannot create output file '{}': {}",
                    path.display(),
                    e
                );
                std::process::exit(1);
            });
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    let mut generator = SchemaGenerator::new(config.clone());
    let mut schema_map = existing_schema.unwrap_or_default();

    process_input(
        input,
        config.input_format,
        &mut generator,
        &mut schema_map,
        cli.ignore_invalid_lines,
        cli.debugging_interval,
        cli.quiet,
    );

    if !cli.quiet {
        eprintln!("Processed {} lines", generator.line_number());
    }

    print_errors(&generator);
    write_output(
        &generator,
        &schema_map,
        output_format,
        &cli.table_name,
        &mut output,
    );
}

/// Process multiple files and merge into single schema
fn process_merged_files(
    input_files: &[PathBuf],
    config: &GeneratorConfig,
    output_format: &OutputFormat,
    cli: &Cli,
    existing_schema: Option<SchemaMap>,
) {
    let num_threads = cli.threads.unwrap_or_else(num_cpus::get);
    let use_parallel = num_threads > 1 && input_files.len() > 1;

    if use_parallel {
        process_files_parallel(
            input_files,
            config,
            output_format,
            cli,
            existing_schema,
            num_threads,
        );
    } else {
        process_files_sequential(input_files, config, output_format, cli, existing_schema);
    }
}

/// Process files sequentially (original behavior)
fn process_files_sequential(
    input_files: &[PathBuf],
    config: &GeneratorConfig,
    output_format: &OutputFormat,
    cli: &Cli,
    existing_schema: Option<SchemaMap>,
) {
    let mut output: Box<dyn io::Write> = match &cli.output {
        Some(path) => {
            let file = File::create(path).unwrap_or_else(|e| {
                eprintln!(
                    "Error: Cannot create output file '{}': {}",
                    path.display(),
                    e
                );
                std::process::exit(1);
            });
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    let mut generator = SchemaGenerator::new(config.clone());
    let mut schema_map = existing_schema.unwrap_or_default();
    let mut total_lines = 0;

    for (idx, path) in input_files.iter().enumerate() {
        if !cli.quiet {
            eprintln!(
                "Processing file {}/{}: {}",
                idx + 1,
                input_files.len(),
                path.display()
            );
        }

        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("Error: Cannot open input file '{}': {}", path.display(), e);
            std::process::exit(1);
        });

        let lines_before = generator.line_number();
        process_input(
            file,
            config.input_format,
            &mut generator,
            &mut schema_map,
            cli.ignore_invalid_lines,
            cli.debugging_interval,
            cli.quiet,
        );
        total_lines += generator.line_number() - lines_before;
    }

    if !cli.quiet {
        eprintln!(
            "Processed {} lines from {} files",
            total_lines,
            input_files.len()
        );
    }

    print_errors(&generator);
    write_output(
        &generator,
        &schema_map,
        output_format,
        &cli.table_name,
        &mut output,
    );
}

/// Process files in parallel using rayon
fn process_files_parallel(
    input_files: &[PathBuf],
    config: &GeneratorConfig,
    output_format: &OutputFormat,
    cli: &Cli,
    existing_schema: Option<SchemaMap>,
    num_threads: usize,
) {
    use indicatif::{ProgressBar, ProgressStyle};
    use rayon::prelude::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    // Set up thread pool
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_threads)
        .build_global()
        .ok(); // Ignore error if already initialized

    let mut output: Box<dyn io::Write> = match &cli.output {
        Some(path) => {
            let file = File::create(path).unwrap_or_else(|e| {
                eprintln!(
                    "Error: Cannot create output file '{}': {}",
                    path.display(),
                    e
                );
                std::process::exit(1);
            });
            Box::new(file)
        }
        None => Box::new(io::stdout()),
    };

    // Set up progress bar
    let progress = if !cli.quiet {
        let pb = ProgressBar::new(input_files.len() as u64);
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

    // Process files in parallel
    let results: Vec<SchemaMap> = input_files
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
                    let iter = JsonRecordIterator::new(buf_reader, cli.ignore_invalid_lines);
                    for record_result in iter {
                        match record_result {
                            Ok((_line_num, record)) => {
                                let _ = generator.process_record(&record, &mut schema_map);
                            }
                            Err(_) if cli.ignore_invalid_lines => continue,
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
    let mut final_schema = existing_schema.unwrap_or_default();

    for schema_map in results {
        merge_schema_maps(&mut final_generator, &mut final_schema, schema_map);
    }

    let total = total_records.load(Ordering::Relaxed);
    if !cli.quiet {
        eprintln!(
            "Processed {} records from {} files using {} threads",
            total,
            input_files.len(),
            num_threads
        );
    }

    // Print collected errors
    if let Ok(errors) = all_errors.lock() {
        for error in errors.iter() {
            eprintln!("Problem on line {}: {}", error.line_number, error.msg);
        }
    }

    write_output(
        &final_generator,
        &final_schema,
        output_format,
        &cli.table_name,
        &mut output,
    );
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

/// Process each file separately, outputting separate schemas
fn process_per_file(
    input_files: &[PathBuf],
    config: &GeneratorConfig,
    output_format: &OutputFormat,
    cli: &Cli,
    existing_schema: Option<SchemaMap>,
) {
    if let Some(output_dir) = &cli.output_dir {
        std::fs::create_dir_all(output_dir).unwrap_or_else(|e| {
            eprintln!(
                "Error: Cannot create output directory '{}': {}",
                output_dir.display(),
                e
            );
            std::process::exit(1);
        });
    }

    for (idx, path) in input_files.iter().enumerate() {
        if !cli.quiet {
            eprintln!(
                "Processing file {}/{}: {}",
                idx + 1,
                input_files.len(),
                path.display()
            );
        }

        let file = File::open(path).unwrap_or_else(|e| {
            eprintln!("Error: Cannot open input file '{}': {}", path.display(), e);
            std::process::exit(1);
        });

        let output_path = get_per_file_output_path(path, &cli.output_dir);
        let mut output: Box<dyn io::Write> = {
            let file = File::create(&output_path).unwrap_or_else(|e| {
                eprintln!(
                    "Error: Cannot create output file '{}': {}",
                    output_path.display(),
                    e
                );
                std::process::exit(1);
            });
            Box::new(file)
        };

        let mut generator = SchemaGenerator::new(config.clone());
        let mut schema_map = existing_schema.clone().unwrap_or_default();

        process_input(
            file,
            config.input_format,
            &mut generator,
            &mut schema_map,
            cli.ignore_invalid_lines,
            cli.debugging_interval,
            cli.quiet,
        );

        if !cli.quiet {
            eprintln!(
                "  Processed {} lines -> {}",
                generator.line_number(),
                output_path.display()
            );
        }

        print_errors(&generator);
        write_output(
            &generator,
            &schema_map,
            output_format,
            &cli.table_name,
            &mut output,
        );
    }
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

/// Process input and update schema
fn process_input<R: Read>(
    input: R,
    input_format: InputFormat,
    generator: &mut SchemaGenerator,
    schema_map: &mut SchemaMap,
    ignore_invalid_lines: bool,
    debugging_interval: usize,
    quiet: bool,
) {
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

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

/// Print error logs from generator
fn print_errors(generator: &SchemaGenerator) {
    for error in generator.error_logs() {
        eprintln!("Problem on line {}: {}", error.line_number, error.msg);
    }
}

/// Write schema output
fn write_output<W: io::Write>(
    generator: &SchemaGenerator,
    schema_map: &SchemaMap,
    output_format: &OutputFormat,
    table_name: &str,
    output: &mut W,
) {
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

    if let Err(e) = write_result {
        eprintln!("Error writing output: {}", e);
        std::process::exit(1);
    }
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

    /// Helper to create a schema entry
    fn make_entry(name: &str, bq_type: BqType, mode: BqMode) -> SchemaEntry {
        SchemaEntry::new(name.to_string(), bq_type, mode)
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
        write_output(
            &generator,
            &schema_map,
            &OutputFormat::Json,
            "test_table",
            &mut output,
        );

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
        write_output(
            &generator,
            &schema_map,
            &OutputFormat::Ddl,
            "my_table",
            &mut output,
        );

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
        write_output(
            &generator,
            &schema_map,
            &OutputFormat::DebugMap,
            "table",
            &mut output,
        );

        let output_str = String::from_utf8(output).unwrap();
        assert!(!output_str.is_empty());
    }
}
