//! Benchmarks for BigQuery Schema Generator
//!
//! Run with: cargo bench

use std::io::Cursor;
use std::time::Instant;

use bq_schema_gen::{
    generate_schema_from_csv, generate_schema_from_json, GeneratorConfig, InputFormat,
    SchemaGenerator, SchemaMap,
};
use serde_json::json;

fn bench_simple_records() {
    println!("\n1. Simple Records (flat JSON with 5 fields)");
    println!("{:-<60}", "");

    let record = json!({
        "id": 12345,
        "name": "test_name",
        "value": 123.456,
        "active": true,
        "created": "2024-01-15T10:30:00Z"
    });

    for &count in &[1_000, 10_000, 100_000] {
        let mut generator = SchemaGenerator::new(GeneratorConfig::default());
        let mut schema_map = SchemaMap::new();

        let start = Instant::now();
        for _ in 0..count {
            generator.process_record(&record, &mut schema_map).unwrap();
        }
        let _schema = generator.flatten_schema(&schema_map);
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec
        );
    }
}

fn bench_nested_records() {
    println!("\n2. Nested Records (3 levels deep)");
    println!("{:-<60}", "");

    let record = json!({
        "user": {
            "profile": {
                "name": "test",
                "age": 30,
                "location": {
                    "city": "NYC",
                    "country": "USA"
                }
            },
            "settings": {
                "theme": "dark",
                "notifications": true
            }
        },
        "metadata": {
            "created": "2024-01-15T10:30:00Z",
            "version": 1
        }
    });

    for &count in &[1_000, 10_000, 100_000] {
        let mut generator = SchemaGenerator::new(GeneratorConfig::default());
        let mut schema_map = SchemaMap::new();

        let start = Instant::now();
        for _ in 0..count {
            generator.process_record(&record, &mut schema_map).unwrap();
        }
        let _schema = generator.flatten_schema(&schema_map);
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec
        );
    }
}

fn bench_large_file() {
    println!("\n3. Streaming JSON (newline-delimited)");
    println!("{:-<60}", "");

    for &count in &[1_000, 10_000, 100_000] {
        // Build input data
        let mut input = String::new();
        for i in 0..count {
            input.push_str(&format!(
                r#"{{"id": {}, "name": "user_{}", "value": {}.{}, "active": {}}}"#,
                i,
                i,
                i,
                i % 100,
                i % 2 == 0
            ));
            input.push('\n');
        }

        let cursor = Cursor::new(input);
        let mut output = Vec::new();

        let start = Instant::now();
        generate_schema_from_json(
            cursor,
            &mut output,
            GeneratorConfig::default(),
            false,
            None,
            None,
        )
        .unwrap();
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec
        );
    }
}

fn bench_wide_records() {
    println!("\n4. Wide Records (100 fields per record)");
    println!("{:-<60}", "");

    // Create a record with 100 fields
    let mut obj = serde_json::Map::new();
    for i in 0..100 {
        obj.insert(format!("field_{:03}", i), json!(i));
    }
    let record = serde_json::Value::Object(obj);

    for &count in &[1_000, 10_000] {
        let mut generator = SchemaGenerator::new(GeneratorConfig::default());
        let mut schema_map = SchemaMap::new();

        let start = Instant::now();
        for _ in 0..count {
            generator.process_record(&record, &mut schema_map).unwrap();
        }
        let _schema = generator.flatten_schema(&schema_map);
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec
        );
    }
}

fn bench_csv_records() {
    println!("\n5. CSV Processing");
    println!("{:-<60}", "");

    for &count in &[1_000, 10_000, 100_000] {
        // Build CSV input
        let mut input = String::from("id,name,value,active,created\n");
        for i in 0..count {
            input.push_str(&format!(
                "{},user_{},{}.{},{},2024-01-15\n",
                i,
                i,
                i,
                i % 100,
                i % 2 == 0
            ));
        }

        let cursor = Cursor::new(input);
        let mut output = Vec::new();
        let config = GeneratorConfig {
            input_format: InputFormat::Csv,
            ..Default::default()
        };

        let start = Instant::now();
        generate_schema_from_csv(cursor, &mut output, config, None, None).unwrap();
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec
        );
    }
}

fn bench_validation_throughput() {
    use bq_schema_gen::validate::{SchemaValidator, ValidationOptions, ValidationResult};
    use bq_schema_gen::BqSchemaField;

    println!("\n6. Validation Throughput");
    println!("{:-<60}", "");

    // Create a schema
    let schema = vec![
        BqSchemaField::new(
            "id".to_string(),
            "INTEGER".to_string(),
            "REQUIRED".to_string(),
        ),
        BqSchemaField::new(
            "name".to_string(),
            "STRING".to_string(),
            "NULLABLE".to_string(),
        ),
        BqSchemaField::new(
            "email".to_string(),
            "STRING".to_string(),
            "NULLABLE".to_string(),
        ),
        BqSchemaField::new(
            "age".to_string(),
            "INTEGER".to_string(),
            "NULLABLE".to_string(),
        ),
        BqSchemaField::new(
            "active".to_string(),
            "BOOLEAN".to_string(),
            "NULLABLE".to_string(),
        ),
    ];

    let options = ValidationOptions::default();
    let validator = SchemaValidator::new(&schema, options);

    for &count in &[1_000, 10_000, 100_000] {
        // Create valid records
        let records: Vec<serde_json::Value> = (0..count)
            .map(|i| {
                json!({
                    "id": i,
                    "name": format!("user_{}", i),
                    "email": format!("user{}@example.com", i),
                    "age": 20 + (i % 50),
                    "active": i % 2 == 0
                })
            })
            .collect();

        let start = Instant::now();
        let mut result = ValidationResult::new();
        for (line, record) in records.iter().enumerate() {
            validator.validate_record(record, line + 1, &mut result);
        }
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec
        );
    }
}

fn bench_validation_with_errors() {
    use bq_schema_gen::validate::{SchemaValidator, ValidationOptions, ValidationResult};
    use bq_schema_gen::BqSchemaField;

    println!("\n7. Validation with Errors (10% error rate)");
    println!("{:-<60}", "");

    let schema = vec![
        BqSchemaField::new(
            "id".to_string(),
            "INTEGER".to_string(),
            "REQUIRED".to_string(),
        ),
        BqSchemaField::new(
            "value".to_string(),
            "INTEGER".to_string(),
            "NULLABLE".to_string(),
        ),
    ];

    let options = ValidationOptions {
        max_errors: 100_000, // Don't limit for benchmark
        ..Default::default()
    };
    let validator = SchemaValidator::new(&schema, options);

    for &count in &[1_000, 10_000] {
        // Create records with 10% having type errors
        let records: Vec<serde_json::Value> = (0..count)
            .map(|i| {
                if i % 10 == 0 {
                    // Invalid: string instead of integer
                    json!({"id": i, "value": "not_a_number"})
                } else {
                    json!({"id": i, "value": i * 2})
                }
            })
            .collect();

        let start = Instant::now();
        let mut result = ValidationResult::new();
        for (line, record) in records.iter().enumerate() {
            validator.validate_record(record, line + 1, &mut result);
        }
        let elapsed = start.elapsed();

        let records_per_sec = count as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>7} records: {:>8.2}ms ({:>10.0} records/sec, {} errors)",
            count,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec,
            result.error_count
        );
    }
}

fn bench_schema_merging() {
    println!("\n8. Schema Map Merging");
    println!("{:-<60}", "");

    for &num_maps in &[10, 100, 1_000] {
        // Create multiple schema maps with overlapping fields
        let maps: Vec<SchemaMap> = (0..num_maps)
            .map(|i| {
                let mut generator = SchemaGenerator::new(GeneratorConfig::default());
                let mut map = SchemaMap::new();

                // Each map has some unique fields and some shared
                let record = json!({
                    "shared_field": i,
                    "another_shared": format!("value_{}", i),
                    format!("unique_{}", i): i * 2,
                });

                let _ = generator.process_record(&record, &mut map);
                map
            })
            .collect();

        let start = Instant::now();

        // Merge all maps together
        let mut final_generator = SchemaGenerator::new(GeneratorConfig::default());
        let mut final_map = SchemaMap::new();

        for map in maps {
            for (_key, entry) in map {
                // Simulate merge by processing the entry
                let json_value = match &entry.bq_type {
                    bq_schema_gen::BqType::Integer => json!(0),
                    bq_schema_gen::BqType::String => json!(""),
                    _ => json!(null),
                };
                let mut temp = serde_json::Map::new();
                temp.insert(entry.name.clone(), json_value);
                let _ = final_generator
                    .process_record(&serde_json::Value::Object(temp), &mut final_map);
            }
        }

        let _schema = final_generator.flatten_schema(&final_map);
        let elapsed = start.elapsed();

        println!(
            "  {:>7} maps: {:>8.2}ms ({} fields in result)",
            num_maps,
            elapsed.as_secs_f64() * 1000.0,
            final_map.len()
        );
    }
}

fn bench_watch_state_creation() {
    use bq_schema_gen::watch::{WatchConfig, WatchState};
    use std::io::Write;
    use tempfile::tempdir;

    println!("\n9. Watch State Creation (initial file processing)");
    println!("{:-<60}", "");

    for &num_files in &[10, 50, 100] {
        let dir = tempdir().unwrap();
        let mut file_paths = Vec::new();

        // Create test files
        for i in 0..num_files {
            let path = dir.path().join(format!("data{}.json", i));
            let mut file = std::fs::File::create(&path).unwrap();
            // Each file has 100 records
            for j in 0..100 {
                writeln!(
                    file,
                    r#"{{"id": {}, "value": {}, "name": "item_{}_{}"}}"#,
                    i * 100 + j,
                    j,
                    i,
                    j
                )
                .unwrap();
            }
            file_paths.push(path);
        }

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();

        let start = Instant::now();
        let state = WatchState::new(&file_paths, config, watch_config).unwrap();
        let elapsed = start.elapsed();

        let total_records = num_files * 100;
        let records_per_sec = total_records as f64 / elapsed.as_secs_f64();
        println!(
            "  {:>4} files ({} records): {:>8.2}ms ({:>10.0} records/sec, {} fields)",
            num_files,
            total_records,
            elapsed.as_secs_f64() * 1000.0,
            records_per_sec,
            state.current_schema().len()
        );
    }
}

fn bench_watch_file_change_handling() {
    use bq_schema_gen::watch::{WatchConfig, WatchState};
    use std::io::Write;
    use tempfile::tempdir;

    println!("\n10. Watch File Change Handling");
    println!("{:-<60}", "");

    for &num_files in &[10, 50] {
        let dir = tempdir().unwrap();
        let mut file_paths = Vec::new();

        // Create test files
        for i in 0..num_files {
            let path = dir.path().join(format!("data{}.json", i));
            let mut file = std::fs::File::create(&path).unwrap();
            for j in 0..100 {
                writeln!(file, r#"{{"id": {}, "value": {}}}"#, i * 100 + j, j).unwrap();
            }
            file_paths.push(path);
        }

        let config = GeneratorConfig::default();
        let watch_config = WatchConfig::default();

        let mut state = WatchState::new(&file_paths, config, watch_config).unwrap();

        // Measure time to handle a file change
        let iterations = 100;
        let start = Instant::now();

        for iter in 0..iterations {
            // Modify the first file
            let path = &file_paths[0];
            let mut file = std::fs::File::create(path).unwrap();
            for j in 0..100 {
                writeln!(
                    file,
                    r#"{{"id": {}, "value": {}, "new_field": {}}}"#,
                    j,
                    j + iter,
                    iter
                )
                .unwrap();
            }

            let _ = state.handle_file_change(path);
        }

        let elapsed = start.elapsed();
        let avg_ms = (elapsed.as_secs_f64() * 1000.0) / iterations as f64;
        println!(
            "  {:>4} files context, {} changes: {:>8.2}ms avg per change",
            num_files, iterations, avg_ms
        );
    }
}

fn main() {
    println!("BigQuery Schema Generator Benchmarks\n");
    println!("{:=<60}", "");

    bench_simple_records();
    bench_nested_records();
    bench_large_file();
    bench_wide_records();
    bench_csv_records();
    bench_validation_throughput();
    bench_validation_with_errors();
    bench_schema_merging();
    bench_watch_state_creation();
    bench_watch_file_change_handling();

    println!("\n{:=<60}", "");
    println!("Benchmarks complete.");
}
