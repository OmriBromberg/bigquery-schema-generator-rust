#!/bin/bash
# Compare performance between Rust and Python implementations

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON_DIR="$PROJECT_DIR/bigquery-schema-generator-python"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "BigQuery Schema Generator: Rust vs Python Performance Comparison"
echo "=================================================================="
echo

# Check if Python implementation exists
if [ ! -d "$PYTHON_DIR" ]; then
    echo -e "${RED}Error: Python implementation not found at $PYTHON_DIR${NC}"
    exit 1
fi

# Build Rust release
echo "Building Rust release binary..."
cargo build --release --manifest-path "$PROJECT_DIR/Cargo.toml" 2>/dev/null
RUST_BIN="$PROJECT_DIR/target/release/bq-schema-gen"

# Check if Python can run
PYTHON_CMD="python3"
if ! command -v $PYTHON_CMD &> /dev/null; then
    PYTHON_CMD="python"
fi

# Create test data
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

echo "Generating test data..."
echo

# Test 1: Simple records
echo "Test 1: 10,000 simple records"
echo "-----------------------------"
for i in $(seq 1 10000); do
    echo "{\"id\": $i, \"name\": \"user_$i\", \"value\": $i.$((i % 100)), \"active\": $((i % 2 == 0))}"
done > "$TEMP_DIR/simple.json"

# Rust timing
echo -n "  Rust:   "
RUST_TIME=$( { time "$RUST_BIN" < "$TEMP_DIR/simple.json" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
echo "$RUST_TIME"

# Python timing
echo -n "  Python: "
PYTHON_TIME=$( { time $PYTHON_CMD -m bigquery_schema_generator.generate_schema < "$TEMP_DIR/simple.json" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}') || PYTHON_TIME="(not available)"
echo "$PYTHON_TIME"
echo

# Test 2: Nested records
echo "Test 2: 5,000 nested records"
echo "----------------------------"
for i in $(seq 1 5000); do
    echo "{\"user\": {\"profile\": {\"name\": \"user_$i\", \"age\": $((20 + i % 50))}, \"settings\": {\"theme\": \"dark\", \"lang\": \"en\"}}}"
done > "$TEMP_DIR/nested.json"

echo -n "  Rust:   "
RUST_TIME=$( { time "$RUST_BIN" < "$TEMP_DIR/nested.json" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
echo "$RUST_TIME"

echo -n "  Python: "
PYTHON_TIME=$( { time $PYTHON_CMD -m bigquery_schema_generator.generate_schema < "$TEMP_DIR/nested.json" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}') || PYTHON_TIME="(not available)"
echo "$PYTHON_TIME"
echo

# Test 3: Large file
echo "Test 3: 50,000 records"
echo "----------------------"
for i in $(seq 1 50000); do
    echo "{\"id\": $i, \"value\": $i}"
done > "$TEMP_DIR/large.json"

echo -n "  Rust:   "
RUST_TIME=$( { time "$RUST_BIN" < "$TEMP_DIR/large.json" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}')
echo "$RUST_TIME"

echo -n "  Python: "
PYTHON_TIME=$( { time $PYTHON_CMD -m bigquery_schema_generator.generate_schema < "$TEMP_DIR/large.json" > /dev/null 2>&1; } 2>&1 | grep real | awk '{print $2}') || PYTHON_TIME="(not available)"
echo "$PYTHON_TIME"
echo

echo "=================================================================="
echo -e "${GREEN}Comparison complete.${NC}"
