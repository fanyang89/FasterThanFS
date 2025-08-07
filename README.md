# DuckDB vs Filesystem Performance Test

This project benchmarks DuckDB's performance as a key-value store for large BLOBs compared to traditional filesystem storage. Based on SQLite's famous [kvtest](https://www.sqlite.org/fasterthanfs.html) benchmark, this implementation demonstrates the performance characteristics of using DuckDB for blob storage versus accessing individual files in the filesystem.

## Overview

The benchmark compares two approaches for storing and accessing binary data:

1. **DuckDB Approach**: Store BLOBs in a DuckDB database table
2. **Filesystem Approach**: Store each BLOB as an individual file on disk

This helps evaluate whether DuckDB can provide similar benefits to SQLite's well-documented performance advantages for blob storage.

## Performance Expectations

Based on SQLite's documented results, you might expect to see:

- **35% faster** reads/writes for small blobs (8-12KB range) due to reduced system call overhead
- **20% less** disk space usage compared to individual files
- **Better performance** with antivirus software on Windows systems
- **Transactional safety** when properly configured

## Features

- Create test databases with configurable blob counts and sizes
- Export DuckDB data to filesystem for direct comparison
- Run performance benchmarks with detailed timing statistics
- Support for various blob sizes and variance
- Cross-platform compatibility (Linux, macOS, Windows)

## Prerequisites

- DuckDB development library (`libduckdb-dev`)
- C compiler (gcc/clang)
- Taskfile (optional, for build automation)

### Installation (Ubuntu/Debian)

```bash
sudo apt-get update
sudo apt-get install -y libduckdb-dev gcc
```

## Building

### Using Taskfile (Recommended)

```bash
# Build the executable
task build

# Clean build artifacts
task clean
```

### Manual Build

```bash
gcc -O2 -std=c23 ducktest.c -lduckdb -o ducktest
```

## Usage

### 1. Create Test Database

```bash
# Create a DuckDB database with 100,000 blobs of 10KB each
./ducktest init test.ddb --count 100k --size 10k --variance 2k
```

### 2. Export to Filesystem

```bash
# Export DuckDB data to individual files
./ducktest export test.ddb test.dir
```

### 3. Run Performance Tests

```bash
# Test DuckDB performance
./ducktest run test.ddb --count 10k --max-id 100k

# Test filesystem performance
./ducktest run test.dir --count 10k --max-id 100k
```

### 4. Quick Test (Taskfile)

```bash
# Run a complete test cycle
task test
```

## Command Reference

### `ducktest init`

Create a new test database.

```bash
./ducktest init <database> --count <number> --size <size> [--variance <variance>]
```

- `--count`: Number of blobs to create (supports k, m suffixes)
- `--size`: Target blob size in bytes (supports k, m suffixes)
- `--variance`: Size variation around target (optional)

### `ducktest export`

Export database contents to filesystem.

```bash
./ducktest export <database> <directory>
```

### `ducktest run`

Run performance benchmark.

```bash
./ducktest run <target> --count <number> [--max-id <id>] [--fsync]
```

- `--count`: Number of operations to perform
- `--max-id`: Maximum ID range for random access
- `--fsync`: Enable fsync for writes (default: off)

## Example Workflow

```bash
# 1. Build the project
task build

# 2. Create test database with 50,000 blobs of 8KB
./ducktest init benchmark.ddb --count 50k --size 8k

# 3. Export to filesystem for comparison
mkdir benchmark_files
./ducktest export benchmark.ddb benchmark_files

# 4. Run DuckDB benchmark
echo "Testing DuckDB performance..."
./ducktest run benchmark.ddb --count 5000

# 5. Run filesystem benchmark
echo "Testing filesystem performance..."
./ducktest run benchmark_files --count 5000

# 6. Compare results
```

## Performance Tips

For optimal results:

1. **Blob Size**: Test with blobs in the 8-12KB range where database performance typically excels
2. **Batch Operations**: Use higher `--count` values to average out system variance
3. **Multiple Runs**: Run tests multiple times to account for caching effects
4. **Transaction Size**: Consider adjusting transaction batch sizes for your use case

## File Structure

```bash
├── ducktest.c          # Main benchmark implementation (DuckDB version)
├── kvtest.c           # Original SQLite benchmark reference
├── sqlite3.c          # SQLite amalgamation source
├── sqlite3.h          # SQLite header
├── Taskfile.yml       # Build automation
├── ducktest           # Compiled executable
├── test.ddb           # Test DuckDB database
├── test.duckdb        # Alternative DuckDB database
├── test1.db           # Test SQLite database
└── test1.dir/         # Filesystem test data
    ├── 000001
    ├── 000002
    └── ...
```

## Technical Details

The benchmark measures:

- **Sequential Read Performance**: Reading blobs in order
- **Random Access Performance**: Reading blobs with random IDs
- **Write Performance**: Inserting new blobs
- **Disk Usage**: Space efficiency comparison

### Database Schema

The DuckDB database uses a simple schema:

```sql
CREATE TABLE kv(
  k INTEGER PRIMARY KEY,
  v BLOB
);
```

### Filesystem Structure

Each blob is stored as an individual file named with its ID (zero-padded):

```plain
test1.dir/
├── 000001
├── 000002
├── 000003
└── ...
```

## Contributing

This project is based on SQLite's kvtest.c implementation, adapted for DuckDB's C API. Contributions to improve accuracy, add features, or support additional database systems are welcome.

## References

- [SQLite: Faster Than The Filesystem](https://www.sqlite.org/fasterthanfs.html)
- [DuckDB C API Documentation](https://duckdb.org/docs/api/c/)
- [Taskfile Documentation](https://taskfile.dev/)

## License

This project includes SQLite's original kvtest.c code, which is in the public domain. The DuckDB adaptation follows similar open-source principles.
