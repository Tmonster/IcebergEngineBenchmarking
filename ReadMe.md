# Iceberg Engine Benchmarking

TPC-H power and analytical benchmarks for Iceberg table engines (DuckDB and Spark), with support for multiple catalog backends.

## Supported engines and catalogs

| Engine | s3tables | local (PyIceberg) | DuckLake |
|--------|----------|-------------------|----------|
| DuckDB | ✓ | ✗ | ✓ |
| Spark  | ✓ | ✗ | ✗ |

## Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv)
- Java 11+ (Spark only)
- AWS credentials configured (s3tables only)

## Installation

```bash
# DuckDB only
uv sync

# DuckDB + Spark
uv sync --extra spark
```

## Machine setup (EC2)

When running on an EC2 instance with NVMe storage, mount the physical drive to avoid Spark spill competing with EBS I/O. The script below finds the largest unmounted NVMe device and mounts it, then clones the repo onto it.

```bash
mount_name=$(sudo lsblk | awk '
NR > 1 && $1 ~ /^nvme/ && $7 == "" {
    size = $4; unit = substr(size, length(size)); value = substr(size, 1, length(size)-1);
    if (unit == "G") { value *= 1024^3; }
    else if (unit == "T") { value *= 1024^4; }
    else if (unit == "M") { value *= 1024^2; }
    else if (unit == "K") { value *= 1024; }
    if (value > max) { max = value; largest = $1; }
}
END { if (largest) print largest; }')

sudo mkfs -t xfs /dev/$mount_name
sudo mkdir -p $HOME/benchmark_mount
sudo mount /dev/$mount_name $HOME/benchmark_mount
sudo chown -R ubuntu:ubuntu $HOME/benchmark_mount

git clone https://github.com/Tmonster/IcebergEngineBenchmarking.git $HOME/benchmark_mount/IcebergEngineBenchmarking
cd $HOME/benchmark_mount/IcebergEngineBenchmarking
```

This is also available as `./setup/mount.sh`.

## Configuration

### Catalog (`config/catalog.yml`)

**AWS S3 Tables:**
```yaml
type: s3tables
region: eu-central-1
account_id: "123456789012"
bucket: my-s3-tables-bucket
namespace: benchmarks
```

**DuckLake (local, DuckDB only):**
```yaml
type: ducklake
namespace: benchmarks
metadata_path: ducklake/tpch.ducklake
data_path: ducklake/files
```

**Local PyIceberg (DuckDB only):**
```yaml
type: local
namespace: benchmarks
warehouse_path: warehouse/
```

### Benchmark (`config/benchmark.yml`)

```yaml
scale_factor: 10
warmup_runs: 1
benchmark_runs: 3
result_dir: results/
```

## Data generation

Generate TPC-H base tables for a given scale factor. Data is stored in `data/sf=<N>/`.

```bash
# Base tables only
uv run python -m setup.generate_data --sf 10

# Base tables + RF1/RF2 refresh files (required for power benchmark)
uv run python -m setup.generate_data --sf 10 --refresh

# Base tables + refresh files + spec-compliant per-stream query files
uv run python -m setup.generate_data --sf 10 --refresh --query-streams
```

`--query-streams` uses `qgen` to generate a different query permutation and parameter substitution for each stream, matching the TPC-H spec. Without it, the power benchmark falls back to a fixed query order and hardcoded parameters.

## Running benchmarks

### Load benchmark

Times how long it takes to provision the TPC-H tables into the catalog. This benchmark performs the data load itself — do not use `--skip-datagen`.

```bash
uv run python run_benchmark.py --engine duckdb --benchmark load --sf 10
```

### Analytical benchmark

Runs the 22 TPC-H queries with configurable warmup and benchmark repetitions. No refresh functions. Results are written to `results/`.

```bash
# Provision data and run
uv run python run_benchmark.py --engine duckdb --benchmark analytical --sf 10

# Re-use an existing namespace (skip data generation)
uv run python run_benchmark.py --engine duckdb --benchmark analytical --sf 10 \
    --skip-datagen --namespace my_namespace --keep-tables
```

`--keep-tables` prevents the namespace from being dropped after the run, useful when you want to run multiple engines against the same data.

### Power benchmark

Runs the TPC-H power test: RF1 → single query stream → RF2 (sequential). Computes the power score. Requires refresh data — generate with `--refresh`.

```bash
uv run python run_benchmark.py --engine duckdb --benchmark power --sf 10
```

### Throughput benchmark

Runs N parallel query streams alongside a refresh thread. Computes the throughput score. The number of streams is determined by the TPC-H spec (e.g. 3 streams at SF=10). The number of refresh sets defaults to `max(1, round(0.1 * SF))`.

```bash
uv run python run_benchmark.py --engine duckdb --benchmark throughput --sf 10
```

### Composite benchmark

Runs the full TPC-H composite metric: power test followed by throughput test. Computes power score, throughput score, and the official QphH composite score (`sqrt(power_score * throughput_score)`).

```bash
uv run python run_benchmark.py --engine duckdb --benchmark composite --sf 10
```

With a non-default catalog config:
```bash
uv run python run_benchmark.py --engine duckdb --benchmark composite --sf 10 \
    --catalog-config config/ducklake_local.yml
```

### Plotting results

```bash
uv run --extra plot python plot_results.py results/duckdb_power_sf10_*.json results/spark_power_sf10_*.json
```

## Benchmark flags

| Flag | Description |
|------|-------------|
| `--engine` | `duckdb` or `spark` |
| `--benchmark` | `load`, `analytical`, `power`, `throughput`, or `composite` |
| `--sf` | Scale factor (overrides `benchmark.yml`) |
| `--catalog-config` | Path to catalog config (default: `config/catalog.yml`) |
| `--namespace` | Namespace name (default: auto-generated) |
| `--keep-tables` | Skip namespace teardown after run |
| `--skip-datagen` | Skip data provisioning, use existing namespace (requires `--namespace`; not applicable to `load`) |
| `--update-streams` | Number of refresh sets in the throughput/composite test (default: `max(1, round(0.1 * SF))`) |

## Spark notes

- **Spill directory** — set to `./spark-spill` relative to the working directory. Change `spark.local.dir` in `engines/spark/catalog_adapters.py` if needed.
- **Driver/executor memory** — set to 25 GB. Adjust in `catalog_adapters.py` for your instance.
- **Delete strategy** — configured as merge-on-read. Copy-on-write causes `ValidationException: Missing required files to delete` on S3 Tables due to background file optimization rewriting Parquet files between DELETE planning and commit.
- **Scheduler** — `spark.scheduler.mode=FAIR` is required for the throughput test so parallel query streams actually run concurrently rather than queuing.


