"""
TPC-H Power Test.

Runs the sequential power test: RF1 → single query stream → RF2.

Score:
  power_score = (3600 * SF) / ((∏ query_times * t_RF1 * t_RF2) ^ (1/24))

If local refresh parquet files are absent, RF steps are skipped and the score
is not computed. Generate them with:
  python -m setup.generate_data --sf <N> --refresh

Shared helpers (RefreshResult, run_stream, etc.) used by throughput.py are
defined here and imported from there.
"""
from __future__ import annotations

import functools
import operator
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psutil

from benchmarks.runner import BenchmarkRunner, QueryResult

if TYPE_CHECKING:
    from engines.base import Engine

QUERY_DIR = Path("queries/tpch/queries")
POWER_ORDER_FILE = Path("queries/tpch/power_order.txt")

# Spec-defined number of query streams for the throughput test (TPC-H section 5.3.4).
_SPEC_STREAMS: dict[int, int] = {1: 2, 10: 3, 20: 3, 30: 4, 100: 5, 300: 6, 1000: 7, 3000: 8, 10000: 9}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class RefreshResult:
    rf: str                  # "RF1", "RF2", or "RF" (combined)
    set_n: int
    elapsed_seconds: float
    error: str | None = None


@dataclass
class PowerTestResult:
    stream: list[QueryResult]
    rf1: RefreshResult | None
    rf2: RefreshResult | None
    power_score: float | None
    monitor_log: Path | None


# ---------------------------------------------------------------------------
# Background monitor (also used by throughput and composite benchmarks)
# ---------------------------------------------------------------------------

class StreamMonitor:
    """
    Background thread sampling process CPU, memory, and disk I/O every second.
    TSV output is compatible with the reference implementation's log format.
    """

    def __init__(self, log_path: Path):
        self.log_path = log_path
        self._proceed = True
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._proceed = False
        self._thread.join(timeout=5)

    def _run(self) -> None:
        proc = psutil.Process()
        disk_baseline = psutil.disk_io_counters()
        start = time.time()

        with self.log_path.open("wb") as log:
            header = "\t".join([
                "time_offset", "cpu_percent", "cpu_user", "cpu_system",
                "memory_rss", "memory_vms", "read_bytes", "write_bytes",
            ])
            log.write(header.encode() + b"\n")

            while self._proceed:
                try:
                    cpu_times = proc.cpu_times()
                    mem = proc.memory_info()
                    disk = psutil.disk_io_counters()
                    row = "\t".join(str(x) for x in [
                        round(time.time() - start, 2),
                        round(proc.cpu_percent()),
                        round(cpu_times.user, 2),
                        round(cpu_times.system, 2),
                        mem.rss,
                        mem.vms,
                        disk.read_bytes - disk_baseline.read_bytes,
                        disk.write_bytes - disk_baseline.write_bytes,
                    ])
                    log.write(row.encode() + b"\n")
                    log.flush()
                except Exception:
                    pass
                time.sleep(1)


# ---------------------------------------------------------------------------
# Shared helpers (also imported by throughput.py)
# ---------------------------------------------------------------------------

def load_power_order() -> list[int]:
    return [
        int(n.strip())
        for n in POWER_ORDER_FILE.read_text().splitlines()
        if n.strip() and not n.strip().startswith("#")
    ]


def spec_stream_count(scale_factor: int) -> int:
    """Return the TPC-H spec stream count for the throughput test."""
    for sf in sorted(_SPEC_STREAMS):
        if scale_factor <= sf:
            return _SPEC_STREAMS[sf]
    return _SPEC_STREAMS[10000]


def default_update_streams(scale_factor: int) -> int:
    """Default number of refresh sets for the throughput test: max(1, round(0.1 * SF))."""
    return max(1, round(0.1 * scale_factor))


def has_refresh_data(data_dir: Path, set_n: int) -> bool:
    return all([
        (data_dir / f"orders_u{set_n}.parquet").exists(),
        (data_dir / f"lineitem_u{set_n}.parquet").exists(),
        (data_dir / f"delete_set_{set_n}.parquet").exists(),
    ])


def time_refresh(engine: Any, data_dir: Path, namespace: str, rf: str, set_n: int) -> RefreshResult:
    error = None
    start = time.perf_counter()
    try:
        if rf == "RF1":
            engine.run_rf1(data_dir, namespace, set_n)
        elif rf == "RF2":
            engine.run_rf2(data_dir, namespace, set_n)
        else:  # combined RF (throughput)
            engine.run_rf1(data_dir, namespace, set_n)
            engine.run_rf2(data_dir, namespace, set_n)
    except Exception as e:
        error = str(e)
    return RefreshResult(rf=rf, set_n=set_n, elapsed_seconds=round(time.perf_counter() - start, 4), error=error)


def fork_runner(runner: BenchmarkRunner) -> BenchmarkRunner:
    """Create a BenchmarkRunner backed by a forked engine for use in a thread."""
    return BenchmarkRunner(
        engine=runner.engine.fork_for_stream(),
        catalog=runner.catalog,
        engine_name=runner.engine_name,
        scale_factor=runner.scale_factor,
        result_dir=runner.result_dir,
    )


def split_stream_sql(sql: str) -> list[str]:
    """
    Split a qgen-produced stream file into individual query strings.
    qgen separates queries with ';' and may emit comment lines.
    Returns only non-empty statements that contain a SELECT.
    """
    return [
        stmt.strip()
        for stmt in sql.split(";")
        if "select" in stmt.lower()
    ]


def run_stream(
    runner: BenchmarkRunner,
    namespace: str,
    stream_idx: int,
    order: list[int],
    streams_dir: Path,
    benchmark_tag: str = "power",
) -> list[QueryResult]:
    """
    Run one query stream and return per-query results.

    If a qgen-generated stream file exists at streams_dir/stream_{stream_idx}.sql,
    use it (spec-compliant permutation + parameter substitution). Otherwise fall back to
    running the fixed-parameter queries in the order given by `order`.
    """
    stream_file = streams_dir / f"stream_{stream_idx}.sql"

    if stream_file.exists():
        return _run_stream_from_file(runner, namespace, stream_idx, stream_file, benchmark_tag)
    else:
        return _run_stream_from_order(runner, namespace, stream_idx, order, benchmark_tag)


def _run_stream_from_file(
    runner: BenchmarkRunner,
    namespace: str,
    stream_idx: int,
    stream_file: Path,
    benchmark_tag: str,
) -> list[QueryResult]:
    statements = split_stream_sql(stream_file.read_text())
    results: list[QueryResult] = []
    for seq_idx, sql in enumerate(statements):
        result = runner.time_query(
            sql=sql,
            query_name=f"q{seq_idx + 1:02d}",
            benchmark=benchmark_tag,
            namespace=namespace,
            run=stream_idx,
        )
        results.append(result)
        status = f"ERROR: {result.error}" if result.error else f"{result.elapsed_seconds:.3f}s"
        print(f"  stream {stream_idx} [{seq_idx + 1:02d}/22] (stream file): {status}")
    return results


def _run_stream_from_order(
    runner: BenchmarkRunner,
    namespace: str,
    stream_idx: int,
    order: list[int],
    benchmark_tag: str,
) -> list[QueryResult]:
    results: list[QueryResult] = []
    for seq_idx, q_num in enumerate(order):
        qfile = QUERY_DIR / f"q{q_num:02d}.sql"
        result = runner.time_query(
            sql=qfile.read_text(),
            query_name=f"q{q_num:02d}",
            benchmark=benchmark_tag,
            namespace=namespace,
            run=stream_idx,
        )
        results.append(result)
        status = f"ERROR: {result.error}" if result.error else f"{result.elapsed_seconds:.3f}s"
        print(f"  stream {stream_idx} [{seq_idx + 1:02d}/22] q{q_num:02d}: {status}")
    return results


# ---------------------------------------------------------------------------
# Power score calculation
# ---------------------------------------------------------------------------

def compute_power_score(
    stream: list[QueryResult],
    rf1: RefreshResult | None,
    rf2: RefreshResult | None,
    scale_factor: int,
) -> float | None:
    if rf1 is None or rf2 is None or rf1.error or rf2.error:
        return None
    if any(r.error for r in stream):
        return None
    product = functools.reduce(operator.mul, (r.elapsed_seconds for r in stream))
    return round(
        (3600 * scale_factor) / ((product * rf1.elapsed_seconds * rf2.elapsed_seconds) ** (1 / 24)),
        2,
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(
    runner: BenchmarkRunner,
    namespace: str,
    data_dir: Path = Path("data"),
    monitor_log_dir: Path | None = None,
) -> PowerTestResult:
    """
    Run the TPC-H power test: RF1 → single query stream → RF2.

    Args:
        runner:          BenchmarkRunner with a set-up engine.
        namespace:       Iceberg namespace containing the TPC-H tables.
        data_dir:        SF-specific data directory (e.g. data/sf=10). Expected to contain
                         refresh parquet files and optionally a streams/ subdirectory
                         with qgen-generated SQL files.
        monitor_log_dir: Directory for the TSV resource-usage log. Defaults to
                         runner.result_dir.
    """
    sf = runner.scale_factor
    streams_dir = data_dir / "streams"
    order = load_power_order()

    log_dir = monitor_log_dir or runner.result_dir
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    monitor_log = log_dir / f"power_monitor_{runner.engine_name}_sf{sf}_{ts}.tsv"

    if streams_dir.exists():
        print(f"  Using qgen stream files from {streams_dir}")
    else:
        print(f"  No stream files found in {streams_dir} — using fixed query order/parameters")
        print(f"  (Run `python -m setup.generate_data --sf {sf} --query-streams` for spec-compliant streams)")

    monitor = StreamMonitor(log_path=monitor_log)
    monitor.start()
    print(f"  Resource monitor logging to {monitor_log}")

    rf1 = rf2 = None
    try:
        if has_refresh_data(data_dir, set_n=1):
            print("  Running RF1...")
            rf1 = time_refresh(runner.engine, data_dir, namespace, "RF1", set_n=1)
            rf1_status = f"ERROR: {rf1.error}" if rf1.error else f"{rf1.elapsed_seconds:.3f}s"
            print(f"  RF1: {rf1_status}")
        else:
            print(
                "  Skipping RF1/RF2 — refresh parquet files not found. "
                "Run `python -m setup.generate_data --refresh` to generate them."
            )

        print("  Starting power query stream...")
        stream = run_stream(runner, namespace, stream_idx=0, order=order, streams_dir=streams_dir, benchmark_tag="power")

        if rf1 is not None:
            print("  Running RF2...")
            rf2 = time_refresh(runner.engine, data_dir, namespace, "RF2", set_n=1)
            rf2_status = f"ERROR: {rf2.error}" if rf2.error else f"{rf2.elapsed_seconds:.3f}s"
            print(f"  RF2: {rf2_status}")
    finally:
        monitor.stop()

    power_score = compute_power_score(stream, rf1, rf2, sf)
    if power_score is not None:
        print(f"\n  power_score = {power_score:.2f} QphH@{sf}GB")

    return PowerTestResult(
        stream=stream,
        rf1=rf1,
        rf2=rf2,
        power_score=power_score,
        monitor_log=monitor_log,
    )
