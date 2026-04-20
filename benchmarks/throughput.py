"""
TPC-H Throughput Test.

Runs N parallel query streams alongside a refresh thread that performs combined
RF1+RF2 operations (using update sets 2..N+1).

Score:
  throughput_score = (n_streams * 22 * 3600 * SF) / throughput_interval

The spec-defined number of streams is determined by scale factor (TPC-H §5.3.4).

Connection model:
  DuckDB — each concurrent stream uses a cursor forked from the parent connection
           (shares catalog ATTACH, independent transaction/USE context).
  Spark  — all streams share the same SparkSession (thread-safe for job submission).
           Requires spark.scheduler.mode=FAIR for true parallel execution.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from benchmarks.runner import BenchmarkRunner, QueryResult
from benchmarks.power import (
    RefreshResult,
    StreamMonitor,
    default_update_streams,
    fork_runner,
    has_refresh_data,
    load_power_order,
    run_stream,
    spec_stream_count,
    time_refresh,
)


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class ThroughputTestResult:
    streams: list[list[QueryResult]]
    refresh_results: list[RefreshResult]
    interval: float
    n_streams: int
    throughput_score: float
    monitor_log: Path | None


# ---------------------------------------------------------------------------
# Score calculation
# ---------------------------------------------------------------------------

def compute_throughput_score(n_streams: int, interval: float, scale_factor: int) -> float:
    return round((n_streams * 22 * 3600 * scale_factor) / interval, 2)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run(
    runner: BenchmarkRunner,
    namespace: str,
    data_dir: Path = Path("data"),
    update_streams: int | None = None,
    monitor_log_dir: Path | None = None,
) -> ThroughputTestResult:
    """
    Run the TPC-H throughput test: N parallel query streams + refresh thread.

    Args:
        runner:          BenchmarkRunner with a set-up engine.
        namespace:       Iceberg namespace containing the TPC-H tables.
        data_dir:        SF-specific data directory (e.g. data/sf=10). Expected to contain
                         refresh parquet files and optionally a streams/ subdirectory
                         with qgen-generated SQL files.
        update_streams:  Number of combined RF operations in the refresh thread.
                         Defaults to max(1, round(0.1 * scale_factor)).
        monitor_log_dir: Directory for the TSV resource-usage log. Defaults to
                         runner.result_dir.
    """
    sf = runner.scale_factor
    n_streams = spec_stream_count(sf)
    n_update = update_streams if update_streams is not None else default_update_streams(sf)
    streams_dir = data_dir / "streams"
    order = load_power_order()

    log_dir = monitor_log_dir or runner.result_dir
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    monitor_log = log_dir / f"throughput_monitor_{runner.engine_name}_sf{sf}_{ts}.tsv"

    print(f"  SF={sf} → {n_streams} stream(s), {n_update} update set(s)")
    if streams_dir.exists():
        print(f"  Using qgen stream files from {streams_dir}")
    else:
        print(f"  No stream files found in {streams_dir} — using fixed query order/parameters")

    monitor = StreamMonitor(log_path=monitor_log)
    monitor.start()
    print(f"  Resource monitor logging to {monitor_log}")

    stream_results: list[list[QueryResult]] = [[] for _ in range(n_streams)]
    refresh_results: list[RefreshResult] = []
    lock = threading.Lock()

    def query_thread(idx: int) -> None:
        forked = fork_runner(runner)
        # Throughput streams are 1-indexed: stream_1.sql .. stream_N.sql
        # (stream_0.sql is reserved for the power test)
        stream_results[idx] = run_stream(
            forked, namespace, stream_idx=idx + 1, order=order,
            streams_dir=streams_dir, benchmark_tag="throughput",
        )

    def refresh_thread() -> None:
        forked_engine = runner.engine.fork_for_stream()
        # Throughput refresh uses sets 2..n_update+1 (set 1 is reserved for power test)
        for set_n in range(2, n_update + 2):
            if not has_refresh_data(data_dir, set_n):
                print(f"  Warning: throughput refresh set {set_n} not found, skipping.")
                continue
            result = time_refresh(forked_engine, data_dir, namespace, "RF", set_n)
            with lock:
                refresh_results.append(result)
            status = f"ERROR: {result.error}" if result.error else f"{result.elapsed_seconds:.3f}s"
            print(f"  throughput RF set {set_n}: {status}")

    threads: list[threading.Thread] = []
    for i in range(n_streams):
        threads.append(threading.Thread(target=query_thread, args=(i,)))
    threads.append(threading.Thread(target=refresh_thread))

    print(f"  Starting {n_streams} query stream(s) + refresh thread ({n_update} set(s))...")
    start = time.perf_counter()
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join()
    finally:
        monitor.stop()

    interval = round(time.perf_counter() - start, 4)
    throughput_score = compute_throughput_score(n_streams, interval, sf)
    print(f"\n  throughput interval = {interval:.2f}s")
    print(f"  throughput_score    = {throughput_score:.2f} QphH@{sf}GB")

    return ThroughputTestResult(
        streams=stream_results,
        refresh_results=refresh_results,
        interval=interval,
        n_streams=n_streams,
        throughput_score=throughput_score,
        monitor_log=monitor_log,
    )
