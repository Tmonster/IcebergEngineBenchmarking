"""
TPC-H Power + Throughput Benchmark.

Mirrors the structure of duckdb-tpch-power-test/benchmark.py:

  1. Power test  — RF1 → single query stream → RF2 (sequential, uses update set 1)
  2. Throughput test — n_streams query streams + refresh thread running update_streams
                       combined RF operations, all in parallel

Scores:
  power_score      = (3600 * SF) / ((∏ query_times * t_RF1 * t_RF2) ^ (1/24))
  throughput_score = (n_streams * 22 * 3600 * SF) / throughput_interval
  qphh             = sqrt(power_score * throughput_score)

If local refresh parquet files are absent, RF steps are skipped and scores that
depend on them are not computed. Generate them with:
  python -m setup.generate_data --sf <N> --refresh

Connection model:
  DuckDB — each concurrent stream uses a cursor forked from the parent connection
           (shares catalog ATTACH, independent transaction/USE context).
  Spark  — all streams share the same SparkSession (thread-safe for job submission).
           Requires spark.scheduler.mode=FAIR for true parallel execution.
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
class PowerSummary:
    # Power test (1 stream, sequential)
    power_stream: list[QueryResult]
    rf1: RefreshResult | None
    rf2: RefreshResult | None
    power_score: float | None

    # Throughput test (n_streams parallel + refresh thread)
    throughput_streams: list[list[QueryResult]]
    throughput_refresh_results: list[RefreshResult]
    throughput_interval: float | None
    throughput_score: float | None

    # Combined
    qphh: float | None

    monitor_log: Path | None


# ---------------------------------------------------------------------------
# Background monitor
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
# Helpers
# ---------------------------------------------------------------------------

def _load_power_order() -> list[int]:
    return [
        int(n.strip())
        for n in POWER_ORDER_FILE.read_text().splitlines()
        if n.strip() and not n.strip().startswith("#")
    ]


def _spec_stream_count(scale_factor: int) -> int:
    """Return the TPC-H spec stream count for the throughput test."""
    for sf in sorted(_SPEC_STREAMS):
        if scale_factor <= sf:
            return _SPEC_STREAMS[sf]
    return _SPEC_STREAMS[10000]


def _default_update_streams(scale_factor: int) -> int:
    """Default number of refresh sets for the throughput test: max(1, round(0.1 * SF))."""
    return max(1, round(0.1 * scale_factor))


def _has_refresh_data(data_dir: Path, set_n: int) -> bool:
    return all([
        (data_dir / f"orders_u{set_n}.parquet").exists(),
        (data_dir / f"lineitem_u{set_n}.parquet").exists(),
        (data_dir / f"delete_set_{set_n}.parquet").exists(),
    ])


def _time_refresh(engine: Any, data_dir: Path, namespace: str, rf: str, set_n: int) -> RefreshResult:
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


def _fork_runner(runner: BenchmarkRunner) -> BenchmarkRunner:
    """Create a BenchmarkRunner backed by a forked engine for use in a thread."""
    return BenchmarkRunner(
        engine=runner.engine.fork_for_stream(),
        catalog=runner.catalog,
        engine_name=runner.engine_name,
        scale_factor=runner.scale_factor,
        result_dir=runner.result_dir,
    )


# ---------------------------------------------------------------------------
# Stream runner
# ---------------------------------------------------------------------------

def run_stream(
    runner: BenchmarkRunner,
    namespace: str,
    stream_idx: int,
    order: list[int],
) -> list[QueryResult]:
    """Run one query stream and return per-query results."""
    results: list[QueryResult] = []
    for seq_idx, q_num in enumerate(order):
        qfile = QUERY_DIR / f"q{q_num:02d}.sql"
        result = runner.time_query(
            sql=qfile.read_text(),
            query_name=f"q{q_num:02d}",
            benchmark="power",
            namespace=namespace,
            run=stream_idx,
        )
        results.append(result)
        status = f"ERROR: {result.error}" if result.error else f"{result.elapsed_seconds:.3f}s"
        print(f"  stream {stream_idx} [{seq_idx + 1:02d}/22] q{q_num:02d}: {status}")
    return results


# ---------------------------------------------------------------------------
# Power test
# ---------------------------------------------------------------------------

def _run_power_test(
    runner: BenchmarkRunner,
    namespace: str,
    data_dir: Path,
    order: list[int],
) -> tuple[list[QueryResult], RefreshResult | None, RefreshResult | None]:
    """RF1 → 1 query stream → RF2, all sequential."""
    rf1 = rf2 = None

    if _has_refresh_data(data_dir, set_n=1):
        print("  Running RF1...")
        rf1 = _time_refresh(runner.engine, data_dir, namespace, "RF1", set_n=1)
        rf1_status = f"ERROR: {rf1.error}" if rf1.error else f"{rf1.elapsed_seconds:.3f}s"
        print(f"  RF1: {rf1_status}")
    else:
        print(
            "  Skipping RF1/RF2 — refresh parquet files not found. "
            "Run `python -m setup.generate_data --refresh` to generate them."
        )

    print("  Starting power query stream...")
    stream = run_stream(runner, namespace, stream_idx=0, order=order)

    if rf1 is not None:
        print("  Running RF2...")
        rf2 = _time_refresh(runner.engine, data_dir, namespace, "RF2", set_n=1)
        rf2_status = f"ERROR: {rf2.error}" if rf2.error else f"{rf2.elapsed_seconds:.3f}s"
        print(f"  RF2: {rf2_status}")

    return stream, rf1, rf2


# ---------------------------------------------------------------------------
# Throughput test
# ---------------------------------------------------------------------------

def _run_throughput_test(
    runner: BenchmarkRunner,
    namespace: str,
    data_dir: Path,
    n_streams: int,
    update_streams: int,
    order: list[int],
) -> tuple[list[list[QueryResult]], list[RefreshResult], float]:
    """
    Run n_streams query streams in parallel threads alongside a refresh thread
    that runs update_streams combined RF operations (sets 2..update_streams+1).
    """
    stream_results: list[list[QueryResult]] = [[] for _ in range(n_streams)]
    refresh_results: list[RefreshResult] = []
    lock = threading.Lock()

    def query_thread(idx: int) -> None:
        forked = _fork_runner(runner)
        stream_results[idx] = run_stream(forked, namespace, stream_idx=idx, order=order)

    def refresh_thread() -> None:
        forked_engine = runner.engine.fork_for_stream()
        # Throughput refresh uses sets 2..update_streams+1 (set 1 was used by power test)
        for set_n in range(2, update_streams + 2):
            if not _has_refresh_data(data_dir, set_n):
                print(f"  Warning: throughput refresh set {set_n} not found, skipping.")
                continue
            result = _time_refresh(forked_engine, data_dir, namespace, "RF", set_n)
            with lock:
                refresh_results.append(result)
            status = f"ERROR: {result.error}" if result.error else f"{result.elapsed_seconds:.3f}s"
            print(f"  throughput RF set {set_n}: {status}")

    threads: list[threading.Thread] = []
    for i in range(n_streams):
        threads.append(threading.Thread(target=query_thread, args=(i,)))
    threads.append(threading.Thread(target=refresh_thread))

    print(f"  Starting {n_streams} query stream(s) + refresh thread ({update_streams} set(s))...")
    start = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    interval = round(time.perf_counter() - start, 4)

    return stream_results, refresh_results, interval


# ---------------------------------------------------------------------------
# Score calculations
# ---------------------------------------------------------------------------

def _compute_power_score(
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


def _compute_throughput_score(
    n_streams: int, interval: float, scale_factor: int
) -> float:
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
) -> PowerSummary:
    """
    Run the full TPC-H power + throughput benchmark.

    Args:
        runner:          BenchmarkRunner with a set-up engine.
        namespace:       Iceberg namespace containing the TPC-H tables.
        data_dir:        Directory containing local refresh parquet files.
        update_streams:  Number of combined RF operations in the throughput refresh
                         thread. Defaults to max(1, round(0.1 * scale_factor)).
                         Tune this down if Iceberg catalog write contention is a
                         bottleneck.
        monitor_log_dir: Directory for the TSV resource-usage log. Defaults to
                         runner.result_dir.
    """
    sf = runner.scale_factor
    n_streams = _spec_stream_count(sf)
    n_update = update_streams if update_streams is not None else _default_update_streams(sf)

    log_dir = monitor_log_dir or runner.result_dir
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    monitor_log = log_dir / f"power_monitor_{runner.engine_name}_sf{sf}_{ts}.tsv"

    order = _load_power_order()

    print(f"  SF={sf} → {n_streams} throughput stream(s), {n_update} update stream(s)")

    monitor = StreamMonitor(log_path=monitor_log)
    monitor.start()
    print(f"  Resource monitor logging to {monitor_log}")

    try:
        # ---- Power test ----
        print("\n  [Power test]")
        power_stream, rf1, rf2 = _run_power_test(runner, namespace, data_dir, order)
        power_score = _compute_power_score(power_stream, rf1, rf2, sf)
        if power_score is not None:
            print(f"  power_score = {power_score:.2f} QphH@{sf}GB")

        # ---- Throughput test ----
        print("\n  [Throughput test]")
        tp_streams, tp_refresh, tp_interval = _run_throughput_test(
            runner, namespace, data_dir, n_streams, n_update, order
        )
        throughput_score = _compute_throughput_score(n_streams, tp_interval, sf)
        print(f"  throughput interval = {tp_interval:.2f}s")
        print(f"  throughput_score    = {throughput_score:.2f} QphH@{sf}GB")

    finally:
        monitor.stop()

    qphh = round((power_score * throughput_score) ** 0.5, 2) if power_score else None
    if qphh is not None:
        print(f"  QphH@{sf}GB = {qphh:.2f}")

    return PowerSummary(
        power_stream=power_stream,
        rf1=rf1,
        rf2=rf2,
        power_score=power_score,
        throughput_streams=tp_streams,
        throughput_refresh_results=tp_refresh,
        throughput_interval=tp_interval,
        throughput_score=throughput_score,
        qphh=qphh,
        monitor_log=monitor_log,
    )
