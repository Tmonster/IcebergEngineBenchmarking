"""
TPC-H Power Test.
Single stream executing all 22 queries in the spec-defined permutation order.
Measures both per-query latency and total elapsed stream time.
"""
from __future__ import annotations

import time
from pathlib import Path

from benchmarks.runner import BenchmarkRunner, QueryResult

QUERY_DIR = Path("queries/tpch/queries")
POWER_ORDER_FILE = Path("queries/tpch/power_order.txt")


def _load_power_order() -> list[int]:
    return [
        int(n.strip())
        for n in POWER_ORDER_FILE.read_text().splitlines()
        if n.strip()
    ]


def run(
    runner: BenchmarkRunner,
    namespace: str,
) -> tuple[list[QueryResult], float]:
    """
    Run the TPC-H power test.
    Returns (per_query_results, total_stream_elapsed_seconds).
    """
    order = _load_power_order()
    results: list[QueryResult] = []

    stream_start = time.perf_counter()
    for seq_idx, q_num in enumerate(order):
        qfile = QUERY_DIR / f"q{q_num:02d}.sql"
        sql = qfile.read_text()
        result = runner.time_query(
            sql=sql,
            query_name=f"q{q_num:02d}",
            benchmark="power",
            namespace=namespace,
            run=seq_idx,
        )
        results.append(result)
        status = f"ERROR: {result.error}" if result.error else f"{result.elapsed_seconds:.3f}s"
        print(f"  [{seq_idx + 1:02d}/22] q{q_num:02d}: {status}")

    total_elapsed = time.perf_counter() - stream_start
    return results, total_elapsed
