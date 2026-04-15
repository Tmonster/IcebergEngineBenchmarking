"""
TPC-H Analytical Benchmark.
Runs each of the 22 queries independently and records per-query latency.
Warmup runs are not verified or recorded.
Verification (if answer files exist) runs on the first timed run only.
"""
from __future__ import annotations

from pathlib import Path

from benchmarks.runner import BenchmarkRunner, QueryResult

QUERY_DIR = Path("queries/tpch/queries")
ANSWER_BASE = Path("queries/tpch/answers")


def _answer_dir(sf: int) -> Path:
    return ANSWER_BASE / f"sf{sf}"


def run(
    runner: BenchmarkRunner,
    namespace: str,
    scale_factor: int,
    warmup_runs: int = 1,
    benchmark_runs: int = 3,
) -> list[QueryResult]:
    answer_dir = _answer_dir(scale_factor)
    query_files = sorted(QUERY_DIR.glob("q*.sql"))
    results: list[QueryResult] = []

    for qfile in query_files:
        sql = qfile.read_text()
        query_name = qfile.stem  # e.g. "q01"
        answer_path = answer_dir / f"{query_name}.csv"

        for _ in range(warmup_runs):
            try:
                runner.engine.run_query(sql, namespace)
            except Exception:
                pass

        for run_idx in range(benchmark_runs):
            # Only verify on the first timed run — answer check doesn't need repeating
            result = runner.time_query(
                sql=sql,
                query_name=query_name,
                benchmark="analytical",
                namespace=namespace,
                run=run_idx,
                answer_path=answer_path if run_idx == 0 else None,
            )
            results.append(result)

            status_parts = [f"{result.elapsed_seconds:.3f}s"]
            if result.result_correct is True:
                status_parts.append("✓")
            elif result.result_correct is False:
                status_parts.append("MISMATCH")
            elif run_idx == 0 and not answer_path.exists():
                status_parts.append("(no answer file)")
            if result.error:
                status_parts = [f"ERROR: {result.error}"]

            print(f"  {query_name} run {run_idx}: {' '.join(status_parts)}")

    return results
