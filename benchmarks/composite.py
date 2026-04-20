"""
TPC-H Composite Benchmark.

Runs the full TPC-H composite metric: power test followed by throughput test.
Computes power score, throughput score, and the official QphH composite score.

  QphH@SF = sqrt(power_score * throughput_score)

This matches the TPC-H spec composite metric (section 5.4).
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from benchmarks.runner import BenchmarkRunner, QueryResult
from benchmarks.power import PowerTestResult, run as run_power
from benchmarks.throughput import ThroughputTestResult, run as run_throughput


@dataclass
class CompositeResult:
    power: PowerTestResult
    throughput: ThroughputTestResult
    qphh: float | None


def run(
    runner: BenchmarkRunner,
    namespace: str,
    data_dir: Path = Path("data"),
    update_streams: int | None = None,
) -> CompositeResult:
    """
    Run the TPC-H composite benchmark (power test + throughput test).

    Args:
        runner:         BenchmarkRunner with a set-up engine.
        namespace:      Iceberg namespace containing the TPC-H tables.
        data_dir:       SF-specific data directory (e.g. data/sf=10).
        update_streams: Number of combined RF operations in the throughput refresh
                        thread. Defaults to max(1, round(0.1 * scale_factor)).
    """
    sf = runner.scale_factor

    print("\n  [Power test]")
    power = run_power(runner, namespace, data_dir=data_dir)

    print("\n  [Throughput test]")
    throughput = run_throughput(runner, namespace, data_dir=data_dir, update_streams=update_streams)

    qphh = None
    if power.power_score is not None:
        qphh = round((power.power_score * throughput.throughput_score) ** 0.5, 2)
        print(f"\n  QphH@{sf}GB = {qphh:.2f}")
    else:
        print(f"\n  QphH not computed (power score unavailable — RF data missing?)")

    return CompositeResult(power=power, throughput=throughput, qphh=qphh)
