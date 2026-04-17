"""
Usage:
    python run_benchmark.py --engine duckdb --benchmark analytical
    python run_benchmark.py --engine duckdb --benchmark power --sf 10
    python run_benchmark.py --engine duckdb --benchmark analytical --keep-tables --namespace my_ns
    python run_benchmark.py --engine duckdb --benchmark analytical --skip-datagen --namespace my_ns
"""
from __future__ import annotations

import argparse
import uuid
from pathlib import Path

import yaml

from catalogs import load_catalog
from engines import load_engine
from benchmarks.runner import BenchmarkRunner


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, choices=["duckdb", "spark"])
    parser.add_argument("--benchmark", required=True, choices=["analytical", "power"])
    parser.add_argument("--sf", type=int, default=None, help="Override scale factor from config")
    parser.add_argument("--catalog-config", default="config/catalog.yml")
    parser.add_argument("--benchmark-config", default="config/benchmark.yml")
    parser.add_argument("--namespace", default=None, help="Namespace (default: auto-generated UUID)")
    parser.add_argument("--keep-tables", action="store_true", help="Skip teardown after run")
    parser.add_argument("--skip-datagen", action="store_true", default=False, help="Skip data generation and use existing data in --namespace")
    parser.add_argument("--update-streams", type=int, default=None, help="Number of refresh sets in the throughput test (default: max(1, round(0.1 * sf)))")
    args = parser.parse_args()

    if args.skip_datagen and not args.namespace:
        parser.error("--namespace is required when --skip-datagen is set")

    catalog_cfg = yaml.safe_load(Path(args.catalog_config).read_text())
    bench_cfg = yaml.safe_load(Path(args.benchmark_config).read_text())

    scale_factor = args.sf or bench_cfg["scale_factor"]
    namespace = args.namespace or f"bench_{uuid.uuid4().hex[:8]}"
    data_dir = Path("data") / f"sf={scale_factor}"
    result_dir = Path(bench_cfg["result_dir"])

    catalog = load_catalog(catalog_cfg)
    engine = load_engine(args.engine, catalog)
    runner = BenchmarkRunner(
        engine=engine,
        catalog=catalog,
        engine_name=args.engine,
        scale_factor=scale_factor,
        result_dir=result_dir,
    )

    if not args.skip_datagen:
        print(f"Provisioning namespace '{namespace}'...")
        catalog.provision(namespace=namespace, data_dir=data_dir)

    print(f"\nRunning {args.benchmark} benchmark with {args.engine}...")
    engine.setup()
    try:
        if args.benchmark == "analytical":
            from benchmarks import analytical
            results = analytical.run(
                runner=runner,
                namespace=namespace,
                scale_factor=scale_factor,
                warmup_runs=bench_cfg["warmup_runs"],
                benchmark_runs=bench_cfg["benchmark_runs"],
            )
            out = runner.write_results(results, tag="analytical")
        else:
            from benchmarks import power
            summary = power.run(
                runner=runner,
                namespace=namespace,
                data_dir=data_dir,
                update_streams=args.update_streams,
            )
            all_results = [
                *summary.power_stream,
                *(r for stream in summary.throughput_streams for r in stream),
            ]
            out = runner.write_results(all_results, tag="power")
            if summary.monitor_log:
                print(f"\nMonitor log: {summary.monitor_log}")
    finally:
        engine.teardown()
        if not args.keep_tables and not args.skip_datagen:
            print(f"\nTearing down namespace '{namespace}'...")
            catalog.teardown(namespace=namespace)

    print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()
