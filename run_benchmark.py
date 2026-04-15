"""
Usage:
    python run_benchmark.py --engine duckdb --benchmark analytical
    python run_benchmark.py --engine duckdb --benchmark power --sf 10
    python run_benchmark.py --engine duckdb --benchmark analytical --keep-tables --namespace my_ns
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
    args = parser.parse_args()

    catalog_cfg = yaml.safe_load(Path(args.catalog_config).read_text())
    bench_cfg = yaml.safe_load(Path(args.benchmark_config).read_text())

    scale_factor = args.sf or bench_cfg["scale_factor"]
    namespace = args.namespace or f"bench_{uuid.uuid4().hex[:8]}"
    data_dir = Path("data")
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
            results, total_elapsed = power.run(runner=runner, namespace=namespace)
            out = runner.write_results(results, tag="power")
            print(f"\nTotal power test stream time: {total_elapsed:.2f}s")
    finally:
        engine.teardown()
        if not args.keep_tables:
            print(f"\nTearing down namespace '{namespace}'...")
            catalog.teardown(namespace=namespace)

    print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()
