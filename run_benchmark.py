"""
Usage:
    python run_benchmark.py --engine duckdb --benchmark load --sf 10
    python run_benchmark.py --engine duckdb --benchmark analytical --sf 10
    python run_benchmark.py --engine duckdb --benchmark power --sf 10
    python run_benchmark.py --engine duckdb --benchmark throughput --sf 10
    python run_benchmark.py --engine duckdb --benchmark composite --sf 10
    python run_benchmark.py --engine duckdb --benchmark analytical --keep-tables --namespace my_ns
    python run_benchmark.py --engine duckdb --benchmark analytical --skip-datagen --namespace my_ns
"""
from __future__ import annotations

import argparse
import json
import uuid
from dataclasses import asdict
from pathlib import Path

import yaml

from catalogs import load_catalog
from engines import load_engine
from benchmarks.runner import BenchmarkRunner

BENCHMARK_CHOICES = ["load", "analytical", "power", "throughput", "composite"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--engine", required=True, choices=["duckdb", "spark"])
    parser.add_argument("--benchmark", required=True, choices=BENCHMARK_CHOICES)
    parser.add_argument("--sf", type=int, default=None, help="Override scale factor from config")
    parser.add_argument("--catalog-config", default="config/catalog.yml")
    parser.add_argument("--benchmark-config", default="config/benchmark.yml")
    parser.add_argument("--namespace", default=None, help="Namespace (default: auto-generated UUID)")
    parser.add_argument("--keep-tables", action="store_true", help="Skip teardown after run")
    parser.add_argument("--skip-datagen", action="store_true", default=False,
                        help="Skip data generation and use existing data in --namespace")
    parser.add_argument("--update-streams", type=int, default=None,
                        help="Number of refresh sets in the throughput test (default: max(1, round(0.1 * sf)))")
    args = parser.parse_args()

    if args.skip_datagen and not args.namespace:
        parser.error("--namespace is required when --skip-datagen is set")

    if args.benchmark == "load" and args.skip_datagen:
        parser.error("--skip-datagen is not applicable to the load benchmark")

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

    # The load benchmark times provisioning itself — skip the pre-provision step.
    # All other benchmarks provision first (unless --skip-datagen).
    if args.benchmark != "load" and not args.skip_datagen:
        print(f"Provisioning namespace '{namespace}'...")
        catalog.provision(namespace=namespace, data_dir=data_dir)

    print(f"\nRunning {args.benchmark} benchmark with {args.engine}...")
    engine.setup()
    out = None
    try:
        if args.benchmark == "load":
            from benchmarks import load
            result = load.run(
                catalog=catalog,
                engine_name=args.engine,
                namespace=namespace,
                data_dir=data_dir,
                scale_factor=scale_factor,
            )
            out = result_dir / f"{args.engine}_load_sf{scale_factor}_{namespace}.json"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(json.dumps(asdict(result), indent=2))

        elif args.benchmark == "analytical":
            from benchmarks import analytical
            results = analytical.run(
                runner=runner,
                namespace=namespace,
                scale_factor=scale_factor,
                warmup_runs=bench_cfg["warmup_runs"],
                benchmark_runs=bench_cfg["benchmark_runs"],
            )
            out = runner.write_results(results, tag="analytical")

        elif args.benchmark == "power":
            from benchmarks import power
            from dataclasses import asdict
            result = power.run(
                runner=runner,
                namespace=namespace,
                data_dir=data_dir,
            )
            ts = result.stream[0].timestamp if result.stream else None
            records = [asdict(r) for r in result.stream]
            for rf in filter(None, [result.rf1, result.rf2]):
                records.append({
                    "engine": args.engine,
                    "benchmark": "power",
                    "query": rf.rf,
                    "namespace": namespace,
                    "scale_factor": scale_factor,
                    "run": 0,
                    "elapsed_seconds": rf.elapsed_seconds,
                    "rows_returned": 0,
                    "timestamp": ts,
                    "result_correct": None,
                    "error": rf.error,
                })
            records.append({
                "engine": args.engine,
                "benchmark": "power",
                "query": "power_score",
                "namespace": namespace,
                "scale_factor": scale_factor,
                "run": 0,
                "elapsed_seconds": None,
                "rows_returned": None,
                "timestamp": ts,
                "result_correct": None,
                "error": None,
                "power_score": result.power_score,
            })
            out = runner.write_json(records, tag="power")
            if result.monitor_log:
                print(f"\nMonitor log: {result.monitor_log}")

        elif args.benchmark == "throughput":
            from benchmarks import throughput
            result = throughput.run(
                runner=runner,
                namespace=namespace,
                data_dir=data_dir,
                update_streams=args.update_streams,
            )
            all_results = [r for stream in result.streams for r in stream]
            out = runner.write_results(all_results, tag="throughput")
            if result.monitor_log:
                print(f"\nMonitor log: {result.monitor_log}")

        elif args.benchmark == "composite":
            from benchmarks import composite
            result = composite.run(
                runner=runner,
                namespace=namespace,
                data_dir=data_dir,
                update_streams=args.update_streams,
            )
            all_results = [
                *result.power.stream,
                *(r for stream in result.throughput.streams for r in stream),
            ]
            out = runner.write_results(all_results, tag="composite")
            for log in (result.power.monitor_log, result.throughput.monitor_log):
                if log:
                    print(f"\nMonitor log: {log}")

    finally:
        engine.teardown()
        if not args.keep_tables and not args.skip_datagen:
            print(f"\nTearing down namespace '{namespace}'...")
            catalog.teardown(namespace=namespace)

    if out:
        print(f"\nResults written to {out}")


if __name__ == "__main__":
    main()
