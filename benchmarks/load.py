"""
TPC-H Load Benchmark.
Times how long it takes to provision the TPC-H tables into the catalog.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from catalogs.base import Catalog


@dataclass
class LoadResult:
    engine: str
    namespace: str
    scale_factor: int
    elapsed_seconds: float
    timestamp: str
    error: str | None = None


def run(
    catalog: "Catalog",
    engine_name: str,
    namespace: str,
    data_dir: Path,
    scale_factor: int,
) -> LoadResult:
    error = None
    start = time.perf_counter()
    try:
        catalog.provision(namespace=namespace, data_dir=data_dir)
    except Exception as e:
        error = str(e)
    elapsed = round(time.perf_counter() - start, 4)

    if error:
        print(f"  Load ERROR: {error}")
    else:
        print(f"  Load time: {elapsed:.3f}s")

    return LoadResult(
        engine=engine_name,
        namespace=namespace,
        scale_factor=scale_factor,
        elapsed_seconds=elapsed,
        timestamp=datetime.now(timezone.utc).isoformat(),
        error=error,
    )
