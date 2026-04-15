from __future__ import annotations

import csv
import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from catalogs.base import Catalog
    from engines.base import Engine


@dataclass
class QueryResult:
    engine: str
    benchmark: str
    query: str
    namespace: str
    scale_factor: int
    run: int
    elapsed_seconds: float
    rows_returned: int
    timestamp: str
    result_correct: bool | None = None
    error: str | None = None


class BenchmarkRunner:
    def __init__(
        self,
        engine: Engine,
        catalog: Catalog,
        engine_name: str,
        scale_factor: int,
        result_dir: Path,
    ):
        self.engine = engine
        self.catalog = catalog
        self.engine_name = engine_name
        self.scale_factor = scale_factor
        self.result_dir = result_dir
        self.result_dir.mkdir(parents=True, exist_ok=True)

    def time_query(
        self,
        sql: str,
        query_name: str,
        benchmark: str,
        namespace: str,
        run: int,
        answer_path: Path | None = None,
    ) -> QueryResult:
        error = None
        rows: list[tuple] = []
        col_names: list[str] = []
        row_count = 0

        start = time.perf_counter()
        try:
            rows, col_names, row_count = self.engine.run_query(sql, namespace)
        except Exception as e:
            error = str(e)
        elapsed = time.perf_counter() - start

        result_correct = None
        if answer_path is not None and error is None:
            result_correct = self._verify(rows, answer_path)

        return QueryResult(
            engine=self.engine_name,
            benchmark=benchmark,
            query=query_name,
            namespace=namespace,
            scale_factor=self.scale_factor,
            run=run,
            elapsed_seconds=round(elapsed, 4),
            rows_returned=row_count,
            timestamp=datetime.now(timezone.utc).isoformat(),
            result_correct=result_correct,
            error=error,
        )

    def write_results(self, results: list[QueryResult], tag: str) -> Path:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
        out = self.result_dir / f"{self.engine_name}_{tag}_{ts}.json"
        out.write_text(json.dumps([asdict(r) for r in results], indent=2))
        return out

    def _verify(self, rows: list[tuple], answer_path: Path) -> bool | None:
        if not answer_path.exists():
            return None

        with open(answer_path, newline="") as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            answer_rows = list(reader)

        return _normalize(rows) == _normalize(answer_rows)


def _normalize(rows: list) -> list[tuple]:
    """Sort rows and round floats to 2 decimal places for comparison."""
    normalized = []
    for row in rows:
        norm_row = []
        for val in row:
            try:
                norm_row.append(f"{float(val):.2f}")
            except (ValueError, TypeError):
                norm_row.append(str(val).strip())
        normalized.append(tuple(norm_row))
    return sorted(normalized)
