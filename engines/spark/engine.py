from __future__ import annotations

from catalogs.base import Catalog
from engines.base import Engine
from engines.spark.catalog_adapters import spark_catalog_alias, spark_config


class SparkEngine(Engine):
    def __init__(self, catalog: Catalog):
        super().__init__(catalog)
        self._spark = None
        self._catalog_alias: str | None = None

    def setup(self) -> None:
        import os
        import sys
        from pyspark.sql import SparkSession

        # Ensure PySpark workers use the same Python as the calling process (venv-safe)
        os.environ["PYSPARK_PYTHON"] = sys.executable

        builder = SparkSession.builder.appName("iceberg-benchmark")
        for key, val in spark_config(self.catalog).items():
            builder = builder.config(key, val)

        self._spark = builder.getOrCreate()
        self._catalog_alias = spark_catalog_alias(self.catalog)

    def run_query(self, sql: str, namespace: str) -> tuple[list[tuple], list[str], int]:
        assert self._spark is not None, "Call setup() before run_query()"
        # Set search path so unqualified table names in TPC-H SQL resolve correctly
        self._spark.sql(f"USE {self._catalog_alias}.{namespace}")
        df = self._spark.sql(sql)
        col_names = df.columns
        # collect() triggers actual execution — this is what we're timing
        rows = [tuple(row) for row in df.collect()]
        return rows, col_names, len(rows)

    def teardown(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            self._catalog_alias = None
