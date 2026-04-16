from __future__ import annotations

from pathlib import Path

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

        from catalogs.ducklake import DuckLakeCatalog
        if isinstance(self.catalog, DuckLakeCatalog):
            raise NotImplementedError(
                "DuckLake catalog is DuckDB-only and cannot be used with the Spark engine."
            )

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

    def run_rf1(self, data_dir: Path, namespace: str, set_n: int) -> None:
        assert self._spark is not None, "Call setup() before run_rf1()"
        orders = str((data_dir / f"orders_u{set_n}.parquet").absolute())
        lineitem = str((data_dir / f"lineitem_u{set_n}.parquet").absolute())
        (self._spark.read.parquet(orders)
            .writeTo(f"{self._catalog_alias}.{namespace}.orders")
            .append())
        (self._spark.read.parquet(lineitem)
            .writeTo(f"{self._catalog_alias}.{namespace}.lineitem")
            .append())

    def run_rf2(self, data_dir: Path, namespace: str, set_n: int) -> None:
        assert self._spark is not None, "Call setup() before run_rf2()"
        delete_keys = str((data_dir / f"delete_set_{set_n}.parquet").absolute())
        # Register as a temp view so it's referenceable in DELETE SQL
        self._spark.read.parquet(delete_keys).createOrReplaceTempView("_rf2_delete_keys")
        self._spark.sql(f"USE {self._catalog_alias}.{namespace}")
        self._spark.sql(
            "DELETE FROM orders WHERE o_orderkey IN "
            "(SELECT o_orderkey FROM _rf2_delete_keys)"
        )
        self._spark.sql(
            "DELETE FROM lineitem WHERE l_orderkey IN "
            "(SELECT o_orderkey FROM _rf2_delete_keys)"
        )

    def teardown(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None
            self._catalog_alias = None
