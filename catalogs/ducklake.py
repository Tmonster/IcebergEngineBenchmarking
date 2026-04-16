"""
DuckLake catalog — DuckDB-native transactional table format.
Uses a local SQLite metadata file (.ducklake) and a data directory for Parquet files.

DuckDB-only: Spark and other engines raise NotImplementedError at setup().

Config (catalog.yml):
  type: ducklake
  namespace: benchmarks
  metadata_path: ducklake/tpch.ducklake   # SQLite metadata file (created if absent)
  data_path: ducklake/files               # directory for DuckLake Parquet data files
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from catalogs.base import Catalog, CatalogConfig

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]

_ALIAS = "ducklake_catalog"


class DuckLakeCatalog(Catalog):
    def __init__(self, config: CatalogConfig):
        super().__init__(config)
        self.metadata_path = Path(config.extra.get("metadata_path", "ducklake/tpch.ducklake"))
        self.data_path = Path(config.extra.get("data_path", "ducklake/files"))

    def provision(self, namespace: str, data_dir: Path) -> None:
        from setup.write_tables import write_tpch_tables
        write_tpch_tables(catalog=self, namespace=namespace, data_dir=data_dir)

    def teardown(self, namespace: str) -> None:
        import duckdb
        from engines.duckdb.catalog_adapters import attach_catalog

        with duckdb.connect() as conn:
            alias = attach_catalog(conn, self)
            for table in TPCH_TABLES:
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {alias}.{namespace}.{table}")
                except Exception:
                    pass
            try:
                conn.execute(f"DROP SCHEMA IF EXISTS {alias}.{namespace}")
            except Exception:
                pass

    def table_ref(self, table: str, namespace: str | None = None) -> str:
        ns = namespace or self.config.namespace
        return f"{_ALIAS}.{ns}.{table}"

    def connection_properties(self) -> dict[str, Any]:
        return {
            "type": "ducklake",
            "metadata_path": str(self.metadata_path.absolute()),
            "data_path": str(self.data_path.absolute()),
        }
