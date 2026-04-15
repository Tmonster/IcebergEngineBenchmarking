"""
SQLite-backed PyIceberg catalog writing to local filesystem.
Intended for local development and CI — no AWS credentials required.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from catalogs.base import Catalog, CatalogConfig


TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


class LocalCatalog(Catalog):
    def __init__(self, config: CatalogConfig):
        super().__init__(config)
        self.warehouse_path = Path(config.extra.get("warehouse_path", "warehouse"))
        self.warehouse_path.mkdir(parents=True, exist_ok=True)

    def provision(self, namespace: str, data_dir: Path) -> None:
        from setup.write_tables import write_tpch_tables

        cat = self._pyiceberg_catalog()
        cat.create_namespace_if_not_exists(namespace)
        write_tpch_tables(catalog=self, namespace=namespace, data_dir=data_dir)

    def teardown(self, namespace: str) -> None:
        cat = self._pyiceberg_catalog()
        for table in TPCH_TABLES:
            try:
                cat.drop_table(f"{namespace}.{table}")
            except Exception:
                pass
        try:
            cat.drop_namespace(namespace)
        except Exception:
            pass

    def table_ref(self, table: str, namespace: str | None = None) -> str:
        """Returns the filesystem path to the Iceberg table directory."""
        ns = namespace or self.config.namespace
        return str((self.warehouse_path / ns / table).absolute())

    def connection_properties(self) -> dict[str, Any]:
        db_path = self.warehouse_path / "catalog.db"
        return {
            "type": "local",
            "warehouse_path": str(self.warehouse_path.absolute()),
            "uri": f"sqlite:///{db_path}",
        }

    def _pyiceberg_catalog(self):
        from pyiceberg.catalog.sql import SqlCatalog

        props = self.connection_properties()
        return SqlCatalog(
            "local",
            **{
                "uri": props["uri"],
                "warehouse": f"file://{props['warehouse_path']}",
            },
        )
