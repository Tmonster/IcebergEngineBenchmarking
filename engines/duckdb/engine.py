from __future__ import annotations

from typing import Any

import duckdb

from catalogs.base import Catalog
from catalogs.local import LocalCatalog
from engines.base import Engine
from engines.duckdb.catalog_adapters import (
    CATALOG_ALIAS,
    attach_catalog,
    setup_local_views,
)

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


class DuckDBEngine(Engine):
    def __init__(self, catalog: Catalog):
        super().__init__(catalog)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._catalog_alias: str | None = None

    def setup(self) -> None:
        self._conn = duckdb.connect()
        self._catalog_alias = attach_catalog(self._conn, self.catalog)

        # Local catalogs need explicit view creation in place of ATTACH
        if isinstance(self.catalog, LocalCatalog):
            props = self.catalog.connection_properties()
            setup_local_views(
                conn=self._conn,
                warehouse_path=props["warehouse_path"],
                namespace=self.catalog.config.namespace,
                tables=TPCH_TABLES,
            )

    def run_query(self, sql: str, namespace: str) -> tuple[list[tuple], list[str], int]:
        assert self._conn is not None, "Call setup() before run_query()"
        # Set search path so unqualified table names in TPC-H SQL resolve correctly
        self._conn.execute(f"USE {self._catalog_alias}.{namespace};")
        relation = self._conn.execute(sql)
        rows = relation.fetchall()
        col_names = [desc[0] for desc in relation.description]
        return rows, col_names, len(rows)

    def teardown(self) -> None:
        if self._conn is not None:
            if self._catalog_alias:
                try:
                    self._conn.execute(f"DETACH {self._catalog_alias};")
                except Exception:
                    pass
            self._conn.close()
            self._conn = None
            self._catalog_alias = None
