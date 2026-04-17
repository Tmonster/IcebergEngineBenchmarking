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


class _DuckDBCursorEngine:
    """
    Lightweight cursor-backed engine for concurrent throughput streams.
    Shares the parent connection's attached catalog but has its own transaction
    and USE context, making it safe to use from multiple threads simultaneously.
    """

    def __init__(self, cursor: duckdb.DuckDBPyConnection, catalog_alias: str):
        self._cursor = cursor
        self._catalog_alias = catalog_alias
        self._current_namespace: str | None = None

    def _use(self, namespace: str) -> None:
        if namespace != self._current_namespace:
            self._cursor.execute(f"USE {self._catalog_alias}.{namespace}")
            self._current_namespace = namespace

    def run_query(self, sql: str, namespace: str) -> tuple[list[tuple], list[str], int]:
        self._use(namespace)
        self._cursor.execute("BEGIN TRANSACTION READ ONLY")
        rel = self._cursor.execute(sql)
        self._cursor.execute("COMMIT")
        rows = rel.fetchall()
        col_names = [desc[0] for desc in rel.description]
        return rows, col_names, len(rows)

    def run_rf1(self, data_dir: Path, namespace: str, set_n: int) -> None:
        self._use(namespace)
        orders = str((data_dir / f"orders_u{set_n}.parquet").absolute())
        lineitem = str((data_dir / f"lineitem_u{set_n}.parquet").absolute())
        self._cursor.begin()
        self._cursor.execute(f"INSERT INTO orders SELECT * FROM read_parquet('{orders}')")
        self._cursor.execute(f"INSERT INTO lineitem SELECT * FROM read_parquet('{lineitem}')")
        self._cursor.commit()

    def run_rf2(self, data_dir: Path, namespace: str, set_n: int) -> None:
        self._use(namespace)
        delete_keys = str((data_dir / f"delete_set_{set_n}.parquet").absolute())
        self._cursor.begin()
        self._cursor.execute(
            f"DELETE FROM orders WHERE o_orderkey IN "
            f"(SELECT o_orderkey FROM read_parquet('{delete_keys}'))"
        )
        self._cursor.execute(
            f"DELETE FROM lineitem WHERE l_orderkey IN "
            f"(SELECT o_orderkey FROM read_parquet('{delete_keys}'))"
        )
        self._cursor.commit()

    def fork_for_stream(self) -> "_DuckDBCursorEngine":
        return _DuckDBCursorEngine(self._cursor.cursor(), self._catalog_alias)


class DuckDBEngine(Engine):
    def __init__(self, catalog: Catalog):
        super().__init__(catalog)
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._catalog_alias: str | None = None
        self._current_namespace: str | None = None

    def _use(self, namespace: str) -> None:
        if namespace != self._current_namespace:
            self._conn.execute(f"USE {self._catalog_alias}.{namespace}")
            self._current_namespace = namespace

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
        self._use(namespace)
        self._conn.execute("BEGIN TRANSACTION READ ONLY")
        relation = self._conn.execute(sql)
        self._conn.execute("COMMIT")
        rows = relation.fetchall()
        col_names = [desc[0] for desc in relation.description]
        return rows, col_names, len(rows)

    def run_rf1(self, data_dir: Path, namespace: str, set_n: int) -> None:
        assert self._conn is not None, "Call setup() before run_rf1()"
        self._use(namespace)
        orders = str((data_dir / f"orders_u{set_n}.parquet").absolute())
        lineitem = str((data_dir / f"lineitem_u{set_n}.parquet").absolute())
        self._conn.begin()
        self._conn.execute(f"INSERT INTO orders SELECT * FROM read_parquet('{orders}')")
        self._conn.execute(f"INSERT INTO lineitem SELECT * FROM read_parquet('{lineitem}')")
        self._conn.commit()

    def run_rf2(self, data_dir: Path, namespace: str, set_n: int) -> None:
        assert self._conn is not None, "Call setup() before run_rf2()"
        self._use(namespace)
        delete_keys = str((data_dir / f"delete_set_{set_n}.parquet").absolute())
        self._conn.begin()
        self._conn.execute(
            f"DELETE FROM orders WHERE o_orderkey IN "
            f"(SELECT o_orderkey FROM read_parquet('{delete_keys}'))"
        )
        self._conn.execute(
            f"DELETE FROM lineitem WHERE l_orderkey IN "
            f"(SELECT o_orderkey FROM read_parquet('{delete_keys}'))"
        )
        self._conn.commit()

    def fork_for_stream(self) -> _DuckDBCursorEngine:
        assert self._conn is not None, "Call setup() before fork_for_stream()"
        return _DuckDBCursorEngine(self._conn.cursor(), self._catalog_alias)

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
