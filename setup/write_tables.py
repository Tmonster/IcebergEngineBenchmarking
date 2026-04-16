"""
Write TPC-H Parquet files into an Iceberg catalog.
Called by Catalog.provision() — not intended to be run directly.

S3 Tables: uses DuckDB ATTACH + CREATE TABLE AS SELECT via read_parquet().
Local:      uses PyIceberg (DuckDB cannot ATTACH a SQLite-backed local catalog).
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from engines.duckdb.catalog_adapters import attach_catalog

if TYPE_CHECKING:
    from catalogs.base import Catalog

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def write_tpch_tables(catalog: "Catalog", namespace: str, data_dir: Path) -> None:
    props = catalog.connection_properties()
    if props["type"] == "local":
        _write_via_pyiceberg(props, namespace, data_dir)
    else:
        # s3tables and ducklake both use DuckDB ATTACH for writes
        _write_via_duckdb(catalog, namespace, data_dir)



def _write_via_duckdb(catalog: "Catalog", namespace: str, data_dir: Path) -> None:
    with duckdb.connect() as conn:
        alias = attach_catalog(conn, catalog)
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{namespace}")
        for table_name in TPCH_TABLES:
            parquet_path = (data_dir / f"{table_name}.parquet").absolute()
            if not parquet_path.exists():
                raise FileNotFoundError(
                    f"Missing {parquet_path}. Run `python -m setup.generate_data` first."
                )
            conn.execute(f"DROP TABLE IF EXISTS {alias}.{namespace}.{table_name}")
            conn.execute(f"""
                CREATE TABLE {alias}.{namespace}.{table_name}
                AS SELECT * FROM read_parquet('{parquet_path}');
            """)
            row_count = conn.execute(
                f"SELECT count(*) FROM {alias}.{namespace}.{table_name}"
            ).fetchone()[0]
            print(f"  {namespace}.{table_name}: {row_count:,} rows written")


def _write_via_pyiceberg(props: dict, namespace: str, data_dir: Path) -> None:
    import pyarrow.parquet as pq
    from pyiceberg.catalog.sql import SqlCatalog

    ice_catalog = SqlCatalog(
        "local",
        **{
            "uri": props["uri"],
            "warehouse": f"file://{props['warehouse_path']}",
        },
    )
    for table_name in TPCH_TABLES:
        parquet_path = data_dir / f"{table_name}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(
                f"Missing {parquet_path}. Run `python -m setup.generate_data` first."
            )
        arrow_table = pq.read_table(str(parquet_path))
        table_id = f"{namespace}.{table_name}"
        if ice_catalog.table_exists(table_id):
            ice_catalog.drop_table(table_id)
        ice_table = ice_catalog.create_table(identifier=table_id, schema=arrow_table.schema)
        ice_table.append(arrow_table)
        print(f"  {table_id}: {len(arrow_table):,} rows written")
