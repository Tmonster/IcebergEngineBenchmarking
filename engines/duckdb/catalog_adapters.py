"""
Translates a Catalog's connection_properties() into DuckDB ATTACH statements.
Each catalog type gets its own _attach_* function.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb

if TYPE_CHECKING:
    from catalogs.base import Catalog

CATALOG_ALIAS = "iceberg_catalog"


def attach_catalog(conn: duckdb.DuckDBPyConnection, catalog: "Catalog") -> str:
    """
    Attach the catalog to a DuckDB connection.
    Returns the catalog alias to use in subsequent USE / qualified references.
    """
    props = catalog.connection_properties()
    catalog_type = props["type"]

    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute("INSTALL aws; LOAD aws;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # turn off external file cache so results are not hot from cache
    conn.execute("pragma enable_external_file_cache=false")
    # conn.execute("SET httpfs_connection_caching=true")

    match catalog_type:
        case "s3tables":
            return _attach_s3tables(conn, props)
        case "local":
            return _attach_local(conn, props)
        case _:
            raise ValueError(f"No DuckDB catalog adapter for type: {catalog_type!r}")


def _attach_s3tables(conn: duckdb.DuckDBPyConnection, props: dict) -> str:
    conn.execute("""
        CREATE SECRET IF NOT EXISTS aws_creds (
            TYPE S3,
            PROVIDER CREDENTIAL_CHAIN
        );
    """)
    # NOTE: DuckDB REST catalog ATTACH syntax may require version >= 1.2 with
    # the iceberg extension. Verify exact parameter names against your DuckDB version.
    conn.execute(f"""
        ATTACH '{props["s3tables_arn"]}' AS {CATALOG_ALIAS} (
            TYPE ICEBERG,
            ENDPOINT_TYPE S3_TABLES
        );
    """)
    return CATALOG_ALIAS


def _attach_local(conn: duckdb.DuckDBPyConnection, props: dict) -> str:
    """
    For local catalogs, create views over iceberg_scan() calls so unqualified
    table names in TPC-H queries resolve correctly after USE <schema>.
    Tables are written to the warehouse path by PyIceberg as standard Iceberg dirs.
    """
    # Local catalog uses direct path scanning rather than catalog attachment
    # because DuckDB's ATTACH doesn't support SQLite-backed PyIceberg catalogs.
    # Views are created in a DuckDB schema that mirrors the namespace.
    return _LOCAL_ALIAS


_LOCAL_ALIAS = "local_iceberg"


def setup_local_views(
    conn: duckdb.DuckDBPyConnection,
    warehouse_path: str,
    namespace: str,
    tables: list[str],
) -> None:
    """Create DuckDB views that point to local Iceberg table directories."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_LOCAL_ALIAS};")
    for table in tables:
        table_path = f"{warehouse_path}/{namespace}/{table}"
        conn.execute(f"""
            CREATE OR REPLACE VIEW {_LOCAL_ALIAS}.{table} AS
            SELECT * FROM iceberg_scan('{table_path}');
        """)
