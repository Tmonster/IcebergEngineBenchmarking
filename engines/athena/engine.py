"""
Athena engine for TPC-H analytical benchmarks against S3 Tables.

Athena queries are asynchronous: run_query() submits via start_query_execution(),
polls with exponential backoff until the query completes, then fetches rows via
the paginated get_query_results() API.

RF1/RF2 are not supported — power, throughput, and composite benchmarks require
staging refresh parquet files in S3 and are disabled for this engine.

Catalog config fields required by this engine (in addition to standard s3tables fields):
  athena_output_location:  S3 URI for Athena query result storage
                           e.g. s3://my-bucket/athena-results/
  athena_catalog:          Athena data source name for the S3 Tables bucket
                           (registered in the Athena console or via API)
                           e.g. my_s3tables_catalog
"""
from __future__ import annotations

import time
from pathlib import Path

import boto3

from catalogs.base import Catalog
from engines.base import Engine


class AthenaEngine(Engine):
    SUPPORTED_BENCHMARKS = frozenset({"load", "analytical"})

    def __init__(self, catalog: Catalog):
        super().__init__(catalog)
        props = catalog.connection_properties()
        self._region: str = props["region"]
        self._output_location: str = props["athena_output_location"]
        self._athena_catalog: str = props.get("athena_catalog", "s3tablescatalog")
        self._client = None

    def setup(self) -> None:
        self._client = boto3.client("athena", region_name=self._region)

    def teardown(self) -> None:
        self._client = None

    def run_query(self, sql: str, namespace: str) -> tuple[list[tuple], list[str], int]:
        assert self._client is not None, "Call setup() before run_query()"

        response = self._client.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={
                "Database": namespace,
                "Catalog": self._athena_catalog,
            },
            ResultConfiguration={
                "OutputLocation": self._output_location,
            },
        )
        execution_id = response["QueryExecutionId"]

        # Poll with exponential backoff: 100ms → max 2s
        wait = 0.1
        while True:
            execution = self._client.get_query_execution(QueryExecutionId=execution_id)
            state = execution["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(wait)
            wait = min(wait * 1.5, 2.0)

        if state != "SUCCEEDED":
            reason = execution["QueryExecution"]["Status"].get("StateChangeReason", "unknown")
            raise RuntimeError(f"Athena query {state}: {reason}")

        # Fetch rows via paginated API.
        # The first row in the first page is the column header — skip it.
        rows: list[tuple] = []
        col_names: list[str] = []
        paginator = self._client.get_paginator("get_query_results")
        first_page = True
        for page in paginator.paginate(QueryExecutionId=execution_id):
            if not col_names:
                col_names = [
                    col["Label"]
                    for col in page["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
                ]
            page_rows = page["ResultSet"]["Rows"]
            for row in page_rows[1 if first_page else 0:]:
                rows.append(tuple(d.get("VarCharValue", "") for d in row["Data"]))
            first_page = False

        return rows, col_names, len(rows)

    def run_rf1(self, data_dir: Path, namespace: str, set_n: int) -> None:
        raise NotImplementedError(
            "Athena does not support RF1 in this benchmark — "
            "use 'analytical' or 'load' benchmarks only."
        )

    def run_rf2(self, data_dir: Path, namespace: str, set_n: int) -> None:
        raise NotImplementedError(
            "Athena does not support RF2 in this benchmark — "
            "use 'analytical' or 'load' benchmarks only."
        )
