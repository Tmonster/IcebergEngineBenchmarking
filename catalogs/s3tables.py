from __future__ import annotations

from pathlib import Path
from typing import Any

import boto3

from catalogs.base import Catalog, CatalogConfig


TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


class S3TablesCatalog(Catalog):
    def __init__(self, config: CatalogConfig):
        super().__init__(config)
        self.region: str = config.extra["region"]
        self.account_id: str = config.extra["account_id"]
        self.bucket: str = config.extra["bucket"]
        self.rest_endpoint = f"https://s3tables.{self.region}.amazonaws.com/iceberg"
        self.warehouse = (
            f"arn:aws:s3tables:{self.region}:{self.account_id}:bucket/{self.bucket}"
        )
        self._client = boto3.client("s3tables", region_name=self.region)

    def provision(self, namespace: str, data_dir: Path) -> None:
        from setup.write_tables import write_tpch_tables

        self._create_namespace(namespace)
        write_tpch_tables(catalog=self, namespace=namespace, data_dir=data_dir)

    def teardown(self, namespace: str) -> None:
        for table in TPCH_TABLES:
            try:
                self._client.delete_table(
                    tableBucketARN=self.warehouse,
                    namespace=namespace,
                    name=table,
                )
            except self._client.exceptions.NotFoundException:
                pass

        try:
            self._client.delete_namespace(
                tableBucketARN=self.warehouse,
                namespace=namespace,
            )
        except self._client.exceptions.NotFoundException:
            pass

    def table_ref(self, table: str, namespace: str | None = None) -> str:
        ns = namespace or self.config.namespace
        return f"{ns}.{table}"

    def connection_properties(self) -> dict[str, Any]:
        return {
            "type": "s3tables",
            "s3tables_arn": self.warehouse,
            "region": self.region,
        }

    def _create_namespace(self, namespace: str) -> None:
        try:
            self._client.create_namespace(
                tableBucketARN=self.warehouse,
                namespace=[namespace],
            )
        except self._client.exceptions.ConflictException:
            pass  # namespace already exists
