"""
Translates a Catalog's connection_properties() into PySpark config key/value pairs.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from catalogs.base import Catalog

SPARK_VERSION = "4.0"
ICEBERG_VERSION = "1.10.1"

_PACKAGES = ",".join([
    "com.amazonaws:aws-java-sdk-bundle:1.12.661",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "software.amazon.awssdk:bundle:2.29.38",
    "com.github.ben-manes.caffeine:caffeine:3.1.8",
    "org.apache.commons:commons-configuration2:2.11.0",
    "software.amazon.s3tables:s3-tables-catalog-for-iceberg:0.1.8",
    f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.13:{ICEBERG_VERSION}",
])


def spark_catalog_alias(catalog: "Catalog") -> str:
    props = catalog.connection_properties()
    match props["type"]:
        case "s3tables":
            return "s3tablesbucket"
        case "local":
            return "local_iceberg"
        case _:
            raise ValueError(f"No Spark adapter for catalog type: {props['type']!r}")


def spark_config(catalog: "Catalog") -> dict[str, str]:
    props = catalog.connection_properties()
    match props["type"]:
        case "s3tables":
            return _s3tables_config(props)
        case "local":
            return _local_config(props)
        case _:
            raise ValueError(f"No Spark adapter for catalog type: {props['type']!r}")


def _s3tables_config(props: dict) -> dict[str, str]:
    alias = "s3tablesbucket"
    return {
        "spark.jars.packages": _PACKAGES,
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{alias}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{alias}.catalog-impl": (
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        ),
        f"spark.sql.catalog.{alias}.warehouse": props["s3tables_arn"],
        f"spark.sql.catalog.{alias}.client.region": props["region"],
        "spark.driver.memory": "25g",
        "spark.executor.memory": "25g",
        "spark.local.dir": "/tmp/spark-spill",
    }


def _local_config(props: dict) -> dict[str, str]:
    alias = "local_iceberg"
    iceberg_runtime = (
        f"org.apache.iceberg:iceberg-spark-runtime-{SPARK_VERSION}_2.13:{ICEBERG_VERSION}"
    )
    return {
        "spark.jars.packages": iceberg_runtime,
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{alias}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{alias}.type": "hadoop",
        f"spark.sql.catalog.{alias}.warehouse": props["warehouse_path"],
        "spark.driver.host": "localhost",
        "spark.driver.bindAddress": "127.0.0.1",
    }
