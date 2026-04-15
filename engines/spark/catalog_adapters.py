"""
Translates a Catalog's connection_properties() into PySpark config key/value pairs.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from catalogs.base import Catalog

# JAR versions — update these if you need a newer Iceberg or S3 Tables release
_ICEBERG_SPARK_RUNTIME = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_S3TABLES_RUNTIME = "software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.4"
# AWS SDK v2 is not bundled in the S3 Tables runtime JAR — it's expected to be
# provided by the environment (EMR/Glue). For local Spark we add it explicitly.
_AWS_SDK_BUNDLE = "software.amazon.awssdk:bundle:2.28.0"


def spark_catalog_alias(catalog: "Catalog") -> str:
    props = catalog.connection_properties()
    match props["type"]:
        case "s3tables":
            return "s3tablescatalog"
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
    alias = "s3tablescatalog"
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{alias}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{alias}.catalog-impl": (
            "software.amazon.s3tables.iceberg.S3TablesCatalog"
        ),
        f"spark.sql.catalog.{alias}.warehouse": props["s3tables_arn"],
        "spark.jars.packages": f"{_ICEBERG_SPARK_RUNTIME},{_S3TABLES_RUNTIME},{_AWS_SDK_BUNDLE}",
        # S3A credential chain for Spark executors reading Iceberg data files from S3
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
        ),
        "spark.hadoop.fs.s3a.endpoint.region": props["region"],
        # Bind driver and block manager to localhost so shuffle fetches don't fail
        # when the machine hostname doesn't resolve to an accessible address
        "spark.driver.host": "localhost",
        "spark.driver.bindAddress": "127.0.0.1",
    }


def _local_config(props: dict) -> dict[str, str]:
    alias = "local_iceberg"
    return {
        "spark.sql.extensions": (
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        ),
        f"spark.sql.catalog.{alias}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{alias}.type": "hadoop",
        f"spark.sql.catalog.{alias}.warehouse": props["warehouse_path"],
        "spark.jars.packages": _ICEBERG_SPARK_RUNTIME,
    }
