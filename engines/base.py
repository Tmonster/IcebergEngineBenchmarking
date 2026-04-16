from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from catalogs.base import Catalog


class Engine(ABC):
    def __init__(self, catalog: Catalog):
        self.catalog = catalog

    @abstractmethod
    def setup(self) -> None:
        """Initialize engine connection/session and attach catalog."""

    @abstractmethod
    def run_query(self, sql: str, namespace: str) -> tuple[list[tuple], list[str], int]:
        """
        Execute sql against the catalog namespace.
        Returns (rows, column_names, row_count).
        """

    @abstractmethod
    def teardown(self) -> None:
        """Close connections and release resources."""

    @abstractmethod
    def run_rf1(self, data_dir: Path, namespace: str, set_n: int) -> None:
        """
        RF1: insert refresh rows from local parquet files into the live tables.
        Reads orders_u{set_n}.parquet and lineitem_u{set_n}.parquet from data_dir.
        """

    @abstractmethod
    def run_rf2(self, data_dir: Path, namespace: str, set_n: int) -> None:
        """
        RF2: delete rows from the live tables using local parquet delete keys.
        Reads delete_set_{set_n}.parquet from data_dir.
        """

    def fork_for_stream(self) -> "Engine":
        """
        Return a lightweight engine suitable for running in a concurrent thread.

        DuckDB: returns a cursor-backed runner that shares the parent connection's
        attached catalog but has independent transaction and USE state.
        Spark: returns self — SparkSession is thread-safe for concurrent job submission.
        """
        return self

    def run_query_file(self, path: Path, namespace: str) -> tuple[list[tuple], list[str], int]:
        return self.run_query(path.read_text(), namespace)
