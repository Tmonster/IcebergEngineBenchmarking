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

    def run_query_file(self, path: Path, namespace: str) -> tuple[list[tuple], list[str], int]:
        return self.run_query(path.read_text(), namespace)
