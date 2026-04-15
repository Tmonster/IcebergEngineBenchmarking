from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class CatalogConfig:
    type: str
    namespace: str
    extra: dict[str, Any] = field(default_factory=dict)


class Catalog(ABC):
    def __init__(self, config: CatalogConfig):
        self.config = config

    @abstractmethod
    def provision(self, namespace: str, data_dir: Path) -> None:
        """Create namespace and write TPC-H Iceberg tables from data_dir Parquet files."""

    @abstractmethod
    def teardown(self, namespace: str) -> None:
        """Drop namespace and all tables within it."""

    @abstractmethod
    def table_ref(self, table: str, namespace: str | None = None) -> str:
        """Return a fully-qualified table reference string for use in engine queries."""

    @abstractmethod
    def connection_properties(self) -> dict[str, Any]:
        """Return engine-agnostic properties that catalog adapters translate per-engine."""
