from catalogs.base import Catalog
from engines.base import Engine


def load_engine(name: str, catalog: Catalog) -> Engine:
    match name:
        case "duckdb":
            from engines.duckdb.engine import DuckDBEngine
            return DuckDBEngine(catalog)
        case "spark":
            from engines.spark.engine import SparkEngine
            return SparkEngine(catalog)
        case _:
            raise ValueError(f"Unknown engine: {name!r}")
