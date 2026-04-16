from catalogs.base import Catalog, CatalogConfig
from catalogs.ducklake import DuckLakeCatalog
from catalogs.local import LocalCatalog
from catalogs.s3tables import S3TablesCatalog


def load_catalog(config: dict) -> Catalog:
    config = dict(config)  # don't mutate caller's dict
    catalog_type = config.pop("type")
    namespace = config.pop("namespace")
    cfg = CatalogConfig(type=catalog_type, namespace=namespace, extra=config)

    match catalog_type:
        case "s3tables":
            return S3TablesCatalog(cfg)
        case "local":
            return LocalCatalog(cfg)
        case "ducklake":
            return DuckLakeCatalog(cfg)
        case _:
            raise ValueError(f"Unknown catalog type: {catalog_type!r}")
