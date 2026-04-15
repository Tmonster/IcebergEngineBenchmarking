"""
Generate TPC-H data using DuckDB's built-in tpch extension.
Output: one Parquet file per table in data/<table>.parquet

Usage:
    python -m setup.generate_data --sf 10
    python -m setup.generate_data --sf 1 --data-dir /tmp/tpch
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]


def generate(scale_factor: int, data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    print(f"Generating TPC-H data at scale factor {scale_factor}...")

    with duckdb.connect() as conn:
        conn.execute("INSTALL tpch; LOAD tpch;")
        conn.execute(f"CALL dbgen(sf={scale_factor});")

        for table in TPCH_TABLES:
            out = data_dir / f"{table}.parquet"
            conn.execute(f"COPY {table} TO '{out}' (FORMAT PARQUET);")
            row_count = conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {row_count:,} rows → {out}")

    print("Done.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", type=int, default=1, help="TPC-H scale factor")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    args = parser.parse_args()
    generate(scale_factor=args.sf, data_dir=args.data_dir)
