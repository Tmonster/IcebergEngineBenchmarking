"""
Generate TPC-H data using DuckDB's built-in tpch extension.
Refresh data (update/delete sets) is generated via the dbgen binary in the submodule.

Usage:
    python -m setup.generate_data --sf 10
    python -m setup.generate_data --sf 10 --refresh   # also generate RF parquet files
    python -m setup.generate_data --sf 1 --data-dir /tmp/tpch
"""
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import duckdb

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]

DBGEN_DIR = Path("duckdb-tpch-power-test/tpch_tools_3.0.1/dbgen")

# TBL files have a trailing | delimiter, so there is always one extra empty column.
# Column definitions map name → DuckDB type to match the base table schemas exactly.
_ORDERS_COLS: dict[str, str] = {
    "o_orderkey": "BIGINT",
    "o_custkey": "BIGINT",
    "o_orderstatus": "VARCHAR",
    "o_totalprice": "DECIMAL(15,2)",
    "o_orderdate": "DATE",
    "o_orderpriority": "VARCHAR",
    "o_clerk": "VARCHAR",
    "o_shippriority": "INTEGER",
    "o_comment": "VARCHAR",
}
_LINEITEM_COLS: dict[str, str] = {
    "l_orderkey": "BIGINT",
    "l_partkey": "BIGINT",
    "l_suppkey": "BIGINT",
    "l_linenumber": "INTEGER",
    "l_quantity": "DECIMAL(15,2)",
    "l_extendedprice": "DECIMAL(15,2)",
    "l_discount": "DECIMAL(15,2)",
    "l_tax": "DECIMAL(15,2)",
    "l_returnflag": "VARCHAR",
    "l_linestatus": "VARCHAR",
    "l_shipdate": "DATE",
    "l_commitdate": "DATE",
    "l_receiptdate": "DATE",
    "l_shipinstruct": "VARCHAR",
    "l_shipmode": "VARCHAR",
    "l_comment": "VARCHAR",
}
# delete.N files: one o_orderkey per line with trailing |
_DELETE_COLS: dict[str, str] = {
    "o_orderkey": "BIGINT",
}


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


def generate_refresh_data(scale_factor: int, data_dir: Path, n_sets: int = 1) -> None:
    """
    Generate TPC-H refresh data using the dbgen binary in the submodule.

    Produces three parquet files per set in data_dir:
      orders_u{n}.parquet     — new orders rows for RF1
      lineitem_u{n}.parquet   — new lineitem rows for RF1
      delete_set_{n}.parquet  — order keys to delete for RF2

    Set 1 is used by the power test. Sets 2..n_sets are used by the throughput
    test refresh thread. n_sets should be 1 + update_streams.
    """
    _compile_dbgen()

    dbgen_bin = (DBGEN_DIR / "dbgen").absolute()
    print(f"Generating TPC-H refresh data (sf={scale_factor}, sets=1..{n_sets})...")

    # Run dbgen once — "-U n_sets" generates all sets 1..n_sets in a single pass.
    # Running it once per set would regenerate earlier sets each time.
    subprocess.run(
        [str(dbgen_bin), "-s", str(scale_factor), "-U", str(n_sets), "-f"],
        cwd=DBGEN_DIR,
        check=True,
        stdout=subprocess.DEVNULL,
    )

    try:
        for n in range(1, n_sets + 1):
            tbl_files = [
                (DBGEN_DIR / f"orders.tbl.u{n}",  data_dir / f"orders_u{n}.parquet",    _ORDERS_COLS),
                (DBGEN_DIR / f"lineitem.tbl.u{n}", data_dir / f"lineitem_u{n}.parquet",  _LINEITEM_COLS),
                (DBGEN_DIR / f"delete.{n}",        data_dir / f"delete_set_{n}.parquet", _DELETE_COLS),
            ]
            for tbl_file, out, col_types in tbl_files:
                _convert_tbl_to_parquet(tbl_file=tbl_file, out=out, col_types=col_types)
            print(f"  set {n}: orders_u{n}.parquet, lineitem_u{n}.parquet, delete_set_{n}.parquet")
    finally:
        for n in range(1, n_sets + 1):
            for pattern in [f"orders.tbl.u{n}", f"lineitem.tbl.u{n}", f"delete.{n}"]:
                (DBGEN_DIR / pattern).unlink(missing_ok=True)

    print("Done.")


def _convert_tbl_to_parquet(tbl_file: Path, out: Path, col_types: dict[str, str]) -> None:
    """
    Convert a pipe-delimited TBL file (with trailing |) to Parquet.
    col_types maps column name → DuckDB type; the trailing empty column is dropped automatically.
    """
    all_cols = {**col_types, "_trailing": "VARCHAR"}
    columns_sql = ", ".join(f"'{c}': '{t}'" for c, t in all_cols.items())
    select_cols = ", ".join(col_types.keys())

    with duckdb.connect() as conn:
        conn.execute(f"""
            COPY (
                SELECT {select_cols}
                FROM read_csv(
                    '{tbl_file}',
                    sep='|',
                    header=false,
                    columns={{{columns_sql}}}
                )
            ) TO '{out}' (FORMAT PARQUET);
        """)


def _compile_dbgen() -> None:
    dbgen_bin = DBGEN_DIR / "dbgen"
    if dbgen_bin.exists():
        return

    if not DBGEN_DIR.exists():
        print(
            f"error: dbgen source not found at {DBGEN_DIR}.\n"
            "Make sure the submodule is initialised:\n"
            "  git submodule update --init",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"Compiling dbgen in {DBGEN_DIR}...")
    # config.h doesn't define MACOSX — LINUX works on macOS (same gcc/clang, same long long int)
    machine = "LINUX"

    # Patch MACHINE in-place for the duration of the build, restore afterwards
    makefile = DBGEN_DIR / "Makefile"
    original = makefile.read_text()
    patched = "\n".join(
        f"MACHINE = {machine}" if line.startswith("MACHINE") else line
        for line in original.splitlines()
    )
    makefile.write_text(patched)
    try:
        subprocess.run(["make", "-C", str(DBGEN_DIR)], check=True)
    finally:
        makefile.write_text(original)

    if not dbgen_bin.exists():
        print("error: dbgen compilation succeeded but binary not found.", file=sys.stderr)
        sys.exit(1)
    print("dbgen compiled successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", type=int, default=1, help="TPC-H scale factor")
    parser.add_argument("--data-dir", type=Path, default=Path("data"))
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Also generate RF1/RF2 refresh parquet files via dbgen",
    )
    parser.add_argument(
        "--refresh-sets", type=int, default=None,
        help=(
            "Number of refresh sets to generate (default: 1 + update_streams, "
            "where update_streams = max(1, round(0.1 * sf)))"
        ),
    )
    args = parser.parse_args()
    generate(scale_factor=args.sf, data_dir=args.data_dir)
    if args.refresh:
        n_sets = args.refresh_sets or (1 + max(1, round(0.1 * args.sf)))
        generate_refresh_data(scale_factor=args.sf, data_dir=args.data_dir, n_sets=n_sets)
