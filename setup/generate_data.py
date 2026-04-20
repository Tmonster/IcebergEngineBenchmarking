"""
Generate TPC-H data using DuckDB's built-in tpch extension.
Refresh data (update/delete sets) is generated via the dbgen binary in the submodule.

Usage:
    python -m setup.generate_data --sf 10
    python -m setup.generate_data --sf 10 --refresh   # also generate RF parquet files
    python -m setup.generate_data --sf 1 --data-dir /tmp/tpch

    # Large scale factors that exhaust memory with the in-memory path:
    python -m setup.generate_data --sf 300 --chunks 10
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

TPCH_TABLES = [
    "customer", "lineitem", "nation", "orders",
    "part", "partsupp", "region", "supplier",
]

# Tables that do NOT scale with SF — dbgen emits the same rows for every chunk.
# We only convert these from chunk 1 and skip subsequent chunks.
_FIXED_TABLES = {"nation", "region"}

DBGEN_DIR = Path("duckdb-tpch-power-test/tpch_tools_3.0.1/dbgen")

# TBL files have a trailing | delimiter, so there is always one extra empty column.
# Column definitions map name → DuckDB type to match the base table schemas exactly.
_TABLE_COLS: dict[str, dict[str, str]] = {
    "customer": {
        "c_custkey": "BIGINT",
        "c_name": "VARCHAR",
        "c_address": "VARCHAR",
        "c_nationkey": "INTEGER",
        "c_phone": "VARCHAR",
        "c_acctbal": "DECIMAL(15,2)",
        "c_mktsegment": "VARCHAR",
        "c_comment": "VARCHAR",
    },
    "lineitem": {
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
    },
    "nation": {
        "n_nationkey": "INTEGER",
        "n_name": "VARCHAR",
        "n_regionkey": "INTEGER",
        "n_comment": "VARCHAR",
    },
    "orders": {
        "o_orderkey": "BIGINT",
        "o_custkey": "BIGINT",
        "o_orderstatus": "VARCHAR",
        "o_totalprice": "DECIMAL(15,2)",
        "o_orderdate": "DATE",
        "o_orderpriority": "VARCHAR",
        "o_clerk": "VARCHAR",
        "o_shippriority": "INTEGER",
        "o_comment": "VARCHAR",
    },
    "part": {
        "p_partkey": "BIGINT",
        "p_name": "VARCHAR",
        "p_mfgr": "VARCHAR",
        "p_brand": "VARCHAR",
        "p_type": "VARCHAR",
        "p_size": "INTEGER",
        "p_container": "VARCHAR",
        "p_retailprice": "DECIMAL(15,2)",
        "p_comment": "VARCHAR",
    },
    "partsupp": {
        "ps_partkey": "BIGINT",
        "ps_suppkey": "BIGINT",
        "ps_availqty": "INTEGER",
        "ps_supplycost": "DECIMAL(15,2)",
        "ps_comment": "VARCHAR",
    },
    "region": {
        "r_regionkey": "INTEGER",
        "r_name": "VARCHAR",
        "r_comment": "VARCHAR",
    },
    "supplier": {
        "s_suppkey": "BIGINT",
        "s_name": "VARCHAR",
        "s_address": "VARCHAR",
        "s_nationkey": "INTEGER",
        "s_phone": "VARCHAR",
        "s_acctbal": "DECIMAL(15,2)",
        "s_comment": "VARCHAR",
    },
}

# delete.N files: one o_orderkey per line with trailing |
_DELETE_COLS: dict[str, str] = {
    "o_orderkey": "BIGINT",
}


def generate(scale_factor: int, data_dir: Path, n_chunks: int = 1) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    if n_chunks > 1:
        _generate_chunked(scale_factor, data_dir, n_chunks)
    else:
        _generate_in_memory(scale_factor, data_dir)


def _generate_in_memory(scale_factor: int, data_dir: Path) -> None:
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


def _generate_chunked(scale_factor: int, data_dir: Path, n_chunks: int) -> None:
    """
    Generate base tables in n_chunks sequential passes using the dbgen binary.
    Each pass produces one slice of the scalable tables. The slices are written
    as individual parquet files and then merged into a single file per table via
    DuckDB streaming COPY (no full-dataset memory spike).

    nation and region do not scale with SF — dbgen emits identical rows for every
    chunk, so they are only converted on chunk 1.
    """
    _compile_dbgen()
    dbgen_bin = (DBGEN_DIR / "dbgen").absolute()

    chunk_dir = data_dir / "_chunks"
    chunk_dir.mkdir(parents=True, exist_ok=True)

    print(f"Generating TPC-H SF={scale_factor} in {n_chunks} chunks via dbgen binary...")
    try:
        for chunk in range(1, n_chunks + 1):
            print(f"  chunk {chunk}/{n_chunks}...")
            subprocess.run(
                [str(dbgen_bin), "-s", str(scale_factor), "-C", str(n_chunks), "-S", str(chunk), "-f"],
                cwd=DBGEN_DIR,
                check=True,
                stdout=subprocess.DEVNULL,
            )
            for table in TPCH_TABLES:
                tbl_file = DBGEN_DIR / f"{table}.tbl"
                if not tbl_file.exists():
                    continue
                # nation/region are identical every chunk — only keep chunk 1
                if table in _FIXED_TABLES and chunk > 1:
                    tbl_file.unlink()
                    continue
                dest = chunk_dir / f"{table}_chunk_{chunk:04d}.parquet"
                _convert_tbl_to_parquet(tbl_file, dest, _TABLE_COLS[table])
                tbl_file.unlink()

        print("  Merging chunks into final parquet files...")
        with duckdb.connect() as conn:
            for table in TPCH_TABLES:
                chunk_files = sorted(chunk_dir.glob(f"{table}_chunk_*.parquet"))
                if not chunk_files:
                    continue
                out = data_dir / f"{table}.parquet"
                file_list = str([str(f) for f in chunk_files])
                conn.execute(
                    f"COPY (SELECT * FROM read_parquet({file_list})) TO '{out}' (FORMAT PARQUET);"
                )
                row_count = conn.execute(f"SELECT count(*) FROM read_parquet('{out}')").fetchone()[0]
                print(f"  {table}: {row_count:,} rows → {out}")
    finally:
        shutil.rmtree(chunk_dir, ignore_errors=True)

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
                (DBGEN_DIR / f"orders.tbl.u{n}",  data_dir / f"orders_u{n}.parquet",    _TABLE_COLS["orders"]),
                (DBGEN_DIR / f"lineitem.tbl.u{n}", data_dir / f"lineitem_u{n}.parquet",  _TABLE_COLS["lineitem"]),
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


def generate_query_streams(scale_factor: int, n_streams: int, data_dir: Path) -> None:
    """
    Generate per-stream SQL files using qgen.

    Produces files in data_dir/streams/:
      stream_0.sql   — power test permutation (qgen -p 0)
      stream_1.sql   — throughput stream 1    (qgen -p 1)
      ...
      stream_N.sql   — throughput stream N    (qgen -p N)

    Each file contains all 22 queries in the spec-defined permutation order
    for that stream, with parameters substituted for the given scale factor.

    Set n_streams to the number of throughput streams (use _spec_stream_count
    from benchmarks.power to get the correct value for your SF).
    """
    _compile_dbgen()

    qgen_bin = (DBGEN_DIR / "qgen").absolute()
    if not qgen_bin.exists():
        print(
            f"error: qgen binary not found at {qgen_bin}. "
            "Run `python -m setup.generate_data --refresh` first to compile dbgen.",
            file=sys.stderr,
        )
        sys.exit(1)

    streams_dir = data_dir / "streams"
    streams_dir.mkdir(parents=True, exist_ok=True)
    total = n_streams + 1  # stream 0 (power) + streams 1..n_streams (throughput)
    print(f"Generating query streams (sf={scale_factor}, streams=0..{n_streams})...")

    for stream_idx in range(total):
        out_path = streams_dir / f"stream_{stream_idx}.sql"
        result = subprocess.run(
            [str(qgen_bin), "-s", str(scale_factor), "-p", str(stream_idx)],
            cwd=DBGEN_DIR,
            check=True,
            capture_output=True,
            text=True,
        )
        # Strip the seed comment line at the top
        sql = "\n".join(
            line for line in result.stdout.splitlines()
            if not line.startswith("-- using") and not line.startswith("-- $")
        )
        out_path.write_text(sql)
        print(f"  stream_{stream_idx}.sql → {out_path}")

    print("Done.")


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
    parser.add_argument(
        "--data-dir", type=Path, default=None,
        help="Base data directory (default: data/sf=<sf>)",
    )
    parser.add_argument(
        "--chunks", type=int, default=1,
        help=(
            "Generate base tables in N sequential chunks using the dbgen binary "
            "instead of loading the full dataset into memory at once. "
            "Recommended for SF >= 100. Each chunk uses roughly (SF / N) GB of RAM. "
            "Example: --sf 300 --chunks 10"
        ),
    )
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
    parser.add_argument(
        "--query-streams",
        action="store_true",
        help=(
            "Generate per-stream SQL files via qgen (data/<sf>/streams/stream_N.sql). "
            "Each stream gets a different query permutation and parameter substitution "
            "per the TPC-H spec. Required for a spec-compliant power/throughput benchmark."
        ),
    )
    parser.add_argument(
        "--n-streams", type=int, default=None,
        help=(
            "Number of throughput streams to generate query files for "
            "(default: spec value for the given SF). Stream 0 (power test) is always included."
        ),
    )
    args = parser.parse_args()
    data_dir = args.data_dir if args.data_dir is not None else Path("data") / f"sf={args.sf}"
    generate(scale_factor=args.sf, data_dir=data_dir, n_chunks=args.chunks)
    if args.refresh:
        n_sets = args.refresh_sets or (1 + max(1, round(0.1 * args.sf)))
        generate_refresh_data(scale_factor=args.sf, data_dir=data_dir, n_sets=n_sets)
    if args.query_streams:
        from benchmarks.power import spec_stream_count
        n_streams = args.n_streams or spec_stream_count(args.sf)
        generate_query_streams(scale_factor=args.sf, n_streams=n_streams, data_dir=data_dir)
