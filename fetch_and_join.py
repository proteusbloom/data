"""
Join a BIN lookup CSV against a buildings reference file.

Usage:
    python duckdb_fun_tools/fetch_and_join.py <lookup_csv> <buildings_file>

    lookup_csv:     CSV with a 'bin' column (plus any other fields)
    buildings_file: CSV or GeoJSON buildings reference file
"""

import argparse
import sys
import duckdb
import polars as pl

sys.stdout.reconfigure(encoding="utf-8")


def load_buildings(path: str) -> pl.DataFrame:
    if path.endswith(".geojson") or path.endswith(".json"):
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        df = con.execute(f"SELECT * FROM ST_Read('{path}')").pl()
        con.close()
        return df
    return pl.read_csv(path, infer_schema_length=0)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("lookup_csv", help="CSV file with a 'bin' column")
    parser.add_argument("buildings_file", help="Buildings reference CSV or GeoJSON")
    args = parser.parse_args()

    lookup = pl.read_csv(args.lookup_csv, infer_schema_length=0)
    buildings = load_buildings(args.buildings_file)

    if "bin" in buildings.columns:
        buildings = buildings.with_columns(pl.col("bin").cast(pl.Utf8))

    joined = lookup.join(buildings, on="bin", how="left")

    matched = joined.filter(pl.col("bin").is_not_null() & pl.all_horizontal(
        pl.col(c).is_not_null() for c in buildings.columns if c != "bin"
    )).height
    print(f"Rows: {len(joined)} | Matched: {matched} | Unmatched: {len(joined) - matched}\n")
    print(joined)


if __name__ == "__main__":
    main()
