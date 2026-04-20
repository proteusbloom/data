"""
Point-in-polygon spatial join using Polars + DuckDB spatial.

Given a CSV with lon/lat coordinates stored as a single "lon,lat" string, matches
each point against polygons from a second file and writes the matched key value
to a new output CSV.

Usage:
    python spatial_join.py <input_csv> <polygon_file> <output_csv>
        --coord-col COL --key-col COL [options]

Examples:
    # Shapefile
    python spatial_join.py points.csv neighborhoods.shp out.csv --coord-col coords --key-col NTA_CODE

    # GeoParquet
    python spatial_join.py points.csv zones.parquet out.csv --coord-col coords --geom-col geometry --key-col zone_id

    # GeoJSON, inner join only
    python spatial_join.py points.csv districts.geojson out.csv --coord-col loc --key-col DIST_NAME --inner

    # CSV with WKT geometry
    python spatial_join.py points.csv polys.csv out.csv --coord-col loc --geom-col wkt_geom --key-col region_id

    # CSV with GeoJSON geometry strings
    python spatial_join.py points.csv polys.csv out.csv --coord-col loc --geom-col geojson_col --key-col region_id --csv-geom-format geojson

Notes:
    - All coordinates and polygons are assumed to be in WGS84 (EPSG:4326). No reprojection is performed.
    - If a point falls inside more than one polygon (overlapping polygons), multiple output
      rows will be produced for that input row.
    - With LEFT JOIN (default), unmatched rows appear in output with a null key value.
      With --inner, unmatched rows are dropped.
"""

import argparse
import pathlib
import sys

import duckdb
import polars as pl

sys.stdout.reconfigure(encoding="utf-8")

SUPPORTED_EXTENSIONS = {".parquet", ".shp", ".geojson", ".json", ".csv"}


def _polygon_source_sql(
    polygon_file: str,
    geom_col: str,
    key_col: str,
    csv_geom_format: str,
) -> str:
    """Return a SQL fragment that produces (key_val, poly_geom) from the polygon file."""
    ext = pathlib.Path(polygon_file).suffix.lower()
    # Use forward slashes for DuckDB path compatibility on Windows
    path = polygon_file.replace("\\", "/")

    if ext in (".shp", ".geojson", ".json"):
        # ST_Read exposes geometry as 'geom' regardless of source column name
        return f"""
            SELECT "{key_col}" AS key_val, geom AS poly_geom
            FROM ST_Read('{path}')
        """
    elif ext == ".parquet":
        return f"""
            SELECT "{key_col}" AS key_val, "{geom_col}" AS poly_geom
            FROM read_parquet('{path}')
        """
    elif ext == ".csv":
        geom_fn = (
            "ST_GeomFromGeoJSON" if csv_geom_format == "geojson" else "ST_GeomFromText"
        )
        return f"""
            SELECT "{key_col}" AS key_val,
                   {geom_fn}("{geom_col}") AS poly_geom
            FROM read_csv_auto('{path}')
            WHERE "{geom_col}" IS NOT NULL
        """
    else:
        sys.exit(f"Error: Unsupported polygon file format '{ext}'. Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}")


def _build_join_sql(
    coord_col: str,
    coord_sep: str,
    polygon_source_sql: str,
    key_col: str,
    join_type: str,
) -> str:
    """Return the full spatial join SQL query."""
    return f"""
        WITH pts AS (
            SELECT *,
                TRY_CAST(string_split("{coord_col}", '{coord_sep}')[1] AS DOUBLE) AS _lon,
                TRY_CAST(string_split("{coord_col}", '{coord_sep}')[2] AS DOUBLE) AS _lat
            FROM points
        ),
        polys AS (
            {polygon_source_sql}
        ),
        joined AS (
            SELECT pts.* EXCLUDE (_lon, _lat),
                   polys.key_val AS "{key_col}"
            FROM pts
            {join_type} JOIN polys
                ON ST_Within(ST_Point(pts._lon, pts._lat), polys.poly_geom)
        )
        SELECT * FROM joined
    """


def main():
    parser = argparse.ArgumentParser(
        description="Point-in-polygon spatial join using Polars + DuckDB spatial.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage:")[1],
    )
    parser.add_argument("input_csv", help="Input CSV file containing coordinate column")
    parser.add_argument(
        "polygon_file",
        help="Polygon file (.parquet, .shp, .geojson, .json, or .csv)",
    )
    parser.add_argument("output_csv", help="Output CSV file path")
    parser.add_argument(
        "--coord-col",
        required=True,
        metavar="COL",
        help="Column in input CSV containing 'lon,lat' string",
    )
    parser.add_argument(
        "--key-col",
        required=True,
        metavar="COL",
        help="Column in polygon file to return on match",
    )
    parser.add_argument(
        "--geom-col",
        default="geometry",
        metavar="COL",
        help="Geometry column name in polygon file (default: geometry). "
             "Ignored for .shp/.geojson/.json — ST_Read handles geometry internally.",
    )
    parser.add_argument(
        "--coord-sep",
        default=",",
        metavar="SEP",
        help="Separator used in the coordinate string (default: ',')",
    )
    parser.add_argument(
        "--inner",
        action="store_true",
        help="Use INNER JOIN instead of LEFT JOIN (drops unmatched rows)",
    )
    parser.add_argument(
        "--csv-geom-format",
        choices=["wkt", "geojson"],
        default="wkt",
        help="Geometry format when polygon-file is a CSV (default: wkt)",
    )
    args = parser.parse_args()

    # --- Validate input files ---
    input_path = pathlib.Path(args.input_csv)
    polygon_path = pathlib.Path(args.polygon_file)

    if not input_path.exists():
        sys.exit(f"Error: Input CSV not found: {args.input_csv}")
    if not polygon_path.exists():
        sys.exit(f"Error: Polygon file not found: {args.polygon_file}")

    ext = polygon_path.suffix.lower()
    if ext not in SUPPORTED_EXTENSIONS:
        sys.exit(
            f"Error: Unsupported polygon file format '{ext}'. "
            f"Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )

    # Warn if --geom-col was explicitly set for a format that ignores it
    if ext in (".shp", ".geojson", ".json") and args.geom_col != "geometry":
        print(
            f"Warning: --geom-col '{args.geom_col}' is ignored for {ext} files "
            f"(ST_Read handles geometry internally).",
            file=sys.stderr,
        )

    # --- Load input CSV ---
    df = pl.read_csv(args.input_csv, infer_schema_length=0)

    if args.coord_col not in df.columns:
        sys.exit(
            f"Error: Coordinate column '{args.coord_col}' not found in {args.input_csv}.\n"
            f"       Available columns: {df.columns}"
        )

    # Warn if key_col already exists in input (will be overwritten by join)
    if args.key_col in df.columns:
        print(
            f"Warning: Column '{args.key_col}' already exists in the input CSV and will be overwritten.",
            file=sys.stderr,
        )

    # --- Init DuckDB with spatial extension ---
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial; LOAD spatial;")
    except Exception as exc:
        sys.exit(f"Error: Failed to load DuckDB spatial extension: {exc}")

    # Register polars DataFrame as a DuckDB relation
    con.register("points", df)

    # --- Build and execute SQL ---
    poly_sql = _polygon_source_sql(
        args.polygon_file, args.geom_col, args.key_col, args.csv_geom_format
    )
    join_type = "INNER" if args.inner else "LEFT"
    sql = _build_join_sql(args.coord_col, args.coord_sep, poly_sql, args.key_col, join_type)

    try:
        result_df = con.execute(sql).pl()
    except Exception as exc:
        sys.exit(f"Error: Spatial join failed: {exc}")
    finally:
        con.close()

    # --- Write output ---
    result_df.write_csv(args.output_csv)

    matched = result_df[args.key_col].drop_nulls().len()
    total = len(result_df)
    print(
        f"Rows: {total} | Matched: {matched} | Unmatched: {total - matched}",
        file=sys.stderr,
    )
    print(f"Output written to: {args.output_csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
