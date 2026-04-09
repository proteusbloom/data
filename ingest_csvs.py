"""
csv_ingest.py
─────────────────────────────────────────────────────────────────────────────
Reads every CSV in a target folder into a single DuckDB table.

• Column names are taken from the FIRST CSV encountered; all subsequent CSVs
  must share the same schema (extra / missing columns raise an error).
• A synthetic composite key is built from up to four user-defined columns.
  Duplicate keys abort the entire import for that CSV.
• No row is committed unless the whole CSV is clean.

USER CONFIGURATION  ↓  edit the block below, nothing else needs to change.
─────────────────────────────────────────────────────────────────────────────
"""

import duckdb
import pathlib
import sys

# ── USER CONFIG ───────────────────────────────────────────────────────────────

# Folder that contains your CSV files
CSV_FOLDER = "./data"

# DuckDB database file (use ":memory:" for an in-process-only database)
DB_FILE = "ingested.duckdb"

# Name of the target table
TABLE_NAME = "unified_data"

# ── SYNTHETIC KEY DEFINITION ─────────────────────────────────────────────────
# Choose 1 to 4 field names from your CSV headers.
# The synthetic key for every row is built by reading the actual VALUES of those
# fields and concatenating them with KEY_SEPARATOR.
#
# Example: if a row has  first_name="Jane", city="Oslo", year="2024"
#   and KEY_COLUMNS = ["first_name", "city", "year"]  with KEY_SEPARATOR = "|"
#   → synthetic key = "Jane|Oslo|2024"
#
# Any combination that is not unique across all loaded rows will abort the import.
# Change these names to any column headers that exist in your CSV files.
KEY_COLUMNS: list[str] = [
    "first_name",   # ← pick real column names from your CSV
    "last_name",
    # "city",       # uncomment to add a third key component
    # "birth_year", # uncomment to add a fourth key component
]

# String placed between field values when building the key
KEY_SEPARATOR = "|"

# Name of the synthetic key column stored in the table
SYNTHETIC_KEY_COLUMN = "synthetic_key"

# ── END USER CONFIG ───────────────────────────────────────────────────────────


def build_key_expression(columns: list[str], separator: str) -> str:
    """Return a SQL expression that concatenates columns into one key string."""
    if not columns:
        raise ValueError("KEY_COLUMNS must contain at least one column name.")
    if len(columns) > 4:
        raise ValueError("KEY_COLUMNS must contain at most four column names.")
    parts = [f"CAST({col} AS VARCHAR)" for col in columns]
    sep = f" || '{separator}' || "
    return sep.join(parts)


def ensure_table(con: duckdb.DuckDBPyConnection, columns: list[str]) -> None:
    """Create the target table if it does not yet exist."""
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in columns)
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            "{SYNTHETIC_KEY_COLUMN}" VARCHAR PRIMARY KEY,
            {col_defs}
        )
        """
    )


def get_existing_keys(con: duckdb.DuckDBPyConnection) -> set[str]:
    rows = con.execute(
        f'SELECT "{SYNTHETIC_KEY_COLUMN}" FROM {TABLE_NAME}'
    ).fetchall()
    return {r[0] for r in rows}


def ingest_csv(
    con: duckdb.DuckDBPyConnection,
    csv_path: pathlib.Path,
    table_columns: list[str],
    key_expr: str,
) -> int:
    """
    Load one CSV into the table.  Returns the number of rows inserted.
    Raises RuntimeError on schema mismatch or duplicate keys.
    """
    # ── 1. read the CSV into a temp view ──────────────────────────────────────
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW staging AS SELECT * FROM read_csv_auto('{csv_path}')"
    )

    # ── 2. validate columns ───────────────────────────────────────────────────
    incoming_cols = [
        row[0]
        for row in con.execute(
            "SELECT column_name FROM (DESCRIBE staging)"
        ).fetchall()
    ]

    missing = set(table_columns) - set(incoming_cols)
    if missing:
        raise RuntimeError(
            f"[{csv_path.name}] Missing columns required by table: {missing}"
        )

    extra = set(incoming_cols) - set(table_columns)
    if extra:
        print(
            f"  ⚠  [{csv_path.name}] Extra columns ignored: {extra}"
        )

    # ── 3. compute synthetic keys for every incoming row ──────────────────────
    col_select = ", ".join(f'"{c}"' for c in table_columns)
    incoming_rows = con.execute(
        f"""
        SELECT {key_expr} AS __key__, {col_select}
        FROM staging
        """
    ).fetchall()

    if not incoming_rows:
        print(f"  ℹ  [{csv_path.name}] No rows found – skipping.")
        return 0

    # ── 4. duplicate check WITHIN this CSV ────────────────────────────────────
    incoming_keys = [row[0] for row in incoming_rows]
    if len(incoming_keys) != len(set(incoming_keys)):
        dupes = {k for k in incoming_keys if incoming_keys.count(k) > 1}
        raise RuntimeError(
            f"[{csv_path.name}] Duplicate keys found WITHIN the file – "
            f"import aborted.\n  Duplicates: {dupes}"
        )

    # ── 5. duplicate check AGAINST existing table rows ────────────────────────
    existing_keys = get_existing_keys(con)
    clashes = set(incoming_keys) & existing_keys
    if clashes:
        raise RuntimeError(
            f"[{csv_path.name}] Keys already present in table – "
            f"import aborted.\n  Clashing keys: {clashes}"
        )

    # ── 6. insert (all-or-nothing via explicit transaction) ───────────────────
    col_placeholders = ", ".join(["?"] * (len(table_columns) + 1))  # +1 for key
    insert_sql = (
        f'INSERT INTO {TABLE_NAME} '
        f'("{SYNTHETIC_KEY_COLUMN}", {col_select}) '
        f'VALUES ({col_placeholders})'
    )

    con.begin()
    try:
        con.executemany(insert_sql, incoming_rows)
        con.commit()
    except Exception:
        con.rollback()
        raise

    return len(incoming_rows)


def main() -> None:
    folder = pathlib.Path(CSV_FOLDER)
    if not folder.is_dir():
        sys.exit(f"ERROR: CSV_FOLDER '{folder}' does not exist or is not a directory.")

    csv_files = sorted(folder.glob("*.csv"))
    if not csv_files:
        sys.exit(f"ERROR: No CSV files found in '{folder}'.")

    print(f"\n📂  Found {len(csv_files)} CSV file(s) in '{folder}'")

    con = duckdb.connect(DB_FILE)

    # Derive canonical column list from the FIRST CSV
    first_csv = csv_files[0]
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW _probe AS SELECT * FROM read_csv_auto('{first_csv}')"
    )
    table_columns = [
        row[0]
        for row in con.execute("SELECT column_name FROM (DESCRIBE _probe)").fetchall()
    ]
    print(f"📋  Table columns  : {table_columns}")

    # Validate that KEY_COLUMNS exist in the schema
    missing_key_cols = set(KEY_COLUMNS) - set(table_columns)
    if missing_key_cols:
        sys.exit(
            f"ERROR: KEY_COLUMNS references columns not found in the CSV: {missing_key_cols}\n"
            f"       Available columns: {table_columns}"
        )

    key_expr = build_key_expression(KEY_COLUMNS, KEY_SEPARATOR)
    print(f"🔑  Synthetic key  : {KEY_COLUMNS}  (separator='{KEY_SEPARATOR}')\n")

    ensure_table(con, table_columns)

    total_inserted = 0
    for csv_path in csv_files:
        print(f"  ⏳  Ingesting  {csv_path.name} …", end=" ")
        try:
            n = ingest_csv(con, csv_path, table_columns, key_expr)
            print(f"✅  {n} row(s) inserted.")
            total_inserted += n
        except RuntimeError as exc:
            print(f"\n  ❌  {exc}\n")

    # ── summary ───────────────────────────────────────────────────────────────
    total_in_table = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
    print(
        f"\n{'─'*60}\n"
        f"  Rows inserted this run : {total_inserted}\n"
        f"  Total rows in table    : {total_in_table}\n"
        f"  Database               : {DB_FILE}\n"
        f"  Table                  : {TABLE_NAME}\n"
        f"{'─'*60}\n"
    )

    # Optional: preview first 10 rows
    print("Preview (first 10 rows):")
    print(con.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 10").df().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
