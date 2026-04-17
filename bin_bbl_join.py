"""
Joins an input CSV with a lookup CSV on the BIN column, adding BBL values to the output.

Example usage:
    python bin_bbl_join.py input.csv lookup.csv output.csv
"""

import polars as pl
import sys

def bin_bbl_join(input_path: str, lookup_path: str, output_path: str) -> None:
    input_df = pl.read_csv(input_path, infer_schema_length=0)
    lookup_df = pl.read_csv(lookup_path, infer_schema_length=0)

    # Find BIN column (case-insensitive) in both files
    input_bin_col = next((c for c in input_df.columns if c.upper() == "BIN"), None)
    lookup_bin_col = next((c for c in lookup_df.columns if c.upper() == "BIN"), None)
    lookup_bbl_col = next((c for c in lookup_df.columns if c.upper() == "BBL"), None)

    if input_bin_col is None:
        print("Error: BIN column not found in input CSV.")
        sys.exit(1)
    if lookup_bin_col is None:
        print("Error: BIN column not found in lookup CSV.")
        sys.exit(1)
    if lookup_bbl_col is None:
        print("Error: BBL column not found in lookup CSV.")
        sys.exit(1)

    # If BBL already exists in input, skip the join entirely
    input_bbl_col = next((c for c in input_df.columns if c.upper() == "BBL"), None)
    if input_bbl_col is not None:
        print(f"BBL column ('{input_bbl_col}') already exists in input CSV. Skipping join.")
        input_df.write_csv(output_path)
        return

    # Normalize BIN to uppercase for joining, drop rows with null/empty BIN
    input_normalized = (
        input_df
        .with_columns(pl.col(input_bin_col).str.strip_chars().str.to_uppercase().alias("__BIN_KEY__"))
        .filter(pl.col("__BIN_KEY__").is_not_null() & (pl.col("__BIN_KEY__") != ""))
    )

    lookup_normalized = (
        lookup_df
        .select([lookup_bin_col, lookup_bbl_col])
        .with_columns(pl.col(lookup_bin_col).str.strip_chars().str.to_uppercase().alias("__BIN_KEY__"))
        .drop(lookup_bin_col)
        .rename({lookup_bbl_col: "BBL"})
    )

    result = (
        input_normalized
        .join(lookup_normalized, on="__BIN_KEY__", how="left")
        .drop("__BIN_KEY__")
    )

    result.write_csv(output_path)
    matched = result["BBL"].drop_nulls().len()
    print(f"Done. {matched} rows matched with a BBL value. Output written to: {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python bin_bbl_join.py <input.csv> <lookup.csv> <output.csv>")
        sys.exit(1)

    bin_bbl_join(sys.argv[1], sys.argv[2], sys.argv[3])
