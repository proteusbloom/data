"""
Quick WKT geometry sanity check mapper.
Usage: python map_wkt.py your_file.csv
       python map_wkt.py your_file.csv --col geometry   # specify WKT column name

Dependencies (preferred): geopandas, shapely, matplotlib, pandas
Fallback (no install needed): matplotlib, pandas  — handles common WKT types via built-in parser
"""

import sys
import re
import argparse
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Polygon as MplPolygon
from matplotlib.collections import PatchCollection, LineCollection
import numpy as np

# ── Try full stack first ──────────────────────────────────────────────────────
try:
    import geopandas as gpd
    from shapely import wkt
    FULL_STACK = True
except ImportError:
    FULL_STACK = False
    print("ℹ️  geopandas/shapely not found — using built-in lightweight parser.")


# ── Lightweight WKT parser (no deps beyond stdlib) ───────────────────────────

def _extract_numbers(s):
    return list(map(float, re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)))

def _ring_coords(ring_str):
    nums = _extract_numbers(ring_str)
    return [(nums[i], nums[i+1]) for i in range(0, len(nums)-1, 2)]

def _parse_wkt_simple(wkt_str):
    """Returns list of (geom_type, list_of_coord_rings)."""
    s = wkt_str.strip().upper()
    results = []

    if s.startswith("POINT"):
        nums = _extract_numbers(wkt_str)
        results.append(("POINT", [(nums[0], nums[1])]))

    elif s.startswith("MULTIPOINT"):
        inner = re.findall(r"\(([^()]+)\)", wkt_str)
        pts = [_ring_coords(i) for i in inner]
        results.append(("MULTIPOINT", [p[0] for p in pts]))

    elif s.startswith("LINESTRING"):
        inner = re.search(r"\(([^()]+)\)", wkt_str)
        if inner:
            results.append(("LINESTRING", [_ring_coords(inner.group(1))]))

    elif s.startswith("MULTILINESTRING"):
        rings = re.findall(r"\(([^()]+)\)", wkt_str)
        results.append(("MULTILINESTRING", [_ring_coords(r) for r in rings]))

    elif s.startswith("MULTIPOLYGON"):
        # flatten all rings
        rings = re.findall(r"\(([^()]+)\)", wkt_str)
        results.append(("MULTIPOLYGON", [_ring_coords(r) for r in rings]))

    elif s.startswith("POLYGON"):
        rings = re.findall(r"\(([^()]+)\)", wkt_str)
        results.append(("POLYGON", [_ring_coords(r) for r in rings]))

    return results


def _plot_lightweight(df, wkt_col, title):
    fig, ax = plt.subplots(figsize=(10, 8))
    ax.set_facecolor("#f0f4f8")
    fig.patch.set_facecolor("#ffffff")

    type_counts = {}
    all_xs, all_ys = [], []

    for _, row in df.iterrows():
        raw = str(row[wkt_col])
        if pd.isna(row[wkt_col]) or raw.strip() == "":
            continue
        geoms = _parse_wkt_simple(raw)
        for geom_type, coords_list in geoms:
            type_counts[geom_type] = type_counts.get(geom_type, 0) + 1

            if geom_type == "POINT":
                x, y = coords_list[0]
                ax.plot(x, y, "o", color="steelblue", markersize=5, alpha=0.8)
                all_xs.append(x); all_ys.append(y)

            elif geom_type == "MULTIPOINT":
                for x, y in coords_list:
                    ax.plot(x, y, "o", color="steelblue", markersize=5, alpha=0.8)
                    all_xs.append(x); all_ys.append(y)

            elif geom_type in ("LINESTRING", "MULTILINESTRING"):
                for ring in coords_list:
                    xs = [p[0] for p in ring]; ys = [p[1] for p in ring]
                    ax.plot(xs, ys, "-", color="darkorange", linewidth=1.5, alpha=0.8)
                    all_xs += xs; all_ys += ys

            elif geom_type in ("POLYGON", "MULTIPOLYGON"):
                for ring in coords_list:
                    xs = [p[0] for p in ring]; ys = [p[1] for p in ring]
                    patch = MplPolygon(list(zip(xs, ys)), closed=True,
                                       facecolor="steelblue", edgecolor="white",
                                       linewidth=0.8, alpha=0.6)
                    ax.add_patch(patch)
                    all_xs += xs; all_ys += ys

    # Auto-scale with a small margin
    if all_xs and all_ys:
        xpad = (max(all_xs) - min(all_xs)) * 0.15 or 0.001
        ypad = (max(all_ys) - min(all_ys)) * 0.15 or 0.001
        ax.set_xlim(min(all_xs) - xpad, max(all_xs) + xpad)
        ax.set_ylim(min(all_ys) - ypad, max(all_ys) + ypad)

    total = sum(type_counts.values())
    type_str = " | ".join(f"{k}: {v}" for k, v in type_counts.items())
    print(f"📦  {total} features loaded | {type_str}")

    ax.set_title(f"{title}\n{total} features — {type_str}", fontsize=12)
    ax.set_xlabel("X / Longitude")
    ax.set_ylabel("Y / Latitude")
    ax.set_aspect("equal")
    plt.tight_layout()
    return fig


# ── Main ──────────────────────────────────────────────────────────────────────

def map_wkt(csv_path: str, wkt_col: str | None = None, save_png: str | None = None):
    df = pd.read_csv(csv_path)

    # Auto-detect WKT column
    if wkt_col is None:
        candidates = [
            c for c in df.columns
            if df[c].astype(str).str.match(
                r"^\s*(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)",
                case=False, na=False
            ).any()
        ]
        if not candidates:
            sys.exit("❌  No WKT column detected. Use --col <column_name> to specify one.")
        wkt_col = candidates[0]
        print(f"✅  Auto-detected WKT column: '{wkt_col}'")

    if FULL_STACK:
        df["geometry"] = df[wkt_col].apply(lambda v: wkt.loads(str(v)) if pd.notna(v) else None)
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
        gdf = gdf[gdf.geometry.notna()]
        print(f"📦  {len(gdf)} features loaded | types: {gdf.geom_type.value_counts().to_dict()}")
        fig, ax = plt.subplots(figsize=(10, 8))
        gdf.plot(ax=ax, color="steelblue", edgecolor="white", linewidth=0.5, alpha=0.7)
        ax.set_title(f"WKT Geometry — {csv_path}  ({len(gdf)} features)", fontsize=13)
        ax.set_xlabel("X / Longitude"); ax.set_ylabel("Y / Latitude")
        ax.set_aspect("equal")
        plt.tight_layout()
        fig_ = fig
    else:
        fig_ = _plot_lightweight(df, wkt_col, title=csv_path)

    if save_png:
        fig_.savefig(save_png, dpi=150, bbox_inches="tight")
        print(f"💾  Saved to {save_png}")
    else:
        plt.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Quick WKT sanity-check map from a CSV.")
    parser.add_argument("csv", help="Path to the CSV file")
    parser.add_argument("--col", default=None, help="Name of the WKT column (auto-detected if omitted)")
    parser.add_argument("--save", default=None, metavar="FILE.png", help="Save map to PNG instead of showing it")
    args = parser.parse_args()
    map_wkt(args.csv, args.col, args.save)
