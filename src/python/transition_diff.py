#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any

# Columns to exclude from comparison
EXCLUDE_COLS = {"jobid", "real_submit", "real_start", "real_finish"}

def load_csv_map(csv_path: Path, key_col: str) -> Tuple[Dict[str, Dict[str, str]], List[str]]:
    """Load a CSV into a dict keyed by `key_col`, ignoring excluded columns."""
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError(f"No header found in {csv_path}")

        # Case-insensitive mapping
        field_map = {name.lower(): name for name in reader.fieldnames}

        def orig(name: str) -> str:
            lower = name.lower()
            if lower not in field_map:
                raise KeyError(
                    f"Required column '{name}' not found in {csv_path.name}. "
                    f"Found columns: {reader.fieldnames}"
                )
            return field_map[lower]

        key_name = orig(key_col)

        # Select columns excluding key and excluded ones
        compare_cols = [
            name for name in reader.fieldnames
            if name.lower() != key_name.lower() and name.lower() not in EXCLUDE_COLS
        ]

        data = {}
        for row in reader:
            key = row[key_name]
            data[key] = {c: row.get(c, "") for c in compare_cols}

        return data, compare_cols

def try_float(val: str):
    try:
        return True, float(val)
    except Exception:
        return False, 0.0

def equal_with_tol(a: str, b: str, eps: float) -> bool:
    if a == b:
        return True
    af, fa = try_float(a)
    bf, fb = try_float(b)
    if af and bf:
        return abs(fa - fb) <= eps
    return False

def diff_csvs(path_a: Path, path_b: Path, key_col: str, float_eps: float) -> int:
    try:
        map_a, cols_a = load_csv_map(path_a, key_col)
        map_b, cols_b = load_csv_map(path_b, key_col)
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 2

    compare_cols = sorted(set(cols_a) & set(cols_b))
    keys_a = set(map_a.keys())
    keys_b = set(map_b.keys())

    only_in_a = sorted(keys_a - keys_b)
    only_in_b = sorted(keys_b - keys_a)
    common = sorted(keys_a & keys_b)

    differences_found = False

    if only_in_a:
        differences_found = True
        print("=== Keys only in A ===")
        for k in only_in_a:
            print(f"  {k}")
        print()

    if only_in_b:
        differences_found = True
        print("=== Keys only in B ===")
        for k in only_in_b:
            print(f"  {k}")
        print()

    for k in common:
        row_a = map_a[k]
        row_b = map_b[k]
        diffs = []
        for c in compare_cols:
            va = row_a.get(c, "")
            vb = row_b.get(c, "")
            if not equal_with_tol(va, vb, float_eps):
                diffs.append((c, va, vb))
        if diffs:
            differences_found = True
            print(f"--- Key: {k}")
            for c, va, vb in diffs:
                print(f"  {c}:")
                print(f"    A: {va}")
                print(f"    B: {vb}")
            print()

    if not differences_found:
        print("No differences found (ignoring jobid and real-time columns).")
        return 0
    else:
        print("Differences detected.")
        return 1

def main():
    ap = argparse.ArgumentParser(description="Compare two CSVs, ignoring jobid and real-time columns.")
    ap.add_argument("csv_a", help="First CSV file")
    ap.add_argument("csv_b", help="Second CSV file")
    ap.add_argument("--key-col", default="trace_idx", help="Column to align rows (default: trace_idx)")
    ap.add_argument("--float-eps", type=float, default=0.0, help="Tolerance for numeric comparison")
    args = ap.parse_args()

    sys.exit(diff_csvs(Path(args.csv_a), Path(args.csv_b), args.key_col, args.float_eps))

if __name__ == "__main__":
    main()
