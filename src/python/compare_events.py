#!/usr/bin/env python3
"""
Compare whether COMPLETE events appear in the same order (by trace_idx)
at each simulated time across two event logs.

Usage:
  python compare_event_orders.py run1.csv run2.csv \
      [--time-col time] [--kind-col kind] [--trace-col trace_idx] \
      [--filter-kind complete] [--round 6]

Exit code is 0 if all comparable buckets match order, 1 otherwise.
"""

import csv
import argparse
from collections import defaultdict, Counter
from math import isfinite

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("run1")
    ap.add_argument("run2")
    ap.add_argument("--time-col", default="time", help="column with simulated time")
    ap.add_argument("--kind-col", default="kind", help="column with event kind")
    ap.add_argument("--trace-col", default="trace_idx", help="column with trace index")
    ap.add_argument("--filter-kind", default="complete",
                    help="only compare rows where kind == this (default: complete)")
    ap.add_argument("--round", type=int, default=6,
                    help="decimal places to round time buckets (default: 6)")
    return ap.parse_args()

def norm_time(s, ndp):
    try:
        t = float(s)
    except Exception:
        return s  
    if not isfinite(t):
        return s
    return f"{round(t, ndp):.{ndp}f}"

def load_sequence_by_time(path, time_col, kind_col, trace_col, filter_kind, ndp):
    seq = defaultdict(list)  
    with open(path, newline="") as f:
        r = csv.DictReader(f)
        missing = [c for c in (time_col, kind_col, trace_col) if c not in r.fieldnames]
        if missing:
            raise SystemExit(f"{path}: missing column(s): {', '.join(missing)}")
        for row in r:
            if filter_kind and row.get(kind_col) != filter_kind:
                continue
            tr = row.get(trace_col)
            if tr in (None, "", "None"):
                continue
            key = norm_time(row.get(time_col, ""), ndp)
            seq[key].append(str(tr))
    return seq

def compare(seq1, seq2):
    all_times = sorted(set(seq1.keys()) | set(seq2.keys()),
                       key=lambda x: (len(x), x))  
    diffs = []
    only1 = []
    only2 = []
    sets_diff = []

    for t in all_times:
        a = seq1.get(t)
        b = seq2.get(t)
        if a is None:
            only2.append(t)
            continue
        if b is None:
            only1.append(t)
            continue

        if Counter(a) != Counter(b):
            sets_diff.append((t, a, b))
            continue

        if a != b:
            diffs.append((t, a, b))

    return diffs, sets_diff, only1, only2

def main():
    args = parse_args()
    s1 = load_sequence_by_time(args.run1, args.time_col, args.kind_col,
                               args.trace_col, args.filter_kind, args.round)
    s2 = load_sequence_by_time(args.run2, args.time_col, args.kind_col,
                               args.trace_col, args.filter_kind, args.round)

    diffs, sets_diff, only1, only2 = compare(s1, s2)

    print("=== Comparison of COMPLETE event order by time bucket ===")
    print(f"Run1: {args.run1}")
    print(f"Run2: {args.run2}")
    print()

    if only1:
        print(f"Buckets only in run1 ({len(only1)}):")
        for t in only1[:20]:
            print(f"  time={t}  (run1 has {len(s1[t])} completes)")
        if len(only1) > 20: print("  ...")
        print()

    if only2:
        print(f"Buckets only in run2 ({len(only2)}):")
        for t in only2[:20]:
            print(f"  time={t}  (run2 has {len(s2[t])} completes)")
        if len(only2) > 20: print("  ...")
        print()

    if sets_diff:
        print(f"Buckets with DIFFERENT sets of trace_idx ({len(sets_diff)}):")
        for t, a, b in sets_diff[:20]:
            print(f"  time={t}")
            print(f"    run1 set: {Counter(a)}")
            print(f"    run2 set: {Counter(b)}")
        if len(sets_diff) > 20: print("  ...")
        print()

    if diffs:
        print(f"Buckets with same items but DIFFERENT ORDER ({len(diffs)}):")
        for t, a, b in diffs[:50]:
            print(f"  time={t}")
            print(f"    run1 order: {a}")
            print(f"    run2 order: {b}")
        if len(diffs) > 50: print("  ...")
        print()
    else:
        print("No order differences for comparable buckets.")
        print()

    problems = len(only1) + len(only2) + len(sets_diff) + len(diffs)
    print("=== Summary ===")
    print(f"Comparable buckets: {len(set(s1.keys()) & set(s2.keys()))}")
    print(f"Only in run1: {len(only1)}")
    print(f"Only in run2: {len(only2)}")
    print(f"Set differences: {len(sets_diff)}")
    print(f"Order differences: {len(diffs)}")

    exit(1 if problems else 0)

if __name__ == "__main__":
    main()
