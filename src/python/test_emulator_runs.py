#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import os
import shlex
import subprocess
import sys
from pathlib import Path
from datetime import datetime

try:
    from tqdm import tqdm
    def progress_iter(iterable, **kwargs):
        return tqdm(iterable, **kwargs)
except Exception:
    def progress_iter(iterable, **kwargs):
        return iterable

def read_subset(csv_path, key_col="trace_idx", cols=("SUBMIT","START","FINISH")):
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        field_map = {name.lower(): name for name in (reader.fieldnames or [])}
        def get_col(name):
            lower = name.lower()
            if lower not in field_map:
                raise KeyError(f"Required column '{name}' not found in {csv_path.name}. "
                               f"Found columns: {reader.fieldnames}")
            return field_map[lower]

        key_name = get_col(key_col)
        col_names = tuple(get_col(c) for c in cols)

        data = {}
        for row in reader:
            key = row[key_name]
            vals = tuple(row[c] for c in col_names)
            data[key] = vals
        return data

def compare_runs(generated_csv, correct_csv):
    gen = read_subset(generated_csv)
    cor = read_subset(correct_csv)
    if set(gen.keys()) != set(cor.keys()):
        return False
    for k in gen:
        if gen[k] != cor[k]:
            return False
    return True

def main():
    parser = argparse.ArgumentParser(description="Run flux-emulator multiple times and verify determinism.")
    parser.add_argument("--runs", type=int, default=10, help="Number of runs to perform (default: 10)")
    parser.add_argument("--cmd", type=str, required=False,
                        default="python flux-emulator.py /home/j/Desktop/flux/sc25_poster/flux-fiction/test-inputs/lassen_first_1000.csv 792 40",
                        help="Command to execute for each run (quote the whole command).")
    parser.add_argument("--workdir", type=str, default=".", help="Working directory to run the command in.")
    parser.add_argument("--output-csv", type=str, default="job_transitions.csv",
                        help="Path (relative to workdir) to the CSV the emulator produces.")
    parser.add_argument("--correct-csv", type=str, default="/home/j/Desktop/flux/sc25_poster/flux-fiction/src/python/job_transitions_correct.csv",
                        help="Path to the baseline CSV to compare against.")
    parser.add_argument("--variations-dir", type=str, default="variations",
                        help="Directory to copy varied CSVs into.")
    parser.add_argument("--verbose", action="store_true", help="Print per-run status.")
    args = parser.parse_args()

    workdir = Path(args.workdir).resolve()
    output_csv = (workdir / args.output_csv).resolve()
    correct_csv = Path(args.correct_csv).resolve()
    variations_dir = (workdir / args.variations_dir).resolve()
    variations_dir.mkdir(parents=True, exist_ok=True)

    if not correct_csv.exists():
        print(f"[ERROR] Correct CSV not found: {correct_csv}", file=sys.stderr)
        sys.exit(2)

    cmd_list = shlex.split(args.cmd)

    correct = 0
    varied = 0
    varied_paths = []

    iterator = progress_iter(range(1, args.runs + 1), desc="Running tests", unit="run")
    for i in iterator:
        try:
            iterator.set_postfix_str(f"ok={correct} var={varied}")
        except Exception:
            pass
        try:
            result = subprocess.run(cmd_list, cwd=workdir, capture_output=True, text=True, check=False)
        except Exception as e:
            print(f"[RUN {i}] ERROR launching command: {e}", file=sys.stderr)
            varied += 1
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            logbase = variations_dir / f"run{i:03d}_{ts}"
            (logbase.parent).mkdir(parents=True, exist_ok=True)
            with (logbase.with_suffix(".out")).open("w") as fo:
                fo.write(f"Exception: {e}\n")
            continue

        if result.returncode != 0 and args.verbose:
            print(f"[RUN {i}] Command returned exit code {result.returncode}", file=sys.stderr)
            print(result.stdout)
            print(result.stderr, file=sys.stderr)

        if not output_csv.exists():
            print(f"[RUN {i}] ERROR: Expected output CSV not found at {output_csv}", file=sys.stderr)
            varied += 1
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            logbase = variations_dir / f"run{i:03d}_{ts}"
            with (logbase.with_suffix(".out")).open("w") as fo:
                fo.write(result.stdout or "")
            with (logbase.with_suffix(".err")).open("w") as fe:
                fe.write(result.stderr or "")
            continue

        # Compare CSVs
        same = False
        try:
            same = compare_runs(output_csv, correct_csv)
        except Exception as e:
            print(f"[RUN {i}] ERROR comparing CSVs: {e}", file=sys.stderr)
            same = False

        if same:
            correct += 1
            if args.verbose:
                print(f"[RUN {i}] MATCH")
        else:
            varied += 1
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            dest_name = f"job_transitions_run{i:03d}_{ts}.csv"
            dest = variations_dir / dest_name
            try:
                with output_csv.open("rb") as src, dest.open("wb") as dst:
                    dst.write(src.read())
                varied_paths.append(str(dest))
                if args.verbose:
                    print(f"[RUN {i}] VARIATION -> saved {dest}")
            except Exception as e:
                print(f"[RUN {i}] ERROR saving variation: {e}", file=sys.stderr)

    print("\n=== Summary ===")
    print(f"Correct runs: {correct}")
    print(f"Varied runs:  {varied}")
    if varied_paths:
        print("Varied files:")
        for p in varied_paths:
            print(f"  - {p}")

if __name__ == "__main__":
    main()

