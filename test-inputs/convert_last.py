#!/usr/bin/env python3

# This program converts the LAST dataset to something usable by Flux Fiction

import csv
import sys
from datetime import datetime, timedelta

SEQUENTIAL_JOB_IDS = True

INPUT_PATH  = sys.argv[1] if len(sys.argv) > 1 else "input.csv"
OUTPUT_PATH = sys.argv[2] if len(sys.argv) > 2 else "output.csv"

def parse_dt(s):
    """Parse datetimes like '2018-08-22 10:43:49.786757' or without micros."""
    if s is None:
        return None
    s = s.strip().strip('"')
    if not s or s.upper() == "NA":
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            pass
    raise ValueError(f"Unrecognized datetime format: {s}")

def seconds_to_hhmmss(x):
    """Convert seconds (int/str) to HH:MM:SS, tolerate NA/blank."""
    if x is None:
        return ""
    if isinstance(x, str):
        x = x.strip().strip('"')
        if not x or x.upper() == "NA":
            return ""
        try:
            x = int(float(x))
        except ValueError:
            return ""
    td = timedelta(seconds=int(x))
    total_seconds = int(td.total_seconds())
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def timedelta_to_hhmmss(td):
    if td is None:
        return ""
    total_seconds = int(round(td.total_seconds()))
    if total_seconds < 0:
        total_seconds = 0
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def to_iso_basic(dt):
    if dt is None:
        return ""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

def read_rows(path):
    with open(path, newline="") as f:
        sniffer = csv.Sniffer()
        sample = f.read(4096)
        f.seek(0)
        dialect = sniffer.sniff(sample)
        reader = csv.DictReader(f, dialect=dialect)
        return list(reader)

def main():
    rows = read_rows(INPUT_PATH)

    out_fields = ["JobID", "NNodes", "NCPUS", "Timelimit", "Submit", "Elapsed", "NGPUS"]
    out = []

    seq_id = 1
    for r in rows:
        def get(name, default=""):
            v = r.get(name, default)
            if v is None:
                return default
            v = str(v).strip()
            return "" if v.upper() == "NA" else v

        begin = parse_dt(get("begin_time"))
        end   = parse_dt(get("end_time"))
        submit= parse_dt(get("job_submit_time"))

        elapsed = timedelta_to_hhmmss(end - begin if (begin and end) else None)

        jobid = str(seq_id) if SEQUENTIAL_JOB_IDS else get("primary_job_id") or str(seq_id)
        nnodes = get("num_nodes") or "0"
        ncpus  = get("num_processors") or "0"
        ngpus  = get("num_gpus") or "0"
        tlimit = seconds_to_hhmmss(get("time_limit"))

        out.append({
            "JobID": jobid,
            "NNodes": nnodes,
            "NCPUS": ncpus,
            "Timelimit": tlimit,
            "Submit": to_iso_basic(submit),
            "Elapsed": elapsed,
            "NGPUS": ngpus,
        })
        seq_id += 1

    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_fields)
        writer.writeheader()
        writer.writerows(out)

if __name__ == "__main__":
    main()
