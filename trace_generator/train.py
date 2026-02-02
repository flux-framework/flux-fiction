#!/usr/bin/env python3
"""
train.py — Learn a weekly NHPP rate from job-submit timestamps.

- Fits a piecewise-constant weekly rate λ(day_of_week, time_of_day_bin).
- Stores rates as "jobs per hour" per weekday×bin, normalized by how many
  occurrences of that weekday exist in the training span.
- Saves a compact model (.npz) with:
    - rate (7 x bins_per_day) float64
    - include_days (7,) bool
    - bin_minutes (int)
    - tz (str or "")
    - start_iso / end_iso (str)  (training span)
    - time_col (str)
    - version (str)

Example:
    python train.py \
        --alloc-csv /path/final_csm_allocation_history_hashed.csv \
        --time-col job_submit_time \
        --freq 15min \
        --omit-zero-days \
        --out weekly_rate_15m.npz
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional, List
import numpy as np
import pandas as pd

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--alloc-csv", required=True, type=Path,
                   help="CSV with allocation/job-level rows (must include a submit time col).")
    p.add_argument("--time-col", default="job_submit_time",
                   help="Timestamp column name (e.g., job_submit_time). Fallback to begin_time if missing.")
    p.add_argument("--freq", default="15min",
                   help="Bin width (e.g., 5min, 10min, 15min, 30min, 60min). Must evenly divide 24h.")
    p.add_argument("--omit-zero-days", action="store_true",
                   help="Drop weekdays with zero total jobs across the training span.")
    p.add_argument("--start", default=None,
                   help="Optional inclusive start time (ISO, e.g. 2019-01-01).")
    p.add_argument("--end", default=None,
                   help="Optional inclusive end time (ISO).")
    p.add_argument("--tz", default="",
                   help="Optional IANA timezone (e.g., America/Los_Angeles). If provided, localizes timestamps before bucketing.")
    p.add_argument("--out", required=True, type=Path,
                   help="Output model path (.npz).")
    return p.parse_args()

def _ensure_time_col(df: pd.DataFrame, time_col: str) -> str:
    if time_col in df.columns:
        return time_col
    # fallback commonly available in your dataset
    if "begin_time" in df.columns:
        return "begin_time"
    # try a couple more typical names
    for alt in ("submit_time", "submit", "created_at"):
        if alt in df.columns:
            return alt
    raise ValueError(f"Timestamp column '{time_col}' not found and no fallback column present.")

def _bin_minutes(freq: str) -> int:
    td = pd.Timedelta(freq)
    mins = int(td.total_seconds() // 60)
    if mins <= 0 or (1440 % mins) != 0:
        raise ValueError("freq must be a positive divisor of 24h (e.g., 5min, 10min, 15min, 30min, 60min).")
    return mins

def main():
    args = parse_args()

    # Load only the time column to conserve memory
    usecols = None  # let pandas read only needed cols
    df = pd.read_csv(args.alloc_csv, usecols=usecols, low_memory=False)
    time_col = _ensure_time_col(df, args.time_col)

    t = pd.to_datetime(df[time_col], errors="coerce")
    df = pd.DataFrame({time_col: t}).dropna()

    if args.tz:
        # Localize (assume naive times are UTC) then convert to tz, or localize naive to given tz
        if df[time_col].dt.tz is None:
            df[time_col] = df[time_col].dt.tz_localize("UTC").dt.tz_convert(args.tz)
        else:
            df[time_col] = df[time_col].dt.tz_convert(args.tz)

    # Optional filtering by span
    if args.start:
        df = df[df[time_col] >= pd.to_datetime(args.start)]
    if args.end:
        df = df[df[time_col] <= pd.to_datetime(args.end)]

    if df.empty:
        raise SystemExit("No timestamps after filtering; nothing to train.")

    start_iso = df[time_col].min().isoformat()
    end_iso = df[time_col].max().isoformat()

    # Build weekday/bin counts
    bin_minutes = _bin_minutes(args.freq)
    bins_per_day = 1440 // bin_minutes

    # time-of-day in minutes; weekday 0=Mon..6=Sun
    weekday = df[time_col].dt.dayofweek
    minute_of_day = df[time_col].dt.hour * 60 + df[time_col].dt.minute
    bin_idx = (minute_of_day // bin_minutes).astype(int)

    counts = (
        pd.DataFrame({"weekday": weekday, "bin_idx": bin_idx})
        .value_counts()
        .rename("count")
        .reset_index()
    )

    # How many occurrences of each weekday in the training span?
    # (Counts how many distinct calendar days of that weekday exist)
    full_days = pd.date_range(
        start=df[time_col].min().normalize(),
        end=df[time_col].max().normalize(),
        freq="D"
    )
    weekday_occ = pd.Series(full_days.dayofweek).value_counts().reindex(range(7), fill_value=0)

    # Optionally omit weekdays with zero jobs
    day_totals = counts.groupby("weekday")["count"].sum()
    if args.omit_zero_days:
        include_days = np.array([d for d in range(7) if day_totals.get(d, 0) > 0], dtype=int)
    else:
        include_days = np.arange(7, dtype=int)

    # Prepare full grid for included days
    grid = pd.MultiIndex.from_product([include_days, range(bins_per_day)],
                                      names=["weekday", "bin_idx"]).to_frame(index=False)
    counts_full = grid.merge(counts, on=["weekday", "bin_idx"], how="left").fillna({"count": 0})

    # Convert to jobs/hour
    bin_hours = bin_minutes / 60.0
    counts_full["occurs"] = counts_full["weekday"].map(weekday_occ).replace(0, np.nan)

    rate = counts_full["count"] / (counts_full["occurs"] * bin_hours)
    rate = rate.fillna(0.0)

    # ADD THIS: store the series into the frame with a column name
    counts_full["rate_jobs_per_hr"] = rate

    # Pivot to 7 x bins_per_day matrix; fill missing (omitted) days with zeros
    rate_df = counts_full.pivot(index="weekday",
                                columns="bin_idx",
                                values="rate_jobs_per_hr")  # use the column name here

    rate_mat = np.zeros((7, bins_per_day), dtype=np.float64)
    for d in range(7):
        if d in rate_df.index:
            rate_mat[d, :] = rate_df.loc[d].to_numpy()
        else:
            rate_mat[d, :] = 0.0

    # Save model
    np.savez_compressed(
        args.out,
        rate=rate_mat,                             # jobs/hour
        include_days=(np.isin(np.arange(7), include_days)).astype(np.uint8),
        bin_minutes=np.array([bin_minutes], dtype=np.int64),
        tz=np.array([args.tz], dtype=object),
        start_iso=np.array([start_iso], dtype=object),
        end_iso=np.array([end_iso], dtype=object),
        time_col=np.array([time_col], dtype=object),
        version=np.array(["weekly-nhpp/v1"], dtype=object),
    )
    print(f"Saved model → {args.out} | bins_per_day={bins_per_day}, bin_minutes={bin_minutes}")
    print(f"Training span: {start_iso} .. {end_iso}")

if __name__ == "__main__":
    main()
