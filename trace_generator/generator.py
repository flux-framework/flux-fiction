#!/usr/bin/env python3
"""
generator.py — Sample event times from a trained weekly NHPP model.

- Loads weekly rate λ(day, bin) in jobs/hour, and bin_minutes.
- Over a requested time window, for each bin:
    - compute expected mean = λ * (bin_seconds / 3600)
    - draw a Poisson count
    - scatter that many events uniformly at random within the bin's seconds
- Outputs a DataFrame (and optional CSV) of event timestamps.

Also includes:
    - plot_event_counts(df, freq="1H") → matplotlib line chart of counts over time.

Example:
    python generate.py \
        --model weekly_rate_15m.npz \
        --start 2020-01-01 \
        --end   2020-01-08 \
        --seed  123 \
        --out   events.csv
"""
from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True, type=Path, help=".npz produced by train.py")
    p.add_argument("--start", required=True, help="Start time (ISO, inclusive)")
    p.add_argument("--end",   required=True, help="End time (ISO, exclusive)")
    p.add_argument("--seed",  default=None, type=int, help="RNG seed for reproducibility")
    p.add_argument("--scale", default=1.0, type=float,
                   help="Global multiplier on rates (e.g., 0.8 to reduce, 1.2 to increase)")
    p.add_argument("--out",   default=None, type=Path, help="Optional CSV to write events to")
    return p.parse_args()

def load_model(path: Path) -> dict:
    z = np.load(path, allow_pickle=True)
    model = {
        "rate": z["rate"],                        # (7, bins_per_day) jobs/hour
        "include_days": z["include_days"].astype(bool),  # (7,)
        "bin_minutes": int(z["bin_minutes"][0]),
        "tz": str(z["tz"][0]) if "tz" in z.files else "",
        "version": str(z["version"][0]) if "version" in z.files else "weekly-nhpp/unknown",
    }
    return model

def _bin_index_from_time(ts: pd.Timestamp, bin_minutes: int) -> tuple[int, int]:
    # weekday 0=Mon..6=Sun, minute-of-day // bin_minutes
    w = ts.dayofweek
    moday_minutes = ts.hour * 60 + ts.minute
    b = moday_minutes // bin_minutes
    return w, int(b)

def _bin_window(ts: pd.Timestamp, bin_minutes: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    # floor to bin start, add bin_minutes
    start = ts.floor(f"{bin_minutes}min")
    end = start + pd.Timedelta(minutes=bin_minutes)
    return start, end

def generate_events(model: dict, start: str, end: str, seed: Optional[int] = None, scale: float = 1.0) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    bin_minutes = model["bin_minutes"]
    bin_seconds = bin_minutes * 60
    rate = model["rate"] * float(scale)  # jobs/hour
    tz = model.get("tz", "")

    # Build bin timeline
    t0 = pd.to_datetime(start)
    t1 = pd.to_datetime(end)
    if tz:
        # keep naive → localize UTC then convert or assume naive already UTC?
        # for generation, we’ll keep naive and rely on consistent bucketing
        pass
    if not (t1 > t0):
        raise ValueError("end must be > start")

    # Create all bin start timestamps covering [t0, t1)
    bin_starts = pd.date_range(t0.floor(f"{bin_minutes}min"), t1, freq=f"{bin_minutes}min", inclusive="left")

    # For each bin, sample count ~ Poisson(rate * bin_hours), then uniform times
    events = []
    for bstart in bin_starts:
        w, b = _bin_index_from_time(bstart, bin_minutes)
        # clamp b within bins_per_day
        bins_per_day = rate.shape[1]
        if b >= bins_per_day:  # can happen if 24h not divisible? (we enforce divisibility in training)
            continue
        lam_hr = rate[w, b]  # jobs/hour
        # calculate bin duration clipped to [t0, t1) in case edges are partial
        bend = bstart + pd.Timedelta(minutes=bin_minutes)
        seg_start = max(bstart, t0)
        seg_end = min(bend, t1)
        seg_seconds = max(0, int((seg_end - seg_start).total_seconds()))
        if seg_seconds == 0:
            continue
        mean_events = lam_hr * (seg_seconds / 3600.0)
        k = rng.poisson(mean_events)
        if k <= 0:
            continue
        # scatter uniformly within [seg_start, seg_end)
        offsets = rng.uniform(0, seg_seconds, size=k)
        stamps = seg_start + pd.to_timedelta(offsets, unit="s")
        events.append(pd.Series(stamps))

    if len(events) == 0:
        return pd.DataFrame({"timestamp": pd.to_datetime([])})

    timestamps = pd.concat(events, ignore_index=True).sort_values().reset_index(drop=True)
    return pd.DataFrame({"timestamp": timestamps})

def plot_event_counts(events_df: pd.DataFrame, freq: str = "1H", ax: Optional[plt.Axes] = None) -> plt.Axes:
    if "timestamp" not in events_df:
        raise ValueError("events_df must have a 'timestamp' column")
    s = pd.Series(1, index=pd.to_datetime(events_df["timestamp"]))
    counts = s.resample(freq).sum().fillna(0)
    if ax is None:
        fig, ax = plt.subplots(figsize=(10, 4))
    ax.plot(counts.index, counts.values, lw=1.8)
    ax.set_ylabel(f"Events / {freq}")
    ax.set_title("Generated event counts")
    ax.grid(True, alpha=0.3)
    return ax

def main():
    args = parse_args()
    model = load_model(args.model)
    events = generate_events(model, start=args.start, end=args.end, seed=args.seed, scale=args.scale)
    print(f"Generated {len(events)} events between {args.start} and {args.end}.")
    if args.out:
        events.to_csv(args.out, index=False)
        print(f"Wrote events → {args.out}")

    # quick preview plot (hourly)
    if len(events) > 0:
        _ = plot_event_counts(events, freq="1H")
        plt.tight_layout()
        plt.savefig("thing.png")

if __name__ == "__main__":
    main()
