#!/root/betting/.venv/bin/python
"""
plot_market_file.py
- Input: Betfair MarketChange messages (JSON lines), optionally gzipped (.gz)
- Output: PNG plot showing price over time and matched-volume deltas, per runner
"""

import argparse
import gzip
import io
import json
import math
import os
from datetime import datetime, timezone
from collections import defaultdict, deque

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd


def open_auto(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")


def parse_stream(path: str):
    """
    Parse JSONL of Betfair stream 'mcm' messages.
    Yields tuples: (publish_dt_utc, market_id, list_of_runner_changes)
    where each runner_change is dict with keys possibly including:
      id (selection_id), ltp, atb, atl, tv, etc.
    """
    with open_auto(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            # Expect Betfair stream shape
            if not isinstance(msg, dict):
                continue
            if msg.get("op") != "mcm":
                continue
            pt = msg.get("pt")  # publish time in ms
            if pt is None:
                # Some recorders store 'publishTime' instead of 'pt'
                pt = msg.get("publishTime")
            if pt is None:
                continue
            ts = datetime.fromtimestamp(float(pt) / 1000.0, tz=timezone.utc)

            mcs = msg.get("mc") or []
            for mc in mcs:
                mid = mc.get("id")
                rcs = mc.get("rc") or []
                yield ts, mid, rcs


def extract_best_price(ladder):
    """
    ladder: list of [price, size] OR dicts with 'price','size'
    Returns (best_price, best_size) or (None, None)
    """
    if not ladder:
        return None, None
    first = ladder[0]
    if isinstance(first, dict):
        return first.get("price"), first.get("size")
    if isinstance(first, (list, tuple)) and len(first) >= 2:
        return first[0], first[1]
    return None, None


def sum_tv(tv):
    """
    tv is cumulative traded volume as list of [price, size] or dicts.
    We sum all sizes to get cumulative matched volume.
    """
    if not tv:
        return 0.0
    total = 0.0
    for x in tv:
        if isinstance(x, dict):
            total += float(x.get("size", 0.0) or 0.0)
        else:
            # [price, size]
            if len(x) >= 2:
                total += float(x[1] or 0.0)
    return total


def build_timeseries(path: str, max_points: int = 0):
    """
    Returns:
      market_id: str
      data_by_sel: dict[selection_id] -> pandas.DataFrame with columns:
        ['time','ltp','best_back','best_lay','vol_delta']
    """
    # per-runner last cumulative traded volume for delta calc
    last_cum_tv = {}
    data_by_sel = defaultdict(list)
    market_id = None

    for ts, mid, rcs in parse_stream(path):
        if market_id is None and mid:
            market_id = str(mid)
        for rc in rcs:
            sel_id = rc.get("id")
            if sel_id is None:
                continue
            ltp = rc.get("ltp")
            atb = rc.get("atb") or rc.get("availableToBack") or []
            atl = rc.get("atl") or rc.get("availableToLay") or []
            bb, bb_sz = extract_best_price(atb)
            bl, bl_sz = extract_best_price(atl)

            # traded volume cumulative (sum of 'tv' sizes)
            cum_tv = sum_tv(rc.get("tv"))
            key = (market_id, sel_id)
            prev = last_cum_tv.get(key, None)
            vol_delta = cum_tv - prev if prev is not None else 0.0
            last_cum_tv[key] = cum_tv

            data_by_sel[int(sel_id)].append(
                {"time": ts, "ltp": ltp, "best_back": bb, "best_lay": bl, "vol_delta": vol_delta}
            )

    # Convert to DataFrames and (optionally) cap points per runner
    dfs = {}
    for sid, rows in data_by_sel.items():
        if not rows:
            continue
        df = pd.DataFrame(rows).sort_values("time")
        # Drop duplicates on time to avoid bar overplot
        df = df.drop_duplicates(subset=["time"], keep="last")
        if max_points and len(df) > max_points:
            # downsample uniformly by index to max_points
            step = math.ceil(len(df) / max_points)
            df = df.iloc[::step].reset_index(drop=True)
        dfs[sid] = df
    return market_id or "UNKNOWN_MARKET", dfs


def resample_runner_df(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample time-indexed series to a fixed interval, taking:
      - last observed ltp/best_back/best_lay within the bin
      - sum of vol_delta within the bin
    """
    sdf = df.copy()
    sdf = sdf.set_index("time")
    agg = {
        "ltp": "last",
        "best_back": "last",
        "best_lay": "last",
        "vol_delta": "sum",
    }
    sdf = sdf.resample(rule).apply(agg).dropna(how="all")
    sdf = sdf.reset_index()
    return sdf


def plot_market(market_id: str, data_by_sel: dict, out_path: str, market_name: str = None,
                ylog=True, y_max=15.0, resample_rule: str | None = None):
    n = len(data_by_sel)
    if n == 0:
        raise SystemExit("No runner data parsed from file.")

    ncols = 3 if n >= 9 else (2 if n >= 4 else 1)
    nrows = math.ceil(n / ncols)
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 3.8 * nrows), squeeze=False)
    axes = axes.flatten()

    # Time span for tick logic
    all_times = []
    for df in data_by_sel.values():
        if not df.empty:
            all_times.append(df["time"].min())
            all_times.append(df["time"].max())
    tmin = min(all_times) if all_times else None
    tmax = max(all_times) if all_times else None

    # X tick setup with concise formatter
    major_locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
    major_formatter = mdates.ConciseDateFormatter(major_locator)

    for idx, (sid, df) in enumerate(sorted(data_by_sel.items(), key=lambda kv: kv[0])):
        ax = axes[idx]
        if df.empty:
            ax.set_visible(False)
            continue

        if resample_rule:
            df = resample_runner_df(df, resample_rule)

        # Left axis: prices
        ax.plot(df["time"], df["ltp"], lw=1.2, label="LTP", alpha=0.9)
        ax.plot(df["time"], df["best_back"], lw=0.8, linestyle="--", label="Best Back", alpha=0.8)
        ax.plot(df["time"], df["best_lay"], lw=0.8, linestyle="--", label="Best Lay", alpha=0.8)

        if ylog:
            ax.set_yscale("log")
            ax.set_ylim(0.9, y_max)
            ax.set_ylabel("Price (log)")
        else:
            ax.set_ylim(1.0, y_max)
            ax.set_ylabel("Price")

        # Right axis: volume deltas as bars
        ax2 = ax.twinx()
        ax2.bar(df["time"], df["vol_delta"], width=0.8 * (df["time"].diff().median() or pd.Timedelta(seconds=1)),
                alpha=0.25, label="ΔMatched Vol")
        ax2.set_ylabel("ΔMatched Vol")
        ax2.margins(y=0.1)

        # X axis formatting
        ax.xaxis.set_major_locator(major_locator)
        ax.xaxis.set_major_formatter(major_formatter)
        ax.tick_params(axis="x", which="major", labelsize=8)
        if tmin and tmax:
            ax.set_xlim(tmin, tmax)
        ax.grid(True, alpha=0.3, which="both")

        ax.set_title(f"selection_id={sid}")

        # Combined legend
        lines, labels = ax.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax.legend(lines + lines2, labels + labels2, loc="best", fontsize=8)

    # Hide unused axes
    for j in range(idx + 1, len(axes)):
        axes[j].set_visible(False)

    fig_title = market_name or f"Market {market_id}"
    fig.suptitle(fig_title, y=0.995)
    plt.tight_layout(rect=[0, 0, 1, 0.97])
    fig.savefig(out_path, dpi=150)
    print(f"✅ Saved plot: {out_path}")


def main():
    ap = argparse.ArgumentParser(description="Plot Betfair market file (price & volume over time).")
    ap.add_argument("path", help="Path to JSONL (optionally .gz) of MCM stream")
    ap.add_argument("--out", help="Output PNG path (default: <input>_plot.png)")
    ap.add_argument("--market-name", help="Optional figure title override")
    ap.add_argument("--resample", help="Optional pandas rule (e.g., 1S, 5S, 10S) to reduce density")
    ap.add_argument("--max-points", type=int, default=0, help="Uniformly downsample each runner to at most N points")
    ap.add_argument("--ymax", type=float, default=15.0, help="Upper y limit for price axis")
    ap.add_argument("--linear", action="store_true", help="Use linear y-scale instead of log")
    args = ap.parse_args()

    in_path = args.path
    if not os.path.isfile(in_path):
        raise SystemExit(f"File not found: {in_path}")

    out_path = args.out
    if not out_path:
        base = in_path[:-3] if in_path.endswith(".gz") else in_path
        out_path = f"{base}_plot.png"

    market_id, data_by_sel = build_timeseries(in_path, max_points=args.max_points)
    if not data_by_sel:
        raise SystemExit("No data parsed (no runner time series built).")

    plot_market(
        market_id,
        data_by_sel,
        out_path=out_path,
        market_name=args.market_name,
        ylog=not args.linear,
        y_max=args.ymax,
        resample_rule=args.resample,
    )


if __name__ == "__main__":
    main()
