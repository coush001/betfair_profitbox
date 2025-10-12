#!/root/betting/.venv/bin/python
"""
plot_market_ltp_and_runner_volume.py

Top subplot:
  • LTP (last traded price, log scale), per runner.
  • Runners with fewer than --min-ticks (default 40) raw LTP ticks are removed.
  • NEW: --every-ltp plots every raw LTP tick (no 1s resample).

Bottom subplot:
  • Per-runner matched amounts (same colors as top):
      - Cumulative matched per runner (solid line)
      - 10-minute bucketed matched per runner (bars)
  • Runner is dropped from bottom if it has < --min-ticks matched updates.

Notes:
  • Uses runner-level 'tv' (snapshot cumulative) to floor cumulative.
  • Uses 'trd' ladder deltas to grow cumulative.
  • Y-axis shows thousands separators.
"""

import argparse
import gzip
import json
import os
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.ticker import StrMethodFormatter


# ---------- File helpers ----------

def open_auto(path: str):
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")


def parse_stream(path: str, skip_lines: int = 50):
    with open_auto(path) as f:
        for i, line in enumerate(f):
            if i < skip_lines:
                continue
            line = line.strip()
            if not line:
                continue
            try:
                msg = json.loads(line)
            except Exception:
                continue
            if not isinstance(msg, dict) or msg.get("op") != "mcm":
                continue
            pt = msg.get("pt") or msg.get("publishTime")
            if pt is None:
                continue
            ts = datetime.fromtimestamp(float(pt) / 1000.0, tz=timezone.utc)
            for mc in msg.get("mc", []) or []:
                mid = mc.get("id")
                rcs = mc.get("rc", []) or []
                yield ts, mid, mc, rcs


# ---------- LTP series ----------

def build_ltp_series(path: str, skip_lines: int = 50, min_ticks: int = 40, every_ltp: bool = False):
    """
    Returns:
      market_id, dict(selection_id -> DataFrame[time, ltp])

    If every_ltp=True: plot raw ticks (no 1s resample).
    Else: resample to 1s (default behaviour).
    """
    raw_points = defaultdict(list)
    market_id = None
    for ts, mid, _mc, rcs in parse_stream(path, skip_lines=skip_lines):
        if market_id is None and mid:
            market_id = str(mid)
        for rc in rcs:
            sid = rc.get("id")
            ltp = rc.get("ltp")
            if sid is not None and ltp is not None:
                raw_points[int(sid)].append({"time": ts, "ltp": ltp})

    kept, dropped = {}, []
    for sid, rows in raw_points.items():
        if len(rows) < min_ticks:
            dropped.append((sid, len(rows)))
            continue
        df = pd.DataFrame(rows).sort_values("time")
        if every_ltp:
            # Keep every raw tick; drop exact-duplicate timestamps only
            df = df.drop_duplicates(subset=["time"]).reset_index(drop=True)
        else:
            # 1-second resample (previous behaviour)
            df = df.drop_duplicates(subset=["time"]).set_index("time")
            df = df.resample("1S").last().dropna(subset=["ltp"]).reset_index()
        kept[sid] = df

    if dropped:
        info = ", ".join(f"{sid}({n})" for sid, n in sorted(dropped))
        print(f"ℹ️ Dropped runners with < {min_ticks} LTP ticks: {info}")

    return market_id or "UNKNOWN_MARKET", kept


# ---------- Matched volume per runner ----------

def build_runner_matched_series(path: str, skip_lines: int = 50, min_ticks: int = 40):
    """
    Returns:
      dict: sid -> DataFrame indexed by time with columns:
            total_matched (cumulative per runner),
            matched_delta (per-second),
            bucket_10min (10T sum of deltas)
    Drops runners with < min_ticks matched updates.
    """
    last_runner_trd_total = defaultdict(float)
    runner_cum = defaultdict(float)
    per_runner_rows = defaultdict(list)

    for ts, _mid, mc, rcs in parse_stream(path, skip_lines=skip_lines):
        for rc in rcs:
            sid = rc.get("id")
            if sid is None:
                continue
            sid = int(sid)

            # tv snapshot floors cumulative
            tv = rc.get("tv")
            if tv is not None:
                try:
                    tv_val = float(tv)
                    runner_cum[sid] = max(runner_cum[sid], tv_val)
                    per_runner_rows[sid].append(
                        {"time": ts, "total_matched": runner_cum[sid], "matched_delta": 0.0}
                    )
                except Exception:
                    pass

            # trd increments grow cumulative
            trd = rc.get("trd")
            if trd:
                try:
                    total_now = sum(
                        float(x[1]) for x in trd if isinstance(x, (list, tuple)) and len(x) >= 2
                    )
                except Exception:
                    total_now = None
                if total_now is not None:
                    prev = last_runner_trd_total[sid]
                    inc = total_now - prev
                    if inc < 0:
                        inc = 0.0
                    last_runner_trd_total[sid] = total_now
                    runner_cum[sid] += inc
                    per_runner_rows[sid].append(
                        {"time": ts, "total_matched": runner_cum[sid], "matched_delta": float(inc)}
                    )

    kept, dropped = {}, []
    for sid, rows in per_runner_rows.items():
        if len(rows) < min_ticks:
            dropped.append((sid, len(rows)))
            continue
        df = pd.DataFrame(rows).drop_duplicates(subset=["time"]).sort_values("time").set_index("time")
        cum = df["total_matched"].resample("1S").last().ffill()
        dlt = df["matched_delta"].resample("1S").sum().fillna(0.0)
        res = pd.concat([cum.rename("total_matched"), dlt.rename("matched_delta")], axis=1)
        res["bucket_10min"] = res["matched_delta"].resample("10T").sum()
        kept[sid] = res

    if dropped:
        info = ", ".join(f"{sid}({n})" for sid, n in sorted(dropped))
        print(f"ℹ️ Dropped runners with < {min_ticks} matched updates: {info}")

    return kept


# ---------- Plotting ----------

def plot_market(market_id, data_by_sel, per_runner_volume, out_path, market_name=None):
    if not data_by_sel:
        raise SystemExit("No runner data to plot (after filtering).")

    fig, (ax_ltp, ax_vol) = plt.subplots(
        nrows=2, ncols=1, figsize=(12, 8), sharex=True,
        gridspec_kw={"height_ratios": [3, 2]}
    )

    # --- Top: LTP per runner ---
    color_map = {}
    for sid, df in sorted(data_by_sel.items()):
        line, = ax_ltp.plot(df["time"], df["ltp"], lw=1.0, label=f"Runner {sid}", alpha=0.9)
        color_map[sid] = line.get_color()

    ax_ltp.set_yscale("log")
    ax_ltp.set_ylim(0.9, 15)
    ax_ltp.set_ylabel("LTP (log)")
    ax_ltp.grid(True, which="both", alpha=0.3)
    title = market_name or f"Market {market_id}"
    ax_ltp.set_title(title)
    ax_ltp.legend(loc="best", fontsize=8)

    # --- Bottom: per-runner matched amounts ---
    any_volume = False
    bar_width_days = 10.0 / (24.0 * 60.0)  # 10 minutes in Matplotlib date units
    for sid in sorted(per_runner_volume.keys()):
        if sid not in data_by_sel:  # stay consistent with top filter
            continue
        vol_df = per_runner_volume[sid]
        if vol_df is None or vol_df.empty:
            continue
        any_volume = True
        c = color_map.get(sid)

        # Cumulative line
        ax_vol.plot(vol_df.index, vol_df["total_matched"], lw=1.5, label=f"Runner {sid} cum", color=c)

        # 10-minute bucket bars
        bucket = vol_df["bucket_10min"].dropna()
        if not bucket.empty:
            ax_vol.bar(bucket.index, bucket.values, width=bar_width_days, align="center",
                       alpha=0.45, label=f"Runner {sid} 10-min", color=c, edgecolor="none")

    ax_vol.set_ylabel("Matched (£)")
    ax_vol.yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
    ax_vol.grid(True, alpha=0.3)

    if any_volume:
        ax_vol.legend(loc="best", fontsize=8)
        ax_vol.set_title("Matched per runner (cum line, 10-min bars)", fontsize=10)
    else:
        ax_vol.text(0.5, 0.5, "No per-runner matched data", ha="center", va="center",
                    transform=ax_vol.transAxes)

    # Shared X axis formatting
    locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
    fmt = mdates.ConciseDateFormatter(locator)
    ax_vol.xaxis.set_major_locator(locator)
    ax_vol.xaxis.set_major_formatter(fmt)
    ax_vol.set_xlabel("Time (UTC)")

    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"✅ Saved plot: {out_path}")


# ---------- CLI ----------

def main():
    ap = argparse.ArgumentParser(description="Plot LTP + per-runner matched volume (10-min bars).")
    ap.add_argument("path", help="Path to JSONL (.gz ok)")
    ap.add_argument("--out", help="Output PNG (default: *_ltp_runner_volume_plot.png)")
    ap.add_argument("--market-name", help="Optional market name for title")
    ap.add_argument("--skip", type=int, default=50, help="Lines to skip (default 50)")
    ap.add_argument("--min-ticks", type=int, default=40,
                    help="Minimum LTP/matched ticks required per runner (default 40)")
    ap.add_argument("--every-ltp", action="store_true",
                    help="Plot every raw LTP tick (no 1-second resample).")
    args = ap.parse_args()

    if not os.path.isfile(args.path):
        raise SystemExit(f"File not found: {args.path}")

    base = args.path[:-3] if args.path.endswith(".gz") else args.path
    out = args.out or f"{base}_ltp_runner_volume_plot.png"

    mid, ltp_data = build_ltp_series(
        args.path, skip_lines=args.skip, min_ticks=args.min_ticks, every_ltp=args.every_ltp
    )
    per_runner_vol = build_runner_matched_series(
        args.path, skip_lines=args.skip, min_ticks=args.min_ticks
    )
    plot_market(mid, ltp_data, per_runner_vol, out, args.market_name)


if __name__ == "__main__":
    main()
