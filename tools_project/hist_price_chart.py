#!/root/betting/.venv/bin/python
"""
plot_market_ltp_only.py
Plot LTP (last traded price) over time for all runners in a Betfair market file (.json or .json.gz).

- Skips the first 50 lines by default (to ignore early partial messages)
- One chart showing all runners' LTP on a log scale
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


def open_auto(path: str):
    """Open normal or gzipped file transparently."""
    if path.endswith(".gz"):
        return gzip.open(path, "rt", encoding="utf-8", errors="ignore")
    return open(path, "r", encoding="utf-8", errors="ignore")


def parse_stream(path: str, skip_lines: int = 50):
    """Yield (timestamp, market_id, runner_change) from a Betfair MCM JSONL stream file."""
    with open_auto(path) as f:
        for i, line in enumerate(f):
            if i < skip_lines:
                continue  # skip early handshake lines
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
            for mc in msg.get("mc", []):
                mid = mc.get("id")
                for rc in mc.get("rc", []):
                    yield ts, mid, rc


def build_ltp_series(path: str, skip_lines: int = 50):
    """Return market_id and {selection_id: DataFrame(time, ltp)}."""
    data = defaultdict(list)
    market_id = None
    for ts, mid, rc in parse_stream(path, skip_lines=skip_lines):
        if market_id is None and mid:
            market_id = str(mid)
        sid = rc.get("id")
        ltp = rc.get("ltp")
        if sid is not None and ltp is not None:
            data[int(sid)].append({"time": ts, "ltp": ltp})

    # Convert to 1-second resampled DataFrames
    dfs = {}
    for sid, rows in data.items():
        if not rows:
            continue
        df = pd.DataFrame(rows).sort_values("time")
        df = df.drop_duplicates(subset=["time"]).set_index("time")
        df = df.resample("1S").last().dropna(subset=["ltp"])
        dfs[sid] = df.reset_index()
    return market_id or "UNKNOWN_MARKET", dfs


def plot_market(market_id, data_by_sel, out_path, market_name=None):
    if not data_by_sel:
        raise SystemExit("No runner data to plot.")

    fig, ax = plt.subplots(figsize=(10, 5))

    for sid, df in sorted(data_by_sel.items()):
        ax.plot(df["time"], df["ltp"], lw=1.0, label=f"Runner {sid}", alpha=0.85)

    ax.set_yscale("log")
    ax.set_ylim(0.9, 15)
    ax.set_ylabel("LTP (log scale)")
    ax.set_xlabel("Time (UTC)")
    ax.grid(True, which="both", alpha=0.3)

    locator = mdates.AutoDateLocator(minticks=5, maxticks=10)
    fmt = mdates.ConciseDateFormatter(locator)
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(fmt)

    title = market_name or f"Market {market_id}"
    ax.set_title(title)
    ax.legend(loc="best", fontsize=8)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150)
    print(f"✅ Saved plot: {out_path}")


def main():
    ap = argparse.ArgumentParser(description="Plot LTP for all runners in a Betfair market (skip early lines).")
    ap.add_argument("path", help="Path to JSONL (.gz ok)")
    ap.add_argument("--out", help="Output PNG (default: input_plot.png)")
    ap.add_argument("--market-name", help="Optional market name for title")
    ap.add_argument("--skip", type=int, default=50, help="Number of initial lines to skip (default 50)")
    args = ap.parse_args()

    if not os.path.isfile(args.path):
        raise SystemExit(f"File not found: {args.path}")

    base = args.path[:-3] if args.path.endswith(".gz") else args.path
    out = args.out or f"{base}_ltp_plot.png"

    mid, data = build_ltp_series(args.path, skip_lines=args.skip)
    plot_market(mid, data, out, args.market_name)


if __name__ == "__main__":
    main()
