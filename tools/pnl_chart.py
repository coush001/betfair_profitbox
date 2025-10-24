#!/root/betfair_profitbox/.venv/bin/python
# -*- coding: utf-8 -*-

"""
Plots:
  1) Account equity / exposure
  2) Daily PnL
  3) Cumulative gross PnL per strategy (from /root/betfair_profitbox/store/trade_csv/*.csv)
And draws a table (inside the PNG) showing current cumulative PnL per strategy + total.

Inputs:
  /root/betfair_profitbox/store/account_stats/date_equity_pnl.csv
  /root/betfair_profitbox/store/trade_csv/*.csv  (needs: settled_date, gross_profit, customer_strategy_ref)
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import re

# --- Paths ---
BASE = Path("/root/betfair_profitbox")
CSV_PATH = BASE / "store/account_stats/date_equity_pnl.csv"
TRADES_DIR = BASE / "store/trade_csv"
PNG_PATH = CSV_PATH.with_suffix(".png")
README_PATH = BASE / "README.md"

# === Load account equity PnL ===
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", skipinitialspace=True)
df.columns = (
    df.columns.str.replace("\ufeff", "", regex=False)
             .str.strip()
             .str.lower()
             .str.replace(r"\s+", "_", regex=True)
             .str.replace("-", "_")
)

needed = ["timestamp_utc", "total_equity", "available_balance", "open_exposure", "pnl_today"]
missing = [c for c in needed if c not in df.columns]
if missing:
    raise SystemExit(f"Missing columns: {missing}\nFound: {df.columns.tolist()}")

df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
for c in ["total_equity", "available_balance", "open_exposure", "pnl_today"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc")

# === Load trades → cumulative gross PnL per strategy ===
cum = None
if TRADES_DIR.exists():
    files = sorted(TRADES_DIR.glob("*.csv"))
    if files:
        frames = []
        for fp in files:
            try:
                tdf = pd.read_csv(fp, low_memory=False)
            except Exception:
                continue
            tdf.columns = [c.lower() for c in tdf.columns]
            if "settled_date" not in tdf.columns or "gross_profit" not in tdf.columns:
                continue
            if "customer_strategy_ref" not in tdf.columns:
                tdf["customer_strategy_ref"] = "UNKNOWN"
            tdf["settled_date"] = pd.to_datetime(tdf["settled_date"], utc=True, errors="coerce")
            tdf["gross_profit"] = pd.to_numeric(tdf["gross_profit"], errors="coerce")
            tdf = tdf.dropna(subset=["settled_date", "gross_profit"])
            frames.append(tdf[["settled_date", "gross_profit", "customer_strategy_ref"]])

        if frames:
            all_trades = pd.concat(frames, ignore_index=True).sort_values("settled_date")
            all_trades["cum_gross"] = (
                all_trades.groupby("customer_strategy_ref", sort=False)["gross_profit"].cumsum()
            )
            cum = (
                all_trades
                .groupby(["customer_strategy_ref", "settled_date"], as_index=False)["cum_gross"]
                .last()
            )

# === Build summary table text ===
summary_text = ""
if cum is not None and not cum.empty:
    latest = cum.sort_values("settled_date").groupby("customer_strategy_ref").last()
    latest = latest.sort_values("cum_gross", ascending=False)
    total = latest["cum_gross"].sum()
    lines = [f"{'Strategy':<30} {'Cum PnL (£)':>15}", "-" * 47]
    for strat, row in latest.iterrows():
        lines.append(f"{strat:<30} {row['cum_gross']:>15,.2f}")
    lines.append("-" * 47)
    lines.append(f"{'TOTAL':<30} {total:>15,.2f}")
    summary_text = "=== MARKET PNL SUMMARY ===\n" + "\n".join(lines)

# === Combine x-axis range across equity + trades ===
xmin = df["timestamp_utc"].min()
xmax = df["timestamp_utc"].max()
if cum is not None and not cum.empty:
    xmin = min(xmin, cum["settled_date"].min())
    xmax = max(xmax, cum["settled_date"].max())

# === Plot: 3 aligned subplots ===
fig, axes = plt.subplots(
    3, 1, figsize=(12, 11),
    sharex=True,
    gridspec_kw={"height_ratios": [3, 1.5, 2.5]}
)

# 1) Equity / exposure
axes[0].plot(df["timestamp_utc"], df["total_equity"], label="Total Equity", linewidth=2)
axes[0].plot(df["timestamp_utc"], df["available_balance"], label="Available Balance", linestyle="--")
axes[0].plot(df["timestamp_utc"], df["open_exposure"], label="Open Exposure", linestyle=":")
axes[0].set_ylabel("Balance / Exposure")
axes[0].set_title("Account Equity & Exposure Over Time")
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# 2) Daily PnL
x, y = df["timestamp_utc"], df["pnl_today"]
axes[1].plot(x, y, color="black", linewidth=1.2)
axes[1].axhline(0, color="grey", linewidth=0.8)
axes[1].fill_between(x, y, 0, where=(y > 0), color="green", alpha=0.3)
axes[1].fill_between(x, y, 0, where=(y < 0), color="red", alpha=0.3)
axes[1].set_ylabel("PnL Today")
axes[1].grid(True, alpha=0.3)

# 3) Cumulative PnL per strategy
if cum is not None and not cum.empty:
    for strat, g in cum.groupby("customer_strategy_ref", sort=False):
        axes[2].plot(g["settled_date"], g["cum_gross"], label=str(strat))
    axes[2].set_title("Cumulative Gross PnL per Strategy (by settled_date)")
    axes[2].set_ylabel("Cumulative Gross PnL")
    axes[2].legend(loc="best", fontsize=8)
    axes[2].grid(True, alpha=0.3)
else:
    axes[2].text(0.5, 0.5, "No trade CSVs found or missing columns.",
                 ha="center", va="center", transform=axes[2].transAxes)
    axes[2].set_ylabel("Cumulative Gross PnL")
    axes[2].grid(True, alpha=0.3)

# Align x-axis across all
for ax in axes:
    ax.set_xlim(xmin, xmax)
axes[2].set_xlabel("Timestamp (UTC)")

# === Figure-level table (not an axes) ===
# Reserve bottom space for the table and draw it in figure coordinates
fig.subplots_adjust(bottom=0.25, hspace=0.35, top=0.95)

if summary_text:
    fig.text(
        0.5, 0.02,  # centered horizontally
        summary_text,
        ha="center", va="bottom",                # center align horizontally
        family="monospace", fontsize=11,
        linespacing=1.2,
        bbox=dict(facecolor="white", edgecolor="0.8",
                  boxstyle="round,pad=0.6", alpha=0.9)
    )

# Save
fig.savefig(PNG_PATH, dpi=150)
print(f"✅ Saved plot to {PNG_PATH}")

# --- Update README.md with image link ---
rel_path = PNG_PATH.relative_to(BASE)
img_md = f"![PnL chart]({rel_path})"

if README_PATH.exists():
    text = README_PATH.read_text(encoding="utf-8")
    pattern = re.compile(r"^!\[PnL chart\]\(.*\)$", re.MULTILINE)
    if pattern.search(text):
        text = pattern.sub(img_md, text)
    else:
        text = text.rstrip() + "\n\n" + img_md + "\n"
    README_PATH.write_text(text, encoding="utf-8")
else:
    README_PATH.write_text("# Account & PnL\n\n" + img_md + "\n", encoding="utf-8")
