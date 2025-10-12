#!/root/betting/.venv/bin/python
# -*- coding: utf-8 -*-

"""
Plot account equity, balances, exposure, and PnL from:
  /root/betting/store/date_equity_pnl.csv

Then save plot as PNG and update README.md to embed the image.

Expected CSV columns:
  timestamp_utc, total_equity, available_balance, open_exposure, pnl_today, currency
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import re

# --- Paths ---
BASE = Path("/root/betting")
CSV_PATH = BASE / "store/account_stats/date_equity_pnl.csv"
PNG_PATH = CSV_PATH.with_suffix(".png")
README_PATH = BASE / "README.md"

# --- Read CSV robustly ---
df = pd.read_csv(CSV_PATH, encoding="utf-8-sig", skipinitialspace=True)
df.columns = (
    df.columns.str.replace("\ufeff", "", regex=False)
             .str.strip()
             .str.lower()
             .str.replace(r"\s+", "_", regex=True)
             .str.replace("-", "_")
)

# --- Verify expected columns ---
needed = ["timestamp_utc", "total_equity", "available_balance", "open_exposure", "pnl_today"]
missing = [c for c in needed if c not in df.columns]
if missing:
    raise SystemExit(f"Missing columns: {missing}\nFound: {df.columns.tolist()}")

# --- Parse and clean ---
df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
for c in ["total_equity", "available_balance", "open_exposure", "pnl_today"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
df = df.dropna(subset=["timestamp_utc"]).sort_values("timestamp_utc")

# --- Plot ---
fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True, gridspec_kw={"height_ratios": [3, 1]})

# Top: balances/exposure
axes[0].plot(df["timestamp_utc"], df["total_equity"], label="Total Equity", linewidth=2)
axes[0].plot(df["timestamp_utc"], df["available_balance"], label="Available Balance", linestyle="--")
axes[0].plot(df["timestamp_utc"], df["open_exposure"], label="Open Exposure", linestyle=":")
axes[0].set_ylabel("Balance / Exposure")
axes[0].set_title("Account Equity & Exposure Over Time")
axes[0].legend()
axes[0].grid(True, alpha=0.3)

# Bottom: PnL (green/red shading)
x, y = df["timestamp_utc"], df["pnl_today"]
axes[1].plot(x, y, color="black", linewidth=1.2)
axes[1].axhline(0, color="grey", linewidth=0.8)
axes[1].fill_between(x, y, 0, where=(y > 0), color="green", alpha=0.3, interpolate=True)
axes[1].fill_between(x, y, 0, where=(y < 0), color="red", alpha=0.3, interpolate=True)
axes[1].set_ylabel("PnL Today")
axes[1].set_xlabel("Timestamp (UTC)")
axes[1].grid(True, alpha=0.3)

plt.tight_layout()
plt.savefig(PNG_PATH, dpi=150)
print(f"✅ Saved plot to {PNG_PATH}")

# --- Update README.md with image link ---
rel_path = PNG_PATH.relative_to(BASE)
img_md = f"![PnL chart]({rel_path})"

if README_PATH.exists():
    print('readme exists')
    text = README_PATH.read_text(encoding="utf-8")

    # Look for an existing line starting with ![PnL chart]
    pattern = re.compile(r"^!\[PnL chart\]\(.*\)$", re.MULTILINE)
    # if pattern.search(text):
    text = pattern.sub(img_md, text)
    print("🔁 Updated existing PnL chart image in README")