#!/root/betting/.venv/bin/python
# -*- coding: utf-8 -*-

"""
Plot equity, balances, exposure and PnL from:
  /root/betting/store/date_equity_pnl.csv

Expected columns:
  timestamp_utc, total_equity, available_balance, open_exposure, pnl_today, currency
"""

import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

CSV_PATH = Path("/root/betting/store/date_equity_pnl.csv")

# --- Read and prepare data ---
df = pd.read_csv(CSV_PATH)
df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
df = df.sort_values("timestamp_utc")

# --- Create figure with two panels ---
fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
                         gridspec_kw={"height_ratios": [3, 1]})

# --- Top plot: account balances & exposure ---
axes[0].plot(df["timestamp_utc"], df["total_equity"], label="Total Equity", linewidth=2)
axes[0].plot(df["timestamp_utc"], df["available_balance"], label="Available Balance", linestyle="--")
axes[0].plot(df["timestamp_utc"], df["open_exposure"], label="Open Exposure", linestyle=":")
axes[0].set_ylabel("Balance / Exposure")
axes[0].legend()
axes[0].grid(True, alpha=0.3)
axes[0].set_title("Account Equity & Exposure Over Time")

# --- Bottom plot: daily PnL ---
axes[1].plot(df["timestamp_utc"], df["pnl_today"], color="black", linewidth=1.5)
axes[1].set_ylabel("PnL Today")
axes[1].set_xlabel("Timestamp (UTC)")
axes[1].grid(True, alpha=0.3)

# --- Tight layout + save ---
plt.tight_layout()
out_path = CSV_PATH.with_suffix(".png")
plt.savefig(out_path, dpi=150)
print(f"Saved plot to {out_path}")

plt.show()
