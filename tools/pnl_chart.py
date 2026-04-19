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

import os
from datetime import datetime, timedelta, timezone

import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from pathlib import Path
import re

# --- Paths ---
BASE = Path("/root/betfair_profitbox")
CSV_PATH = BASE / "store/account_stats/date_equity_pnl.csv"
TRADES_DIR = BASE / "store/trade_csv"
PNG_PATH = CSV_PATH.with_suffix(".png")
README_PATH = BASE / "README.md"
CERTS = (
    "/root/betfair_profitbox/certs/client-2048.crt",
    "/root/betfair_profitbox/certs/client-2048.key",
)

def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def fetch_betfair_daily_pnl(start_date: datetime, end_date: datetime):
    try:
        import betfairlightweight as bflw
        from betfairlightweight.filters import time_range
    except ImportError:
        print("⚠️ betfairlightweight not installed; using local daily pnl from CSV")
        return None

    load_dotenv()
    USERNAME = os.getenv("BETFAIR_USERNAME")
    PASSWORD = os.getenv("BETFAIR_PASSWORD")
    APP_KEY = os.getenv("BETFAIR_APP_KEY")
    if not USERNAME or not PASSWORD or not APP_KEY:
        print("⚠️ Missing BETFAIR_USERNAME/PASSWORD/APP_KEY; using local daily pnl from CSV")
        return None

    client = bflw.APIClient(USERNAME, password=PASSWORD, app_key=APP_KEY, cert_files=CERTS)
    try:
        client.login()
    except Exception as exc:
        print(f"⚠️ Betfair login failed: {exc}; using local daily pnl from CSV")
        return None

    settled_range = time_range(
        from_=iso_utc(start_date),
        to=iso_utc(end_date + timedelta(days=1) - timedelta(microseconds=1)),
    )
    try:
        report = client.betting.list_cleared_orders(
            bet_status="SETTLED",
            settled_date_range=settled_range,
            include_item_description=False,
            locale="en",
        )
    except Exception as exc:
        print(f"⚠️ Betfair cleared orders query failed: {exc}; using local daily pnl from CSV")
        client.logout()
        return None

    cleared = getattr(report, "cleared_orders", []) or getattr(report, "orders", [])
    rows = []
    for co in cleared or []:
        profit = float(getattr(co, "profit", 0.0) or 0.0)
        settled_date = getattr(co, "settled_date", None)
        if not settled_date:
            continue
        if isinstance(settled_date, str):
            try:
                settled_date = datetime.fromisoformat(settled_date.replace("Z", "+00:00"))
            except Exception:
                continue
        settled_date = settled_date.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        rows.append((settled_date, profit))

    client.logout()
    if not rows:
        print("⚠️ Betfair cleared orders returned no rows; using local daily pnl from CSV")
        return None

    pnl_df = pd.DataFrame(rows, columns=["date", "betfair_pnl"])
    pnl_df = pnl_df.groupby("date", as_index=False)["betfair_pnl"].sum().sort_values("date")
    return pnl_df


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

# === Build daily PnL series ===
daily_pnl_source = "local CSV"
daily_pnl_df = (
    df.assign(date=df["timestamp_utc"].dt.normalize())
      .groupby("date", as_index=False)["pnl_today"].sum()
      .rename(columns={"pnl_today": "daily_pnl"})
)

# Ensure a continuous daily index so the chart covers every calendar day
if not daily_pnl_df.empty:
    full_dates = pd.date_range(
        start=daily_pnl_df["date"].min(),
        end=daily_pnl_df["date"].max(),
        freq="D",
        tz="UTC",
    )
    daily_pnl_df = (
        daily_pnl_df.set_index("date")
                    .reindex(full_dates, fill_value=0)
                    .rename_axis("date")
                    .reset_index()
    )

betfair_daily = fetch_betfair_daily_pnl(
    daily_pnl_df["date"].min(),
    daily_pnl_df["date"].max(),
)
if betfair_daily is not None and not betfair_daily.empty:
    betfair_daily = betfair_daily.set_index("date").reindex(full_dates, fill_value=0).rename_axis("date").reset_index()
    daily_pnl_df = betfair_daily.rename(columns={"betfair_pnl": "daily_pnl"})
    daily_pnl_source = "Betfair API"

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

# === Build all-time / last-month subsets ===
end_ts = df["timestamp_utc"].max()
recent_threshold = end_ts - pd.Timedelta(days=30)
df_recent = df[df["timestamp_utc"] >= recent_threshold]

cum_recent = None
if cum is not None and not cum.empty:
    cum_recent = cum[cum["settled_date"] >= recent_threshold].copy()
    if not cum_recent.empty:
        filled = []
        for strat, g in cum_recent.groupby("customer_strategy_ref", sort=False):
            g = g.sort_values("settled_date")
            before = cum[(cum["customer_strategy_ref"] == strat) & (cum["settled_date"] < recent_threshold)]
            if not before.empty:
                last_before = before.sort_values("settled_date").iloc[-1]
                start_row = pd.DataFrame([
                    {
                        "settled_date": recent_threshold,
                        "customer_strategy_ref": strat,
                        "cum_gross": last_before["cum_gross"],
                    }
                ])
            else:
                start_row = None

            end_row = None
            if g["settled_date"].iloc[-1] < end_ts:
                end_row = pd.DataFrame([
                    {
                        "settled_date": end_ts,
                        "customer_strategy_ref": strat,
                        "cum_gross": g["cum_gross"].iloc[-1],
                    }
                ])

            parts = [part for part in (start_row, g, end_row) if part is not None]
            filled.append(pd.concat(parts, ignore_index=True))
        cum_recent = pd.concat(filled, ignore_index=True)

recent_date_threshold = recent_threshold.normalize()
daily_pnl_recent = daily_pnl_df[daily_pnl_df["date"] >= recent_date_threshold]

# === Build per-strategy reporting table ===
strategy_table_data = []
if cum is not None and not cum.empty:
    latest = cum.sort_values("settled_date").groupby("customer_strategy_ref").last()
    latest = latest.sort_values("cum_gross", ascending=False)
    total_all = latest["cum_gross"].sum()

    if 'all_trades' in locals():
        overall_summary = (
            all_trades.groupby("customer_strategy_ref", sort=False)
            .agg(total_trades=("gross_profit", "count"))
        )
        recent_summary = (
            all_trades[all_trades["settled_date"] >= recent_threshold]
            .groupby("customer_strategy_ref", sort=False)
            .agg(last_30d_pnl=("gross_profit", "sum"), recent_trades=("gross_profit", "count"))
        )
    else:
        overall_summary = pd.DataFrame(columns=["total_trades"])
        recent_summary = pd.DataFrame(columns=["last_30d_pnl", "recent_trades"])

    latest = latest.join(overall_summary, how="left")
    latest = latest.join(recent_summary, how="left").fillna(0)
    latest = latest.sort_values(["cum_gross", "last_30d_pnl"], ascending=False)
    latest["total_trades"] = latest["total_trades"].astype(int)
    latest["recent_trades"] = latest["recent_trades"].astype(int)
    total_recent = latest["last_30d_pnl"].sum()
    total_trades = latest["total_trades"].sum()
    total_recent_trades = latest["recent_trades"].sum()

    for strat, row in latest.iterrows():
        strategy_table_data.append([
            str(strat),
            f"{row['cum_gross']:,.2f}",
            f"{row['last_30d_pnl']:,.2f}",
            f"{row['total_trades']}",
            f"{row['recent_trades']}"
        ])

    strategy_table_data.append([
        "TOTAL",
        f"{total_all:,.2f}",
        f"{total_recent:,.2f}",
        f"{total_trades}",
        f"{total_recent_trades}"
    ])

# === Plot: all-time / last-month chart matrix ===
fig = plt.figure(figsize=(18, 20))
gs = fig.add_gridspec(4, 2, height_ratios=[2, 2, 2, 1], hspace=0.35, wspace=0.22)

axes = [
    fig.add_subplot(gs[0, 0]), fig.add_subplot(gs[0, 1]),
    fig.add_subplot(gs[1, 0]), fig.add_subplot(gs[1, 1]),
    fig.add_subplot(gs[2, 0]), fig.add_subplot(gs[2, 1]),
]

def style_plot_axis(ax):
    ax.grid(True, alpha=0.25)
    for label in ax.get_xticklabels():
        label.set_rotation(20)
        label.set_ha("right")


def cumulative_totals(data):
    if data is None or data.empty:
        return None
    series_frames = []
    for strat, g in data.groupby("customer_strategy_ref", sort=False):
        s = g.sort_values("settled_date")[['settled_date', 'cum_gross']].set_index('settled_date')
        s = s.rename(columns={'cum_gross': strat})
        series_frames.append(s)
    if not series_frames:
        return None
    totals = pd.concat(series_frames, axis=1).sort_index()
    totals = totals.ffill().fillna(0)
    totals = totals.assign(cum_gross=totals.sum(axis=1))
    return totals.reset_index()[['settled_date', 'cum_gross']]

# Equity / exposure
for ax, data, title in [
    (axes[0], df, "All Time: Account Equity & Exposure"),
    (axes[1], df_recent, "Last 30 Days: Account Equity & Exposure")
]:
    if data.empty:
        ax.text(0.5, 0.5, "No data for this period.", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        ax.set_ylabel("Balance / Exposure")
        continue
    ax.plot(data["timestamp_utc"], data["total_equity"], label="Total Equity", linewidth=2)
    ax.plot(data["timestamp_utc"], data["available_balance"], label="Available Balance", linestyle="--")
    ax.plot(data["timestamp_utc"], data["open_exposure"], label="Open Exposure", linestyle=":")
    ax.set_title(title)
    ax.set_ylabel("Balance / Exposure")
    ax.legend(fontsize=9)
    style_plot_axis(ax)

# Daily PnL
for ax, data, title in [
    (axes[2], daily_pnl_df, f"All Time: Daily PnL ({daily_pnl_source})"),
    (axes[3], daily_pnl_recent, f"Last 30 Days: Daily PnL ({daily_pnl_source})")
]:
    if data.empty:
        ax.text(0.5, 0.5, "No data for this period.", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        ax.set_ylabel("PnL Today")
        continue
    x = data["date"]
    y = data["daily_pnl"]
    colors = ["green" if v >= 0 else "red" for v in y]
    width = pd.Timedelta(days=0.8)
    ax.bar(x, y, width=width, color=colors, edgecolor="black", linewidth=0.3)
    ax.axhline(0, color="grey", linewidth=0.8)
    ax.set_title(title)
    ax.set_ylabel("PnL Today")
    style_plot_axis(ax)

# Cumulative PnL per strategy
for ax, data, title in [
    (axes[4], cum, "All Time: Cumulative Gross PnL per Strategy"),
    (axes[5], cum_recent, "Last 30 Days: Cumulative Gross PnL per Strategy")
]:
    if data is None or data.empty:
        ax.text(0.5, 0.5, "No trade data for this period.", ha="center", va="center", transform=ax.transAxes)
        ax.set_title(title)
        ax.set_ylabel("Cumulative Gross PnL")
        continue
    for strat, g in data.groupby("customer_strategy_ref", sort=False):
        ax.plot(g["settled_date"], g["cum_gross"], label=str(strat), linewidth=1.5)
    totals = cumulative_totals(data)
    if totals is not None and not totals.empty:
        ax.plot(
            totals["settled_date"],
            totals["cum_gross"],
            label="TOTALS",
            color="black",
            linewidth=1.5,
            linestyle="--",
            zorder=10,
        )
    ax.set_title(title)
    ax.set_ylabel("Cumulative Gross PnL")
    ax.legend(loc="best", fontsize=8, ncol=2)
    style_plot_axis(ax)

# Ensure the bottom row of charts labels dates clearly
axes[4].set_xlabel("Date (UTC)")
axes[5].set_xlabel("Date (UTC)")

# === Table summary row ===
table_ax = fig.add_subplot(gs[3, :])
table_ax.axis("off")
if strategy_table_data:
    col_labels = ["Strategy", "Total Cum PnL (£)", "Last 30d PnL (£)", "Trades", "Recent Trades"]
    table = table_ax.table(
        cellText=strategy_table_data,
        colLabels=col_labels,
        cellLoc="center",
        colLoc="center",
        loc="center",
        colWidths=[0.30, 0.18, 0.18, 0.14, 0.14]
    )
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.4)
    table_ax.set_title("Strategy PnL Summary", fontsize=14, pad=12)
else:
    table_ax.text(0.5, 0.5, "No strategy trade data available to render a summary table.",
                  ha="center", va="center", fontsize=12, transform=table_ax.transAxes)

# Save
fig.savefig(PNG_PATH, dpi=150, bbox_inches="tight")
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
