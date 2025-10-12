#!/root/betting/.venv/bin/python
# plot_trades_grid.py — per (market_id, selection_id) chart with winner badge, log y-axis, fixed limits, smart x-ticks
import os, json, math
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from collections import defaultdict

# ===== CONFIG =====
TODAY = datetime.utcnow().strftime("%Y-%m-%d")
CSV_PATH = f"/root/betting/store/trade_csv/{TODAY}.csv"
OUT_DIR = "/root/betting/store/trade_chart"
OUT_IMG = os.path.join(OUT_DIR, f"{TODAY}.png")
CACHE_JSON = "/root/betting/store/cache/market_selection_names.json"

CATALOGUE_CSV_CANDIDATES = [
    "/root/betting/store/reports/market_catalogue.csv",
    "/root/betting/store/reports/selection_catalogue.csv",
]
os.makedirs(OUT_DIR, exist_ok=True)

# ===== LOAD DATA =====
df = pd.read_csv(CSV_PATH)
for col in ("placed_date", "settled_date"):
    if col in df.columns:
        df[col] = pd.to_datetime(df[col], errors="coerce")
df = df.dropna(subset=["placed_date"])
df = df.sort_values("placed_date")

if df.empty:
    raise SystemExit("No trades found to plot (empty dataframe).")

# Determine overall time span (for tick density)
tmin = df["placed_date"].min()
tmax = df["placed_date"].max()
span_seconds = max(1.0, (tmax - tmin).total_seconds())
span_hours = span_seconds / 3600.0
span_days = span_seconds / 86400.0

# ===== ENRICHMENT (names) =====
market_event_name = {}
market_runner_name = defaultdict(dict)

# From cache
CACHE_HAD_ERROR = False
if os.path.isfile(CACHE_JSON):
    try:
        with open(CACHE_JSON, "r") as f:
            cache = json.load(f)
        market_event_name.update(cache.get("market_event_name", {}))
        for mid, selmap in cache.get("market_runner_name", {}).items():
            market_runner_name[mid].update({int(k): v for k, v in selmap.items()})
    except Exception as e:
        CACHE_HAD_ERROR = True
        print(f"⚠️ Cache read error: {e}")

# From local CSVs
def try_load_catalogue_csv(path):
    try:
        cat = pd.read_csv(path)
        if "market_id" in cat.columns and "event_name" in cat.columns:
            for _, r in cat[["market_id", "event_name"]].dropna().iterrows():
                market_event_name[str(r["market_id"])] = str(r["event_name"])
        if {"market_id", "selection_id", "runner_name"}.issubset(cat.columns):
            for _, r in cat[["market_id", "selection_id", "runner_name"]].dropna().iterrows():
                market_runner_name[str(r["market_id"])][int(r["selection_id"])] = str(r["runner_name"])
        print(f"ℹ️ Loaded enrichment from {path}")
    except Exception as e:
        print(f"ℹ️ Skipped {path}: {e}")

for p in CATALOGUE_CSV_CANDIDATES:
    if os.path.isfile(p):
        try_load_catalogue_csv(p)

# ===== ENRICHMENT (winners) =====
market_winners = defaultdict(set)  # market_id -> set(selection_id)

# From CSV (optional)
if "is_winner" in df.columns or "result" in df.columns:
    for (mid, sid), g in df.groupby(["market_id", "selection_id"]):
        is_win = False
        if "is_winner" in g.columns:
            is_win = bool(g["is_winner"].astype(bool).max())
        elif "result" in g.columns:
            is_win = any(str(x).upper() == "WINNER" for x in g["result"].dropna().unique())
        if is_win:
            market_winners[str(mid)].add(int(sid))

# From Betfair API if needed
def api_enrich_missing(market_to_selids):
    try:
        import betfairlightweight as bflw
        from dotenv import load_dotenv
        load_dotenv()

        trading = bflw.APIClient(
            os.getenv("BETFAIR_USERNAME"),
            os.getenv("BETFAIR_PASSWORD"),
            app_key=os.getenv("BETFAIR_APP_KEY"),
            cert_files=("/root/betting/certs/client-2048.crt", "/root/betting/certs/client-2048.key"),
        )
        trading.login()

        # Names
        missing_markets = [
            mid for mid in market_to_selids
            if mid not in market_event_name
            or any(sel not in market_runner_name[mid] for sel in market_to_selids[mid])
        ]
        for mid in sorted(set(missing_markets)):
            try:
                cats = trading.betting.list_market_catalogue(
                    filter=bflw.filters.market_filter(market_ids=[mid]),
                    market_projection=["EVENT", "RUNNER_DESCRIPTION"],
                    max_results=1,
                )
                if not cats:
                    continue
                cat = cats[0]
                if getattr(cat, "event", None) and getattr(cat.event, "name", None):
                    market_event_name[mid] = cat.event.name
                if getattr(cat, "runners", None):
                    for ru in cat.runners:
                        market_runner_name[mid][int(ru.selection_id)] = ru.runner_name
            except Exception as e:
                print(f"⚠️ Enrich failed for {mid}: {e}")

        # Winners if unknown
        unknown_winners = [mid for mid in market_to_selids if not market_winners.get(mid)]
        if unknown_winners:
            books = trading.betting.list_market_book(market_ids=unknown_winners)
            for bk in books or []:
                mid = str(bk.market_id)
                try:
                    for rb in getattr(bk, "runners", []) or []:
                        if str(getattr(rb, "status", "")).upper() == "WINNER":
                            market_winners[mid].add(int(rb.selection_id))
                except Exception:
                    pass

        trading.logout()
    except Exception as e:
        print(f"ℹ️ Skipping Betfair API enrichment (winners/names): {e}")

market_to_selids = defaultdict(set)
for mid, sid in df.groupby(["market_id", "selection_id"]).groups.keys():
    market_to_selids[str(mid)].add(int(sid))

needs_api = (
    any(mid not in market_event_name or sid not in market_runner_name[mid]
        for (mid, sid) in df.groupby(["market_id", "selection_id"]).groups.keys())
    or any(not market_winners.get(str(mid)) for mid, _ in df.groupby(["market_id", "selection_id"]).groups.keys())
)
if needs_api:
    api_enrich_missing(market_to_selids)

# Write cache (names only)
try:
    os.makedirs(os.path.dirname(CACHE_JSON), exist_ok=True)
    with open(CACHE_JSON, "w") as f:
        json.dump(
            {
                "market_event_name": market_event_name,
                "market_runner_name": {k: {str(kk): vv for kk, vv in v.items()} for k, v in market_runner_name.items()},
            },
            f,
            indent=2,
        )
except Exception as e:
    print(f"ℹ️ Could not write cache: {e}")

# ===== PLOT =====
groups = list(df.groupby(["market_id", "selection_id"]))
n = len(groups)
if n == 0:
    raise SystemExit("No trades found to plot.")

ncols = 3 if n >= 9 else (2 if n >= 4 else 1)
nrows = math.ceil(n / ncols)
fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6*ncols, 3.8*nrows), squeeze=False)
axes = axes.flatten()

fig.suptitle(f"Trade Prices Over Time — {TODAY}", y=0.995)

# Prepare date locator/formatter once (consistent across all subplots)
major_locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
major_formatter = mdates.ConciseDateFormatter(major_locator)

# Decide if we’ll draw minor hour ticks, and at what interval
minor_hour_interval = None
if span_hours <= 900:  # avoid exploding to >1000 ticks
    # Aim for <= ~200 minor ticks across the figure
    target_minor_ticks = 200
    minor_hour_interval = max(1, int(math.ceil(span_hours / target_minor_ticks)))

for idx, ((mid, sid), g) in enumerate(groups):
    ax = axes[idx]
    backs = g[g["side"].str.upper() == "BACK"]
    lays  = g[g["side"].str.upper() == "LAY"]

    # Plot trades
    if not backs.empty:
        ax.scatter(backs["placed_date"], backs["avg_price_matched"], marker="^", color="green", alpha=0.8, label="BACK")
        ax.plot(backs["placed_date"], backs["avg_price_matched"], color="green", linestyle="--", alpha=0.5)
        for _, r in backs.iterrows():
            ax.text(r["placed_date"], r["avg_price_matched"], f"{r['size_matched']:.2f}",
                    fontsize=7, color="green", alpha=0.8, ha="left", va="bottom")

    if not lays.empty:
        ax.scatter(lays["placed_date"], lays["avg_price_matched"], marker="v", color="red", alpha=0.8, label="LAY")
        ax.plot(lays["placed_date"], lays["avg_price_matched"], color="red", linestyle="--", alpha=0.5)
        for _, r in lays.iterrows():
            ax.text(r["placed_date"], r["avg_price_matched"], f"{r['size_matched']:.2f}",
                    fontsize=7, color="red", alpha=0.8, ha="left", va="top")

    # Labels / title
    total_net = g["net_pnl"].sum() if "net_pnl" in g.columns else float("nan")
    trades_ct = len(g)
    mid_s, sid_i = str(mid), int(sid)
    event_nm  = market_event_name.get(mid_s, "Unknown Event")
    runner_nm = market_runner_name.get(mid_s, {}).get(sid_i, f"Selection {sid_i}")

    # Winner badge + color (only set color when result is known)
    is_winner = sid_i in market_winners.get(mid_s, set())
    know_result = bool(market_winners.get(mid_s))
    badge = "[WINNER]" if is_winner else ("[LOSER]" if know_result else "[RESULT?]")
    title_kwargs = {}
    if know_result:
        title_kwargs["color"] = "green" if is_winner else "red"

    # Standardised log scale and fixed limits
    ax.set_yscale("log")
    ax.set_ylim(0.9, 15)
    ax.set_ylabel("Avg Price Matched (log)")
    ax.set_xlabel("Placed Time")

    # X-axis formatting — shared major locator/formatter
    ax.xaxis.set_major_locator(major_locator)
    ax.xaxis.set_major_formatter(major_formatter)

    # Minor hour ticks only if interval decided (span small enough)
    if minor_hour_interval is not None:
        ax.xaxis.set_minor_locator(mdates.HourLocator(interval=minor_hour_interval))
        # Label minor ticks as HH:MM, but keep them small
        ax.xaxis.set_minor_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(axis="x", which="minor", labelsize=7, rotation=90)
    # Major tick styling
    ax.tick_params(axis="x", which="major", labelsize=8, pad=6)

    # Restrict x-limits to data range (helps suppress unnecessary ticks)
    ax.set_xlim(tmin, tmax)

    ax.grid(True, alpha=0.3, which="both")

    ax.set_title(
        f"{event_nm}\n{runner_nm} {badge}\nmarket_id={mid_s} (trades={trades_ct}, net_pnl={total_net:.2f})",
        fontsize=9, **title_kwargs
    )
    ax.legend(loc="best", fontsize=8)

# Hide unused subplots if any
for j in range(idx + 1, len(axes)):
    axes[j].set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.97])
fig.savefig(OUT_IMG, dpi=150)
print(f"✅ Saved figure: {OUT_IMG}")
