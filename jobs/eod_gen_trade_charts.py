#!/root/betting/.venv/bin/python
# plot_trades_grid.py — one big figure per (market_id, selection_id) with size labels
import os, json, math
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from collections import defaultdict

# ===== CONFIG =====
TODAY = datetime.utcnow().strftime("%Y-%m-%d")
CSV_PATH = f"/root/betting/store/reports/pnl_per_trade_{TODAY}.csv"
OUT_DIR = "/root/betting/store/reports/trade_charts"
OUT_IMG = os.path.join(OUT_DIR, f"trade_charts_{TODAY}.png")
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

# ===== ENRICHMENT =====
market_event_name = {}
market_runner_name = defaultdict(dict)

# From cache
if os.path.isfile(CACHE_JSON):
    try:
        with open(CACHE_JSON, "r") as f:
            cache = json.load(f)
        market_event_name.update(cache.get("market_event_name", {}))
        for mid, selmap in cache.get("market_runner_name", {}).items():
            market_runner_name[mid].update({int(k): v for k, v in selmap.items()})
    except Exception as e:
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

        trading.logout()
    except Exception as e:
        print(f"ℹ️ Skipping Betfair API enrichment: {e}")

market_to_selids = defaultdict(set)
for mid, sid in df.groupby(["market_id", "selection_id"]).groups.keys():
    market_to_selids[str(mid)].add(int(sid))

needs_api = any(
    mid not in market_event_name or sid not in market_runner_name[mid]
    for (mid, sid) in df.groupby(["market_id", "selection_id"]).groups.keys()
)
if needs_api:
    api_enrich_missing(market_to_selids)

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
fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6*ncols, 3.5*nrows), squeeze=False)
axes = axes.flatten()

fig.suptitle(f"Trade Prices Over Time — {TODAY}", y=0.995)

for idx, ((mid, sid), g) in enumerate(groups):
    ax = axes[idx]
    backs = g[g["side"].str.upper() == "BACK"]
    lays = g[g["side"].str.upper() == "LAY"]

    if not backs.empty:
        ax.scatter(backs["placed_date"], backs["avg_price_matched"], marker="^", color="green", alpha=0.8, label="BACK")
        ax.plot(backs["placed_date"], backs["avg_price_matched"], color="green", linestyle="--", alpha=0.5)
        # Add size labels
        for _, r in backs.iterrows():
            ax.text(
                r["placed_date"], r["avg_price_matched"],
                f"{r['size_matched']:.2f}",
                fontsize=7, color="green", alpha=0.8,
                ha="left", va="bottom"
            )

    if not lays.empty:
        ax.scatter(lays["placed_date"], lays["avg_price_matched"], marker="v", color="red", alpha=0.8, label="LAY")
        ax.plot(lays["placed_date"], lays["avg_price_matched"], color="red", linestyle="--", alpha=0.5)
        for _, r in lays.iterrows():
            ax.text(
                r["placed_date"], r["avg_price_matched"],
                f"{r['size_matched']:.2f}",
                fontsize=7, color="red", alpha=0.8,
                ha="left", va="top"
            )

    total_net = g["net_pnl"].sum() if "net_pnl" in g.columns else float("nan")
    trades_ct = len(g)
    mid_s, sid_i = str(mid), int(sid)
    event_nm  = market_event_name.get(mid_s, "Unknown Event")
    runner_nm = market_runner_name.get(mid_s, {}).get(sid_i, f"Selection {sid_i}")

    ax.set_title(f"{event_nm}\n{runner_nm} (trades={trades_ct}, net_pnl={total_net:.2f})", fontsize=9)
    ax.set_xlabel("Placed Time")
    ax.set_ylabel("Avg Price Matched")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8)

for j in range(idx + 1, len(axes)):
    axes[j].set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.97])
fig.savefig(OUT_IMG, dpi=150)
print(f"✅ Saved figure: {OUT_IMG}")
