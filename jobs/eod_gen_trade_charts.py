#!/root/betfair_profitbox/.venv/bin/python
# plot_trades_grid.py — plot (market_id, selection_id) that settled today; include all their trades from last 7d
# Names populated via Betfair API **list_cleared_orders(includeItemDescription=True)** with very verbose logging.
# Winners populated via Betfair API **list_market_book** (runner.status == "WINNER").

import os, json, math, tempfile, time, sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# ========== CONFIG ==========
TODAY_DT = datetime.now(timezone.utc).date()
TODAY = TODAY_DT.strftime("%Y-%m-%d")

CSV_DIR = "/root/betfair_profitbox/store/trade_csv"
OUT_DIR = "/root/betfair_profitbox/store/trade_chart"
OUT_IMG = os.path.join(OUT_DIR, f"{TODAY}.png")

CACHE_JSON = "/root/betfair_profitbox/store/cache/market_selection_names.json"
LOOKBACK_DAYS = 7  # scan last N days of CSVs for trades

VERBOSE = True
os.makedirs(OUT_DIR, exist_ok=True)

# ========== LOGGING ==========
def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S%z")
    print(f"[{ts}] {msg}", flush=True)

def norm_mid(x):
    """Normalize market_id to exact string (avoid float/scientific notation issues)."""
    return (str(x).strip() if x is not None else "")

# ========== LOAD TRADES (last 7d) ==========
def list_last_nday_csvs(base_dir, n=7, ref_date=TODAY_DT):
    files = []
    for d in range(n):
        day = (ref_date - timedelta(days=d)).strftime("%Y-%m-%d")
        p = os.path.join(base_dir, f"{day}.csv")
        if os.path.isfile(p):
            files.append(p)
    return sorted(files)

log(f"=== Start trade plot generation for {TODAY} ===")
log(f"Scanning trade CSVs (last {LOOKBACK_DAYS} days) in: {CSV_DIR}")
csv_files = list_last_nday_csvs(CSV_DIR, LOOKBACK_DAYS, TODAY_DT)
if not csv_files:
    raise SystemExit("No trade CSVs found in last 7 days.")
log(f"Found CSVs: {', '.join(os.path.basename(x) for x in csv_files)}")

dfs, total_rows = [], 0
for p in csv_files:
    try:
        log(f"Reading CSV → {p}")
        dfp = pd.read_csv(
            p,
            dtype={"market_id": "string"},
            converters={"selection_id": lambda x: int(float(x)) if x not in (None, "", "nan", "NaN") else None},
        )
        log(f"  rows={len(dfp)}")
        dfs.append(dfp); total_rows += len(dfp)
    except Exception as e:
        log(f"⚠️ Skipped {p}: {e}")

if not dfs:
    raise SystemExit("No readable trade CSVs in last 7 days.")
df_all = pd.concat(dfs, ignore_index=True)
log(f"Total rows across CSVs: {len(df_all)}")

# Parse dates
for col in ("placed_date", "settled_date"):
    if col in df_all.columns:
        log(f"Parsing datetimes for column: {col}")
        df_all[col] = pd.to_datetime(df_all[col], errors="coerce", utc=True)

if "settled_date" not in df_all.columns:
    raise SystemExit("No 'settled_date' column in trade CSVs.")

df_settled_today = df_all.dropna(subset=["settled_date"]).copy()
df_settled_today["settled_day"] = df_settled_today["settled_date"].dt.date
n_settled_today = int((df_settled_today["settled_day"] == TODAY_DT).sum())
log(f"Rows with settled_date == {TODAY}: {n_settled_today}")

pairs_today = {
    (norm_mid(r["market_id"]), int(r["selection_id"]))
    for _, r in df_settled_today.iterrows()
    if r["settled_day"] == TODAY_DT and pd.notna(r.get("market_id")) and pd.notna(r.get("selection_id"))
}
log(f"Unique (market_id, selection_id) pairs settled today: {len(pairs_today)}")

if not pairs_today:
    raise SystemExit("No trades settled today; nothing to plot.")

log("Filtering ALL trades for those pairs (any placed_date)...")
mask = df_all.apply(
    lambda r: (
        pd.notna(r.get("market_id")) and pd.notna(r.get("selection_id"))
        and (norm_mid(r["market_id"]), int(r["selection_id"])) in pairs_today
    ),
    axis=1,
)
df = df_all[mask].copy()
log(f"Rows after filter: {len(df)}")

if "placed_date" not in df.columns:
    raise SystemExit("No 'placed_date' column in trade CSVs.")
df["market_id"] = df["market_id"].apply(norm_mid)
df = df.dropna(subset=["placed_date"]).sort_values("placed_date")
log(f"Rows after dropna(placed_date) & sort: {len(df)}")
if df.empty:
    raise SystemExit("Filtered dataframe is empty after selecting pairs that settled today.")

# Time span (for ticks)
tmin = df["placed_date"].min(); tmax = df["placed_date"].max()
span_seconds = max(1.0, (tmax - tmin).total_seconds()); span_hours = span_seconds / 3600.0
log(f"Time span: {tmin} → {tmax} ({span_hours:.2f} hours)")

# ========== NAME ENRICHMENT (BETFAIR API via list_cleared_orders) ==========
market_event_name = {}                 # market_id -> market/marketDesc (title)
market_runner_name = defaultdict(dict) # market_id -> {selection_id: selectionDesc}
market_winners = defaultdict(set)      # market_id -> set(selection_id) winners (filled via MarketBook)

# Seed from df if present (cheap wins)
if {"market_id", "selection_id", "runner_name"}.issubset(df.columns):
    log("Seeding runner names from 'runner_name' in trades.")
    for _, r in df[["market_id", "selection_id", "runner_name"]].dropna().iterrows():
        market_runner_name[norm_mid(r["market_id"])][int(r["selection_id"])] = str(r["runner_name"])
elif {"market_id", "selection_id", "selection_name"}.issubset(df.columns):
    log("Seeding runner names from 'selection_name' in trades.")
    for _, r in df[["market_id", "selection_id", "selection_name"]].dropna().iterrows():
        market_runner_name[norm_mid(r["market_id"])][int(r["selection_id"])] = str(r["selection_name"])

for col_ev in ("event_name", "eventName", "market_name", "marketDesc"):
    if col_ev in df.columns:
        log(f"Seeding market names from '{col_ev}' in trades.")
        for _, r in df[["market_id", col_ev]].dropna().iterrows():
            market_event_name[norm_mid(r["market_id"])] = str(r[col_ev])

# Cache read (optional)
def load_cache(path):
    if not os.path.isfile(path) or os.path.getsize(path) == 0:
        return None
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        try:
            bad = path + ".bad"
            os.replace(path, bad)
            log(f"Cache corrupt → moved to {bad}")
        except Exception:
            pass
        return None

log(f"Loading cache (if any): {CACHE_JSON}")
cache = load_cache(CACHE_JSON)
if cache:
    log("Cache loaded; merging cached names.")
    market_event_name.update(cache.get("market_event_name", {}))
    for mid, selmap in cache.get("market_runner_name", {}).items():
        market_runner_name[mid].update({int(k): v for k, v in selmap.items()})
else:
    log("No valid cache found.")

# Targets for enrichment
markets_to_plot = sorted({norm_mid(mid) for mid, _ in df.groupby(["market_id", "selection_id"]).groups.keys()})
log(f"Markets needing potential enrichment: {len(markets_to_plot)} (sample: {markets_to_plot[:5]})")

def api_fill_names_from_cleared_orders(market_to_selids, lookback_days=7):
    """
    Pull market & runner names for CLOSED (settled) markets using list_cleared_orders
    with includeItemDescription=True. We page via from_record until moreAvailable=False.
    """
    try:
        import betfairlightweight as bflw
        from betfairlightweight import filters
        from dotenv import load_dotenv
        load_dotenv()

        user = os.getenv("BETFAIR_USERNAME")
        pwd  = os.getenv("BETFAIR_PASSWORD")
        appk = os.getenv("BETFAIR_APP_KEY")
        if not (user and pwd and appk):
            log("❌ Missing BETFAIR_* env vars. Skipping API enrichment.")
            return

        log("Logging in to Betfair API for cleared orders (cert files expected in /root/betfair_profitbox/certs/...)")
        trading = bflw.APIClient(
            user, pwd, appk,
            cert_files=("/root/betfair_profitbox/certs/client-2048.crt", "/root/betfair_profitbox/certs/client-2048.key"),
        )
        trading.login()
        log("✅ Betfair REST login OK (cleared orders)")

        # Build settled window (UTC) for last N days up to end of today
        end_utc = datetime.combine(TODAY_DT, datetime.max.time()).replace(tzinfo=timezone.utc)
        start_utc = end_utc - timedelta(days=lookback_days-1)
        log(f"Settled window UTC: {start_utc.isoformat()} → {end_utc.isoformat()}")

        need_mids = [m for m in market_to_selids.keys()
                     if (m not in market_event_name) or any(sel not in market_runner_name[m] for sel in market_to_selids[m])]
        log(f"Markets missing names before cleared-orders fetch: {len(need_mids)}")

        if not need_mids:
            trading.logout(); log("Nothing missing; skipping cleared-orders."); return

        # Fetch in slices of marketIds to respect payload size (50 per slice is safe)
        SLICE = 50
        total_orders_seen = 0
        for i in range(0, len(need_mids), SLICE):
            mids_slice = need_mids[i:i+SLICE]
            log(f"ClearedOrders slice {i//SLICE+1}: {len(mids_slice)} marketIds")
            from_record = 0
            while True:
                try:
                    res = trading.betting.list_cleared_orders(
                        bet_status="SETTLED",
                        market_ids=mids_slice,
                        settled_date_range=filters.time_range(from_=start_utc, to=end_utc),
                        include_item_description=True,
                        from_record=from_record,
                    )
                except Exception as e:
                    log(f"  !! list_cleared_orders failed (from_record={from_record}): {e}")
                    break

                orders = getattr(res, "orders", None) or []
                more  = bool(getattr(res, "more_available", False))
                log(f"  -> got {len(orders)} orders, more_available={more}, from_record={from_record}")
                total_orders_seen += len(orders)

                # Build maps from itemDescription
                for o in orders:
                    mid = norm_mid(getattr(o, "market_id", None))
                    if not mid:
                        continue
                    item = getattr(o, "item_description", None)
                    if not item:
                        continue
                    # Market/runner descriptions
                    mdesc = getattr(item, "market_desc", None) or getattr(item, "marketName", None)
                    sdesc = getattr(item, "selection_desc", None) or getattr(item, "selectionName", None)
                    selid = getattr(o, "selection_id", None)
                    if mdesc:
                        if mid not in market_event_name:
                            market_event_name[mid] = str(mdesc)
                    if selid is not None and sdesc:
                        sid = int(selid)
                        if sid not in market_runner_name[mid]:
                            market_runner_name[mid][sid] = str(sdesc)

                if more:
                    from_record += len(orders)
                    time.sleep(0.12)
                else:
                    break
            time.sleep(0.15)

        log(f"Cleared-orders processed; total orders seen: {total_orders_seen}")
        trading.logout()
        log("Logged out of Betfair API (cleared orders).")
    except Exception as e:
        log(f"❌ Cleared-orders enrichment aborted: {e}")

def api_mark_winners_via_marketbook(market_ids):
    """
    Mark winners for given market_ids using list_market_book.
    Any runner with runner.status == 'WINNER' is recorded.
    """
    if not market_ids:
        return
    try:
        import betfairlightweight as bflw
        from dotenv import load_dotenv
        load_dotenv()
        user = os.getenv("BETFAIR_USERNAME")
        pwd  = os.getenv("BETFAIR_PASSWORD")
        appk = os.getenv("BETFAIR_APP_KEY")
        if not (user and pwd and appk):
            log("❌ Missing BETFAIR_* env vars. Skipping MarketBook winners.")
            return

        log("Logging in to Betfair API for MarketBook winners...")
        trading = bflw.APIClient(
            user, pwd, appk,
            cert_files=("/root/betfair_profitbox/certs/client-2048.crt", "/root/betfair_profitbox/certs/client-2048.key"),
        )
        trading.login()
        log("✅ Betfair REST login OK (market book)")

        CHUNK = 25
        found_markets = 0
        for i in range(0, len(market_ids), CHUNK):
            mids = market_ids[i:i+CHUNK]
            try:
                books = trading.betting.list_market_book(market_ids=mids) or []
            except Exception as e:
                log(f"  !! list_market_book failed for chunk starting {i}: {e}")
                books = []

            for bk in books:
                mid = norm_mid(getattr(bk, "market_id", None))
                winners_here = set()
                try:
                    for rb in getattr(bk, "runners", []) or []:
                        status = str(getattr(rb, "status", "")).upper()
                        selid = getattr(rb, "selection_id", None)
                        if status == "WINNER" and selid is not None:
                            winners_here.add(int(selid))
                    if winners_here:
                        market_winners[mid] = winners_here
                        found_markets += 1
                        log(f"  WINNERS: market {mid} -> {sorted(list(winners_here))}")
                except Exception:
                    pass
            time.sleep(0.15)

        log(f"MarketBook winners marked for {found_markets} / {len(market_ids)} markets.")
        trading.logout()
        log("Logged out of Betfair API (market book).")
    except Exception as e:
        log(f"❌ MarketBook winners aborted: {e}")

# Build market_to_selids from df
market_to_selids = defaultdict(set)
for mid, sid in df.groupby(["market_id", "selection_id"]).groups.keys():
    market_to_selids[norm_mid(mid)].add(int(sid))

log("Starting API enrichment for names via cleared orders...")
api_fill_names_from_cleared_orders(market_to_selids, lookback_days=LOOKBACK_DAYS)

# Add winners via MarketBook
log("Fetching winners via MarketBook…")
api_mark_winners_via_marketbook(sorted(market_to_selids.keys()))

# Print summary of names found
have_evt = sum(1 for m in market_to_selids if m in market_event_name)
have_any_runner = sum(1 for m in market_to_selids if market_runner_name.get(m))
log(f"Name summary → markets with titles: {have_evt}/{len(market_to_selids)}, markets with any runner name: {have_any_runner}/{len(market_to_selids)}")

# Print each market with name + winners
for mid in sorted(market_to_selids.keys()):
    mname = market_event_name.get(mid)
    rcnt = len(market_runner_name.get(mid, {}))
    wins = sorted(list(market_winners.get(mid, set())))
    print(f"MARKET SUMMARY: {mid} | name='{mname or 'UNKNOWN'}' | runner_names={rcnt} | winners={wins or 'UNKNOWN'}")

# Cache write (atomic)
log("Writing cache (atomic).")
try:
    os.makedirs(os.path.dirname(CACHE_JSON), exist_ok=True)
    to_dump = {
        "market_event_name": market_event_name,
        "market_runner_name": {m: {str(s): n for s, n in selmap.items()} for m, selmap in market_runner_name.items()},
    }
    with tempfile.NamedTemporaryFile("w", delete=False, dir=os.path.dirname(CACHE_JSON), prefix=".cache_", suffix=".json") as tmp:
        json.dump(to_dump, tmp, indent=2)
        tmp_path = tmp.name
    os.replace(tmp_path, CACHE_JSON)
    log(f"Cache written: {CACHE_JSON}")
except Exception as e:
    log(f"Cache write failed: {e}")

# ========== PLOT ==========
groups = list(df.groupby(["market_id", "selection_id"]))
n = len(groups)
log(f"Preparing plot panels: {n}")
if n == 0:
    raise SystemExit("No qualifying trades to plot.")

ncols = 3 if n >= 9 else (2 if n >= 4 else 1)
nrows = math.ceil(n / ncols)
fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6*ncols, 3.8*nrows), squeeze=False)
axes = axes.flatten()

fig.suptitle(f"Trades for Runners Settled on {TODAY}", y=0.995)

major_locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
major_formatter = mdates.ConciseDateFormatter(major_locator)

minor_hour_interval = None
if span_hours <= 900:
    target_minor_ticks = 200
    minor_hour_interval = max(1, int(math.ceil(span_hours / target_minor_ticks)))

for idx, ((mid, sid), g) in enumerate(groups):
    ax = axes[idx]
    backs = g[g["side"].str.upper() == "BACK"]
    lays  = g[g["side"].str.upper() == "LAY"]

    if not backs.empty:
        ax.scatter(backs["placed_date"], backs["avg_price_matched"], marker="^", label="BACK", alpha=0.8)
        ax.plot(backs["placed_date"], backs["avg_price_matched"], linestyle="--", alpha=0.5)
        for _, r in backs.iterrows():
            ax.text(r["placed_date"], r["avg_price_matched"], f"{r['size_matched']:.2f}",
                    fontsize=7, alpha=0.8, ha="left", va="bottom")

    if not lays.empty:
        ax.scatter(lays["placed_date"], lays["avg_price_matched"], marker="v", label="LAY", alpha=0.8)
        ax.plot(lays["placed_date"], lays["avg_price_matched"], linestyle="--", alpha=0.5)
        for _, r in lays.iterrows():
            ax.text(r["placed_date"], r["avg_price_matched"], f"{r['size_matched']:.2f}",
                    fontsize=7, alpha=0.8, ha="left", va="top")

    total_net = g["net_pnl"].sum() if "net_pnl" in g.columns else float("nan")
    trades_ct = len(g)
    mid_s, sid_i = norm_mid(mid), int(sid)
    event_nm  = market_event_name.get(mid_s, "Unknown Event")
    runner_nm = market_runner_name.get(mid_s, {}).get(sid_i, f"Selection {sid_i}")

    # Winner badge & color
    winners = market_winners.get(mid_s, set())
    know_result = bool(winners)
    is_winner = sid_i in winners
    badge = "[WINNER]" if is_winner else ("[LOSER]" if know_result else "[RESULT?]")
    title_kwargs = {}
    if know_result:
        title_kwargs["color"] = "green" if is_winner else "red"

    ax.set_yscale("log")
    ax.set_ylim(0.9, 15)
    ax.set_ylabel("Avg Price Matched (log)")
    ax.set_xlabel("Placed Time")

    ax.xaxis.set_major_locator(major_locator)
    ax.xaxis.set_major_formatter(major_formatter)
    if minor_hour_interval is not None:
        ax.xaxis.set_minor_locator(mdates.HourLocator(interval=minor_hour_interval))
        ax.xaxis.set_minor_formatter(mdates.DateFormatter("%H:%M"))
        ax.tick_params(axis="x", which="minor", labelsize=7, rotation=90)
    ax.tick_params(axis="x", which="major", labelsize=8, pad=6)

    ax.set_xlim(tmin, tmax)
    ax.grid(True, alpha=0.3, which="both")

    ax.set_title(
        f"{event_nm}\n{runner_nm} {badge}\nmarket_id={mid_s} (trades={trades_ct}, net_pnl={total_net:.2f})",
        fontsize=9, **title_kwargs
    )
    ax.legend(loc="best", fontsize=8)

# Hide unused axes
for j in range(idx + 1, len(axes)):
    axes[j].set_visible(False)

plt.tight_layout(rect=[0, 0, 1, 0.97])
fig.savefig(OUT_IMG, dpi=150)
log(f"Saved figure → {OUT_IMG}")
print(f"✅ Saved figure: {OUT_IMG}")
