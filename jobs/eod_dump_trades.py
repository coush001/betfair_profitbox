#!/root/betfair_profitbox/.venv/bin/python
# /root/betfair_profitbox/tools/strat_trades_report.py
"""
Rolling 7-day per-trade dump (Betfair):
- Settled bets → file dated by their settled_date (UTC)
- Open (matched/part-matched, not settled) bets → go into TODAY's file (UTC)
- Overwrites existing CSVs each run

Auth env:
  BETFAIR_USERNAME, BETFAIR_PASSWORD, BETFAIR_APP_KEY
Certs:
  /root/betfair_profitbox/certs/client-2048.crt
  /root/betfair_profitbox/certs/client-2048.key

Usage:
  cd /root/betfair_profitbox/tools/
  python strat_trades_report.py
  # Optional: --outdir /root/betfair_profitbox/store/trade_csv
Output:
  CSVs → /root/betfair_profitbox/store/trade_csv/YYYY-MM-DD.csv
"""
import os, sys, argparse, datetime as dt, warnings, shutil, tempfile
from collections import defaultdict
import pandas as pd
from dotenv import load_dotenv
from betfairlightweight import APIClient, filters

warnings.filterwarnings("ignore", category=FutureWarning)

# -------- Config --------
CERT_CRT = "/root/betfair_profitbox/certs/client-2048.crt"
CERT_KEY = "/root/betfair_profitbox/certs/client-2048.key"
DEFAULT_OUTDIR = "/root/betfair_profitbox/store/trade_csv"
ROLLING_DAYS = 7  # last N days window (inclusive of today)

# -------- Helpers --------
def utcnow():
    return dt.datetime.now(dt.timezone.utc)

def utc_bounds(from_dt: dt.datetime, to_dt: dt.datetime):
    """Clamp to seconds and ensure tz-aware UTC."""
    f = from_dt.astimezone(dt.timezone.utc).replace(microsecond=0)
    t = to_dt.astimezone(dt.timezone.utc).replace(microsecond=0)
    return f, t

def client_login():
    load_dotenv()
    try:
        user = os.environ["BETFAIR_USERNAME"]
        pwd  = os.environ["BETFAIR_PASSWORD"]
        appk = os.environ["BETFAIR_APP_KEY"]
    except KeyError as e:
        print(f"[ERROR] Missing env var: {e.args[0]}", file=sys.stderr)
        sys.exit(2)
    if not (os.path.isfile(CERT_CRT) and os.path.isfile(CERT_KEY)):
        print(f"[ERROR] Cert files not found: {CERT_CRT} / {CERT_KEY}", file=sys.stderr)
        sys.exit(2)
    c = APIClient(user, pwd, appk, cert_files=(CERT_CRT, CERT_KEY))
    c.login()
    return c

def fetch_settled(client, start_utc, end_utc) -> pd.DataFrame:
    """Official settled bets in [start, end]."""
    res = client.betting.list_cleared_orders(
        bet_status="SETTLED",
        settled_date_range=filters.time_range(from_=start_utc, to=end_utc),
        include_item_description=True,
    )
    rows = []
    for o in (getattr(res, "orders", None) or []):
        rows.append({
            "status": "SETTLED",
            "bet_id": getattr(o, "bet_id", None),
            "market_id": getattr(o, "market_id", None),
            "selection_id": getattr(o, "selection_id", None),
            "handicap": getattr(o, "handicap", 0.0),
            "side": getattr(o, "side", None),
            "avg_price_matched": getattr(o, "price_matched", None),
            "size_matched": getattr(o, "size_settled", 0.0) or 0.0,
            "size_remaining": 0.0,
            "placed_date": getattr(o, "placed_date", None),
            "settled_date": getattr(o, "settled_date", None),
            "customer_order_ref": getattr(o, "customer_order_ref", None),
            "customer_strategy_ref": getattr(o, "customer_strategy_ref", None),
            "gross_profit": getattr(o, "profit", 0.0) or 0.0,
            "commission": getattr(o, "commission", 0.0) or 0.0,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["net_pnl"] = df["gross_profit"] - df["commission"]
    return df

def _iter_current_orders(resp):
    if resp is None: return []
    if hasattr(resp, "current_orders") and resp.current_orders is not None:
        return resp.current_orders
    if hasattr(resp, "orders") and resp.orders is not None:
        return resp.orders
    if isinstance(resp, dict):
        return resp.get("currentOrders") or resp.get("orders") or []
    if isinstance(resp, (list, tuple)): return resp
    try:
        from betfairlightweight.resources.orders import CurrentOrder
        if isinstance(resp, CurrentOrder): return [resp]
    except Exception:
        pass
    return []

def fetch_current_matched(client, start_utc, end_utc) -> pd.DataFrame:
    """Matched/part-matched OPEN orders placed in [start, end]."""
    resp = client.betting.list_current_orders(
        date_range=filters.time_range(from_=start_utc, to=end_utc),
        order_projection="ALL",
    )
    rows = []
    for o in _iter_current_orders(resp):
        size_matched = getattr(o, "size_matched", 0.0) or 0.0
        if size_matched <= 0:
            continue
        avg_px = getattr(o, "average_price_matched", None) or getattr(o, "avg_price_matched", None)
        rows.append({
            "status": "OPEN",
            "bet_id": getattr(o, "bet_id", None),
            "market_id": getattr(o, "market_id", None),
            "selection_id": getattr(o, "selection_id", None),
            "handicap": getattr(o, "handicap", 0.0),
            "side": getattr(o, "side", None),
            "avg_price_matched": avg_px,
            "size_matched": size_matched,
            "size_remaining": getattr(o, "size_remaining", 0.0) or 0.0,
            "placed_date": getattr(o, "placed_date", None),
            "settled_date": None,
            "customer_order_ref": getattr(o, "customer_order_ref", None),
            "customer_strategy_ref": getattr(o, "customer_strategy_ref", None),
            "gross_profit": None,
            "commission": None,
            "net_pnl": None,
        })
    return pd.DataFrame(rows)

def market_price_map(client, market_ids):
    """Best opposing prices + LTP per (market_id, selection_id)."""
    out = defaultdict(dict)
    if not market_ids: return out
    price_proj = filters.price_projection(price_data=["EX_BEST_OFFERS", "EX_TRADED"])
    MID_CHUNK = 20
    for i in range(0, len(market_ids), MID_CHUNK):
        mids = market_ids[i:i+MID_CHUNK]
        books = client.betting.list_market_book(market_ids=mids, price_projection=price_proj)
        for mb in (books or []):
            mid = mb.market_id
            for r in (mb.runners or []):
                sel = r.selection_id
                last = getattr(r, "last_price_traded", None)
                ex = getattr(r, "ex", None)
                backs = getattr(ex, "available_to_back", []) or []
                lays  = getattr(ex, "available_to_lay", []) or []
                out[mid][sel] = {
                    "ltp": last,
                    "best_back": backs[0].price if backs else None,
                    "best_lay":  lays[0].price  if lays  else None,
                }
    return out

def green_mtm(side, stake, avg_price, opp_price):
    if not opp_price or not avg_price:
        return None
    if side == "BACK":
        return stake * (avg_price / opp_price - 1.0)
    if side == "LAY":
        return stake * (1.0 - avg_price / opp_price)
    return None

def write_csv_atomic(df: pd.DataFrame, path: str):
    """Overwrite atomically (write to tmp, then rename)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    d = os.path.dirname(path)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=d, prefix=".tmp_", suffix=".csv") as tmp:
        df.to_csv(tmp.name, index=False)
        tmp_path = tmp.name
    os.replace(tmp_path, path)

# -------- Main --------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default=DEFAULT_OUTDIR, help=f"output dir (default={DEFAULT_OUTDIR})")
    ap.add_argument("--days", type=int, default=ROLLING_DAYS, help=f"rolling window in days (default={ROLLING_DAYS})")
    args = ap.parse_args()

    now = utcnow()
    window_start = (now - dt.timedelta(days=args.days)).replace(microsecond=0)
    window_end   = now.replace(microsecond=0)
    start_utc, end_utc = utc_bounds(window_start, window_end)
    today_date = now.date()

    client = client_login()

    # 1) Fetch data
    df_set = fetch_settled(client, start_utc, end_utc)
    df_open = fetch_current_matched(client, start_utc, end_utc)

    # 2) MTM for OPEN using best opposing price (fallback LTP)
    if df_open is not None and not df_open.empty:
        mkt_ids = sorted(set(df_open["market_id"]))
        pxmap = market_price_map(client, mkt_ids)
        mtms = []
        for _, row in df_open.iterrows():
            side = row["side"]
            mid  = row["market_id"]
            sel  = int(row["selection_id"])
            avg  = row["avg_price_matched"]
            px   = pxmap.get(mid, {}).get(sel, {})
            if side == "BACK":
                opp = px.get("best_lay") or px.get("ltp")
                mtm = green_mtm("BACK", row["size_matched"], avg, opp)
            else:
                opp = px.get("best_back") or px.get("ltp")
                mtm = green_mtm("LAY", row["size_matched"], avg, opp)
            mtms.append(mtm)
        df_open["mtm_green_pnl"] = mtms

    # 3) Harmonise columns
    wanted = [
        "status","bet_id","market_id","selection_id","handicap","side",
        "avg_price_matched","size_matched","size_remaining",
        "gross_profit","commission","net_pnl","mtm_green_pnl",
        "placed_date","settled_date","customer_order_ref","customer_strategy_ref",
    ]
    def ensure_cols(df):
        if df is None: return None
        for c in wanted:
            if c not in df.columns: df[c] = None
        return df[wanted]

    frames = []
    if df_set is not None and not df_set.empty: frames.append(ensure_cols(df_set))
    if df_open is not None and not df_open.empty: frames.append(ensure_cols(df_open))
    df_all = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=wanted)

    # 4) Parse dates & assign target file date
    if not df_all.empty:
        df_all["placed_date"]  = pd.to_datetime(df_all["placed_date"], utc=True, errors="coerce")
        df_all["settled_date"] = pd.to_datetime(df_all["settled_date"], utc=True, errors="coerce")

        # target_date: settled → settled_date.date ; open → today_date
        target_dates = []
        for _, r in df_all.iterrows():
            if r["status"] == "SETTLED" and pd.notna(r["settled_date"]):
                target_dates.append(r["settled_date"].date())
            else:
                target_dates.append(today_date)
        df_all["target_date"] = target_dates

        # 5) Write one CSV per target_date (overwrite)
        total_rows = 0
        for d in sorted(df_all["target_date"].unique()):
            out_path = os.path.join(args.outdir, f"{d.isoformat()}.csv")
            df_day = df_all[df_all["target_date"] == d].drop(columns=["target_date"]).reset_index(drop=True)
            write_csv_atomic(df_day, out_path)
            print(f"Wrote {len(df_day)} rows → {out_path}")
            total_rows += len(df_day)

        # Optional console summary
        df_all["_pnl_combined"] = df_all["net_pnl"].fillna(0.0) + df_all["mtm_green_pnl"].fillna(0.0)
        total_bets = df_all["bet_id"].nunique()
        total_matched = float(df_all["size_matched"].fillna(0.0).sum())
        total_pnl = float(df_all["_pnl_combined"].fillna(0.0).sum())
        print(f"\nWINDOW {start_utc.isoformat()} → {end_utc.isoformat()}")
        print(f"TOTAL rows: {total_rows:,} | bets: {total_bets:,} | matched: {total_matched:.2f} | pnl: {total_pnl:+.2f}")
    else:
        print("No trades found in window.")

    client.logout()

if __name__ == "__main__":
    main()
