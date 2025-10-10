#!/root/betting/.venv/bin/python
# /root/betting/tools_project/strat_trades_report.py
"""
End-of-day per-trade report (Betfair):
- Settled bets => official net PnL (profit - commission)
- Unsettled (matched/part-matched) bets placed today => MTM "green" PnL using best opposing price

Auth env:
  BETFAIR_USERNAME
  BETFAIR_PASSWORD
  BETFAIR_APP_KEY

Certs (fixed paths):
  /root/betting/certs/client-2048.crt
  /root/betting/certs/client-2048.key

Usage:
  cd /root/betting/tools_project/
  python strat_trades_report.py --date 2025-10-08
  # If --date omitted, uses TODAY (UTC)

Output:
  CSV → /root/betting/store/reports/pnl_per_trade_YYYY-MM-DD.csv
  Plus a per-strategy summary printed to stdout.
"""
import os
import sys
import argparse
import datetime as dt
from collections import defaultdict
from dotenv import load_dotenv
load_dotenv()
import pandas as pd
from betfairlightweight import APIClient, filters
import warnings
warnings.filterwarnings("ignore", category=FutureWarning) #, message=".*.DataFrame concatenation*")

# ---------------- Config ----------------
CERT_CRT = "/root/betting/certs/client-2048.crt"
CERT_KEY = "/root/betting/certs/client-2048.key"
DEFAULT_OUTDIR = "/root/betting/store/reports"


# ---------------- Helpers ----------------
def utc_bounds(day: dt.date):
    start = dt.datetime.combine(day, dt.time(0, 0, 0, tzinfo=dt.timezone.utc))
    end = dt.datetime.combine(day, dt.time(23, 59, 59, tzinfo=dt.timezone.utc))
    return start, end


def client_login():
    """Login using env vars and fixed cert paths."""
    try:
        user = os.environ["BETFAIR_USERNAME"]
        pwd = os.environ["BETFAIR_PASSWORD"]
        appk = os.environ["BETFAIR_APP_KEY"]
    except KeyError as e:
        missing = e.args[0]
        print(f"[ERROR] Missing environment variable: {missing}", file=sys.stderr)
        sys.exit(2)

    if not (os.path.isfile(CERT_CRT) and os.path.isfile(CERT_KEY)):
        print(f"[ERROR] Cert files not found at {CERT_CRT} / {CERT_KEY}", file=sys.stderr)
        sys.exit(2)

    # <- per your request: use cert_files=(...)
    client = APIClient(user, pwd, appk, cert_files=(CERT_CRT, CERT_KEY))
    client.login()
    return client


def fetch_settled(client, start_utc, end_utc) -> pd.DataFrame:
    """Official settled bets & PnL for the window."""
    res = client.betting.list_cleared_orders(
        bet_status="SETTLED",
        settled_date_range=filters.time_range(from_=start_utc, to=end_utc),
        include_item_description=True,
    )
    rows = []
    for o in (getattr(res, "orders", None) or []):
        rows.append(
            {
                "status": "SETTLED",
                "bet_id": getattr(o, "bet_id", None),
                "market_id": getattr(o, "market_id", None),
                "selection_id": getattr(o, "selection_id", None),
                "handicap": getattr(o, "handicap", 0.0),
                "side": getattr(o, "side", None),  # "BACK"/"LAY"
                "avg_price_matched": getattr(o, "price_matched", None),
                "size_matched": getattr(o, "size_settled", 0.0) or 0.0,
                "size_remaining": 0.0,
                "placed_date": getattr(o, "placed_date", None),
                "settled_date": getattr(o, "settled_date", None),
                "customer_order_ref": getattr(o, "customer_order_ref", None),
                "customer_strategy_ref": getattr(o, "customer_strategy_ref", None),
                "gross_profit": getattr(o, "profit", 0.0) or 0.0,
                "commission": getattr(o, "commission", 0.0) or 0.0,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df["net_pnl"] = df["gross_profit"] - df["commission"]
    return df


def _iter_current_orders(resp):
    """
    Normalise list_current_orders(...) result across betfairlightweight versions.
    Returns an iterable of CurrentOrder-like objects.
    """
    if resp is None:
        return []
    # Common attribute
    if hasattr(resp, "current_orders") and resp.current_orders is not None:
        return resp.current_orders
    # Some versions use 'orders'
    if hasattr(resp, "orders") and resp.orders is not None:
        return resp.orders
    # Dict-like
    if isinstance(resp, dict):
        return resp.get("currentOrders") or resp.get("orders") or []
    # Already a list/tuple
    if isinstance(resp, (list, tuple)):
        return resp
    # Single object fallback
    try:
        from betfairlightweight.resources.orders import CurrentOrder
        if isinstance(resp, CurrentOrder):
            return [resp]
    except Exception:
        pass
    return []


def fetch_current_matched_today(client, start_utc, end_utc) -> pd.DataFrame:
    """
    Matched/part-matched (unsettled) orders placed today.
    """
    resp = client.betting.list_current_orders(
        date_range=filters.time_range(from_=start_utc, to=end_utc),
        order_projection="ALL",
    )

    rows = []
    for o in _iter_current_orders(resp):
        size_matched = getattr(o, "size_matched", 0.0) or 0.0
        if size_matched <= 0:
            continue  # skip pure-unmatched

        # average price naming differs across versions
        avg_px = getattr(o, "average_price_matched", None)
        if avg_px is None:
            avg_px = getattr(o, "avg_price_matched", None)

        rows.append(
            {
                "status": "OPEN",
                "bet_id": getattr(o, "bet_id", None),
                "market_id": getattr(o, "market_id", None),
                "selection_id": getattr(o, "selection_id", None),
                "handicap": getattr(o, "handicap", 0.0),
                "side": getattr(o, "side", None),  # "BACK"/"LAY"
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
            }
        )
    return pd.DataFrame(rows)


def market_price_map(client, market_ids):
    """Return best opposing prices and LTP for each (market_id, selection_id)."""
    out = defaultdict(dict)
    if not market_ids:
        return out
    price_proj = filters.price_projection(price_data=["EX_BEST_OFFERS", "EX_TRADED"])
    MID_CHUNK = 20
    for i in range(0, len(market_ids), MID_CHUNK):
        mids = market_ids[i : i + MID_CHUNK]
        books = client.betting.list_market_book(
            market_ids=mids, price_projection=price_proj
        )
        for mb in (books or []):
            mid = mb.market_id
            for r in (mb.runners or []):
                sel = r.selection_id
                last = getattr(r, "last_price_traded", None)
                ex = getattr(r, "ex", None)
                best_backs = getattr(ex, "available_to_back", []) or []
                best_lays = getattr(ex, "available_to_lay", []) or []
                best_back = best_backs[0].price if best_backs else None
                best_lay = best_lays[0].price if best_lays else None
                out[mid][sel] = {"ltp": last, "best_back": best_back, "best_lay": best_lay}
    return out


def green_mtm(side, stake, avg_price, opp_price):
    """
    Cash-out (green) PnL if hedged at opp_price.
      BACK @ b with stake s, hedge LAY @ L -> green = s*(b/L - 1)
      LAY  @ y with stake s, hedge BACK @ B -> green = s*(1 - y/B)
    Returns None if opp_price/avg_price missing.
    """
    if not opp_price or not avg_price:
        return None
    if side == "BACK":
        return stake * (avg_price / opp_price - 1.0)
    if side == "LAY":
        return stake * (1.0 - avg_price / opp_price)
    return None


# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", help="UTC date YYYY-MM-DD (default=today UTC)")
    ap.add_argument(
        "--outdir",
        default=DEFAULT_OUTDIR,
        help=f"output directory (default={DEFAULT_OUTDIR})",
    )
    args = ap.parse_args()

    # Date window (UTC)
    if args.date:
        day = dt.date.fromisoformat(args.date)
    else:
        day = dt.datetime.now(dt.timezone.utc).date()
    start_utc, end_utc = utc_bounds(day)

    os.makedirs(args.outdir, exist_ok=True)
    out_csv = os.path.join(args.outdir, f"pnl_per_trade_{day.isoformat()}.csv")

    client = client_login()

    # 1) Settled PnL
    df_set = fetch_settled(client, start_utc, end_utc)

    # 2) Open (matched/part-matched) today
    df_open = fetch_current_matched_today(client, start_utc, end_utc)

    # 3) MTM 'green' for OPEN using best opposing price (fallback LTP)
    if df_open is not None and not df_open.empty:
        mkt_ids = sorted(set(df_open["market_id"]))
        pxmap = market_price_map(client, mkt_ids)

        mtms = []
        for _, row in df_open.iterrows():
            side = row["side"]
            mid = row["market_id"]
            sel = int(row["selection_id"])
            avg = row["avg_price_matched"]
            px = pxmap.get(mid, {}).get(sel, {})
            if side == "BACK":
                opp = px.get("best_lay") or px.get("ltp")
                mtm = green_mtm("BACK", row["size_matched"], avg, opp)
            else:  # LAY
                opp = px.get("best_back") or px.get("ltp")
                mtm = green_mtm("LAY", row["size_matched"], avg, opp)
            mtms.append(mtm)
        df_open["mtm_green_pnl"] = mtms

    # 4) Harmonise columns and combine
    wanted = [
        "status",
        "bet_id",
        "market_id",
        "selection_id",
        "handicap",
        "side",
        "avg_price_matched",
        "size_matched",
        "size_remaining",
        "gross_profit",
        "commission",
        "net_pnl",
        "mtm_green_pnl",
        "placed_date",
        "settled_date",
        "customer_order_ref",
        "customer_strategy_ref",
    ]

    def ensure_cols(df: pd.DataFrame):
        if df is None:
            return None
        for c in wanted:
            if c not in df.columns:
                df[c] = None
        return df[wanted]

    frames = []
    if df_set is not None and not df_set.empty:
        frames.append(ensure_cols(df_set))
    if df_open is not None and not df_open.empty:
        frames.append(ensure_cols(df_open))

    if frames:
        df = pd.concat(frames, ignore_index=True)
    else:
        df = pd.DataFrame(columns=wanted)

    # 5) Write CSV
    df.to_csv(out_csv, index=False)
    print(f"Wrote {len(df)} rows → {out_csv}")

    # 6) Quick per-strategy summary (settled net + MTM for open)
    # 6) Quick per-strategy summary (settled net + MTM for open) + GRAND TOTALS
    if not df.empty:
        df["_pnl"] = df["net_pnl"].fillna(0.0) + df["mtm_green_pnl"].fillna(0.0)

        summ = (
            df.groupby(["customer_strategy_ref", "status"], dropna=False)
            .agg(
                bets=("bet_id", "nunique"),
                matched=("size_matched", "sum"),
                pnl=("_pnl", "sum"),
            )
            .reset_index()
            .sort_values(["customer_strategy_ref", "status"])
        )

        # Grand totals across all strategies + statuses
        total_bets = df["bet_id"].nunique()
        total_matched = float(df["size_matched"].fillna(0.0).sum())
        total_pnl = float(df["_pnl"].fillna(0.0).sum())

        with pd.option_context("display.max_rows", 200, "display.width", 160):
            print("\nPer-strategy summary (net settled + MTM for open):")
            print(summ)

    print(f"\nTOTALS — bets: {total_bets:,} | matched: {total_matched:.2f} | pnl: {total_pnl:+.2f}")

    client.logout()


if __name__ == "__main__":
    main()
