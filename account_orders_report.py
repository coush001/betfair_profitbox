#!/root/betting/betenv/bin/python3
"""
snapshot.py – Betfair snapshot (version-agnostic betfairlightweight)

Flow:
  1) Print account funds + total equity (+ optional today's PnL if available)
  2) Print ALL open (unmatched) and matched orders FIRST
  3) Then, for each unique market you've bet on, print one compact table:
       • Event/market metadata (sport, competition, country, venue, times)
       • Current LTP for ALL runners in that market

Adds:
  • Total equity = available_to_bet_balance + exposure
  • PnL for today (UTC) if list_cleared_orders is available
  • Market TYPE + NAME shown alongside orders and event tables
  • Robust logging around list_market_book (LTP) attempts

Requires:
  .env with BETFAIR_USERNAME, BETFAIR_PASSWORD, BETFAIR_APP_KEY
  certs/client-2048.crt and certs/client-2048.key
"""

import os
import sys
from pathlib import Path
from collections import defaultdict
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv
from betfairlightweight import APIClient
from betfairlightweight.filters import market_filter, price_projection, time_range

CERT_DIR = Path(__file__).parent / "certs"


# -------------------- helpers --------------------

def log(msg: str):
    print(f"[snapshot] {msg}", file=sys.stderr)


def pretty_num(x):
    try:
        return f"{float(x):.2f}"
    except Exception:
        return str(x)


def iso(dt):
    if not dt:
        return ""
    try:
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    except Exception:
        return str(dt)


def gget(o, *keys):
    """Get attribute or dict key across betfairlightweight versions."""
    for k in keys:
        if hasattr(o, k):
            return getattr(o, k)
        if isinstance(o, dict) and k in o:
            return o[k]
    return None


def extract_orders(resp):
    """Return orders list regardless of betfairlightweight version."""
    if hasattr(resp, "current_orders"):
        return resp.current_orders
    if hasattr(resp, "orders"):
        return resp.orders
    d = getattr(resp, "_data", {}) or {}
    return d.get("currentOrders") or d.get("orders") or []


def extract_funds(funds_obj):
    """Version-agnostic read of account funds."""
    def g(k):
        if hasattr(funds_obj, k):
            return getattr(funds_obj, k)
        d = getattr(funds_obj, "_data", {}) or {}
        return d.get(k)
    return {
        "available_to_bet_balance": g("available_to_bet_balance"),
        "exposure": g("exposure"),
        "retained_commission": g("retained_commission"),
        "exposure_limit": g("exposure_limit"),
        "currency": g("currency_code") or g("currency"),
    }


def extract_today_pnl(client: APIClient):
    """
    Try to compute today's UTC PnL from cleared orders.
    Returns (pnl_value or None, details_str or None).
    """
    try:
        # Today (UTC) range
        now_utc = datetime.now(timezone.utc)
        start_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        tr = time_range(from_=start_utc, to=now_utc)

        # Betfair API expects settledDateRange for cleared orders; bflw maps time_range appropriately.
        resp = client.betting.list_cleared_orders(
            bet_status="SETTLED",
            settled_date_range=tr,
            from_record=0,
            record_count=1000,
        )

        # Version-agnostic: resp.cleared_orders or resp._data['clearedOrders']
        cleared = []
        if hasattr(resp, "cleared_orders") and resp.cleared_orders:
            cleared = resp.cleared_orders
        else:
            d = getattr(resp, "_data", {}) or {}
            cleared = d.get("clearedOrders") or []

        total = 0.0
        count = 0
        for co in cleared:
            prof = gget(co, "profit")  # object attr
            if prof is None and isinstance(co, dict):
                prof = co.get("profit")
            if prof is not None:
                try:
                    total += float(prof)
                    count += 1
                except Exception:
                    pass
        return total if count > 0 else 0.0, f"{count} settled bets"
    except Exception as e:
        log(f"PnL fetch failed (list_cleared_orders): {repr(e)}")
        return None, None


# -------------------- main --------------------

def main():
    # --- env & certs ---
    load_dotenv()
    user = os.getenv("BETFAIR_USERNAME")
    pwd = os.getenv("BETFAIR_PASSWORD")
    app = os.getenv("BETFAIR_APP_KEY")
    if not all([user, pwd, app]):
        raise SystemExit("❌ Missing BETFAIR_* env vars in .env")

    if not (CERT_DIR / "client-2048.crt").exists() or not (CERT_DIR / "client-2048.key").exists():
        raise SystemExit("❌ Missing certs/client-2048.crt or certs/client-2048.key")

    # --- login ---
    client = APIClient(user, pwd, app_key=app, certs=str(CERT_DIR))
    client.login()

    # --- funds (header) ---
    try:
        funds = client.account.get_account_funds()
        f = extract_funds(funds)
        avail = f.get("available_to_bet_balance")
        expo = f.get("exposure")
        currency = f.get("currency") or ""
        total_equity = None
        try:
            total_equity = (float(avail) if avail is not None else 0.0) + (abs(float(expo)) if expo is not None else 0.0)
        except Exception:
            pass

        print("ACCOUNT FUNDS")
        print("-------------")
        print(f"Available : {pretty_num(avail)} {currency}")
        print(f"Exposure  : {pretty_num(expo)}")
        if total_equity is not None:
            print(f"Total Eq. : {pretty_num(total_equity)} {currency}")
        if f.get("retained_commission") is not None:
            print(f"Retained  : {pretty_num(f.get('retained_commission'))}")
        if f.get("exposure_limit") is not None:
            print(f"Exp Limit : {pretty_num(f.get('exposure_limit'))}")
    except Exception as e:
        print(f"⚠️ Could not fetch account funds: {e}")

    # --- PnL (today) ---
    pnl_value, pnl_details = extract_today_pnl(client)
    if pnl_value is None:
        print("PnL Today : (unavailable)")
    else:
        suffix = f"  [{pnl_details}]" if pnl_details else ""
        print(f"PnL Today : {pretty_num(pnl_value)}{suffix}")

    # --- orders ---
    resp = client.betting.list_current_orders(order_projection="ALL", from_record=0, record_count=1000)
    orders = extract_orders(resp)

    if not orders:
        print("\nNo current orders.")
        client.logout()
        return

    unmatched, matched, market_ids = [], [], set()
    for o in orders:
        mid = gget(o, "market_id", "marketId")
        size_rem = float(gget(o, "size_remaining", "sizeRemaining") or 0)
        size_mat = float(gget(o, "size_matched", "sizeMatched") or 0)
        status = gget(o, "status")
        market_ids.add(mid)
        if status == "EXECUTABLE" and size_rem > 0:
            unmatched.append(o)
        if size_mat > 0:
            matched.append(o)

    # --- FIRST: print all orders (unmatched then matched) ---

    # For nicer order print, get runner names quickly + market types/names
    runner_quick = defaultdict(dict)
    market_name_quick = {}
    market_type_quick = {}

    if market_ids:
        try:
            cats_quick = client.betting.list_market_catalogue(
                filter=market_filter(market_ids=list(market_ids)),
                max_results=len(market_ids),
                market_projection=["RUNNER_DESCRIPTION", "MARKET_DESCRIPTION"],
            )
            for m in cats_quick:
                runner_quick[m.market_id] = {r.selection_id: r.runner_name for r in (m.runners or [])}
                desc = getattr(m, "description", None)
                market_type_quick[m.market_id] = getattr(desc, "market_type", None) if desc else None
                market_name_quick[m.market_id] = getattr(m, "market_name", None)
        except Exception as e:
            log(f"Quick catalogue fetch failed: {repr(e)}")

    def label_runner(mid, sel, runner_map):
        nm = runner_map.get(mid, {}).get(sel)
        return f"{sel} | {nm}" if nm else str(sel)

    def print_orders_section(title, rows):
        print("\n" + title)
        print("-" * len(title))
        if not rows:
            print("(none)")
            return
        by_market = defaultdict(list)
        for o in rows:
            by_market[gget(o, "market_id", "marketId")].append(o)
        for mid in sorted(by_market.keys(), key=str):
            mtype = market_type_quick.get(mid) or "-"
            mname = market_name_quick.get(mid) or "-"
            print(f"\nMarket: {mid}  |  Type: {mtype}  |  Name: {mname}")
            print("BET_ID            SIDE   PRICE   SIZE   MATCHED  REMAIN  SELECTION")
            print("----------------- -----  ------  -----  -------  ------  ----------------")
            for o in by_market[mid]:
                bet_id = gget(o, "bet_id", "betId")
                side = gget(o, "side")
                price = gget(o, "price")
                size = gget(o, "size")
                if price is None or size is None:
                    ps = gget(o, "price_size", "priceSize") or {}
                    if isinstance(ps, dict):
                        price = price or ps.get("price")
                        size = size or ps.get("size")
                    else:
                        price = price or getattr(ps, "price", None)
                        size = size or getattr(ps, "size", None)
                sel_id = gget(o, "selection_id", "selectionId")
                print(f"{str(bet_id):<17} {str(side):<5} {pretty_num(price):>6} {pretty_num(size):>6} "
                      f"{pretty_num(gget(o,'size_matched','sizeMatched')):>7} {pretty_num(gget(o,'size_remaining','sizeRemaining')):>6} "
                      f"{label_runner(mid, sel_id, runner_quick)}")
    print("\n\n")
    print_orders_section("UNMATCHED (open orders)", unmatched)
    print("\n\n")
    print_orders_section("MATCHED (filled/partial)", matched)

    # --- THEN: per-unique-market tables with metadata + LTP only ---

    market_meta = {}   # marketId -> dict of metadata
    runner_name = {}   # marketId -> {selectionId: runnerName}

    if market_ids:
        cats = client.betting.list_market_catalogue(
            filter=market_filter(market_ids=list(market_ids)),
            max_results=len(market_ids),
            market_projection=[
                "EVENT_TYPE", "COMPETITION", "EVENT",
                "MARKET_DESCRIPTION", "RUNNER_DESCRIPTION"
            ],
        )
        for m in cats:
            event_type_name = getattr(m.event_type, "name", None) if getattr(m, "event_type", None) else None
            comp_name = getattr(m.competition, "name", None) if getattr(m, "competition", None) else None
            evt = getattr(m, "event", None)
            evt_name = getattr(evt, "name", None) if evt else None
            country = getattr(evt, "country_code", None) if evt else None
            venue = getattr(evt, "venue", None) if evt else None
            tz = getattr(evt, "timezone", None) if evt else None
            open_date = getattr(evt, "open_date", None) if evt else None

            desc = getattr(m, "description", None)
            suspend_time = getattr(desc, "suspend_time", None) if desc else None
            settle_time = getattr(desc, "settle_time", None) if desc else None
            market_time = getattr(desc, "market_time", None) if desc else None

            market_meta[m.market_id] = {
                "market_name": getattr(m, "market_name", None),
                "market_type": getattr(desc, "market_type", None) if desc else None,
                "event_type": event_type_name,
                "competition": comp_name,
                "event_name": evt_name,
                "country": country,
                "venue": venue,
                "timezone": tz,
                "open_date": open_date,
                "market_start_time": getattr(m, "market_start_time", None),
                "scheduled_suspend_time": suspend_time,
                "scheduled_settle_time": settle_time,
                "market_time": market_time,
                "turn_in_play_enabled": getattr(desc, "turn_in_play_enabled", None) if desc else None,
                "betting_type": getattr(desc, "betting_type", None) if desc else None,
                "regulator": getattr(desc, "regulator", None) if desc else None,
            }

            runner_name[m.market_id] = {r.selection_id: r.runner_name for r in (m.runners or [])}

    # LTP only (keeps API call simple & robust) with detailed logging
    books = {}
    if market_ids:
        mids = list(market_ids)

        # 1) Minimal call (no projection)
        try:
            log("Attempt #1: list_market_book(market_ids=...) with NO price_projection")
            mb = client.betting.list_market_book(market_ids=mids)
        except Exception as e:
            log(f"Attempt #1 failed: {repr(e)}")
            mb = None

        # 2) If needed, LTP only
        if not mb:
            try:
                log("Attempt #2: list_market_book with price_projection=EX_LTP")
                pp_ltp = price_projection(price_data=["EX_LTP"])
                mb = client.betting.list_market_book(market_ids=mids, price_projection=pp_ltp)
            except Exception as e:
                log(f"Attempt #2 failed: {repr(e)}")
                mb = None

        # 3) If still needed, LTP + best offers (can trip DSC-0018 on some accounts/markets)
        if not mb:
            try:
                log("Attempt #3: list_market_book with price_projection=EX_LTP+EX_BEST_OFFERS (virtualise=True)")
                pp_best = price_projection(price_data=["EX_LTP", "EX_BEST_OFFERS"], virtualise=True)
                mb = client.betting.list_market_book(market_ids=mids, price_projection=pp_best)
            except Exception as e:
                log(f"Attempt #3 failed: {repr(e)}")
                mb = None

        if mb:
            for b in mb:
                books[b.market_id] = b
        else:
            log("All attempts to fetch market books failed; proceeding without LTP tables.")

    # Print one compact table PER market (unique event you’ve bet on)
    print("\nEVENT INFO + LTP TABLES")
    print("------------------------")
    for mid in sorted(market_ids, key=str):
        meta = market_meta.get(mid, {})
        mtype = meta.get("market_type") or "-"
        mname = meta.get("market_name") or "-"
        print(f"\nMarket: {mid}  |  Type: {mtype}  |  Name: {mname}")
        if meta.get("event_name"):
            print(f"Event: {meta.get('event_name')}  |  Competition: {meta.get('competition') or '-'}  |  Sport: {meta.get('event_type') or '-'}")
        if meta.get("country") or meta.get("venue"):
            print(f"Country: {meta.get('country') or '-'}  |  Venue: {meta.get('venue') or '-'}")
        if meta.get("timezone"):
            print(f"Timezone: {meta.get('timezone')}")
        ms = meta.get("market_start_time")
        ss = meta.get("scheduled_suspend_time")
        st = meta.get("scheduled_settle_time")
        od = meta.get("open_date")
        if any([ms, ss, st, od]):
            print("Times (UTC): "
                  f"Start={iso(ms) or '-'}  |  Suspend≈{iso(ss) or '-'}  |  Settle≈{iso(st) or '-'}  |  EventOpen={iso(od) or '-'}")
        tip = meta.get("turn_in_play_enabled")
        if tip is not None:
            print(f"Turn In-Play Enabled: {tip}")

        # LTP table
        print("SELECTION                         LTP")
        print("-------------------------------  ------")
        rmap = runner_name.get(mid, {})
        b = books.get(mid)
        ltp_by_sel = {}
        if b and getattr(b, "runners", None):
            for br in b.runners:
                ltp_by_sel[br.selection_id] = getattr(br, "last_price_traded", None)
        else:
            # log which market had no LTP
            log(f"No LTP data for market {mid} (no book or no runners).")

        # print rows for all runners we know (stable ordering by selectionId)
        for sel in sorted(rmap.keys()):
            nm = rmap[sel]
            ltp = ltp_by_sel.get(sel)
            label = f"{sel} | {nm}" if nm else str(sel)
            print(f"{label:<31}  {pretty_num(ltp):>6}")

    client.logout()


if __name__ == "__main__":
    main()

