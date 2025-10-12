#!/root/betting/.venv/bin/python
# -*- coding: utf-8 -*-

"""
live_state.py – Betfair aggregated snapshot (funds + orders + LTP)

Outputs:
  - Funds summary
  - Today's PnL
  - UNMATCHED + MATCHED tables (one row per runner):
      BackQty/BackAvg, LayQty/LayAvg, LTP,
      MTM @ LTP (£)  ← hedge-at-LTP green pnl for current side exposure
      Mkt PnL (£)    ← settled net today (from cleared orders)
  - === MARKET PNL SUMMARY === (per market)
"""

import os, math
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import betfairlightweight as bflw
from betfairlightweight.filters import market_filter, time_range

# -------- Helpers --------
def chunked(seq, n): return (seq[i:i+n] for i in range(0, len(seq), n))
def fmt(x):
    if x is None: return "-"
    try:
        if isinstance(x, (int, float)) and math.isfinite(x):
            return f"{x:.2f}"
        return str(x)
    except Exception:
        return str(x)
def today_utc_range_london():
    tz = ZoneInfo("Europe/London")
    start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end   = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)
def print_table(title, rows, headers):
    print(f"\n{title}")
    if not rows: print("(none)"); return
    cols = list(zip(*([headers] + rows)))
    widths = [max(len(str(x)) for x in c) for c in cols]
    line = lambda r: " | ".join(str(v).ljust(w) for v, w in zip(r, widths))
    print(line(headers))
    print("-+-".join("-"*w for w in widths))
    for r in rows: print(line(r))
def side_str(s):
    s = str(s).upper() if s else ""
    return "BACK" if "BACK" in s else ("LAY" if "LAY" in s else "?")

# Hedge-at-LTP green PnL (same as earlier logic)
def mtm_green(side, stake, avg_price, ltp):
    if not stake or not avg_price or not ltp:
        return None
    if ltp <= 0:
        return None
    if side == "BACK":
        # hedge by laying @ LTP
        return stake * (avg_price / ltp - 1.0)
    if side == "LAY":
        # hedge by backing @ LTP
        return stake * (1.0 - avg_price / ltp)
    return None

# -------- Client --------
load_dotenv()
client = bflw.APIClient(
    os.getenv("BETFAIR_USERNAME"),
    os.getenv("BETFAIR_PASSWORD"),
    app_key=os.getenv("BETFAIR_APP_KEY"),
    cert_files=("/root/betting/certs/client-2048.crt","/root/betting/certs/client-2048.key"),
)
client.login()

# -------- Funds + today PnL --------
acc = client.account.get_account_funds()
avail = acc.available_to_bet_balance or 0
expo  = acc.exposure or 0
total = (avail + abs(expo)) if expo < 0 else (avail + expo)

start, end = today_utc_range_london()
cleared = client.betting.list_cleared_orders(
    bet_status="SETTLED",
    settled_date_range=time_range(from_=start,to=end),
    include_item_description=False,
)
orders_today = getattr(cleared,"orders",None) or getattr(cleared,"cleared_orders",[]) or []
pnl_today = sum((getattr(o,"profit",0.0) or 0.0) - (getattr(o,"commission",0.0) or 0.0) for o in orders_today)

print("\n=== ACCOUNT FUNDS ===")
print(f"Available funds      : {fmt(avail)}")
print(f"Exposure (liability) : {fmt(expo)}")
print(f"Total funds          : {fmt(total)}")
print(f"Today's PnL (net)    : {fmt(pnl_today)}")

# -------- Orders --------
cur = client.betting.list_current_orders()
orders = getattr(cur,"current_orders",None) or getattr(cur,"orders",None) or []
unmatched = [o for o in orders if getattr(o,"status","")=="EXECUTABLE"]
matched   = [o for o in orders if getattr(o,"status","")=="EXECUTION_COMPLETE"]
mids = sorted({o.market_id for o in orders if getattr(o,"market_id",None)})

# -------- Market metadata --------
meta = {}
for ch in chunked(mids,100):
    cats = client.betting.list_market_catalogue(
        filter=market_filter(market_ids=ch),
        market_projection=["RUNNER_DESCRIPTION","EVENT"],
        max_results=1000,
    ) or []
    for m in cats:
        meta[m.market_id] = {
            "event": m.event.name if m.event else "",
            "eid":   m.event.id if m.event else "",
            "mkt":   m.market_name,
            "rmap":  {r.selection_id:r.runner_name for r in (m.runners or [])}
        }

# -------- LTPs --------
ltp = {}
for ch in chunked(mids,25):
    books = client.betting.list_market_book(market_ids=ch) or []
    for b in books:
        for r in b.runners or []:
            ltp[(b.market_id,r.selection_id)] = r.last_price_traded

# -------- Market-level (settled) PnL for today --------
market_pnl = defaultdict(float)
for o in orders_today:
    mid = getattr(o,"market_id",None)
    if not mid: continue
    market_pnl[mid] += (getattr(o,"profit",0.0) or 0.0) - (getattr(o,"commission",0.0) or 0.0)

# -------- Aggregation with MTM vs LTP --------
def aggregate_split_with_mtm(olist, unmatched=False):
    """
    Row: [Event ID, Event Name, Market, Runner,
          BackQty, BackAvg, LayQty, LayAvg,
          LTP, MTM @ LTP (£), Mkt PnL (£)]
    MTM computed from side exposure (qty, avg) hedged at current LTP.
    """
    # key -> side -> accumulators
    agg = defaultdict(lambda: {"BACK":{"sz":0.0,"pxsz":0.0},"LAY":{"sz":0.0,"pxsz":0.0}})
    for o in olist:
        m = meta.get(o.market_id, {})
        eid, ename, mname = m.get("eid",""), m.get("event",""), m.get("mkt", o.market_id)
        rname = m.get("rmap", {}).get(getattr(o,"selection_id",None), str(getattr(o,"selection_id","?")))
        key = (eid, ename, mname, rname, o.market_id, getattr(o,"selection_id",None))

        sd = side_str(getattr(o,"side",None))
        if sd not in ("BACK","LAY"):
            continue

        if unmatched:
            sz = getattr(o,"size_remaining",0) or 0
            px = getattr(getattr(o,"price_size",None),"price",None)
        else:
            sz = getattr(o,"size_matched",0) or 0
            px = getattr(o,"average_price_matched",None)

        try: sz = float(sz)
        except: sz = 0.0
        try: px = float(px) if px else None
        except: px = None

        if sz and px:
            agg[key][sd]["sz"]   += sz
            agg[key][sd]["pxsz"] += sz * px

    rows = []
    for (eid, ename, mname, rname, mid, sid), sides in agg.items():
        bsz, bpxsz = sides["BACK"]["sz"], sides["BACK"]["pxsz"]
        lsz, lpxsz = sides["LAY"]["sz"],  sides["LAY"]["pxsz"]
        bavg = (bpxsz / bsz) if bsz else None
        lavg = (lpxsz / lsz) if lsz else None
        ltp_now = ltp.get((mid, sid))

        # MTM per side (hedge at LTP), sum if both exist
        mtm_back = mtm_green("BACK", bsz, bavg, ltp_now) if bsz and bavg else None
        mtm_lay  = mtm_green("LAY",  lsz, lavg, ltp_now) if lsz and lavg else None
        mtm_total = None
        if mtm_back is not None or mtm_lay is not None:
            mtm_total = (mtm_back or 0.0) + (mtm_lay or 0.0)

        rows.append([
            eid, ename, mname, rname,
            fmt(bsz if bsz else None), fmt(bavg),
            fmt(lsz if lsz else None), fmt(lavg),
            fmt(ltp_now),
            fmt(mtm_total),
            fmt(market_pnl.get(mid))
        ])
    rows.sort(key=lambda r: (r[1], r[2], r[3]))
    return rows

hdr = [
    "Event ID","Event Name","Market","Runner",
    "BackQty","BackAvg","LayQty","LayAvg",
    "LTP","MTM @ LTP (£)"
]

print_table("UNMATCHED ORDERS (by runner; split BACK/LAY; MTM vs LTP)", aggregate_split_with_mtm(unmatched, unmatched=True), hdr)
print_table("MATCHED ORDERS (by runner; split BACK/LAY; MTM vs LTP)",   aggregate_split_with_mtm(matched,   unmatched=False), hdr)

# -------- Market total summary --------
if market_pnl:
    print("\n=== SETTLED MARKET PNL SUMMARY ===")
    sp = 0 
    for mid, pnl in sorted(market_pnl.items(), key=lambda x: x[0]):
        m = meta.get(mid, {})
        sp += pnl
        print(f"{mid} | {m.get('event','')}  : {fmt(pnl)}")

    print(f"TOTAL SUM   |   : {round(sp,2)}")
client.logout()
print("\n✅ Live-state snapshot complete.")
