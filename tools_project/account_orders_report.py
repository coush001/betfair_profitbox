#!/root/betting/.venv/bin/python
# -*- coding: utf-8 -*-

"""
snapshot.py – Betfair aggregated snapshot (funds + orders + LTP)

Requires:
  /root/betting/.env  with:
    BETFAIR_USERNAME=...
    BETFAIR_PASSWORD=...
    BETFAIR_APP_KEY=...
    (optionally) BETFAIR_CERTS_PATH=/root/betting/s

Output:
  - Funds summary
  - Today's PnL
  - Aggregated unmatched orders (Event/Market/Runner, Amount, Avg Price, LTP)
  - Aggregated matched orders (same)
"""

import os
import math
from collections import defaultdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import betfairlightweight as bflw
from betfairlightweight.filters import market_filter
from betfairlightweight.filters import time_range

# ---------------------- Helpers ----------------------
def chunked(seq, n): return (seq[i:i+n] for i in range(0, len(seq), n))
def fmt(x): return f"{x:.2f}" if isinstance(x, (int, float)) else x or "-"
def today_utc_range_london():
    tz = ZoneInfo("Europe/London")
    start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    end = start.replace(hour=23, minute=59, second=59, microsecond=999999)
    return start.astimezone(timezone.utc), end.astimezone(timezone.utc)
def print_table(title, rows, headers):
    print(f"\n{title}")
    if not rows:
        print("(none)")
        return
    cols = list(zip(*([headers] + rows)))
    widths = [max(len(str(x)) for x in c) for c in cols]
    line = lambda r: " | ".join(str(v).ljust(w) for v, w in zip(r, widths))
    print(line(headers))
    print("-+-".join("-" * w for w in widths))
    for r in rows: print(line(r))

# ---------------------- Client ----------------------
load_dotenv()

client = bflw.APIClient(
    os.getenv("BETFAIR_USERNAME"),
    os.getenv("BETFAIR_PASSWORD"),
    app_key=os.getenv("BETFAIR_APP_KEY"),
    cert_files=("certs/client-2048.crt", "certs/client-2048.key"),
)
client.login()

# ---------------------- Funds + PnL ----------------------
acc = client.account.get_account_funds()
avail = acc.available_to_bet_balance or 0
expo = acc.exposure or 0
pos_expo = abs(expo) if expo < 0 else expo
total = avail + pos_expo

start, end = today_utc_range_london()


# after calling list_cleared_orders(...)
cleared = client.betting.list_cleared_orders(
    bet_status="SETTLED",
    settled_date_range=time_range(from_=start, to=end),
    include_item_description=False,
)

# handle different attribute names across versions
items = (
    getattr(cleared, "cleared_orders", None)
    or getattr(cleared, "orders", None)
    or []
)

pnl_today = sum((co.profit or 0.0) - (co.commission or 0.0) for co in items)

print("\n=== ACCOUNT FUNDS ===")
print(f"Available funds      : {fmt(avail)}")
print(f"Exposure (liability) : {fmt(expo)}")
print(f"Total funds          : {fmt(total)}")
print(f"Today's PnL (net)    : {fmt(pnl_today)}")

# ---------------------- Orders ----------------------
cur = client.betting.list_current_orders()
orders = (
    getattr(cur, "current_orders", None)   # newer versions
    or getattr(cur, "orders", None)        # older versions
    or []
)


unmatched = [o for o in orders if o.status == "EXECUTABLE"]
matched = [o for o in orders if o.status == "EXECUTION_COMPLETE"]
mids = list({o.market_id for o in orders})

# ---------------------- Market metadata ----------------------
meta = {}
for ch in chunked(mids, 100):
    cats = client.betting.list_market_catalogue(
        filter=market_filter(market_ids=ch),
        market_projection=["RUNNER_DESCRIPTION", "EVENT"],
        max_results=1000,
    )
    for m in cats:
        meta[m.market_id] = {
            "event": m.event.name if m.event else "",
            "eid": m.event.id if m.event else "",
            "mkt": m.market_name,
            "rmap": {r.selection_id: r.runner_name for r in (m.runners or [])},
        }

# ---------------------- LTPs ----------------------
ltp = {}
for ch in chunked(mids, 25):
    books = client.betting.list_market_book(market_ids=ch)
    for b in books:
        for r in b.runners or []:
            ltp[(b.market_id, r.selection_id)] = r.last_price_traded

# ---------------------- Aggregation ----------------------
def aggregate(olist, unmatched=False):
    agg = defaultdict(lambda: {"sz": 0, "pxsz": 0})
    for o in olist:
        m = meta.get(o.market_id, {})
        eid, ename, mname = m.get("eid", ""), m.get("event", ""), m.get("mkt", o.market_id)
        rname = m.get("rmap", {}).get(o.selection_id, str(o.selection_id))
        key = (eid, ename, mname, rname, o.market_id, o.selection_id)
        sz = o.size_remaining if unmatched else o.size_matched
        px = o.price_size.price if unmatched else (o.average_price_matched or 0)
        if sz and px:
            agg[key]["sz"] += sz
            agg[key]["pxsz"] += sz * px
    rows = []
    for (eid, ename, mname, rname, mid, sid), d in agg.items():
        avg = d["pxsz"] / d["sz"] if d["sz"] else 0
        rows.append([
            eid, ename, mname, rname,
            fmt(d["sz"]), fmt(avg),
            fmt(ltp.get((mid, sid)))
        ])
    rows.sort(key=lambda r: (r[1], r[2], r[3]))
    return rows

hdr = ["Event ID", "Event Name", "Market", "Runner", "Amount", "Avg Price", "LTP"]
print_table("UNMATCHED ORDERS (aggregated)", aggregate(unmatched, unmatched=True), hdr)
print_table("MATCHED ORDERS (aggregated)", aggregate(matched, unmatched=False), hdr)

client.logout()
