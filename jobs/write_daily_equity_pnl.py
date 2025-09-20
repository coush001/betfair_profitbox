#!/root/betting/betenv/bin/python3
"""
Append a line to /root/betting/store/date_equity_pnl.csv

Columns (in order):
    timestamp_utc, total_equity, available_balance,
    open_exposure, pnl_today, currency

Prints the same data to stdout with UTC timestamps.
"""

import os
import csv
from pathlib import Path
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
import betfairlightweight as bflw
from betfairlightweight.filters import time_range

# --------------------------------------------------------------------
CSV_PATH = Path("/root/betting/store/date_equity_pnl.csv")
CERTS    = ("/root/betting/certs/client-2048.crt",
            "/root/betting/certs/client-2048.key")
TZ_LOCAL = ZoneInfo("UTC")   # daily PnL window in UTC

def ts() -> str:
    """Readable UTC timestamp for prints."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

def iso_utc(dt: datetime) -> str:
    """UTC ISO8601 string with 'Z' suffix."""
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def main():
    load_dotenv()
    USERNAME = os.getenv("BETFAIR_USERNAME")
    PASSWORD = os.getenv("BETFAIR_PASSWORD")
    APP_KEY  = os.getenv("BETFAIR_APP_KEY")

    print(f"{ts()}  Logging in as {USERNAME}")
    cli = bflw.APIClient(USERNAME, password=PASSWORD, app_key=APP_KEY, cert_files=CERTS)
    cli.login()

    # ----- Current funds -----
    funds = cli.account.get_account_funds()
    avail    = float(getattr(funds, "available_to_bet_balance", 0.0) or 0.0)
    exposure = float(getattr(funds, "exposure", 0.0) or 0.0)
    currency = getattr(funds, "currency_code", "") or ""

    total_equity = avail + abs(exposure)
    print(f"{ts()}  Available balance: {avail:.2f}  "
          f"Open exposure: {exposure:.2f}  Total equity: {total_equity:.2f}")

    # ----- Today's settled PnL -----
    now_local   = datetime.now(TZ_LOCAL)
    start_local = datetime.combine(now_local.date(), time.min, TZ_LOCAL)

    settled_range = time_range(
        from_=iso_utc(start_local),
        to=iso_utc(now_local)
    )

    report = cli.betting.list_cleared_orders(
        bet_status="SETTLED",
        settled_date_range=settled_range,
        group_by=None,
        include_item_description=False,
        locale="en",
    )

    pnl_today = 0.0
    cleared = getattr(report, "cleared_orders", []) or getattr(report, "orders", [])
    for co in cleared or []:
        pnl_today += float(getattr(co, "profit", 0.0) or 0.0)

    print(f"{ts()}  Settled PnL today: {pnl_today:.2f}")

    cli.logout()
    print(f"{ts()}  Logged out of Betfair API")

    # ----- Write CSV -----
    CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
    header = ["timestamp_utc", "total_equity",
              "available_balance", "open_exposure",
              "pnl_today", "currency"]
    write_header = not CSV_PATH.exists()

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    row = [now_utc.isoformat(),
           f"{total_equity:.2f}",
           f"{avail:.2f}",
           f"{exposure:.2f}",
           f"{pnl_today:.2f}",
           currency]

    with CSV_PATH.open("a", newline="") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        writer.writerow(row)

    print(f"{ts()}  CSV row appended: {row}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"{ts()}  ERROR: {e}")
        raise

