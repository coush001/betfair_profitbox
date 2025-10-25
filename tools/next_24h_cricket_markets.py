#!/root/betfair_profitbox/.venv/bin/python
# -*- coding: utf-8 -*-import os
from datetime import datetime, timedelta, timezone
import betfairlightweight as bflw
from dotenv import load_dotenv
import os

load_dotenv()
print(os.getenv("BETFAIR_USERNAME"),
    os.getenv("BETFAIR_PASSWORD"),)
    
client = bflw.APIClient(
    os.getenv("BETFAIR_USERNAME"),
    os.getenv("BETFAIR_PASSWORD"),
    app_key=os.getenv("BETFAIR_APP_KEY"),
    cert_files=("certs/client-2048.crt", "certs/client-2048.key"),
)
client.login()

# --- All times are handled in UTC ---
def to_z(dt: datetime) -> str:
    """Return UTC ISO-8601 with 'Z'."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def ensure_utc(dt: datetime) -> datetime:
    """Normalize any datetime to UTC (assume naïve datetimes are already UTC)."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

now_utc   = datetime.now(timezone.utc)
later_utc = now_utc + timedelta(hours=24)

# --- REST filter: UTC window (Z-suffixed ISO strings) ---
market_filter_raw = {
    "eventTypeIds": ["4"],           # Cricket
    "marketTypeCodes": ["MATCH_ODDS"],
    "marketCountries": ["GB", "IN"],
    "marketStartTime": {
        "from": to_z(now_utc),       # e.g. '2025-09-16T14:00:00Z'
        "to":   to_z(later_utc),
    },
}

catalogues = client.betting.list_market_catalogue(
    filter=market_filter_raw,
    max_results=1000,
    market_projection=["MARKET_START_TIME", "RUNNER_DESCRIPTION", "EVENT"],
)

print(f"# All timestamps below are UTC (Z). Window: {to_z(now_utc)} → {to_z(later_utc)}")
for m in catalogues:
    start_utc = ensure_utc(m.market_start_time)
    print(m.market_id, m.event.name, to_z(start_utc))

client.logout()


