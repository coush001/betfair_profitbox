#!/root/betting/.venv/bin/python
# md_recorder_v4.py — per-market raw mcm recorder (in-play only) with logging & summaries
import os, gzip, json, logging, datetime as dt
import betfairlightweight as bflw
from betfairlightweight import filters
from dotenv import load_dotenv

# -------- settings --------
BASE = "/root/betting/research/hist_data/self_recorded"
EVENT_TYPE_IDS = ["4"]   # e.g. 1=soccer, 4=tennis, 7=horse racing
LOOKAHEAD_HOURS = 24
MAX_MARKETS = 200
GZIP_LEVEL = int(os.getenv("RECORDER_GZIP_LEVEL", "6"))  # 1=fast … 9=max compression
LOG_LEVEL = os.getenv("RECORDER_LOG_LEVEL", "INFO").upper()

# -------- logging --------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("md_recorder")

def utcnow():
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

def iso_to_dt(s):
    try:
        return dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None

def market_path(market_id, evt="Unknown", start_iso=None):
    d = iso_to_dt(start_iso) or utcnow()
    day = d.strftime("%Y-%m-%d")
    return os.path.join(BASE, day, str(evt), f"{market_id}.jsonl.gz")

class PerMarketRecorder(bflw.StreamListener):
    """Writes raw 'mcm' JSON lines to one gzip file per marketId, ONLY while in-play."""
    def __init__(self):
        super().__init__(max_latency=None)
        self.files = {}     # marketId -> gzip handle
        self.paths = {}     # marketId -> file path
        self.meta  = {}     # marketId -> {'eventTypeId','marketTime','marketName'}
        self.stats = {}     # marketId -> {'lines','first_pt','last_pt','bytes'}
        self.inplay = {}    # marketId -> bool

    def _open_if_needed(self, mid: str):
        if mid in self.files:
            return
        info = self.meta.get(mid, {})
        path = market_path(mid, info.get("eventTypeId", "Unknown"), info.get("marketTime"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # buffered fileobj under gzip for fewer syscalls
        raw_f = open(path, "ab", buffering=1024 * 1024)
        fh = gzip.GzipFile(fileobj=raw_f, mode="ab", compresslevel=GZIP_LEVEL)
        self.files[mid] = fh
        self.paths[mid] = path
        self.stats[mid] = {"lines": 0, "first_pt": None, "last_pt": None, "bytes": 0}
        mn = info.get("marketName") or "?"
        log.info(f"🟢 Started recording (in-play) market {mid} | {mn}")

    def _close(self, mid: str):
        fh = self.files.pop(mid, None)
        path = self.paths.pop(mid, None)
        st = self.stats.pop(mid, {})
        meta = self.meta.get(mid, {})
        if fh:
            try: fh.close()
            except Exception: pass
        # summary
        on_disk = os.path.getsize(path) if path and os.path.exists(path) else 0
        n = st.get("lines", 0)
        first_pt = st.get("first_pt"); last_pt = st.get("last_pt")
        span_s = (max(0.0, (last_pt - first_pt) / 1000.0) if (first_pt and last_pt) else 0.0)
        rate = (n / span_s) if span_s > 0 else 0.0
        name = meta.get("marketName") or "?"
        log.info("🔴 Closed market %s | %s | lines=%d, span=%.1fs, rate=%.2f msg/s, file=%s (%.2f MB)",
                 mid, name, n, span_s, rate, path or "?", on_disk / (1024 * 1024.0))
        # keep meta so we can still log its name later if needed

    def on_data(self, raw: str):
        # Trim traffic: skip heartbeats & non-mcm
        if '"ct":"HEARTBEAT"' in raw or '"op":"mcm"' not in raw:
            return

        try:
            obj = json.loads(raw)
        except Exception:
            return

        pt = obj.get("pt")  # epoch ms
        encoded = (raw.rstrip("\n") + "\n").encode("utf-8")

        for mc in obj.get("mc", []):
            mid = mc.get("id")
            if not mid:
                continue

            # marketDefinition may update name/inPlay/status
            md = mc.get("marketDefinition")
            if md:
                rec = self.meta.setdefault(mid, {})
                if md.get("eventTypeId") is not None:
                    rec["eventTypeId"] = str(md.get("eventTypeId"))
                if md.get("marketTime"):
                    rec["marketTime"] = md.get("marketTime")
                if md.get("name"):
                    rec["marketName"] = md.get("name")

                # update in-play state (keys vary by feed casing)
                if "inPlay" in md:
                    self.inplay[mid] = bool(md.get("inPlay"))
                elif "inplay" in md:
                    self.inplay[mid] = bool(md.get("inplay"))

            # only record while in-play
            if not self.inplay.get(mid, False):
                # Not yet known or not in-play → skip
                continue

            # lazy-open and append
            self._open_if_needed(mid)
            fh = self.files.get(mid)
            if fh:
                try:
                    fh.write(encoded)
                except Exception:
                    self._close(mid)
                    continue

            # stats
            st = self.stats[mid]
            st["lines"] += 1
            st["bytes"] += len(encoded)
            if pt:
                if st["first_pt"] is None:
                    st["first_pt"] = pt
                st["last_pt"] = pt

            # auto-close when CLOSED
            if md and md.get("status") == "CLOSED":
                self._close(mid)

def collect_markets(client, event_type_ids, lookahead_hours, limit):
    """Fetch MarketCatalogue and return a list of dicts with id & info (for logging)."""
    start = utcnow(); end = start + dt.timedelta(hours=lookahead_hours)
    mf = filters.market_filter(
        event_type_ids=event_type_ids,
        market_start_time={
            "from": start.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "to":   end.isoformat(timespec="seconds").replace("+00:00", "Z"),
        },
    )
    cats = client.betting.list_market_catalogue(filter=mf, max_results=min(limit, 1000))
    out = []
    for c in cats[:limit]:
        ev = getattr(c, "event", None)
        out.append({
            "marketId": c.market_id,
            "marketName": getattr(c, "market_name", None),
            "marketStartTime": getattr(c, "market_start_time", None),
            "eventName": getattr(ev, "name", None) if ev else None,
            "countryCode": getattr(ev, "country_code", None) if ev else None,
        })
    return out

if __name__ == "__main__":
    load_dotenv()
    USERNAME = os.getenv("BETFAIR_USERNAME")
    APP_KEY  = os.getenv("BETFAIR_APP_KEY")
    PASSWORD = os.getenv("BETFAIR_PASSWORD")
    if not (USERNAME and APP_KEY and PASSWORD):
        raise SystemExit("❌ Missing BETFAIR_USERNAME / BETFAIR_APP_KEY / BETFAIR_PASSWORD in .env")

    c = bflw.APIClient(
        username=USERNAME, app_key=APP_KEY, password=PASSWORD,
        cert_files=("/root/betting/certs/client-2048.crt", "/root/betting/certs/client-2048.key"),
    )
    c.login()
    log.info("✅ REST login OK")

    markets = collect_markets(c, EVENT_TYPE_IDS, LOOKAHEAD_HOURS, MAX_MARKETS)
    if not markets:
        raise SystemExit("❌ No markets found; widen LOOKAHEAD_HOURS or change EVENT_TYPE_IDS")

    log.info("🧩 Subscribing to %d markets:", len(markets))
    for m in markets:
        log.info(" • %s | %s | %s | %s (%s)",
                 m["marketId"], m.get("marketName") or "?", m.get("marketStartTime") or "?",
                 m.get("eventName") or "?", m.get("countryCode") or "?")

    mids = [m["marketId"] for m in markets]

    listener = PerMarketRecorder()
    stream = c.streaming.create_stream(listener=listener)
    stream.subscribe_to_markets(
        market_filter={"marketIds": mids},
        market_data_filter={
            "fields": ["EX_MARKET_DEF", "EX_LTP", "EX_ALL_OFFERS", "EX_TRADED"],
            "ladderLevels": 10,
        },
    )

    log.info("📡 Recording (in-play only)… Ctrl+C to stop | GZIP_LEVEL=%d", GZIP_LEVEL)
    try:
        stream.start()
    except KeyboardInterrupt:
        log.info("⏹ Interrupted by user")
    finally:
        for mid in list(listener.files.keys()):
            listener._close(mid)
        log.info("🧹 Closed open market files.")
