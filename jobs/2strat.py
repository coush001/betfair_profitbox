#!/root/betfair_profitbox/.venv/bin/python
import os
import traceback
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

import betfairlightweight as bflw
from dotenv import load_dotenv
from pythonjsonlogger import jsonlogger  # noqa: F401

from flumine import Flumine, clients, BaseStrategy
from flumine.order.trade import Trade
from flumine.order.order import OrderStatus
from flumine.order.ordertype import LimitOrder
from flumine.utils import get_price

from betfairlightweight.filters import market_filter, streaming_market_data_filter

from betfair_profitbox.strat_utils.setup_logging import build_logger

print("Strat imports done")

class FlumineStrat(BaseStrategy):
    def __init__(self, enter_threshold, exit_threshold, order_hold, price_add,
                 log_root, log_level, *a, **k):
        self.log = build_logger(log_root, log_level)
        super().__init__(name="risk_backfave", *a, **k)
        self.hist = defaultdict(lambda: deque(maxlen=400))  # per runner
        self.enter_threshold = enter_threshold
        self.exit_threshold = exit_threshold
        self.order_hold = order_hold
        self.price_add = price_add
        self.startdt = None
        self._last_matched = {}    # order_id -> last size_matched
        self.rows = []             # collected fills
        self.pnl = 0.0
        print("its all innited")

    # ---- Flumine callbacks ----

    def add_market(self, market):
        msg = f"[add_market] {market.market_id} {market.event_name} type={market.event_type_id}"
        self.log.info(msg)

    def check_market_book(self, market, market_book):
        # For debugging: accept ALL open books (pre-off + in-play)
        msg = f"[check_market_book] {market.market_id} status={market_book.status} inplay={market_book.inplay}"
        self.log.debug(msg)
        return market_book.status == "OPEN" and market_book.inplay

    def _price_now(self, r):
        return r.last_price_traded

    def matched_summary(self, r, market):
        back_total = lay_total = 0.0
        back_weighted = lay_weighted = 0.0

        for o in market.blotter:
            if o.selection_id != r.selection_id:
                continue
            m = float(getattr(o, "size_matched", 0) or 0)
            if m <= 0:
                continue
            p = float(getattr(o, "average_price_matched", 0) or 0)
            side = str(getattr(o, "side", "")).upper()
            if "BACK" in side:
                back_total += m
                back_weighted += m * p
            elif "LAY" in side:
                lay_total += m
                lay_weighted += m * p

        avg_back = back_weighted / back_total if back_total else 0.0
        avg_lay = lay_weighted / lay_total if lay_total else 0.0
        return back_total, avg_back, lay_total, avg_lay

    def best_prices_for_runner(self, r):
        ex = getattr(r, "ex", None)
        atb = getattr(ex, "available_to_back", []) or []
        atl = getattr(ex, "available_to_lay", []) or []

        def _extract(item):
            if isinstance(item, dict):
                return item.get("price"), item.get("size")
            return getattr(item, "price", None), getattr(item, "size", None)

        bb_price, bb_size = _extract(atb[0]) if atb else (None, None)
        bl_price, bl_size = _extract(atl[0]) if atl else (None, None)
        return bb_price, bb_size, bl_price, bl_size

    def process_market_book(self, market, market_book):
        try:
            self.log.info(
                f"[process_market_book] {market.market_id} @ {market_market_time(market_book)} "
                f"inplay={market_book.inplay}"
            )

            if not self.startdt:
                self.startdt = market_book.publish_time

            elapsed = market_book.publish_time.timestamp() - self.startdt.timestamp()
            if elapsed <= 1:
                return

            for r in market_book.runners:
                context = f"->{market.market_id} {r.selection_id} "
                back_total, avg_back, lay_total, avg_lay = self.matched_summary(r, market)
                runner_context = self.get_runner_context(
                    market.market_id, r.selection_id, r.handicap
                )

                ltp = self._price_now(r)
                if not ltp:
                    self.log.debug(f"No LTP for {context}, skipping this runner")
                    continue

                self.log.debug(f"Market tick for {context}, price {ltp}")

                # ENTRY
                if ltp < self.enter_threshold and runner_context.live_trade_count == 0:
                    self.log.info(
                        f"Trigger back trade: {context} placing order, ltp price {ltp}"
                    )
                    ex = getattr(r, "ex", None)
                    available_to_lay = getattr(ex, "available_to_lay", []) if ex else []
                    back0 = get_price(available_to_lay, 0)

                    if back0 is None:
                        available_to_back = getattr(ex, "available_to_back", []) if ex else []
                        back0 = ltp or get_price(available_to_back, 0)

                    if back0 is None:
                        self.log.debug(f"No usable price for {context}, skipping entry")
                        continue

                    back = round(back0 + self.price_add, 2)

                    trade = Trade(
                        market_book.market_id,
                        r.selection_id,
                        r.handicap,
                        self,
                        notes={"entry_px": back},
                    )
                    order = trade.create_order(
                        side="BACK",
                        order_type=LimitOrder(back, self.context["stake"]),
                    )
                    try:
                        self.log.info(
                            f"Placing BACK order for {context} at {back}, stake={self.context['stake']}"
                        )
                        market.place_order(order)
                    except Exception as e:
                        self.log.warning(f"place_order failed for {context}: {e}")
                    else:
                        self.log.info({"ORDER PLACED": context, "order_price": back})

                # EXIT / HEDGE
                if ltp > self.exit_threshold and back_total:
                    bestb, _, bestl, _ = self.best_prices_for_runner(r)
                    loss_on_loss_covered_prc = (
                        round(lay_total / back_total, 2) if back_total else 0.0
                    )
                    cover_ratio = 0.3

                    if (
                        loss_on_loss_covered_prc < cover_ratio
                        and runner_context.live_trade_count == 0
                    ):
                        self.log.warning(
                            f"Seeing reason to hedge: {context}, ltp {ltp} "
                            f"back_total:{back_total}, avg_back:{avg_back}, "
                            f"lay_total:{lay_total}, avg_lay:{avg_lay}, "
                            f"Loss Cover Ratio:{lay_total}/{back_total} = {loss_on_loss_covered_prc} "
                            f"Bestback : {bestb} , bestlay : {bestl}"
                        )
                        lay_price = 10
                        if bestl and lay_price > bestl:
                            self.log.warning(
                                f"{context} layprice {lay_price} > best lay {bestl}, expecting fast fill"
                            )
                        self.hedge_selection(
                            r,
                            market,
                            market_book,
                            ltp,
                            context,
                            size=2,
                            price=lay_price,
                        )

        except Exception as e:
            tb = traceback.format_exc()
            self.log.warning(f"Failed to process market book: {e}\n{tb}")

    def hedge_selection(self, r, market, market_book, ltp, context, size, price):
        try:
            self.log.warning(
                f"LAY order pre send : {r.selection_id}, hedge size {size}@ hedge price {price}"
            )
            trade = Trade(market_book.market_id, r.selection_id, r.handicap, self)
            order = trade.create_order("LAY", order_type=LimitOrder(price, size))
            market.place_order(order)
            self.log.warning(f"LAY order placed : {context} ")
        except Exception as e:
            self.log.warning(f"Failed send LAY order  : {str(e)}")

    def process_orders(self, market, orders):
        # Cancel stale executable orders
        for order in orders:
            if order.status == OrderStatus.EXECUTABLE:
                if order.elapsed_seconds and order.elapsed_seconds > self.order_hold:
                    market.cancel_order(order)

        # Print fills
        try:
            for o in orders:
                prev = self._last_matched.get(o.id, 0)
                curr = o.size_matched or 0
                if curr > prev:
                    inc = curr - prev
                    side = o.side
                    mid = market.market_id
                    sid = getattr(o.trade, "selection_id", None)
                    self.log.warning(f"⚡⚡ Order FILL | {side:<4} | runner={sid} | market={mid} | {inc:.2f} matched @ {o.average_price_matched:.2f} ({curr:.2f} total)" )
                self._last_matched[o.id] = curr
        except Exception as e:
            tb = traceback.format_exc()
            self.log.warning(f"Failed to print order fill state: {e}\n{tb}")

    def process_closed_market(self, market, market_book):
        self.pnl = 0.0
        self.log.info(
            f"Processing closed market: {market.event_name}, {market.market_id}"
        )
        for order in market.blotter:
            self.pnl += order.profit
            self.log.info(
                f"Order PNL {order.profit}, av size matched: {order.size_matched} "
                f"av price matched: {order.average_price_matched}, "
                f"date_time_created: {order.date_time_created}"
            )
        self.log.warning(
            f"Total pnl for market:{market.event_name}, {market.market_id}, : PNL :: {self.pnl}"
        )


def market_market_time(market_book):
    # Safety: publish_time sometimes None in weird states
    return market_book.publish_time or "UNKNOWN_TIME"


# =========================
# load .env and login
load_dotenv()
USERNAME = os.getenv("BETFAIR_USERNAME")
APP_KEY = os.getenv("BETFAIR_APP_KEY")
PASSWORD = os.getenv("BETFAIR_PASSWORD")

trading = bflw.APIClient(
    USERNAME,
    app_key=APP_KEY,
    password=PASSWORD,
    cert_files=(
        "/root/betfair_profitbox/certs/client-2048.crt",
        "/root/betfair_profitbox/certs/client-2048.key",
    ),
)
trading.login()

# ====== Time window ======
now_utc = datetime.now(timezone.utc) - timedelta(hours=24)
to_utc = now_utc + timedelta(hours=48)

# REST filter (for list_market_catalogue) – SIMPLE and RELAXED for debugging
catalogue_filter = market_filter(
    event_type_ids=["4"],       # 1 = Soccer (change to 4 for Cricket)
    market_type_codes=["MATCH_ODDS"],
    market_countries=["GB", "AU"],
    market_start_time={
        "from": now_utc.isoformat(),
        "to": to_utc.isoformat(),
    },
)

catalogues = trading.betting.list_market_catalogue(
    filter=catalogue_filter,
    max_results=20,
    market_projection=["EVENT", "MARKET_START_TIME"],
)

print(f"Markets matching filter (UTC window {now_utc} – {to_utc}):")
print(f"Found {len(catalogues)} markets")

for m in catalogues:
    print(
        f"{m.market_id}  {m.event.name}  "
        f"{m.market_start_time.astimezone(timezone.utc)}"
    )

if not catalogues:
    print("No markets found for this filter. Exiting.")
    raise SystemExit(0)

# Explicit list of market_ids for streaming (in streaming filter)
markets_to_trade = [m.market_id for m in catalogues]
print(f"Streaming filter marketIds: {markets_to_trade}")

streaming_filter = {
    "marketIds": markets_to_trade,
}

# Market data fields for streaming
STREAM_FIELDS = ["EX_MARKET_DEF", "EX_BEST_OFFERS", "EX_TRADED", "EX_LTP"]
LADDER_LEVELS = 3
stream_data = streaming_market_data_filter(
    fields=STREAM_FIELDS, ladder_levels=LADDER_LEVELS
)

client = clients.BetfairClient(trading, paper_trade=False)
framework = Flumine(client)

strategy = FlumineStrat(
    market_filter=streaming_filter,      # << IMPORTANT: dict with marketIds
    market_data_filter=stream_data,
    max_order_exposure=30,
    max_selection_exposure=20,
    context={"stake": 2},
    enter_threshold=1.2,
    exit_threshold=6.5,
    order_hold=17,
    price_add=0.00,
    log_root="./logs/live_prod/",
    log_level="I",
)

framework.add_strategy(strategy)
print("just framework.run is left ..")
framework.run()
