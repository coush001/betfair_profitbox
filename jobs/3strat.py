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


class LiquidityMomentumStrat(BaseStrategy):
    def __init__(
        self,
        enter_threshold,
        imbalance_threshold,
        momentum_threshold,
        exit_threshold,
        order_hold,
        price_add,
        log_root,
        log_level,
        *a,
        **k,
    ):
        self.log = build_logger(log_root, log_level)
        super().__init__(name="liquidity_momentum", *a, **k)
        self.imbalance_history = defaultdict(lambda: deque(maxlen=8))
        self.enter_threshold = enter_threshold
        self.imbalance_threshold = imbalance_threshold
        self.momentum_threshold = momentum_threshold
        self.exit_threshold = exit_threshold
        self.order_hold = order_hold
        self.price_add = price_add
        self._last_matched = {}
        self.pnl = 0.0

    def add_market(self, market):
        self.log.info(
            f"[add_market] {market.market_id} {market.event_name} type={market.event_type_id}"
        )

    def check_market_book(self, market, market_book):
        self.log.debug(
            f"[check_market_book] {market.market_id} status={market_book.status} inplay={market_book.inplay}"
        )
        return market_book.status == "OPEN" and market_book.inplay

    def _price_now(self, runner):
        return runner.last_price_traded

    def best_prices_for_runner(self, runner):
        ex = getattr(runner, "ex", None)
        atb = getattr(ex, "available_to_back", []) or []
        atl = getattr(ex, "available_to_lay", []) or []

        def _extract(item):
            if isinstance(item, dict):
                return item.get("price"), item.get("size")
            return getattr(item, "price", None), getattr(item, "size", None)

        bb_price, bb_size = _extract(atb[0]) if atb else (None, None)
        bl_price, bl_size = _extract(atl[0]) if atl else (None, None)
        return bb_price, bb_size, bl_price, bl_size

    def matched_summary(self, runner, market):
        back_total = lay_total = 0.0
        back_weighted = lay_weighted = 0.0

        for order in market.blotter:
            if order.selection_id != runner.selection_id:
                continue
            size = float(getattr(order, "size_matched", 0) or 0)
            if size <= 0:
                continue
            price = float(getattr(order, "average_price_matched", 0) or 0)
            side = str(getattr(order, "side", "")).upper()
            if "BACK" in side:
                back_total += size
                back_weighted += size * price
            elif "LAY" in side:
                lay_total += size
                lay_weighted += size * price

        avg_back = back_weighted / back_total if back_total else 0.0
        avg_lay = lay_weighted / lay_total if lay_total else 0.0
        return back_total, avg_back, lay_total, avg_lay

    def process_market_book(self, market, market_book):
        try:
            self.log.info(
                f"[process_market_book] {market.market_id} @ {market_market_time(market_book)} "
                f"inplay={market_book.inplay}"
            )

            for runner in market_book.runners:
                context = f"->{market.market_id} {runner.selection_id} "
                runner_context = self.get_runner_context(
                    market.market_id, runner.selection_id, runner.handicap
                )

                ltp = self._price_now(runner)
                if not ltp:
                    self.log.debug(f"No LTP for {context}, skipping")
                    continue

                best_back, back_size, best_lay, lay_size = self.best_prices_for_runner(runner)
                if best_back is None or best_lay is None:
                    self.log.debug(f"Missing book prices for {context}, skipping")
                    continue

                imbalance = 0.0
                total_size = (back_size or 0) + (lay_size or 0)
                if total_size > 0:
                    imbalance = (back_size - lay_size) / total_size

                hist = self.imbalance_history[runner.selection_id]
                prev_imbalance = hist[-1] if hist else 0.0
                hist.append(imbalance)
                momentum = imbalance - prev_imbalance

                self.log.debug(
                    f"{context} ltp={ltp:.2f} imbalance={imbalance:.2f} "
                    f"momentum={momentum:.3f} best_back={best_back:.2f} "
                    f"best_lay={best_lay:.2f}"
                )

                back_total, avg_back, lay_total, avg_lay = self.matched_summary(runner, market)

                if self._should_enter(imbalance, momentum, ltp, runner_context, back_total):
                    self._enter_trade(runner, market, market_book, ltp, best_back, context)

                if self._should_hedge(back_total, ltp, momentum):
                    self._hedge_trade(runner, market, market_book, context, back_total, best_lay)

        except Exception as exc:
            tb = traceback.format_exc()
            self.log.warning(f"Failed to process market book: {exc}\n{tb}")

    def _should_enter(self, imbalance, momentum, ltp, runner_context, back_total):
        if back_total > 0:
            return False
        if runner_context.live_trade_count > 0:
            return False
        if ltp >= self.enter_threshold:
            return False
        if imbalance < self.imbalance_threshold:
            return False
        if momentum < self.momentum_threshold:
            return False
        return True

    def _enter_trade(self, runner, market, market_book, ltp, best_back, context):
        price = round((best_back or ltp) + self.price_add, 2)
        trade = Trade(
            market_book.market_id,
            runner.selection_id,
            runner.handicap,
            self,
            notes={"entry_px": price},
        )
        order = trade.create_order(
            side="BACK",
            order_type=LimitOrder(price, self.context["stake"]),
        )
        try:
            self.log.info(f"Entering BACK trade {context} price={price} stake={self.context['stake']}")
            market.place_order(order)
        except Exception as exc:
            self.log.warning(f"Failed to place entry order for {context}: {exc}")

    def _should_hedge(self, back_total, ltp, momentum):
        if back_total <= 0:
            return False
        if ltp >= self.exit_threshold:
            return True
        if momentum < -0.2:
            return True
        return False

    def _hedge_trade(self, runner, market, market_book, context, back_total, best_lay):
        size = max(1, int(back_total))
        price = round((best_lay or (self.exit_threshold + 0.1)), 2)
        try:
            trade = Trade(market_book.market_id, runner.selection_id, runner.handicap, self)
            order = trade.create_order("LAY", order_type=LimitOrder(price, size))
            self.log.warning(f"Hedging {context} size={size} price={price}")
            market.place_order(order)
        except Exception as exc:
            self.log.warning(f"Failed to place hedge order for {context}: {exc}")

    def process_orders(self, market, orders):
        for order in orders:
            if order.status == OrderStatus.EXECUTABLE and order.elapsed_seconds and order.elapsed_seconds > self.order_hold:
                market.cancel_order(order)

        try:
            for order in orders:
                prev = self._last_matched.get(order.id, 0)
                curr = order.size_matched or 0
                if curr > prev:
                    inc = curr - prev
                    side = order.side
                    mid = market.market_id
                    sid = getattr(order.trade, "selection_id", None)
                    self.log.warning(
                        f"⚡⚡ Order FILL | {side:<4} | runner={sid} | market={mid} | "
                        f"{inc:.2f} matched @ {order.average_price_matched:.2f} ({curr:.2f} total)"
                    )
                self._last_matched[order.id] = curr
        except Exception as exc:
            tb = traceback.format_exc()
            self.log.warning(f"Failed to print order fill state: {exc}\n{tb}")

    def process_closed_market(self, market, market_book):
        self.pnl = 0.0
        self.log.info(f"Processing closed market: {market.event_name}, {market.market_id}")
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
    return market_book.publish_time or "UNKNOWN_TIME"


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

now_utc = datetime.now(timezone.utc) - timedelta(hours=24)
to_utc = now_utc + timedelta(hours=48)

catalogue_filter = market_filter(
    event_type_ids=["4"],
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

markets_to_trade = [m.market_id for m in catalogues]
print(f"Streaming filter marketIds: {markets_to_trade}")

streaming_filter = {
    "marketIds": markets_to_trade,
}

STREAM_FIELDS = ["EX_MARKET_DEF", "EX_BEST_OFFERS", "EX_TRADED", "EX_LTP"]
LADDER_LEVELS = 3
stream_data = streaming_market_data_filter(
    fields=STREAM_FIELDS, ladder_levels=LADDER_LEVELS
)

client = clients.BetfairClient(trading, paper_trade=False)
framework = Flumine(client)

strategy = LiquidityMomentumStrat(
    market_filter=streaming_filter,
    market_data_filter=stream_data,
    max_order_exposure=30,
    max_selection_exposure=20,
    context={"stake": 2},
    enter_threshold=2.0,
    imbalance_threshold=0.6,
    momentum_threshold=0.12,
    exit_threshold=5.0,
    order_hold=17,
    price_add=0.00,
    log_root="./logs/live_prod/",
    log_level="I",
)

framework.add_strategy(strategy)
print("just framework.run is left ..")
framework.run()
