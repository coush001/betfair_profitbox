#!/root/betfair_profitbox/.venv/bin/python
import os
import csv
import traceback
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

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

CONFIG_PATH = Path("/root/betfair_profitbox/configs.csv")
CONFIG_FIELDS = [
    "timestamp",
    "enter_threshold",
    "anomaly_threshold",
    "imbalance_threshold",
    "exit_threshold",
    "price_add",
    "order_hold",
    "stake",
    "adaptive_step",
    "avg_recent_pnl",
    "recent_market_count",
    "note",
]

DEFAULT_CONFIG = {
    "enter_threshold": 2.5,
    "anomaly_threshold": 2.0,
    "imbalance_threshold": 0.5,
    "exit_threshold": 5.0,
    "price_add": 0.00,
    "order_hold": 17,
    "stake": 2,
    "adaptive_step": 0.1,
}


def load_latest_config():
    if not CONFIG_PATH.exists():
        return {}
    with CONFIG_PATH.open("r", newline="") as f:
        reader = csv.DictReader(f)
        rows = [r for r in reader if any(r.values())]
    if not rows:
        return {}
    return rows[-1]


def parse_config(row):
    result = {}
    for key, value in DEFAULT_CONFIG.items():
        raw = row.get(key)
        if raw is None or raw == "":
            result[key] = value
            continue
        try:
            if isinstance(value, int):
                result[key] = int(float(raw))
            else:
                result[key] = float(raw)
        except Exception:
            result[key] = value
    return result


def save_config_row(row):
    is_new = not CONFIG_PATH.exists()
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CONFIG_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


class LatentMicrostructureStrat(BaseStrategy):
    def __init__(
        self,
        enter_threshold,
        anomaly_threshold,
        imbalance_threshold,
        exit_threshold,
        order_hold,
        price_add,
        adaptive_step,
        log_root,
        log_level,
        *a,
        **k,
    ):
        self.log = build_logger(log_root, log_level)
        super().__init__(name="latent_microstructure", *a, **k)
        self.imbalance_history = defaultdict(lambda: deque(maxlen=32))
        self.state_history = defaultdict(lambda: deque(maxlen=32))
        self.last_price = {}
        self.last_book = {}
        self.enter_threshold = enter_threshold
        self.anomaly_threshold = anomaly_threshold
        self.imbalance_threshold = imbalance_threshold
        self.exit_threshold = exit_threshold
        self.order_hold = order_hold
        self.price_add = price_add
        self.adaptive_step = adaptive_step
        self._last_matched = {}
        self.pnl = 0.0
        self.recent_market_results = deque(maxlen=12)
        self.total_closed_markets = 0

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

    def _book_features(self, runner):
        best_back, back_size, best_lay, lay_size = self.best_prices_for_runner(runner)
        if best_back is None or best_lay is None:
            return None

        total_size = (back_size or 0) + (lay_size or 0)
        imbalance = 0.0
        if total_size > 0:
            imbalance = ((back_size or 0) - (lay_size or 0)) / total_size

        spread = max(0.0, (best_lay - best_back))

        prev_back, prev_lay = self.last_book.get(runner.selection_id, (None, None))
        churn = 0.0
        if prev_back is not None and prev_lay is not None:
            churn = abs((back_size or 0) - prev_back) + abs((lay_size or 0) - prev_lay)

        self.last_book[runner.selection_id] = (back_size or 0, lay_size or 0)
        return imbalance, spread, churn

    def _state_vector(self, runner):
        ltp = self._price_now(runner)
        if ltp is None:
            return None

        imbalance_spread = self._book_features(runner)
        if imbalance_spread is None:
            return None

        imbalance, spread, churn = imbalance_spread
        last_price = self.last_price.get(runner.selection_id, ltp)
        price_delta = ltp - last_price
        self.last_price[runner.selection_id] = ltp

        return imbalance, spread, churn, price_delta

    def _anomaly_score(self, runner, state):
        history = self.state_history[runner.selection_id]
        if len(history) < 10:
            return 0.0

        feature_means = [0.0, 0.0, 0.0, 0.0]
        feature_vars = [0.0, 0.0, 0.0, 0.0]
        n = len(history)
        for vec in history:
            for idx, value in enumerate(vec):
                feature_means[idx] += value
        for idx in range(4):
            feature_means[idx] /= n
        for vec in history:
            for idx, value in enumerate(vec):
                feature_vars[idx] += (value - feature_means[idx]) ** 2
        for idx in range(4):
            feature_vars[idx] = feature_vars[idx] / n if n else 0.0

        score = 0.0
        z = []
        for idx, value in enumerate(state):
            sigma = feature_vars[idx] ** 0.5
            zscore = (value - feature_means[idx]) / (sigma + 1e-6)
            z.append(zscore)

        # Anomaly is strong back-side pressure, tightening spread, and book churn.
        score = max(z[0], z[2], -z[1], z[3])
        return score

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

                state = self._state_vector(runner)
                if not state:
                    self.log.debug(f"No valid state for {context}, skipping")
                    continue

                self.state_history[runner.selection_id].append(state)
                anomaly_score = self._anomaly_score(runner, state)
                imbalance, spread, churn, price_delta = state
                ltp = self._price_now(runner)

                self.log.debug(
                    f"{context} ltp={ltp:.2f} imbalance={imbalance:.2f} spread={spread:.2f} "
                    f"churn={churn:.2f} price_delta={price_delta:.2f} "
                    f"anomaly={anomaly_score:.2f}"
                )

                back_total, avg_back, lay_total, avg_lay = self.matched_summary(runner, market)

                if self._should_enter(anomaly_score, imbalance, ltp, runner_context, back_total):
                    self._enter_trade(runner, market, market_book, ltp, context)

                if self._should_hedge(back_total, ltp, imbalance, price_delta):
                    self._hedge_trade(runner, market, market_book, context, back_total)

        except Exception as exc:
            tb = traceback.format_exc()
            self.log.warning(f"Failed to process market book: {exc}\n{tb}")

    def _should_enter(self, anomaly_score, imbalance, ltp, runner_context, back_total):
        if back_total > 0:
            return False
        if runner_context.live_trade_count > 0:
            return False
        if ltp >= self.enter_threshold:
            return False
        if anomaly_score < self.anomaly_threshold:
            return False
        if imbalance < self.imbalance_threshold:
            return False
        return True

    def _enter_trade(self, runner, market, market_book, ltp, context):
        best_back, _, best_lay, _ = self.best_prices_for_runner(runner)
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
            self.log.info(f"Entering latent entry {context} price={price} stake={self.context['stake']}")
            market.place_order(order)
        except Exception as exc:
            self.log.warning(f"Failed to place entry order for {context}: {exc}")

    def _should_hedge(self, back_total, ltp, imbalance, price_delta):
        if back_total <= 0:
            return False
        if ltp >= self.exit_threshold:
            return True
        if imbalance < -0.2:
            return True
        if price_delta > 0.5:
            return True
        return False

    def _hedge_trade(self, runner, market, market_book, context, back_total):
        size = max(1, int(back_total))
        _, _, best_lay, _ = self.best_prices_for_runner(runner)
        price = round((best_lay or (self.exit_threshold + 0.1)), 2)
        try:
            trade = Trade(market_book.market_id, runner.selection_id, runner.handicap, self)
            order = trade.create_order("LAY", order_type=LimitOrder(price, size))
            self.log.warning(f"Hedging latent exposure {context} size={size} price={price}")
            market.place_order(order)
        except Exception as exc:
            self.log.warning(f"Failed to place hedge order for {context}: {exc}")

    def _tune_on_performance(self):
        if len(self.recent_market_results) < 4:
            return

        if self.total_closed_markets % 4 != 0:
            return

        avg_pnl = sum(self.recent_market_results) / len(self.recent_market_results)
        note = None
        if avg_pnl < 0:
            self.enter_threshold = min(self.enter_threshold + self.adaptive_step, 6.0)
            self.anomaly_threshold = min(self.anomaly_threshold + self.adaptive_step, 10.0)
            self.imbalance_threshold = min(self.imbalance_threshold + 0.05, 0.95)
            self.exit_threshold = max(self.exit_threshold - 0.2, 2.5)
            note = "conservative"
        elif avg_pnl > 0.5:
            self.enter_threshold = max(self.enter_threshold - self.adaptive_step, 1.4)
            self.anomaly_threshold = max(self.anomaly_threshold - self.adaptive_step, 1.5)
            self.imbalance_threshold = max(self.imbalance_threshold - 0.05, 0.3)
            self.exit_threshold = min(self.exit_threshold + 0.2, 8.0)
            note = "aggressive"

        if note:
            self.log.info(
                f"Tuning parameters after {self.total_closed_markets} closed markets: "
                f"avg_pnl={avg_pnl:.2f} -> enter={self.enter_threshold:.2f}, "
                f"anomaly={self.anomaly_threshold:.2f}, imbalance={self.imbalance_threshold:.2f}, "
                f"exit={self.exit_threshold:.2f}"
            )
            save_config_row({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "enter_threshold": self.enter_threshold,
                "anomaly_threshold": self.anomaly_threshold,
                "imbalance_threshold": self.imbalance_threshold,
                "exit_threshold": self.exit_threshold,
                "price_add": self.price_add,
                "order_hold": self.order_hold,
                "stake": self.context["stake"],
                "adaptive_step": self.adaptive_step,
                "avg_recent_pnl": avg_pnl,
                "recent_market_count": len(self.recent_market_results),
                "note": note,
            })

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
        self.recent_market_results.append(self.pnl)
        self.total_closed_markets += 1
        self._tune_on_performance()
        self.log.warning(
            f"Total pnl for market:{market.event_name}, {market.market_id}, : PNL :: {self.pnl}"
        )


def market_market_time(market_book):
    return market_book.publish_time or "UNKNOWN_TIME"


load_dotenv()
USERNAME = os.getenv("BETFAIR_USERNAME")
APP_KEY = os.getenv("BETFAIR_APP_KEY")
PASSWORD = os.getenv("BETFAIR_PASSWORD")

current_config = parse_config(load_latest_config())

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

strategy = LatentMicrostructureStrat(
    market_filter=streaming_filter,
    market_data_filter=stream_data,
    max_order_exposure=30,
    max_selection_exposure=20,
    context={"stake": current_config["stake"]},
    enter_threshold=current_config["enter_threshold"],
    anomaly_threshold=current_config["anomaly_threshold"],
    imbalance_threshold=current_config["imbalance_threshold"],
    exit_threshold=current_config["exit_threshold"],
    order_hold=current_config["order_hold"],
    price_add=current_config["price_add"],
    adaptive_step=current_config["adaptive_step"],
    log_root="./logs/live_prod/",
    log_level="I",
)

framework.add_strategy(strategy)

save_config_row({
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "enter_threshold": current_config["enter_threshold"],
    "anomaly_threshold": current_config["anomaly_threshold"],
    "imbalance_threshold": current_config["imbalance_threshold"],
    "exit_threshold": current_config["exit_threshold"],
    "price_add": current_config["price_add"],
    "order_hold": current_config["order_hold"],
    "stake": current_config["stake"],
    "adaptive_step": current_config["adaptive_step"],
    "avg_recent_pnl": 0.0,
    "recent_market_count": 0,
    "note": "startup",
})

print("just framework.run is left ..")
framework.run()
