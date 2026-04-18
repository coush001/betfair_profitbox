#!/root/betfair_profitbox/.venv/bin/python
import os
import sys
import time
import csv
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import betfairlightweight as bflw
from betfairlightweight.filters import market_filter, price_projection
from dotenv import load_dotenv


def build_logger():
    logger = logging.getLogger("arbstrat")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    if not logger.handlers:
        logger.addHandler(handler)
    return logger


class ArbStrat:
    EVENT_TYPE_IDS = ["1", "2", "3", "4", "5", "6", "7", "8", "10", "11"]
    MARKET_COUNTRIES = ["GB", "AU", "IE", "US", "NZ", "ZA"]
    MARKET_TYPE_CODES = ["MATCH_ODDS"]
    SCAN_WINDOW_HOURS = 48
    MAX_MARKETS_PER_TYPE = 80
    MIN_STAKE = 1.0
    MIN_ARBITRAGE_EDGE = 0.005
    DEFAULT_SCAN_INTERVAL_SECONDS = 250
    HOT_SPORT_SCAN_SECONDS = 20
    MID_SPORT_SCAN_SECONDS = 25
    HIGH_ACTIVITY_SCAN_SECONDS = 10
    DAILY_LOOKBACK_HOURS = 24
    WEEK_LOOKBACK_DAYS = 7
    CUSTOMER_STRATEGY_REF = "arbstrat"
    ARB_SPORTS_PATH = Path("/root/betfair_profitbox/arb_sports.csv")
    ARB_SPORTS_FIELDS = [
        "timestamp",
        "event_type_id",
        "event_type_name",
        "arb_count",
        "daily_arb_count",
        "weekly_arb_count",
        "interval_seconds",
    ]
    EVENT_TYPE_LABELS = {
        "1": "Soccer",
        "2": "Tennis",
        "3": "Cricket",
        "4": "Unknown",
        "5": "Unknown",
        "6": "Unknown",
        "7": "Unknown",
        "8": "Unknown",
        "10": "Unknown",
        "11": "Unknown",
    }

    def __init__(self, api_client, logger):
        self.api_client = api_client
        self.logger = logger
        self.last_scan = None
        self.arb_log_entries = self._load_arb_log()
        self.next_scan_by_event_type = {
            event_type: datetime.now(timezone.utc)
            for event_type in self.EVENT_TYPE_IDS
        }

    def run(self):
        self.logger.info("Starting arbstrat scanner")
        while True:
            now_utc = datetime.now(timezone.utc)
            due_event_types = [
                et for et, next_t in self.next_scan_by_event_type.items() if next_t <= now_utc
            ]
            if not due_event_types:
                next_due = min(self.next_scan_by_event_type.values())
                sleep_seconds = max(1, (next_due - now_utc).total_seconds())
                self.logger.info("No event types due; sleeping %s seconds", int(sleep_seconds))
                time.sleep(sleep_seconds)
                continue

            for event_type in due_event_types:
                try:
                    self.scan_event_type(event_type, now_utc)
                except Exception as exc:
                    self.logger.warning("Event type %s scan failed: %s", event_type, exc)
            self.last_scan = datetime.now(timezone.utc)

    def scan_all_markets(self):
        now_utc = datetime.now(timezone.utc)
        self.logger.info("Scanning markets from %s to %s", now_utc.isoformat(), (now_utc + timedelta(hours=self.SCAN_WINDOW_HOURS)).isoformat())

        for event_type in self.EVENT_TYPE_IDS:
            try:
                self.scan_event_type(event_type, now_utc)
            except Exception as exc:
                self.logger.warning("Event type %s scan failed: %s", event_type, exc)

        self.last_scan = datetime.now(timezone.utc)

    def scan_event_type(self, event_type_id, from_dt):
        self.logger.info("Scanning event type %s", event_type_id)
        to_dt = from_dt + timedelta(hours=self.SCAN_WINDOW_HOURS)
        catalogue_filter = market_filter(
            event_type_ids=[event_type_id],
            market_type_codes=self.MARKET_TYPE_CODES,
            market_countries=self.MARKET_COUNTRIES,
            market_start_time={
                "from": from_dt.isoformat(),
                "to": to_dt.isoformat(),
            },
        )

        catalogues = self.api_client.betting.list_market_catalogue(
            filter=catalogue_filter,
            max_results=self.MAX_MARKETS_PER_TYPE,
            market_projection=["EVENT", "MARKET_START_TIME", "RUNNER_DESCRIPTION"],
        )

        self.logger.info("Found %d markets for event type %s", len(catalogues), event_type_id)
        found_arbs = 0
        for market in catalogues:
            runner_count = len(getattr(market, "runners", []))
            if runner_count == 0 or runner_count < 2:
                continue
            found_arbs += self.scan_market(market)

        daily_count = self._daily_arb_count(event_type_id) + found_arbs
        weekly_count = self._weekly_arb_count(event_type_id) + found_arbs
        interval = self._interval_for_recent_counts(daily_count, weekly_count)
        self._append_arb_log(
            event_type_id=event_type_id,
            event_type_name=self.EVENT_TYPE_LABELS.get(event_type_id, "Unknown"),
            arb_count=found_arbs,
            daily_arb_count=daily_count,
            weekly_arb_count=weekly_count,
            interval=interval,
        )
        self.next_scan_by_event_type[event_type_id] = datetime.now(timezone.utc) + timedelta(seconds=interval)
        self.logger.info(
            "Event type %s found %d arbs; recent week total=%d; next scan in %s seconds",
            event_type_id,
            found_arbs,
            weekly_count,
            interval,
        )

    def scan_market(self, market):
        market_id = market.market_id
        self.logger.debug("Scanning market %s (%s)", market_id, getattr(market.event, "name", market_id))

        runner_name_map = {
            runner.selection_id: runner.runner_name
            for runner in getattr(market, "runners", [])
            if getattr(runner, "selection_id", None) is not None
        }

        projection = price_projection(
            price_data=["EX_BEST_OFFERS"],
            virtualise=True,
            rollover_stakes=True,
        )

        books = self.api_client.betting.list_market_book(
            market_ids=[market_id],
            price_projection=projection,
            order_projection="ALL",
            match_projection="NO_ROLLUP",
        )
        if not books:
            return 0

        book = books[0]
        runners = [r for r in book.runners if r.status == "ACTIVE"]
        found_count = 0
        if len(runners) == 2:
            for back_runner, lay_runner in [(runners[0], runners[1]), (runners[1], runners[0])]:
                back_name = runner_name_map.get(back_runner.selection_id, str(back_runner.selection_id))
                lay_name = runner_name_map.get(lay_runner.selection_id, str(lay_runner.selection_id))
                arb = self.check_binary_arbitrage(back_runner, lay_runner, back_name, lay_name)
                if arb:
                    self.logger.info("Binary arb found in market %s: %s", market_id, arb)
                    self.execute_arbitrage(market_id, arb, getattr(market.event, "name", market_id))
                    found_count += 1
        elif len(runners) >= 3:
            runner_names = [
                runner_name_map.get(runner.selection_id, str(runner.selection_id))
                for runner in runners
            ]
            arb_multi = self.check_multi_outcome_arbitrage(runners, runner_names)
            if arb_multi:
                self.logger.info("Multi-outcome arb found in market %s: %s", market_id, arb_multi)
                self.execute_multi_outcome_arbitrage(market_id, arb_multi, getattr(market.event, "name", market_id))
                found_count += 1
        return found_count

    def check_binary_arbitrage(self, back_runner, lay_runner, back_runner_name, lay_runner_name):
        back_price, back_size = self.get_best_back(back_runner)
        lay_price, lay_size = self.get_best_lay(lay_runner)
        if back_price is None or lay_price is None:
            return None

        edge = (back_price - 1.0) * (lay_price - 1.0) - 1.0
        if edge < self.MIN_ARBITRAGE_EDGE:
            return None

        max_back_stake = back_size
        max_lay_stake = lay_size
        if lay_price - 1.0 <= 0:
            return None

        stake_back = min(max_back_stake, max_lay_stake * (lay_price - 1.0) / (back_price - 1.0))
        if stake_back < self.MIN_STAKE:
            return None

        stake_lay = stake_back * (back_price - 1.0) / (lay_price - 1.0)
        profit_if_back_wins = stake_back * (back_price - 1.0) + stake_lay
        profit_if_lay_wins = -stake_back + stake_lay * (lay_price - 1.0)

        if profit_if_back_wins < 0 or profit_if_lay_wins < 0:
            return None

        return {
            "back_runner_id": back_runner.selection_id,
            "back_runner_name": back_runner_name,
            "lay_runner_id": lay_runner.selection_id,
            "lay_runner_name": lay_runner_name,
            "back_price": back_price,
            "lay_price": lay_price,
            "stake_back": round(stake_back, 2),
            "stake_lay": round(stake_lay, 2),
            "edge": round(edge, 6),
            "profit_back": round(profit_if_back_wins, 2),
            "profit_lay": round(profit_if_lay_wins, 2),
        }

    def get_best_back(self, runner):
        ex = getattr(runner, "ex", None)
        if not ex:
            return None, None
        available_to_back = getattr(ex, "available_to_back", []) or []
        if not available_to_back:
            return None, None
        price = available_to_back[0].price if hasattr(available_to_back[0], "price") else available_to_back[0].get("price")
        size = available_to_back[0].size if hasattr(available_to_back[0], "size") else available_to_back[0].get("size")
        return float(price), float(size)

    def get_best_lay(self, runner):
        ex = getattr(runner, "ex", None)
        if not ex:
            return None, None
        available_to_lay = getattr(ex, "available_to_lay", []) or []
        if not available_to_lay:
            return None, None
        price = available_to_lay[0].price if hasattr(available_to_lay[0], "price") else available_to_lay[0].get("price")
        size = available_to_lay[0].size if hasattr(available_to_lay[0], "size") else available_to_lay[0].get("size")
        return float(price), float(size)

    def _load_arb_log(self):
        if not self.ARB_SPORTS_PATH.exists():
            return []
        with self.ARB_SPORTS_PATH.open("r", newline="") as f:
            reader = csv.DictReader(f)
            entries = []
            for row in reader:
                event_type_id = row.get("event_type_id")
                if not event_type_id:
                    continue
                timestamp = row.get("timestamp")
                try:
                    timestamp_dt = datetime.fromisoformat(timestamp)
                except Exception:
                    continue
                entries.append({
                    "timestamp": timestamp_dt,
                    "event_type_id": event_type_id,
                    "event_type_name": row.get("event_type_name", "Unknown"),
                    "arb_count": int(row.get("arb_count", "0") or 0),
                    "daily_arb_count": int(row.get("daily_arb_count", "0") or 0),
                    "weekly_arb_count": int(row.get("weekly_arb_count", "0") or 0),
                    "interval_seconds": int(row.get("interval_seconds", self.DEFAULT_SCAN_INTERVAL_SECONDS) or self.DEFAULT_SCAN_INTERVAL_SECONDS),
                })
            return entries

    def _append_arb_log(self, event_type_id, event_type_name, arb_count, daily_arb_count, weekly_arb_count, interval):
        self.ARB_SPORTS_PATH.parent.mkdir(parents=True, exist_ok=True)
        row_timestamp = datetime.now(timezone.utc)
        row = {
            "timestamp": row_timestamp.isoformat(),
            "event_type_id": event_type_id,
            "event_type_name": event_type_name,
            "arb_count": arb_count,
            "daily_arb_count": daily_arb_count,
            "weekly_arb_count": weekly_arb_count,
            "interval_seconds": interval,
        }
        file_exists = self.ARB_SPORTS_PATH.exists()
        with self.ARB_SPORTS_PATH.open("a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.ARB_SPORTS_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        self.arb_log_entries.append({
            "timestamp": row_timestamp,
            "event_type_id": event_type_id,
            "event_type_name": event_type_name,
            "arb_count": arb_count,
            "daily_arb_count": daily_arb_count,
            "weekly_arb_count": weekly_arb_count,
            "interval_seconds": interval,
        })

    def _daily_arb_count(self, event_type_id):
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.DAILY_LOOKBACK_HOURS)
        return sum(
            entry["arb_count"]
            for entry in self.arb_log_entries
            if entry["event_type_id"] == event_type_id and entry["timestamp"] >= cutoff
        )

    def _weekly_arb_count(self, event_type_id):
        cutoff = datetime.now(timezone.utc) - timedelta(days=self.WEEK_LOOKBACK_DAYS)
        return sum(
            entry["arb_count"]
            for entry in self.arb_log_entries
            if entry["event_type_id"] == event_type_id and entry["timestamp"] >= cutoff
        )

    def _interval_for_recent_counts(self, daily_count, weekly_count):
        if daily_count >= 40 or weekly_count >= 250:
            return self.HIGH_ACTIVITY_SCAN_SECONDS
        if daily_count >= 15 or weekly_count >= 100:
            return self.HOT_SPORT_SCAN_SECONDS
        if daily_count >= 5 or weekly_count >= 30:
            return self.MID_SPORT_SCAN_SECONDS
        if daily_count >= 1 or weekly_count >= 1:
            return 150
        return self.DEFAULT_SCAN_INTERVAL_SECONDS

    def check_multi_outcome_arbitrage(self, runners, runner_names):
        prices = []
        sizes = []
        for runner in runners:
            best_back, best_size = self.get_best_back(runner)
            if best_back is None or best_size is None or best_size <= 0:
                return None
            prices.append(best_back)
            sizes.append(best_size)

        inv_sum = sum(1.0 / p for p in prices)
        if inv_sum >= 1.0:
            return None

        max_T = float("inf")
        for price, size in zip(prices, sizes):
            stake_allowed = size * inv_sum * price
            max_T = min(max_T, stake_allowed)
        if max_T < self.MIN_STAKE:
            return None

        stakes = []
        for price in prices:
            stakes.append(round(max_T * (1.0 / price) / inv_sum, 2))

        profit_per_outcome = []
        total_stake = sum(stakes)
        for stake, price in zip(stakes, prices):
            profit_per_outcome.append(round(stake * (price - 1.0) - (total_stake - stake), 2))
        profit = min(profit_per_outcome)
        if profit <= 0:
            return None

        return {
            "type": "multi",
            "runner_ids": [r.selection_id for r in runners],
            "runner_names": runner_names,
            "prices": prices,
            "stakes": stakes,
            "edge": round(1.0 / inv_sum - 1.0, 6),
            "profit": round(profit, 2),
        }

    def execute_multi_outcome_arbitrage(self, market_id, arb, event_name):
        instructions = []
        for runner_id, price, stake in zip(arb["runner_ids"], arb["prices"], arb["stakes"]):
            instructions.append({
                "selectionId": runner_id,
                "handicap": 0,
                "side": "BACK",
                "orderType": "LIMIT",
                "limitOrder": {
                    "price": price,
                    "size": stake,
                    "persistenceType": "LAPSE",
                },
            })

        customer_ref = f"arbstrat-multi-{market_id}-{int(time.time())}"
        self.logger.warning(
            "Placing multi-outcome arb in market %s (%s): edge=%s profit=%s",
            market_id,
            event_name,
            arb["edge"],
            arb["profit"],
        )
        try:
            result = self.api_client.betting.place_orders(
                market_id=market_id,
                instructions=instructions,
                customer_ref=customer_ref,
                customer_strategy_ref=self.CUSTOMER_STRATEGY_REF,
            )
            self.logger.info("Multi arb place_orders result: %s", result)
            self._protect_arb_orders(market_id, customer_ref, event_name, result)
        except Exception as exc:
            self.logger.exception("Failed to place multi-outcome arbitrage orders: %s", exc)

    def execute_arbitrage(self, market_id, arb, event_name):
        instructions = [
            {
                "selectionId": arb["back_runner_id"],
                "handicap": 0,
                "side": "BACK",
                "orderType": "LIMIT",
                "limitOrder": {
                    "price": arb["back_price"],
                    "size": arb["stake_back"],
                    "persistenceType": "LAPSE",
                },
            },
            {
                "selectionId": arb["lay_runner_id"],
                "handicap": 0,
                "side": "LAY",
                "orderType": "LIMIT",
                "limitOrder": {
                    "price": arb["lay_price"],
                    "size": arb["stake_lay"],
                    "persistenceType": "LAPSE",
                },
            },
        ]
        customer_ref = f"arbstrat-{market_id}-{int(time.time())}"
        try:
            self.logger.info(
                "Placing arb orders for market %s (%s): back %s @%s, lay %s @%s",
                market_id,
                event_name,
                arb["back_runner_name"],
                arb["back_price"],
                arb["lay_runner_name"],
                arb["lay_price"],
            )
            result = self.api_client.betting.place_orders(
                market_id=market_id,
                instructions=instructions,
                customer_ref=customer_ref,
                customer_strategy_ref=self.CUSTOMER_STRATEGY_REF,
            )
            self.logger.info("Arb place_orders result: %s", result)
            self._protect_arb_orders(market_id, customer_ref, event_name, result)
        except Exception as exc:
            self.logger.exception("Failed to place arbitrage orders: %s", exc)

    def _protect_arb_orders(self, market_id, customer_ref, event_name, place_result=None):
        time.sleep(1)
        current_orders = self._get_current_orders(customer_ref)
        place_reports = getattr(place_result, "place_instruction_reports", []) if place_result else []

        if not current_orders and not place_reports:
            self.logger.warning("No order information available for arb customer_ref=%s", customer_ref)
            return

        net_exposure = {}
        for order in current_orders:
            if order.size_matched:
                delta = order.size_matched if order.side == "BACK" else -order.size_matched
                net_exposure[order.selection_id] = net_exposure.get(order.selection_id, 0.0) + delta

        current_selection_ids = {order.selection_id for order in current_orders}
        for report in place_reports:
            if not report.instruction or not report.size_matched:
                continue
            selection_id = report.instruction.selection_id
            if selection_id in current_selection_ids:
                continue
            delta = report.size_matched if report.instruction.side == "BACK" else -report.size_matched
            net_exposure[selection_id] = net_exposure.get(selection_id, 0.0) + delta

        total_matched = sum(abs(v) for v in net_exposure.values())
        if total_matched == 0:
            self.logger.info("No matched arb legs for market %s yet", market_id)
            return

        back_remaining = sum(o.size_remaining for o in current_orders if o.side == "BACK")
        lay_remaining = sum(o.size_remaining for o in current_orders if o.side == "LAY")
        if back_remaining > 0 or lay_remaining > 0:
            self.logger.warning(
                "Cancelling open arb orders for market %s (%s): back_remaining=%s lay_remaining=%s",
                market_id,
                event_name,
                back_remaining,
                lay_remaining,
            )
            self._cancel_open_orders(market_id, current_orders)

        for selection_id, net in net_exposure.items():
            if net == 0:
                continue
            if net > 0:
                self._hedge_exposure(market_id, "LAY", net, selection_id)
            else:
                self._hedge_exposure(market_id, "BACK", -net, selection_id)

    def _get_current_orders(self, customer_ref):
        try:
            current = self.api_client.betting.list_current_orders(
                customer_order_refs=[customer_ref], order_projection="ALL"
            )
            return getattr(current, "orders", []) or []
        except Exception as exc:
            self.logger.exception("Failed to list current orders for %s: %s", customer_ref, exc)
            return []

    def _cancel_open_orders(self, market_id, orders):
        cancel_ids = [o.bet_id for o in orders if o.size_remaining and o.bet_id]
        if not cancel_ids:
            self.logger.info("No open orders to cancel for market %s", market_id)
            return
        try:
            result = self.api_client.betting.cancel_orders(
                market_id=market_id,
                instructions=[{"betId": bet_id} for bet_id in cancel_ids],
            )
            self.logger.info("Cancelled open arb orders for market %s: %s", market_id, result)
        except Exception as exc:
            self.logger.exception("Failed to cancel open orders for market %s: %s", market_id, exc)

    def _hedge_exposure(self, market_id, side, size, selection_id):
        if size <= 0:
            return
        self.logger.warning(
            "Hedging exposure on market %s for runner %s with side %s size %s",
            market_id,
            selection_id,
            side,
            size,
        )
        projection = price_projection(
            price_data=["EX_BEST_OFFERS"],
            virtualise=True,
            rollover_stakes=True,
        )
        books = self.api_client.betting.list_market_book(
            market_ids=[market_id],
            price_projection=projection,
            order_projection="ALL",
            match_projection="NO_ROLLUP",
        )
        if not books:
            self.logger.warning("No market book available for hedge on market %s", market_id)
            return

        book = books[0]
        runner_book = next((r for r in book.runners if r.selection_id == selection_id), None)
        if not runner_book:
            self.logger.warning(
                "Runner %s not found in market book for hedge on market %s",
                selection_id,
                market_id,
            )
            return

        best_price = None
        if side == "BACK":
            available = getattr(runner_book.ex, "available_to_back", []) or []
            if available:
                best_price = available[0].price if hasattr(available[0], "price") else available[0].get("price")
        else:
            available = getattr(runner_book.ex, "available_to_lay", []) or []
            if available:
                best_price = available[0].price if hasattr(available[0], "price") else available[0].get("price")

        if best_price is None:
            self.logger.warning("Cannot hedge exposure for market %s runner %s: no available %s price", market_id, selection_id, side)
            return

        instructions = [
            {
                "selectionId": selection_id,
                "handicap": 0,
                "side": side,
                "orderType": "LIMIT",
                "limitOrder": {
                    "price": best_price,
                    "size": round(size, 2),
                    "persistenceType": "LAPSE",
                },
            }
        ]
        try:
            result = self.api_client.betting.place_orders(
                market_id=market_id,
                instructions=instructions,
                customer_ref=f"arbstrat-hedge-{market_id}-{selection_id}-{int(time.time())}",
                customer_strategy_ref=self.CUSTOMER_STRATEGY_REF,
            )
            self.logger.info("Placed hedge order for market %s runner %s: %s", market_id, selection_id, result)
        except Exception as exc:
            self.logger.exception("Failed to place hedge order for market %s runner %s: %s", market_id, selection_id, exc)


def main():
    load_dotenv()
    username = os.getenv("BETFAIR_USERNAME")
    password = os.getenv("BETFAIR_PASSWORD")
    app_key = os.getenv("BETFAIR_APP_KEY")
    if not username or not password or not app_key:
        raise RuntimeError("Missing BETFAIR_USERNAME, BETFAIR_PASSWORD or BETFAIR_APP_KEY")

    cert_files = (
        "/root/betfair_profitbox/certs/client-2048.crt",
        "/root/betfair_profitbox/certs/client-2048.key",
    )
    logger = build_logger()
    api_client = bflw.APIClient(username, password=password, app_key=app_key, cert_files=cert_files)
    api_client.login()
    logger.info("Logged in to Betfair API")

    try:
        strategy = ArbStrat(api_client, logger)
        strategy.run()
    finally:
        api_client.logout()
        logger.info("Logged out of Betfair API")


if __name__ == "__main__":
    main()
