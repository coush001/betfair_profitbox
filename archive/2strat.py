#!/root/betting/betenv/bin/python3
import warnings
import sys
import pandas as pd
import time
import logging
from pythonjsonlogger import jsonlogger
from flumine import FlumineSimulation, clients
from collections import OrderedDict, deque, defaultdict
from flumine import BaseStrategy
from flumine.order.trade import Trade
from flumine.order.order import OrderStatus
from flumine.order.ordertype import LimitOrder
from flumine.utils import get_price
from setup_logging import build_logger
import time
from datetime import timedelta, datetime, timezone
import betfairlightweight as bflw
from flumine import Flumine, clients, BaseStrategy
from betfairlightweight.filters import streaming_market_filter, streaming_market_data_filter, market_filter
from flumine import Flumine, clients
import os
from dotenv import load_dotenv

# ====== SMALL CONFIG ADD ======
STREAM_FIELDS  = ["EX_MARKET_DEF", "EX_BEST_OFFERS", "EX_TRADED", "EX_LTP"]  # ensure market_def present
LADDER_LEVELS  = 3

print("strat start")
class HugoStrat(BaseStrategy):
    """
    Example strateg
    """
    def __init__(self, enter_threshold, exit_threshold, order_hold, price_add, log_root, log_level, *a, **k):
        self.log = build_logger(log_root,log_level)  # logs/trades.log, rotated nightly
        super().__init__(*a, **k)
        self.hist = defaultdict(lambda: deque(maxlen=400))  # per runner
        self.enter_threshold = enter_threshold
        self.exit_threshold = exit_threshold
        self.order_hold = order_hold
        self.price_add = price_add
        self.startdt = None
        self._last = {}    # order_id -> last size_matched
        self.rows = []     # collected fills: [market_id, selection_id, time, size, price, side, order_id]
        self.pnl = 0.0     

    def add_market(self, market):
        self.log.info("ADD", market.market_id, market.event_name, market.event_type_id)
    
    def check_market_book(self, market, market_book):
        if market_book.status == "OPEN" and market_book.inplay:
            return True
    
    def _price_now(self, r):
        return r.last_price_traded
    
    def _price_n_secs_ago(self, key, now_dt, n=5):
        cutoff = now_dt - timedelta(seconds=n)
        dq = self.hist.get(key)
        if not dq: return None
        for t,p in reversed(dq):
            if t <= cutoff:
                return p
        return None

    def avg_back_odds(self, market, selection_id):
        total_stake, weighted = 0, 0
        for order in market.blotter:
            if order.selection_id == selection_id and order.side == "BACK":
                matched = order.size_matched
                if matched > 0:
                    total_stake += matched
                    weighted += matched * order.average_price_matched
        return weighted / total_stake if total_stake else None
    
    def process_market_book(self, market, market_book):
        if not self.startdt: self.startdt = market_book.publish_time
        elapsed = market_book.publish_time.timestamp() - self.startdt.timestamp()
        self.log.debug(f"process_market_book: {market.event_name}, time elapse {elapsed},  publishtime:{market_book.publish_time}")

        if elapsed > 1:
            for r in market_book.runners:
                now_dt = market_book.publish_time
                key = (market.market_id, r.selection_id)
                p = self._price_now(r)
                if p and p < self.enter_threshold:
                    runner_context = self.get_runner_context(market.market_id, r.selection_id, r.handicap)
                    if runner_context.live_trade_count == 0:
                        self.log.info(f"price less than thresh, no live trades, placing order: {r.selection_id}")
                        back = round(get_price(r.ex.available_to_lay, 0) + self.price_add,2)
                        trade = Trade(market_book.market_id, r.selection_id, r.handicap,self, notes={"entry_px": back})
                        order = trade.create_order(side="BACK", order_type=LimitOrder(back, self.context["stake"]))
                        market.place_order(order)
                        self.log.info({"PLACE ORDER":market.market_id,"price":back,"event_name":market.event_name})

                if not p : return
                if p > self.exit_threshold:
                    self.hedge_selection(r, market)

    def avg_back_odds(self, market, selection_id):
        total_stake, weighted = 0, 0
        for order in market.blotter:
            if order.selection_id == selection_id and order.side == "BACK":
                matched = order.size_matched
                if matched > 0:
                    total_stake += matched
                    weighted += matched * order.average_price_matched
        return weighted / total_stake if total_stake else None
    
    def hedge_selection(self,r, market):        
        backs = [o for o in market.blotter if o.selection_id==r.selection_id and o.side=="BACK" and o.size_matched>0]
        stake = sum(o.size_matched for o in backs)
        best_lay = get_price(r.ex.available_to_lay, 0)
        if not best_lay: return
        av = self.avg_back_odds(market, r.selection_id)
        if not av: return
        hsize = (av*stake - stake) / best_lay
        if hsize <= 0: return
        self.log.info(f"Closing risk : runner {r.selection_id}, {hsize} @ {best_lay}")
        trade = Trade(market.market_id, r.selection_id, r.handicap, self)
        order = trade.create_order("LAY", LimitOrder(best_lay, round(hsize,2), persistence_type="LAPSE"))
        self.log.info("hedged selection", r, market)
        market.place_order(order)
 
    def process_orders(self, market, orders):
        for order in orders:
            if order.status == OrderStatus.EXECUTABLE:
                if order.elapsed_seconds and order.elapsed_seconds > self.order_hold:
                     market.cancel_order(order)

    def process_order(self, order):
        if order.status == OrderStatus.EXECUTION_COMPLETE:
            a = {"Order fully matched": order.selection_id,"avg_px":order.average_price_matched,
                 "size":order.size_matched}
            self.log.info(a)
        elif order.size_matched and order.size_remaining:
            a = {"Partial match":order.selection_id,
                 "matched":order.size_matched,
                 "remaining":order.size_remaining,
                 "avg_px":order.average_price_matched}
            self.log.info(a)

    def process_closed_market(self, market, market_book):
        self.pnl = 0.0
        self.log.info(f"Processing closed market: {market.event_name}, {market.market_id}")
        for order in market.blotter:
            self.pnl += order.profit
            self.log.info(f"Order PNL {order.profit}, av size matched: {order.size_matched} av price matched: {order.average_price_matched}, date_time_created: {order.date_time_created}")
        self.log.warning(f"Total pnl for market:{market.event_name}, {market.market_id}, : PNL :: {self.pnl}")

# load .env
load_dotenv()
USERNAME = os.getenv("BETFAIR_USERNAME")
APP_KEY  = os.getenv("BETFAIR_APP_KEY")
PASSWORD = os.getenv("BETFAIR_PASSWORD")

trading = bflw.APIClient(
    USERNAME, app_key=APP_KEY, password=PASSWORD,
    cert_files=("certs/client-2048.crt", "certs/client-2048.key")
)
trading.login()

# ====== MINIMAL CHANGE: use a STREAMING filter with a tight rolling window ======


now_utc = datetime.now(timezone.utc)
to_utc  = now_utc + timedelta(hours=24)

stream_filter = market_filter(
    event_type_ids=["4"],                # Cricket
    market_type_codes=["MATCH_ODDS"],
    market_countries=["IN", "AU", "GB"],
    market_start_time={
        "from": now_utc.isoformat(),
        "to":   to_utc.isoformat(),
    },
)

# Ask for market definition to avoid None errors in Flumine
stream_data = streaming_market_data_filter(
    fields=STREAM_FIELDS,
    ladder_levels=LADDER_LEVELS
)

# --- one-off snapshot of markets ---
catalogues = trading.betting.list_market_catalogue(
    filter=stream_filter,
    max_results=500,   # adjust if you truly expect more
    market_projection=["EVENT", "MARKET_START_TIME"]
)

print(f"Markets matching filter (UTC window {now_utc} – {to_utc}):\n")
for m in catalogues:
    print(f"{m.market_id}  {m.event.name}  {m.market_start_time.astimezone(timezone.utc)}")


client  = clients.BetfairClient(trading, paper_trade=False)
framework = Flumine(client)

# ====== MINIMAL CHANGE: pass the streaming filter & data to the strategy ======
strategy = HugoStrat(   
        market_filter=stream_filter,             # use streaming filter (time window, IN+GB)
        market_data_filter=stream_data,          # include EX_MARKET_DEF
        max_order_exposure=30,
        max_selection_exposure=90,
        context={"stake": 2},
        enter_threshold=1.2,
        exit_threshold=5.9,
        order_hold=17,
        price_add=0.01,
        log_root="./logs/live_prod/",
        log_level="I")

framework.add_strategy(strategy)
framework.run()


