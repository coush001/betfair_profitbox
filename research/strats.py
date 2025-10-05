from collections import OrderedDict, deque, defaultdict
from flumine import BaseStrategy
from flumine.order.trade import Trade
from flumine.order.order import OrderStatus
from flumine.order.ordertype import LimitOrder
from flumine.utils import get_price
from logging_setup import build_logger

import time
from datetime import timedelta

class HugoStrat(BaseStrategy):
