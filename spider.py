# coding: utf8

import logging
import click
from collections import defaultdict
from skyb.model import MysqlEngine
import simplejson as json
from exchange import *
from order import *
import os
from util import slack
from concurrent.futures import ThreadPoolExecutor, wait, as_completed

AMOUNT_THRESHOLD = {
    "BTC": 0.001,
    "EOS": 4,
    "LTC": 0.3,
    "ETH": 0.05,
    "XRP": 50,
    "ETC": 1.5,
}


def fix_float_radix(f):
    return float('%.4f' % f)


import time


def now():
    return int(time.time() * 1000)


def avg(li):
    return sum(li) / len(li)


class TestStrategy(object):
    # CHECK_COINS = ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS']
    CHECK_COINS = ['BTC']

    def __init__(self, config, debug=True):
        key_config = config['apikey']
        self.debug = debug
        self.exchanges = {
            'zb': Zb(key_config['zb']['key'], key_config['zb']['secret']),
            'okex': Okex(key_config['okex']['key'], key_config['okex']['secret']),
        }

        self.engine = MysqlEngine(config['db']['url'])
        self.order_manager = OrderManager(self.engine)
        self.strategy_manager = StrategyManager(self.engine)
        self.accounts = {}
        # self.refresh_account()
        self.init_min_a = 1.007
        self.init_min_b = 1.007

        self.min_a = self.init_min_a
        self.min_b = self.init_min_b

        self.cur_a = self.min_a
        self.cur_b = self.min_b
        self.miss_a = 0
        self.miss_b = 0

        self.trade_cnt = 0

        self.total_btc_amount = 0.09

        self.has_init_strategy_threshold = False

        self.pool = ThreadPoolExecutor(3)  # for many urls, this should probably be capped at some value.

    def _cal_due_amount(self, strategy, v):
        if strategy == 'a':
            min_v = self.min_a
        elif strategy == 'b':
            min_v = self.min_b
        assert v > min_v
        if v >= 1.011:
            return 0.003
        if v >= 1.02:
            return 0.004
        return 0.002

    def refresh_amount(self, first, second):
        self.strategy_a_key = '%s_%s_%s' % (first, second, 'a')
        self.strategy_b_key = '%s_%s_%s' % (first, second, 'b')
        self.amount_a = float(self.strategy_manager.get_sum_amount_by_name(self.strategy_a_key))
        self.amount_b = float(self.strategy_manager.get_sum_amount_by_name(self.strategy_b_key))
        self.refresh_strategy_min_v()
        logging.info("amount is %s\t%s" % (self.amount_a, self.amount_b))

    def refresh_strategy_min_v(self):
        if self.amount_a - self.amount_b > 0.08:  # a策略执行太多, 增加a策略阈值
            self.min_a = self.init_min_a + 0.008
        elif self.amount_a - self.amount_b > 0.04:
            self.min_a = self.init_min_a + 0.005
        elif self.amount_a - self.amount_b > 0.02:
            self.min_a = self.init_min_a + 0.002
        elif self.amount_a - self.amount_b > 0:
            self.min_a = self.init_min_a
        elif self.amount_b - self.amount_a > 0.1:  # b策略执行太多, 增加b策略阈值
            self.min_b = self.init_min_b + 0.008
        elif self.amount_b - self.amount_a > 0.07:
            self.min_b = self.init_min_b + 0.005
        elif self.amount_b - self.amount_a > 0.04:
            self.min_b = self.init_min_b + 0.003
        elif self.amount_b - self.amount_a > 0.02:
            self.min_b = self.init_min_b + 0.0015
        elif self.amount_b - self.amount_a > 0:
            self.min_b = self.init_min_b
        self.min_a = max(1.004, self.min_a)
        self.min_b = max(1.004, self.min_b)
        if not self.has_init_strategy_threshold:
            self.cur_a = self.min_a
            self.cur_b = self.min_b

        self.has_init_strategy_threshold = True

    def refresh_account(self):
        for k, v in self.exchanges.iteritems():
            self.accounts[k] = v.account()

    def insert(self, table, coin, a, b, ts):
        sql = "insert into " + table + " (coin, ab, ba, ts) values (?, ?, ?, ?)"
        self.engine.execute(sql, (coin, a, b, ts))

    def trade(self, first, second):
        self.first_api = self.exchanges.get(first)
        self.second_api = self.exchanges.get(second)
        # self.first_account = self.accounts.get(first)
        # self.second_account = self.accounts.get(second)
        self.tablename = 'diff_%s_%s' % (first, second)
        self.refresh_amount(first, second)
        while True:
            bs = time.time()
            self._trade()
            print time.time() - bs
            time.sleep(2)

    def get_right(self, exchange, coin, li, side='bid', amount=None):
        total = 0
        res = []
        if amount is None:
            amount = AMOUNT_THRESHOLD[coin]
        if side == 'ask':
            li.reverse()
        for x in li:
            total += x[1]
            if total >= amount:
                res.append(x[0])
                break
            else:
                res.append(x[0])
        return avg(res)

    def has_unfinish_order(self):
        a = len(self.order_manager.list_by_status(ORDER_STATUS.PLACED)) >= 4
        if a:
            return a
        a = len(self.order_manager.list_by_status(ORDER_STATUS.INIT)) > 2
        return a

    def _trade(self):
        ts = now()
        for x in self.CHECK_COINS:
            try:
                symbol = x + "_USDT"
                # balance = [self.first_account.get_avail('usdt'), self.first_account.get_avail('btc'),
                #            self.second_account.get_avail('usdt'), self.second_account.get_avail('btc')
                #            ]
                balance = [1000, 0.05, 1000, 0.05]
                # first_depth = self.first_api.fetch_depth(symbol)
                # second_depth = self.second_api.fetch_depth(symbol)

                future_first = self.pool.submit(self.first_api.fetch_depth, symbol)
                future_second = self.pool.submit(self.second_api.fetch_depth, symbol)

                first_depth = future_first.result()
                second_depth = future_second.result()

                first_bid = self.get_right(self.first_api, x, first_depth['bids'], 'bid')
                first_ask = self.get_right(self.first_api, x, first_depth['asks'], 'ask')
                second_bid = self.get_right(self.second_api, x, second_depth['bids'], 'bid')
                second_ask = self.get_right(self.second_api, x, second_depth['asks'], 'ask')

                a = fix_float_radix(first_bid / second_ask)  # 左卖右买
                b = fix_float_radix(second_bid / first_ask)  # 左买右卖
                logging.info("策略结果 %s\t%s, 阈值: %s\t%s" % (a, b, self.cur_a, self.cur_b))
                if not self.debug and x == 'BTC':
                    self.insert(self.tablename, x, a, b, ts)
                    if self.trade_cnt >= 40:
                        logging.info("交易太多次")
                    elif self.has_unfinish_order():
                        logging.info("有未完成订单")
                    else:
                        if a > self.cur_a:
                            if self.amount_a - self.amount_b > 0.064:
                                logging.info("[a]单向操作太多, 停止下单")
                            else:
                                self.miss_a = 0
                                logging.info("[a]准备执行a策略\t%s" % a)
                                # self.cur_a = (a + self.cur_a) / 2
                                self.cur_a = a
                                if balance[1] > 0.001 and balance[2] > 20:
                                    # second_price = second_ask - 0.0001
                                    second_price = second_ask
                                    logging.info("[a]真正执行a策略, price is: %s %s", first_bid, second_price)
                                    # amount = 0.001
                                    amount = self._cal_due_amount('a', a)
                                    sell_record_id = self.order_manager.init_order(self.first_api.id, x, 'sell', amount,
                                                                                   first_bid)
                                    buy_record_id = self.order_manager.init_order(self.second_api.id, x, 'buy', amount,
                                                                                  second_price)
                                    logging.info("[a]创建订单记录 sell_record_id: %s , buy_record_id %s" % (
                                        sell_record_id, buy_record_id))
                                    # sell_order_id = self.first_api.sell_limit(symbol, price=first_bid, amount=amount)
                                    # buy_order_id = self.second_api.buy_limit(symbol, price=second_price, amount=amount)

                                    sell_order_id_future = self.pool.submit(self.first_api.sell_limit, symbol,
                                                                            price=first_bid, amount=amount)
                                    buy_order_id_future = self.pool.submit(self.second_api.buy_limit, symbol,
                                                                           price=second_price, amount=amount)

                                    sell_order_id = sell_order_id_future.result()
                                    buy_order_id = buy_order_id_future.result()

                                    logging.info("[a]发送卖单成功 sell_order_id: %s" % sell_order_id)
                                    logging.info("[a]发送买单成功 buy_order_id: %s" % buy_order_id)

                                    self.order_manager.update_ex_id(sell_record_id, sell_order_id)
                                    self.order_manager.update_ex_id(buy_record_id, buy_order_id)
                                    self.amount_a += amount
                                    self.trade_cnt += 1
                                    self.strategy_manager.insert(self.strategy_a_key, sell_record_id, buy_record_id, a,
                                                                 amount)
                                    slack("execute a strategy success a: %s" % a)
                                else:
                                    logging.info("[a]执行a策略失败, 余额不足")
                        else:
                            self.miss_a += 1
                            if self.miss_a > 9:
                                self.cur_a = max(a, self.min_a)
                        if b > self.cur_b:
                            if self.amount_b - self.amount_a > 0.064:
                                logging.info("[b]单向操作太多, 停止下单")
                            else:
                                self.miss_b = 0
                                logging.info("[b]准备执行b策略\t%s" % b)
                                # self.cur_b = (b + self.cur_b) / 2
                                self.cur_b = b
                                if balance[3] > 0.001 and balance[0] > 20:
                                    # second_price = second_bid  + 0.0001
                                    second_price = second_bid
                                    logging.info("[b]真正执行b策略, price is: %s %s", first_ask, second_price)
                                    # amount = 0.001
                                    amount = self._cal_due_amount('b', b)
                                    buy_record_id = self.order_manager.init_order(self.first_api.id, x, 'buy', amount,
                                                                                  first_ask)
                                    sell_record_id = self.order_manager.init_order(self.second_api.id, x, 'sell',
                                                                                   amount,
                                                                                   second_price)
                                    logging.info("[b]创建订单记录 sell_record_id: %s , buy_record_id %s" % (
                                        sell_record_id, buy_record_id))
                                    # buy_order_id = self.first_api.buy_limit(symbol, price=first_ask, amount=amount)
                                    # sell_order_id = self.second_api.sell_limit(symbol, price=second_price,
                                    #                                            amount=amount)

                                    buy_order_id_future = self.pool.submit(self.first_api.buy_limit, symbol,
                                                                           price=first_ask, amount=amount)
                                    sell_order_id_future = self.pool.submit(self.second_api.sell_limit, symbol,
                                                                            price=second_price,
                                                                            amount=amount)

                                    sell_order_id = sell_order_id_future.result()
                                    buy_order_id = buy_order_id_future.result()

                                    logging.info("[b]发送买单成功 buy_order_id: %s" % buy_order_id)
                                    logging.info("[b]发送卖单成功 sell_order_id: %s" % sell_order_id)
                                    self.order_manager.update_ex_id(buy_record_id, buy_order_id)
                                    self.order_manager.update_ex_id(sell_record_id, sell_order_id)
                                    self.amount_b += amount
                                    self.trade_cnt += 1
                                    self.strategy_manager.insert(self.strategy_b_key, buy_record_id, sell_record_id, b,
                                                                 amount)
                                    slack("execute b strategy success, b is %s" % b)
                                else:
                                    logging.info("[b]执行b策略失败, 余额不足")
                        else:
                            self.miss_b += 1
                            if self.miss_b > 9:
                                self.cur_b = max(b, self.min_b)
                        self.refresh_strategy_min_v()
            except Exception, e:
                logging.exception("")


@click.command()
@click.option("--first", default="zb")
@click.option("--second", default="okex")
@click.option("-d", "--debug", is_flag=True)
def main(first, second, debug):
    from util import read_conf
    config = read_conf("./config.json")
    TestStrategy(config, debug).trade(first, second)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename="strategy.log")
    main()
