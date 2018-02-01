# coding: utf8

import logging
import click
from collections import defaultdict
from skyb.model import MysqlEngine
import simplejson as json
from exchange import *
from order import *
import os
import time
from util import slack, cur_ms, avg, fix_float_radix
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from balancer import DefaultTwoSideBalancer, Balancer, NightSleepTwoSideBalancer, BackSeeTwoSideBalancer

AMOUNT_THRESHOLD = {
    "BTC": 0.001,
    "EOS": 4,
    "LTC": 0.3,
    "ETH": 0.05,
    "XRP": 50,
    "ETC": 1.5,
}


class PriceChooser(object):
    def choose(self, left_bid, left_ask, right_bid, right_ask):
        """
        :param left_bid:
        :param left_ask:
        :param right_bid:
        :param right_ask:
        :return: (left_buy_price, left_sell_price, right_buy_price, right_sell_price)
        """
        raise NotImplementedError


class TakerPriceChooser(PriceChooser):
    def choose(self, left_bid, left_ask, right_bid, right_ask):
        return left_ask, left_bid, right_ask, right_bid


class MakerPriceChooser(PriceChooser):
    def __init__(self, delta_price):
        self.delta_price = delta_price

    def choose(self, left_bid, left_ask, right_bid, right_ask):
        return left_bid + self.delta_price, left_bid - self.delta_price, \
               right_bid + self.delta_price, right_ask - self.delta_price


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

        self.min_a = 1.0055
        self.min_b = 1.0055

        self.cur_a = self.min_a
        self.cur_b = self.min_b
        self.miss_a = 0
        self.miss_b = 0

        self.has_init_strategy_threshold = False

        self.pool = ThreadPoolExecutor(3)

        self.accounts = {}
        self.balancer = None
        self.init_balancer()

        self.price_chooser = TakerPriceChooser()
        # self.price_chooser = MakerPriceChooser(0.0001)

    def refresh_strategy_min_v(self):
        self.min_a, self.min_b = self.balancer.get_threshold()
        if not self.has_init_strategy_threshold:
            self.cur_a = self.min_a
            self.cur_b = self.min_b
        else:
            self.cur_a = max(self.cur_a, self.min_a)
            self.cur_b = max(self.cur_b, self.min_b)

        self.has_init_strategy_threshold = True

    def init_balancer(self):
        for k, v in self.exchanges.iteritems():
            a = v.account()
            logging.info("account %s: %s" % (k, a))
            self.accounts[k] = a

    def insert_diff_to_table(self, coin, a, b):
        sql = "insert into " + self.diff_tablename + " (coin, ab, ba, ts) values (?, ?, ?, ?)"
        self.engine.execute(sql, (coin, a, b, cur_ms()))

    def trade(self, first, second):
        self.first_api = self.exchanges.get(first)
        self.second_api = self.exchanges.get(second)
        first_account = self.accounts.get(first)
        second_account = self.accounts.get(second)
        self.diff_tablename = 'diff_%s_%s' % (first, second)
        self.balancer = BackSeeTwoSideBalancer(
            first_account.get_avail("btc"), first_account.get_avail("usdt"),
            second_account.get_avail("btc"), second_account.get_avail("usdt")
        ).init(self.engine, self.diff_tablename)
        self.strategy_a_key = '%s_%s_%s' % (first, second, 'a')
        self.strategy_b_key = '%s_%s_%s' % (first, second, 'b')
        self.refresh_strategy_min_v()
        while True:
            self._trade("BTC")
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
        a = len(self.order_manager.list_by_status(ORDER_STATUS.PLACED)) >= 6
        if a:
            return a
        a = len(self.order_manager.list_by_status(ORDER_STATUS.INIT)) > 2
        return a

    def analyze_price_from_depth(self, first_depth, second_depth, coin):
        first_bid = self.get_right(self.first_api, coin, first_depth['bids'], 'bid')
        first_ask = self.get_right(self.first_api, coin, first_depth['asks'], 'ask')
        second_bid = self.get_right(self.second_api, coin, second_depth['bids'], 'bid')
        second_ask = self.get_right(self.second_api, coin, second_depth['asks'], 'ask')

        return first_bid, first_ask, second_bid, second_ask

    def trade_from_left_to_right(self, coin, symbol, buy_price, sell_price, radio):
        avg_coin_price = (buy_price + sell_price) / 2
        self.execute_trade_in_exchange(coin, symbol, Balancer.TRADE_SIDE_LEFT_TO_RIGHT,
                                       self.second_api, self.first_api, buy_price, sell_price,
                                       avg_coin_price, radio)

    def trade_from_right_to_left(self, coin, symbol, buy_price, sell_price, radio):
        avg_coin_price = (buy_price + sell_price) / 2
        self.execute_trade_in_exchange(coin, symbol, Balancer.TRADE_SIDE_RIGHT_TO_LEFT,
                                       self.first_api, self.second_api, buy_price, sell_price,
                                       avg_coin_price, radio)

    def execute_trade_in_exchange(self, coin, symbol, trade_side,
                                  buy_exchange, sell_exchange, buy_price, sell_price,
                                  avg_price, radio):
        assert buy_price < sell_price
        logging.info("[%s]准备执行策略==========\t%s" % (trade_side, radio))

        # self.cur_b = (b + self.cur_b) / 2
        self.cur_b = radio
        coin_amount = self.balancer.get_trade_coin_amount(trade_side, radio)
        if not self.balancer.can_trade(coin_amount, coin_price=avg_price, side=trade_side):
            logging.info("执行策略失败, 余额不足")
            return

        # second_price = second_bid  + 0.0001
        assert trade_side in Balancer.TRADE_SIDES
        logging.info("真正执行策略, price is: buy: %s, sell: %s" % (buy_price, sell_price))
        buy_record_id = self.order_manager.init_order(buy_exchange.id, coin, 'buy', coin_amount,
                                                      buy_price)
        sell_record_id = self.order_manager.init_order(sell_exchange.id, coin, 'sell',
                                                       coin_amount,
                                                       sell_price)
        logging.info("创建订单记录 buy_record_id: %s , sell_record_id %s" % (
            buy_record_id, sell_record_id))

        buy_order_id_future = self.pool.submit(buy_exchange.buy_limit, symbol,
                                               price=buy_price, amount=coin_amount)
        sell_order_id_future = self.pool.submit(sell_exchange.sell_limit, symbol,
                                                price=sell_price,
                                                amount=coin_amount)

        sell_order_id = sell_order_id_future.result()
        buy_order_id = buy_order_id_future.result()

        logging.info("发送买单成功 buy_order_id: %s" % buy_order_id)
        logging.info("发送卖单成功 sell_order_id: %s" % sell_order_id)

        self.order_manager.update_ex_id(buy_record_id, buy_order_id)
        self.order_manager.update_ex_id(sell_record_id, sell_order_id)

        strategy_key = self.strategy_b_key
        if trade_side == Balancer.TRADE_SIDE_LEFT_TO_RIGHT:
            strategy_key = self.strategy_a_key
        self.strategy_manager.insert(strategy_key, buy_record_id, sell_record_id, radio,
                                     coin_amount)
        self.balancer.sync_by_trade(coin_amount, coin_price=avg_price, side=trade_side)
        logging.info("[%s]完成执行策略==========\t%s" % (trade_side, radio))

        slack("[%s] execute strategy success, radio is %s" % (trade_side, radio))

    def _trade(self, coin="BTC"):
        try:
            symbol = coin + "_USDT"

            future_first = self.pool.submit(self.first_api.fetch_depth, symbol)
            future_second = self.pool.submit(self.second_api.fetch_depth, symbol)

            first_depth = future_first.result()
            second_depth = future_second.result()

            first_bid, first_ask, second_bid, second_ask = self.analyze_price_from_depth(first_depth, second_depth,
                                                                                         coin)

            left_buy_price, left_sell_price, right_buy_price, right_sell_price = \
                self.price_chooser.choose(first_bid, first_ask, second_bid, second_ask)

            a = fix_float_radix(left_sell_price / right_buy_price)  # 左卖右买
            b = fix_float_radix(right_sell_price / left_buy_price)  # 左买右卖

            logging.info("策略结果 %s\t%s, 阈值: %s\t%s" % (a, b, self.cur_a, self.cur_b))
            if self.debug or coin != 'BTC':
                return

            self.insert_diff_to_table(coin, a, b)
            if max(a, b) < 1.016 and self.has_unfinish_order():
                logging.info("利润太小且有未完成订单")
            else:
                if a >= self.cur_a:
                    self.miss_a = 0
                    self.trade_from_left_to_right(coin, symbol, right_buy_price, left_sell_price, a)
                else:
                    self.miss_a += 1
                    if self.miss_a > 9:
                        self.cur_a = max(a - 0.0001, self.min_a)
                if b >= self.cur_b:
                    self.miss_b = 0
                    self.trade_from_right_to_left(coin, symbol, left_buy_price, right_sell_price, b)
                else:
                    self.miss_b += 1
                    if self.miss_b > 9:
                        self.cur_b = max(b - 0.0001, self.min_b)
                self.refresh_strategy_min_v()
        except Exception, e:
            logging.exception("")


@click.command()
@click.option("--first", default="zb")
@click.option("--second", default="okex")
@click.option("-d", "--debug", is_flag=True)
def main(first, second, debug):
    if debug:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                            filename="strategy.log")
    from util import read_conf
    config = read_conf("./config.json")
    TestStrategy(config, debug).trade(first, second)


if __name__ == '__main__':
    main()
