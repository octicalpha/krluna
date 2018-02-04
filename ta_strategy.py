# coding: utf8

import arrow
import time
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
from exchange.model import *
import talib
import pandas as pd

try:
    import matplotlib.pyplot as plt
except:
    plt = None

from back_test_util import *


class TaStrategy(BackTestMixin):
    def __init__(self, config, symbol, backtest=False, debug=True):
        key_config = config['apikey']
        self.okex_exchange = Okex(key_config['okex']['key'], key_config['okex']['secret'])
        self.symbol = symbol
        self.record = None
        self.in_backtest = backtest

        self.amount = 0.002
        self.debug = debug

        self.engine = MysqlEngine(config['db']['url'])
        self.order_manager = OrderManager(self.engine)

        self.buy_price = None
        self.buy_amount = None
        self.sell_price = None
        self.prod_status = BtStatus.INIT
        self.cmp_buy_price_cnt = [0, 0]

        BackTestMixin.__init__(self)

    def recover_from_db(self):
        sql = "select * from `order` where status in (1, 100) order by id desc limit 1"
        od = self.engine.fetchone_row(sql, ())
        if od:
            if od['side'] == 'buy':
                self.buy_price = float(od['price'])
                self.buy_amount = float(od['amount'])
                if od['status'] == 1:
                    self.prod_status = BtStatus.PLACE_BUY_ORDER
                else:
                    self.prod_status = BtStatus.SUCCESS_BUY_ORDER
            else:
                self.sell_price = float(od['price'])
                if od['status'] == 1:
                    self.prod_status = BtStatus.PLACE_SELL_ORDER
                else:
                    self.prod_status = BtStatus.SUCCESS_SELL_ORDER

    def run(self):
        while True:
            try:
                if self.has_unfinish_order():
                    time.sleep(2)
                    continue
                self.recover_from_db()
                if arrow.now().to('local').second > 18:
                    self.record = self.okex_exchange.fetch_kline(self.symbol, type="1min", size=500)
                    self._run()
            except:
                logging.exception("")
            time.sleep(10)

    def back_test(self):
        self.in_backtest = True
        self.bt_min_round_benefit = -60
        data = self.okex_exchange.fetch_kline(self.symbol, type="1min", size=1000)
        # graph(data)
        # return
        l = len(data)
        for i in range(400, l):
            self.record = data.iloc[0: i + 1, :]
            self._run()
        print self.bt_benefit - self.bt_tx_cnt * 10

    def has_unfinish_order(self):
        a = len(self.order_manager.list_by_status(ORDER_STATUS.PLACED)) >= 1
        if a:
            return a
        a = len(self.order_manager.list_by_status(ORDER_STATUS.INIT)) >= 1
        return a

    def _run(self):
        data = self.record
        if self.in_backtest:
            self.back_test_check_tx_success(data.iloc[-1]['high'], data.iloc[-1]['low'])
        # if self.bt_status == BtStatus.SUCCESS_BUY_ORDER:
        #         if float(data.iloc[-1]['high']) < (self.bt_buy_price + 20):
        #             self.cmp_buy_price_cnt[0] += 6
        #         else:
        #             self.cmp_buy_price_cnt[1] += 6
        #         if self.cmp_buy_price_cnt[0] - self.cmp_buy_price_cnt[1] > 30:
        #             logging.info("长时间不涨, 设定价格卖出")
        #             self.back_test_sell(price=self.bt_buy_price+20)
        #         if abs(self.bt_buy_price - 9381.4795) < 0.001:
        #             logging.info(self.cmp_buy_price_cnt)
        #     else:
        #         self.cmp_buy_price_cnt = [0, 0]
        # else:
        #     if self.prod_status == BtStatus.SUCCESS_BUY_ORDER:
        #         if float(data.iloc[-1]['high']) < (self.buy_price + 20):
        #             self.cmp_buy_price_cnt[0] += 1
        #         else:
        #             self.cmp_buy_price_cnt[1] += 1
        #         if self.cmp_buy_price_cnt[0] - self.cmp_buy_price_cnt[1] > 24:
        #             logging.info("长时间不涨, 设定价格卖出")
        #             self.sell(price=self.buy_price + 40, role='maker')
        #     else:
        #         self.cmp_buy_price_cnt = [0, 0]

        # data['RSI6'] = talib.RSI(data['close'].values, timeperiod=6)
        # data['RSI20'] = talib.RSI(data['close'].values, timeperiod=20)
        # data['EMA30'] = talib.EMA(data['close'].values, timeperiod=30)
        # data['MACD'], data['MACDsignal'], data['MACDhist'] = talib.MACD(data['close'].values)
        data['MACD'], data['MACDsignal'], data['MACDhist'] = talib.MACD(data['close'].values,
                                                                        fastperiod=5, slowperiod=34, signalperiod=5)
        data['MACDhist'] = data['MACDhist'] * 2
        zero = data.iloc[-4]['MACDhist']
        one = data.iloc[-3]['MACDhist']
        two = data.iloc[-2]['MACDhist']
        row = data.iloc[-1]['MACDhist']

        cur_row = data.iloc[-1]
        if zero * row > 0:  # 没有x
            return
        gold_cross, dead_cross = False, False
        steep = 5.4
        small = 1.8

        sl1 = one - zero
        sl2 = two - one
        sl3 = row - two
        if row > 0:
            if cur_row['MACD'] < -5:
                steep = 17
                small = 7
            else:
                steep = 11
                small = 5
            if sl3 > steep:
                gold_cross = True
            elif sl3 > small and sl2 > small and two > 0:
                gold_cross = True
        if row < 0:
            if cur_row['MACD'] > 5:
                steep = 17
                small = 7
            else:
                steep = 11
                small = 5
            if sl3 < -steep:
                dead_cross = True
            if sl3 < -small and sl2 < -small and two < 0:
                dead_cross = True

        msg = (zero, one, two, row, sl1, sl2, sl3)
        if gold_cross:
            logging.info("find gold cross, %s, %s", data.index[-1], msg)
            if self.in_backtest:
                buy_price = cur_row['high'] + 10
                self.back_test_buy(buy_price, msg=data.index[-1])
            else:
                role = 'taker' if sl3 > steep else 'maker'
                self.buy(self.amount, role)

        if dead_cross:
            logging.info("find dead cross, %s, %s", data.index[-1], msg)
            if self.in_backtest:
                sell_price = cur_row['low']
                self.back_test_try_cancel_buy_order()
                self.back_test_sell(sell_price, msg=data.index[-1])
            else:
                self.try_cancel_buy_order()
                role = 'taker' if sl3 < -steep else 'maker'
                self.sell(role=role)

    def try_cancel_buy_order(self):
        if self.prod_status != BtStatus.PLACE_BUY_ORDER:  # 有未成交买单是处理
            return
        sql = "select * from `order` where status in (1, 100) order by id desc limit 1"
        od = self.engine.fetchone_row(sql, ())
        if not od:
            return
        if od['side'] != 'buy':
            return
        if self.okex_exchange.order_info(od['symbol'], od['ex_id'])['status'] == ORDER_STATUS.PLACED:
            self.okex_exchange.cancel_order(od['symbol'], od['ex_id'])
            self.order_manager.update_status(od['id'], ORDER_STATUS.CANCELLED)
            self.prod_status = BtStatus.INIT

    def buy(self, amount, role):
        if not (self.prod_status == BtStatus.INIT or self.prod_status == BtStatus.SUCCESS_SELL_ORDER):
            logging.info("不是初始状态或者卖单未完成, 不能买")
            return
        if role == 'maker':
            price = self.okex_exchange.fetch_depth(self.symbol)['bids'][0][0]
        else:
            price = self.okex_exchange.fetch_depth(self.symbol)['asks'][-1][0]
        logging.info("try buy, price %s, amount: %s" % (price, self.amount))
        if self.debug:
            order_id = cur_ms()
            self.prod_status = BtStatus.SUCCESS_BUY_ORDER
        else:
            buy_record_id = self.order_manager.init_order(self.okex_exchange.id, self.symbol, 'buy', self.amount, price)
            order_id = self.okex_exchange.buy_limit(self.symbol, price=price, amount=self.amount)
            self.order_manager.update_ex_id(buy_record_id, order_id)
            self.buy_price = price
            self.prod_status = BtStatus.PLACE_BUY_ORDER
        logging.info("发送买单成功 buy_order_id: %s" % order_id)
        slack("buy price %s" % price)

    def check_price_level(self, price):
        if price > 8900:
            return 'high'
        if price > 8500:
            return 'mid'
        return 'low'

    def _check_sell_price_is_ok(self, price):
        delta = price - self.buy_price
        if self.buy_price > 8900:
            return delta > -60
        if self.buy_price > 8500:
            return delta > -15
        if self.buy_price < 8200:
            return delta > 40
        return delta > 0

    def sell(self, price=None, role=None):
        if self.bt_force_buy_first and self.prod_status == BtStatus.INIT:
            logging.info("没有买单, 不能卖")
            return
        if not (self.prod_status == BtStatus.INIT or self.prod_status == BtStatus.SUCCESS_BUY_ORDER):
            logging.info("没有买单, 不能卖")
            return

        amount = self.buy_amount
        if not price:
            if role == 'maker':
                price = self.okex_exchange.fetch_depth(self.symbol)['asks'][-1][0]
            else:
                price = self.okex_exchange.fetch_depth(self.symbol)['bids'][0][0]
        logging.info("try sell, price %s, amount: %s" % (price, amount))
        if not self._check_sell_price_is_ok(price):
            logging.warn("卖价太低了, 等吧, buy: %s, sell: %s" % (self.buy_price, price))
            return
        if self.debug:
            order_id = cur_ms()
            self.prod_status = BtStatus.SUCCESS_SELL_ORDER
        else:
            record_id = self.order_manager.init_order(self.okex_exchange.id, self.symbol, 'sell', amount, price)
            order_id = self.okex_exchange.sell_limit(self.symbol, price=price, amount=amount)
            self.order_manager.update_ex_id(record_id, order_id)
            self.prod_status = BtStatus.PLACE_SELL_ORDER

        logging.info("发送卖单成功 sell_order_id: %s" % order_id)
        slack("sell price %s" % price)


def graph(data):
    macd = pd.DataFrame({
        "macd": data['MACD'],
        "MACDsignal": data['MACDsignal'],
    }, index=data.index)
    macd.plot()
    plt.show()


@click.command()
@click.option("-d", "--debug", is_flag=True)
@click.option("--back", is_flag=True)
def main(debug, back):
    if debug or back:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                            filename="log_ta_strategy.log")
    from util import read_conf
    config = read_conf("./config.json")
    if back:
        TaStrategy(config, "btc_usdt", back, debug=debug).back_test()
    else:
        TaStrategy(config, "btc_usdt", debug=debug).run()


if __name__ == '__main__':
    main()
