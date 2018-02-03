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
import matplotlib.pyplot as plt

from back_test_util import *


def slope(a, b):
    return b - a


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
        self.sell_price = None
        self.prod_status = BtStatus.INIT

        BackTestMixin.__init__(self)

    def recover_from_db(self):
        sql = "select * from `order` where status in (1, 100) order by id desc limit 1"
        od = self.engine.fetchone_row(sql, ())
        if od:
            if od['side'] == 'buy':
                self.buy_price = float(od['price'])
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
                self.record = self.okex_exchange.fetch_kline(self.symbol, type="1min", size=500)
                self._run()
            except:
                logging.exception("")
            time.sleep(10)

    def back_test(self):
        self.in_backtest = True
        self.bt_min_round_benefit = -60
        data = self.okex_exchange.fetch_kline(self.symbol, type="1min", size=2000)
        data['MACD'], data['MACDsignal'], data['MACDhist'] = talib.MACD(data['close'].values)
        # graph(data)
        # return
        l = len(data)
        for i in range(200, l):
            self.record = data.iloc[0: i + 1, :]
            self._run()
        print self.bt_benefit

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

        # data['RSI6'] = talib.RSI(data['close'].values, timeperiod=6)
        # data['RSI20'] = talib.RSI(data['close'].values, timeperiod=20)
        data['MACD'], data['MACDsignal'], data['MACDhist'] = talib.MACD(data['close'].values)
        print data
        one = data.iloc[-3]
        two = data.iloc[-2]
        row = data.iloc[-1]
        if -21 < row['MACD'] < 30 and one['MACDhist'] < 0 and two['MACDhist'] > 0 and row['MACDhist'] > two[
            'MACDhist']:
            slopes = []
            logging.info("找到cross, %s" % (data.index[-1]))
            real_cross = True
            for j in range(-5, -1):
                t_row = data.iloc[j]
                n_row = data.iloc[j + 1]
                a = n_row['MACDhist'] - t_row['MACDhist']
                slopes.append(a)
            if slopes[-1] < 0.2:
                real_cross = False
            elif slopes[-1] < slopes[-2] - 0.1 and slopes[-1] < 2:
                real_cross = False
            elif slopes[0] > 0:
                all_small = False
                for x in slopes:
                    if abs(x) > 1.2:
                        all_small = False
                        break
                if all_small:
                    real_cross = False
            else:
                if slopes[-1] < 3:
                    real_cross = False
            if not real_cross:
                return
            if self.in_backtest:
                buy_price = row['high']
                self.back_test_buy(buy_price, msg=data.index[-1])
            else:
                self.buy()
        elif one['MACDhist'] > 0 and two['MACDhist'] < 0 and row['MACDhist'] < two['MACDhist']:
            if self.in_backtest:
                sell_price = row['low']
                self.back_test_sell(sell_price, msg=data.index[-1])
            else:
                self.sell()

    def buy(self):
        if not (self.prod_status == BtStatus.INIT or self.prod_status == BtStatus.SUCCESS_SELL_ORDER):
            return
        price = self.okex_exchange.fetch_depth(self.symbol)['bids'][0][0]
        logging.info("try buy, price %s, amount: %s" % (price, self.amount))
        order_id = None
        if not self.debug:
            buy_record_id = self.order_manager.init_order(self.okex_exchange.id, self.symbol, 'buy', self.amount, price)
            order_id = self.okex_exchange.buy_limit(self.symbol, price=price, amount=self.amount)
            self.order_manager.update_ex_id(buy_record_id, order_id)
            self.buy_price = price
        logging.info("发送买单成功 buy_order_id: %s" % order_id)

    def _check_sell_price_is_ok(self, price):
        delta = price - self.buy_price
        if self.buy_price > 8900:
            return delta > -60
        if self.buy_price > 8500:
            return delta > -15
        if self.buy_price < 8200:
            return delta > 40
        return delta > 0

    def sell(self):
        if self.bt_force_buy_first and self.prod_status == BtStatus.INIT:
            return
        if not (self.prod_status == BtStatus.INIT or self.prod_status == BtStatus.SUCCESS_BUY_ORDER):
            return

        price = self.okex_exchange.fetch_depth(self.symbol)['asks'][-1][0]
        logging.info("try sell, price %s, amount: %s" % (price, self.amount))
        if not self._check_sell_price_is_ok(price):
            logging.warn("卖价太低了, 等吧, buy: %s, sell: %s" % (self.buy_price, price))
            return
        order_id = None
        if not self.debug:
            record_id = self.order_manager.init_order(self.okex_exchange.id, self.symbol, 'sell', self.amount, price)
            order_id = self.okex_exchange.sell_limit(self.symbol, price=price, amount=self.amount)
            self.order_manager.update_ex_id(record_id, order_id)

        logging.info("发送卖单成功 sell_order_id: %s" % order_id)


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
