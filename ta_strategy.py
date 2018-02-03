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
            time.sleep(8)

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

        # data['RSI6'] = talib.RSI(data['close'].values, timeperiod=6)
        # data['RSI20'] = talib.RSI(data['close'].values, timeperiod=20)
        data['MACD'], data['MACDsignal'], data['MACDhist'] = talib.MACD(data['close'].values)
        data['MACDhist'] = data['MACDhist'] * 2
        one = data.iloc[-3]
        two = data.iloc[-2]
        row = data.iloc[-1]
        if -21 < row['MACD'] < 30 and one['MACDhist'] < 0 and two['MACDhist'] > 0 and row['MACDhist'] > two[
            'MACDhist']:
            slopes = []
            logging.info("找到cross, %s" % (data.index[-1]))
            slack("find cross")
            real_cross = True
            for j in range(-5, -1):
                t_row = data.iloc[j]
                n_row = data.iloc[j + 1]
                a = n_row['MACDhist'] - t_row['MACDhist']  # 斜率
                slopes.append(a)
            if slopes[-1] < 0.4:  # 如果最后一个斜率 < 0.4
                real_cross = False
            elif slopes[-1] < slopes[-2] - 0.4 and slopes[-1] < 3.5:
                # 如果最后一个<倒数第二个, 一般判断不行, 但是为防止误判, 加上最后一个<2, 如果斜率很大的话，暂时认为可以
                real_cross = False
            elif slopes[0] > 0:  # 如果第一个斜率为负数, 那么做判断是不是所有的都很小，如果都很小，判断为假穿越
                all_small = False
                for x in slopes:
                    if abs(x) > 2:
                        all_small = False
                        break
                if all_small:
                    real_cross = False
            else:
                if slopes[-1] < 4:
                    real_cross = False
            logging.info("slopes is %s, result: %s" % (slopes, real_cross))
            if not real_cross:
                return
            if self.in_backtest:
                buy_price = row['high']
                self.back_test_buy(buy_price, msg=data.index[-1])
            else:
                role = 'taker' if slopes[-1] > 6 else 'maker'
                if row['MACD'] > 20 or slopes[-1] < 4.2:
                    self.buy(self.amount / 2, role)
                else:
                    self.buy(self.amount, role)
        elif one['MACDhist'] > 0 and two['MACDhist'] < 0 and row['MACDhist'] < two['MACDhist']:
            logging.info("find down cross, try sell")
            if self.in_backtest:
                sell_price = row['low']
                self.back_test_sell(sell_price, msg=data.index[-1])
            else:
                self.try_cancel_buy_order()
                slope = row['MACDhist'] - one['MACDhist']
                role = 'taker' if slope < -6 else 'maker'
                self.sell(role)

    def try_cancel_buy_order(self):
        if self.prod_status != BtStatus.PLACE_BUY_ORDER: #有未成交买单是处理
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

    def sell(self, role):
        if self.bt_force_buy_first and self.prod_status == BtStatus.INIT:
            logging.info("没有买单, 不能卖")
            return
        if not (self.prod_status == BtStatus.INIT or self.prod_status == BtStatus.SUCCESS_BUY_ORDER):
            logging.info("没有买单, 不能卖")
            return

        amount = self.buy_amount
        if role == 'maker':
            price = self.okex_exchange.fetch_depth(self.symbol)['asks'][-1][0]
            price = self.buy_price + 10
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
