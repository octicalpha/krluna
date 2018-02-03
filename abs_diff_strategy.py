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
from util import slack, cur_ms, avg, fix_float_radix, ms_to_str
from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from exchange.model import *


from back_test_util import *

class AbsDiffStrategy(object):
    def __init__(self, config, debug):
        self.config = config
        self.debug = debug
        self.backtest = False
        self.pool = ThreadPoolExecutor(3)
        self.engine = MysqlEngine(config['db']['url'])

        self.max_miss_ms = 2000

        self.base_prices = []
        self.trade_prices = []
        self.base_price_mas = []
        self.trade_price_mas = []
        self.diff_mas = []

        self.diffs = []

        # --------- backtest vars --------------
        self.bt_status = 0
        self.bt_buy_price = 0
        self.bt_sell_price = 0
        self.bt_benefit = 0
        self.bt_tx_cnt = 0
        self.bt_record = None
        self.bt_buy_diff_ma = None
        self.bt_buy_record_id = None
        self.bt_buy_slope = 0
        # --------- backtest vars --------------

    def _check_table_exist(self, tablename):
        sql = '''
        CREATE TABLE if not EXISTS `%s` (
          `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
          `symbol` varchar(24) NOT NULL DEFAULT '',
          `base_bid` decimal(14,4) NOT NULL DEFAULT '0.0000',
          `base_ask` decimal(14,4) NOT NULL DEFAULT '0.0000',
          `base_price` decimal(14,4) NOT NULL DEFAULT '0.0000',
          `trade_bid` decimal(14,4) NOT NULL DEFAULT '0.0000',
          `trade_ask` decimal(14,4) NOT NULL DEFAULT '0.0000',
          `trade_price` decimal(14,4) NOT NULL DEFAULT '0.0000',
          `ts` bigint(20) NOT NULL DEFAULT '0',
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        ''' % tablename
        self.engine.execute(sql)

    def run(self, base_exchange, trade_exchange, symbol):
        self.diff_tablename = "abs_diff_%s_%s" % (trade_exchange.id, base_exchange.id)
        self._check_table_exist(self.diff_tablename)
        while True:
            try:
                future_base_ticker = self.pool.submit(base_exchange.fetch_ticker, symbol)
                future_trade_ticker = self.pool.submit(trade_exchange.fetch_ticker, symbol)
                base_ticker = future_base_ticker.result()
                trade_ticker = future_trade_ticker.result()

                self.tick(base_ticker, trade_ticker, symbol)
            except:
                logging.exception("")
            time.sleep(2)

    def back_test(self, base_exchange, trade_exchange, symbol, begin_time, end_time=None):
        self.backtest = True
        self.diff_tablename = "abs_diff_%s_%s" % (trade_exchange.id, base_exchange.id)
        sql = "select * from " + self.diff_tablename + " where symbol = ? and ts > ? and ts < ?"
        if not end_time:
            end_time = cur_ms()
        data = self.engine.fetch_row(sql, (symbol, begin_time, end_time))
        print begin_time, end_time
        for x in data:
            x.ar = ms_to_str(x.ts)
            self.bt_record = x
            bt = Ticker(x['base_bid'], x['base_ask'], x['base_price'], ms=x['ts'])
            tt = Ticker(x['trade_bid'], x['trade_ask'], x['trade_price'], ms=x['ts'])
            self.back_test_tick(bt, tt, symbol)
        print self.bt_benefit * 0.01

    def back_test_tick(self, base_ticker, trade_ticker, symbol):
        diff_price = trade_ticker.price - base_ticker.price
        self.diffs.append(diff_price)
        self.base_prices.append(base_ticker.price)
        self.trade_prices.append(trade_ticker.price)

        if self.in_warming():
            return
        window = 10
        base_ma = avg(self.base_prices[-window:])
        trade_ma = avg(self.trade_prices[-window:])
        self.base_price_mas.append(base_ma)
        self.trade_price_mas.append(trade_ma)
        self.diff_mas.append(trade_ma - base_ma)

        if len(self.diff_mas) < 10:
            return

        if self.bt_status == BtStatus.PLACE_BUY_ORDER:
            if trade_ticker.price < self.bt_buy_price:
                print 'buy success %s, id: %s, ts: %s' % (trade_ticker.price, self.bt_record.id, self.bt_record.ar)
                self.bt_status = BtStatus.SUCCESS_BUY_ORDER
                self.bt_tx_cnt += 1
                if self.bt_tx_cnt % 2 == 0:
                    self.bt_benefit += self.bt_sell_price - self.bt_buy_price
        elif self.bt_status == BtStatus.PLACE_SELL_ORDER:
            if trade_ticker.price > self.bt_sell_price:
                print 'sell success %s, id: %s, ts: %s' % (trade_ticker.price, self.bt_record.id, self.bt_record.ar)
                self.bt_status = BtStatus.SUCCESS_SELL_ORDER
                self.bt_tx_cnt += 1
                if self.bt_tx_cnt % 2 == 0:
                    self.bt_benefit += self.bt_sell_price - self.bt_buy_price
        elif self.bt_status == BtStatus.PLACE_BUY_ORDER and self.bt_record.id > self.bt_buy_record_id + 5:
            self.bt_cancel_buy()
        else:
            can_buy = self.ana_buy_status()
            can_sell = self.ana_sell_status()
            add_price = -0.0001
            if can_buy:
                buy_price = trade_ticker.ask + add_price
                self.back_test_buy(buy_price, 0.001)
            if can_sell:
                sell_price = trade_ticker.bid - add_price
                if sell_price > self.bt_buy_price + 10:
                    self.back_test_sell(sell_price, 0.001)

    def ana_sell_status(self):
        if self.bt_status != BtStatus.SUCCESS_BUY_ORDER: # 必须先买后卖
            return False
        window = 3
        base_slope = self.cal_slope(self.base_prices, window)
        trade_slope = self.cal_slope(self.trade_prices, window)
        if base_slope < -1 and trade_slope - base_slope > 1.5:
            return True
        return False

    def cal_slope(self, li, window):
        slope = (li[-1] - li[-window]) / window
        return slope

    def ana_buy_status(self):
        window = 4
        up_cnt = 0
        down_cnt = 0
        for i in range(window, 1, -1):
            delta = self.base_price_mas[-i+1] - self.base_price_mas[-i]
            if delta > 0.01:
                up_cnt += 1
            elif delta < -0.01:
                down_cnt += 1
        if up_cnt < window - 1:
            return False
        base_slope = self.cal_slope(self.base_prices, window)
        trade_slope = self.cal_slope(self.trade_prices, window)
        if base_slope >= 3 and base_slope - trade_slope > 2.2:
            self.bt_buy_slope = base_slope
            return True
        return False

    def bt_cancel_buy(self):
        if self.bt_status != BtStatus.PLACE_BUY_ORDER:
            return
        self.bt_buy_price = 0
        self.bt_status = BtStatus.INIT
        self.bt_buy_record_id = None
        print 'cancel buy %s, id: %s, ts: %s' % (self.bt_buy_price, self.bt_record.id, self.bt_record.ar)

    def back_test_buy(self, price, amount):
        if not (self.bt_status == BtStatus.INIT or self.bt_status == BtStatus.SUCCESS_SELL_ORDER):
            return
        self.bt_buy_price = price
        self.bt_status = BtStatus.PLACE_BUY_ORDER
        self.bt_buy_record_id = self.bt_record.id
        print 'place buy %s, id: %s, ts: %s' % (self.bt_buy_price, self.bt_record.id, self.bt_record.ar)

    def back_test_sell(self, price, amount):
        if not (self.bt_status == BtStatus.INIT or self.bt_status == BtStatus.SUCCESS_BUY_ORDER):
            return
        self.bt_sell_price = price
        self.bt_status = BtStatus.PLACE_SELL_ORDER
        print 'place sell %s, id: %s, ts: %s' % (self.bt_sell_price, self.bt_record.id, self.bt_record.ar)

    def in_warming(self):
        return len(self.base_prices) < 50

    def tick(self, base_ticker, trade_ticker, symbol):

        # now = cur_ms()
        # if abs(now - trade_ticker.ms) > self.max_miss_ms:
        #     logging.warn("%s ticker 过期, %s" % (base_exchange.id, now - trade_ticker.ms))
        #     return
        # if abs(now - base_ticker.ms) > self.max_miss_ms:
        #     logging.warn("%s ticker 过期, %s" % (base_exchange.id, now - base_ticker.ms))
        #     return

        # diff_price, diff_bid, diff_ask = trade_ticker.price - base_ticker.price, \
        #                                  trade_ticker.bid - base_ticker.bid, \
        #                                  trade_ticker.ask - base_ticker.ask
        # logging.info("diff is : %s\t%s\t%s", diff_price, diff_bid, diff_ask)
        if self.debug:
            return
        if not self.backtest:
            self.insert_diff_to_table(symbol, trade_ticker, base_ticker)

    def insert_diff_to_table(self, symbol, trade_ticker, base_ticker):
        sql = "insert into " + self.diff_tablename + \
              " (symbol, base_bid, base_ask, base_price, trade_bid, trade_ask, trade_price, ts) values (?, ?, ?, ?, ?, ?, ?, ?)"
        self.engine.execute(sql, (symbol, base_ticker.bid, base_ticker.ask, base_ticker.price,
                                  trade_ticker.bid, trade_ticker.ask, trade_ticker.price, cur_ms()))


@click.command()
@click.option("--base", default="binance")
@click.option("--trade", default="okex")
@click.option("--symbol", default="BTC_USDT")
@click.option("-d", "--debug", is_flag=True)
@click.option("--back", is_flag=True)
def main(base, trade, symbol, debug, back):
    if debug or back:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                            filename="log_abs_strategy.log")
    from util import read_conf
    config = read_conf("./config.json")
    if base == 'binance':
        base_exchange = Binance()
    elif base == 'bitfinex':
        base_exchange = Bitfinex()
    elif base == 'gdax':
        base_exchange = Gdax()
    trade_exchange = Okex(config['apikey']['okex']['key'], config['apikey']['okex']['secret'])
    if back:
        b = arrow.now().shift(hours=-1).timestamp * 1000
        #AbsDiffStrategy(config, debug).back_test(base_exchange, trade_exchange, symbol, 1517615151000 ,1517618751000)
        AbsDiffStrategy(config, debug).back_test(base_exchange, trade_exchange, symbol, b)
    else:
        AbsDiffStrategy(config, debug).run(base_exchange, trade_exchange, symbol)


if __name__ == '__main__':
    main()
