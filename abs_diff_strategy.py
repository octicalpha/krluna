# coding: utf8

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


class AbsDiffStrategy(object):
    def __init__(self, config, debug):
        self.config = config
        self.debug = debug
        self.pool = ThreadPoolExecutor(3)
        self.engine = MysqlEngine(config['db']['url'])

        self.max_miss_ms = 2000

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
                self._run(base_exchange, trade_exchange, symbol)
            except:
                logging.exception("")
            time.sleep(2)

    def _run(self, base_exchange, trade_exchange, symbol):
        future_base_ticker = self.pool.submit(base_exchange.fetch_ticker, symbol)
        future_trade_ticker = self.pool.submit(trade_exchange.fetch_ticker, symbol)

        base_ticker = future_base_ticker.result()
        trade_ticker = future_trade_ticker.result()

        # now = cur_ms()
        # if abs(now - trade_ticker.ms) > self.max_miss_ms:
        #     logging.warn("%s ticker 过期, %s" % (base_exchange.id, now - trade_ticker.ms))
        #     return
        # if abs(now - base_ticker.ms) > self.max_miss_ms:
        #     logging.warn("%s ticker 过期, %s" % (base_exchange.id, now - base_ticker.ms))
        #     return

        diff_price, diff_bid, diff_ask = trade_ticker.price - base_ticker.price, \
                                         trade_ticker.bid - base_ticker.bid, \
                                         trade_ticker.ask - base_ticker.ask
        logging.info("diff is : %s\t%s\t%s", diff_price, diff_bid, diff_ask)
        if not self.debug:
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
def main(base, trade, symbol, debug):
    if debug:
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
    AbsDiffStrategy(config, debug).run(base_exchange, trade_exchange, symbol)


if __name__ == '__main__':
    main()
