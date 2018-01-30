# coding: utf8

import ccxt
import logging
import click
from skyb.model import MysqlEngine
import simplejson as json
from exchange import *
from order import *
import os
from util import slack

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


class Test(object):
    CHECK_COINS = ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS']

    def __init__(self, config, debug=True):
        key_config = config['apikey']
        self.debug = debug
        self.exchanges = {
            'zb': Zb(key_config['zb']['key'], key_config['zb']['secret']),
            'okex': Okex(key_config['okex']['key'], key_config['okex']['secret']),
            # 'huobipro': Huobipro(key_config['huobipro']['key'], key_config['huobipro']['secret']),
            'bithumb': ccxt.bithumb(),
            'binance': ccxt.binance(),
            'bitflyer': ccxt.bitflyer(),
            'hitbtc': ccxt.hitbtc(),
        }

        self.engine = MysqlEngine(config['db']['url'])
        self.order_manager = OrderManager(self.engine)
        self.accounts = {}

    def _check_and_create_table(self, tablename):
        sql = '''
            CREATE TABLE if not EXISTS `%s` (
              `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
              `coin` varchar(8) DEFAULT NULL,
              `ab` decimal(8,4) DEFAULT NULL,
              `ba` decimal(8,4) DEFAULT NULL,
              `ts` bigint(20) DEFAULT NULL,
              PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        ''' % tablename
        self.engine.execute(sql)

    def insert(self, table, coin, a, b, ts):
        sql = "insert into " + table + " (coin, ab, ba, ts) values (?, ?, ?, ?)"
        self.engine.execute(sql, (coin, a, b, ts))

    def trade(self, first, second, coin):
        self.first_api = self.exchanges.get(first)
        self.second_api = self.exchanges.get(second)
        self.tablename = 'diff_%s_%s' % (first, second)
        self._check_and_create_table(self.tablename)
        while True:
            self._trade(coin)
            time.sleep(2)

    def get_right(self, exchange, coin, li, side='bid', amount=None):
        if exchange.id == 'bithumb':
            return self.ccxt_get_right(exchange, side)
        total = 0
        res = []
        if amount is None:
            amount = AMOUNT_THRESHOLD[coin]
        if exchange.id != 'huobipro':
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

    def ccxt_get_bid_ask(self, exchange, coin, symbol):
        assert exchange.id in ('bithumb', 'binance', 'bitflyer', 'hitbtc')
        if exchange.id == 'bithumb':
            data = exchange.fetch_ticker(coin + '/KRW')
            return data['bid'] / 1068.6, data['ask'] / 1068.6
        elif exchange.id == 'binance':
            data = exchange.fetch_ticker(coin + "/USDT")
            return data['bid'], data['ask']
        elif exchange.id == 'bitflyer':
            data = exchange.fetch_ticker(coin + '/JPY')
            return data['bid'] / 108.84, data['ask'] / 108.84
        elif exchange.id == 'hitbtc':
            data = exchange.fetch_ticker(coin + '/USDT')
            return data['bid'], data['ask']

    def get_bid_ask(self, exchange, coin, symbol):
        first_depth = exchange.fetch_depth(symbol)
        first_bid = self.get_right(exchange, coin, first_depth['bids'], 'bid')
        first_ask = self.get_right(exchange, coin, first_depth['asks'], 'ask')
        return first_bid, first_ask

    def _trade(self, coin):
        x = coin
        try:
            symbol = x + "_USDT"

            try:
                first_bid, first_ask = self.get_bid_ask(self.first_api, coin, symbol)
            except:
                first_bid, first_ask = self.ccxt_get_bid_ask(self.first_api, coin, symbol)
            try:
                second_bid, second_ask = self.get_bid_ask(self.second_api, coin, symbol)
            except:
                second_bid, second_ask = self.ccxt_get_bid_ask(self.second_api, coin, symbol)

            a = fix_float_radix(first_bid / second_ask)  # 左卖右买
            b = fix_float_radix(second_bid / first_ask)  # 左买右卖
            logging.info("结果 %s\t%s\t%s\t%s\t%s\t%s" % (first_bid, first_ask, second_bid, second_ask, a, b))
            if not self.debug:
                self.insert(self.tablename, x, a, b, cur_ms())
        except Exception, e:
            logging.exception("")


@click.command()
@click.option("--first", default="zb")
@click.option("--second", default="okex")
@click.option("--coin", default="BTC")
@click.option('-d', "--debug", is_flag=True)
def main(first, second, coin, debug):
    from util import read_conf
    config = read_conf("./config.json")
    Test(config, debug).trade(first, second, coin)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    main()
