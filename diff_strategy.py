# coding: utf8

import ccxt
import logging
from collections import defaultdict
from skyb.model import MysqlEngine
import simplejson as json
from exchange.zb import Zb
from exchange.okex import Okex


AMOUNT_THRESHOLD = {
    "BTC": 0.005,
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
    CHECK_COINS = ['BTC', 'ETH', 'LTC', 'ETC', 'XRP', 'EOS']
    def __init__(self, config):
        key_config = config['apikey']
        self.first_api = Zb(key_config['zb']['key'], key_config['zb']['secret'])
        self.second_api = Okex(key_config['okex']['key'], key_config['okex']['secret'])

        self.engine = MysqlEngine(config['db']['url'])

    def insert(self, table, coin, a, b, ts):
        sql = "insert into " + table + " (coin, ab, ba, ts) values (?, ?, ?, ?)"
        self.engine.execute(sql, (coin, a, b, ts))

    def trade(self):
        while True:
            self._trade()
            time.sleep(3)

    def get_right(self, coin, li, side='bid'):
        total = 0
        res = []
        if side == 'ask':
            li.reverse()
        for x in li:
            total += x[1]
            if total >= AMOUNT_THRESHOLD[coin]:
                res.append(x[0])
                break
            else:
                res.append(x[0])
        return avg(res)

    def _trade(self):
        ts  = now()
        for x in self.CHECK_COINS:
            try:
                symbol = x + "_USDT"
                first_depth = self.first_api.fetch_depth(symbol)
                second_depth = self.second_api.fetch_depth(symbol)

                first_bid = self.get_right(x, first_depth['bids'], 'bid')
                first_ask = self.get_right(x, first_depth['asks'], 'ask')
                if self.second_api.id == 'okex':
                    second_bid = self.get_right(x, second_depth['bids'], 'bid')
                    second_ask = self.get_right(x, second_depth['asks'], 'ask')
                a = fix_float_radix(first_bid / second_ask) # 左卖右买
                b = fix_float_radix(second_bid / first_ask) # 左买右卖
                print x, first_bid,  first_ask, second_bid, second_ask, a, b
                self.insert("diff_zb_okex", x, a, b, ts)
            except Exception, e:
                logging.exception("")

    def _trade_(self):
        ts = now()
        for x in self.CHECK_COINS:
            try:
                symbol = x + "/USDT"
                first_coin_tickers = self.first_api.fetch_ticker(symbol)
                second_coin_tickers = self.second_api.fetch_ticker(symbol)
                # print first_coin_tickers
                # print second_coin_tickers
                first_bid = float(first_coin_tickers['info']['buy'])
                first_ask = float(first_coin_tickers['info']['sell'])
                if self.second_api.id == 'okex':
                    second_bid = float(second_coin_tickers['info']['buy'])
                    second_ask = float(second_coin_tickers['info']['sell'])
                if self.second_api.id == 'huobipro':
                    second_bid = second_coin_tickers['info']['bid'][0]
                    second_ask = second_coin_tickers['info']['ask'][0]
                print first_bid, first_ask, second_bid, second_ask
                a = fix_float_radix(first_bid / second_ask) # 左卖右买
                b = fix_float_radix(second_bid / first_ask) # 左买右卖
                print a, b, a*b
                self.insert("diff_zb_okex", x, a, b, ts)
            except Exception, e:
                print 'error ' + str(e)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    from util import read_conf
    config = read_conf("./config.json")
    TestStrategy(config).trade()
