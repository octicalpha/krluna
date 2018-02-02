# coding: utf8
import time
import simplejson as json
import six
import requests
import traceback
import logging
import hashlib

from model import *
from base import Exchange


class Binance(Exchange):
    def __init__(self, api_key='', secret=''):
        super(Binance, self).__init__('binance', api_key, secret, 'https://api.binance.com')

    def __trasfer_symbol(self, s):
        return s.replace('_', '').upper()

    def time(self):
        endpoint = "/api/v1/time"
        return self.get(endpoint)

    def fetch_ticker(self, symbol):
        symbol = self.__trasfer_symbol(symbol)
        endpoint = "/api/v1/ticker/24hr"
        params = {
            "symbol": symbol,
        }
        res = self.get(endpoint, data=params)
        return self._parse_ticker(res)

    def _parse_ticker(self, data):
        return Ticker(data['bidPrice'], data['askPrice'], data['lastPrice'], seconds=time.time())

    def fetch_depth(self, symbol):
        symbol = self.__trasfer_symbol(symbol)
        endpoint = "/book/" + symbol
        return self.get(endpoint)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api = Binance()
    bs = time.time()
    print api.fetch_ticker('btc_usdt')
    print time.time() - bs
    # print time.time() * 1000 - api.time()['serverTime']
    # print api.fetch_depth('btc_usdt')
    # print api.account()
    # print api.order('eth_usdt', 'buy', 'limit', 0.1, 10)
    # print api.order_info('eth_usdt', 1000)
    # print api.cancel_order('eth_usdt', 1000)
