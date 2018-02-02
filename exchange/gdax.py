# coding: utf8
import arrow
import time
import simplejson as json
import six
import requests
import traceback
import logging
import hashlib

from model import *
from base import Exchange


class Gdax(Exchange):
    def __init__(self, api_key='', secret=''):
        super(Gdax, self).__init__('gdax', api_key, secret, 'https://api.gdax.com')

    def __trasfer_symbol(self, s):
        return s.replace('_', '-').lower().replace("usdt", "usd").upper()

    def fetch_ticker(self, symbol):
        symbol = self.__trasfer_symbol(symbol)
        endpoint = "/products/%s/ticker" % symbol
        res = self.get(endpoint)
        return self._parse_ticker(res)

    def _parse_ticker(self, data):
        date = arrow.get(data['time'])
        return Ticker(data['bid'], data['ask'], data['price'], seconds=date.float_timestamp)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api = Gdax()
    a = api.fetch_ticker('eth_usdt')
    print time.time() * 1000 - a.ms
    # print api.fetch_depth('btc_usdt')
    # print api.account()
    # print api.order('eth_usdt', 'buy', 'limit', 0.1, 10)
    # print api.order_info('eth_usdt', 1000)
    # print api.cancel_order('eth_usdt', 1000)
