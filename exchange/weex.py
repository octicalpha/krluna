# coding: utf8

import simplejson as json
import six
import requests
import traceback
import logging
import hashlib

from model import *
from base import Exchange


class Weex(Exchange):
    def __init__(self, api_key='', secret=''):
        super(Weex, self).__init__('weex', api_key, secret, 'https://api.weex.com')

    def __transfer_symbol(self, s):
        return s.replace("_", "").lower().replace("usdt", "usd")

    def fetch_ticker(self, symbol):
        symbol = symbol.lower()
        endpoint = '%s?symbol=%s' % ('/api/v1/ticker.do', symbol)
        return self.get(endpoint)

    def fetch_depth(self, symbol):
        symbol = self.__transfer_symbol(symbol)
        endpoint = "/v1/market/depth"
        params = {
            "market": symbol,
            "limit": 10,
            "merge": 0,
        }
        res = self.get(endpoint, data=params)
        asks = [[float(x[0]), float(x[1])] for x in res['data']['asks']]
        asks.reverse()
        return {
            'bids': [[float(x[0]), float(x[1])] for x in res['data']['bids']],
            'asks': asks,
        }


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api = Weex()
    # print api.fetch_ticker('eth_usdt')
    print api.fetch_depth('btc_usd')
    # print api.account()
    # print api.order('eth_usdt', 'buy', 'limit', 0.1, 10)
    # print api.order_info('eth_usdt', 1000)
    # print api.cancel_order('eth_usdt', 1000)
