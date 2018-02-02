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


class Bitfinex(Exchange):
    def __init__(self, api_key='', secret=''):
        super(Bitfinex, self).__init__('bitfinex', api_key, secret, 'https://api.bitfinex.com/v1')

    def __trasfer_symbol(self, s):
        return s.replace('_', '').lower().replace("usdt", "usd")

    def fetch_ticker(self, symbol):
        symbol = self.__trasfer_symbol(symbol)
        endpoint = "/pubticker/" + symbol
        res = self.get(endpoint)
        return self._parse_ticker(res)

    def _parse_ticker(self, data):
        return Ticker(data['bid'], data['ask'], data['last_price'], seconds=data['timestamp'])

    def fetch_depth(self, symbol):
        symbol = self.__trasfer_symbol(symbol)
        endpoint = "/book/" + symbol
        return self.get(endpoint)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api = Bitfinex()
    a = api.fetch_ticker('eth_usdt')
    # print api.fetch_depth('btc_usdt')
    # print api.account()
    # print api.order('eth_usdt', 'buy', 'limit', 0.1, 10)
    # print api.order_info('eth_usdt', 1000)
    # print api.cancel_order('eth_usdt', 1000)
