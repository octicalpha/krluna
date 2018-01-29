# coding: utf8

__author__ = 'fyz'

import simplejson as json
import six
import requests
import traceback
import logging
import hashlib

from binance.client import Client

from model import *
from base import Exchange


class Binance(Exchange):
    def __init__(self, key="", secret=""):
        super(Binance, self).__init__("binance", key, secret, "")
        self.client = Client(key, secret)

    def __transfer_symbol(self, s):
        return s.replace("_", "").upper()

    def fetch_depth(self, symbol):
        symbol = self.__transfer_symbol(symbol)
        return self.client.get_order_book(symbol=symbol)

    def account(self):
        data = self.client.get_account()
        return self._parse_account(data)

    def _parse_account(self, data):
        acc = Account(self)
        for x in data['balances']:
            coin = x['asset'].lower()
            freeze = float(x['locked'])
            avail = float(x['free'])
            if freeze > 0 or avail > 0:
                acc.set_avail(coin, avail).set_freeze(coin, freeze)
        return acc

    def order(self, symbol, side, type="limit", amount=None, price=None):
        assert type == 'limit'  # 暂时支持limit
        assert side in self.TRADE_SIDE

        symbol = self.__transfer_symbol(symbol)
        if side == 'buy':
            return self.client.order_limit_buy(symbol=symbol, quantity=amount, price=price)['orderId']
        else:
            return self.client.order_limit_sell(symbol=symbol, quantity=amount, price=price)['orderId']

    def order_info(self, symbol, order_id):
        data = self.client.get_order(symbol=self.__transfer_symbol(symbol), orderId=order_id)
        return self._parse_order(data)

    def _parse_order(self, data):
        return data

    def cancel_order(self, symbol, order_id):
        self.client.cancel_order(symbol=self.__transfer_symbol(symbol), orderId=order_id)
        return True


if __name__ == '__main__':
    import os, sys

    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from util import read_conf

    conf = read_conf(os.path.join(os.path.dirname(__file__), "../config.json"))
    api = Binance(conf['apikey']['binance']['key'], conf['apikey']['binance']['secret'])
    # print api.fetch_ticker('eth_usdt')
    # print api.fetch_depth("btc_usdt")
    # print api.account()
    print api.buy_limit('btc_usdt', 0.001, 8000)
    # print api.order("eth_usdt", 'buy', price=5000, amount=0.001)
    # print api.cancel_order('eth_usdt', '2018012731290381')
    # print api.order_info('eth_usdt', '2018012529759714')
