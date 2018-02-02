# coding: utf8
import simplejson as json
import requests
import traceback
import logging
import hashlib
import struct, sha, time

from base import Exchange

from model import *


class Zb(Exchange):
    def __init__(self, key="", secret=""):
        super(Zb, self).__init__('zb', key, secret, 'https://trade.zb.com/api/')
        self.get_host = 'http://api.zb.com'

    def __fill(self, value, lenght, fillByte):
        if len(value) >= lenght:
            return value
        else:
            fillSize = lenght - len(value)
        return value + chr(fillByte) * fillSize

    def __doXOr(self, s, value):
        slist = list(s)
        for index in xrange(len(slist)):
            slist[index] = chr(ord(slist[index]) ^ value)
        return "".join(slist)

    def __hmacSign(self, aValue, aKey):
        aValue = aValue.encode('utf8')
        keyb = struct.pack("%ds" % len(aKey), aKey)
        value = struct.pack("%ds" % len(aValue), aValue)
        k_ipad = self.__doXOr(keyb, 0x36)
        k_opad = self.__doXOr(keyb, 0x5c)
        k_ipad = self.__fill(k_ipad, 64, 54)
        k_opad = self.__fill(k_opad, 64, 92)
        m = hashlib.md5()
        m.update(k_ipad)
        m.update(value)
        dg = m.digest()

        m = hashlib.md5()
        m.update(k_opad)
        subStr = dg[0:16]
        m.update(subStr)
        dg = m.hexdigest()
        return dg

    def __digest(self, aValue):
        value = struct.pack("%ds" % len(aValue), aValue)
        h = sha.new()
        h.update(value)
        dg = h.hexdigest()
        return dg

    def __api_call(self, path, params='', timeout=5):
        SHA_secret = self.__digest(self.secret)
        sign = self.__hmacSign(params, SHA_secret)
        reqTime = (int)(time.time() * 1000)
        params += '&sign=%s&reqTime=%d' % (sign, reqTime)
        url = path + '?' + params
        return self.get(url)

    def _parse_account(self, acc):
        account = Account(self)
        for c in acc['result']['coins']:
            if float(c['available']) > 0 or float(c['freez']) > 0:
                account.add(Asset(c['key'], float(c['available']), float(c['freez'])))
        return account

    def _side_to_code(self, trade_type):
        assert trade_type in ('buy', 'sell')
        if trade_type == 'buy':
            return '1'
        return '0'

    def fetch_ticker(self, symbol):
        symbol = symbol.lower()
        endpoint = '/data/v1/ticker?market=%s' % symbol
        return self.get(endpoint, host=self.get_host)

    def fetch_depth(self, symbol, size=10):
        symbol = symbol.lower()
        endpoint = '/data/v1/depth?market=%s&size=%s' % (symbol, size)
        return self.get(endpoint, host=self.get_host)

    def account(self):
        params_tpl = "accesskey=%s&method=getAccountInfo"
        params = params_tpl % (self.api_key,)
        path = 'getAccountInfo'
        obj = self.__api_call(path, params, timeout=20)
        return self._parse_account(obj)

    def order(self, currency, side, type='limit', price=None, amount=None):
        trade_type_code = self._side_to_code(side)
        obj = ''
        params_tpl = "accesskey=%s&amount=%s&currency=%s&method=order&price=%s&tradeType=%s"
        params = params_tpl % (self.api_key, amount, currency, price, trade_type_code)
        path = 'order'
        obj = self.__api_call(path, params, timeout=20)
        return obj['id']

    def order_info(self, currency, id):
        params_tpl = "accesskey=%s&currency=%s&id=%s&method=getOrder"
        params = params_tpl % (self.api_key, currency, id)
        path = 'getOrder'
        obj = self.__api_call(path, params)
        import pdb; pdb.set_trace()
        return self._parser_order(obj)

    def _parser_order(self, data):
        '''
            挂单状态(1：取消,2：交易完成,3：待成交/待成交未交易部份)
            type : 挂单类型 1/0[buy/sell]
        '''
        od = data
        status = None
        status_int = od['status']
        if status_int == 1:
            status = ORDER_STATUS.CANCELLED
        elif status_int == 2:
            status = ORDER_STATUS.SUCCESS
        elif status_int == 3:
            if od['trade_amount'] > 0:
                status = ORDER_STATUS.PARTIAL_SUCCESS
            else:
                status = ORDER_STATUS.PLACED
        side = 'buy' if od['type'] == 1 else 'sell'
        return Order(od['id'], od['currency'], od['price'], od['total_amount'], od['trade_date'], side, status)

    def cancel_order(self, currency, id):
        params_tpl = "accesskey=%s&currency=%s&id=%s&method=cancelOrder"
        params = params_tpl % (self.api_key, currency, id,)
        path = 'cancelOrder'
        obj = self.__api_call(path, params)
        logging.info("cancel_order %s %s result: %s", self.id, id, obj)
        return obj['code'] == 1000


if __name__ == '__main__':
    import os, sys
    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from util import read_conf
    conf = read_conf(os.path.join(os.path.dirname(__file__), "../config.json"))
    api = Zb(conf['apikey']['zb']['key'], conf['apikey']['zb']['secret'])
    # print api.fetch_ticker('eth_usdt')
    # print api.fetch_depth('eth_usdt')
    print api.account()
    # print api.order("eth_usdt", 'buy', price=5000, amount=0.001)
    # print api.cancel_order('eth_usdt', '2018012731290381')
    # print api.order_info('eth_usdt', '2018012529759714')
