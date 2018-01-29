# coding: utf8
import simplejson as json
import six
import requests
import traceback
import logging
import hashlib

from model import *
from base import Exchange


class Okex(Exchange):
    def __init__(self, api_key='', secret=''):
        super(Okex, self).__init__('okex', api_key, secret, 'https://www.okex.com')

    def fetch_ticker(self, symbol):
        symbol = symbol.lower()
        endpoint = '%s?symbol=%s' % ('/api/v1/ticker.do', symbol)
        return self.get(endpoint)

    def fetch_depth(self, symbol):
        symbol = symbol.lower()
        endpoint = '%s?symbol=%s' % ("/api/v1/depth.do", symbol)
        return self.get(endpoint)

    def sign(self, params):
        sign = ''

        for key in sorted(params.keys()):
            sign += key + '=' + str(params[key]) + '&'
        data = sign + 'secret_key=' + self.secret
        return hashlib.md5(data.encode("utf8")).hexdigest().upper()

    def account(self):
        endpoint = '/api/v1/userinfo.do'
        params = {}
        params['api_key'] = self.api_key
        params['sign'] = self.sign(params)
        res = self.post(endpoint, params)
        return self._parse_account(res)

    def _parse_account(self, acc):
        account = Account(self)
        freeze = acc['info']['funds']['freezed']
        free = acc['info']['funds']['free']
        for k, v in six.iteritems(freeze):
            if float(v) > 0:
                account.add(Asset(k, float(free[k]), float(freeze[k])))
        for k, v in six.iteritems(free):
            if float(v) > 0:
                account.add(Asset(k, float(free[k]), float(freeze[k])))
        return account

    def _check_trade_param(self, side, type):
        assert side in self.TRADE_SIDE
        assert type in self.TRADE_TYPE

    def order(self, symbol, side, type='limit', amount=None, price=None):
        endpoint = '/api/v1/trade.do'
        trade_type = None
        if type == 'limit':
            trade_type = side
        elif type == 'market':
            trade_type = side + "_market"
        assert trade_type is not None
        params = {
            'api_key': self.api_key,
            'symbol': symbol,
            'type': trade_type
        }
        if price is not None:
            params['price'] = price
        if amount is not None:
            params['amount'] = amount
        params['sign'] = self.sign(params)
        res = self.post(endpoint, params)
        try:
            return res['order_id']
        except:
            raise Exception("okex order error: " + res)

    def cancel_order(self, symbol, order_id):
        endpoint = "/api/v1/cancel_order.do"
        params = {
            'api_key': self.api_key,
            'symbol': symbol,
            'order_id': order_id
        }
        params['sign'] = self.sign(params)
        return self.post(endpoint, params)

    def order_info(self, symbol, order_id):
        endpoint = "/api/v1/order_info.do"
        params = {
            'api_key': self.api_key,
            'symbol': symbol,
            'order_id': order_id
        }
        params['sign'] = self.sign(params)
        
        return self._parser_order(self.post(endpoint, params))

    def _parser_order(self, data):
        '''
            status: -1 = cancelled, 0 = unfilled, 1 = partially filled, 2 = fully filled, 3 = cancel request in process
        '''
        od = data['orders'][0]
        status = None
        status_int = od['status']
        if status_int == -1 or status_int == 3:
            status = ORDER_STATUS.CANCELLED
        elif status_int == 0:
            status = ORDER_STATUS.PLACED
        elif status_int == 1:
            status = ORDER_STATUS.PARTIAL_SUCCESS
        elif status_int == 2:
            status = ORDER_STATUS.SUCCESS
        return Order(od['order_id'], od['symbol'], od['price'], od['amount'], od['create_date'], od['type'], status)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    api = Okex()
    # print api.fetch_ticker('eth_usdt')
    # print api.fetch_depth('eth_usdt')
    print api.account()
    # print api.order('eth_usdt', 'buy', 'limit', 0.1, 10)
    # print api.order_info('eth_usdt', 1000)
    # print api.cancel_order('eth_usdt', 1000)
