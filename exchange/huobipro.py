# coding: utf8
import simplejson as json
import urllib, hmac, base64, datetime
import requests
import traceback
import logging
import hashlib
from model import *

from base import Exchange


class Huobipro(Exchange):
    def __init__(self, key="", secret=""):
        super(Huobipro, self).__init__("huobipro", key, secret, "https://api.huobi.pro")
        self.sign_hostname = "api.huobi.pro"
        self.acc_id = self._get_account_id()

    def fetch_depth(self, symbol):
        symbol = symbol.replace("_", "")
        symbol = symbol.lower()
        endpoint = '/market/depth'
        params = {
            "symbol": symbol,
            "type": "step0"
        }
        return self.get(endpoint, data=params)['tick']

    def fetch_ticker(self, symbol):
        pass

    def sign(self, params, method, request_path):
        sorted_params = sorted(params.items(), key=lambda d: d[0], reverse=False)
        encode_params = urllib.urlencode(sorted_params)
        payload = [method, self.sign_hostname, request_path, encode_params]
        payload = '\n'.join(payload)
        payload = payload.encode(encoding='UTF8')
        secret_key = self.secret.encode(encoding='UTF8')
        digest = hmac.new(secret_key, payload, digestmod=hashlib.sha256).digest()
        signature = base64.b64encode(digest)
        signature = signature.decode()
        return signature.encode("utf8")

    def _api_key_get(self, path, params):
        method = 'GET'
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params.update({'AccessKeyId': self.api_key,
                       'SignatureMethod': 'HmacSHA256',
                       'SignatureVersion': '2',
                       'Timestamp': timestamp})

        params['Signature'] = self.sign(params, method, path)
        return self.get(path, data=params)

    def _api_key_post(self, path, params):
        method = 'POST'
        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
        params_to_sign = {'AccessKeyId': self.api_key,
                          'SignatureMethod': 'HmacSHA256',
                          'SignatureVersion': '2',
                          'Timestamp': timestamp}

        params_to_sign['Signature'] = self.sign(params_to_sign, method, path)
        path = path + '?' + urllib.urlencode(params_to_sign)
        return self.post_json(path, data=params)

    def _get_account_id(self):
        endpoint = "/v1/account/accounts"
        return self._api_key_get(endpoint, {})['data'][0]['id']

    def account(self):
        endpoint = "/v1/account/accounts/{0}/balance".format(self.acc_id)
        params = {"account-id": self.acc_id}
        res = self._api_key_get(endpoint, params)
        return self._parse_account(res)

    def _parse_account(self, data):
        acc = Account(self)
        for x in data['data']['list']:
            if float(x['balance']) > 0:
                if x['type'] == 'trade':
                    acc.set_avail(float(x['balance']))
                elif x['type'] == 'frozen':
                    acc.set_freeze(float(x['balance']))
        return acc

    def order(self, symbol, side, type="limit", amount=None, price=None):
        huobi_type = '%s-%s' % (side, type)
        huobi_symbol = symbol.replace("_", "")
        params = {"account-id": self.acc_id,
                  "amount": amount,
                  "symbol": huobi_symbol,
                  "type": huobi_type,
                  "source": "api"}
        if price is not None:
            params["price"] = price

        endpoint = '/v1/order/orders/place'
        return self._api_key_post(endpoint, params)['data']

    def cancel_order(self, symbol, order_id):
        endpoint = "/v1/order/orders/{0}/submitcancel".format(order_id)
        return self._api_key_post(endpoint, {})['status'] == 'ok'

    def order_info(self, symbol, order_id):
        url = "/v1/order/orders/{0}".format(order_id)
        res = self._api_key_get(url, {})
        return self._parse_order(res)

    def _parse_order(self, data):
        '''
            pre-submitted 准备提交, submitting , submitted 已提交, partial-filled 部分成交, partial-canceled 部分成交撤销,
            filled 完全成交, canceled 已撤销
        '''
        data = data['data']
        state = data['state']
        status = ORDER_STATUS.UNKNOWN
        if state == 'submitted':
            status = ORDER_STATUS.PLACED
        elif state == 'partial-filled':
            status = ORDER_STATUS.PARTIAL_SUCCESS
        elif state == 'filled':
            status = ORDER_STATUS.SUCCESS
        elif state == 'canceled':
            status = ORDER_STATUS.CANCELLED
        return Order(data['id'], data['symbol'], data['price'], data['amount'], data['created-at'], data['type'].split("-")[0], status)

if __name__ == '__main__':
    import os, sys

    sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
    from util import read_conf

    conf = read_conf(os.path.join(os.path.dirname(__file__), "../config.json"))
    api = Huobipro(conf['apikey']['huobipro']['key'], conf['apikey']['huobipro']['secret'])
    # print api.fetch_depth("eth_usdt")
    # print api.account()
    print api.order("btc_usdt", "buy", amount=0.001, price=7000)
