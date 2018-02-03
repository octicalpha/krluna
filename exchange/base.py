# coding: utf8

import simplejson as json
import urllib
import requests
import traceback
import logging
import hashlib
import arrow

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

try:
    import pandas as pd
    import numpy as np
except:
    pd = None


class Exchange(object):
    TRADE_SIDE = ('buy', 'sell',)
    TRADE_TYPE = ('limit', 'market',)

    def __init__(self, id, api_key, secret, host):
        self.id = id
        self.api_key = api_key
        self.secret = secret
        self.host = host
        self.timeout = 10

    def order(self, symbol, side, type='limit', amount=None, price=None):
        # TODO(zz) 实现余额不足Exception
        raise NotImplementedError

    def buy_limit(self, symbol, amount=None, price=None):
        assert amount is not None and price is not None
        return self.order(symbol, "buy", type="limit", amount=amount, price=price)

    def sell_limit(self, symbol, amount=None, price=None):
        assert amount is not None and price is not None
        return self.order(symbol, "sell", type="limit", amount=amount, price=price)

    def get(self, path, host=None, data=None):
        if host is None:
            url = self.host + path
        else:
            url = host + path
        if data is not None:
            data = urllib.urlencode(data)
        try:
            return json.loads(requests.get(url, params=data, timeout=self.timeout, verify=False).content)
        except Exception, e:
            logging.exception("get data error %s" % url)
            raise e

    def post(self, path, data, host=None):
        if host is None:
            url = self.host + path
        else:
            url = host + path
        try:
            return json.loads(requests.post(url, data=data, timeout=self.timeout, verify=False).content)
        except Exception, e:
            logging.exception("post data error %s" % url)
            raise e

    def post_json(self, path, data, host=None):
        if host is None:
            url = self.host + path
        else:
            url = host + path
        headers = {
            "Accept": "application/json",
            'Content-Type': 'application/json',
            "User-Agent": "Chrome/39.0.2171.71",
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:53.0) Gecko/20100101 Firefox/53.0'
        }
        try:
            return json.loads(
                requests.post(url, data=json.dumps(data), timeout=self.timeout, verify=False, headers=headers).content)
        except Exception, e:
            logging.exception("post data error %s" % url)
            raise e

    def _kline_to_data_frame(self, data, freq=None, idx=[0, 1, 2, 3, 4, 5]):
        ts_idx, open_idx, high_idx, low_idx, close_idx, vol_idx = idx
        uniq_data = []
        exist = set([])
        for x in data:
            if x[ts_idx] not in exist:
                uniq_data.append(x)
                exist.add(x[ts_idx])
        data = uniq_data
        begin = arrow.get(data[0][ts_idx] / 1000).to('local').datetime
        end = arrow.get(data[-1][ts_idx] / 1000).to('local').datetime
        dr = pd.date_range(begin, end, freq=freq)
        dataframe = pd.DataFrame({
            "open": np.array([float(x[open_idx]) for x in data]),
            "high": np.array([float(x[high_idx]) for x in data]),
            "low": np.array([float(x[low_idx]) for x in data]),
            "close": np.array([float(x[close_idx]) for x in data]),
            "volume": np.array([float(x[vol_idx]) for x in data])
        }, index=dr)
        return dataframe
