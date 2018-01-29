# coding: utf8

import simplejson as json
import urllib
import requests
import traceback
import logging
import hashlib

from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class Exchange(object):
    TRADE_SIDE = ('buy', 'sell',)
    TRADE_TYPE = ('limit', 'market',)

    def __init__(self, id, api_key, secret, host):
        self.id = id
        self.api_key = api_key
        self.secret = secret
        self.host = host
        self.timeout = 10

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
