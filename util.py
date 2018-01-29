# coding: utf8

import os
import arrow
import time
import requests
import simplejson as json
import threading


def read_conf(path="./config.json"):
    with open(path, 'r') as fp:
        content = ''.join(fp.readlines())
        return eval(content)


def cur_ms():
    return int(time.time() * 1000)


def avg(li):
    return sum(li) / len(li)


def ms_to_str(ms):
    if ms <= 0:
        return ''
    return arrow.get(int(ms) / 1000).to('local').format("YYYY-MM-DD HH:mm:ss")


def ms_to_humanize(ms):
    if ms <= 0:
        return ''
    return arrow.get(int(ms) / 1000).to('local').humanize()


_conf = read_conf(os.path.join(os.path.dirname(__file__), "./config.json"))


def slack(msg):
    url = _conf['slack']['bot']['push_url']

    def _inner(msg):
        data = {'text': msg}
        requests.post(url, data=json.dumps(data))

    threading.Thread(target=_inner, args=(msg,)).start()
