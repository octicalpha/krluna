# coding: utf8

import os
import os.path
import simplejson as json
import decimal
import logging
import time
import arrow
import heapq

from skyb.quick.tornado import BaseApplication
import tornado.web

from skyb.model import MysqlEngine
from order import *
from util import ms_to_str, ms_to_humanize


class ApiApplication(BaseApplication):
    def __init__(self, debug=False, config=None):
        handlers = [
            ('/', IndexHandler),
            ('/orders', OrderHandler),
            ('/st', StrategyHandler),
            ('/cancel_order', CancelOrderHandler),
        ]
        settings = dict(template_path=os.path.join(os.path.dirname(__file__), "./web/template"),
                        static_path=os.path.join(os.path.dirname(__file__), "./web/static"),
                        debug=debug,
                        cookie_secret="woshifyz",
                        autoescape=None
                        )
        self.engine = MysqlEngine(config['db']['url'])
        self.order_manager = OrderManager(self.engine)
        super(ApiApplication, self).__init__(handlers, **settings)


def top(li, percent=0.1):
    x = min(int(len(li) * percent), 50)

    return min(heapq.nlargest(x, li))


class OrderHandler(tornado.web.RequestHandler):
    def get(self):
        status = self.get_argument('status', None)
        if status is None:
            sql = "select * from `order` order by id desc"
            data = self.application.engine.fetch_row(sql, ())
        else:
            sql = "select * from `order` where status = ? order by id desc"
            data = self.application.engine.fetch_row(sql, (int(status),))
        for x in data:
            x['ts'] = ms_to_humanize(x['ts'])
            x['success_ts'] = ms_to_humanize(x['success_ts'])

        self.render("orders.html", orders=data)


class StrategyHandler(tornado.web.RequestHandler):
    def get(self):
        sql = "select * from `strategy` order by id desc"
        data = self.application.engine.fetch_row(sql, ())
        for x in data:
            x['ts'] = ms_to_humanize(x['ts'])

        self.render("strategy.html", s=data)


class CancelOrderHandler(tornado.web.RequestHandler):
    def post(self):
        id = int(self.get_argument("id"))
        pass


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        coin = self.get_argument('coin', 'BTC')
        limit = int(self.get_argument('limit', '2000'))
        table = self.get_argument('table', 'zb_okex')

        coin = coin.upper()
        table = "diff_" + table
        sql = "select * from (select * from " + table + " where coin = ? order by id desc limit ?) sub order by id asc"
        data = self.application.engine.fetch(sql, (coin, limit))

        ab_seq = [[x[0], x[2]] for x in data]
        ba_seq = [[x[0], x[3]] for x in data]

        top_ab = top([x[1] for x in ab_seq])
        top_ba = top([x[1] for x in ba_seq])
        title = '%s ~ %s GOOD: %s %s %s' % (
        ms_to_str(data[0][-1]), ms_to_str(data[-1][-1]), top_ab, top_ba, top_ab * top_ba)

        option = {
            "title": {"text": title},
            "xAxis": {
                "type": 'value',
                # "data": range(len(data))
                "min": "dataMin",
                "max": "dataMax",
            },
            "yAxis": {
                "type": 'value',
                "min": "dataMin",
                "max": "dataMax",
            },
            "boundaryGap": [0, '100%'],
        }
        series = []
        series.append({
            'name': 'ab',
            'type': 'line',
            'data': ab_seq
        })
        series.append({
            'name': 'ba',
            'type': 'line',
            'data': ba_seq
        })
        option['series'] = series

        self.render("index.html", option=json.dumps(option))


if __name__ == '__main__':
    from util import read_conf

    ApiApplication(True, config=read_conf()).boot(8000)
