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
from util import ms_to_str, ms_to_humanize, top, avg


class ApiApplication(BaseApplication):
    def __init__(self, debug=False, config=None):
        handlers = [
            ('/', IndexHandler),
            ('/abs_diff', AbsDiffHandler),
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


class OrderHandler(tornado.web.RequestHandler):
    def get(self):
        status = self.get_argument('status', None)
        limit = int(self.get_argument('limit', 30))
        if status is None:
            sql = "select * from `order` order by id desc limit ?"
            data = self.application.engine.fetch_row(sql, (limit,))
        else:
            sql = "select * from `order` where status in (?, 99) order by id desc limit ?"
            data = self.application.engine.fetch_row(sql, (int(status), limit))
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
        fee = 0.003
        title = '%s ~ %s GOOD: %s %s %s' % (
            ms_to_str(data[0][-1]), ms_to_str(data[-1][-1]), top_ab, top_ba,
            (float(top_ab) - fee) * (float(top_ba) - fee))

        option = {
            "animation": False,
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


class AbsDiffHandler(tornado.web.RequestHandler):
    def get(self):
        symbol = self.get_argument('symbol', 'BTC_USDT')
        limit = int(self.get_argument('limit', '2000'))
        table = self.get_argument('table', 'okex_binance')
        delta = int(self.get_argument('delta', '0'))
        begin = int(self.get_argument('begin', '0'))
        end = int(self.get_argument('end', '0'))

        table = "abs_diff_" + table
        if begin > 0 and end > 0:
            sql = "select * from " + table + " where symbol=? and ts between ? and ?"
            data = self.application.engine.fetch_row(sql, (symbol, begin, end))
        else:
            sql = "select * from (select * from " + table + " where symbol = ? order by id desc limit ?) sub order by id asc"
            data = self.application.engine.fetch_row(sql, (symbol, limit))

        option = {
            "animation": False,
            "title": {"text": "diff"},
            "xAxis": {
                "type": 'value',
                "min": "dataMin",
                "max": "dataMax",
            },
            "yAxis": {
                "type": 'value',
                "min": "dataMin",
                "max": "dataMax",
            },
            "boundaryGap": [0, '100%'],
            "legend": {
                "data": ["diff_bid", "diff_ask", "diff_price"],
            }
        }
        series = []
        # series.append({
        #     'name': 'diff_bid',
        #     'type': 'line',
        #     'data': [[x.ts, x.trade_bid - x.base_bid] for x in data]
        # })
        # series.append({
        #     'name': 'diff_ask',
        #     'type': 'line',
        #     'data': [[x.ts, x.trade_ask - x.base_ask] for x in data]
        # })
        window = 10
        base_data = []
        dt = [float(x.base_price) for x in data]
        for i in range(len(dt)):
            if i < window:
                v = avg(dt[:i+1])
            else:
                v = avg(dt[i-window:i+1])
            if v > 0:
                base_data.append([data[i].id, v])
                data[i].base_ma = v
                
        t_data = []
        dt = [float(x.trade_price) for x in data]
        for i in range(len(dt)):
            if i < window:
                v = avg(dt[:i+1])
            else:
                v = avg(dt[i-window:i+1])
            if v > 0:
                t_data.append([data[i].id, v])
                data[i].trade_ma = v
        if delta > 0:
            series.append({
                'name': 'diff',
                'type': 'line',
                'data': [[x.id, x.trade_ma - x.base_ma] for x in data],
            })
        else:
                
            series.append({
                'name': 'base',
                'type': 'line',
                'data': base_data
            })
            series.append({
                'name': 'price',
                'type': 'line',
                'data': t_data
            })
            series.append({
                'name': 'bid',
                'type': 'line',
                'data': [[x.id, float(x.trade_bid)] for x in data]
            })
        option['series'] = series

        self.render("abs_diff.html", option=json.dumps(option))


if __name__ == '__main__':
    from util import read_conf

    ApiApplication(True, config=read_conf()).boot(8000)
