# coding: utf8

from util import cur_ms, read_conf

from exchange.model import ORDER_STATUS


class OrderManager(object):
    def __init__(self, engine=None):
        self.engine = engine

    def init_order(self, exchange, coin, side, amount, price, type="limit"):
        coin = coin.lower()
        sql = "insert into `order` (exchange, coin, type, side, amount, price, status, ts) values \
			(?, ?, ?, ?, ?, ?, ?, ?)"
        a = self.engine.execute(sql, (exchange, coin, type, side, amount, price, ORDER_STATUS.INIT, cur_ms()))
        return a.lastrowid

    def update_ex_id(self, id, ex_id):
        sql = "update `order` set ex_id = ?, status=? where id = ?"
        self.engine.execute(sql, (ex_id, ORDER_STATUS.PLACED, id))

    def update_status(self, id, status):
        sql = "update `order` set status = ? where id = ?"
        self.engine.execute(sql, (status, id))

    def success(self, id):
        sql = "update `order` set status = ?, success_ts = ? where id = ?"
        self.engine.execute(sql, (ORDER_STATUS.SUCCESS, cur_ms(), id))

    def list_by_status(self, status):
        sql = "select * from `order` where status = ?"
        return self.engine.fetch_row(sql, (status,))

    def get_by_id(self, order_id):
        sql = "select * from `order` where id = ?"
        return self.engine.fetchone_row(sql, (order_id,))


class StrategyManager(object):
    def __init__(self, engine):
        self.engine = engine

    def insert(self, name, first_order_id, second_order_id, benefit, amount):
        sql = "insert into `strategy` (name, first_order_id, second_order_id, benefit, amount, ts) values \
              (?, ?, ?, ?, ?, ?)"
        a = self.engine.execute(sql, (name, first_order_id, second_order_id, benefit, amount, cur_ms()))
        return a.lastrowid

    def get_sum_amount_by_name(self, name):
        sql = "select sum(amount) from `strategy` where name = ?"
        a = self.engine.fetchone(sql, (name,))[0]
        if not a:
            return 0
        return a

    def list_by_status(self, sta):
        sql = "select * from `strategy` where status = ?"
        return self.engine.fetch_row(sql, (sta,))

    def update_status(self, id, status):
        sql = "update `strategy` set status = ? where id = ?"
        self.engine.execute(sql, (status, id))

    def get_unfinished(self):
        sql = "select * from `strategy` where status = 0"
        return self.engine.fetch_row(sql, ())
