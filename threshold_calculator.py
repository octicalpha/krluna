# coding: utf8

from util import cur_ms, top


class ThresholdCalculator(object):
    def __init__(self, engine, diff_table_name, coin="BTC", fee=0.003, debug=True):
        self.engine = engine
        self.diff_table_name = diff_table_name
        self.diff_table_name_cnt = 4000
        self.coin = coin
        self.fee = fee
        self.debug = debug

        self.st_pair_name = '%s_%s' % (self.diff_table_name, coin)

        self.min_benefit = 1.004

        self.smallest = 0.99

        self.default_pair = (1.0055, 1.0055)

    def get_default_pair_id(self):
        sql = "select * from st_pair where name = ? and ab=? and ba = ?"

    def fetch_diff(self):
        sql = "select * from (select * from " + self.diff_table_name + " where coin = ? order by id desc limit ?) sub order by id asc"
        data = self.engine.fetch(sql, (self.coin, self.diff_table_name_cnt))

        ab_seq = [[x[0], x[2]] for x in data]
        ba_seq = [[x[0], x[3]] for x in data]

        top_ab = top([x[1] for x in ab_seq])
        top_ba = top([x[1] for x in ba_seq])

        return top_ab, top_ba

    def back_test(self, limit):
        sql = "select * from (select * from " + self.diff_table_name + " where coin = ? order by id"
        data = self.engine.fetch(sql, (self.coin,))

        


    def insert_st_pair(self, ab, ba):
        if self.debug:
            return
        sql = "insert into st_pair (`name`, ab, ba, fee, ts) values (?, ? ,? ,? ,?)"
        res = self.engine.execute(sql, (self.st_pair_name, ab, ba, self.fee, cur_ms()))
        return res.lastrowid

    def add_st_pair(self):
        top_ab, top_ba = self.fetch_diff()
        if top_ab < self.smallest or top_ba < self.smallest:
            return self.default_pair
        if (top_ab - self.fee) * (top_ba - self.fee) < self.min_benefit:
            return self.default_pair
        self.insert_st_pair(top_ab, top_ba)
        return top_ab, top_ba

    def cal(self):
        if self.amount_a - self.amount_b > 0.09:  # a策略执行太多, 增加a策略阈值
            self.min_a = self.init_min_a + 1
        elif self.amount_a - self.amount_b > 0.08:  # a策略执行太多, 增加a策略阈值
            self.min_a = self.init_min_a + 0.008
        elif self.amount_a - self.amount_b > 0.04:
            self.min_a = self.init_min_a + 0.005
        elif self.amount_a - self.amount_b > 0.02:
            self.min_a = self.init_min_a + 0.002
        elif self.amount_a - self.amount_b > 0:
            self.min_a = self.init_min_a
        elif self.amount_b - self.amount_a > 0.12:  # b策略执行太多, 增加b策略阈值
            self.min_b = self.init_min_b + 1
        elif self.amount_b - self.amount_a > 0.1:  # b策略执行太多, 增加b策略阈值
            self.min_b = self.init_min_b + 0.008
        elif self.amount_b - self.amount_a > 0.07:
            self.min_b = self.init_min_b + 0.005
        elif self.amount_b - self.amount_a > 0.04:
            self.min_b = self.init_min_b + 0.0035
        elif self.amount_b - self.amount_a > 0.02:
            self.min_b = self.init_min_b + 0.002
        elif self.amount_b - self.amount_a > 0:
            self.min_b = self.init_min_b
        self.min_a = max(1.004, self.min_a)
        self.min_b = max(1.004, self.min_b)
        if not self.has_init_strategy_threshold:
            self.cur_a = self.min_a
            self.cur_b = self.min_b
        else:
            self.cur_a = max(self.cur_a, self.min_a)
            self.cur_b = max(self.cur_b, self.min_b)
