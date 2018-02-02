# coding: utf8

import logging

import arrow

from util import top, cur_ms


class Balancer(object):
    TRADE_SIDE_LEFT_TO_RIGHT = "->"
    TRADE_SIDE_RIGHT_TO_LEFT = "<-"
    TRADE_SIDES = (TRADE_SIDE_RIGHT_TO_LEFT, TRADE_SIDE_LEFT_TO_RIGHT)


class DefaultTwoSideBalancer(Balancer):
    def __init__(self, left_coin, left_money, right_coin, right_money, retracement=0):
        self.left_coin = left_coin
        self.left_money = left_money
        self.right_coin = right_coin
        self.right_money = right_money
        self.retracement = retracement

        self.trade_cnt = 0

        self.tick_cnt = 0

    def init(self):
        pass

    def can_trade(self, amount_coin, amount_money=None, side=Balancer.TRADE_SIDE_LEFT_TO_RIGHT, coin_price=None):
        if amount_money is None:
            assert coin_price is not None
            amount_money = amount_coin * coin_price

        assert side in self.TRADE_SIDES
        if side == Balancer.TRADE_SIDE_LEFT_TO_RIGHT:
            if self.left_coin <= amount_coin:
                return False
            if self.right_money <= amount_money:
                return False
        elif side == Balancer.TRADE_SIDE_RIGHT_TO_LEFT:
            if self.right_coin <= amount_coin:
                return False
            if self.left_money <= amount_money:
                return False
        return True

    def sync_by_trade(self, amount_coin, amount_money=None, side=Balancer.TRADE_SIDE_LEFT_TO_RIGHT, coin_price=None):
        assert side in self.TRADE_SIDES
        if amount_money is None:
            assert coin_price is not None
            amount_money = amount_coin * coin_price
        if side == Balancer.TRADE_SIDE_LEFT_TO_RIGHT:
            self.left_coin -= amount_coin
            self.right_coin += amount_coin
            self.left_money += amount_money
            self.right_money -= amount_money
        else:
            self.left_coin += amount_coin
            self.right_coin -= amount_coin
            self.left_money -= amount_money
            self.right_money += amount_money

        self.trade_cnt += 1

    def tick(self):
        pass

    def get_init_threshold(self):
        # TODO(zz) 添加根据历史回归判断
        return 1.0055, 1.0055

    def get_threshold(self):
        # TODO(zz) 根据历史情况决定值, 否则会错过大机会

        self.tick_cnt += 1

        total = self.left_coin + self.right_coin
        left_radio = self.left_coin / total
        right_radio = self.right_coin / total

        l_to_r, r_to_l = self.get_init_threshold()
        if left_radio < 0.01:  # 左边太少, 需要 left <- right
            l_to_r += 2
        elif left_radio < 0.1:
            l_to_r += 0.0065
        elif left_radio < 0.2:
            l_to_r += 0.004
        elif left_radio < 0.3:
            l_to_r += 0.003
        elif left_radio < 0.4:
            l_to_r += 0.0025
        elif left_radio < 0.5:
            l_to_r += 0.002
        elif left_radio < 0.6:
            l_to_r += 0.0015
        elif left_radio < 0.7:
            l_to_r += 0.001

        if right_radio < 0.01:  # 右边太少, 需要 left -> right
            r_to_l += 2
        elif right_radio < 0.1:
            r_to_l += 0.0065
        elif right_radio < 0.2:
            r_to_l += 0.004
        elif right_radio < 0.3:
            r_to_l += 0.003
        elif right_radio < 0.4:
            r_to_l += 0.0025
        elif right_radio < 0.5:
            r_to_l += 0.002
        elif right_radio < 0.6:
            r_to_l += 0.0015
        elif right_radio < 0.7:
            r_to_l += 0.001

        return l_to_r, r_to_l

    def get_trade_coin_amount(self, side, benefit):
        assert side in Balancer.TRADE_SIDES

        base = 0.001
        if benefit > 1.04:
            return 0.005
        if benefit > 1.028:
            return 0.004
        elif benefit > 1.018:
            return 0.003
        elif benefit > 1.0:
            return 0.002
        else:
            return base


class NightSleepTwoSideBalancer(DefaultTwoSideBalancer):
    '''
    凌晨交易稀少
    '''

    def get_init_threshold(self):
        now = arrow.now().to('local')
        if now.hour < 7 or now.hour >= 20:
            return 1.004, 1.004
        else:
            return super(NightSleepTwoSideBalancer, self).get_init_threshold()

class BackSeeTwoSideBalancer(DefaultTwoSideBalancer):
    '''
    看最近最大差价, 如果太小, 那么要动态调整基础值, 提高交易量
    '''

    def init(self, engine, diff_table_name, coin="BTC"):
        self.engine = engine
        self.diff_table_name = diff_table_name
        self.last_fetch_ts = 0

        self.min_threshold = 1.0235 # 暂时还不支持回撤方式

        self.last_init_l_to_r = 1.0285
        self.last_init_r_to_l = 1.0255

        self.coin = coin

        return self

    def _get_base_thres_by_window(self, strategy, ts, limit):
        sql = "select " + strategy + " from " + self.diff_table_name + " where coin=? and ts > ? order by " + strategy + " desc limit ?"
        logging.info(sql)
        res = self.engine.fetch(sql, (self.coin, ts, limit))
        if len(res) == 0:
            return 0
        return float(res[-1][0])

    def refresh_back_context(self):
        now = cur_ms()
        if now - self.last_fetch_ts < 3 * 60 * 1000: # 3min one time
            return False, None 
        now = arrow.now().to('local')
        res = []
        for strategy in ('ab', 'ba'):
            big_window = self._get_base_thres_by_window(strategy, now.shift(hours=-2).timestamp * 1000, 40)
            mid_window = self._get_base_thres_by_window(strategy, now.shift(hours=-1).timestamp * 1000, 25)
            small_window = self._get_base_thres_by_window(strategy, now.shift(minutes=-5).timestamp * 1000, 5)
            estimate_thres = (big_window * 2 + mid_window * 3 + small_window * 5) / 10
            if estimate_thres < 0.8:
                return False, None
            res.append(estimate_thres)
        logging.info("back see threshold %s" % res)
        self.last_fetch_ts = cur_ms()
        return True, res 

    def get_init_threshold(self):
        refresh, res = self.refresh_back_context()
        if not refresh:
            return self.last_init_l_to_r, self.last_init_r_to_l
        else:
            if res[0] < self.last_init_l_to_r: # 暂时不增大基础值, 后续需要改进
                self.last_init_l_to_r = max(self.min_threshold, res[0])
            if res[1] < self.last_init_r_to_l:
                self.last_init_r_to_l = max(self.min_threshold, res[1])
        return self.last_init_l_to_r, self.last_init_r_to_l

class CrossOneTwoSideBalancer(DefaultTwoSideBalancer):
    def get_init_threshold(self):
        return super(CrossOneTwoSideBalancer, self).get_init_threshold()

    def get_threshold(self):
        return super(CrossOneTwoSideBalancer, self).get_threshold()
