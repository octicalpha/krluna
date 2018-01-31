# coding: utf8

import logging

import arrow

from util import top


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
        if left_radio < 0.02:  # 左边太少, 需要 left <- right
            l_to_r += 2
        elif left_radio < 0.1:
            l_to_r += 0.012
        elif left_radio < 0.2:
            l_to_r += 0.008
        elif left_radio < 0.3:
            l_to_r += 0.006
        elif left_radio < 0.4:
            l_to_r += 0.003
        elif left_radio < 0.5:
            l_to_r += 0.0025
        elif left_radio < 0.6:
            l_to_r += 0.002
        elif left_radio < 0.7:
            l_to_r += 0.0015

        if right_radio < 0.02:  # 右边太少, 需要 left -> right
            r_to_l += 2
        elif right_radio < 0.1:
            r_to_l += 0.012
        elif right_radio < 0.2:
            r_to_l += 0.008
        elif right_radio < 0.3:
            r_to_l += 0.006
        elif right_radio < 0.4:
            r_to_l += 0.0025
        elif right_radio < 0.5:
            r_to_l += 0.0025
        elif right_radio < 0.6:
            r_to_l += 0.002
        elif right_radio < 0.7:
            r_to_l += 0.0015

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
        if 0 < now.hour < 7:
            return 1.004, 1.004
        else:
            return super(NightSleepTwoSideBalancer, self).get_init_threshold()


class CrossOneTwoSideBalancer(DefaultTwoSideBalancer):
    def get_init_threshold(self):
        return super(CrossOneTwoSideBalancer, self).get_init_threshold()

    def get_threshold(self):
        return super(CrossOneTwoSideBalancer, self).get_threshold()
