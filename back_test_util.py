# coding: utf8
import logging


class BtStatus(object):
    INIT = 0
    PLACE_BUY_ORDER = 1
    SUCCESS_BUY_ORDER = 2
    PLACE_SELL_ORDER = 3
    SUCCESS_SELL_ORDER = 4


class BackTestMixin(object):
    def __init__(self):
        # --------- backtest vars --------------
        self.bt_status = BtStatus.INIT
        self.bt_buy_price = None
        self.bt_sell_price = None
        self.bt_benefit = 0
        self.bt_tx_cnt = 0
        self.bt_force_buy_first = True
        self.bt_min_round_benefit = -10
        # --------- backtest vars --------------

    def _back_test_check_sell_price_is_ok(self, price):
        delta = price - self.bt_buy_price
        if self.bt_buy_price > 8900:
            return delta > -60
        if self.bt_buy_price > 8500:
            return delta > -15
        if self.bt_buy_price < 8200:
            return delta > 40
        return delta > 0

    def back_test_buy(self, price, amount=1, msg=""):
        if not (self.bt_status == BtStatus.INIT or self.bt_status == BtStatus.SUCCESS_SELL_ORDER):
            return
        self.bt_buy_price = price
        self.bt_status = BtStatus.PLACE_BUY_ORDER
        logging.info("buy with price %s, msg: %s" % (self.bt_buy_price, msg))

    def back_test_sell(self, price, amount=1, msg=""):
        if self.bt_force_buy_first and self.bt_status == BtStatus.INIT:
            return
        if not (self.bt_status == BtStatus.INIT or self.bt_status == BtStatus.SUCCESS_BUY_ORDER):
            return
        self.bt_sell_price = price
        if not self._back_test_check_sell_price_is_ok(price):
            return
        self.bt_status = BtStatus.PLACE_SELL_ORDER
        logging.info("sell with price %s, msg: %s" % (self.bt_sell_price, msg))

    def back_test_try_cancel_buy_order(self):
        if self.bt_status != BtStatus.PLACE_BUY_ORDER:  # 有未成交买单是处理
            return
        logging.info("cancel buy order")
        self.bt_status = BtStatus.INIT

    def back_test_check_tx_success(self, high_price, low_price):
        if self.bt_status == BtStatus.PLACE_BUY_ORDER:
            if low_price < self.bt_buy_price:
                logging.info("success buy with price %s" % self.bt_buy_price)
                self.bt_status = BtStatus.SUCCESS_BUY_ORDER
                self.bt_tx_cnt += 1
                if self.bt_tx_cnt % 2 == 0:
                    self.bt_benefit += self.bt_sell_price - self.bt_buy_price
        if self.bt_status == BtStatus.PLACE_SELL_ORDER:
            if high_price > self.bt_sell_price:
                logging.info("success sell with price %s" % self.bt_sell_price)
                self.bt_status = BtStatus.SUCCESS_SELL_ORDER
                self.bt_tx_cnt += 1
                if self.bt_tx_cnt % 2 == 0:
                    self.bt_benefit += self.bt_sell_price - self.bt_buy_price
