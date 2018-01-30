# coding: utf8

import time
import logging

from exchange import Zb, Okex
from util import cur_ms, read_conf
from order import OrderManager, StrategyManager
from exchange.model import ORDER_STATUS


class OrderScanner(object):
    def __init__(self, config, *args, **kw):
        key_config = config['apikey']
        self.order_manager = OrderManager(*args, **kw)
        self.strategy_manager = StrategyManager(*args, **kw)
        self.exchanges = {
            'zb': Zb(key_config['zb']['key'], key_config['zb']['secret']),
            'okex': Okex(key_config['okex']['key'], key_config['okex']['secret'])
        }

    def scan(self):
        while True:
            placed_orders = self.order_manager.list_by_status(ORDER_STATUS.PLACED)
            for order in placed_orders:
                api = self.exchanges[order.exchange]
                symbol = order.coin + "_usdt"
                order_info = api.order_info(symbol, order.ex_id)
                if order_info.status == ORDER_STATUS.SUCCESS:
                    logging.info("update order success %s, %s, %s" % (order.exchange, order.id, order.ex_id))
                    self.order_manager.success(order['id'])

            sts = self.strategy_manager.list_by_status(0)
            for st in sts:
                first_order = self.order_manager.get_by_id(st['first_order_id'])
                second_order = self.order_manager.get_by_id(st['second_order_id'])
                if first_order['status'] == ORDER_STATUS.SUCCESS and second_order['status'] == ORDER_STATUS.SUCCESS:
                    self.strategy_manager.update_status(st['id'], 1)

            time.sleep(5)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    from skyb import MysqlEngine

    conf = read_conf()
    engine = MysqlEngine(conf['db']['url'])
    scanner = OrderScanner(conf, engine=engine)
    print scanner.scan()
