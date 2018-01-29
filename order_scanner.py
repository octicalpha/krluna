# coding: utf8

import time
import logging

from exchange import Zb, Okex
from util import cur_ms, read_conf
from order import OrderManager
from exchange.model import ORDER_STATUS


class OrderScanner(object):
    def __init__(self, config, *args, **kw):
        key_config = config['apikey']
        self.manager = OrderManager(*args, **kw)
        self.exchanges = {
            'zb': Zb(key_config['zb']['key'], key_config['zb']['secret']),
            'okex': Okex(key_config['okex']['key'], key_config['okex']['secret'])
        }

    def scan(self):
        while True:
            placed_orders = self.manager.list_by_status(ORDER_STATUS.PLACED)
            for order in placed_orders:
                api = self.exchanges[order.exchange]
                symbol = order.coin + "_usdt"
                order_info = api.order_info(symbol, order.ex_id)
                if order_info.status == ORDER_STATUS.SUCCESS:
                    logging.info("update order success %s, %s, %s" % (order.exchange, order.id, order.ex_id))
                    self.manager.success(order['id'])

            time.sleep(5)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    from skyb import MysqlEngine

    conf = read_conf()
    engine = MysqlEngine(conf['db']['url'])
    scanner = OrderScanner(conf, engine=engine)
    print scanner.scan()
