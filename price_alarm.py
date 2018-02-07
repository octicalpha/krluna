# coding: utf8

import logging
import click
from collections import defaultdict
from skyb.model import MysqlEngine
import simplejson as json
from exchange import *
from order import *
import os
import time
from util import slack, cur_ms, avg, fix_float_radix, run_cmd
from concurrent.futures import ThreadPoolExecutor
from threading import Thread


def run_thread(fn, args=()):
    t = Thread(target=fn, args=args)
    t.daemon = True
    t.start()


class PriceAlarm(object):
    def __init__(self, config, debug):
        self.debug = debug
        self.exchange = Okex(config['apikey']['okex']['key'], config['apikey']['okex']['secret'])
        self.engine = MysqlEngine(config['db']['url'])
        self.init_db()

        self.symbols = self.refresh_db_config()

        self.pool = ThreadPoolExecutor(4)
        run_thread(self.sched_refresh_db_config)
        run_thread(self.reset_success_alarm)

        self.success_alarm = False

    def init_db(self):
        sql = """
        CREATE TABLE if not EXISTS `price_monitor_symbols` (
          `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
          `exchange` varchar(24) NOT NULL DEFAULT '',
          `symbol` varchar(24) NOT NULL DEFAULT '',
          `low` decimal(18,8) DEFAULT NULL,
          `high` decimal(18,8) DEFAULT NULL,
          `status` int(11) NOT NULL DEFAULT '1',
          `ts` bigint(20) DEFAULT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY `ukey__exchange_symbol` (`exchange`,`symbol`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8
        """
        self.engine.execute(sql, ())

    def refresh_db_config(self):
        sql = "select * from price_monitor_symbols where status = 1"
        return self.engine.fetch_row(sql, ())

    def is_symbols_diff(self, a, b):
        if len(a) != len(b):
            return True
        a_map = {}
        for x in a:
            a_map[x.exchange + x.symbol] = [float(x.low), float(x.high)]
        for x in b:
            key = x.exchange + x.symbol
            if key not in a_map:
                return True
            a_v = a_map[key]
            if float(x.low) - a_v[0] > 1:
                return True
            if float(x.high) - a_v[1] > 1:
                return True
        return False

    def sched_refresh_db_config(self):
        while True:
            try:
                new_data = self.refresh_db_config()
                logging.info("refresh %s" % new_data)
                if self.is_symbols_diff(self.symbols, new_data):
                    self.success_alarm = False
                    logging.info("succ")
                self.symbols = new_data
            except:
                logging.exception("refresh error")
            time.sleep(2)

    def reset_success_alarm(self):
        while True:
            self.success_alarm = False
            time.sleep(3 * 60)

    def run(self):
        if not self.symbols:
            logging.error("no symbol to monitor")
            return
        while True:
            try:
                self._run()
            except:
                logging.exception("")
            time.sleep(5)

    def _run(self):
        for x in self.symbols:
            ticker = self.exchange.fetch_ticker(x.symbol)
            if x.low < ticker.price < x.high:
                self.notify("price reach %s" % ticker.price)
                return True
            return False

    def notify(self, msg):
        if not self.success_alarm:
            if self.debug:
                os.system('say "price monitor finished"')
            else:
                slack(msg)
            self.success_alarm = True


@click.command()
@click.option("-d", "--debug", is_flag=True)
def main(debug):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    from util import read_conf
    config = read_conf("./config.json")
    PriceAlarm(config, debug).run()


if __name__ == '__main__':
    main()
