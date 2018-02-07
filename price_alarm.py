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


class PriceAlarm(object):
    def __init__(self, config, debug):
        self.debug = debug
        self.exchange = Okex(config['apikey']['okex']['key'], config['apikey']['okex']['secret'])
        self.engine = MysqlEngine(config['db']['url'])
        self.symbols = None
        self.refresh_db_config()

        self.pool = ThreadPoolExecutor(4)
        self.pool.submit(self.sched_refresh_db_config, ())
        self.pool.submit(self.reset_success_alarm, ())

        self.success_alarm = False

    def refresh_db_config(self):
        sql = "select * from price_monitor_symbols where status = 1"
        self.symbols = self.engine.fetch_row(sql, ())

    def sched_refresh_db_config(self):
        while True:
            try:
                self.refresh_db_config()
            except:
                logging.error("refresh error")
            time.sleep(1 * 60)

    def reset_success_alarm(self):
        while True:
            self.success_alarm = False
            time.sleep(5 * 60)

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
