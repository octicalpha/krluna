# coding: utf8

from util import cur_ms, read_conf
import simplejson as json
from exchange import *
from skyb import MysqlEngine
import click


class Tool(object):
    def __init__(self, config):
        key_config = config['apikey']
        self.exchanges = {
            'zb': Zb(key_config['zb']['key'], key_config['zb']['secret']),
            'okex': Okex(key_config['okex']['key'], key_config['okex']['secret']),
        }

        self.engine = MysqlEngine(config['db']['url'])

    def record_account(self):
        sql = "insert into `account` values (null, ?, ?)"
        acc = {}
        for k, v in self.exchanges.iteritems():
            acc[k] = str(v.account())

        self.engine.execute(sql, (json.dumps(acc), cur_ms()))

    def check_benefit(self):
        total_usdt = 0
        total_btc = 0
        for k, v in self.exchanges.iteritems():
            acc = v.account()
            total_usdt += acc.get_avail("usdt") + acc.get_freeze('usdt')
            total_btc += acc.get_avail("btc") + acc.get_freeze('btc')
            print v.id, acc.get_avail("usdt") + acc.get_freeze('usdt'), acc.get_avail("btc") + acc.get_freeze('btc')

        # sql = "select * from `account` order by id limit 1"
        init_usdt = 3869.3595346
        init_btc = 0.209667402

        rate = 10000
        return 100 * (total_usdt - init_usdt) / init_usdt, 100 * (total_btc - init_btc) / init_btc, \
               100 * (total_btc * rate + total_usdt - init_btc * rate - init_usdt) / (init_btc * rate + init_usdt)

    def cancel_order(self, exchange, id, symbol):
        print self.exchanges[exchange].cancel_order(symbol, id)


tool = Tool(read_conf("./config.json"))


@click.group()
@click.pass_context
def cli(*args, **kw):
    pass


@cli.command()
def record():
    tool.record_account()

@cli.command()
def benefit():
    print tool.check_benefit()

@cli.command()
@click.option("--id")
@click.option("--symbol", default="btc_usdt")
@click.option("--ex")
def cancel_order(id, symbol, ex):
    tool.cancel_order(ex, id, symbol)


if __name__ == '__main__':
    cli()
