# coding: utf8

from util import cur_ms, read_conf, fix_float_radix
import simplejson as json
from exchange import *
from skyb import MysqlEngine
import click
from order import StrategyManager


class Tool(object):
    def __init__(self, config):
        key_config = config['apikey']
        self.exchanges = {
            'zb': Zb(key_config['zb']['key'], key_config['zb']['secret']),
            'okex': Okex(key_config['okex']['key'], key_config['okex']['secret']),
        }

        self.engine = MysqlEngine(config['db']['url'])
        self.strategy_manager = StrategyManager(self.engine)

    def record_account(self):
        sql = "insert into `account` values (null, ?, ?)"
        acc = {}
        for k, v in self.exchanges.iteritems():
            acc[k] = str(v.account())

        self.engine.execute(sql, (json.dumps(acc), cur_ms()))

    def check_benefit(self, origin=False):
        total_usdt = 0
        total_btc = 0
        for k, v in self.exchanges.iteritems():
            acc = v.account()
            total_usdt += acc.get_avail("usdt") + acc.get_freeze('usdt')
            total_btc += acc.get_avail("btc") + acc.get_freeze('btc')
            print v.id, acc.get_avail("usdt") + acc.get_freeze('usdt'), acc.get_avail("btc") + acc.get_freeze('btc')

        # sql = "select * from `account` order by id limit 1"
        # baseline  1/28 19:00
        #init_usdt = 1057.17674094 + 2783.4295696
        #init_btc = 0.115674402 + 0.096944
        init_usdt = 2100.72694018 + 1688.17922997
        init_btc = 0.0214174019997 + 0.202096

        if origin:
            rate = 11000
        else:
            rate = float(self.exchanges['okex'].fetch_ticker('btc_usdt')['ticker']['last'])
        return total_usdt - init_usdt, total_btc - init_btc, \
               total_btc * rate + total_usdt - init_btc * rate - init_usdt, \
               total_btc * rate + total_usdt, \
               str(fix_float_radix(
                   100 * (total_btc * rate + total_usdt - init_btc * rate - init_usdt) / (init_btc * rate + init_usdt),
                   2)) + '%'

    def cancel_order(self, exchange, id, symbol):
        print self.exchanges[exchange].cancel_order(symbol, id)

    def get_unfinish_strategy(self):
        sts = self.strategy_manager.get_unfinished()
        res = []
        for x in sts:
            s = '%s__%s__%s\n' % (x['name'], x['benefit'], x['amount'])
            res.append(s)
        return '\n'.join(res)


tool = Tool(read_conf("./config.json"))


@click.group()
@click.pass_context
def cli(*args, **kw):
    pass


@cli.command()
def record():
    tool.record_account()


@cli.command()
@click.option("-o", is_flag=True)
def benefit(o):
    print tool.check_benefit(o)


@cli.command()
def unfinish():
    print tool.get_unfinish_strategy()


@cli.command()
@click.option("--id")
@click.option("--symbol", default="btc_usdt")
@click.option("--ex")
def cancel_order(id, symbol, ex):
    tool.cancel_order(ex, id, symbol)


if __name__ == '__main__':
    cli()
