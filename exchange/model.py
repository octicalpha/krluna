# coding: utf8

class ORDER_STATUS(object):
    INIT = 0
    PLACED = 1

    CANCELLED = 50

    PARTIAL_SUCCESS = 99
    SUCCESS = 100

    UNKNOWN = 1000

    @classmethod
    def from_code(cls, c):
        if c == 0:
            return "INIT"
        elif c == 1:
            return 'PLACED'
        elif c == 50:
            return 'CANCELLED'
        elif c == 99:
            return 'PARTIAL_SUCCESS'
        elif c == 100:
            return 'SUCCESS'
        elif c == 1000:
            return 'UNKNOWN'


class Account(object):
    def __init__(self, exchange):
        self.exchange = exchange
        self.assets = {}

    def add(self, asset):
        self.assets[asset.coin] = asset

    def get_avail(self, coin):
        if coin not in self.assets:
            return 0
        return self.assets[coin].avail

    def get_freeze(self, coin):
        if coin not in self.assets:
            return 0
        return self.assets[coin].freeze

    def set_avail(self, coin, amount):
        if coin not in self.assets:
            asset = Asset(coin, amount, 0)
            self.add(asset)
        else:
            self.assets[coin].avail = amount
        return self

    def set_freeze(self, coin, amount):
        if coin not in self.assets:
            asset = Asset(coin, 0, amount)
            self.add(asset)
        else:
            self.assets[coin].freeze = amount
        return self

    def __str__(self):
        a = ' , '.join([str(x) for x in self.assets.values()])
        return '[%s | %s]' % (self.exchange.id, a)

    __repr__ = __str__


class Asset(object):
    def __init__(self, coin, avail, freeze):
        self.coin = coin.lower()
        self.avail = avail
        self.freeze = freeze

    def __str__(self):
        return '<Asset %s|%s|%s>' % (self.coin, self.avail, self.freeze)

    __repr__ = __str__


class Order(object):
    def __init__(self, id, symbol, price, amount, ts, side, status):
        self.id = id
        self.symbol = symbol
        self.price = float(price)
        self.amount = float(amount)
        self.ts = int(ts)
        self.side = side
        self.status = status

    def __str__(self):
        return "<Order %s|%s|%s|%s|%s|%s|%s>" % (
        self.id, ORDER_STATUS.from_code(self.status), self.symbol, self.price, self.amount, self.ts, self.side)

    __repr__ = __str__
