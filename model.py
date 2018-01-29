# coding: utf8

class Account(object):
	def __init__(self, exchange):
		self.exchange = exchange
		self.assets = {}

	def add(self, asset):
		self.assets[asset.coin] = asset

	def __str__(self):
		a =  ' , '.join([str(x) for x in self.assets.values()])
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