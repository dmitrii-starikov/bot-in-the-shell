from .exchangeAbstractClass import exchangeAbstractClass
import binance
import os

class BinanceStrategy(exchangeAbstractClass):
    def connect(self):
        self.sendMessage('INFO', 'BinanceStrategy has chosen', {})

    def initCache(self):
        pass

    def getBalance(self):
        pass

    def getMarketData(self, symbol):
        pass

    def cancelOrder(self, orderId):
        pass

    def checkOrder(self, orderId):
        pass