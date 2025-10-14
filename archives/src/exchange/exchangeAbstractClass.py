from abc import ABC, abstractmethod
import os

class exchangeAbstractClass(ABC):
    session=None
    cache=None
    config=None

    def sendMessage(self, level: str, message: str, context: dict):
        self.cache.logEvent(level, message, context)

    def connect(self):
        print('Exchange Abstract Class')

    def initCache(self, cacheConnection):
        self.cache = cacheConnection

    def initConfig(self, config):
        self.config = config

    @abstractmethod
    def getBalance(self):
        pass

    def checkCandle(self, currentDeposit: float) -> bool:
        # todo:
        pass

    @abstractmethod
    def getMarketData(self, symbol):
        pass

    @abstractmethod
    def placeOrder(self, symbol, quantity, orderType, price=None):
        pass

    @abstractmethod
    def cancelOrder(self, orderId):
        pass

    @abstractmethod
    def checkOrder(self, orderId):
        pass

    def now(self):
        now = datetime.datetime.now()
        return str(now)

    # todo принимать числа