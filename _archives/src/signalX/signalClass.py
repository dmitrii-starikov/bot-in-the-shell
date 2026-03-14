from abc import ABC, abstractmethod


class SignalClass(ABC):
    # any strategy should have all tools
    tradingConfig = None
    signalConfig = None
    signalWeight = None
    cache = None
    elasticSearchConnection = None
    databaseConnection = None

    signalName = None
    signalAnswer = {
        "BUY": None,
        "SELL": None,
        "HOLD": None,
        "WAIT": None,
        "MODIFIERS": [],
        "BLOCKERS": []
    }

    @staticmethod
    def isSuitable(names: list) -> bool:
        return __class__.__name__ in names

    def __init__(self, tradingConfig, cache, elasticSearchConnection, databaseConnection):
        self.tradingConfig = tradingConfig
        self.setSignalName()
        self.signalConfig = tradingConfig['SIGNALS'][self.signalName]
        self.signalWeight = self.signalConfig['WEIGHT']
        self.cache = cache
        self.elasticSearchConnection = elasticSearchConnection
        self.databaseConnection = databaseConnection
        self.sendMessage('INFO', 'Init Signal - ' + self.signalName, self.signalConfig)

    def sendMessage(self, level: str, message: str, context: dict):
        self.cache.logEvent(level, message, context)

    @abstractmethod
    def setSignalName(self):
        pass

    @abstractmethod
    def getWeightedForecast(self, time: str) -> dict:
        pass