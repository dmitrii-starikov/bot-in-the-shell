from elasticsearch import Elasticsearch
from collections import defaultdict
from cache import redis
import importlib
import jstyleson
import datetime
import psycopg2
import orjson
import time
import os

class BotEntrypointClass:
    # Singleton pattern
    botInstance = None

    # attributes
    exchangeInstance = None
    signalsConfig = None
    cache = None
    cacheSubscriber = None
    elasticSearchConnection = None
    databaseConnection = None
    loggers = []
    signals = []

    # trading
    availableModes = ['BUY', 'SELL', 'HOLD', 'WAIT']
    mode = 'BUY'
    # todo: подумать над этими параметрами
    lastOrderPrice = 0.0
    nextOrderPrice = 0.0
    exchangeConfig = None

    def __new__(cls, *args, **kwargs):
        if cls.botInstance is None:
            cls.botInstance = object.__new__(cls, *args, **kwargs)
        return cls.botInstance

    def __init__(self):
        self.initCache()
        self.setLoggers()
        self.initElastic()
        self.initDatabase()
        self.initExchange()
        self.loadSignalsConfig()
        self.setSignals()
        ###############
        self.startBot()

    def __del__(self):
        print('Bot has stopped')

    def now(self):
        now = datetime.datetime.now()
        return str(now)

    def initCache(self):
        self.cache = redis.RedisCustom()
        print('Init subscribers')
        self.cacheSubscriber = self.cache.redisConnection.pubsub()
        self.cacheSubscriber.psubscribe(**{self.cache.logsChannel: self.onLogMessage})
        # exchange (socket/http/emulator) -> event -> redis -> bot
        self.cacheSubscriber.psubscribe(**{self.cache.orderChannel: self.onOrderMessage})

    def initElastic(self):
        self.elasticSearchConnection = Elasticsearch(os.environ['ELASTICSEARCH_HOST'])
        self.sendMessage('INFO', 'Init ES', {})

    def initDatabase(self):
        dbParams = {
            'dbname': os.environ['POSTGRES_DB'],
            'user': os.environ['POSTGRES_USER'],
            'password': os.environ['POSTGRES_PASSWORD'],
            'host': os.environ['POSTGRES_HOST'],
            'port': os.environ['POSTGRES_PORT']
        }
        self.databaseConnection = psycopg2.connect(**dbParams)
        self.databaseCursor = self.databaseConnection.cursor()
        self.sendMessage('INFO', 'Init DB', {})
        del dbParams

    def initExchange(self):
        configPath = os.path.dirname(os.path.realpath(__file__)) + '/../' + os.environ['EXCHANGE_COMMON_PARAMETERS']
        try:
            if os.path.isfile(configPath):
                with open(configPath) as file:
                    self.exchangeConfig = jstyleson.loads(file.read())
                    file.close()

            if self.exchangeConfig is None:
                raise Exception("Bad JSON")
            else:
                exchangeStrategy = importlib.import_module('exchange.' + os.environ['EXCHANGE'] + 'Strategy')
                exchangeClass = getattr(exchangeStrategy, os.environ['EXCHANGE'] + 'Strategy')
                self.exchangeInstance = exchangeClass()
                self.exchangeInstance.initCache(self.cache)
                self.exchangeInstance.initConfig(self.exchangeConfig)
                self.sendMessage('OK', 'Exchange config has loaded - ' + configPath, self.exchangeConfig)
                self.exchangeInstance.connect()
                del exchangeStrategy, exchangeClass

                self.exchangeInstance.setBalance(self.exchangeConfig['STABLECOIN'], self.exchangeConfig['STABLECOIN_DEPOSIT'])
                self.exchangeInstance.setBalance(self.exchangeConfig['COIN'], 0)
        except Exception as e:
            self.sendMessage('CRITICAL', 'Can\'t load exchange config - ' + configPath, {"error": e})
        finally:
            del configPath

    def setLoggers(self):
        loggersList = os.environ['LOGGERS_LIST'].split(',')
        loggersStrategies = importlib.import_module('logger')
        loggerName = None
        i = 0

        for loggerName in loggersStrategies.__all__:
            loggerClass = importlib.import_module('logger.' + loggerName)
            loggerClass = getattr(loggerClass, loggerName)
            if loggerClass.isSuitable(loggersList):
                loggerInstance = loggerClass()
                try:
                    statuses = os.environ[loggersList[i].upper()+'_LOGGED_LIST']
                    if len(statuses) == 0:
                        raise Exception("Empty status list for logger")
                    loggerInstance.setLoggedStatuses(statuses)
                except Exception:
                    loggerInstance.setLoggedStatuses([])
                self.loggers.append(loggerInstance)
            i += 1

        del loggersList, loggersStrategies, loggerName

    def getAllLogMessages(self):
        while True:
            message = self.cacheSubscriber.get_message()
            if message and message['type'] == 'message':
                continue
            else:
                break

    def onLogMessage(self, channelEvent):
        try:
            messageDTO = orjson.loads(channelEvent['data'])
            level, message, context, time = (messageDTO['level'], messageDTO['message'],
                                             messageDTO['context'], messageDTO['time'])
            for logger in self.loggers:
                logger.log(level, message, context, time)
            # recursive check current messages
            self.getAllLogMessages()
        except Exception as e:
            self.sendMessage('WARNING', 'Bad log message! ' + str(e), {})

    def onOrderMessage(self, channelEvent):
        try:
            orderDTO = orjson.loads(channelEvent['data'])

            orderMode = orderDTO['ORDER_TYPE']
            orderPrice = float(orderDTO['ORDER_PRICE'])
            self.setMode("BUY" if orderMode == "SELL" else "SELL")
            self.setLastOrderPrice(orderPrice)

            # todo deposits
            #     'COIN_AMOUNT': coinValue,
            #     'STABLECOIN_AMOUNT': deposit,
            #     'FEE_SUM': feeSum

            for logger in self.loggers:
                logger.log('OK', 'Order completed', orderDTO, self.now())
        except Exception as e:
            self.sendMessage('WARNING', 'Bad order message! ' + str(e), {})

    def setSignals(self):
        signalList = list(self.signalsConfig['SIGNALS'].keys())
        signalStrategies = importlib.import_module('signalX')
        signalName = None

        for signalName in signalStrategies.__all__:
            signalClass = importlib.import_module('signalX.' + signalName)
            signalClass = getattr(signalClass, signalName)
            if signalClass.isSuitable(signalList):
                self.signals.append(signalClass(
                    self.signalsConfig,
                    self.cache,
                    self.elasticSearchConnection,
                    self.databaseConnection
                ))

        del signalList, signalStrategies, signalName

    def selfCheck(self):
        # todo каждый час
        print('selfCheck')

    def sendMessage(self, level: str, message: str, context: dict):
        self.cache.logEvent(level, message, context)

    def loadSignalsConfig(self):
        configPath = os.path.dirname(os.path.realpath(__file__)) + '/../' + os.environ['TRADING_STRATEGY_CONFIG']
        try:
            if os.path.isfile(configPath):
                with open(configPath) as file:
                    self.signalsConfig = jstyleson.loads(file.read())
                    file.close()

            if self.signalsConfig is None:
                raise Exception("Bad JSON")
            else:
                self.sendMessage('OK', 'Trading strategy has loaded - ' + configPath, self.signalsConfig)
        except Exception as e:
            self.sendMessage('CRITICAL', 'Can\'t load trading config - ' + configPath, {"error": e})
        finally:
            del configPath

    def setMode(self, mode):
        if mode in self.availableModes:
            self.mode = mode
            self.cache.setKeyValue('BOT_MODE', mode)
        else:
            self.sendMessage('ERROR', 'Unknown mode: ' + mode, {'currentMode': self.mode})

    def getMode(self):
        return self.mode

    def setLastOrderPrice(self, price: int):
        self.lastOrderPrice = price
        #self.cache.setKeyValue('', price)

    def getLastOrderPrice(self):
        return self.lastOrderPrice

    def getNextOrderPrice(self, mode):
        lastOrderPrice = self.getLastOrderPrice()

        if self.nextOrderPrice > 0:
            part = float(self.exchangeConfig['ORDER_REWARD_PERCENT'])
            self.nextOrderPrice = lastOrderPrice * ((1.0 - part) if mode == 'BUY' else (1.0 + part))
        else:
            # use config price first
            self.nextOrderPrice = lastOrderPrice
        return self.nextOrderPrice

    def aggregateAnswer(self, decisions: list):
        sums = defaultdict(float)
        counts = defaultdict(int)
        uniqueModifiers = set()
        uniqueBlockers = set()

        for entry in decisions:
            for key in self.availableModes:
                value = entry.get(key)
                if value is not None:
                    sums[key] += value
                    counts[key] += 1

            uniqueModifiers.update(entry.get('MODIFIERS', []))
            uniqueBlockers.update(entry.get('BLOCKERS', []))

        answer = {key: (sums[key] / counts[key]) if counts[key] > 0 else None for key in self.availableModes}

        answer['MODIFIERS'] = list(uniqueModifiers)
        answer['BLOCKERS'] = list(uniqueBlockers)

        return answer

    def makeDecision(self, forecast: dict):
        #currentPrice: float, currentTime: str,
        currentMode = self.getMode()
        #self.sendMessage('NOTICE', 'Current time: ' + currentTime + ', price = ' + str(currentPrice), {})
        #self.sendMessage('INFO', 'Mode: ' + self.getMode() + ', deposit = ' + str(self.exchangeInstance.getBalance()), {})

        # order:
        # bot(makeDecision) -> exchange(order) -> check order -> redis(orderEvent) -> bot(onOrderMessage) -> change status

        if currentMode == 'WAIT':
            #self.sendMessage('DEBUG', 'Just waiting', {})
            pass
        elif currentMode == 'BUY' and forecast['BUY'] >= self.exchangeConfig['BUY_MIN_CONFIDENCE']:
            # print(currentPrice)
            # print(currentPrice * (1.0 - float(self.exchangeConfig['ORDER_REWARD_PERCENT'])))
            self.exchangeInstance.placeOrder('BUY', self.getNextOrderPrice('BUY'), 1.0)
            self.setMode('WAIT')
        elif currentMode == 'SELL' and forecast['SELL'] >= self.exchangeConfig['SELL_MIN_CONFIDENCE']:
            #print(currentPrice * (1.0 + float(self.signalsConfig['ORDER_REWARD_PERCENT'])))
            self.exchangeInstance.placeOrder('SELL', self.getNextOrderPrice('SELL'), 1.0)
            self.setMode('WAIT')
        elif currentMode == 'HOLD':
            pass

        # todo wait сделать таймеры и слипы внутри


    def startBot(self):
        self.sendMessage('OK', 'Bot has been launched', {})

        # at once
        self.getAllLogMessages()
        if 'START_ORDER_PRICE' in self.exchangeConfig.keys() and self.exchangeConfig['START_ORDER_PRICE'] > 0:
            self.setLastOrderPrice(self.exchangeConfig['START_ORDER_PRICE'])
        else:
            currentMarketData = self.exchangeInstance.getMarketData(self.exchangeConfig['COIN'])
            currentPrice = currentMarketData['price']
            self.setLastOrderPrice(currentPrice)

        #print(self.getLastOrderPrice())
        # todo: условия что все ок
        while True:
            self.getAllLogMessages()

            # exchange (market data, check order) -> bot
            currentMarketData = self.exchangeInstance.getMarketData(self.exchangeConfig['COIN'])
            if currentMarketData is None:
                # todo test/prod
                print('ERROR', 'Exchange response error. Stop bot', {})
                break
                #self.__del__()

            currentPrice, currentTime = currentMarketData['price'], currentMarketData['time']
            commonForecast = []

            #todo: здесь должна считаться и меняться цена, по которой хотим купить или продать
            # в makeDecision нужно только передавать текущую цену и результат.
            # ордер ставится при достижении нужной цены - как отложить действие?

            # bot -> signal -> bot
            for signal in self.signals:
                signalAnswer = signal.getWeightedForecast(currentPrice, currentTime)
                # self.sendMessage('DEBUG', 'signalAnswer', {
                #     'currentPrice': currentPrice,
                #     'currentTime': currentTime,
                #     'signalName': signal.signalName,
                #     'signalAnswer': signalAnswer})

                commonForecast.append(signalAnswer)

            # todo: надо учесть лаг на расчеты?
            # todo: blockers-modificators (сначала подключаем все? тут просто вызываем)

            aggregatedAnswer = self.aggregateAnswer(commonForecast)

            # bot -> exchange
            self.makeDecision(aggregatedAnswer)
            #currentTime,

            time.sleep(0.01)
            #time.sleep(int(os.environ['SLEEP_DURATION']))

            # если больше уверенности по конфигу - ставим ордер
            # текущий статус и инфу по ордеру держим в кэше, пишем в базу, если упали


x = BotEntrypointClass()
